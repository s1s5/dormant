//! HTTPリバースプロキシ: リクエスト転送、起動待ちフロー、WebSocketブリッジ

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::{SinkExt, Stream, StreamExt};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, BodyStream, Empty, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::docker::DockerClient;
use crate::lifecycle;
use crate::lifecycle::Sessions;
use crate::router::Router;

/// HTTP転送用クライアント
type ProxyClient = Client<hyper_util::client::legacy::connect::HttpConnector, Incoming>;

/// リクエストのホストを解決する(HTTP/1.1 Hostヘッダー → HTTP/2 :authority の順)
fn resolve_host<B>(req: &Request<B>) -> String {
    req.headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.uri().authority().map(|a| a.as_str()))
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("")
        .to_string()
}

/// WSアップグレード用クライアント(ボディなし)
type WsClient = Client<hyper_util::client::legacy::connect::HttpConnector, Empty<Bytes>>;
type BoxResp = Response<BoxBody<Bytes, std::io::Error>>;

/// HTTPサーバー起動
pub async fn serve(
    config: &Config,
    docker: DockerClient,
    router: Arc<Router>,
    sessions: Sessions,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&config.listen).await?;
    tracing::info!("dormant listening on {}", config.listen);
    serve_listener(listener, docker, router, sessions).await
}

/// 指定リスナーで HTTP サーバーを起動(テストからも利用)
pub async fn serve_listener(
    listener: TcpListener,
    docker: DockerClient,
    router: Arc<Router>,
    sessions: Sessions,
) -> anyhow::Result<()> {
    let client = Client::builder(TokioExecutor::new())
        .timer(TokioTimer::new())
        .build_http();
    let ws_client = Client::builder(TokioExecutor::new())
        .timer(TokioTimer::new())
        .build_http();
    let h2_client = Client::builder(TokioExecutor::new())
        .timer(TokioTimer::new())
        .http2_only(true)
        .build_http();

    loop {
        let (stream, _) = listener.accept().await?;
        let client = client.clone();
        let ws_client = ws_client.clone();
        let h2_client = h2_client.clone();
        let docker = docker.clone();
        let router = router.clone();
        let sessions = sessions.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |req: Request<Incoming>| {
                let client = client.clone();
                let ws_client = ws_client.clone();
                let h2_client = h2_client.clone();
                let docker = docker.clone();
                let router = router.clone();
                let sessions = sessions.clone();
                async move { handle(req, client, ws_client, h2_client, docker, router, sessions).await }
            });
            let mut builder = auto::Builder::new(TokioExecutor::new());
            builder.http1().timer(TokioTimer::new());
            let conn = builder.serve_connection_with_upgrades(io, service);
            if let Err(e) = conn.await {
                tracing::debug!("connection error: {}", e);
            }
        });
    }
}

/// リクエストハンドラ
async fn handle(
    req: Request<Incoming>,
    client: ProxyClient,
    ws_client: WsClient,
    h2_client: ProxyClient,
    docker: DockerClient,
    router: Arc<Router>,
    sessions: Sessions,
) -> Result<BoxResp, Infallible> {
    // /healthz は自身のヘルスチェック
    if req.uri().path() == "/healthz" {
        return Ok(ok_response("ok\n"));
    }

    // Hostヘッダー → HTTP/2 :authority の順でホストを解決
    let host = resolve_host(&req);

    // ホストからルートを解決(動的コンテナ or 静的ルート)
    let route = match router.resolve_with_static(&host).await {
        Some(r) => r,
        None => {
            tracing::warn!("no route for host: {}", host);
            return Ok(error_response("no route for host", StatusCode::NOT_FOUND));
        }
    };

    // WebSocket判定
    let is_ws = req
        .headers()
        .get("upgrade")
        .map(|v| v.to_str().unwrap_or("").eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);

    // 静的ルート: dormant が管理しない外部固定宛先へ直接転送
    // (起動待ち・セッション管理なし。dormant は起動・停止しない)
    if let Some(target) = route.static_target() {
        let target_addr = format!("{}:{}", target.ip, target.port);
        tracing::debug!("static forward {} -> {}", host, target_addr);
        if is_ws {
            return handle_ws_static(req, ws_client, &target_addr).await;
        }
        return handle_http_static(req, client, h2_client, &target_addr).await;
    }

    // 動的ルート(dormant 管理対象コンテナ)
    let (container, route_port) = match route {
        crate::router::RouteResult::Dynamic(c, p) => (*c, p),
        crate::router::RouteResult::Static(_) => unreachable!("static handled above"),
    };

    // セッション記録(アクセスでタイマーリセット)
    // 起動の成否に関わらずアクセス時点で記録し、次のアクセスまでセッションを維持する
    sessions
        .touch(&container.id, container.session_duration)
        .await;

    // グループ起動(G2: グループ内全コンテナを起動してから転送)
    if let Some(group) = &container.group {
        match lifecycle::ensure_group_started(&docker, &router, group).await {
            Ok(()) => tracing::debug!("group {} started", group),
            Err(e) => {
                tracing::warn!("group {} start failed: {}", group, e);
                return Ok(error_response(
                    "container failed to start",
                    StatusCode::GATEWAY_TIMEOUT,
                ));
            }
        }
    }

    // 依存解決用のコンテナ一覧(D1)
    let containers = router.containers().await;

    if is_ws {
        return handle_ws(
            req,
            ws_client,
            docker,
            (container.clone(), route_port),
            &containers,
            sessions,
        )
        .await;
    }

    // 通常HTTP / SSE: 起動待ち → 転送
    handle_http(
        req,
        client,
        h2_client,
        docker,
        (container, route_port),
        &containers,
        sessions,
    )
    .await
}

/// gRPC判定: Content-Type が application/grpc で始まるか
fn is_grpc<T>(req: &Request<T>) -> bool {
    req.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.starts_with("application/grpc"))
        .unwrap_or(false)
}

/// 通常HTTP/SSE転送(起動待ち込み)
async fn handle_http(
    req: Request<Incoming>,
    client: ProxyClient,
    h2_client: ProxyClient,
    docker: DockerClient,
    (container, route_port): (crate::docker::ManagedContainer, u16),
    containers: &[crate::docker::ManagedContainer],
    sessions: Sessions,
) -> Result<BoxResp, Infallible> {
    // 起動待ち(成功時は転送先アドレスが返る)。ルート指定ポートで疎通確認
    let target_addr =
        match lifecycle::ensure_started_with_port(&docker, &container, containers, Some(route_port))
            .await
        {
            Ok(addr) => addr,
            Err(e) => {
                tracing::warn!(
                    "startup failed for {} ({}): {}",
                    container.name,
                    container.id,
                    e
                );

                return Ok(error_response(
                    "container failed to start",
                    StatusCode::GATEWAY_TIMEOUT,
                ));
            }
        };

    // バックエンドへ転送(コンテナIP直アクセス)
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    let target = format!(
        "http://{}/{}",
        target_addr,
        trim_leading_slash(path_and_query)
    );
    tracing::debug!("forward {} -> {}", container.name, target);

    let mut builder = Request::builder().method(req.method()).uri(&target);
    // ヘッダーを転送(host は転送先アドレスに置き換わるため除外)
    for (k, v) in req.headers() {
        if k != hyper::header::HOST {
            builder = builder.header(k, v);
        }
    }
    let forwarded_req = builder.body(req.into_body()).unwrap();

    // gRPC(C1)は HTTP/2 クライアント、それ以外は従来の HTTP/1.1 クライアント(C3)
    let selected = if is_grpc(&forwarded_req) {
        h2_client
    } else {
        client
    };
    // 転送開始(接続保護対象)
    sessions.connect(&container.id).await;

    match selected.request(forwarded_req).await {
        Ok(resp) => {
            let status = resp.status();
            let headers = resp.headers().clone();
            let body = resp
                .into_body()
                .map_err(std::io::Error::other)
                .boxed();
            let stream = ActiveStream::new(BodyStream::new(body), sessions, container.id.clone());
            let mut out = Response::new(BodyExt::boxed(StreamBody::new(stream)));
            *out.status_mut() = status;
            *out.headers_mut() = headers;
            Ok(out)
        }
        Err(e) => {
            sessions.disconnect(&container.id).await;
            tracing::warn!("backend error for {}: {}", container.name, e);
            Ok(error_response("backend error", StatusCode::BAD_GATEWAY))
        }
    }
}

/// 静的ルートへの通常HTTP/SSE転送(起動待ちなし・セッション管理なし)
/// dormant が管理しない外部固定宛先へ直接転送する
async fn handle_http_static(
    req: Request<Incoming>,
    client: ProxyClient,
    h2_client: ProxyClient,
    target_addr: &str,
) -> Result<BoxResp, Infallible> {
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    let target = format!(
        "http://{}/{}",
        target_addr,
        trim_leading_slash(path_and_query)
    );
    tracing::debug!("static forward -> {}", target);

    // 元の Host ヘッダーを保持する(転送先ホストのルーティングを維持するため)。
    // 転送先アドレス(target_addr)で Host を上書きすると、下流のプロキシが
    // 元のホスト名(例: *.duet3.localhost)でルーティングできず no route になる。
    let orig_host = req.headers().get(hyper::header::HOST).cloned();

    let mut builder = Request::builder().method(req.method()).uri(&target);
    // ヘッダーを転送(Host は元の値を維持し、重複登録を避ける)
    for (k, v) in req.headers() {
        if k != hyper::header::HOST {
            builder = builder.header(k, v);
        }
    }
    if let Some(h) = orig_host {
        builder = builder.header(hyper::header::HOST, h);
    }
    let forwarded_req = builder.body(req.into_body()).unwrap();

    // gRPC(C1)は HTTP/2 クライアント、それ以外は従来の HTTP/1.1 クライアント(C3)
    let selected = if is_grpc(&forwarded_req) {
        h2_client
    } else {
        client
    };

    match selected.request(forwarded_req).await {
        Ok(resp) => {
            let status = resp.status();
            let headers = resp.headers().clone();
            let body = resp
                .into_body()
                .map_err(std::io::Error::other)
                .boxed();
            let mut out = Response::new(body);
            *out.status_mut() = status;
            *out.headers_mut() = headers;
            Ok(out)
        }
        Err(e) => {
            tracing::warn!("static backend error for {}: {}", target_addr, e);
            Ok(error_response("backend error", StatusCode::BAD_GATEWAY))
        }
    }
}

/// 静的ルートへのWebSocketブリッジ(起動待ちなし・セッション管理なし)
async fn handle_ws_static(
    mut req: Request<Incoming>,
    ws_client: WsClient,
    target_addr: &str,
) -> Result<BoxResp, Infallible> {
    // パスとヘッダーを先に取得
    let path = req.uri().path().to_string();
    let ws_key = req
        .headers()
        .get("sec-websocket-key")
        .cloned()
        .unwrap_or_else(|| hyper::header::HeaderValue::from_static(""));
    let ws_version = req
        .headers()
        .get("sec-websocket-version")
        .cloned()
        .unwrap_or_else(|| hyper::header::HeaderValue::from_static("13"));
    // 元の Host ヘッダーを保持する(転送先ホストのルーティングを維持するため)
    let orig_host = req.headers().get(hyper::header::HOST).cloned();

    let client_upgraded_fut = hyper::upgrade::on(&mut req);

    // バックエンドへアップグレードリクエスト(外部固定宛先へ直接)
    let target = format!("http://{}/{}", target_addr, trim_leading_slash(&path));
    let mut ws_req_builder = Request::builder()
        .method("GET")
        .uri(&target)
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-key", ws_key)
        .header("sec-websocket-version", ws_version);
    if let Some(h) = orig_host {
        ws_req_builder = ws_req_builder.header(hyper::header::HOST, h);
    }
    let ws_req = ws_req_builder.body(Empty::new()).unwrap();

    match ws_client.request(ws_req).await {
        Ok(resp) => {
            if resp.status() == StatusCode::SWITCHING_PROTOCOLS {
                let backend_headers = resp.headers().clone();
                let backend_upgraded = match hyper::upgrade::on(resp).await {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::warn!("backend upgrade error: {}", e);
                        return Ok(error_response("upgrade failed", StatusCode::BAD_GATEWAY));
                    }
                };

                // クライアントに101を返す
                let mut resp101 = Response::new(
                    Empty::new()
                        .map_err(std::io::Error::other)
                        .boxed(),
                );
                *resp101.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
                for (k, v) in &backend_headers {
                    resp101.headers_mut().insert(k, v.clone());
                }

                tokio::spawn(async move {
                    match client_upgraded_fut.await {
                        Ok(client_upgraded) => {
                            bridge_websocket(client_upgraded, backend_upgraded).await;
                        }
                        Err(e) => tracing::warn!("client upgrade error: {}", e),
                    }
                });

                Ok(resp101)
            } else {
                let status = resp.status();
                let body = resp
                    .into_body()
                    .map_err(std::io::Error::other)
                    .boxed();
                let mut out = Response::new(body);
                *out.status_mut() = status;
                Ok(out)
            }
        }
        Err(e) => {
            tracing::warn!("ws static backend request error: {}", e);
            Ok(error_response("backend error", StatusCode::BAD_GATEWAY))
        }
    }
}

/// WebSocketブリッジ
async fn handle_ws(
    mut req: Request<Incoming>,
    ws_client: WsClient,
    docker: DockerClient,
    (container, route_port): (crate::docker::ManagedContainer, u16),
    containers: &[crate::docker::ManagedContainer],
    sessions: Sessions,
) -> Result<BoxResp, Infallible> {
    // 起動待ち(成功時は転送先アドレスが返る)。ルート指定ポートで疎通確認
    let target_addr =
        match lifecycle::ensure_started_with_port(&docker, &container, containers, Some(route_port))
            .await
        {
            Ok(addr) => addr,
            Err(e) => {
                tracing::warn!("startup failed for ws {}: {}", container.name, e);
                return Ok(error_response(
                    "container failed to start",
                    StatusCode::GATEWAY_TIMEOUT,
                ));
            }
        };

    // パスとヘッダーを先に取得
    let path = req.uri().path().to_string();

    let client_upgraded_fut = hyper::upgrade::on(&mut req);

    // バックエンドへアップグレードリクエスト(コンテナIP直アクセス)
    // クライアントのヘッダーを全て転送する(host は転送先アドレスに置き換わるため除外)。
    // connection/upgrade/sec-websocket-key/version もクライアントから来るのでそのまま渡り、
    // sec-websocket-protocol 等の拡張ヘッダーも正しく転送される。
    let target = format!("http://{}/{}", target_addr, trim_leading_slash(&path));
    let mut builder = Request::builder().method("GET").uri(&target);
    for (k, v) in req.headers() {
        if k != hyper::header::HOST {
            builder = builder.header(k, v);
        }
    }
    let ws_req = builder.body(Empty::new()).unwrap();

    match ws_client.request(ws_req).await {
        Ok(resp) => {
            if resp.status() == StatusCode::SWITCHING_PROTOCOLS {
                let backend_headers = resp.headers().clone();
                let backend_upgraded = match hyper::upgrade::on(resp).await {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::warn!("backend upgrade error: {}", e);
                        return Ok(error_response("upgrade failed", StatusCode::BAD_GATEWAY));
                    }
                };

                // クライアントに101を返す
                let mut resp101 = Response::new(
                    Empty::new()
                        .map_err(std::io::Error::other)
                        .boxed(),
                );
                *resp101.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
                for (k, v) in &backend_headers {
                    resp101.headers_mut().insert(k, v.clone());
                }

                // A3: ブリッジ動作中は停止対象外
                let id = container.id.clone();
                sessions.connect(&id).await;
                tokio::spawn(async move {
                    match client_upgraded_fut.await {
                        Ok(client_upgraded) => {
                            bridge_websocket(client_upgraded, backend_upgraded).await;
                        }
                        Err(e) => tracing::warn!("client upgrade error: {}", e),
                    }
                    sessions.disconnect(&id).await;
                });

                Ok(resp101)
            } else {
                let status = resp.status();
                let body = resp
                    .into_body()
                    .map_err(std::io::Error::other)
                    .boxed();
                let mut out = Response::new(body);
                *out.status_mut() = status;
                Ok(out)
            }
        }
        Err(e) => {
            tracing::warn!("ws backend request error: {}", e);
            Ok(error_response("backend error", StatusCode::BAD_GATEWAY))
        }
    }
}

/// 双方向WebSocketブリッジ
async fn bridge_websocket(client_upgraded: Upgraded, backend_upgraded: Upgraded) {
    let client_ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioIo::new(client_upgraded),
        tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let backend_ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioIo::new(backend_upgraded),
        tungstenite::protocol::Role::Client,
        None,
    )
    .await;

    let (mut client_tx, mut client_rx) = client_ws.split();
    let (mut backend_tx, mut backend_rx) = backend_ws.split();

    let c2b = tokio::spawn(async move {
        while let Some(Ok(msg)) = client_rx.next().await {
            if backend_tx.send(msg).await.is_err() {
                break;
            }
        }
    });
    let b2c = tokio::spawn(async move {
        while let Some(Ok(msg)) = backend_rx.next().await {
            if client_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    let _ = tokio::try_join!(c2b, b2c);
}

fn trim_leading_slash(path: &str) -> String {
    path.trim_start_matches('/').to_string()
}

/// レスポンスボディのストリームをラップし、EOF/切断でアクティブ接続を解放する
struct ActiveStream<S> {
    inner: S,
    sessions: Sessions,
    id: String,
}

impl<S> ActiveStream<S> {
    fn new(inner: S, sessions: Sessions, id: String) -> Self {
        Self {
            inner,
            sessions,
            id,
        }
    }

    fn end(&self) {
        let sessions = self.sessions.clone();
        let id = self.id.clone();
        tokio::spawn(async move {
            sessions.disconnect(&id).await;
        });
    }
}

impl<S, E> Stream for ActiveStream<S>
where
    S: Stream<Item = Result<Frame<Bytes>, E>> + Unpin,
{
    type Item = Result<Frame<Bytes>, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(e))) => {
                self.end();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.end();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl<S> Drop for ActiveStream<S> {
    fn drop(&mut self) {
        self.end();
    }
}

fn ok_response(body: &'static str) -> BoxResp {
    Response::new(
        Full::new(Bytes::from(body))
            .map_err(std::io::Error::other)
            .boxed(),
    )
}

fn error_response(body: &'static str, status: StatusCode) -> BoxResp {
    let mut resp = Response::new(
        Full::new(Bytes::from(body))
            .map_err(std::io::Error::other)
            .boxed(),
    );
    *resp.status_mut() = status;
    resp
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use crate::docker::Route;
    use crate::testutil::{self, MockContainer};
    use http_body_util::Full;
    use std::sync::Arc;
    use std::time::Duration;

    /// モックDocker + プロキシサーバーを立て、その待ち受け addr を返す
    async fn spawn_proxy(
        containers: Vec<MockContainer>,
        managed: Vec<crate::docker::ManagedContainer>,
    ) -> (String, testutil::MockDocker) {
        spawn_proxy_with_sessions(containers, managed, Sessions::new()).await
    }

    /// 静的ルート付きでプロキシサーバーを立てる
    async fn spawn_proxy_with_static(
        containers: Vec<MockContainer>,
        managed: Vec<crate::docker::ManagedContainer>,
        static_routes: Vec<crate::config::StaticRouteEntry>,
    ) -> (String, testutil::MockDocker) {
        let (docker, mock) = testutil::setup_mock_docker(containers).await;
        let router = Arc::new(Router::new());
        router.update(managed).await;
        router.set_static_routes(&static_routes).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = serve_listener(listener, docker, router, Sessions::new()).await;
        });
        (addr, mock)
    }

    /// Sessions を外から注入できる版(アクティブ接続保護のテスト用)
    async fn spawn_proxy_with_sessions(
        containers: Vec<MockContainer>,
        managed: Vec<crate::docker::ManagedContainer>,
        sessions: Sessions,
    ) -> (String, testutil::MockDocker) {
        let (docker, mock) = testutil::setup_mock_docker(containers).await;
        let router = Arc::new(Router::new());
        router.update(managed).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = serve_listener(listener, docker, router, sessions).await;
        });
        (addr, mock)
    }

    /// 実バックエンドを立て、モックコンテナの ip/port に合わせた ManagedContainer を生成
    async fn backend_container(
        id: &str,
        group: Option<&str>,
    ) -> (MockContainer, crate::docker::ManagedContainer) {
        let addr = testutil::spawn_backend().await;
        let (ip, port) = addr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();
        let mut c = testutil::make_container(id, group);
        c.port = Some(port);
        c.startup_timeout = Duration::from_secs(3);
        c.ip = Some(ip.to_string());
        c.routes = vec![Route {
            host: format!("{}.localhost", id),
            port: None,
        }];
        (MockContainer::new(id, ip, port), c)
    }

    async fn get(addr: &str, host: &str) -> (u16, String) {
        let client = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .build_http();
        let req = Request::builder()
            .uri(format!("http://{}/test", addr))
            .header("host", host)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.request(req).await.unwrap();
        let status = resp.status().as_u16();
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, String::from_utf8_lossy(&body).to_string())
    }

    // G1: グループなし単体 → 起動して200
    #[tokio::test]
    async fn test_group_G1_proxy_returns_200() {
        let (mc, c) = backend_container("web", None).await;
        let (addr, mock) = spawn_proxy(vec![mc], vec![c]).await;
        let (status, _) = get(&addr, "web.localhost").await;
        assert_eq!(status, 200);
        assert!(mock.is_running("web"));
    }

    // G2-1: グループ全員 ready → 200
    #[tokio::test]
    async fn test_group_G2_1_proxy_returns_200() {
        let (mc1, c1) = backend_container("web1", Some("grp")).await;
        let (mc2, c2) = backend_container("web2", Some("grp")).await;
        let (addr, mock) = spawn_proxy(vec![mc1, mc2], vec![c1, c2]).await;
        let (status, _) = get(&addr, "web1.localhost").await;
        assert_eq!(status, 200);
        // グループ全員起動
        assert!(mock.is_running("web1"));
        assert!(mock.is_running("web2"));
    }

    // G2-2: グループの一部が失敗 → 504
    #[tokio::test]
    async fn test_group_G2_2_proxy_returns_504() {
        let (mc1, c1) = backend_container("web1", Some("grp")).await;
        let (mc2, c2) = backend_container("web2", Some("grp")).await;
        let mut mc2 = mc2;
        mc2.start_fails = true;
        let (addr, _mock) = spawn_proxy(vec![mc1, mc2], vec![c1, c2]).await;
        let (status, _) = get(&addr, "web1.localhost").await;
        assert_eq!(status, 504);
    }

    // G2-3: グループ内に起動済みがあればスキップして残りを起動
    #[tokio::test]
    async fn test_group_G2_3_proxy_skips_running() {
        let (mc1, c1) = backend_container("web1", Some("grp")).await;
        let (mc2, c2) = backend_container("web2", Some("grp")).await;
        let mut mc1 = mc1;
        mc1.running = true;
        let (addr, mock) = spawn_proxy(vec![mc1, mc2], vec![c1, c2]).await;
        let (status, _) = get(&addr, "web1.localhost").await;
        assert_eq!(status, 200);
        // 起動済み web1 は起動されず web2 のみ起動
        assert_eq!(mock.start_order(), vec!["web2"]);
        assert!(mock.is_running("web1"));
    }

    // gRPC判定: application/grpc 系は true
    #[test]
    fn test_grpc_detect_content_type() {
        let req = Request::builder()
            .uri("http://x/")
            .header("content-type", "application/grpc")
            .body(Empty::<Bytes>::new())
            .unwrap();
        assert!(is_grpc(&req));

        let req = Request::builder()
            .uri("http://x/")
            .header("content-type", "application/grpc+proto")
            .body(Empty::<Bytes>::new())
            .unwrap();
        assert!(is_grpc(&req));

        let req = Request::builder()
            .uri("http://x/")
            .header("content-type", "application/json")
            .body(Empty::<Bytes>::new())
            .unwrap();
        assert!(!is_grpc(&req));

        let req = Request::builder()
            .uri("http://x/")
            .body(Empty::<Bytes>::new())
            .unwrap();
        assert!(!is_grpc(&req));
    }

    // A1: SSE接続中はアクティブカウントが増え、レスポンスボディ完了で解放される
    #[tokio::test]
    async fn test_sse_A1_active_during_stream_and_released_on_eof() {
        // SSEバックエンドを立てる
        let addr = testutil::spawn_sse_backend().await;
        let (ip, port) = addr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();
        let mut mc = MockContainer::new("sse", ip, port);
        mc.running = true; // 起動済みにしておく
        let mut c = testutil::make_container("sse", None);
        c.port = Some(port);
        c.startup_timeout = Duration::from_secs(3);
        c.ip = Some(ip.to_string());
        c.routes = vec![Route {
            host: "sse.localhost".to_string(),
            port: None,
        }];
        c.session_duration = Duration::from_millis(50);

        let sessions = Sessions::new();
        let (addr, _mock) = spawn_proxy_with_sessions(vec![mc], vec![c], sessions.clone()).await;

        // SSEリクエスト(ボディは読まずにコネクションを保持)
        let client = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .build_http();
        let req = Request::builder()
            .uri(format!("http://{}/stream", addr))
            .header("host", "sse.localhost")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.request(req).await.unwrap();
        assert_eq!(resp.status(), 200);
        // 接続中のアクティブカウントが1
        assert_eq!(sessions.active_count("sse").await, 1);
        // アクティブ接続中は expired に出ない
        assert!(sessions.expired().await.is_empty());

        // 最初のフレームを1つだけ読む(EOFは待たない)
        let mut body = resp.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(frame.into_data().unwrap().as_ref(), b"data: hi\n\n");
        // ボディをドロップ → ActiveStream::drop → disconnect
        drop(body);
        // 非同期disconnectが走るのを待つ
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(sessions.active_count("sse").await, 0);
        // 期限超過(50ms)後は expired に出る
        let expired = sessions.expired().await;
        assert!(expired.contains(&"sse".to_string()));
    }

    // E32: HTTP/2 (h2c) でアクセス → 200
    #[tokio::test]
    async fn test_http2_E32_h2c_request_returns_200() {
        let (mc, c) = backend_container("web", None).await;
        let (addr, _mock) = spawn_proxy(vec![mc], vec![c]).await;

        let client = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .http2_only(true)
            .build_http();
        let req = Request::builder()
            .uri(format!("http://{}/test", addr))
            .header("host", "web.localhost")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.request(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"ok");
    }

    // E33: gRPC (application/grpc) リクエストは HTTP/2 バックエンドへ転送される
    #[tokio::test]
    async fn test_grpc_E33_h2_backend_forward() {
        // h2バックエンドを立てる
        let baddr = testutil::spawn_h2_backend().await;
        let (ip, port) = baddr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();
        let mut mc = MockContainer::new("grpc", ip, port);
        mc.running = true;
        let mut c = testutil::make_container("grpc", None);
        c.port = Some(port);
        c.startup_timeout = Duration::from_secs(3);
        c.ip = Some(ip.to_string());
        c.routes = vec![Route {
            host: "grpc.localhost".to_string(),
            port: None,
        }];

        let (addr, _mock) = spawn_proxy(vec![mc], vec![c]).await;

        // gRPCリクエスト(Content-Type: application/grpc)を h2c で送る
        let client = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .http2_only(true)
            .build_http();
        let req = Request::builder()
            .uri(format!("http://{}/grpc.health.v1.Health/Check", addr))
            .header("host", "grpc.localhost")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.request(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"h2-ok");
    }

    // C3: gRPCでない通常リクエストは従来どおり HTTP/1.1 バックエンドへ転送
    #[tokio::test]
    async fn test_grpc_C3_non_grpc_uses_http1_backend() {
        // h2バックエンド(非gRPCリクエストではHTTP/1.1で転送されるためh2のみだと失敗する)
        let baddr = testutil::spawn_h2_backend().await;
        let (ip, port) = baddr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();
        let mut mc = MockContainer::new("grpc", ip, port);
        mc.running = true;
        let mut c = testutil::make_container("grpc", None);
        c.port = Some(port);
        c.startup_timeout = Duration::from_secs(3);
        c.ip = Some(ip.to_string());
        c.routes = vec![Route {
            host: "grpc.localhost".to_string(),
            port: None,
        }];

        let (addr, _mock) = spawn_proxy(vec![mc], vec![c]).await;

        // gRPC でない通常リクエスト → HTTP/1.1 で h2 バックエンドに送ると失敗するはず
        let client = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .build_http();
        let req = Request::builder()
            .uri(format!("http://{}/plain", addr))
            .header("host", "grpc.localhost")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.request(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 502);
    }

    // H1: HTTP/1.1 Hostヘッダーからホストを解決する
    #[test]
    fn test_host_resolution_http1_host_header() {
        let req = Request::builder()
            .uri("http://example.localhost/test")
            .header("host", "web.localhost")
            .body(Full::new(Bytes::new()))
            .unwrap();
        assert_eq!(resolve_host(&req), "web.localhost");
    }

    // H2: HTTP/2 :authority(URI authority)からホストを解決する
    #[test]
    fn test_host_resolution_http2_authority() {
        let req = Request::builder()
            .uri("http://grpc.localhost/grpc.health.v1.Health/Check")
            .body(Full::new(Bytes::new()))
            .unwrap();
        assert_eq!(resolve_host(&req), "grpc.localhost");
    }

    // H3: authority にポートが付いていてもホスト名のみ抽出する
    #[test]
    fn test_host_resolution_authority_with_port() {
        let req = Request::builder()
            .uri("http://foo.localhost:18000/")
            .body(Full::new(Bytes::new()))
            .unwrap();
        assert_eq!(resolve_host(&req), "foo.localhost");
    }

    // ---- 静的ルート(外部固定宛先へ直接転送) ----

    /// テスト用 StaticRouteEntry
    fn sre(pattern: &str, ip: &str, port: u16) -> crate::config::StaticRouteEntry {
        crate::config::StaticRouteEntry {
            pattern: pattern.to_string(),
            ip: ip.to_string(),
            port,
        }
    }

    // S1: 静的完全一致ルートへのリクエストは起動待ちなしで直接転送される(200)
    #[tokio::test]
    async fn test_static_S1_exact_route_forwards_directly() {
        // 静的宛先として使う実バックエンドを立てる
        let addr = testutil::spawn_backend().await;
        let (ip, port) = addr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();

        // 管理対象コンテナ(起動されるべきでない)
        let mc = testutil::MockContainer::new("managed", "127.0.0.1", 1);
        let c = testutil::make_container("managed", None);

        let (proxy_addr, mock) = spawn_proxy_with_static(
            vec![mc],
            vec![c],
            vec![sre("api.example.com", ip, port)],
        )
        .await;

        let (status, body) = get(&proxy_addr, "api.example.com").await;
        assert_eq!(status, 200);
        assert_eq!(body, "ok");
        // 静的転送は起動待ちなし → 管理対象コンテナは起動されない
        assert!(mock.start_order().is_empty());
    }

    // S2: 静的ワイルドカードルート(任意深度)へ直接転送される
    #[tokio::test]
    async fn test_static_S2_wildcard_route_forwards_directly() {
        let addr = testutil::spawn_backend().await;
        let (ip, port) = addr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();

        let (addr, _mock) = spawn_proxy_with_static(
            vec![],
            vec![],
            vec![sre("*.example.com", ip, port)],
        )
        .await;

        // 1段・多段の両方が直接転送される
        let (status, body) = get(&addr, "foo.example.com").await;
        assert_eq!(status, 200);
        assert_eq!(body, "ok");
        let (status, _) = get(&addr, "a.b.c.example.com").await;
        assert_eq!(status, 200);
        // ベースドメインは静的ルートにマッチしない → 404
        let (status, _) = get(&addr, "example.com").await;
        assert_eq!(status, 404);
    }

    // S2b: 静的転送時に元の Host ヘッダーが維持される(下流プロキシのルーティングに使える)
    // 修正前: 転送先アドレスで Host を上書きしており、下流の dormant が
    // 元のホスト名(例: *.duet3.localhost)で解決できず no route になる問題があった。
    #[tokio::test]
    async fn test_static_S2b_forward_preserves_original_host() {
        // Host ヘッダーを body として返すバックエンド
        let addr = testutil::spawn_backend_echo_host().await;
        let (ip, port) = addr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();

        let (addr, _mock) = spawn_proxy_with_static(
            vec![],
            vec![],
            vec![sre("*.duet3.localhost", ip, port)],
        )
        .await;

        // 元の Host がそのままバックエンドへ届くこと
        let (status, body) = get(&addr, "agent-gateway.duet3.localhost").await;
        assert_eq!(status, 200);
        assert_eq!(body, "agent-gateway.duet3.localhost");
    }

    // S3: 静的ルートと動的ルートが衝突する場合は動的(dormant.host)優先
    #[tokio::test]
    async fn test_static_S3_dynamic_wins_on_conflict() {
        let (mc, c) = backend_container("web", None).await;
        // 動的ルート web.localhost と、それを呑み込む静的ワイルドカードを同時に登録
        let (addr, mock) = spawn_proxy_with_static(
            vec![mc],
            vec![c],
            vec![sre("*.localhost", "127.0.0.1", 1)],
        )
        .await;

        // web.localhost は動的コンテナに解決され起動して200
        let (status, _) = get(&addr, "web.localhost").await;
        assert_eq!(status, 200);
        assert!(mock.is_running("web"));

        // 動的ルートにないサブドメインは静的ワイルドカードへ(宛先1番ポートは閉じている → 502)
        let (status, _) = get(&addr, "other.localhost").await;
        assert_eq!(status, 502);
    }

    // S4: 静的完全一致は静的ワイルドカードより優先される
    #[tokio::test]
    async fn test_static_S4_exact_beats_wildcard() {
        let addr1 = testutil::spawn_backend().await;
        let (ip1, port1) = addr1.rsplit_once(':').unwrap();
        let port1: u16 = port1.parse().unwrap();
        // 完全一致側の宛先は閉じたポート(届けば502になる)
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_port = probe.local_addr().unwrap().port();
        drop(probe);

        let (addr, _mock) = spawn_proxy_with_static(
            vec![],
            vec![],
            vec![
                sre("api.example.com", ip1, dead_port),
                sre("*.example.com", ip1, port1),
            ],
        )
        .await;

        // 完全一致が勝つ → 宛先が閉じているので502
        let (status, _) = get(&addr, "api.example.com").await;
        assert_eq!(status, 502);
        // それ以外はワイルドカード → 200
        let (status, _) = get(&addr, "other.example.com").await;
        assert_eq!(status, 200);
    }

    // WS1: WebSocket ハンドシェイクが 101 を返し、sec-websocket-protocol が転送される
    #[tokio::test]
    async fn test_ws_WS1_handshake_and_protocol_forwarded() {
        // WS バックエンドを立てる
        let baddr = testutil::spawn_ws_backend().await;
        let (ip, port) = baddr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();
        let mut mc = MockContainer::new("ws", ip, port);
        mc.running = true;
        let mut c = testutil::make_container("ws", None);
        c.port = Some(port);
        c.startup_timeout = Duration::from_secs(3);
        c.ip = Some(ip.to_string());
        c.routes = vec![Route {
            host: "ws.localhost".to_string(),
            port: None,
        }];

        let (addr, _mock) = spawn_proxy(vec![mc], vec![c]).await;

        // tokio-tungstenite クライアントで WS 接続(sec-websocket-protocol 付き)
        let url = format!("ws://{}/ws", addr);
        let req = hyper::http::Request::builder()
            .uri(&url)
            .header("host", "ws.localhost")
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-protocol", "vite-hmr")
            .body(())
            .unwrap();
        let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        // バックエンドが受信した sec-websocket-protocol を応答に含める
        assert_eq!(
            resp.headers()
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok()),
            Some("vite-hmr")
        );
        ws.close(None).await.unwrap();
    }

    // WS2: WebSocket ハンドシェイクが 101 を返す(sec-websocket-protocol なしでも成立)
    #[tokio::test]
    async fn test_ws_WS2_handshake_without_protocol() {
        let baddr = testutil::spawn_ws_backend().await;
        let (ip, port) = baddr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();
        let mut mc = MockContainer::new("ws", ip, port);
        mc.running = true;
        let mut c = testutil::make_container("ws", None);
        c.port = Some(port);
        c.startup_timeout = Duration::from_secs(3);
        c.ip = Some(ip.to_string());
        c.routes = vec![Route {
            host: "ws.localhost".to_string(),
            port: None,
        }];

        let (addr, _mock) = spawn_proxy(vec![mc], vec![c]).await;

        let url = format!("ws://{}/ws", addr);
        let req = hyper::http::Request::builder()
            .uri(&url)
            .header("host", "ws.localhost")
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "13")
            .body(())
            .unwrap();
        let (_ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    }
}
