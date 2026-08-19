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

    let container = match router.resolve(&host).await {
        Some(c) => c,
        None => {
            tracing::warn!("no route for host: {}", host);
            return Ok(error_response("no route for host", StatusCode::NOT_FOUND));
        }
    };

    // セッション記録(アクセスでタイマーリセット)
    // 起動の成否に関わらずアクセス時点で記録し、次のアクセスまでセッションを維持する
    sessions
        .touch(&container.id, container.session_duration)
        .await;

    // WebSocket判定
    let is_ws = req
        .headers()
        .get("upgrade")
        .map(|v| v.to_str().unwrap_or("").to_ascii_lowercase() == "websocket")
        .unwrap_or(false);

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
            container.clone(),
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
        container,
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
    container: crate::docker::ManagedContainer,
    containers: &[crate::docker::ManagedContainer],
    sessions: Sessions,
) -> Result<BoxResp, Infallible> {
    // 起動待ち(成功時は転送先アドレスが返る)
    let target_addr = match lifecycle::ensure_started(&docker, &container, containers).await {
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
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
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

/// WebSocketブリッジ
async fn handle_ws(
    mut req: Request<Incoming>,
    ws_client: WsClient,
    docker: DockerClient,
    container: crate::docker::ManagedContainer,
    containers: &[crate::docker::ManagedContainer],
    sessions: Sessions,
) -> Result<BoxResp, Infallible> {
    // 起動待ち(成功時は転送先アドレスが返る)
    let target_addr = match lifecycle::ensure_started(&docker, &container, containers).await {
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

    let client_upgraded_fut = hyper::upgrade::on(&mut req);

    // バックエンドへアップグレードリクエスト(コンテナIP直アクセス)
    let target = format!("http://{}/{}", target_addr, trim_leading_slash(&path));
    let ws_req = Request::builder()
        .method("GET")
        .uri(&target)
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-key", ws_key)
        .header("sec-websocket-version", ws_version)
        .body(Empty::new())
        .unwrap();

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
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
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

                return Ok(resp101);
            } else {
                let status = resp.status();
                let body = resp
                    .into_body()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                    .boxed();
                let mut out = Response::new(body);
                *out.status_mut() = status;
                return Ok(out);
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
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .boxed(),
    )
}

fn error_response(body: &'static str, status: StatusCode) -> BoxResp {
    let mut resp = Response::new(
        Full::new(Bytes::from(body))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .boxed(),
    );
    *resp.status_mut() = status;
    resp
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
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
        c.hosts = vec![format!("{}.localhost", id)];
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
        c.hosts = vec!["sse.localhost".to_string()];
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
        c.hosts = vec!["grpc.localhost".to_string()];

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
        c.hosts = vec!["grpc.localhost".to_string()];

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
}
