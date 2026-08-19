//! テスト用ユーティリティ: Docker Engine API を模した Unix ソケットサーバー

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::{TcpListener, UnixListener};

use crate::docker::{Dependency, DockerClient, ManagedContainer};

/// モック内の1コンテナの状態
#[derive(Debug, Clone)]
pub struct MockContainer {
    /// コンテナID
    pub id: String,
    /// 起動中か
    pub running: bool,
    /// 起動後に使うIP
    pub ip: String,
    /// 公開ポート
    pub port: u16,
    /// 起動を失敗させる(start が 500 を返す)
    pub start_fails: bool,
    /// Docker healthcheck の状態(None = healthcheckなし)
    pub health: Option<String>,
}

impl MockContainer {
    pub fn new(id: &str, ip: &str, port: u16) -> Self {
        Self {
            id: id.to_string(),
            running: false,
            ip: ip.to_string(),
            port,
            start_fails: false,
            health: None,
        }
    }
}

/// モック Docker Engine サーバーの状態
#[derive(Clone)]
pub struct MockDocker {
    state: Arc<Mutex<MockState>>,
}

struct MockState {
    containers: Vec<MockContainer>,
    start_order: Vec<String>,
}

impl MockDocker {
    pub fn new(containers: Vec<MockContainer>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MockState {
                containers,
                start_order: Vec::new(),
            })),
        }
    }

    /// Unix ソケットでサーバーを起動し、ソケットパスを返す
    pub async fn serve(self) -> PathBuf {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "dormant-test-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let state = self.state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let state = state.clone();
                        async move { handle(state, req).await }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        path
    }

    pub fn snapshot(&self) -> Vec<MockContainer> {
        self.state.lock().unwrap().containers.clone()
    }

    pub fn container(&self, id: &str) -> Option<MockContainer> {
        self.snapshot().into_iter().find(|c| c.id == id)
    }

    pub fn is_running(&self, id: &str) -> bool {
        self.container(id).map(|c| c.running).unwrap_or(false)
    }

    /// 起動済みコンテナ数
    pub fn running_count(&self) -> usize {
        self.snapshot().iter().filter(|c| c.running).count()
    }

    /// 起動が記録された順序(ID列)
    pub fn start_order(&self) -> Vec<String> {
        self.state.lock().unwrap().start_order.clone()
    }
}

/// テスト用バックエンド: 接続を受け付けるだけの TCP サーバー
pub async fn spawn_backend() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |_req: Request<Incoming>| async {
                    let mut resp = Response::new(Full::new(Bytes::from_static(b"ok")));
                    *resp.status_mut() = StatusCode::OK;
                    Ok::<_, std::io::Error>(resp)
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });
    addr
}

/// SSE バックエンド: ヘッダー送出後にストリームを保持し続けるサーバー
/// 接続が確立したら body を閉じるまで接続を維持する(アクティブ接続保護のテスト用)
pub async fn spawn_sse_backend() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |_req: Request<Incoming>| async {
                    // 最初のフレームを送った後、永遠に pending するストリーム(接続維持)
                    let first = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(
                        hyper::body::Frame::data(Bytes::from_static(b"data: hi\n\n")),
                    )]);
                    let rest = futures_util::stream::pending::<Result<_, std::io::Error>>();
                    let body = http_body_util::StreamBody::new(first.chain(rest));
                    let mut resp = Response::new(body);
                    resp.headers_mut().insert(
                        "content-type",
                        hyper::header::HeaderValue::from_static("text/event-stream"),
                    );
                    Ok::<_, std::io::Error>(resp)
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });
    addr
}

/// HTTP/2 (h2c) バックエンド: 常に200を返す h2 サーバー
pub async fn spawn_h2_backend() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |_req: Request<Incoming>| async {
                    let mut resp = Response::new(Full::new(Bytes::from_static(b"h2-ok")));
                    *resp.status_mut() = StatusCode::OK;
                    Ok::<_, std::io::Error>(resp)
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    addr
}

/// モック Docker サーバーを立て、DockerClient と状態確認用 MockDocker を返す
pub async fn setup_mock_docker(
    containers: Vec<MockContainer>,
) -> (DockerClient, MockDocker) {
    let mock = MockDocker::new(containers);
    let path = mock.clone().serve().await;
    let docker = DockerClient::new(path.to_str().unwrap()).unwrap();
    (docker, mock)
}
async fn handle(
    state: Arc<Mutex<MockState>>,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<String>, std::io::Error> {
    let path = req.uri().path().to_string();
    let method = req.method().to_string();

    // GET /containers/json → 一覧
    if method == "GET" && path == "/containers/json" {
        let containers = state.lock().unwrap().containers.clone();
        let arr = containers
            .iter()
            .map(|c| {
                format!(
                    "{{\"Id\":\"{}\",\"Names\":[\"/{}-1\"],\"Ports\":[{{\"PrivatePort\":{}}}],\"Labels\":{{\"dormant.enable\":\"true\",\"dormant.port\":\"{}\"}},\"State\":\"{}\"}}",
                    c.id, c.id, c.port, c.port, if c.running { "running" } else { "exited" }
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        return json_response(&format!("[{}]", arr));
    }

    // POST /containers/{id}/start
    if method == "POST" && path.starts_with("/containers/") && path.ends_with("/start") {
        let id = path
            .strip_prefix("/containers/")
            .unwrap()
            .strip_suffix("/start")
            .unwrap()
            .to_string();
        let mut st = state.lock().unwrap();
        if let Some(c) = st.containers.iter_mut().find(|c| c.id == id) {
            if c.start_fails {
                return empty_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
            c.running = true;
            let cid = c.id.clone();
            st.start_order.push(cid);
        }
        return empty_response(StatusCode::NO_CONTENT);
    }

    // POST /containers/{id}/stop
    if method == "POST" && path.starts_with("/containers/") && path.ends_with("/stop") {
        let id = path
            .strip_prefix("/containers/")
            .unwrap()
            .strip_suffix("/stop")
            .unwrap()
            .to_string();
        {
            let mut st = state.lock().unwrap();
            if let Some(c) = st.containers.iter_mut().find(|c| c.id == id) {
                c.running = false;
            }
        }
        return empty_response(StatusCode::NO_CONTENT);
    }

    // GET /containers/{id}/json (inspect)
    if method == "GET" && path.starts_with("/containers/") && path.ends_with("/json") {
        let id = path
            .strip_prefix("/containers/")
            .unwrap()
            .strip_suffix("/json")
            .unwrap()
            .to_string();
        let st = state.lock().unwrap();
        let Some(c) = st.containers.iter().find(|c| c.id == id) else {
            return empty_response(StatusCode::NOT_FOUND);
        };
        let running = c.running;
        let ip = &c.ip;
        let state_json = match &c.health {
            Some(h) => format!(
                "{{\"Running\":{},\"Health\":{{\"Status\":\"{}\"}}}}",
                running, h
            ),
            None => format!("{{\"Running\":{}}}", running),
        };
        return json_response(&format!(
            "{{\"State\":{},\"NetworkSettings\":{{\"Networks\":{{\"dormant\":{{\"IPAddress\":\"{}\"}}}}}}}}",
            state_json, ip
        ));
    }

    empty_response(StatusCode::NOT_FOUND)
}

fn json_response(body: &str) -> Result<Response<String>, std::io::Error> {
    let mut resp = Response::new(body.to_string());
    resp.headers_mut().insert(
        "content-type",
        hyper::header::HeaderValue::from_static("application/json"),
    );
    Ok(resp)
}

fn empty_response(status: StatusCode) -> Result<Response<String>, std::io::Error> {
    let mut resp = Response::new(String::new());
    *resp.status_mut() = status;
    Ok(resp)
}

/// テスト用の ManagedContainer を生成
pub fn make_container(id: &str, group: Option<&str>) -> ManagedContainer {
    ManagedContainer {
        id: id.to_string(),
        name: format!("/{}-1", id),
        port: 8000,
        group: group.map(|s| s.to_string()),
        session_duration: Duration::from_secs(3600),
        startup_timeout: Duration::from_secs(5),
        healthcheck_path: None,
        healthcheck_port: None,
        healthcheck_status: None,
        hosts: Vec::new(),
        ip: None,
        running: false,
        created: None,
        depends_on: Vec::new(),
        compose_project: None,
        compose_service: None,
    }
}

/// compose ラベル付きのテスト用 ManagedContainer を生成
pub fn make_compose_container(
    id: &str,
    project: &str,
    service: &str,
    depends_on: Vec<Dependency>,
) -> ManagedContainer {
    let mut c = make_container(id, None);
    c.compose_project = Some(project.to_string());
    c.compose_service = Some(service.to_string());
    c.depends_on = depends_on;
    c
}
