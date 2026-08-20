//! TCP リバースプロキシ: 待ち受けポート → コンテナへの透過転送
//!
//! `dormant.tcp` ラベルで公開したコンテナの TCP ポートを dormant に待ち受けさせ、
//! そのポートへの接続を当該コンテナへ転送する。HTTP と同様に scale-to-zero
//! (起動待ち・アイドル停止・アクティブ接続保護) を実現する。

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::docker::DockerClient;
use crate::lifecycle;
use crate::lifecycle::Sessions;
use crate::router::Router;

/// 現在 bind 済みの TCP リスナー (ポート → accept タスクのハンドル)
struct TcpListeners {
    handles: HashMap<u16, JoinHandle<()>>,
}

/// TCP 転送サーバーを起動 (config.listen のホスト部分に bind)
pub async fn serve_tcp(
    config: &Config,
    router: Arc<Router>,
    docker: DockerClient,
    sessions: Sessions,
) -> Result<()> {
    let host = config
        .listen
        .rsplit_once(':')
        .map(|(h, _)| h.to_string())
        .unwrap_or_else(|| "0.0.0.0".to_string());
    serve_tcp_on(host, router, docker, sessions).await
}

/// 指定ホストで TCP 転送サーバーを起動 (テストからも利用)
pub async fn serve_tcp_on(
    host: String,
    router: Arc<Router>,
    docker: DockerClient,
    sessions: Sessions,
) -> Result<()> {
    let mut listeners = TcpListeners {
        handles: HashMap::new(),
    };

    loop {
        // ラベル変化に追従するためポーリングでポート差分を調整
        let desired = router.tcp_listen_ports().await;

        // 新規ポートを bind
        for &port in &desired {
            if listeners.handles.contains_key(&port) {
                continue;
            }
            let addr = format!("{}:{}", host, port);
            match TcpListener::bind(&addr).await {
                Ok(listener) => {
                    tracing::info!("tcp listening on {}", addr);
                    let docker = docker.clone();
                    let router = router.clone();
                    let sessions = sessions.clone();
                    let handle = tokio::spawn(async move {
                        accept_loop(listener, docker, router, sessions).await;
                    });
                    listeners.handles.insert(port, handle);
                }
                Err(e) => {
                    tracing::warn!("failed to bind tcp {}: {}", addr, e);
                }
            }
        }

        // 削除されたポートのリスナーを停止
        let stale: Vec<u16> = listeners
            .handles
            .keys()
            .copied()
            .filter(|p| !desired.contains(p))
            .collect();
        for port in stale {
            if let Some(handle) = listeners.handles.remove(&port) {
                handle.abort();
                tracing::info!("tcp listener {} removed", port);
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// 1 リスナーに対する accept ループ
async fn accept_loop(
    listener: TcpListener,
    docker: DockerClient,
    router: Arc<Router>,
    sessions: Sessions,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let docker = docker.clone();
        let router = router.clone();
        let sessions = sessions.clone();
        tokio::spawn(async move {
            handle_conn(stream, docker, router, sessions).await;
        });
    }
}

/// 1 接続を処理する
async fn handle_conn(
    mut client: TcpStream,
    docker: DockerClient,
    router: Arc<Router>,
    sessions: Sessions,
) {
    // 接続が確立したローカルポートから転送先コンテナを解決
    let port = match client.local_addr() {
        Ok(addr) => addr.port(),
        Err(_) => {
            tracing::warn!("tcp: cannot resolve local addr");
            return;
        }
    };

    let Some(container) = router.resolve_tcp(port).await else {
        tracing::warn!("tcp: no route for port {}", port);
        return;
    };

    // この待ち受けポートに対応するコンテナ側ポートを特定
    let Some(container_port) = container
        .tcp_expose
        .iter()
        .find(|e| e.listen_port == port)
        .map(|e| e.container_port)
    else {
        tracing::warn!("tcp: container {} has no tcp expose for port {}", container.id, port);
        return;
    };

    // セッション記録 (アクセスでタイマーリセット)
    sessions
        .touch(&container.id, container.session_duration)
        .await;

    // 起動待ち (成功時は転送先ホストが含まれるアドレスが返る)
    let containers = router.containers().await;
    let started = match lifecycle::ensure_started_with_port(
        &docker,
        &container,
        &containers,
        Some(container_port),
    )
    .await
    {
        Ok(addr) => addr,
        Err(e) => {
            tracing::warn!(
                "tcp: startup failed for {} ({}): {}",
                container.name,
                container.id,
                e
            );
            return;
        }
    };

    // 転送先アドレス (ensure_started が返すホスト + tcp コンテナ側ポート)
    let host = started
        .rsplit_once(':')
        .map(|(h, _)| h.to_string())
        .unwrap_or_else(|| started.clone());
    let target = format!("{}:{}", host, container_port);
    tracing::debug!("tcp: forward {} -> {}", container.name, target);

    // アクティブ接続保護 (接続中は停止対象外)
    sessions.connect(&container.id).await;

    match TcpStream::connect(&target).await {
        Ok(mut backend) => {
            // 双方向にコピー。どちらかが閉じたら終了
            let _ = copy_bidirectional(&mut client, &mut backend).await;
        }
        Err(e) => {
            tracing::warn!("tcp: connect to {} failed: {}", target, e);
        }
    }

    sessions.disconnect(&container.id).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// dormant の TCP 転送サーバーを起動する。
    /// バックエンドは TCP エコーサーバーを立て、その addr をコンテナの ip/container_port として設定する。
    async fn spawn_tcp_proxy(mc: &testutil::MockContainer, c: crate::docker::ManagedContainer) {
        let (docker, _mock) = testutil::setup_mock_docker(vec![mc.clone()]).await;
        let router = Arc::new(Router::new());
        router.update(vec![c]).await;
        let sessions = Sessions::new();
        tokio::spawn(async move {
            let _ = serve_tcp_on("127.0.0.1".to_string(), router, docker, sessions).await;
        });
    }

    /// TCP エコーバックエンドを立て、それ用の (MockContainer, ManagedContainer) を返す
    async fn tcp_setup(
        id: &str,
        running: bool,
    ) -> (testutil::MockContainer, crate::docker::ManagedContainer) {
        // 未使用のポートを確保してから閉じる (dormant がそのポートを bind する)
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_port = probe.local_addr().unwrap().port();
        drop(probe);

        let addr = testutil::spawn_tcp_echo().await;
        let (ip, port) = addr.rsplit_once(':').unwrap();
        let port: u16 = port.parse().unwrap();
        let mut mc = testutil::MockContainer::new(id, ip, port);
        mc.port = port;
        mc.running = running;
        let mut c = testutil::make_container(id, None);
        c.tcp_expose = vec![crate::docker::TcpExpose {
            listen_port,
            container_port: port,
        }];
        // ensure_started は port で疎通確認するため、tcp の転送先ポートと合わせる
        c.port = Some(port);
        c.startup_timeout = Duration::from_secs(3);
        c.ip = Some(ip.to_string());
        c.running = running;
        (mc, c)
    }

    /// エコーバックエンドに接続してデータが往復することを確認
    async fn echo_roundtrip(addr: &str) -> bool {
        let Ok(mut stream) = TcpStream::connect(addr).await else {
            return false;
        };
        if stream.write_all(b"hello").await.is_err() {
            return false;
        }
        let mut buf = [0u8; 5];
        match tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf)).await {
            Ok(Ok(_)) => &buf == b"hello",
            _ => false,
        }
    }

    /// リスナーが立ち上がるまでリトライ (最大 ~3s)
    async fn wait_echo(listen_port: u16) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let addr = format!("127.0.0.1:{}", listen_port);
        while std::time::Instant::now() < deadline {
            if echo_roundtrip(&addr).await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        false
    }

    #[tokio::test]
    async fn test_tcp_forward() {
        let (mc, c) = tcp_setup("tcp-app", true).await;
        let listen_port = c.tcp_expose[0].listen_port;
        spawn_tcp_proxy(&mc, c).await;
        assert!(wait_echo(listen_port).await);
    }

    #[tokio::test]
    async fn test_tcp_no_route() {
        let (mc, c) = tcp_setup("tcp-app", true).await;
        spawn_tcp_proxy(&mc, c).await;
        // 登録外の別ポートは何も listen していない → 接続失敗
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unused_port = probe.local_addr().unwrap().port();
        drop(probe);
        assert!(TcpStream::connect(format!("127.0.0.1:{}", unused_port)).await.is_err());
    }

    #[tokio::test]
    async fn test_tcp_auto_start() {
        // 停止状態のコンテナに接続 → dormant が自動起動してから転送
        let (mc, c) = tcp_setup("tcp-on-demand", false).await;
        let listen_port = c.tcp_expose[0].listen_port;
        spawn_tcp_proxy(&mc, c).await;
        assert!(wait_echo(listen_port).await);
    }
}


