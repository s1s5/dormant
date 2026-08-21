//! ライフサイクル管理: 起動待ちフローとアイドル停止

use anyhow::{anyhow, Result};
use bytes::Bytes;
use http_body_util::Empty;
use hyper::Request;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::config::*;
use crate::docker::{DockerClient, ManagedContainer};
use crate::router::Router;

/// コンテナごとの最終アクセス時刻とセッション情報
struct SessionState {
    /// 最終アクセス時刻
    last_access: Instant,
    /// セッション保持時間
    duration: Duration,
}

/// セッション管理
#[derive(Clone, Default)]
pub struct Sessions {
    inner: Arc<RwLock<HashMap<String, SessionState>>>,
    active_conns: Arc<RwLock<HashMap<String, usize>>>,
}

impl Sessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// アクセスを記録(タイマーリセット)
    pub async fn touch(&self, id: &str, duration: Duration) {
        let mut map = self.inner.write().await;
        map.insert(
            id.to_string(),
            SessionState {
                last_access: Instant::now(),
                duration,
            },
        );
    }

    /// アクティブ接続を記録(SSE/WS等の長時間接続中は停止対象外)
    pub async fn connect(&self, id: &str) {
        *self.active_conns.write().await.entry(id.to_string()).or_insert(0) += 1;
    }

    /// アクティブ接続の終了を記録(0未満にはしない)
    pub async fn disconnect(&self, id: &str) {
        let mut map = self.active_conns.write().await;
        if let Some(n) = map.get_mut(id) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                map.remove(id);
            }
        }
    }

    /// アクティブ接続数
    #[cfg(test)]
    pub async fn active_count(&self, id: &str) -> usize {
        self.active_conns.read().await.get(id).copied().unwrap_or(0)
    }

    /// 期限切れのコンテナID一覧を返す(アクティブ接続中のコンテナは除外)
    pub async fn expired(&self) -> Vec<String> {
        let map = self.inner.read().await;
        let active = self.active_conns.read().await;
        map.iter()
            .filter(|(_, s)| s.last_access.elapsed() > s.duration)
            .filter(|(id, _)| active.get(*id).copied().unwrap_or(0) == 0)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub async fn remove(&self, id: &str) {
        self.inner.write().await.remove(id);
        self.active_conns.write().await.remove(id);
    }
}

/// アイドルチェックループ
pub async fn idle_loop(
    docker: &DockerClient,
    router: &Router,
    sessions: Sessions,
    interval_secs: u64,
) {
    loop {
        sleep(Duration::from_secs(interval_secs)).await;
        let expired = sessions.expired().await;
        for id in expired {
            tracing::info!("session expired, stopping container {}", id);
            let containers = router.containers().await;
            match containers.iter().find(|c| c.id == id) {
                Some(c) => {
                    // D3: 依存先(管理対象)を連鎖停止
                    for c in stop_chain(c, &containers, MAX_DEPENDENCY_DEPTH) {
                        if let Err(e) = docker.stop(&c.id).await {
                            tracing::warn!("failed to stop {}: {}", c.id, e);
                        }
                    }
                }
                None => {
                    if let Err(e) = docker.stop(&id).await {
                        tracing::warn!("failed to stop {}: {}", id, e);
                    }
                }
            }
            sessions.remove(&id).await;
        }
    }
}

/// 停止対象とその依存先(管理対象のみ)を連鎖的に収集
pub fn stop_chain(
    target: &ManagedContainer,
    containers: &[ManagedContainer],
    max_depth: usize,
) -> Vec<ManagedContainer> {
    let mut out = vec![target.clone()];
    let mut visited = HashSet::from([target.id.clone()]);
    let mut stack = vec![(target.clone(), 0)];
    while let Some((c, depth)) = stack.pop() {
        if depth >= max_depth {
            continue;
        }
        for dep in c.resolve_dependencies(containers) {
            if visited.insert(dep.id.clone()) {
                out.push(dep.clone());
                stack.push((dep, depth + 1));
            }
        }
    }
    out
}

/// コンテナを起動し、依存先を先に起動してから本体を起動する(デフォルトポート使用)
/// テストから利用する簡便ラッパー。本番は ensure_started_with_port を使う
#[cfg(test)]
pub async fn ensure_started(
    docker: &DockerClient,
    container: &ManagedContainer,
    containers: &[ManagedContainer],
) -> Result<String> {
    ensure_started_with_port(docker, container, containers, container.port).await
}

/// コンテナを起動し、依存先を先に起動してから本体を起動する(ポート指定可能)
/// port は転送先・疎通確認に使う。None の場合はコンテナのデフォルトポートを使う
pub async fn ensure_started_with_port(
    docker: &DockerClient,
    container: &ManagedContainer,
    containers: &[ManagedContainer],
    port: Option<u16>,
) -> Result<String> {
    let levels = dependency_levels_multi(std::slice::from_ref(container), containers, MAX_DEPENDENCY_DEPTH);
    for level in levels.iter().take(levels.len().saturating_sub(1)) {
        start_level(docker, level).await?;
    }
    ensure_started_single(docker, container, port).await
}

/// 依存関係をレベル別(深い順)に並べる。各レベルは並列起動可能
fn dependency_levels_multi(
    targets: &[ManagedContainer],
    containers: &[ManagedContainer],
    max_depth: usize,
) -> Vec<Vec<(ManagedContainer, Option<String>)>> {
    let mut depth_map: HashMap<String, usize> = HashMap::new();
    let mut cond_map: HashMap<String, Option<String>> = HashMap::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    for t in targets {
        depth_map.insert(t.id.clone(), 0);
        cond_map.insert(t.id.clone(), None);
        queue.push_back((t.id.clone(), 0));
    }
    while let Some((id, d)) = queue.pop_front() {
        if d >= max_depth {
            continue;
        }
        let Some(c) = containers.iter().find(|c| c.id == id) else {
            continue;
        };
        for dep in resolve_deps_logged(c, containers) {
            if depth_map.contains_key(&dep.id) {
                continue;
            }
            let cond = c
                .depends_on
                .iter()
                .find(|x| dep.compose_service.as_deref() == Some(x.service.as_str()))
                .map(|x| x.condition.clone());
            depth_map.insert(dep.id.clone(), d + 1);
            cond_map.insert(dep.id.clone(), cond);
            queue.push_back((dep.id, d + 1));
        }
    }
    let max = depth_map.values().copied().max().unwrap_or(0);
    let mut levels: Vec<Vec<(ManagedContainer, Option<String>)>> = vec![Vec::new(); max + 1];
    for (id, d) in &depth_map {
        if let Some(c) = containers.iter().find(|c| &c.id == id) {
            levels[*d].push((c.clone(), cond_map.get(id).cloned().flatten()));
        }
    }
    levels.reverse();
    levels
}

/// depends_on を解決し、見つからない依存は警告ログを出す(D1-3)
fn resolve_deps_logged(
    c: &ManagedContainer,
    containers: &[ManagedContainer],
) -> Vec<ManagedContainer> {
    let mut out = Vec::new();
    for d in &c.depends_on {
        match containers.iter().find(|x| {
            x.compose_project.as_deref() == c.compose_project.as_deref()
                && x.compose_service.as_deref() == Some(d.service.as_str())
        }) {
            Some(dep) => out.push(dep.clone()),
            None => tracing::warn!(
                "dependency {} of {} not found (not managed?)",
                d.service,
                c.name
            ),
        }
    }
    out
}

/// 1レベルのコンテナを並列起動する
async fn start_level(
    docker: &DockerClient,
    level: &[(ManagedContainer, Option<String>)],
) -> Result<()> {
    let mut handles = Vec::new();
    for (c, cond) in level {
        let docker = docker.clone();
        let c = c.clone();
        let cond = cond.clone();
        handles.push(tokio::spawn(async move {
            ensure_started_single(&docker, &c, c.port).await?;
            if cond.as_deref() == Some("service_healthy") {
                wait_healthy(&docker, &c).await?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }
    for h in handles {
        h.await.map_err(|e| anyhow!("join error: {e}"))??;
    }
    Ok(())
}

/// 依存先が Docker healthcheck で healthy になるまで待つ
async fn wait_healthy(docker: &DockerClient, c: &ManagedContainer) -> Result<()> {
    let deadline = Instant::now() + c.startup_timeout;
    while Instant::now() < deadline {
        if docker.is_ready(c).await? {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow!(
        "dependency {} not healthy within {:?}",
        c.name,
        c.startup_timeout
    ))
}

/// コンテナを起動し、ポートがlistenするまで待つ
/// port: 転送・疎通確認に使うポート。None ならコンテナのデフォルトポート
async fn ensure_started_single(
    docker: &DockerClient,
    container: &ManagedContainer,
    port: Option<u16>,
) -> Result<String> {
    // すでに起動済みならOK(IPは起動後に取り直す)
    if docker.is_running(&container.id).await? {
        if let Ok(ip) = docker.resolve_ip(&container.id).await {
            return Ok(addr_with_port(&ip, port));
        }
        return Ok(container.target_addr());
    }

    tracing::info!("starting container {}", container.name);
    docker.start(&container.id).await?;

    // running → ポート疎通OK までポーリング
    let deadline = Instant::now() + container.startup_timeout;
    let mut reported_running = false;
    while Instant::now() < deadline {
        // 1. runningになったか
        if !reported_running {
            if docker.is_running(&container.id).await? {
                tracing::info!("container {} is running", container.name);
                reported_running = true;
            } else {
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        }

        // 2. 起動後にIPを再解決し、ポート疎通を確認
        // 停止中コンテナはIPが空のため、起動後に取り直す
        if let Ok(ip) = docker.resolve_ip(&container.id).await {
            // ポート未指定(依存専用コンテナ)は running になった時点で ready
            let Some(port) = port else {
                tracing::info!("container {} is ready at {}", container.name, ip);
                return Ok(ip);
            };
            let addr = format!("{}:{}", ip, port);
            if port_is_open(&addr).await {
                let ready = match &container.healthcheck_status {
                    // status 指定あり → ヘルスチェックパスへGETし許容ステータスを確認
                    Some(allowed) => {
                        let hc_addr = match container.healthcheck_port {
                            Some(p) => format!("{}:{}", ip, p),
                            None => addr.clone(),
                        };
                        let path = container.healthcheck_path.as_deref().unwrap_or("/");
                        http_status_ok(&hc_addr, path, allowed).await
                    }
                    // 未指定 → 従来動作(ポート疎通のみ)
                    None => true,
                };
                if ready {
                    tracing::info!("container {} is ready at {}", container.name, addr);
                    return Ok(addr);
                }
                tracing::debug!("healthcheck failed for {} at {}", container.name, addr);
            } else {
                tracing::debug!("port check failed for {} at {}", container.name, addr);
            }
        }

        sleep(Duration::from_millis(500)).await;
    }

    Err(anyhow!(
        "container {} did not become ready within {:?}",
        container.name,
        container.startup_timeout
    ))
}

/// グループ内の全コンテナを起動(依存解決込み、タイムアウトはグループ内最大値)
pub async fn ensure_group_started(
    docker: &DockerClient,
    router: &Router,
    group: &str,
) -> Result<()> {
    let containers = router.containers().await;
    let members = router.group_containers(group).await;
    if members.is_empty() {
        return Err(anyhow!("group {} not found", group));
    }
    let timeout = members
        .iter()
        .map(|m| m.startup_timeout)
        .max()
        .unwrap_or(Duration::from_secs(180));

    let levels = dependency_levels_multi(&members, &containers, MAX_DEPENDENCY_DEPTH);
    match tokio::time::timeout(timeout, async {
        for level in &levels {
            start_level(docker, level).await?;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(anyhow!(
            "group {} start timed out after {:?}",
            group,
            timeout
        )),
    }
}

/// IP とポートから転送先アドレスを組み立てる(ポート未指定は IP のみ)
fn addr_with_port(ip: &str, port: Option<u16>) -> String {
    match port {
        Some(p) => format!("{}:{}", ip, p),
        None => ip.to_string(),
    }
}

/// TCPポート疎通チェック
async fn port_is_open(addr: &str) -> bool {
    match TcpStream::connect(addr).await {
        Ok(mut stream) => {
            // 接続できたら閉じる前に少し待ってからクローズ(サーバー側のaccept完了を保証)
            let _ = stream.write_all(b"").await;
            true
        }
        Err(_) => false,
    }
}

/// HTTPクライアント(ヘルスチェック用)
fn http_client() -> &'static Client<HttpConnector, Empty<Bytes>> {
    static CLIENT: OnceLock<Client<HttpConnector, Empty<Bytes>>> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .build_http()
    })
}
/// ヘルスチェックパスへGETし、応答ステータスが許容リストに含まれるか
async fn http_status_ok(addr: &str, path: &str, allowed: &[u16]) -> bool {
    let Ok(req) = Request::builder()
        .uri(format!("http://{}/{}", addr, path.trim_start_matches('/')))
        .body(Empty::new())
    else {
        return false;
    };
    match http_client().request(req).await {
        Ok(resp) => allowed.contains(&resp.status().as_u16()),
        Err(_) => false,
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use crate::docker::Dependency;
    use crate::testutil::MockContainer;
    use http_body_util::Full;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    /// 指定ステータスを返すだけのテストサーバーを立て、addrを返す
    async fn spawn_status_server(status: StatusCode) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let status = status;
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |_req: Request<hyper::body::Incoming>| {
                        let status = status;
                        async move {
                            let mut resp = Response::new(Full::new(Bytes::from_static(b"ok")));
                            *resp.status_mut() = status;
                            Ok::<_, std::io::Error>(resp)
                        }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });
        addr
    }

    // A1: アクティブ接続中のコンテナは expired の対象外
    #[tokio::test]
    async fn test_active_conn_A1_expired_excludes_active() {
        let sessions = Sessions::new();
        let id = "web";
        sessions.touch(id, Duration::from_millis(10)).await;
        sleep(Duration::from_millis(30)).await;
        sessions.connect(id).await;
        assert!(sessions.expired().await.is_empty());
    }

    // A2: 切断後は期限超過で expired に戻る
    #[tokio::test]
    async fn test_active_conn_A2_disconnect_restores_expired() {
        let sessions = Sessions::new();
        let id = "web";
        sessions.touch(id, Duration::from_millis(10)).await;
        sessions.connect(id).await;
        sessions.disconnect(id).await;
        sleep(Duration::from_millis(30)).await;
        assert_eq!(sessions.expired().await, vec![id.to_string()]);
    }

    // A3: connect/disconnect はカウントを維持し 0 未満にならない
    #[tokio::test]
    async fn test_active_conn_A3_counter_and_saturating() {
        let sessions = Sessions::new();
        let id = "web";
        sessions.connect(id).await;
        sessions.connect(id).await;
        assert_eq!(sessions.active_count(id).await, 2);
        sessions.disconnect(id).await;
        sessions.disconnect(id).await;
        sessions.disconnect(id).await;
        assert_eq!(sessions.active_count(id).await, 0);
    }

    // A4: アクティブ接続がない場合は従来どおり expired する
    #[tokio::test]
    async fn test_active_conn_A4_no_active_expires_normally() {
        let sessions = Sessions::new();
        let id = "web";
        sessions.touch(id, Duration::from_millis(10)).await;
        sleep(Duration::from_millis(30)).await;
        assert_eq!(sessions.expired().await, vec![id.to_string()]);
    }

    // A5: カウント>0 のまま期限超過 → 除外、カウント0に戻ると次の判定で対象
    #[tokio::test]
    async fn test_active_conn_A5_skip_until_zero() {
        let sessions = Sessions::new();
        let id = "web";
        sessions.touch(id, Duration::from_millis(10)).await;
        sleep(Duration::from_millis(30)).await;
        sessions.connect(id).await;
        assert!(sessions.expired().await.is_empty());
        sessions.disconnect(id).await;
        assert_eq!(sessions.expired().await, vec![id.to_string()]);
    }

    // remove でアクティブカウントも消える
    #[tokio::test]
    async fn test_active_conn_remove_clears_active() {
        let sessions = Sessions::new();
        let id = "web";
        sessions.connect(id).await;
        sessions.remove(id).await;
        assert_eq!(sessions.active_count(id).await, 0);
    }

    #[tokio::test]
    async fn test_http_status_ok_allowed() {
        let addr = spawn_status_server(StatusCode::OK).await;
        assert!(http_status_ok(&addr, "/health", &[200, 204]).await);
    }

    #[tokio::test]
    async fn test_http_status_ok_not_allowed() {
        let addr = spawn_status_server(StatusCode::SERVICE_UNAVAILABLE).await;
        assert!(!http_status_ok(&addr, "/", &[200]).await);
    }

    fn make_container(
        id: &str,
        service: &str,
        project: &str,
        depends_on: Vec<Dependency>,
    ) -> ManagedContainer {
        ManagedContainer {
            id: id.to_string(),
            name: format!("/{}-{}", project, service),
            port: Some(8000),
            tcp_expose: Vec::new(),
            group: None,
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            routes: Vec::new(),
            aliases: Vec::new(),
            ip: Some("172.20.0.99".to_string()),
            running: false,
            created: None,
            depends_on,
            compose_project: Some(project.to_string()),
            compose_service: Some(service.to_string()),
        }
    }

    fn dep(service: &str, condition: &str) -> Dependency {
        Dependency {
            service: service.to_string(),
            condition: condition.to_string(),
        }
    }

    #[test]
    fn test_dependency_levels_simple() {
        // app → db
        let db = make_container("db", "db", "proj", vec![]);
        let app = make_container("app", "app", "proj", vec![dep("db", "service_started")]);
        let containers = vec![db.clone(), app.clone()];
        let levels = dependency_levels_multi(std::slice::from_ref(&app), &containers, 10);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0][0].0.id, "db"); // 深い方が先
        assert_eq!(levels[1][0].0.id, "app");
        // healthcheck condition が引き継がれる
        let app2 = make_container("app", "app", "proj", vec![dep("db", "service_healthy")]);
        let containers2 = vec![db.clone(), app2.clone()];
        let levels = dependency_levels_multi(std::slice::from_ref(&app2), &containers2, 10);
        assert_eq!(levels[0][0].1.as_deref(), Some("service_healthy"));
    }

    #[test]
    fn test_dependency_levels_recursive() {
        // app → mid → base
        let base = make_container("base", "base", "proj", vec![]);
        let mid = make_container("mid", "mid", "proj", vec![dep("base", "service_started")]);
        let app = make_container("app", "app", "proj", vec![dep("mid", "service_started")]);
        let containers = vec![base.clone(), mid.clone(), app.clone()];
        let levels = dependency_levels_multi(std::slice::from_ref(&app), &containers, 10);
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0][0].0.id, "base");
        assert_eq!(levels[1][0].0.id, "mid");
        assert_eq!(levels[2][0].0.id, "app");
    }

    #[test]
    fn test_dependency_levels_missing_dep() {
        // D1-3: 依存先が見つからない場合も本体は含まれる
        let app = make_container("app", "app", "proj", vec![dep("nonexistent", "service_started")]);
        let containers = vec![app.clone()];
        let levels = dependency_levels_multi(std::slice::from_ref(&app), &containers, 10);
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0][0].0.id, "app");
    }

    #[test]
    fn test_dependency_levels_circular() {
        // 循環参照でも深さ制限で無限ループしない
        let a = make_container("a", "a", "proj", vec![dep("b", "service_started")]);
        let b = make_container("b", "b", "proj", vec![dep("a", "service_started")]);
        let containers = vec![a.clone(), b];
        let levels = dependency_levels_multi(std::slice::from_ref(&a), &containers, 3);
        // レベル数は深さ制限以内に収まる
        assert!(levels.len() <= 4);
    }

    #[test]
    fn test_stop_chain_recursive() {
        // A→B→C の依存でA停止時はB,Cも停止対象
        let c = make_container("c", "c", "proj", vec![]);
        let b = make_container("b", "b", "proj", vec![dep("c", "service_started")]);
        let a = make_container("a", "a", "proj", vec![dep("b", "service_started")]);
        let chain = stop_chain(&a, &[a.clone(), b.clone(), c.clone()], 10);
        assert_eq!(chain.len(), 3);
        let ids: Vec<_> = chain.iter().map(|x| x.id.as_str()).collect();
        assert!(ids.contains(&"a") && ids.contains(&"b") && ids.contains(&"c"));
    }

    #[test]
    fn test_stop_chain_circular_depth_limit() {
        // 循環参照でも深さ制限で停止
        let a = make_container("a", "a", "proj", vec![dep("b", "service_started")]);
        let b = make_container("b", "b", "proj", vec![dep("a", "service_started")]);
        let chain = stop_chain(&a, &[a.clone(), b.clone()], 10);
        // visited により a は重複しない
        assert_eq!(chain.len(), 2);
    }

    #[test]
    fn test_stop_chain_ignores_unmanaged() {
        // 管理対象外(compose_service不一致・別project)は依存解決されない
        let c = make_container("c", "c", "proj", vec![]);
        let other = make_container("other", "db", "other-proj", vec![]);
        let a = make_container("a", "a", "proj", vec![dep("db", "service_started")]);
        // 依存サービス名 db はあるが project が違うので解決されない
        let chain = stop_chain(&a, &[a.clone(), c, other], 10);
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn test_group_levels_parallel_members() {
        // グループメンバー2つが同じ依存を共有 → レベルは3段(依存, メンバー2つ)
        let db = make_container("db", "db", "proj", vec![]);
        let m1 = make_container("m1", "m1", "proj", vec![dep("db", "service_started")]);
        let m2 = make_container("m2", "m2", "proj", vec![dep("db", "service_started")]);
        let levels = dependency_levels_multi(&[m1.clone(), m2.clone()], &[db.clone(), m1, m2], 10);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].len(), 1); // db は1回だけ
        assert_eq!(levels[1].len(), 2); // m1, m2 は並列
    }

    /// モックDocker + 実バックエンドポートで起動可能なテスト環境を用意
    async fn mock(
        containers: Vec<MockContainer>,
    ) -> (DockerClient, crate::testutil::MockDocker) {
        crate::testutil::setup_mock_docker(containers).await
    }

    /// バックエンドサーバーを立て (ip, port) を返す
    async fn backend() -> (String, u16) {
        let addr = crate::testutil::spawn_backend().await;
        let (ip, port) = addr.rsplit_once(':').unwrap();
        (ip.to_string(), port.parse().unwrap())
    }

    /// 起動対象の ManagedContainer をモックと一致する形で生成
    fn managed(
        id: &str,
        group: Option<&str>,
        port: u16,
        startup_timeout: Duration,
    ) -> ManagedContainer {
        let mut c = crate::testutil::make_container(id, group);
        c.port = Some(port);
        c.startup_timeout = startup_timeout;
        c
    }

    /// compose コンテナ生成 + ポート/タイムアウト設定
    fn compose(
        id: &str,
        project: &str,
        service: &str,
        depends_on: Vec<Dependency>,
        port: u16,
    ) -> ManagedContainer {
        let mut c = crate::testutil::make_compose_container(id, project, service, depends_on);
        c.port = Some(port);
        c.startup_timeout = Duration::from_secs(3);
        c
    }

    // G1: グループなし → 単体起動
    #[tokio::test]
    async fn test_group_G1_single_container_started() {
        let (ip, port) = backend().await;
        let (docker, mock) = mock(vec![MockContainer::new("web", &ip, port)]).await;
        let c = managed("web", None, port, Duration::from_secs(3));
        let addr = ensure_started(&docker, &c, std::slice::from_ref(&c)).await.unwrap();
        assert!(addr.contains(&ip));
        assert!(mock.is_running("web"));
        assert_eq!(mock.start_order(), vec!["web"]);
    }

    // G2: グループ起動 → 全メンバー起動
    #[tokio::test]
    async fn test_group_G2_starts_all_members() {
        let (ip, p1) = backend().await;
        let (_, p2) = backend().await;
        let (docker, mock) = mock(vec![
            MockContainer::new("web1", &ip, p1),
            MockContainer::new("web2", &ip, p2),
        ])
        .await;
        let c1 = managed("web1", Some("grp"), p1, Duration::from_secs(3));
        let c2 = managed("web2", Some("grp"), p2, Duration::from_secs(3));
        let router = Router::new();
        router.update(vec![c1, c2]).await;
        ensure_group_started(&docker, &router, "grp").await.unwrap();
        assert!(mock.is_running("web1"));
        assert!(mock.is_running("web2"));
        let mut order = mock.start_order();
        order.sort();
        assert_eq!(order, vec!["web1", "web2"]);
    }

    // G2-1: 全員 ready → 成功
    #[tokio::test]
    async fn test_group_G2_1_all_ready_ok() {
        let (ip, p1) = backend().await;
        let (_, p2) = backend().await;
        let (docker, mock) = mock(vec![
            MockContainer::new("web1", &ip, p1),
            MockContainer::new("web2", &ip, p2),
        ])
        .await;
        let c1 = managed("web1", Some("grp"), p1, Duration::from_secs(3));
        let c2 = managed("web2", Some("grp"), p2, Duration::from_secs(3));
        let router = Router::new();
        router.update(vec![c1, c2]).await;
        // 全員 ready で Ok が返る
        assert!(ensure_group_started(&docker, &router, "grp").await.is_ok());
        assert_eq!(mock.running_count(), 2);
    }

    // G2-2: 一部失敗 → Err(proxyでは504)
    #[tokio::test]
    async fn test_group_G2_2_partial_failure_err() {
        let (ip, p1) = backend().await;
        let (_, p2) = backend().await;
        let mut fail = MockContainer::new("web2", &ip, p2);
        fail.start_fails = true;
        let (docker, mock) = mock(vec![MockContainer::new("web1", &ip, p1), fail]).await;
        let c1 = managed("web1", Some("grp"), p1, Duration::from_secs(3));
        let c2 = managed("web2", Some("grp"), p2, Duration::from_secs(3));
        let router = Router::new();
        router.update(vec![c1, c2]).await;
        assert!(ensure_group_started(&docker, &router, "grp").await.is_err());
        // 起動済みコンテナは停止しない
        assert!(mock.is_running("web1"));
    }

    // G2-3: 起動済みメンバーはスキップ
    #[tokio::test]
    async fn test_group_G2_3_skips_running_member() {
        let (ip, p1) = backend().await;
        let (_, p2) = backend().await;
        let mut running = MockContainer::new("web1", &ip, p1);
        running.running = true;
        let (docker, mock) = mock(vec![running, MockContainer::new("web2", &ip, p2)]).await;
        let c1 = managed("web1", Some("grp"), p1, Duration::from_secs(3));
        let c2 = managed("web2", Some("grp"), p2, Duration::from_secs(3));
        let router = Router::new();
        router.update(vec![c1, c2]).await;
        ensure_group_started(&docker, &router, "grp").await.unwrap();
        // 起動済み web1 は起動されず、web2 のみ起動
        assert_eq!(mock.start_order(), vec!["web2"]);
    }

    // D1: 起動対象が depends_on を持つ → 依存先を先に起動
    #[tokio::test]
    async fn test_depends_on_D1_dep_started_first() {
        let (ip, pd) = backend().await;
        let (_, pa) = backend().await;
        let (docker, mock) = mock(vec![
            MockContainer::new("db", &ip, pd),
            MockContainer::new("app", &ip, pa),
        ])
        .await;
        let db = compose("db", "proj", "db", vec![], pd);
        let app = compose("app", "proj", "app", vec![dep("db", "service_started")], pa);
        ensure_started(&docker, &app, &[db.clone(), app.clone()]).await.unwrap();
        // db → app の順で起動
        assert_eq!(mock.start_order(), vec!["db", "app"]);
        assert!(mock.is_running("db"));
        assert!(mock.is_running("app"));
    }

    // D1-1: 依存先が管理対象(dormant.enable=true) → ensure_started で起動・ready待ち
    #[tokio::test]
    async fn test_depends_on_D1_1_managed_dep_started() {
        let (ip, pd) = backend().await;
        let (_, pa) = backend().await;
        let (docker, mock) = mock(vec![
            MockContainer::new("db", &ip, pd),
            MockContainer::new("app", &ip, pa),
        ])
        .await;
        let db = compose("db", "proj", "db", vec![], pd);
        let app = compose("app", "proj", "app", vec![dep("db", "service_started")], pa);
        let addr = ensure_started(&docker, &app, &[db.clone(), app.clone()]).await.unwrap();
        // 依存 db が起動・ready になってから本体の addr が返る
        assert!(mock.is_running("db"));
        assert!(addr.contains(&ip));
    }

    // D1-1b: 依存先がポート未指定(依存専用コンテナ)でも管理対象として起動される
    #[tokio::test]
    async fn test_depends_on_D1_1b_dep_without_port_started() {
        let (ip, pa) = backend().await;
        let (docker, mock) = mock(vec![
            MockContainer::new("backend", &ip, 0),
            MockContainer::new("web", &ip, pa),
        ])
        .await;
        // backend はポート未指定(依存専用)の管理対象コンテナ
        let mut backend = crate::testutil::make_compose_container(
            "backend",
            "pb",
            "backend",
            vec![],
        );
        backend.port = None;
        backend.startup_timeout = Duration::from_secs(3);
        let web = compose("web", "pb", "web", vec![dep("backend", "service_started")], pa);
        let addr = ensure_started(&docker, &web, &[backend.clone(), web.clone()])
            .await
            .unwrap();
        // ポート未指定の依存 backend も起動される
        assert!(mock.is_running("backend"));
        assert!(mock.is_running("web"));
        assert!(addr.contains(&ip));
    }

    // D1-2: 依存先が管理対象外 → 起動しない(ログのみ)
    #[tokio::test]
    async fn test_depends_on_D1_2_unmanaged_dep_untouched() {
        let (ip, pa) = backend().await;
        let (docker, mock) = mock(vec![MockContainer::new("app", &ip, pa)]).await;
        let app = compose(
            "app",
            "proj",
            "app",
            vec![dep("external-db", "service_started")],
            pa,
        );
        // 管理対象外の依存は managed 一覧に存在しないので起動されない
        ensure_started(&docker, &app, std::slice::from_ref(&app)).await.unwrap();
        assert_eq!(mock.start_order(), vec!["app"]);
    }

    // D1-3: 依存先が見つからない → 警告のみで本体は起動継続(failしない)
    #[tokio::test]
    async fn test_depends_on_D1_3_missing_dep_continues() {
        let (ip, pa) = backend().await;
        let (docker, mock) = mock(vec![MockContainer::new("app", &ip, pa)]).await;
        let app = compose("app", "proj", "app", vec![dep("ghost", "service_started")], pa);
        let addr = ensure_started(&docker, &app, std::slice::from_ref(&app)).await.unwrap();
        assert!(addr.contains(&ip));
        assert!(mock.is_running("app"));
    }

    // D1-4: 依存先が複数 → 全て並列起動してから本体
    #[tokio::test]
    async fn test_depends_on_D1_4_multiple_deps_started_first() {
        let (ip, pd) = backend().await;
        let (_, pc) = backend().await;
        let (_, pa) = backend().await;
        let (docker, mock) = mock(vec![
            MockContainer::new("db", &ip, pd),
            MockContainer::new("cache", &ip, pc),
            MockContainer::new("app", &ip, pa),
        ])
        .await;
        let db = compose("db", "proj", "db", vec![], pd);
        let cache = compose("cache", "proj", "cache", vec![], pc);
        let app = compose(
            "app",
            "proj",
            "app",
            vec![dep("db", "service_started"), dep("cache", "service_started")],
            pa,
        );
        ensure_started(&docker, &app, &[db.clone(), cache.clone(), app.clone()]).await.unwrap();
        let order = mock.start_order();
        // db / cache が app より先
        let db_idx = order.iter().position(|x| x == "db").unwrap();
        let cache_idx = order.iter().position(|x| x == "cache").unwrap();
        let app_idx = order.iter().position(|x| x == "app").unwrap();
        assert!(db_idx < app_idx && cache_idx < app_idx);
        assert!(mock.is_running("db") && mock.is_running("cache") && mock.is_running("app"));
    }

    // D2: depends_on ラベル無し → 従来どおり単体起動
    #[tokio::test]
    async fn test_depends_on_D2_no_label_single_start() {
        let (ip, p) = backend().await;
        let (docker, mock) = mock(vec![MockContainer::new("solo", &ip, p)]).await;
        let c = managed("solo", None, p, Duration::from_secs(3));
        ensure_started(&docker, &c, std::slice::from_ref(&c)).await.unwrap();
        assert_eq!(mock.start_order(), vec!["solo"]);
    }

    // D3: 停止時に依存先(管理対象)を連鎖停止
    #[tokio::test]
    async fn test_depends_on_D3_cascade_stop() {
        let (ip, pc) = backend().await;
        let (_, pb) = backend().await;
        let (_, pa) = backend().await;
        let mut mc = MockContainer::new("c", &ip, pc);
        mc.running = true;
        let mut mb = MockContainer::new("b", &ip, pb);
        mb.running = true;
        let mut ma = MockContainer::new("a", &ip, pa);
        ma.running = true;
        let (docker, mock) = mock(vec![mc, mb, ma]).await;
        let c = compose("c", "proj", "c", vec![], pc);
        let b = compose("b", "proj", "b", vec![dep("c", "service_started")], pb);
        let a = compose("a", "proj", "a", vec![dep("b", "service_started")], pa);
        let containers = vec![a.clone(), b.clone(), c.clone()];
        // A を停止 → 依存先 B, C も停止される
        for target in stop_chain(&a, &containers, MAX_DEPENDENCY_DEPTH) {
            docker.stop(&target.id).await.unwrap();
        }
        assert!(!mock.is_running("a"));
        assert!(!mock.is_running("b"));
        assert!(!mock.is_running("c"));
    }

    // D4: 再帰的依存 (A→B→C) を逐段解決して起動
    #[tokio::test]
    async fn test_depends_on_D4_recursive_resolution() {
        let (ip, pc) = backend().await;
        let (_, pb) = backend().await;
        let (_, pa) = backend().await;
        let (docker, mock) = mock(vec![
            MockContainer::new("c", &ip, pc),
            MockContainer::new("b", &ip, pb),
            MockContainer::new("a", &ip, pa),
        ])
        .await;
        let c = compose("c", "proj", "c", vec![], pc);
        let b = compose("b", "proj", "b", vec![dep("c", "service_started")], pb);
        let a = compose("a", "proj", "a", vec![dep("b", "service_started")], pa);
        ensure_started(&docker, &a, &[a.clone(), b.clone(), c.clone()]).await.unwrap();
        // c → b → a の順
        assert_eq!(mock.start_order(), vec!["c", "b", "a"]);
    }

    // D4: 循環依存でも深さ制限で無限ループしない(実起動)
    #[tokio::test]
    async fn test_depends_on_D4_circular_depth_limited() {
        let (ip, p1) = backend().await;
        let (_, p2) = backend().await;
        let (docker, _) = mock(vec![
            MockContainer::new("a", &ip, p1),
            MockContainer::new("b", &ip, p2),
        ])
        .await;
        let a = compose("a", "proj", "a", vec![dep("b", "service_started")], p1);
        let b = compose("b", "proj", "b", vec![dep("a", "service_started")], p2);
        // 深さ制限内で完了する(無限ループ・タイムアウトしない)
        let _ = tokio::time::timeout(
            Duration::from_secs(10),
            ensure_started(&docker, &a, &[a.clone(), b.clone()]),
        )
        .await
        .unwrap();
    }
}

