//! ルーティング: Hostヘッダー → コンテナのマッピング管理

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::StaticRouteEntry;
use crate::docker::ManagedContainer;

/// ルートキー(Host名)に対応する候補エントリ
#[derive(Debug, Clone)]
pub struct RouteEntry {
    /// 候補コンテナ
    pub container: ManagedContainer,
    /// そのルートで使う転送ポート(None = コンテナのデフォルトポート)
    pub port: Option<u16>,
}

impl RouteEntry {
    /// running状態か
    pub fn is_running(&self) -> bool {
        self.container.is_running()
    }
}

/// 静的ルートの転送先(dormant が管理しない外部固定宛先)
#[derive(Debug, Clone)]
pub struct StaticTarget {
    pub ip: String,
    pub port: u16,
}

/// 解決結果
#[derive(Debug, Clone)]
pub enum RouteResult {
    /// 動的(dormant 管理対象コンテナ + 転送ポート)
    /// コンテナは大きいため Box で包む(enum のサイズ差を小さく保つ)
    Dynamic(Box<ManagedContainer>, u16),
    /// 静的(dormant が管理しない外部固定宛先。起動待ち不要で直接転送)
    Static(StaticTarget),
}

impl RouteResult {
    /// 動的ルートのコンテナ(静的なら None)
    #[cfg(test)]
    pub fn container(&self) -> Option<&ManagedContainer> {
        match self {
            RouteResult::Dynamic(c, _) => Some(c),
            RouteResult::Static(_) => None,
        }
    }

    /// 静的ルートの転送先(動的なら None)
    pub fn static_target(&self) -> Option<&StaticTarget> {
        match self {
            RouteResult::Static(t) => Some(t),
            RouteResult::Dynamic(_, _) => None,
        }
    }
}

/// ルーティングテーブル
/// key: Host名(例: "graphql.sb.carrot.localhost")
/// value: そのホストの管理対象コンテナ候補リスト(転送ポート情報付き)
#[derive(Clone, Default)]
pub struct Router {
    inner: Arc<RwLock<HashMap<String, Vec<RouteEntry>>>>,
    /// TCP転送用のルーティング表
    /// key: dormant 側の待ち受けポート
    /// value: そのポートの管理対象コンテナ候補リスト
    tcp_listen: Arc<RwLock<HashMap<u16, Vec<ManagedContainer>>>>,
    /// 静的ルート表(dormant が管理しない外部固定宛先)
    static_routes: Arc<RwLock<StaticRoutes>>,
}

/// 静的ルート表
/// 優先順位: 完全一致 → ワイルドカード(複数マッチ時は最も長いサフィックス優先)
#[derive(Debug, Clone, Default)]
pub struct StaticRoutes {
    /// 完全一致ホスト(ワイルドカードなし)
    pub exact: HashMap<String, StaticTarget>,
    /// ワイルドカード(`*.example.com` → サフィックス `.example.com` で任意深度マッチ)
    pub wildcard: Vec<(String, StaticTarget)>,
}

impl StaticRoutes {
    /// 環境変数由来の静的ルート一覧から表を構築する
    pub fn from_entries(entries: &[StaticRouteEntry]) -> Self {
        let mut routes = StaticRoutes {
            exact: HashMap::new(),
            wildcard: Vec::new(),
        };
        for e in entries {
            let target = StaticTarget {
                ip: e.ip.clone(),
                port: e.port,
            };
            if let Some(suffix) = e.pattern.strip_prefix("*.") {
                if suffix.is_empty() || suffix.contains('*') {
                    tracing::warn!(
                        "invalid static route pattern '{}', skipping",
                        e.pattern
                    );
                    continue;
                }
                routes.wildcard.push((suffix.to_string(), target));
            } else if e.pattern.contains('*') {
                tracing::warn!(
                    "invalid static route pattern '{}', skipping",
                    e.pattern
                );
            } else {
                routes.exact.insert(e.pattern.clone(), target);
            }
        }
        // 決定性のためサフィックスが長い順(最も長いサフィックスが先にマッチ)
        routes
            .wildcard
            .sort_by_key(|(suffix, _)| std::cmp::Reverse(suffix.len()));
        routes
    }

    /// 静的ルートを解決する(完全一致 → ワイルドカードの順)
    fn resolve(&self, host: &str) -> Option<StaticTarget> {
        if let Some(t) = self.exact.get(host) {
            return Some(t.clone());
        }
        // ワイルドカード: `*.example.com` → サフィックス `.example.com`
        // `example.com` 自体はマッチしない。`foo.example.com` も `foo.bar.example.com` もマッチ
        for (suffix, t) in &self.wildcard {
            if host.ends_with(suffix.as_str()) && host.len() > suffix.len() {
                return Some(t.clone());
            }
        }
        None
    }
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// 静的ルート表を置き換える。
    /// 動的(dormant.host ラベル由来)ホストとの衝突があれば warning を出す
    /// (解決時は動的優先のため、静的ルートは動的ホストに負ける)
    pub async fn set_static_routes(&self, entries: &[StaticRouteEntry]) {
        let routes = StaticRoutes::from_entries(entries);
        let map = self.inner.read().await;
        // 衝突検出: 静的完全一致ホストと動的ホストの重複
        for host in routes.exact.keys() {
            if map.contains_key(host) {
                tracing::warn!(
                    "static route '{}' conflicts with dynamic route, dynamic wins",
                    host
                );
            }
        }
        // ワイルドカードが動的ホストを呑み込む場合も warning(動的優先)
        for (suffix, _) in &routes.wildcard {
            for k in map.keys() {
                if k.ends_with(suffix.as_str()) && k.len() > suffix.len() {
                    tracing::warn!(
                        "static wildcard '*{}' overlaps dynamic host '{}'; dynamic wins",
                        suffix,
                        k
                    );
                }
            }
        }
        *self.static_routes.write().await = routes;
    }

    /// 静的ルート表へのアクセス(テスト用)
    #[cfg(test)]
    pub async fn static_routes(&self) -> StaticRoutes {
        self.static_routes.read().await.clone()
    }

    /// ルーターを再構築
    pub async fn update(&self, containers: Vec<ManagedContainer>) {
        let mut map = HashMap::new();
        let mut tcp_map = HashMap::new();
        for c in containers {
            // コンテナ名からHost名を導出
            // 例: /federation-router-federation-router.sizebook-1 → federation-router-federation-router.sizebook
            // 先頭の / を除去し、末尾の -数字列 (composeレプリカ番号) のみ剥がす
            let trimmed = c.name.trim_start_matches('/');
            let name = match trimmed.rfind('-') {
                Some(idx)
                    if idx + 1 < trimmed.len()
                        && trimmed[idx + 1..].bytes().all(|b| b.is_ascii_digit()) =>
                {
                    trimmed[..idx].to_string()
                }
                _ => trimmed.to_string(),
            };

            // コンテナ名由来の導出キーは従来どおり残す(後方互換、デフォルトポート)
            Self::add(&mut map, name, &c, None);

            // ラベルで明示指定があればそれも使う
            // dormant.host ラベル: `host[:port]` のカンマ区切りで複数ルートを登録
            for route in &c.routes {
                Self::add(&mut map, route.host.clone(), &c, route.port);
            }

            // TCP転送: dormant.tcp で公開するポートを登録
            for expose in &c.tcp_expose {
                Self::add_tcp(&mut tcp_map, expose.listen_port, &c);
            }
        }
        *self.inner.write().await = map;
        *self.tcp_listen.write().await = tcp_map;
    }

    /// 候補リストに追加(同一コンテナIDは重複追加しない)
    fn add(
        map: &mut HashMap<String, Vec<RouteEntry>>,
        host: String,
        c: &ManagedContainer,
        port: Option<u16>,
    ) {
        let v = map.entry(host).or_default();
        if !v.iter().any(|x| x.container.id == c.id) {
            v.push(RouteEntry {
                container: c.clone(),
                port,
            });
        }
    }

    /// TCPルーティング表への追加(同一コンテナIDは重複追加しない)
    fn add_tcp(map: &mut HashMap<u16, Vec<ManagedContainer>>, port: u16, c: &ManagedContainer) {
        let v = map.entry(port).or_default();
        if !v.iter().any(|x| x.id == c.id) {
            v.push(c.clone());
        }
    }

    /// 候補リストから選択: running 優先、次に作成日時が新しい方を優先
    fn pick(candidates: &[RouteEntry]) -> Option<RouteEntry> {
        candidates
            .iter()
            .max_by(|a, b| {
                (a.is_running(), a.container.created).cmp(&(b.is_running(), b.container.created))
            })
            .cloned()
    }

    /// Host名から解決済みルート(コンテナ + 転送ポート)を取得
    /// ポート未指定ルートはコンテナのデフォルトポートに解決する
    ///
    /// 優先順位: 動的完全一致 → 動的ラベル前方一致 → 静的完全一致 → 静的ワイルドカード
    pub async fn resolve_with_static(&self, host: &str) -> Option<RouteResult> {
        let map = self.inner.read().await;
        // 1. 動的完全一致 → 2. 動的前方一致(サブドメイン)
        if let Some(e) = map.get(host).and_then(|v| Self::pick(v)) {
            let (c, p) = resolved_port(e);
            return Some(RouteResult::Dynamic(Box::new(c), p));
        }
        // 前方一致: "foo.bar.localhost" に対する "bar.localhost"
        if let Some(e) = map
            .iter()
            .find(|(k, _)| host.ends_with(k.as_str()) && host.len() > k.len())
            .and_then(|(_, v)| Self::pick(v))
        {
            let (c, p) = resolved_port(e);
            return Some(RouteResult::Dynamic(Box::new(c), p));
        }
        // 3. 静的完全一致 → 4. 静的ワイルドカード
        self.static_routes
            .read()
            .await
            .resolve(host)
            .map(RouteResult::Static)
    }

    /// 従来の動的解決のみ(静的ルートを考慮しない)
    /// テストからのみ使用(本番は resolve_with_static を使う)
    #[cfg(test)]
    pub async fn resolve(&self, host: &str) -> Option<(ManagedContainer, u16)> {
        let map = self.inner.read().await;
        if let Some(e) = map.get(host).and_then(|v| Self::pick(v)) {
            return Some(resolved_port(e));
        }
        // 前方一致: "foo.bar.localhost" に対する "bar.localhost"
        map.iter()
            .find(|(k, _)| host.ends_with(k.as_str()) && host.len() > k.len())
            .and_then(|(_, v)| Self::pick(v))
            .map(resolved_port)
    }

    /// TCP待ち受けポートからコンテナを解決
    pub async fn resolve_tcp(&self, listen_port: u16) -> Option<ManagedContainer> {
        let map = self.tcp_listen.read().await;
        map.get(&listen_port).and_then(|v| Self::pick_tcp(v))
    }

    /// TCP候補リストから選択: running 優先、次に作成日時が新しい方を優先
    fn pick_tcp(candidates: &[ManagedContainer]) -> Option<ManagedContainer> {
        candidates
            .iter()
            .max_by(|a, b| (a.is_running(), a.created).cmp(&(b.is_running(), b.created)))
            .cloned()
    }

    /// 登録されているTCP待ち受けポート一覧
    pub async fn tcp_listen_ports(&self) -> Vec<u16> {
        self.tcp_listen.read().await.keys().copied().collect()
    }

    /// 管理対象コンテナの一覧(コンテナIDでユニーク)
    pub async fn containers(&self) -> Vec<ManagedContainer> {
        let map = self.inner.read().await;
        let mut seen = HashSet::new();
        map.values()
            .flatten()
            .map(|e| &e.container)
            .filter(|c| seen.insert(c.id.clone()))
            .cloned()
            .collect()
    }

    /// グループに属するコンテナ一覧(コンテナIDでユニーク)
    pub async fn group_containers(&self, group: &str) -> Vec<ManagedContainer> {
        let map = self.inner.read().await;
        let mut seen = HashSet::new();
        map.values()
            .flatten()
            .map(|e| &e.container)
            .filter(|c| c.group.as_deref() == Some(group))
            .filter(|c| seen.insert(c.id.clone()))
            .cloned()
            .collect()
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }
}

/// ルートエントリを解決済み(コンテナ, ポート)に変換する。
/// ポート未指定ならコンテナのデフォルトポートを使用
fn resolved_port(e: RouteEntry) -> (ManagedContainer, u16) {
    let port = e.port.or(e.container.port);
    let port = port.unwrap_or(0);
    (e.container, port)
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use crate::docker::Route;
    use std::time::Duration;

    fn make_container(name: &str, group: Option<&str>) -> ManagedContainer {
        ManagedContainer {
            id: format!("id-{name}"),
            name: name.to_string(),
            port: Some(8000),
            tcp_expose: Vec::new(),
            group: group.map(|s| s.to_string()),
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
            depends_on: Vec::new(),
            compose_project: None,
            compose_service: None,
        }
    }

    /// テスト用ルート(ポートなし = デフォルトポート)
    fn route(host: &str) -> Route {
        Route {
            host: host.to_string(),
            port: None,
        }
    }

    /// テスト用ルート(ポート付き)
    fn route_port(host: &str, port: u16) -> Route {
        Route {
            host: host.to_string(),
            port: Some(port),
        }
    }

    #[tokio::test]
    async fn test_resolve_exact() {
        let router = Router::new();
        router
            .update(vec![make_container(
                "/federation-router-federation-router.sizebook-1",
                None,
            )])
            .await;

        let c = router
            .resolve("federation-router-federation-router.sizebook")
            .await;
        assert!(c.is_some());
    }

    #[tokio::test]
    async fn test_name_with_trailing_digits_kept() {
        let router = Router::new();
        router
            .update(vec![make_container("/dormant-test-web3", None)])
            .await;

        assert!(router.resolve("dormant-test-web3").await.is_some());
        assert!(router.resolve("dormant-test-web").await.is_none());
    }

    #[tokio::test]
    async fn test_replica_number_stripped() {
        let router = Router::new();
        router
            .update(vec![
                make_container("/service-1", None),
                make_container("/myapp-2", None),
            ])
            .await;

        assert!(router.resolve("service").await.is_some());
        assert!(router.resolve("myapp").await.is_some());
    }

    #[tokio::test]
    async fn test_resolve_host_label() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.routes = vec![route("app.example.com")];
        router.update(vec![c]).await;

        assert!(router.resolve("app.example.com").await.is_some());
        // コンテナ名由来の導出キーも従来どおり
        assert!(router.resolve("app").await.is_some());
    }

    #[tokio::test]
    async fn test_resolve_multiple_host_labels() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.routes = vec![route("app.example.com"), route("api.example.com")];
        router.update(vec![c]).await;

        assert!(router.resolve("app.example.com").await.is_some());
        assert!(router.resolve("api.example.com").await.is_some());
    }

    #[tokio::test]
    async fn test_alias_not_used_for_http_resolution() {
        // dormant.alias は HTTP ルーティング表に載せない
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.routes = vec![route("app.example.com")];
        c.aliases = vec!["myredis.local".to_string()];
        router.update(vec![c]).await;

        // dormant.host 由来は解決できる
        assert!(router.resolve("app.example.com").await.is_some());
        // dormant.alias の値では HTTP 解決されない
        assert!(router.resolve("myredis.local").await.is_none());
    }

    #[tokio::test]
    async fn test_resolve_route_port() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.routes = vec![route_port("app.example.com", 8080)];
        router.update(vec![c]).await;

        let (c, port) = router.resolve("app.example.com").await.unwrap();
        assert_eq!(c.id, "id-/app-1");
        assert_eq!(port, 8080);
    }

    #[tokio::test]
    async fn test_resolve_route_no_port_falls_back_to_default() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.port = Some(9000);
        c.routes = vec![route("app.example.com")];
        router.update(vec![c]).await;

        let (_, port) = router.resolve("app.example.com").await.unwrap();
        assert_eq!(port, 9000);
    }

    #[tokio::test]
    async fn test_resolve_multiple_routes_same_container_different_ports() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.port = Some(9000);
        c.routes = vec![route_port("api.example.com", 8081), route_port("web.example.com", 8080)];
        router.update(vec![c]).await;

        let (_, port_a) = router.resolve("api.example.com").await.unwrap();
        let (_, port_w) = router.resolve("web.example.com").await.unwrap();
        assert_eq!(port_a, 8081);
        assert_eq!(port_w, 8080);
    }

    #[tokio::test]
    async fn test_conflict_running_wins() {
        let router = Router::new();
        let mut stopped = make_container("/old-1", None);
        let mut running = make_container("/new-1", None);
        stopped.running = false;
        running.running = true;
        stopped.routes = vec![route("app.example.com")];
        running.routes = vec![route("app.example.com")];
        router.update(vec![stopped, running]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_conflict_stopped_loses() {
        let router = Router::new();
        let mut running = make_container("/old-1", None);
        let mut stopped = make_container("/new-1", None);
        running.running = true;
        stopped.running = false;
        running.routes = vec![route("app.example.com")];
        stopped.routes = vec![route("app.example.com")];
        router.update(vec![running, stopped]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/old-1");
    }

    #[tokio::test]
    async fn test_conflict_both_stopped() {
        let router = Router::new();
        let mut first = make_container("/old-1", None);
        let mut second = make_container("/new-1", None);
        first.running = false;
        second.running = false;
        first.created = Some(1000);
        second.created = Some(2000);
        first.routes = vec![route("app.example.com")];
        second.routes = vec![route("app.example.com")];
        router.update(vec![first, second]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(!c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_conflict_both_nonrunning_newest_wins() {
        let router = Router::new();
        let mut old = make_container("/old-1", None);
        let mut new = make_container("/new-1", None);
        old.running = false;
        new.running = false;
        old.created = Some(1000);
        new.created = Some(2000);
        old.routes = vec![route("app.example.com")];
        new.routes = vec![route("app.example.com")];
        // 新しい方が後に並んでも勝つ(後勝ちではなく作成日時優先)
        router.update(vec![new, old]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(!c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_conflict_both_running_newest_wins() {
        let router = Router::new();
        let mut old = make_container("/old-1", None);
        let mut new = make_container("/new-1", None);
        old.running = true;
        new.running = true;
        old.created = Some(1000);
        new.created = Some(2000);
        old.routes = vec![route("app.example.com")];
        new.routes = vec![route("app.example.com")];
        router.update(vec![old, new]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_conflict_running_wins_regardless_of_created() {
        let router = Router::new();
        let mut running_old = make_container("/old-1", None);
        let mut stopped_new = make_container("/new-1", None);
        running_old.running = true;
        stopped_new.running = false;
        running_old.created = Some(1000);
        stopped_new.created = Some(2000);
        running_old.routes = vec![route("app.example.com")];
        stopped_new.routes = vec![route("app.example.com")];
        // 新規が後に来ても、running の既存が優先(R2)
        router.update(vec![running_old, stopped_new]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/old-1");
    }

    #[tokio::test]
    async fn test_conflict_running_new_overwrites_stopped() {
        let router = Router::new();
        let mut stopped_old = make_container("/old-1", None);
        let mut running_new = make_container("/new-1", None);
        stopped_old.running = false;
        running_new.running = true;
        stopped_old.created = Some(2000);
        running_new.created = Some(1000);
        stopped_old.routes = vec![route("app.example.com")];
        running_new.routes = vec![route("app.example.com")];
        // 既存の作成日時が新しくても running の新規が優先(R1)
        router.update(vec![stopped_old, running_new]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_resolve_prefers_running() {
        let router = Router::new();
        let mut stopped = make_container("/old-1", None);
        let mut running = make_container("/new-1", None);
        stopped.running = false;
        running.running = true;
        stopped.created = Some(3000);
        running.created = Some(1000);
        stopped.routes = vec![route("app.example.com")];
        running.routes = vec![route("app.example.com")];
        router.update(vec![stopped, running]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_resolve_prefers_newest_when_all_stopped() {
        let router = Router::new();
        let mut old = make_container("/old-1", None);
        let mut new = make_container("/new-1", None);
        old.running = false;
        new.running = false;
        old.created = Some(1000);
        new.created = Some(2000);
        old.routes = vec![route("app.example.com")];
        new.routes = vec![route("app.example.com")];
        router.update(vec![old, new]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(!c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_resolve_prefers_newest_when_all_running() {
        let router = Router::new();
        let mut old = make_container("/old-1", None);
        let mut new = make_container("/new-1", None);
        old.running = true;
        new.running = true;
        old.created = Some(1000);
        new.created = Some(2000);
        old.routes = vec![route("app.example.com")];
        new.routes = vec![route("app.example.com")];
        router.update(vec![old, new]).await;

        let (c, _) = router.resolve("app.example.com").await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_containers_unique() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.routes = vec![route("app.example.com"), route("api.example.com")];
        router.update(vec![c]).await;

        // 名前由来キー + ホストラベル2つ → 同一コンテナが複数キーに現れるが一覧は1件
        let all = router.containers().await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, "id-/app-1");
    }

    #[tokio::test]
    async fn test_routing_G3_group_name_not_resolvable() {
        let router = Router::new();
        router
            .update(vec![
                make_container("/router-1", Some("federation-router-sizebook")),
                make_container("/account-1", Some("federation-router-sizebook")),
            ])
            .await;

        // G3: グループ名では解決できない(ルーティングはしない)
        assert!(router.resolve("federation-router-sizebook").await.is_none());

        // group_containers() はグループ起動用に維持
        let group = router.group_containers("federation-router-sizebook").await;
        assert_eq!(group.len(), 2);
    }

    #[tokio::test]
    async fn test_resolve_tcp_port() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.tcp_expose = vec![crate::docker::TcpExpose {
            listen_port: 6334,
            container_port: 9000,
        }];
        router.update(vec![c]).await;

        let c = router.resolve_tcp(6334).await.unwrap();
        assert_eq!(c.id, "id-/app-1");
        assert!(router.resolve_tcp(9999).await.is_none());
    }

    #[tokio::test]
    async fn test_tcp_listen_ports() {
        let router = Router::new();
        let mut a = make_container("/a-1", None);
        a.tcp_expose = vec![crate::docker::TcpExpose {
            listen_port: 6334,
            container_port: 6334,
        }];
        let mut b = make_container("/b-1", None);
        b.tcp_expose = vec![crate::docker::TcpExpose {
            listen_port: 7000,
            container_port: 8000,
        }];
        router.update(vec![a, b]).await;

        let mut ports = router.tcp_listen_ports().await;
        ports.sort_unstable();
        assert_eq!(ports, vec![6334, 7000]);
    }

    #[tokio::test]
    async fn test_resolve_tcp_conflict_running_wins() {
        let router = Router::new();
        let mut stopped = make_container("/old-1", None);
        let mut running = make_container("/new-1", None);
        stopped.running = false;
        running.running = true;
        stopped.created = Some(3000);
        running.created = Some(1000);
        stopped.tcp_expose = vec![crate::docker::TcpExpose {
            listen_port: 6334,
            container_port: 6334,
        }];
        running.tcp_expose = vec![crate::docker::TcpExpose {
            listen_port: 6334,
            container_port: 6334,
        }];
        router.update(vec![stopped, running]).await;

        let c = router.resolve_tcp(6334).await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_resolve_tcp_all_stopped_newest_wins() {
        let router = Router::new();
        let mut old = make_container("/old-1", None);
        let mut new = make_container("/new-1", None);
        old.running = false;
        new.running = false;
        old.created = Some(1000);
        new.created = Some(2000);
        old.tcp_expose = vec![crate::docker::TcpExpose {
            listen_port: 6334,
            container_port: 6334,
        }];
        new.tcp_expose = vec![crate::docker::TcpExpose {
            listen_port: 6334,
            container_port: 6334,
        }];
        router.update(vec![old, new]).await;

        let c = router.resolve_tcp(6334).await.unwrap();
        assert!(!c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    // ---- 静的ルート(ワイルドカード対応) ----

    /// テスト用 StaticRouteEntry
    fn sre(pattern: &str, ip: &str, port: u16) -> crate::config::StaticRouteEntry {
        crate::config::StaticRouteEntry {
            pattern: pattern.to_string(),
            ip: ip.to_string(),
            port,
        }
    }

    // ワイルドカード: 1段・多段・深いサブドメインすべてにマッチし、ベースドメイン自体はマッチしない
    #[tokio::test]
    async fn test_static_wildcard_matches_any_depth() {
        let router = Router::new();
        router
            .set_static_routes(&[sre("*.example.com", "203.0.113.10", 8080)])
            .await;

        // 1段
        let r = router.resolve_with_static("foo.example.com").await.unwrap();
        let t = r.static_target().unwrap();
        assert_eq!(t.ip, "203.0.113.10");
        assert_eq!(t.port, 8080);

        // 多段
        let r = router.resolve_with_static("foo.bar.example.com").await.unwrap();
        assert!(r.static_target().is_some());

        // 深い
        let r = router
            .resolve_with_static("a.b.c.d.example.com")
            .await
            .unwrap();
        assert!(r.static_target().is_some());

        // ベースドメイン自体はマッチしない
        assert!(router.resolve_with_static("example.com").await.is_none());
        // 関係ないホストはマッチしない
        assert!(router.resolve_with_static("other.org").await.is_none());
        // サフィックス一致のみ(前方部分にexample.comを含んでもマッチしない)
        assert!(router
            .resolve_with_static("example.com.evil.org")
            .await
            .is_none());
    }

    // 静的完全一致は静的ワイルドカードより優先される
    #[tokio::test]
    async fn test_static_exact_beats_wildcard() {
        let router = Router::new();
        router
            .set_static_routes(&[
                sre("api.example.com", "203.0.113.11", 8443),
                sre("*.example.com", "203.0.113.10", 8080),
            ])
            .await;

        let r = router.resolve_with_static("api.example.com").await.unwrap();
        let t = r.static_target().unwrap();
        assert_eq!(t.ip, "203.0.113.11");
        assert_eq!(t.port, 8443);

        // 完全一致のないサブドメインはワイルドカード
        let r = router.resolve_with_static("other.example.com").await.unwrap();
        let t = r.static_target().unwrap();
        assert_eq!(t.ip, "203.0.113.10");
        assert_eq!(t.port, 8080);
    }

    // 複数ワイルドカードがマッチする場合は最も長いサフィックスが優先
    #[tokio::test]
    async fn test_static_wildcard_longest_suffix_wins() {
        let router = Router::new();
        router
            .set_static_routes(&[
                sre("*.example.com", "203.0.113.10", 8080),
                sre("*.api.example.com", "203.0.113.20", 9000),
            ])
            .await;

        // 両方にマッチするホスト → 長いサフィックス(.api.example.com)が勝つ
        let r = router
            .resolve_with_static("x.api.example.com")
            .await
            .unwrap();
        let t = r.static_target().unwrap();
        assert_eq!(t.ip, "203.0.113.20");
        assert_eq!(t.port, 9000);

        // .api にマッチしないホスト → 短い方
        let r = router.resolve_with_static("x.example.com").await.unwrap();
        let t = r.static_target().unwrap();
        assert_eq!(t.ip, "203.0.113.10");
        assert_eq!(t.port, 8080);
    }

    // 衝突時は動的(dormant.host)優先。静的完全一致は動的完全一致に負ける
    #[tokio::test]
    async fn test_static_conflict_dynamic_exact_wins() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.routes = vec![route("app.example.com")];
        router.update(vec![c]).await;
        router
            .set_static_routes(&[sre("app.example.com", "203.0.113.11", 8443)])
            .await;

        // 動的完全一致が勝つ
        let r = router.resolve_with_static("app.example.com").await.unwrap();
        let (c, port) = match r {
            RouteResult::Dynamic(c, p) => (*c, p),
            RouteResult::Static(_) => panic!("expected dynamic"),
        };
        assert_eq!(c.id, "id-/app-1");
        assert_eq!(port, 8000);
    }

    // 衝突時: 動的ラベル前方一致も静的より優先される
    #[tokio::test]
    async fn test_static_conflict_dynamic_prefix_wins() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.routes = vec![route("bar.example.com")];
        router.update(vec![c]).await;
        router
            .set_static_routes(&[sre("*.example.com", "203.0.113.10", 8080)])
            .await;

        // foo.bar.example.com は動的ラベル bar.example.com の前方一致 → 動的優先
        let r = router
            .resolve_with_static("foo.bar.example.com")
            .await
            .unwrap();
        let c = r.container().unwrap();
        assert_eq!(c.id, "id-/app-1");

        // 動的ラベルに含まれないサブドメインは静的ワイルドカード
        let r = router
            .resolve_with_static("foo.other.example.com")
            .await
            .unwrap();
        assert!(r.static_target().is_some());
    }

    // 動的解決にヒットしないホストは静的完全一致で解決される
    #[tokio::test]
    async fn test_static_exact_resolves() {
        let router = Router::new();
        router
            .set_static_routes(&[sre("api.example.com", "203.0.113.11", 8443)])
            .await;

        let r = router.resolve_with_static("api.example.com").await.unwrap();
        let t = r.static_target().unwrap();
        assert_eq!(t.ip, "203.0.113.11");
        assert_eq!(t.port, 8443);

        // 完全一致にないホストは静的ルートでは解決されない
        assert!(router.resolve_with_static("other.example.com").await.is_none());
        // 従来の動的解決(静的考慮なし)では解決されない
        assert!(router.resolve("api.example.com").await.is_none());
    }

    // 不正なパターン(`*` が先頭以外・`*.` のみ)はスキップされる
    #[tokio::test]
    async fn test_static_invalid_patterns_skipped() {
        let router = Router::new();
        router
            .set_static_routes(&[
                sre("a*b.example.com", "203.0.113.1", 80),
                sre("*.", "203.0.113.2", 80),
                sre("ok.example.com", "203.0.113.3", 80),
            ])
            .await;

        // 不正パターンは表に入らず、正しいものだけ残る
        let routes = router.static_routes().await;
        assert_eq!(routes.exact.len(), 1);
        assert!(routes.exact.contains_key("ok.example.com"));
        assert!(routes.wildcard.is_empty());

        let r = router.resolve_with_static("ok.example.com").await.unwrap();
        let t = r.static_target().unwrap();
        assert_eq!(t.ip, "203.0.113.3");
    }
}
