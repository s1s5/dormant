//! ルーティング: Hostヘッダー → コンテナのマッピング管理

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::docker::ManagedContainer;

/// ルーティングテーブル
/// key: Host名(例: "graphql.sb.carrot.localhost")
/// value: そのホストの管理対象コンテナ候補リスト
#[derive(Clone, Default)]
pub struct Router {
    inner: Arc<RwLock<HashMap<String, Vec<ManagedContainer>>>>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// ルーターを再構築
    pub async fn update(&self, containers: Vec<ManagedContainer>) {
        let mut map = HashMap::new();
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

            // コンテナ名由来の導出キーは従来どおり残す(後方互換)
            Self::add(&mut map, name, &c);

            // ラベルで明示指定があればそれも使う
            // dormant.host ラベル: カンマ区切りで複数ホストを登録
            for host in &c.hosts {
                Self::add(&mut map, host.clone(), &c);
            }
        }
        *self.inner.write().await = map;
    }

    /// 候補リストに追加(同一コンテナIDは重複追加しない)
    fn add(map: &mut HashMap<String, Vec<ManagedContainer>>, host: String, c: &ManagedContainer) {
        let v = map.entry(host).or_default();
        if !v.iter().any(|x| x.id == c.id) {
            v.push(c.clone());
        }
    }

    /// 候補リストから選択: running 優先、次に作成日時が新しい方を優先
    fn pick(candidates: &[ManagedContainer]) -> Option<ManagedContainer> {
        candidates
            .iter()
            .max_by(|a, b| (a.is_running(), a.created).cmp(&(b.is_running(), b.created)))
            .cloned()
    }

    /// Host名からコンテナを解決
    pub async fn resolve(&self, host: &str) -> Option<ManagedContainer> {
        let map = self.inner.read().await;
        // 完全一致 → 前方一致(サブドメイン)の順で探す
        if let Some(c) = map.get(host).and_then(|v| Self::pick(v)) {
            return Some(c);
        }
        // 前方一致: "foo.bar.localhost" に対する "bar.localhost"
        map.iter()
            .find(|(k, _)| host.ends_with(k.as_str()) && host.len() > k.len())
            .and_then(|(_, v)| Self::pick(v))
    }

    /// 管理対象コンテナの一覧(コンテナIDでユニーク)
    pub async fn containers(&self) -> Vec<ManagedContainer> {
        let map = self.inner.read().await;
        let mut seen = HashSet::new();
        map.values()
            .flatten()
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
            .filter(|c| c.group.as_deref() == Some(group))
            .filter(|c| seen.insert(c.id.clone()))
            .cloned()
            .collect()
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_container(name: &str, group: Option<&str>) -> ManagedContainer {
        ManagedContainer {
            id: format!("id-{name}"),
            name: name.to_string(),
            port: Some(8000),
            group: group.map(|s| s.to_string()),
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            hosts: Vec::new(),
            ip: Some("172.20.0.99".to_string()),
            running: false,
            created: None,
            depends_on: Vec::new(),
            compose_project: None,
            compose_service: None,
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
        c.hosts = vec!["app.example.com".to_string()];
        router.update(vec![c]).await;

        assert!(router.resolve("app.example.com").await.is_some());
        // コンテナ名由来の導出キーも従来どおり
        assert!(router.resolve("app").await.is_some());
    }

    #[tokio::test]
    async fn test_resolve_multiple_host_labels() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.hosts = vec![
            "app.example.com".to_string(),
            "api.example.com".to_string(),
        ];
        router.update(vec![c]).await;

        assert!(router.resolve("app.example.com").await.is_some());
        assert!(router.resolve("api.example.com").await.is_some());
    }

    #[tokio::test]
    async fn test_conflict_running_wins() {
        let router = Router::new();
        let mut stopped = make_container("/old-1", None);
        let mut running = make_container("/new-1", None);
        stopped.running = false;
        running.running = true;
        stopped.hosts = vec!["app.example.com".to_string()];
        running.hosts = vec!["app.example.com".to_string()];
        router.update(vec![stopped, running]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        running.hosts = vec!["app.example.com".to_string()];
        stopped.hosts = vec!["app.example.com".to_string()];
        router.update(vec![running, stopped]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        first.hosts = vec!["app.example.com".to_string()];
        second.hosts = vec!["app.example.com".to_string()];
        router.update(vec![first, second]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        old.hosts = vec!["app.example.com".to_string()];
        new.hosts = vec!["app.example.com".to_string()];
        // 新しい方が後に並んでも勝つ(後勝ちではなく作成日時優先)
        router.update(vec![new, old]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        old.hosts = vec!["app.example.com".to_string()];
        new.hosts = vec!["app.example.com".to_string()];
        router.update(vec![old, new]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        running_old.hosts = vec!["app.example.com".to_string()];
        stopped_new.hosts = vec!["app.example.com".to_string()];
        // 新規が後に来ても、running の既存が優先(R2)
        router.update(vec![running_old, stopped_new]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        stopped_old.hosts = vec!["app.example.com".to_string()];
        running_new.hosts = vec!["app.example.com".to_string()];
        // 既存の作成日時が新しくても running の新規が優先(R1)
        router.update(vec![stopped_old, running_new]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        stopped.hosts = vec!["app.example.com".to_string()];
        running.hosts = vec!["app.example.com".to_string()];
        router.update(vec![stopped, running]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        old.hosts = vec!["app.example.com".to_string()];
        new.hosts = vec!["app.example.com".to_string()];
        router.update(vec![old, new]).await;

        let c = router.resolve("app.example.com").await.unwrap();
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
        old.hosts = vec!["app.example.com".to_string()];
        new.hosts = vec!["app.example.com".to_string()];
        router.update(vec![old, new]).await;

        let c = router.resolve("app.example.com").await.unwrap();
        assert!(c.is_running());
        assert_eq!(c.id, "id-/new-1");
    }

    #[tokio::test]
    async fn test_containers_unique() {
        let router = Router::new();
        let mut c = make_container("/app-1", None);
        c.hosts = vec![
            "app.example.com".to_string(),
            "api.example.com".to_string(),
        ];
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
}
