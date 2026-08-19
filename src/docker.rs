//! Docker連携: ラベル収集、コンテナ起動/停止、イベント監視

use anyhow::{anyhow, Result};
use bollard::models::{ContainerSummary, ContainerSummaryStateEnum, EventMessageTypeEnum, HealthStatusEnum};
use bollard::query_parameters::{
    EventsOptions, InspectContainerOptions, ListContainersOptions, StartContainerOptions,
    StopContainerOptions,
};
use bollard::Docker;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::config::*;
use crate::router::Router;

/// compose depends_on の1エントリ
#[derive(Debug, Clone)]
pub struct Dependency {
    /// compose サービス名
    pub service: String,
    /// コンディション(service_started / service_healthy / ...)
    pub condition: String,
}

/// dormant 管理対象コンテナの情報
#[derive(Debug, Clone)]
pub struct ManagedContainer {
    /// コンテナID
    pub id: String,
    /// コンテナ名(/ 付き)
    pub name: String,
    /// 公開ポート(依存専用コンテナなど未指定の場合は None)
    pub port: Option<u16>,
    /// 所属グループ
    pub group: Option<String>,
    /// セッション保持時間
    pub session_duration: Duration,
    /// 起動タイムアウト
    pub startup_timeout: Duration,
    /// ヘルスチェックパス(あれば)
    pub healthcheck_path: Option<String>,
    /// ヘルスチェックポート(あれば)
    pub healthcheck_port: Option<u16>,
    /// ヘルスチェック許容ステータス(あれば)
    pub healthcheck_status: Option<Vec<u16>>,
    /// ラベル指定のホスト名(カンマ区切り)
    pub hosts: Vec<String>,
    /// コンテナIP(ネットワーク解決用)
    pub ip: Option<String>,
    /// running状態か
    pub running: bool,
    /// 作成日時(Unixタイムスタンプ秒)
    pub created: Option<i64>,
    /// compose depends_on エントリ
    pub depends_on: Vec<Dependency>,
    /// compose プロジェクト名(依存解決用)
    pub compose_project: Option<String>,
    /// compose サービス名(依存解決用)
    pub compose_service: Option<String>,
}

/// Dockerクライアントのラッパー
#[derive(Clone)]
pub struct DockerClient {
    docker: Docker,
    /// dormant 自身が接続しているネットワーク名(共有ネットワークのIP優先に使う)
    self_networks: Arc<RwLock<Option<HashSet<String>>>>,
}

impl DockerClient {
    pub fn new(socket_path: &str) -> Result<Self> {
        let docker = Docker::connect_with_unix(socket_path, 120, bollard::API_DEFAULT_VERSION)?;
        Ok(Self {
            docker,
            self_networks: Arc::new(RwLock::new(None)),
        })
    }

    pub fn inner(&self) -> &Docker {
        &self.docker
    }

    /// dormant 自身が接続しているネットワーク名を取得(初回のみ解決してキャッシュ)
    pub async fn self_networks(&self) -> HashSet<String> {
        if let Some(nets) = self.self_networks.read().await.as_ref() {
            return nets.clone();
        }
        let nets = self.resolve_self_networks().await;
        *self.self_networks.write().await = Some(nets.clone());
        nets
    }

    /// 自身のコンテナIDから接続ネットワーク名を解決
    async fn resolve_self_networks(&self) -> HashSet<String> {
        let Ok(hostname) = std::fs::read_to_string("/etc/hostname") else {
            return HashSet::new();
        };
        let hostname = hostname.trim().to_string();
        let Ok(inspect) = self
            .docker
            .inspect_container(&hostname, None::<InspectContainerOptions>)
            .await
        else {
            return HashSet::new();
        };
        inspect
            .network_settings
            .and_then(|ns| ns.networks)
            .unwrap_or_default()
            .keys()
            .cloned()
            .collect()
    }

    /// テスト用: dormant 自身のネットワーク名を直接設定する
    #[cfg(test)]
    pub async fn set_self_networks(&self, nets: HashSet<String>) {
        *self.self_networks.write().await = Some(nets);
    }

    /// 管理対象コンテナの一覧を取得
    pub async fn list_managed(&self) -> Result<Vec<ManagedContainer>> {
        let containers = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                ..Default::default()
            }))
            .await?;

        let mut managed = Vec::new();
        for c in containers {
            if let Some(mut m) = parse_container(&c) {
                // コンテナIPを解決(転送先に必要)
                m.ip = self.resolve_ip(&m.id).await.ok();
                managed.push(m);
            }
        }
        Ok(managed)
    }

    /// コンテナのIPアドレスを取得
    /// dormant 自身と共有するネットワークのIPを優先し、なければ最初のネットワークのIPを使う
    pub async fn resolve_ip(&self, id: &str) -> Result<String> {
        let inspect = self
            .docker
            .inspect_container(id, None::<InspectContainerOptions>)
            .await?;
        let networks = inspect
            .network_settings
            .and_then(|ns| ns.networks)
            .unwrap_or_default();

        // dormant 自身と共有するネットワークのIPを優先
        let self_nets = self.self_networks().await;
        if !self_nets.is_empty() {
            if let Some(ip) = networks
                .iter()
                .filter(|(name, _)| self_nets.contains(*name))
                .find_map(|(_, n)| n.ip_address.clone().filter(|ip| !ip.is_empty()))
            {
                return Ok(ip);
            }
        }

        // 共有ネットワークが無い/解決できない場合は最初のネットワークのIPを使う
        let ip = networks
            .values()
            .find_map(|n| n.ip_address.clone().filter(|ip| !ip.is_empty()))
            .ok_or_else(|| anyhow!("container {} has no IP", id))?;
        Ok(ip)
    }

    /// コンテナを起動
    pub async fn start(&self, id: &str) -> Result<()> {
        self.docker
            .start_container(id, None::<StartContainerOptions>)
            .await?;
        Ok(())
    }

    /// コンテナを停止
    pub async fn stop(&self, id: &str) -> Result<()> {
        self.docker
            .stop_container(id, None::<StopContainerOptions>)
            .await?;
        Ok(())
    }

    /// コンテナがrunning状態か
    pub async fn is_running(&self, id: &str) -> Result<bool> {
        let inspect = self
            .docker
            .inspect_container(id, None::<InspectContainerOptions>)
            .await?;
        Ok(inspect.state.and_then(|s| s.running).unwrap_or(false))
    }

    /// コンテナがreadyか(healthcheck定義時はhealthyを要求)
    pub async fn is_ready(&self, m: &ManagedContainer) -> Result<bool> {
        let inspect = self
            .docker
            .inspect_container(&m.id, None::<InspectContainerOptions>)
            .await?;

        let state = inspect.state.unwrap_or_default();
        if !state.running.unwrap_or(false) {
            return Ok(false);
        }

        if let Some(status) = state.health.and_then(|h| h.status) {
            return Ok(status == HealthStatusEnum::HEALTHY);
        }

        Ok(true)
    }

    /// Dockerイベントを監視し、ルーターを更新
    pub async fn watch_events(&self, router: &Router) {
        let mut stream = self.docker.events(Some(EventsOptions {
            since: None,
            until: None,
            filters: Some(HashMap::new()),
        }));

        while let Some(ev) = stream.next().await {
            match ev {
                Ok(event) => {
                    let t = match event.typ {
                        Some(EventMessageTypeEnum::CONTAINER) => "container",
                        _ => "other",
                    };
                    let action = event.action.as_deref().unwrap_or("");
                    tracing::debug!("docker event: type={} action={}", t, action);

                    if matches!(action, "create" | "start" | "stop" | "destroy" | "die" | "rename")
                    {
                        if let Err(e) = sync_routes(self, router).await {
                            tracing::warn!("route sync failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("docker event stream error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

/// コンテナ一覧からルーターを再構築
pub async fn sync_routes(docker: &DockerClient, router: &Router) -> Result<()> {
    let managed = docker.list_managed().await?;
    router.update(managed).await;
    tracing::info!("routes updated: {} managed containers", router.len().await);
    Ok(())
}

/// コンテナ情報から管理対象を判定・パース
fn parse_container(c: &ContainerSummary) -> Option<ManagedContainer> {
    let labels = c.labels.as_ref()?;
    let enabled = labels.get(LABEL_ENABLE).map(|v| v == "true").unwrap_or(false);
    if !enabled {
        return None;
    }

    let id = c.id.clone()?;
    let name = c.names.clone()?.into_iter().next()?;

    // ポート: dormant.port > dormant.healthcheck.port > 公開ポートの先頭
    let healthcheck_port = labels
        .get(LABEL_HEALTHCHECK_PORT)
        .and_then(|v| v.parse::<u16>().ok());

    // ポート: dormant.port > dormant.healthcheck.port > 公開ポートの先頭
    // 未指定でも管理対象として扱う(依存専用コンテナなど)。転送時のみ必要
    let port = labels
        .get("dormant.port")
        .and_then(|v| v.parse::<u16>().ok())
        .or(healthcheck_port)
        .or_else(|| {
            c.ports.as_ref().and_then(|ports| {
                ports.iter().filter_map(|p| Some(p.private_port)).next()
            })
        });

    let session_duration = parse_duration(
        labels.get(LABEL_SESSION_DURATION).map(|s| s.as_str()),
        DEFAULT_SESSION_DURATION,
    );
    let startup_timeout = parse_duration(
        labels.get(LABEL_STARTUP_TIMEOUT).map(|s| s.as_str()),
        DEFAULT_STARTUP_TIMEOUT,
    );

    let healthcheck_status = labels
        .get(LABEL_HEALTHCHECK_STATUS)
        .map(|v| {
            v.split(',')
                .filter_map(|s| s.trim().parse::<u16>().ok())
                .collect::<Vec<u16>>()
        })
        .filter(|v| !v.is_empty());
    let hosts = labels
        .get(LABEL_HOST)
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();

    // depends_on: `サービス名:コンディション:必須フラグ` のカンマ区切り
    let depends_on = labels
        .get(LABEL_COMPOSE_DEPENDS_ON)
        .map(|v| {
            v.split(',')
                .filter_map(|s| {
                    let mut parts = s.trim().splitn(3, ':');
                    let service = parts.next()?.to_string();
                    if service.is_empty() {
                        return None;
                    }
                    let condition = parts.next().unwrap_or("service_started").to_string();
                    Some(Dependency { service, condition })
                })
                .collect::<Vec<Dependency>>()
        })
        .unwrap_or_default();

    Some(ManagedContainer {
        id,
        name,
        port,
        group: labels.get(LABEL_GROUP).cloned(),
        session_duration,
        startup_timeout,
        healthcheck_path: labels.get(LABEL_HEALTHCHECK_PATH).cloned(),
        healthcheck_port,
        healthcheck_status,
        hosts,
        ip: None,
        running: c.state == Some(ContainerSummaryStateEnum::RUNNING),
        created: c.created,
        depends_on,
        compose_project: labels.get(LABEL_COMPOSE_PROJECT).cloned(),
        compose_service: labels.get(LABEL_COMPOSE_SERVICE).cloned(),
    })
}

impl ManagedContainer {
    /// running状態か
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// 転送先アドレスを計算(コンテナIP直アクセス)
    /// ip が解決できない場合はコンテナIDにフォールバック
    /// ポート未指定(依存専用コンテナ)は IP のみ返す
    pub fn target_addr(&self) -> String {
        match (&self.ip, self.port) {
            (Some(ip), Some(port)) => format!("{}:{}", ip, port),
            (Some(ip), None) => ip.clone(),
            (None, Some(port)) => format!("{}:{}", self.id, port),
            (None, None) => self.id.clone(),
        }
    }

    /// 依存先のコンテナを解決(同一 compose project + service 名で検索)
    pub fn resolve_dependencies(&self, containers: &[ManagedContainer]) -> Vec<ManagedContainer> {
        self.depends_on
            .iter()
            .filter_map(|d| {
                containers.iter().find(|c| {
                    c.compose_project.as_deref() == self.compose_project.as_deref()
                        && c.compose_service.as_deref() == Some(d.service.as_str())
                })
            })
            .cloned()
            .collect()
    }
}

/// "1h" / "30m" / "90s" 形式の時間をパース
fn parse_duration(v: Option<&str>, default: &str) -> Duration {
    v.and_then(|s| humantime::parse_duration(s).ok())
        .unwrap_or_else(|| humantime::parse_duration(default).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::models::ContainerSummary;

    #[test]
    fn test_parse_duration() {
        assert_eq!(
            parse_duration(Some("30m"), DEFAULT_SESSION_DURATION),
            Duration::from_secs(1800)
        );
        assert_eq!(
            parse_duration(Some("90s"), DEFAULT_SESSION_DURATION),
            Duration::from_secs(90)
        );
        // 不正な値はデフォルト
        assert_eq!(
            parse_duration(Some("abc"), DEFAULT_SESSION_DURATION),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn test_parse_container_host_and_healthcheck_status() {
        let mut labels = HashMap::new();
        labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        labels.insert("dormant.port".to_string(), "8080".to_string());
        labels.insert(LABEL_HOST.to_string(), "app.example.com, api.example.com".to_string());
        labels.insert(LABEL_HEALTHCHECK_STATUS.to_string(), "200,204,abc".to_string());
        let c = ContainerSummary {
            id: Some("abc123".to_string()),
            names: Some(vec!["/test-1".to_string()]),
            labels: Some(labels),
            ..Default::default()
        };
        let m = parse_container(&c).unwrap();
        assert_eq!(
            m.hosts,
            vec!["app.example.com".to_string(), "api.example.com".to_string()]
        );
        assert_eq!(m.healthcheck_status, Some(vec![200, 204]));
    }

    #[test]
    fn test_parse_container_depends_on() {
        let mut labels = HashMap::new();
        labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        labels.insert("dormant.port".to_string(), "8080".to_string());
        labels.insert(
            LABEL_COMPOSE_DEPENDS_ON.to_string(),
            "searxng-valkey:service_started:false,db:service_healthy:true".to_string(),
        );
        labels.insert(LABEL_COMPOSE_PROJECT.to_string(), "prospector".to_string());
        labels.insert(LABEL_COMPOSE_SERVICE.to_string(), "searxng".to_string());
        let c = ContainerSummary {
            id: Some("abc123".to_string()),
            names: Some(vec!["/prospector-searxng-1".to_string()]),
            labels: Some(labels),
            ..Default::default()
        };
        let m = parse_container(&c).unwrap();
        assert_eq!(m.compose_project.as_deref(), Some("prospector"));
        assert_eq!(m.compose_service.as_deref(), Some("searxng"));
        assert_eq!(m.depends_on.len(), 2);
        assert_eq!(m.depends_on[0].service, "searxng-valkey");
        assert_eq!(m.depends_on[0].condition, "service_started");
        assert_eq!(m.depends_on[1].service, "db");
        assert_eq!(m.depends_on[1].condition, "service_healthy");
    }

    #[test]
    fn test_resolve_dependencies() {
        let dep = ManagedContainer {
            id: "id-dep".to_string(),
            name: "/dep-1".to_string(),
            port: Some(8000),
            group: None,
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            hosts: Vec::new(),
            ip: None,
            running: false,
            created: None,
            depends_on: Vec::new(),
            compose_project: Some("proj".to_string()),
            compose_service: Some("db".to_string()),
        };
        let app = ManagedContainer {
            id: "id-app".to_string(),
            name: "app-1".to_string(),
            port: Some(8000),
            group: None,
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            hosts: Vec::new(),
            ip: None,
            running: false,
            created: None,
            depends_on: vec![
                Dependency {
                    service: "db".to_string(),
                    condition: "service_started".to_string(),
                },
                // D1-4: 複数依存
                Dependency {
                    service: "cache".to_string(),
                    condition: "service_healthy".to_string(),
                },
            ],
            compose_project: Some("proj".to_string()),
            compose_service: Some("app".to_string()),
        };
        let other = ManagedContainer {
            id: "id-other".to_string(),
            name: "other-1".to_string(),
            port: Some(8000),
            group: None,
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            hosts: Vec::new(),
            ip: None,
            running: false,
            created: None,
            depends_on: Vec::new(),
            compose_project: Some("other-proj".to_string()),
            compose_service: Some("db".to_string()),
        };
        let cache = ManagedContainer {
            id: "id-cache".to_string(),
            name: "cache-1".to_string(),
            port: Some(8000),
            group: None,
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            hosts: Vec::new(),
            ip: None,
            running: false,
            created: None,
            depends_on: Vec::new(),
            compose_project: Some("proj".to_string()),
            compose_service: Some("cache".to_string()),
        };
        let resolved = app.resolve_dependencies(&[dep.clone(), other, cache]);
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].id, "id-dep");
        assert_eq!(resolved[1].id, "id-cache");
        // 未解決(存在しない)依存は空
        let missing = ManagedContainer {
            id: "id-missing".to_string(),
            name: "missing-1".to_string(),
            port: Some(8000),
            group: None,
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            hosts: Vec::new(),
            ip: None,
            running: false,
            created: None,
            depends_on: vec![Dependency {
                service: "nonexistent".to_string(),
                condition: "service_started".to_string(),
            }],
            compose_project: Some("proj".to_string()),
            compose_service: Some("app2".to_string()),
        };
        assert!(missing.resolve_dependencies(&[dep]).is_empty());
    }

    // 共有ネットワークのIPを優先する(複数ネットワーク接続時)
    #[tokio::test]
    async fn test_resolve_ip_prefers_shared_network() {
        use crate::testutil::{setup_mock_docker, MockContainer};

        // 対象コンテナは global(共有) と trace(非共有) の2ネットワークに接続
        let mut target = MockContainer::new("app", "172.22.0.3", 8000);
        target.networks = vec![
            ("trace".to_string(), "172.21.0.2".to_string()),
            ("global".to_string(), "172.22.0.3".to_string()),
        ];
        let (docker, _mock) = setup_mock_docker(vec![target]).await;

        // dormant 自身は global にのみ接続
        docker
            .set_self_networks(HashSet::from(["global".to_string()]))
            .await;

        // 共有ネットワーク(global)のIPが選ばれる
        let ip = docker.resolve_ip("app").await.unwrap();
        assert_eq!(ip, "172.22.0.3");
    }

    // 共有ネットワークが無い場合は最初のネットワークのIPにフォールバック
    #[tokio::test]
    async fn test_resolve_ip_falls_back_when_no_shared_network() {
        use crate::testutil::{setup_mock_docker, MockContainer};

        let mut target = MockContainer::new("app", "172.22.0.3", 8000);
        target.networks = vec![
            ("trace".to_string(), "172.21.0.2".to_string()),
            ("global".to_string(), "172.22.0.3".to_string()),
        ];
        let (docker, _mock) = setup_mock_docker(vec![target]).await;

        // dormant 自身はどのネットワークにも接続していない(空)
        docker.set_self_networks(HashSet::new()).await;

        // フォールバックで最初のネットワークのIPが返る
        let ip = docker.resolve_ip("app").await.unwrap();
        assert_eq!(ip, "172.21.0.2");
    }
}
