//! Docker連携: ラベル収集、コンテナ起動/停止、イベント監視

use anyhow::{anyhow, Result};
use bollard::models::{
    ContainerSummary, ContainerSummaryStateEnum, EventMessageTypeEnum, HealthStatusEnum,
};
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

/// TCP転送の公開設定
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpExpose {
    /// dormant 側の待ち受けポート
    pub listen_port: u16,
    /// コンテナ側の転送先ポート
    pub container_port: u16,
}

/// ルーティングエントリ(dormant.host ラベル由来)
/// `host:port` 形式で指定し、port は省略可能(省略時はコンテナのデフォルトポートへ振り分け)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// ルーティング用ホスト名
    pub host: String,
    /// 転送先ポート(未指定は None = コンテナのデフォルトポート)
    pub port: Option<u16>,
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
    /// TCP転送の公開設定(dormant.tcp ラベルで指定、複数可)
    pub tcp_expose: Vec<TcpExpose>,
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
    /// ルーティングエントリ(dormant.host ラベル由来、host[:port] のカンマ区切り)
    pub routes: Vec<Route>,
    /// ネットワークエイリアス(dormant.alias ラベル由来、カンマ区切りのホスト名のみ)
    /// HTTP ルーティングには使わない。dormant 自身のネットワークエイリアス付与のみに使う
    pub aliases: Vec<String>,
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
    /// 常時ON(dormant.always-on=true)。アイドル停止・補助回収の対象外
    pub always_on: bool,
}

/// Dockerクライアントのラッパー
#[derive(Clone)]
pub struct DockerClient {
    docker: Docker,
    /// dormant 自身が接続しているネットワーク名(共有ネットワークのIP優先に使う)
    self_networks: Arc<RwLock<Option<HashSet<String>>>>,
    /// dormant が起動したコンテナID(メモリ追跡。参照カウント0の回収対象判定に使う)
    started_by_dormant: Arc<RwLock<HashSet<String>>>,
    /// テスト用: 自身のコンテナIDの上書き(/etc/hostname 解決の代わり)
    #[cfg(test)]
    self_id: Arc<RwLock<Option<String>>>,
}

impl DockerClient {
    pub fn new(socket_path: &str) -> Result<Self> {
        let docker = Docker::connect_with_unix(socket_path, 120, bollard::API_DEFAULT_VERSION)?;
        Ok(Self {
            docker,
            self_networks: Arc::new(RwLock::new(None)),
            started_by_dormant: Arc::new(RwLock::new(HashSet::new())),
            #[cfg(test)]
            self_id: Arc::new(RwLock::new(None)),
        })
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

    /// 自身のコンテナIDを `/etc/hostname` から解決する
    /// (テストでは set_self_id で直接設定した値を使う)
    async fn resolve_self_id(&self) -> Option<String> {
        #[cfg(test)]
        {
            if let Some(id) = self.self_id.read().await.as_ref() {
                return Some(id.clone());
            }
        }
        let hostname = std::fs::read_to_string("/etc/hostname").ok()?;
        let hostname = hostname.trim().to_string();
        let inspect = self
            .docker
            .inspect_container(&hostname, None::<InspectContainerOptions>)
            .await
            .ok()?;
        // ID が無い場合はホスト名そのままをフォールバックとして使う
        Some(inspect.id.unwrap_or(hostname))
    }

    /// 自身のコンテナ名を `/etc/hostname` から解決する
    /// (sync_self_aliases で切断時に自身の名前エイリアスを保護するために使う)
    async fn resolve_self_name(&self) -> Option<String> {
        let hostname = std::fs::read_to_string("/etc/hostname").ok()?;
        Some(hostname.trim().to_string())
    }

    /// コンテナをネットワークに接続し、エイリアスを付与する
    /// (`docker network connect --alias <host> ... <network> <container>` 相当)
    async fn connect_with_aliases(
        &self,
        network: &str,
        id: &str,
        aliases: &[String],
    ) -> Result<()> {
        self.docker
            .connect_network(
                network,
                bollard::models::NetworkConnectRequest {
                    container: id.to_string(),
                    endpoint_config: Some(bollard::models::EndpointSettings {
                        aliases: Some(aliases.to_vec()),
                        ..Default::default()
                    }),
                },
            )
            .await
            .map_err(|e| anyhow!("connect self to network '{}' failed: {}", network, e))?;
        Ok(())
    }

    /// コンテナをネットワークから切断する(エイリアスを付け直すために使う)
    /// force: true で強制切断する
    async fn disconnect_from_network(&self, network: &str, id: &str) -> Result<()> {
        self.docker
            .disconnect_network(
                network,
                bollard::models::NetworkDisconnectRequest {
                    container: id.to_string(),
                    force: Some(true),
                },
            )
            .await
            .map_err(|e| anyhow!("disconnect self from network '{}' failed: {}", network, e))?;
        Ok(())
    }

    /// コンテナの指定ネットワーク上の現在のエイリアスを取得する
    /// ネットワークに未接続なら Ok(None) を返す
    async fn current_network_aliases(
        &self,
        id: &str,
        network: &str,
    ) -> Result<Option<Vec<String>>> {
        let inspect = self
            .docker
            .inspect_container(id, None::<InspectContainerOptions>)
            .await?;
        let mut networks = inspect.network_settings.and_then(|ns| ns.networks);
        Ok(match networks.as_mut().and_then(|m| m.remove(network)) {
            Some(endpoint) => Some(endpoint.aliases.unwrap_or_default()),
            None => None,
        })
    }

    /// テスト用: dormant 自身のネットワーク名を直接設定する
    #[cfg(test)]
    pub async fn set_self_networks(&self, nets: HashSet<String>) {
        *self.self_networks.write().await = Some(nets);
    }

    /// テスト用: 自身のコンテナIDを直接設定する(/etc/hostname 解決の代わり)
    #[cfg(test)]
    pub async fn set_self_id(&self, id: Option<String>) {
        *self.self_id.write().await = id;
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
        // dormant が起動したコンテナとして記録(参照カウント0の回収対象判定に使う)
        self.started_by_dormant.write().await.insert(id.to_string());
        Ok(())
    }

    /// dormant が起動したコンテナID一覧(メモリ追跡)
    pub async fn started_by_dormant(&self) -> HashSet<String> {
        self.started_by_dormant.read().await.clone()
    }

    /// コンテナIDを dormant 起動記録から削除
    pub async fn forget_started(&self, id: &str) {
        self.started_by_dormant.write().await.remove(id);
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
    /// `self_network` が Some なら、ルート同期の後に dormant 自身のネットワークエイリアスも同期する
    pub async fn watch_events(&self, router: &Router, self_network: Option<&str>) {
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

                    if matches!(
                        action,
                        "create" | "start" | "stop" | "destroy" | "die" | "rename"
                    ) {
                        if let Err(e) = sync_routes(self, router).await {
                            tracing::warn!("route sync failed: {}", e);
                            continue;
                        }
                        // ルート同期後に自身のエイリアスも同期(対象ネットワーク指定時のみ)
                        if let Some(network) = self_network {
                            if let Err(e) = sync_self_aliases(self, router, network).await {
                                tracing::warn!("self alias sync failed: {}", e);
                            }
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

/// 管理対象コンテナの dormant.host ホスト名を、dormant 自身のネットワークエイリアスとして
/// `docker network connect --alias` で付与する(冪等)。
///
/// - 自身のコンテナIDは `/etc/hostname` から解決(`resolve_self_networks` と同手法)
/// - 対象ネットワークに未接続なら、未付与の全ホストをまとめて1回の connect で接続する
/// - 接続済みだが一部ホストが未付与の場合、現在のエイリアス + 未付与ホストをマージし、
///   disconnect → マージ済みエイリアスで connect して動的に追加する
///   (bollard の connect_network は接続済みエンドポイントへの新規 alias 追加が効かないため)
/// - connect が競合(他コンテナが alias を占有)などで失敗しても warn ログを出して続行する
/// - エイリアス未指定のルートも `--alias` に使う(host:port 形式は host のみ)
pub async fn sync_self_aliases(
    docker: &DockerClient,
    router: &Router,
    network: &str,
) -> Result<()> {
    // 管理対象の dormant.host ホスト名を収集(空なら何もしない)
    let hosts = collect_route_hosts(router).await;
    if hosts.is_empty() {
        tracing::debug!("self alias sync: no dormant.host routes, nothing to do");
        return Ok(());
    }

    // 自身のコンテナIDを解決(未解決なら何もしない)
    let self_id = match docker.resolve_self_id().await {
        Some(id) => id,
        None => {
            tracing::warn!(
                "self alias sync: cannot resolve own container id (/etc/hostname), skipping"
            );
            return Ok(());
        }
    };

    // 自身の対象ネットワーク上の現在のエイリアスを取得(冪等性チェック用)
    // 取得エラーは warn して安全側に抜ける
    let attached = match docker.current_network_aliases(&self_id, network).await {
        Ok(aliases) => aliases,
        Err(e) => {
            tracing::warn!("self alias sync: inspect self failed: {}", e);
            return Ok(());
        }
    };

    // すでに付与済みのホストを除いた未付与ホストを計算
    let existing: HashSet<&str> = attached
        .as_ref()
        .map(|aliases| aliases.iter().map(|s| s.as_str()).collect())
        .unwrap_or_default();
    let missing: Vec<String> = hosts
        .iter()
        .filter(|h| !existing.contains(h.as_str()))
        .cloned()
        .collect();

    // 未接続なら、未付与ホストをエイリアスとして1回の connect で接続する
    // (missing が空 = 接続すべきホストが無いなら何もしない)
    let Some(current) = attached else {
        if missing.is_empty() {
            tracing::debug!("self alias sync: not connected and no hosts to add");
            return Ok(());
        }
        tracing::info!(
            "connecting self to network '{}' with aliases: {}",
            network,
            missing.join(", ")
        );
        // 競合(他コンテナが alias を占有)を含む connect エラーは warn を出して続行する
        if let Err(e) = docker.connect_with_aliases(network, &self_id, &missing).await {
            tracing::warn!(
                "self alias sync: connect to network '{}' with aliases [{}] failed: {}",
                network,
                missing.join(", "),
                e
            );
        }
        return Ok(());
    };

    // 接続済み: 現在エイリアスから余剰(管理対象ホストでも自身のコンテナ名でもない)を除去し、
    // 未付与ホストを追加したマージを作る。変更がある場合のみ disconnect → connect で反映する
    // (bollard の connect_network は接続済みエンドポイントへの新規 alias 追加/削除が効かないため、
    //  disconnect→connect 方式を使う)
    // 削除の検出: 現在エイリアスのうち、いずれにも該当しないもの(余剰)を除外する
    //   - 管理対象の dormant.host ホスト
    //   - dormant 自身のコンテナ名 / 短縮名(切断時に自分を失わないため保護)
    let self_name = docker.resolve_self_name().await;
    let mut merged: Vec<String> = current
        .iter()
        .filter(|a| {
            if hosts.contains(*a) {
                return true;
            }
            // 自身のコンテナ名(長・短両方)は保護
            if let Some(n) = &self_name {
                if a.as_str() == n || a.as_str() == n.split('.').next().unwrap_or(n.as_str()) {
                    return true;
                }
            }
            false
        })
        .cloned()
        .collect();
    // 未付与ホストを追加
    for h in &missing {
        if !merged.contains(h) {
            merged.push(h.clone());
        }
    }
    // 決定的な順序にするためソート
    merged.sort();

    // 現在エイリアス(ソート済み)との差分を判定し、変更が無ければ何もしない(冪等)
    let mut current_sorted = current;
    current_sorted.sort();
    if merged == current_sorted {
        tracing::info!(
            "self aliases up to date on network '{}' ({} hosts)",
            network,
            hosts.len()
        );
        return Ok(());
    }

    tracing::info!(
        "self aliases on network '{}': {} missing, {} current -> merged: {}",
        network,
        missing.len(),
        current_sorted.len(),
        merged.join(", ")
    );
    if let Err(e) = docker.disconnect_from_network(network, &self_id).await {
        tracing::warn!(
            "self alias sync: disconnect from network '{}' failed: {}",
            network,
            e
        );
        return Ok(());
    }
    // 競合(他コンテナが alias を占有)を含む connect エラーは warn を出して続行(握りつぶし)
    if let Err(e) = docker.connect_with_aliases(network, &self_id, &merged).await {
        tracing::warn!(
            "self alias sync: connect to network '{}' with aliases [{}] failed: {}",
            network,
            merged.join(", "),
            e
        );
    }
    Ok(())
}

/// 管理対象コンテナの dormant.host ホスト名と dormant.alias を収集する(重複排除・ソート済み)
async fn collect_route_hosts(router: &Router) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();
    for c in router.containers().await {
        // dormant.host ルート
        for r in &c.routes {
            if !r.host.is_empty() && seen.insert(r.host.clone()) {
                hosts.push(r.host.clone());
            }
        }
        // dormant.alias(ネットワークエイリアス専用)
        for a in &c.aliases {
            if !a.is_empty() && seen.insert(a.clone()) {
                hosts.push(a.clone());
            }
        }
    }
    // コンテナ列挙順(HashMap)に依存しないようソートして返す
    hosts.sort();
    hosts
}

/// コンテナ情報から管理対象を判定・パース
fn parse_container(c: &ContainerSummary) -> Option<ManagedContainer> {
    let labels = c.labels.as_ref()?;
    let enabled = labels
        .get(LABEL_ENABLE)
        .map(|v| v == "true")
        .unwrap_or(false);
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
            c.ports
                .as_ref()
                .and_then(|ports| ports.iter().map(|p| p.private_port).next())
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
    // dormant.host ラベル: `host[:port]` のカンマ区切り。port 省略時はデフォルトポートへ
    let routes = labels
        .get(LABEL_HOST)
        .map(|v| parse_routes(v))
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

    // dormant.tcp ラベル: `PORT` / `LISTEN_PORT:CONTAINER_PORT` のカンマ区切り(複数可)
    let tcp_expose = labels
        .get(LABEL_TCP)
        .map(|v| parse_tcp_exposes(v))
        .unwrap_or_default();

    // dormant.always-on ラベル: `true` で常時ON(アイドル停止・補助回収の対象外)
    let always_on = labels
        .get(LABEL_ALWAYS_ON)
        .map(|v| v == "true")
        .unwrap_or(false);

    Some(ManagedContainer {
        id,
        name,
        port,
        tcp_expose,
        group: labels.get(LABEL_GROUP).cloned(),
        session_duration,
        startup_timeout,
        healthcheck_path: labels.get(LABEL_HEALTHCHECK_PATH).cloned(),
        healthcheck_port,
        healthcheck_status,
        routes,
        aliases: parse_aliases(labels.get(LABEL_ALIAS).map(|s| s.as_str()).unwrap_or("")),
        ip: None,
        running: c.state == Some(ContainerSummaryStateEnum::RUNNING),
        created: c.created,
        depends_on,
        compose_project: labels.get(LABEL_COMPOSE_PROJECT).cloned(),
        compose_service: labels.get(LABEL_COMPOSE_SERVICE).cloned(),
        always_on,
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

/// dormant.host ラベルをパースする
/// 形式: `host` または `host:port` のカンマ区切り
fn parse_routes(v: &str) -> Vec<Route> {
    v.split(',').filter_map(|s| parse_route(s.trim())).collect()
}

/// dormant.alias ラベルをパースする
/// 形式: `host` のカンマ区切り(ホスト名のみ。ポート情報は扱わない)
fn parse_aliases(v: &str) -> Vec<String> {
    v.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// 単一のルーティングエントリをパースする
/// 形式: `host` または `host:port`
fn parse_route(s: &str) -> Option<Route> {
    if s.is_empty() {
        return None;
    }
    // host:port 形式(最後の ':' を区切りとして扱う。IPv6は非対応)
    match s.rsplit_once(':') {
        Some((host, port_str)) => match port_str.parse::<u16>() {
            Ok(port) if port != 0 => Some(Route {
                host: host.to_string(),
                port: Some(port),
            }),
            // ポート部分が数字でない場合は host 全体をドメインとして扱う
            _ => Some(Route {
                host: s.to_string(),
                port: None,
            }),
        },
        None => Some(Route {
            host: s.to_string(),
            port: None,
        }),
    }
}

/// dormant.tcp ラベルをパースする
/// 形式: `PORT` / `LISTEN_PORT:CONTAINER_PORT` のカンマ区切り(複数可)
fn parse_tcp_exposes(v: &str) -> Vec<TcpExpose> {
    v.split(',')
        .filter_map(|s| parse_tcp_expose(s.trim()))
        .collect()
}

/// 単一の dormant.tcp ラベル要素をパースする
/// 形式: `PORT` → listen もコンテナも同じポート
///       `LISTEN_PORT:CONTAINER_PORT` → 別ポートを指定
fn parse_tcp_expose(v: &str) -> Option<TcpExpose> {
    let v = v.trim();
    if v.is_empty() {
        return None;
    }
    match v.split_once(':') {
        // LISTEN_PORT:CONTAINER_PORT 形式
        Some((l, c)) => {
            let listen_port = l.trim().parse::<u16>().ok()?;
            let container_port = c.trim().parse::<u16>().ok()?;
            if listen_port == 0 || container_port == 0 {
                return None;
            }
            Some(TcpExpose {
                listen_port,
                container_port,
            })
        }
        // PORT 形式 (listen = コンテナ = 同一ポート)
        None => {
            let port = v.parse::<u16>().ok()?;
            if port == 0 {
                return None;
            }
            Some(TcpExpose {
                listen_port: port,
                container_port: port,
            })
        }
    }
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
    fn test_parse_tcp_expose() {
        // 単一ポート (listen = コンテナ = 同一)
        assert_eq!(
            parse_tcp_expose("6334"),
            Some(TcpExpose {
                listen_port: 6334,
                container_port: 6334,
            })
        );
        // 別ポート指定
        assert_eq!(
            parse_tcp_expose("6334:8000"),
            Some(TcpExpose {
                listen_port: 6334,
                container_port: 8000,
            })
        );
        // 空白許容
        assert_eq!(
            parse_tcp_expose(" 6334 : 9000 "),
            Some(TcpExpose {
                listen_port: 6334,
                container_port: 9000,
            })
        );
        // 不正な値は None
        assert_eq!(parse_tcp_expose("abc"), None);
        assert_eq!(parse_tcp_expose(""), None);
        assert_eq!(parse_tcp_expose("0"), None);
        assert_eq!(parse_tcp_expose("6334:0"), None);
        assert_eq!(parse_tcp_expose("99999"), None);
    }

    #[test]
    fn test_parse_tcp_exposes_multi() {
        // カンマ区切りで複数指定可能
        assert_eq!(
            parse_tcp_exposes("6334:9000,6379"),
            vec![
                TcpExpose {
                    listen_port: 6334,
                    container_port: 9000,
                },
                TcpExpose {
                    listen_port: 6379,
                    container_port: 6379,
                },
            ]
        );
        // 空要素は無視
        assert_eq!(
            parse_tcp_exposes("6334,,6379"),
            vec![
                TcpExpose {
                    listen_port: 6334,
                    container_port: 6334,
                },
                TcpExpose {
                    listen_port: 6379,
                    container_port: 6379,
                },
            ]
        );
        assert_eq!(parse_tcp_exposes(""), Vec::<TcpExpose>::new());
    }

    #[test]
    fn test_parse_route() {
        // host のみ (ポートなし = デフォルト)
        assert_eq!(
            parse_route("app.example.com"),
            Some(Route {
                host: "app.example.com".to_string(),
                port: None,
            })
        );
        // host:port 形式
        assert_eq!(
            parse_route("api.example.com:8080"),
            Some(Route {
                host: "api.example.com".to_string(),
                port: Some(8080),
            })
        );
        // 空は None
        assert_eq!(parse_route(""), None);
        // ポート部分が数字でない場合は host 全体として扱う
        assert_eq!(
            parse_route("host-with-colon:abc"),
            Some(Route {
                host: "host-with-colon:abc".to_string(),
                port: None,
            })
        );
    }

    #[test]
    fn test_parse_routes_multi() {
        // カンマ区切り + 一部のみポート付き
        assert_eq!(
            parse_routes("api.example.com:8080, web.example.com"),
            vec![
                Route {
                    host: "api.example.com".to_string(),
                    port: Some(8080),
                },
                Route {
                    host: "web.example.com".to_string(),
                    port: None,
                },
            ]
        );
        assert_eq!(parse_routes(""), Vec::<Route>::new());
    }

    #[test]
    fn test_parse_container_tcp_expose() {
        let mut labels = HashMap::new();
        labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        labels.insert(LABEL_TCP.to_string(), "6334:9000".to_string());
        let c = ContainerSummary {
            id: Some("abc123".to_string()),
            names: Some(vec!["/test-1".to_string()]),
            labels: Some(labels),
            ..Default::default()
        };
        let m = parse_container(&c).unwrap();
        assert_eq!(
            m.tcp_expose,
            vec![TcpExpose {
                listen_port: 6334,
                container_port: 9000,
            }]
        );
    }

    #[test]
    fn test_parse_container_host_and_healthcheck_status() {
        let mut labels = HashMap::new();
        labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        labels.insert("dormant.port".to_string(), "8080".to_string());
        labels.insert(
            LABEL_HOST.to_string(),
            "app.example.com, api.example.com".to_string(),
        );
        labels.insert(
            LABEL_HEALTHCHECK_STATUS.to_string(),
            "200,204,abc".to_string(),
        );
        let c = ContainerSummary {
            id: Some("abc123".to_string()),
            names: Some(vec!["/test-1".to_string()]),
            labels: Some(labels),
            ..Default::default()
        };
        let m = parse_container(&c).unwrap();
        assert_eq!(
            m.routes,
            vec![
                Route {
                    host: "app.example.com".to_string(),
                    port: None,
                },
                Route {
                    host: "api.example.com".to_string(),
                    port: None,
                },
            ]
        );
        assert_eq!(m.healthcheck_status, Some(vec![200, 204]));
    }

    #[test]
    fn test_parse_container_always_on() {
        // dormant.always-on=true → always_on=true
        let mut labels = HashMap::new();
        labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        labels.insert(LABEL_ALWAYS_ON.to_string(), "true".to_string());
        let c = ContainerSummary {
            id: Some("abc123".to_string()),
            names: Some(vec!["/test-1".to_string()]),
            labels: Some(labels),
            ..Default::default()
        };
        let m = parse_container(&c).unwrap();
        assert!(m.always_on, "always-on=true で always_on が立つ");

        // 未指定 → false
        let mut labels = HashMap::new();
        labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        let c = ContainerSummary {
            id: Some("abc123".to_string()),
            names: Some(vec!["/test-1".to_string()]),
            labels: Some(labels),
            ..Default::default()
        };
        let m = parse_container(&c).unwrap();
        assert!(!m.always_on, "未指定は false");

        // true 以外(例: "1") → false
        let mut labels = HashMap::new();
        labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        labels.insert(LABEL_ALWAYS_ON.to_string(), "1".to_string());
        let c = ContainerSummary {
            id: Some("abc123".to_string()),
            names: Some(vec!["/test-1".to_string()]),
            labels: Some(labels),
            ..Default::default()
        };
        let m = parse_container(&c).unwrap();
        assert!(!m.always_on, "true 以外は false");
    }

    #[test]
    fn test_parse_aliases() {
        // ホスト名のみのカンマ区切り
        assert_eq!(
            parse_aliases("myredis.local, mydb.local"),
            vec!["myredis.local".to_string(), "mydb.local".to_string()]
        );
        // 空要素は除外・trim
        assert_eq!(
            parse_aliases(" a.local ,, b.local "),
            vec!["a.local".to_string(), "b.local".to_string()]
        );
        // 空入力は空
        assert_eq!(parse_aliases(""), Vec::<String>::new());
        assert_eq!(parse_aliases("   "), Vec::<String>::new());
    }

    #[test]
    fn test_parse_container_aliases() {
        // dormant.alias は dormant.host とは独立にパースされる
        let mut labels = HashMap::new();
        labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        labels.insert(LABEL_HOST.to_string(), "app.example.com".to_string());
        labels.insert(LABEL_ALIAS.to_string(), "myredis.local, mydb.local".to_string());
        let c = ContainerSummary {
            id: Some("abc123".to_string()),
            names: Some(vec!["/test-1".to_string()]),
            labels: Some(labels),
            ..Default::default()
        };
        let m = parse_container(&c).unwrap();
        // dormant.host は routes にのみ
        assert_eq!(
            m.routes,
            vec![Route {
                host: "app.example.com".to_string(),
                port: None,
            }]
        );
        // dormant.alias は aliases にのみ(HTTP ルーティングには載らない)
        assert_eq!(
            m.aliases,
            vec!["myredis.local".to_string(), "mydb.local".to_string()]
        );
    }

    #[tokio::test]
    async fn test_collect_route_hosts_includes_aliases() {
        use crate::testutil::make_container;
        // dormant.host ルート + dormant.alias の両方を収集し、重複排除・ソートされる
        let mut c = make_container("app", None);
        c.routes = vec![
            Route {
                host: "shared.example.com".to_string(),
                port: None,
            },
            Route {
                host: "app.example.com".to_string(),
                port: None,
            },
        ];
        c.aliases = vec![
            "shared.example.com".to_string(),
            "myredis.local".to_string(),
        ];
        let router = Router::new();
        router.update(vec![c]).await;
        let hosts = collect_route_hosts(&router).await;
        assert_eq!(
            hosts,
            vec![
                "app.example.com".to_string(),
                "myredis.local".to_string(),
                "shared.example.com".to_string(),
            ]
        );
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
            tcp_expose: Vec::new(),
            group: None,
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            routes: Vec::new(),
            aliases: Vec::new(),
            ip: None,
            running: false,
            created: None,
            depends_on: Vec::new(),
            compose_project: Some("proj".to_string()),
            compose_service: Some("db".to_string()),
            always_on: false,
        };
        let app = ManagedContainer {
            id: "id-app".to_string(),
            name: "app-1".to_string(),
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
            always_on: false,
        };
        let other = ManagedContainer {
            id: "id-other".to_string(),
            name: "other-1".to_string(),
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
            ip: None,
            running: false,
            created: None,
            depends_on: Vec::new(),
            compose_project: Some("other-proj".to_string()),
            compose_service: Some("db".to_string()),
            always_on: false,
        };
        let cache = ManagedContainer {
            id: "id-cache".to_string(),
            name: "cache-1".to_string(),
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
            ip: None,
            running: false,
            created: None,
            depends_on: Vec::new(),
            compose_project: Some("proj".to_string()),
            compose_service: Some("cache".to_string()),
            always_on: false,
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
            tcp_expose: Vec::new(),
            group: None,
            session_duration: Duration::from_secs(3600),
            startup_timeout: Duration::from_secs(180),
            healthcheck_path: None,
            healthcheck_port: None,
            healthcheck_status: None,
            routes: Vec::new(),
            aliases: Vec::new(),
            ip: None,
            running: false,
            created: None,
            depends_on: vec![Dependency {
                service: "nonexistent".to_string(),
                condition: "service_started".to_string(),
            }],
            compose_project: Some("proj".to_string()),
            compose_service: Some("app2".to_string()),
            always_on: false,
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

        // フォールバックでいずれかのネットワークのIPが返る(HashMap の順序は非決定的)
        let ip = docker.resolve_ip("app").await.unwrap();
        assert!(
            ip == "172.21.0.2" || ip == "172.22.0.3",
            "unexpected fallback IP: {}",
            ip
        );
    }

    // ---- 自身のネットワークエイリアス付与 (sync_self_aliases) ----

    /// 指定ホスト名を dormant.host ルートに持つルーターを作る
    async fn router_with_hosts(hosts: &[&str]) -> Router {
        use crate::testutil::make_container;
        let mut c = make_container("app", None);
        c.routes = hosts
            .iter()
            .map(|h| Route {
                host: h.to_string(),
                port: None,
            })
            .collect();
        let router = Router::new();
        router.update(vec![c]).await;
        router
    }

    // 未接続のネットワークに、未付与のホストがエイリアスとして1回の connect で付与される
    #[tokio::test]
    async fn test_sync_self_aliases_connects_with_missing_hosts() {
        use crate::testutil::{setup_mock_docker, MockContainer};

        // 自身は対象ネットワーク(global)に未接続
        let self_c = MockContainer::new("self", "172.30.0.5", 80);
        let (docker, mock) = setup_mock_docker(vec![self_c]).await;
        docker.set_self_id(Some("self".to_string())).await;

        let router = router_with_hosts(&["app.example.com", "api.example.com"]).await;

        sync_self_aliases(&docker, &router, "global").await.unwrap();

        // connect が1回、両ホストのエイリアス付きで呼ばれる(ホストはソート順)
        let calls = mock.connect_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "global");
        assert_eq!(calls[0].1, "self");
        assert_eq!(calls[0].2, vec!["api.example.com", "app.example.com"]);
    }

    // dormant.alias のみのコンテナでも、そのホスト名がエイリアスとして付与される
    #[tokio::test]
    async fn test_sync_self_aliases_from_alias_label() {
        use crate::testutil::{make_container, setup_mock_docker, MockContainer};

        // TCP 専用コンテナ(dormant.host なし)で dormant.alias のみを指定
        let mut c = make_container("redis", None);
        c.routes = Vec::new(); // HTTP ルーティングなし
        c.aliases = vec!["myredis.local".to_string()];
        let router = Router::new();
        router.update(vec![c]).await;

        let self_c = MockContainer::new("self", "172.30.0.5", 80);
        let (docker, mock) = setup_mock_docker(vec![self_c]).await;
        docker.set_self_id(Some("self".to_string())).await;

        sync_self_aliases(&docker, &router, "global").await.unwrap();

        let calls = mock.connect_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].2, vec!["myredis.local"]);
    }

    // すでに全ホストが付与済みなら connect を呼ばない(冪等)
    #[tokio::test]
    async fn test_sync_self_aliases_idempotent() {
        use crate::testutil::{setup_mock_docker, MockContainer};

        let mut self_c = MockContainer::new("self", "172.30.0.5", 80);
        self_c.networks = vec![("global".to_string(), "172.30.0.5".to_string())];
        self_c.aliases = vec![(
            "global".to_string(),
            vec!["app.example.com".to_string(), "api.example.com".to_string()],
        )];
        let (docker, mock) = setup_mock_docker(vec![self_c]).await;
        docker.set_self_id(Some("self".to_string())).await;

        let router = router_with_hosts(&["app.example.com", "api.example.com"]).await;

        sync_self_aliases(&docker, &router, "global").await.unwrap();

        assert!(mock.connect_calls().is_empty());
    }

    // 接続済みで一部ホストが未付与の場合は、現在エイリアス + 未付与ホストをマージし、
    // disconnect → connect で動的に追加される
    #[tokio::test]
    async fn test_sync_self_aliases_partial_reconnects_with_merged() {
        use crate::testutil::{setup_mock_docker, MockContainer};

        let mut self_c = MockContainer::new("self", "172.30.0.5", 80);
        self_c.networks = vec![("global".to_string(), "172.30.0.5".to_string())];
        self_c.aliases = vec![("global".to_string(), vec!["app.example.com".to_string()])];
        let (docker, mock) = setup_mock_docker(vec![self_c]).await;
        docker.set_self_id(Some("self".to_string())).await;

        let router = router_with_hosts(&["app.example.com", "api.example.com"]).await;

        // エラーにせず、disconnect→connect(マージ済みエイリアス)で同期する
        sync_self_aliases(&docker, &router, "global").await.unwrap();

        // 呼び出し順: 1. disconnect, 2. connect(マージ済み)
        let disconnects = mock.disconnect_calls();
        assert_eq!(disconnects.len(), 1);
        assert_eq!(disconnects[0].0, "global");
        assert_eq!(disconnects[0].1, "self");
        let connects = mock.connect_calls();
        assert_eq!(connects.len(), 1);
        assert_eq!(connects[0].0, "global");
        assert_eq!(connects[0].1, "self");
        // 現在エイリアス + missing がマージされている(ソート順)
        assert_eq!(connects[0].2, vec!["api.example.com", "app.example.com"]);
    }

    // 接続済みで未付与ホストがあるが connect が競合(他コンテナが alias 占有)で失敗する場合は
    // warn を出して握りつぶし、エラーにはしない
    #[tokio::test]
    async fn test_sync_self_aliases_connect_conflict_swallowed() {
        use crate::testutil::{setup_mock_docker, MockContainer};

        let mut self_c = MockContainer::new("self", "172.30.0.5", 80);
        self_c.networks = vec![("global".to_string(), "172.30.0.5".to_string())];
        self_c.aliases = vec![("global".to_string(), vec!["app.example.com".to_string()])];
        // connect を失敗させる(alias 競合の再現)
        self_c.connect_fails = true;
        let (docker, mock) = setup_mock_docker(vec![self_c]).await;
        docker.set_self_id(Some("self".to_string())).await;

        let router = router_with_hosts(&["app.example.com", "api.example.com"]).await;

        // エラーにはせず続行(warn ログで握りつぶし)
        sync_self_aliases(&docker, &router, "global").await.unwrap();

        // disconnect は呼ばれ、connect は失敗したが1回試行されている
        assert_eq!(mock.disconnect_calls().len(), 1);
        assert_eq!(mock.connect_calls().len(), 1);
    }

    // 管理対象に dormant.host が無ければ何もしない
    #[tokio::test]
    async fn test_sync_self_aliases_no_routes() {
        use crate::testutil::{setup_mock_docker, MockContainer};

        let self_c = MockContainer::new("self", "172.30.0.5", 80);
        let (docker, mock) = setup_mock_docker(vec![self_c]).await;
        docker.set_self_id(Some("self".to_string())).await;

        let router = Router::new();
        sync_self_aliases(&docker, &router, "global").await.unwrap();

        assert!(mock.connect_calls().is_empty());
    }

    // 自身のコンテナIDが解決できない場合は何もしない
    #[tokio::test]
    async fn test_sync_self_aliases_cannot_resolve_self() {
        use crate::testutil::{setup_mock_docker, MockContainer};

        let self_c = MockContainer::new("self", "172.30.0.5", 80);
        let (docker, mock) = setup_mock_docker(vec![self_c]).await;
        // self_id を設定しない → /etc/hostname 解決が失敗し何もしない

        let router = router_with_hosts(&["app.example.com"]).await;
        sync_self_aliases(&docker, &router, "global").await.unwrap();

        assert!(mock.connect_calls().is_empty());
    }

    // ホスト名収集: 重複は排除され、dormant.host ルートのみが対象
    #[tokio::test]
    async fn test_collect_route_hosts_dedup() {
        use crate::testutil::make_container;

        let mut a = make_container("a", None);
        a.routes = vec![
            Route {
                host: "app.example.com".to_string(),
                port: None,
            },
            Route {
                host: "shared.example.com".to_string(),
                port: Some(8080),
            },
        ];
        let mut b = make_container("b", None);
        b.routes = vec![Route {
            host: "shared.example.com".to_string(),
            port: None,
        }];
        let router = Router::new();
        router.update(vec![a, b]).await;

        let hosts = collect_route_hosts(&router).await;
        assert_eq!(hosts, vec!["app.example.com", "shared.example.com"]);
    }
}
