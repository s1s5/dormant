//! 設定とDockerラベル定数

use crate::Args;

/// ラベルキー
pub const LABEL_ENABLE: &str = "dormant.enable";
pub const LABEL_GROUP: &str = "dormant.group";
pub const LABEL_SESSION_DURATION: &str = "dormant.session-duration";
pub const LABEL_STARTUP_TIMEOUT: &str = "dormant.startup.timeout";
pub const LABEL_HEALTHCHECK_PATH: &str = "dormant.healthcheck.path";
pub const LABEL_HEALTHCHECK_PORT: &str = "dormant.healthcheck.port";
pub const LABEL_HEALTHCHECK_STATUS: &str = "dormant.healthcheck.status";
pub const LABEL_HOST: &str = "dormant.host";
/// ネットワークエイリアス専用ラベル。dormant 自身のネットワークエイリアスにのみ使い、
/// HTTP ルーティング表には載せない。カンマ区切りで複数指定可(ホスト名のみ)
pub const LABEL_ALIAS: &str = "dormant.alias";
/// TCP転送ラベル。形式: `PORT` または `LISTEN_PORT:CONTAINER_PORT`
pub const LABEL_TCP: &str = "dormant.tcp";

/// compose が付与するラベル
pub const LABEL_COMPOSE_DEPENDS_ON: &str = "com.docker.compose.depends_on";
pub const LABEL_COMPOSE_PROJECT: &str = "com.docker.compose.project";
pub const LABEL_COMPOSE_SERVICE: &str = "com.docker.compose.service";

/// 依存解決の深さ制限(循環参照対策)
pub const MAX_DEPENDENCY_DEPTH: usize = 10;

/// デフォルト値
pub const DEFAULT_SESSION_DURATION: &str = "1h";
pub const DEFAULT_STARTUP_TIMEOUT: &str = "3m";

/// 設定
#[derive(Debug, Clone)]
pub struct Config {
    /// 待ち受けポート
    pub listen: String,
    /// Dockerソケットのパス
    pub docker_socket: String,
    /// アイドルチェックの間隔(秒)
    pub idle_check_interval_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:80".to_string(),
            docker_socket: "/var/run/docker.sock".to_string(),
            idle_check_interval_secs: 30,
        }
    }
}

impl Config {
    /// clap引数(環境変数で上書き可)から設定を構築する
    pub fn from_args(args: &Args) -> Self {
        Self {
            listen: args.listen.clone(),
            docker_socket: args.docker_socket.clone(),
            idle_check_interval_secs: args.idle_check_interval_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.listen, "0.0.0.0:80");
        assert_eq!(cfg.docker_socket, "/var/run/docker.sock");
    }

    #[test]
    fn test_from_args() {
        let args = Args {
            listen: "0.0.0.0:8080".to_string(),
            docker_socket: "/tmp/docker.sock".to_string(),
            idle_check_interval_secs: 10,
            self_network: String::new(),
        };
        let cfg = Config::from_args(&args);
        assert_eq!(cfg.listen, "0.0.0.0:8080");
        assert_eq!(cfg.docker_socket, "/tmp/docker.sock");
        assert_eq!(cfg.idle_check_interval_secs, 10);
    }
}
