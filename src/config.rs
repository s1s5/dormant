//! 設定ファイルの解析とDockerラベル定数

use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// dormant のラベルプレフィックス
pub const LABEL_PREFIX: &str = "dormant.";

/// ラベルキー
pub const LABEL_ENABLE: &str = "dormant.enable";
pub const LABEL_GROUP: &str = "dormant.group";
pub const LABEL_SESSION_DURATION: &str = "dormant.session-duration";
pub const LABEL_STARTUP_TIMEOUT: &str = "dormant.startup.timeout";
pub const LABEL_HEALTHCHECK_PATH: &str = "dormant.healthcheck.path";
pub const LABEL_HEALTHCHECK_PORT: &str = "dormant.healthcheck.port";
pub const LABEL_HEALTHCHECK_STATUS: &str = "dormant.healthcheck.status";
pub const LABEL_HOST: &str = "dormant.host";

/// compose が付与するラベル
pub const LABEL_COMPOSE_DEPENDS_ON: &str = "com.docker.compose.depends_on";
pub const LABEL_COMPOSE_PROJECT: &str = "com.docker.compose.project";
pub const LABEL_COMPOSE_SERVICE: &str = "com.docker.compose.service";

/// 依存解決の深さ制限(循環参照対策)
pub const MAX_DEPENDENCY_DEPTH: usize = 10;

/// デフォルト値
pub const DEFAULT_SESSION_DURATION: &str = "1h";
pub const DEFAULT_STARTUP_TIMEOUT: &str = "3m";
pub const DEFAULT_HEALTHCHECK_INTERVAL: u64 = 2; // 秒

/// 設定ファイル(dormant.yml)
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
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
    /// YAMLファイルから設定を読み込む。ファイルが無ければデフォルト
    pub fn load(path: &Path) -> Result<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            let cfg: Config = serde_yaml::from_str(&content)?;
            Ok(cfg)
        } else {
            tracing::warn!("config file not found: {:?}, using defaults", path);
            Ok(Config::default())
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
    fn test_load_config_from_yaml() {
        let yaml = r#"
listen: "0.0.0.0:8080"
docker_socket: "/tmp/docker.sock"
idle_check_interval_secs: 10
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:8080");
        assert_eq!(cfg.docker_socket, "/tmp/docker.sock");
        assert_eq!(cfg.idle_check_interval_secs, 10);
    }

    #[test]
    fn test_partial_config_defaults() {
        // 一部だけ指定しても残りはデフォルト
        let yaml = "listen: \"0.0.0.0:9999\"\n";
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:9999");
        assert_eq!(cfg.docker_socket, "/var/run/docker.sock");
    }
}
