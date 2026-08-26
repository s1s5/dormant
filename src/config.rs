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

/// 静的ルート1エントリ(DORMANT_STATIC_ROUTES 由来)
/// 形式: `ホストパターン=IP:ポート`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticRouteEntry {
    /// ホストパターン(`*.example.com` はワイルドカード、`api.example.com` は完全一致)
    pub pattern: String,
    /// 転送先IPアドレス(外部固定宛先。dormant は起動・停止しない)
    pub ip: String,
    /// 転送先ポート
    pub port: u16,
}

/// DORMANT_STATIC_ROUTES をパースする。
/// `ホストパターン=IP:ポート` の並びを、カンマ(,)と改行(\n)の**両方**を区切りとして解釈する。
/// 空行・空要素は無視し、形式不正な要素はスキップする。
pub fn parse_static_routes(v: &str) -> Vec<StaticRouteEntry> {
    v.split([',', '\n'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(parse_static_route)
        .collect()
}

/// 単一の静的ルート要素をパースする。形式: `パターン=IP:ポート`
/// ホストと宛先の区切りは `=`(IPv6 や host:port との混同を避けるため)
fn parse_static_route(s: &str) -> Option<StaticRouteEntry> {
    let (pattern, target) = s.split_once('=')?;
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return None;
    }
    // IP:ポート は最後の ':' を区切りとして扱う(IPv6は非対応)
    let (ip, port_str) = target.rsplit_once(':')?;
    let ip = ip.trim();
    let port = port_str.trim().parse::<u16>().ok()?;
    if ip.is_empty() || port == 0 {
        return None;
    }
    Some(StaticRouteEntry {
        pattern: pattern.to_string(),
        ip: ip.to_string(),
        port,
    })
}

/// 設定
#[derive(Debug, Clone)]
pub struct Config {
    /// 待ち受けポート
    pub listen: String,
    /// Dockerソケットのパス
    pub docker_socket: String,
    /// アイドルチェックの間隔(秒)
    pub idle_check_interval_secs: u64,
    /// 静的ルート(dormant が管理しない外部固定宛先)
    pub static_routes: Vec<StaticRouteEntry>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:80".to_string(),
            docker_socket: "/var/run/docker.sock".to_string(),
            idle_check_interval_secs: 30,
            static_routes: Vec::new(),
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
            static_routes: parse_static_routes(&args.static_routes),
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
            static_routes: String::new(),
        };
        let cfg = Config::from_args(&args);
        assert_eq!(cfg.listen, "0.0.0.0:8080");
        assert_eq!(cfg.docker_socket, "/tmp/docker.sock");
        assert_eq!(cfg.idle_check_interval_secs, 10);
        assert!(cfg.static_routes.is_empty());
    }

    #[test]
    fn test_from_args_static_routes() {
        let args = Args {
            listen: "0.0.0.0:8080".to_string(),
            docker_socket: "/tmp/docker.sock".to_string(),
            idle_check_interval_secs: 10,
            self_network: String::new(),
            static_routes: "*.example.com=203.0.113.10:8080".to_string(),
        };
        let cfg = Config::from_args(&args);
        assert_eq!(cfg.static_routes.len(), 1);
        assert_eq!(cfg.static_routes[0].pattern, "*.example.com");
        assert_eq!(cfg.static_routes[0].ip, "203.0.113.10");
        assert_eq!(cfg.static_routes[0].port, 8080);
    }

    #[test]
    fn test_parse_static_route() {
        // 完全一致
        assert_eq!(
            parse_static_route("api.example.com=203.0.113.11:8443"),
            Some(StaticRouteEntry {
                pattern: "api.example.com".to_string(),
                ip: "203.0.113.11".to_string(),
                port: 8443,
            })
        );
        // ワイルドカード
        assert_eq!(
            parse_static_route("*.example.com=203.0.113.10:8080"),
            Some(StaticRouteEntry {
                pattern: "*.example.com".to_string(),
                ip: "203.0.113.10".to_string(),
                port: 8080,
            })
        );
        // 空白許容
        assert_eq!(
            parse_static_route(" *.example.com = 203.0.113.10 : 8080 "),
            Some(StaticRouteEntry {
                pattern: "*.example.com".to_string(),
                ip: "203.0.113.10".to_string(),
                port: 8080,
            })
        );
        // 形式不正は None
        assert_eq!(parse_static_route(""), None);
        assert_eq!(parse_static_route("api.example.com"), None); // 宛先なし
        assert_eq!(parse_static_route("=1.2.3.4:80"), None); // パターンなし
        assert_eq!(parse_static_route("api.example.com=1.2.3.4"), None); // ポートなし
        assert_eq!(parse_static_route("api.example.com=1.2.3.4:0"), None); // ポート0
        assert_eq!(parse_static_route("api.example.com=1.2.3.4:99999"), None); // ポート範囲外
    }

    #[test]
    fn test_parse_static_routes_comma_and_newline() {
        // カンマ区切りと改行区切りの両方が解釈される
        let routes = parse_static_routes(
            "*.example.com=203.0.113.10:8080,api.example.com=203.0.113.11:8443\n\
             web.example.com=203.0.113.12:80",
        );
        assert_eq!(routes.len(), 3);
        assert_eq!(routes[0].pattern, "*.example.com");
        assert_eq!(routes[0].ip, "203.0.113.10");
        assert_eq!(routes[0].port, 8080);
        assert_eq!(routes[1].pattern, "api.example.com");
        assert_eq!(routes[1].port, 8443);
        assert_eq!(routes[2].pattern, "web.example.com");
        assert_eq!(routes[2].port, 80);
    }

    #[test]
    fn test_parse_static_routes_empty_and_invalid_skipped() {
        assert_eq!(parse_static_routes(""), Vec::<StaticRouteEntry>::new());
        assert_eq!(parse_static_routes("   \n , "), Vec::<StaticRouteEntry>::new());
        // 不正な要素はスキップされ、正しいものだけ残る
        let routes = parse_static_routes("bad,ok.example.com=1.2.3.4:80\nalso_bad");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].pattern, "ok.example.com");
        assert_eq!(routes[0].ip, "1.2.3.4");
        assert_eq!(routes[0].port, 80);
    }
}
