//! dormant: Docker scale-to-zero リバースプロキシ
//!
//! ラベル dormant.enable=true のコンテナをオンデマンド起動し、
//! アイドル時に停止する。

mod config;
mod docker;
mod lifecycle;
mod proxy;
mod router;

#[cfg(test)]
mod testutil;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

/// コマンドライン引数
#[derive(Parser, Debug)]
#[command(name = "dormant", version, about = "Docker scale-to-zero reverse proxy")]
struct Args {
    /// 設定ファイルのパス
    #[arg(short, long, default_value = "dormant.yml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    // ログ初期化
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let config = config::Config::load(&args.config)?;
    tracing::info!("dormant starting: listen={}", config.listen);

    // Dockerクライアント初期化
    let docker = docker::DockerClient::new(&config.docker_socket)?;

    // ルーター初期化(コンテナ一覧からラベルを収集)
    let router = Arc::new(router::Router::new());
    docker::sync_routes(&docker, &router).await?;
    tracing::info!("initial route sync done");

    // セッション管理(proxyのtouchとidle_loopのexpired判定で共有)
    let sessions = lifecycle::Sessions::new();

    // Dockerイベント監視タスク
    let docker_watch = docker.clone();
    let router_watch = router.clone();
    let event_task = tokio::spawn(async move {
        docker_watch.watch_events(&router_watch).await;
    });

    // アイドル停止タスク
    let docker_idle = docker.clone();
    let router_idle = router.clone();
    let sessions_idle = sessions.clone();
    let idle_task = tokio::spawn(async move {
        lifecycle::idle_loop(
            &docker_idle,
            &router_idle,
            sessions_idle,
            config.idle_check_interval_secs,
        )
        .await;
    });

    // HTTPサーバー起動
    proxy::serve(&config, docker.clone(), router.clone(), sessions).await?;

    let _ = event_task.await;
    let _ = idle_task.await;
    Ok(())
}
