//! dormant: Docker scale-to-zero リバースプロキシ
//!
//! ラベル dormant.enable=true のコンテナをオンデマンド起動し、
//! アイドル時に停止する。

mod config;
mod docker;
mod lifecycle;
mod proxy;
mod router;
mod tcp;

#[cfg(test)]
mod testutil;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
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
    let config_server = config.clone();
    let docker_server = docker.clone();
    let router_server = router.clone();
    let sessions_server = sessions.clone();
    let server_task = tokio::spawn(async move {
        proxy::serve(
            &config_server,
            docker_server,
            router_server,
            sessions_server,
        )
        .await
    });

    // TCP転送サーバー起動 (dormant.tcp ラベルのポートを待ち受け)
    let config_tcp = config.clone();
    let docker_tcp = docker.clone();
    let router_tcp = router.clone();
    let sessions_tcp = sessions.clone();
    let tcp_task = tokio::spawn(async move {
        tcp::serve_tcp(&config_tcp, router_tcp, docker_tcp, sessions_tcp).await
    });

    // Ctrl+C / SIGTERM を待って graceful shutdown
    let shutdown = wait_for_shutdown_signal();

    tokio::select! {
        result = server_task => {
            result??;
        }
        result = tcp_task => {
            result??;
        }
        _ = shutdown => {
            tracing::info!("shutdown signal received, stopping tasks");
        }
    }

    // バックグラウンドタスクを停止
    event_task.abort();
    idle_task.abort();
    let _ = event_task.await;
    let _ = idle_task.await;
    tracing::info!("dormant stopped");
    Ok(())
}

/// Ctrl+C または SIGTERM を待つ
async fn wait_for_shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    let sigterm_fut = async {
        sigterm.recv().await;
    };
    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("received SIGINT (Ctrl+C)");
        }
        _ = sigterm_fut => {
            tracing::info!("received SIGTERM");
        }
    }
}
