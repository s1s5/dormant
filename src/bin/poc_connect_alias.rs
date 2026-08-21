//! 検証: 接続済みコンテナのエイリアスを動的に更新できるか
//!
//! 方式(ボス指定):
//!   1. 現在設定されているエイリアスを取得
//!   2. 新しいエイリアスを既存リストにマージ
//!   3. Disconnect を呼ぶ
//!   4. 更新したエイリアスリストで Connect を呼ぶ
//!
//! 使い方: cargo run --example connect_alias_test
use anyhow::Result;
use bollard::Docker;
use bollard::models::NetworkConnectRequest;
use bollard::query_parameters::{InspectContainerOptions};

const NET: &str = "alias-test-net";
const ID_A: &str = "alias-test-a";

/// コンテナの指定ネットワーク上の現在のエイリアスを取得
async fn current_aliases(docker: &Docker, id: &str, net: &str) -> Result<Vec<String>> {
    let insp = docker
        .inspect_container(id, None::<InspectContainerOptions>)
        .await?;
    let mut nets = insp.network_settings.and_then(|ns| ns.networks);
    let aliases = nets
        .as_mut()
        .and_then(|m| m.remove(net))
        .map(|ep| ep.aliases.unwrap_or_default())
        .unwrap_or_default();
    Ok(aliases)
}

#[tokio::main]
async fn main() -> Result<()> {
    let docker = Docker::connect_with_unix(
        "/run/user/1000/docker.sock",
        120,
        bollard::API_DEFAULT_VERSION,
    )?;
    println!("== Docker connect OK ==");

    // 現在のエイリアスを確認
    println!("--- 現在のエイリアス ---");
    let current = current_aliases(&docker, ID_A, NET).await?;
    println!("current aliases on {NET}: {:?}", current);

    // 初期状態が空なら alpha を付けておく(検証の前提)
    if current.is_empty() {
        println!("(未接続 or エイリアスなし → alpha.local で接続)");
        docker
            .connect_network(
                NET,
                NetworkConnectRequest {
                    container: ID_A.to_string(),
                    endpoint_config: Some(bollard::models::EndpointSettings {
                        aliases: Some(vec!["alpha.local".to_string()]),
                        ..Default::default()
                    }),
                },
            )
            .await?;
        println!("connected with alpha.local");
    }

    let before = current_aliases(&docker, ID_A, NET).await?;
    println!("before: {:?}", before);

    // 1. 新しいエイリアスを既存にマージ
    let mut merged = before.clone();
    let new_alias = "beta.local".to_string();
    if !merged.contains(&new_alias) {
        merged.push(new_alias.clone());
    }
    println!("merged: {:?}", merged);

    // 2. Disconnect
    println!("\n--- Disconnect ---");
    docker
        .disconnect_network(
            NET,
            bollard::models::NetworkDisconnectRequest {
                container: ID_A.to_string(),
                force: Some(true),
            },
        )
        .await?;
    println!("disconnected");

    // 3. マージ済みエイリアスで Connect
    println!("\n--- Connect (merged aliases) ---");
    docker
        .connect_network(
            NET,
            NetworkConnectRequest {
                container: ID_A.to_string(),
                endpoint_config: Some(bollard::models::EndpointSettings {
                    aliases: Some(merged.clone()),
                    ..Default::default()
                }),
            },
        )
        .await?;
    println!("connected with merged aliases");

    // 最終確認
    println!("\n--- 最終状態 ---");
    let after = current_aliases(&docker, ID_A, NET).await?;
    println!("after: {:?}", after);

    if after.contains(&"beta.local".to_string()) {
        println!("✅ disconnect→connect 方式で alias 追加(マージ)が成功した");
    } else {
        println!("⚠️  beta.local が付与されていない");
    }

    Ok(())
}
