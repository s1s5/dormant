# dormant

Docker **scale-to-zero リバースプロキシ**。`dormant.enable=true` ラベルが付いたコンテナを、リクエストが来たときにオンデマンドで起動し、アイドル状態が続くと自動で停止します。コンテナの台数を減らしてリソースを節約したいときに使います。

## 特徴

- **オンデマンド起動**：Host ヘッダーや `dormant.host` ラベルでルーティングし、未起動のコンテナを自動起動してからリクエストを転送。
- **TCP 転送**：`dormant.tcp` ラベルで公開したポートへ dormant が待ち受け、接続をコンテナへ透過転送。
- **アイドル停止**：最終アクセスからセッション保持時間（`dormant.session-duration`、既定 1h）を超えると自動停止。
- **グループ連鎖**：`dormant.group` で複数コンテナをまとめ、依存（`compose.depends_on`）を連鎖的に起動・停止。
- **ヘルスチェック**：`dormant.healthcheck.*` で起動待ちを確認してから転送開始。
- **WebSocket / SSE 対応**：アクティブ接続中のコンテナは停止対象外。
- **HTTP/1.1 ・ HTTP/2 対応**、`/healthz` で自ヘルスチェック。
- **graceful shutdown**：SIGINT / SIGTERM で停止。

## 前提

- Docker ソケット（Unix ソケット）にアクセス可能な環境
- ラベルを付与した管理対象コンテナ
- ルーティングにはコンテナ名（末尾 `-数字` は剥がす）または `dormant.host` ラベルを使います

## インストール

```bash
cargo build --release
# バイナリは target/release/dormant
```

## 設定

設定はコマンドライン引数と環境変数で行います。コマンドライン引数が優先され、未指定の場合は環境変数→既定値の順で解決されます。

| 引数 | 環境変数 | 既定値 | 説明 |
|------|----------|--------|------|
| `--listen` | `DORMANT_LISTEN` | `0.0.0.0:80` | HTTP 待ち受け |
| `--docker-socket` | `DORMANT_DOCKER_SOCKET` | `/var/run/docker.sock` | Docker ソケットのパス |
| `--idle-check-interval-secs` | `DORMANT_IDLE_CHECK_INTERVAL_SECS` | `30` | アイドル判定の周期（秒） |
| `--self-network` | `DORMANT_SELF_NETWORK` | (空) | 自身のネットワークエイリアスを付与するネットワーク名 |

## 使い方

```bash
# 起動
dormant

# 引数で指定
dormant --listen 0.0.0.0:8080 --docker-socket /run/user/1000/docker.sock

# 環境変数で指定
DORMANT_LISTEN=0.0.0.0:8080 dormant

# 環境変数 RUST_LOG でログレベル変更（例: debug）
RUST_LOG=debug dormant
```

### コンテナへのラベル付け

管理対象にするには `dormant.enable=true` を付けます。ルーティングはコンテナ名（末尾の `-数字` を剥がしたもの）か `dormant.host` で行います。

```bash
docker run -d --name myapp \
  --label dormant.enable=true \
  --label dormant.port=80 \
  myapp-image
```

利用可能なラベル：

| ラベル | 説明 |
|--------|------|
| `dormant.enable` | `true` で管理対象にする |
| `dormant.port` | 転送先ポート（未指定なら公開ポート / 依存専用コンテナは IP のみ） |
| `dormant.host` | ルーティング用ホスト名。`host[:port]` 形式のカンマ区切りで複数指定可能。ポート省略時は `dormant.port`（または公開ポート）へ振り分け |
| `dormant.tcp` | TCP転送。`PORT`（listen=コンテナ=同一）または `LISTEN_PORT:CONTAINER_PORT`、カンマ区切りで複数 |
| `dormant.group` | グループ名。同一グループを連動起動・停止 |
| `dormant.session-duration` | セッション保持時間（例 `30m`, `2h`。既定 `1h`） |
| `dormant.startup.timeout` | 起動タイムアウト（既定 `3m`） |
| `dormant.healthcheck.path` | 起動待ちヘルスチェックのパス |
| `dormant.healthcheck.port` | ヘルスチェックのポート |
| `dormant.healthcheck.status` | 許容ステータス（カンマ区切り複数可） |

### TCP 転送

`dormant.tcp` ラベルで TCP ポートを公開すると、dormant がそのポートで待ち受け、接続を当該コンテナへ透過転送します。HTTP と同様に、未起動なら自動起動してから接続し、接続中はアイドル停止しません。

```bash
# 同じポート番号で待ち受け (listen とコンテナ側が同一)
docker run -d --name mytcp \
  --label dormant.enable=true \
  --label dormant.tcp=6379 \
  mytcp-image

# 待ち受けポートとコンテナ側ポートを別に指定
docker run -d --name mytcp \
  --label dormant.enable=true \
  --label dormant.tcp=18102:6379 \
  mytcp-image
```

- `dormant.tcp=PORT` … dormant は `PORT` で待ち受け、コンテナの `PORT` へ転送。
- `dormant.tcp=LISTEN_PORT:CONTAINER_PORT` … dormant は `LISTEN_PORT` で待ち受け、コンテナの `CONTAINER_PORT` へ転送。
- コンテナ側の疎通確認には `dormant.port`（または `dormant.tcp` のコンテナ側ポート）を使います。

### 複数ポートの振り分け

`dormant.host` に `host:port` 形式で指定すると、一つのコンテナの複数ポートをドメインごとに振り分けられます。ポートを省略したエントリは `dormant.port`（または公開ポート）へ転送されます。

```bash
# 例: 同一コンテナで api.example.com → 8081、web.example.com → 8080、old.example.com → デフォルト(80)
docker run -d --name myapp \
  --label dormant.enable=true \
  --label dormant.port=80 \
  --label "dormant.host=api.example.com:8081,web.example.com:8080,old.example.com" \
  myapp-image
```

- `dormant.host=host` … `dormant.port`（または公開ポート）へ振り分け（従来互換）
- `dormant.host=host:port` … 指定したコンテナ側ポートへ振り分け
- `dormant.host` はカンマ区切りで複数ドメインを登録可能
- 起動待ち・ヘルスチェックも解決された転送先ポートで行います

### 依存関係（compose）

`compose.depends_on` を使うと、`docker compose` の依存先コンテナを管理対象として一緒に起動します。依存先のポート未指定コンテナは、起動して IP だけ返します。

## 開発

### ビルド・テスト

```bash
cargo build
cargo test
```

### E2E テスト

```bash
# 前提: dormant が localhost:18000 で起動中（docker-compose の dormant コンテナ）
docker compose up -d
./e2e-test.sh
```

- テスト用コンテナは `dormant-e2e-*` という名前で作成され、終了時に必ず削除されます。
- `DORMANT_CONTAINER` 環境変数で dormant 本体のコンテナ名を上書きできます。
- 詳細は `tests/pa` / `tests/pb` の `docker-compose.yml` と `e2e-wsclient.py` / `e2e-wskeep.py`（WebSocket 検証）を参照。

## アーキテクチャ

```text
HTTP クライアント              TCP クライアント
   │  HTTP/1.1・HTTP/2（Host: xxx）   │  TCP（dormant.tcp ポート）
   ▼                                ▼
[ proxy ] ──HTTP転送──▶       [ tcp ] ──TCP透過転送──▶
   │  ▲ touch / connect / disconnect（セッション記録）   │
   │  │                                                    │
   ▼  │                                                    ▼
[ router ]  Host→コンテナ と TCPポート→コンテナ のルーティング表
   │
   ▼
[ lifecycle ]  idle_loop: 期限切れコンテナを停止（連鎖）
   │
   ▼
[ docker ]  ラベル収集・起動・停止・イベント監視
```

- `src/main.rs` … 起動、設定読み込み、タスクの起動・ graceful shutdown
- `src/config.rs` … 設定の解析と Docker ラベル定数
- `src/docker.rs` … ラベル収集、コンテナ起動/停止、イベント監視、IP 解決
- `src/tcp.rs` … `dormant.tcp` による TCP 透過転送（起動待ち・セッション連携込み）
- `src/router.rs` … Host → コンテナのルーティング表
- `src/lifecycle.rs` … セッション管理とアイドル停止
- `src/proxy.rs` … HTTP リバースプロキシ、WebSocket ブリッジ

## 既知の制限 / TODO

- 現時点で特記事項はありません

## ライセンス

このリポジトリは現時点でライセンスを明示していません。利用前に管理者へ確認してください。
