#!/usr/bin/env bash
# =============================================================================
# dormant E2E テストスクリプト
#
# 前提:
#   - dormant が localhost:18000 で起動中 (docker-compose の dormant コンテナ)
#   - docker コマンドが利用可能
#
# 実行:
#   ./e2e-test.sh
#
# 検証パターン: E1 〜 E28 (plan-e2e-test.md, plan-e2e-ip-change.md 参照)
#   各パターンを関数化し、pass/fail を判定して最後にサマリを表示する。
#   テスト用コンテナはすべて <PREFIX>-* という名前で作成し、
#   スクリプト終了時に trap 経由で必ず削除する。
# =============================================================================

set -u  # 未定義変数をエラーにする (set -e はテスト内の戻り値判定を妨げるため使わない)

BASE_URL="http://localhost:18000"
NETWORK="dormant"        # dormant コンテナと同じネットワーク
PREFIX="dormant-e2e"     # テストコンテナの名前プレフィックス
DORMANT_CONTAINER="${DORMANT_CONTAINER:-dormant-dormant-1}"  # dormant 本体のコンテナ名(環境に応じて上書き可)

PASS=0
FAIL=0
RESULTS=()

# ---------------------------------------------------------------------------
# ヘルパー関数
# ---------------------------------------------------------------------------

# テスト用コンテナを一括削除 (EXIT トラップで保証)
cleanup() {
    local ids
    ids=$(docker ps -aq --filter "name=^/${PREFIX}-" 2>/dev/null) || true
    if [ -n "$ids" ]; then
        docker rm -f $ids >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# コンテナが running 状態かどうか
container_running() { # name
    local state
    state=$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null) || return 1
    [ "$state" = "true" ]
}

# HTTP ステータスコードを取得 (Host ヘッダ指定、起動待ち込みで最大30秒)
http_code() { # host [extra curl args...]
    curl -s -o /dev/null -w "%{http_code}" -m 30 -H "Host: $1" "${@:2}" "$BASE_URL/"
}

# 指定ステータスが返るまでリトライする
wait_http() { # host expected_code [timeout_sec]
    local host=$1 expected=$2 t=${3:-30}
    local deadline=$((SECONDS + t))
    while (( SECONDS < deadline )); do
        local code
        code=$(http_code "$host") || true
        if [ "$code" = "$expected" ]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# 指定パスで指定ステータスが返るまでリトライする
wait_http_path() { # host path expected_code [timeout_sec]
    local host=$1 path=$2 expected=$3 t=${4:-30}
    local deadline=$((SECONDS + t))
    while (( SECONDS < deadline )); do
        local code
        code=$(http_code_path "$host" "$path") || true
        if [ "$code" = "$expected" ]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# ルート同期待ち込みのステータス取得: 404(ルート未同期)はリトライし、
# それ以外のステータスが返った時点でその結果を返す(コンテナ起動を1回に抑える)
# 404 → ルート同期待ち → 実リクエスト(504等) の順に進むため、
# コンテナ作成直後のアクセスが404になるタイミング問題を解消する。
probe_code() { # host [timeout_sec]
    local host=$1 t=${2:-30}
    local deadline=$((SECONDS + t))
    while (( SECONDS < deadline )); do
        local code
        code=$(http_code "$host") || true
        if [ "$code" != "404" ]; then
            echo "$code"
            return 0
        fi
        sleep 1
    done
    echo "$code"
    return 1
}

# 汎用 nginx テストコンテナを作成 (dormant 管理対象)
# 注意: dormant のルーティングは末尾 "-数字" を剥がすため、
#       コンテナ名は "xxx-1" のような末尾ハイフン数字にしないこと。
run_nginx_container() { # name [extra docker run args...]
    docker run -d --name "$1" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=15s \
        "${@:2}" \
        nginx:alpine >/dev/null 2>&1
}

# 特定パスへの HTTP ステータス取得 (SSE 等のストリーミング応答を避ける)
http_code_path() { # host path [extra curl args...]
    curl -s -o /dev/null -w "%{http_code}" -m 10 -H "Host: $1" "$BASE_URL$2"
}

# SSE テストサーバーコンテナを起動 (dormant 管理対象、session-duration=10s)
# /health は即 200、それ以外のパスは SSE ストリームを返し続ける
run_sse_container() { # name
    local code
    code=$(mktemp /tmp/dormant-sse.XXXXXX.py)
    cat > "$code" <<'PYEOF'
from http.server import HTTPServer, BaseHTTPRequestHandler
import time
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        try:
            while True:
                self.wfile.write(b"data: hi\n\n")
                self.wfile.flush()
                time.sleep(1)
        except (BrokenPipeError, ConnectionResetError):
            pass
    def log_message(self, *a):
        pass
HTTPServer(("0.0.0.0", 8000), H).serve_forever()
PYEOF
    docker run -d --name "$1" --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=8000 \
        --label dormant.startup.timeout=15s \
        --label dormant.session-duration=10s \
        -v "$code":/sse.py:ro \
        python:3.12-alpine python3 /sse.py >/dev/null 2>&1
    local rc=$?
    rm -f "$code"
    return $rc
}

# テスト実行ラッパー: 関数を実行して pass/fail を記録
run_test() { # name fn [args...]
    local name=$1
    shift
    if "$@"; then
        PASS=$((PASS + 1))
        RESULTS+=("PASS: $name")
        echo "PASS: $name"
    else
        FAIL=$((FAIL + 1))
        RESULTS+=("FAIL: $name")
        echo "FAIL: $name"
    fi
}

# ---------------------------------------------------------------------------
# A. 基本転送 (E1-E5)
# ---------------------------------------------------------------------------

# E1: 起動済みコンテナへ転送 → 200 + nginx HTML
test_e1() {
    run_nginx_container "$PREFIX-one" || return 1
    wait_http "$PREFIX-one" 200 15 || return 1
    local body
    body=$(curl -s -m 10 -H "Host: $PREFIX-one" "$BASE_URL/") || return 1
    case "$body" in
        *"Welcome to nginx"*) return 0 ;;
        *) return 1 ;;
    esac
}

# E2: dormant.host ラベルでのルーティング → 200
test_e2() {
    run_nginx_container "$PREFIX-two" --label dormant.host=e2.host.localhost || return 1
    wait_http "e2.host.localhost" 200 15
}

# E3: コンテナ名由来ルーティング (後方互換) → 200
test_e3() {
    run_nginx_container "$PREFIX-three" || return 1
    wait_http "$PREFIX-three" 200 15
}

# E4: 未登録ホスト → 404
test_e4() {
    local code
    code=$(http_code "no-such-host.invalid") || true
    [ "$code" = "404" ]
}

# E5: /healthz → 200 "ok"
test_e5() {
    local body
    body=$(curl -s -m 5 "$BASE_URL/healthz") || return 1
    [ "$body" = "ok" ]
}

# ---------------------------------------------------------------------------
# B. 自動起動 (on-demand) (E6-E7)
# ---------------------------------------------------------------------------

# E6: 停止コンテナへアクセス → 自動起動 → 200
test_e6() {
    run_nginx_container "$PREFIX-six" || return 1
    docker stop "$PREFIX-six" >/dev/null || return 1
    local code
    code=$(http_code "$PREFIX-six") || true  # dormant が自動起動してから転送される
    [ "$code" = "200" ] || return 1
    container_running "$PREFIX-six"
}

# E7: 起動しないコンテナ → 504
# 注意: 存在しないイメージ名は docker create 自体が失敗するため再現不可。
#       代わりに「即座に exit するエントリポイント」で起動失敗を再現する。
#       startup.timeout を 5s に短縮して待ち時間を最小化。
test_e7() {
    docker run -d --name "$PREFIX-seven" --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=5s \
        alpine sh -c "exit 1" >/dev/null 2>&1 || return 1
    sleep 1  # exited 状態になるのを待つ
    local code
    code=$(http_code "$PREFIX-seven") || true
    [ "$code" = "504" ]
}

# ---------------------------------------------------------------------------
# C. アイドル停止 (scale-to-zero) (E8-E9)
# ---------------------------------------------------------------------------

# E8: session-duration 短縮 (10s) → アクセス後放置 → 自動停止
# 注意: 現行実装は proxy 側のセッション touch が未実装のため
#       idle_loop の expired が空になり、自動停止しない可能性がある
#       (その場合このテストは FAIL になる = 未実装機能の検出)。
test_e8() {
    run_nginx_container "$PREFIX-eight" --label dormant.session-duration=10s || return 1
    wait_http "$PREFIX-eight" 200 15 || return 1
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        if ! container_running "$PREFIX-eight"; then
            return 0  # 自動停止された
        fi
        sleep 2
    done
    return 1
}

# E9: セッション中の再アクセス → タイマーリセットで停止しない (回帰)
# session=30s: t≈0 にアクセス → t≈20 に再アクセス (期限が t≈30 → t≈50 に延長)
# t≈45 まで生存していればリセットが効いている。リセットなしだと t≈40 で停止する。
test_e9() {
    run_nginx_container "$PREFIX-nine" --label dormant.session-duration=30s || return 1
    wait_http "$PREFIX-nine" 200 15 || return 1  # t≈0: アクセス
    sleep 20
    wait_http "$PREFIX-nine" 200 15 || return 1  # t≈20: 再アクセス (タイマーリセット)
    local deadline=$((SECONDS + 25))  # t≈45 まで監視
    while (( SECONDS < deadline )); do
        if ! container_running "$PREFIX-nine"; then
            return 1  # 停止してしまった → 失敗
        fi
        sleep 2
    done
    return 0
}

# ---------------------------------------------------------------------------
# D. ヘルスチェック (E10-E12)
# ---------------------------------------------------------------------------

# E10: healthcheck.status=200 + nginx (常時200) → ready → 200
test_e10() {
    run_nginx_container "$PREFIX-ten" --label dormant.healthcheck.status=200 || return 1
    wait_http "$PREFIX-ten" 200 15
}

# E11: healthcheck.status=500 (nginxは200を返す) → 許容外 → 504
# 注意: docker run -d はコンテナを起動してしまうため、起動済みだとヘルスチェックを
#       バイパスして200が返る。停止状態にして dormant が起動+ヘルスチェックを行う
#       フローを通すこと。
test_e11() {
    run_nginx_container "$PREFIX-eleven" \
        --label dormant.healthcheck.status=500 \
        --label dormant.startup.timeout=10s || return 1
    docker stop "$PREFIX-eleven" >/dev/null || return 1
    local code
    code=$(probe_code "$PREFIX-eleven") || true
    [ "$code" = "504" ]
}

# E12: healthcheck.path=/hoge (存在しない) → 504
# 注意: 現行実装では healthcheck.path は status 指定時のみ有効なため
#       status=200 と併用する (path のみだとポート疎通だけで ready になる)。
#       E11 同様に docker stop してからアクセスする。
test_e12() {
    run_nginx_container "$PREFIX-twelve" \
        --label dormant.healthcheck.status=200 \
        --label dormant.healthcheck.path=/hoge \
        --label dormant.startup.timeout=10s || return 1
    docker stop "$PREFIX-twelve" >/dev/null || return 1
    local code
    code=$(probe_code "$PREFIX-twelve") || true
    [ "$code" = "504" ]
}

# ---------------------------------------------------------------------------
# E. グループ (E13-E16)
# ---------------------------------------------------------------------------

# E13: グループ2台、Aへアクセス → Bも起動、Aに200
test_e13() {
    run_nginx_container "$PREFIX-e13a" --label dormant.group=test-e13 || return 1
    run_nginx_container "$PREFIX-e13b" --label dormant.group=test-e13 || return 1
    docker stop "$PREFIX-e13a" >/dev/null || return 1
    docker stop "$PREFIX-e13b" >/dev/null || return 1
    local code
    code=$(http_code "$PREFIX-e13a") || true
    [ "$code" = "200" ] || return 1
    container_running "$PREFIX-e13a" || return 1
    container_running "$PREFIX-e13b" || return 1  # グループのBも起動している
}

# E14: グループ内1台が起動失敗 → 504
test_e14() {
    run_nginx_container "$PREFIX-e14a" --label dormant.group=test-e14 || return 1
    docker run -d --name "$PREFIX-e14b" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=5s \
        --label dormant.group=test-e14 \
        alpine sh -c "exit 1" >/dev/null 2>&1 || return 1
    docker stop "$PREFIX-e14a" >/dev/null || return 1
    local code
    code=$(http_code "$PREFIX-e14a") || true  # グループ起動失敗 → 504
    [ "$code" = "504" ]
}

# E15: グループ内起動済みはスキップ → Aのみ起動 (Bは起動済みのまま) → 200
test_e15() {
    run_nginx_container "$PREFIX-e15a" --label dormant.group=test-e15 || return 1
    run_nginx_container "$PREFIX-e15b" --label dormant.group=test-e15 || return 1
    # B は起動済みのまま、A だけ停止
    docker stop "$PREFIX-e15a" >/dev/null || return 1
    local code
    code=$(http_code "$PREFIX-e15a") || true
    [ "$code" = "200" ] || return 1
    container_running "$PREFIX-e15a" || return 1
    container_running "$PREFIX-e15b" || return 1  # B は起動済みを維持 (スキップ)
}

# E16: グループ名でアクセス → 404 (ルーティングしない)
test_e16() {
    run_nginx_container "$PREFIX-e16a" --label dormant.group=test-e16 || return 1
    local code
    code=$(http_code "test-e16") || true
    [ "$code" = "404" ]
}

# ---------------------------------------------------------------------------
# F. depends_on (E17-E21)
# ---------------------------------------------------------------------------

# E17: 本体+依存 (両方管理対象) 停止 → 本体へアクセス → 依存が先に起動 → 200
test_e17() {
    docker run -d --name "$PREFIX-e17db" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=15s \
        --label com.docker.compose.project=e2e17 \
        --label com.docker.compose.service=db \
        nginx:alpine >/dev/null 2>&1 || return 1
    docker run -d --name "$PREFIX-e17app" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=15s \
        --label com.docker.compose.project=e2e17 \
        --label com.docker.compose.service=app \
        --label com.docker.compose.depends_on=db:service_started:false \
        nginx:alpine >/dev/null 2>&1 || return 1
    docker stop "$PREFIX-e17db" >/dev/null || return 1
    docker stop "$PREFIX-e17app" >/dev/null || return 1
    local code
    code=$(http_code "$PREFIX-e17app") || true
    [ "$code" = "200" ] || return 1
    container_running "$PREFIX-e17db" || return 1   # 依存が起動している
    container_running "$PREFIX-e17app" || return 1
}

# E18: 依存先が管理対象外 → 本体は起動、依存は触られない
test_e18() {
    # 依存先 (db) は管理対象外 (dormant.enable なし) で停止状態
    docker run -d --name "$PREFIX-e18db" \
        --network "$NETWORK" \
        --label com.docker.compose.project=e2e18 \
        --label com.docker.compose.service=db \
        nginx:alpine >/dev/null 2>&1 || return 1
    docker stop "$PREFIX-e18db" >/dev/null || return 1
    docker run -d --name "$PREFIX-e18app" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=15s \
        --label com.docker.compose.project=e2e18 \
        --label com.docker.compose.service=app \
        --label com.docker.compose.depends_on=db:service_started:false \
        nginx:alpine >/dev/null 2>&1 || return 1
    local code
    code=$(probe_code "$PREFIX-e18app") || true
    [ "$code" = "200" ] || return 1
    container_running "$PREFIX-e18app" || return 1
    ! container_running "$PREFIX-e18db"  # 管理対象外の依存は触れない (停止のまま)
}

# E19: 依存先が存在しない → 警告のみで本体起動 → 200
test_e19() {
    docker run -d --name "$PREFIX-e19app" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=15s \
        --label com.docker.compose.project=e2e19 \
        --label com.docker.compose.service=app \
        --label com.docker.compose.depends_on=nonexistent-svc:service_started:false \
        nginx:alpine >/dev/null 2>&1 || return 1
    wait_http "$PREFIX-e19app" 200 15
}

# E20: 停止時 → 依存先 (管理対象) も連鎖停止
# 注意: 現行実装はセッション touch 未実装のため idle_loop が発火せず
#       連鎖停止が行われない可能性がある (その場合は FAIL = 未実装の検出)。
test_e20() {
    docker run -d --name "$PREFIX-e20db" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=15s \
        --label dormant.session-duration=10s \
        --label com.docker.compose.project=e2e20 \
        --label com.docker.compose.service=db \
        nginx:alpine >/dev/null 2>&1 || return 1
    docker run -d --name "$PREFIX-e20app" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=15s \
        --label dormant.session-duration=10s \
        --label com.docker.compose.project=e2e20 \
        --label com.docker.compose.service=app \
        --label com.docker.compose.depends_on=db:service_started:false \
        nginx:alpine >/dev/null 2>&1 || return 1
    wait_http "$PREFIX-e20app" 200 15 || return 1
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        if ! container_running "$PREFIX-e20app" && ! container_running "$PREFIX-e20db"; then
            return 0  # 本体と依存の両方が連鎖停止した
        fi
        sleep 2
    done
    return 1
}

# E21: 依存先が管理対象外 → 停止時も触らない → 本体のみ停止
test_e21() {
    # 依存先 (db) は管理対象外で停止状態のまま
    docker run -d --name "$PREFIX-e21db" \
        --network "$NETWORK" \
        --label com.docker.compose.project=e2e21 \
        --label com.docker.compose.service=db \
        nginx:alpine >/dev/null 2>&1 || return 1
    docker stop "$PREFIX-e21db" >/dev/null || return 1
    docker run -d --name "$PREFIX-e21app" \
        --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=80 \
        --label dormant.startup.timeout=15s \
        --label dormant.session-duration=10s \
        --label com.docker.compose.project=e2e21 \
        --label com.docker.compose.service=app \
        --label com.docker.compose.depends_on=db:service_started:false \
        nginx:alpine >/dev/null 2>&1 || return 1
    wait_http "$PREFIX-e21app" 200 15 || return 1
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        if ! container_running "$PREFIX-e21app"; then
            # 本体は停止した。管理対象外の依存は停止のまま (触られていない)
            if ! container_running "$PREFIX-e21db"; then
                return 0
            fi
        fi
        sleep 2
    done
    return 1
}

# ---------------------------------------------------------------------------
# G. WebSocket (E22)
# ---------------------------------------------------------------------------

# E22: WS アップグレード転送 (簡易チェック)
# 備考: nginx は WebSocket 非対応のため 101 は期待できない。
#       curl で Upgrade ヘッダ付きリクエストを送り、dormant がバックエンド
#       (nginx) へ転送して何らかの HTTP 応答 (426 等) が返れば転送経路は
#       成立と判定する。502/504/000 は転送失敗として失敗扱い。
test_e22() {
    run_nginx_container "$PREFIX-ws" || return 1
    wait_http "$PREFIX-ws" 200 15 || return 1
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" -m 30 \
        -H "Host: $PREFIX-ws" \
        -H "Connection: Upgrade" \
        -H "Upgrade: websocket" \
        -H "Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==" \
        -H "Sec-WebSocket-Version: 13" \
        "$BASE_URL/") || true
    if [ "$code" = "502" ] || [ "$code" = "504" ] || [ "$code" = "000" ]; then
        return 1
    fi
    return 0
}

# ---------------------------------------------------------------------------
# H. ルート追従 (E23-E24)
# ---------------------------------------------------------------------------

# E23: コンテナ rename → ルート更新 → 新名で 200、旧名は 404
test_e23() {
    run_nginx_container "$PREFIX-rn" || return 1
    wait_http "$PREFIX-rn" 200 15 || return 1
    docker rename "$PREFIX-rn" "$PREFIX-rn-new" || return 1
    wait_http "$PREFIX-rn-new" 200 15 || return 1
    wait_http "$PREFIX-rn" 404 15  # 旧名はルートから消える
}

# E24: コンテナ削除 → ルートから消える → 404
test_e24() {
    run_nginx_container "$PREFIX-rm" || return 1
    wait_http "$PREFIX-rm" 200 15 || return 1
    docker rm -f "$PREFIX-rm" >/dev/null || return 1
    wait_http "$PREFIX-rm" 404 15
}

# ---------------------------------------------------------------------------
# I. コンテナ再作成 (IP変更) 時のルーティング維持 (E25-E28)
# ---------------------------------------------------------------------------

# コンテナのIPアドレスを取得 (docker ネットワーク上のもの)
container_ip() { # name
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1" 2>/dev/null
}

# E25: dormant.host 付きコンテナを削除 → 同名の新コンテナを再作成 → 同じホスト名で 200
# 検証: 再作成後も同じホスト名で 200 が返る (ルートが新しいコンテナを指す)。
#       第1世代は削除済みのため、200 は新コンテナからの応答である。
# 注意: Docker の IPAM は解放した IP を再利用するため、IP 差異は情報として出力する
#       だけで PASS/FAIL 判定には使用しない。
test_e25() {
    local host="ipchange25.example.localhost"
    run_nginx_container "$PREFIX-ip25" --label dormant.host="$host" || return 1
    wait_http "$host" 200 15 || return 1
    local ip1 ip2
    ip1=$(container_ip "$PREFIX-ip25") || return 1
    docker rm -f "$PREFIX-ip25" >/dev/null || return 1
    sleep 2  # ルート同期待ち (destroy イベント反映)
    run_nginx_container "$PREFIX-ip25" --label dormant.host="$host" || return 1
    wait_http "$host" 200 15 || return 1  # 再作成後も同じホスト名で 200
    ip2=$(container_ip "$PREFIX-ip25") || return 1
    [ -n "$ip1" ] && [ -n "$ip2" ] || return 1
    if [ "$ip1" != "$ip2" ]; then
        echo "    (INFO) コンテナ再作成でIP変更: $ip1 → $ip2"
    else
        echo "    (INFO) IPは同一 ($ip1)。Docker IPAM が再利用。ルーティングは新コンテナへ"
    fi
    return 0
}

# E26: E25 の後、旧コンテナが削除済みであること (ルートが新コンテナを指す) を確認
# 検証: 旧コンテナは存在しない (docker inspect 失敗) + 同じホスト名で 200 (新コンテナから応答)
test_e26() {
    local host="ip26-e26.example.localhost"
    run_nginx_container "$PREFIX-ip26old" --label dormant.host="$host" || return 1
    wait_http "$host" 200 15 || return 1
    docker rm -f "$PREFIX-ip26old" >/dev/null || return 1
    sleep 2  # ルート同期待ち
    run_nginx_container "$PREFIX-ip26new" --label dormant.host="$host" || return 1
    wait_http "$host" 200 15 || return 1  # 新コンテナへ転送される
    if container_running "$PREFIX-ip26old"; then
        return 1  # 旧コンテナがまだ存在している → 失敗
    fi
    container_running "$PREFIX-ip26new"
}

# E27: 旧コンテナを停止のみ (削除せず) → 新コンテナ作成 → アクセスは新コンテナへ
# 検証: 同じホスト名で 200 (新コンテナ) + 旧コンテナは停止のまま (自動起動されない)
test_e27() {
    local host="ip27-e27.example.localhost"
    run_nginx_container "$PREFIX-ip27old" --label dormant.host="$host" || return 1
    wait_http "$host" 200 15 || return 1
    docker stop "$PREFIX-ip27old" >/dev/null || return 1
    sleep 2  # ルート同期待ち
    run_nginx_container "$PREFIX-ip27new" --label dormant.host="$host" || return 1
    sleep 2  # ルート同期待ち (create イベント反映)
    wait_http "$host" 200 15 || return 1  # 新コンテナへ転送される
    container_running "$PREFIX-ip27new" || return 1
    ! container_running "$PREFIX-ip27old"  # 旧コンテナは停止のまま
}

# E28: 再作成後の新コンテナも停止 → アクセスで自動起動 → 200
# 検証: コンテナ再作成後も scale-to-zero (on-demand 起動) が機能する
test_e28() {
    local host="ip28-eip.example.localhost"
    run_nginx_container "$PREFIX-ip28old" --label dormant.host="$host" || return 1
    wait_http "$host" 200 15 || return 1
    docker rm -f "$PREFIX-ip28old" >/dev/null || return 1
    sleep 2  # ルート同期待ち
    run_nginx_container "$PREFIX-ip28new" --label dormant.host="$host" || return 1
    wait_http "$host" 200 15 || return 1
    docker stop "$PREFIX-ip28new" >/dev/null || return 1
    local code
    code=$(http_code "$host") || true  # dormant が自動起動してから転送される
    [ "$code" = "200" ] || return 1
    container_running "$PREFIX-ip28new"
}

# ---------------------------------------------------------------------------
# J. アクティブ接続保護 + HTTP/2 + gRPC (E29-E33)
# ---------------------------------------------------------------------------

# E29: SSE 接続中 (curl -N で10秒保持) は session-duration=10s でも停止しない
# 検証: SSE ストリームを開いたまま 20 秒待ち、接続中はコンテナが停止しないことを確認。
test_e29() {
    run_sse_container "$PREFIX-sse" || return 1
    # 起動 + ルート同期を待つ (SSE パスはストリーム応答のため /health で確認)
    wait_http_path "$PREFIX-sse" /health 200 15 || return 1
    # バックグラウンドで SSE ストリームを保持 (10秒)
    curl -s -N -m 20 -H "Host: $PREFIX-sse" "$BASE_URL/stream" >/dev/null 2>&1 &
    local cpid=$!
    sleep 12  # session-duration=10s を超えるまで待つ (接続中は停止しないはず)
    if ! container_running "$PREFIX-sse"; then
        kill "$cpid" 2>/dev/null
        return 1  # 接続中なのに停止した → 失敗
    fi
    kill "$cpid" 2>/dev/null
    wait "$cpid" 2>/dev/null
    return 0
}

# E30: SSE 切断後は自動停止する
# 検証: SSE 接続を閉じた後、アクティブカウントが0に戻り、期限超過で停止する。
test_e30() {
    run_sse_container "$PREFIX-sse2" || return 1
    wait_http_path "$PREFIX-sse2" /health 200 15 || return 1
    # 短時間だけ SSE に接続して切断する
    timeout 3 curl -s -N -H "Host: $PREFIX-sse2" "$BASE_URL/stream" >/dev/null 2>&1 || true
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        if ! container_running "$PREFIX-sse2"; then
            return 0  # 切断後に自動停止した
        fi
        sleep 2
    done
    return 1
}

# E31: WS 接続中は停止しない
# 検証: python websockets サーバーをバックエンドに立て、WS 接続を保持したまま
#       session-duration を超えても停止しないことを確認する。
test_e31() {
    local code
    code=$(mktemp /tmp/dormant-ws.XXXXXX.py)
    cat > "$code" <<'PYEOF'
import asyncio
import websockets

async def handler(ws):
    try:
        while True:
            msg = await ws.recv()
            await ws.send(msg)
    except websockets.ConnectionClosed:
        pass

async def main():
    async with websockets.serve(handler, "0.0.0.0", 8080):
        await asyncio.Future()

asyncio.run(main())
PYEOF
    docker run -d --name "$PREFIX-wss" --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=8080 \
        --label dormant.startup.timeout=15s \
        --label dormant.session-duration=10s \
        -v "$code":/ws.py:ro \
        python:3.12-alpine sh -c \
        "pip install -q --no-cache-dir websockets && python3 /ws.py" >/dev/null 2>&1 || return 1
    rm -f "$code"
    # WS エコーが通るまでリトライ (dormant 経由)
    local deadline=$((SECONDS + 40))
    local ok=1
    while (( SECONDS < deadline )); do
        if timeout 10 docker run --rm --network "$NETWORK" \
            -e DORMANT_CONTAINER="$DORMANT_CONTAINER" \
            -v "$(dirname "$0")/e2e-wsclient.py":/client.py:ro \
            python:3.12-alpine sh -c \
            "pip install -q --no-cache-dir websockets && python3 /client.py" 2>/dev/null | grep -q "echo: hello"; then
            ok=0
            break
        fi
        sleep 2
    done
    [ $ok -eq 0 ] || { docker rm -f "$PREFIX-wss" >/dev/null 2>&1; return 1; }
    # WS 接続を保持したまま session-duration (10s) を超えるまで待つ
    timeout 25 docker run --rm --network "$NETWORK" \
        -e DORMANT_CONTAINER="$DORMANT_CONTAINER" \
        -v "$(dirname "$0")/e2e-wskeep.py":/keep.py:ro \
        python:3.12-alpine sh -c \
        "pip install -q --no-cache-dir websockets && python3 /keep.py" >/dev/null 2>&1 &
    local cpid=$!
    sleep 12
    if container_running "$PREFIX-wss"; then
        kill "$cpid" 2>/dev/null
        wait "$cpid" 2>/dev/null
        docker rm -f "$PREFIX-wss" >/dev/null 2>&1
        return 0  # 接続中は停止しない
    fi
    kill "$cpid" 2>/dev/null
    wait "$cpid" 2>/dev/null
    docker rm -f "$PREFIX-wss" >/dev/null 2>&1
    return 1  # 接続中なのに停止した → 失敗
}

# E32: curl --http2 (h2c) でアクセス → 200
# 検証: クライアント→dormant が HTTP/2 (cleartext) で通ること。
test_e32() {
    run_nginx_container "$PREFIX-h2" || return 1
    wait_http "$PREFIX-h2" 200 15 || return 1
    docker stop "$PREFIX-h2" >/dev/null || return 1
    local code
    # curl --http2 は h2c (cleartext HTTP/2) で接続する
    code=$(curl -s -o /dev/null -w "%{http_code}" -m 30 --http2 \
        -H "Host: $PREFIX-h2" "$BASE_URL/") || true
    [ "$code" = "200" ]
}

# E33: gRPC サーバー (grpc-health-check) への転送
# 検証: バックエンドに gRPC ヘルスサーバーを立て、grpcurl で Health/Check が通る。
test_e33() {
    # gRPC ヘルスサーバー (Python) をバックエンドとして起動
    local code
    code=$(mktemp /tmp/dormant-grpc.XXXXXX.py)
    cat > "$code" <<'PYEOF'
from concurrent import futures
import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

class Health(health_pb2_grpc.HealthServicer):
    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING)

server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
health_pb2_grpc.add_HealthServicer_to_server(Health(), server)
# grpcurl はデフォルトで reflection API を要求するため、reflection を有効化する
service_names = (
    health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
    reflection.SERVICE_NAME,
)
reflection.enable_server_reflection(service_names, server)
server.add_insecure_port("0.0.0.0:50051")
server.start()
server.wait_for_termination()
PYEOF
    docker run -d --name "$PREFIX-grpc" --network "$NETWORK" \
        --label dormant.enable=true \
        --label dormant.port=50051 \
        --label dormant.startup.timeout=30s \
        -v "$code":/grpc_srv.py:ro \
        python:3.12-alpine sh -c \
        "pip install -q --no-cache-dir grpcio grpcio-health-checking grpcio-reflection && python3 /grpc_srv.py" >/dev/null 2>&1 || return 1
    rm -f "$code"
    # ヘルスチェック付きで ready を待つ (gRPC はHTTP/2でポート疎通だけでは不十分なため)
    sleep 8  # pip install + サーバー起動待ち
    # 転送が通るまでリトライ (grpcurl は dormant 経由で gRPC バックエンドに到達)
    local deadline=$((SECONDS + 40))
    while (( SECONDS < deadline )); do
        local out rc
        out=$(timeout 5 docker run --rm --network "$NETWORK" fullstorydev/grpcurl \
            -plaintext -d '{}' \
            -authority "$PREFIX-grpc" \
            "$DORMANT_CONTAINER:18000" grpc.health.v1.Health/Check 2>&1) && rc=0 || rc=1
        if [ $rc -eq 0 ]; then
            case "$out" in
                *"SERVING"*) return 0 ;;
            esac
        fi
        sleep 2
    done
    return 1
}

# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------
main() {
    echo "=== dormant E2E テスト開始 ==="
    echo "対象: $BASE_URL  (dormant コンテナが起動していること)"
    echo

    # 前提確認: dormant の /healthz
    local hz
    hz=$(curl -s -m 5 "$BASE_URL/healthz") || true
    if [ "$hz" != "ok" ]; then
        echo "ERROR: dormant が localhost:18000 で正常稼働していません (/healthz = '$hz')"
        echo "       docker-compose で dormant を起動してから再実行してください。"
        exit 1
    fi

    run_test "E1 起動済みコンテナへ転送 (200 + nginx HTML)" test_e1
    run_test "E2 dormant.host ラベルでのルーティング (200)" test_e2
    run_test "E3 コンテナ名由来ルーティング (200)" test_e3
    run_test "E4 未登録ホストは404" test_e4
    run_test "E5 /healthz は 200 'ok'" test_e5
    run_test "E6 停止コンテナへのアクセスで自動起動 (200)" test_e6
    run_test "E7 起動しないコンテナは504" test_e7
    run_test "E8 アイドル停止 (session-duration=10s)" test_e8
    run_test "E9 インターバル内の再アクセスでは停止しない" test_e9
    run_test "E10 healthcheck.status=200 で200" test_e10
    run_test "E11 healthcheck.status=500 は504" test_e11
    run_test "E12 healthcheck.path=/hoge は504" test_e12
    run_test "E13 グループ2台: AへアクセスでBも起動 (200)" test_e13
    run_test "E14 グループ内1台の起動失敗で504" test_e14
    run_test "E15 グループ内起動済みはスキップ (200)" test_e15
    run_test "E16 グループ名ではルーティングしない (404)" test_e16
    run_test "E17 依存先を先に起動して200" test_e17
    run_test "E18 依存先が管理対象外なら触らない" test_e18
    run_test "E19 依存先が存在しない場合も本体は起動 (200)" test_e19
    run_test "E20 停止時に依存先も連鎖停止" test_e20
    run_test "E21 管理対象外の依存は停止時も触らない" test_e21
    run_test "E22 WebSocket アップグレード転送 (簡易)" test_e22
    run_test "E23 コンテナrenameでルート追従 (新名200/旧名404)" test_e23
    run_test "E24 コンテナ削除でルートから消える (404)" test_e24
    run_test "E25 コンテナ再作成で新コンテナへ転送 (200)" test_e25
    run_test "E26 再作成後は旧コンテナが削除済みでルートが新コンテナを指す" test_e26
    run_test "E27 旧コンテナ停止のみでも新コンテナへ転送 (200)" test_e27
    run_test "E28 再作成後の新コンテナも自動起動が機能 (200)" test_e28
    run_test "E29 SSE 接続中は停止しない (session-duration 超過でも)" test_e29
    run_test "E30 SSE 切断後は自動停止する" test_e30
    run_test "E31 WS 接続中は停止しない" test_e31
    run_test "E32 HTTP/2 (h2c) でアクセス (200)" test_e32
    run_test "E33 gRPC バックエンドへの転送 (Health/Check)" test_e33

    # サマリ
    echo
    echo "=== サマリ ==="
    printf '%s\n' "${RESULTS[@]}"
    echo "----------------------------------------"
    echo "PASS: $PASS / $((PASS + FAIL))   FAIL: $FAIL"
    [ "$FAIL" -eq 0 ]
}

main "$@"
