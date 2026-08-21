#!/usr/bin/env bash
# 検証: ネットワーク接続済みコンテナに後から alias を追加できるか?
#
# 検証したいこと:
#   A. 未接続のコンテナに alias 付きで network connect → 成功するはず
#   B. 接続済みのコンテナに別の alias を追加しようと network connect → ?
#   C. 接続済みコンテナを disconnect → 全 alias 付きで reconnect → 成功するはず
#
# 使い方: ./test-connect-alias.sh
set -u

NET="global"
PREFIX="alias-test"
ID_A="${PREFIX}-a"
ID_B="${PREFIX}-b"
IMG="alpine:latest"

echo "== ネットワーク: $NET (Docker $(docker version --format '{{.Server.Version}}')) =="

cleanup() {
    echo
    echo "== cleanup =="
    docker rm -f "$ID_A" "$ID_B" >/dev/null 2>&1 || true
    # 使った一時ネットワークがあれば削除
    docker network rm "${PREFIX}-net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# プライベートネットワークを用意(global を汚さないため)
docker network create "${PREFIX}-net" >/dev/null
echo "== 一時ネットワーク作成: ${PREFIX}-net =="

# 2つのコンテナを作成(まだネットワークには接続しない)
docker create --name "$ID_A" --label dormant.enable=true "$IMG" sleep 3600 >/dev/null
docker create --name "$ID_B" --label dormant.enable=true "$IMG" sleep 3600 >/dev/null
echo "== コンテナ作成: $ID_A, $ID_B (未接続) =="

echo
echo "--- [A] 未接続コンテナへ alias 付きで connect ---"
if docker network connect --alias alpha.local "${PREFIX}-net" "$ID_A" 2>&1; then
    echo "[A] OK: 未接続コンテナへの alias 付き connect 成功"
else
    echo "[A] NG: connect 失敗"
fi

echo
echo "--- [B] 接続済みコンテナ($ID_A)へ 別の alias を追加しようとする ---"
if docker network connect --alias beta.local "${PREFIX}-net" "$ID_A" 2>&1; then
    echo "[B] ✅ 接続済みコンテナへの alias 追加が成功した(拒否されない)"
else
    echo "[B] ⚠️  接続済みコンテナへの alias 追加が拒否された"
fi

echo
echo "--- 現在の ${PREFIX}-net 上の $ID_A のエイリアス確認 ---"
docker inspect "$ID_A" \
    --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} aliases={{$v.Aliases}}{{end}}'

echo
echo "--- [C] disconnect → 全 alias 付きで reconnect できるか ---"
if docker network disconnect "${PREFIX}-net" "$ID_A" 2>&1; then
    echo "[C-1] disconnect 成功"
    if docker network connect --alias alpha.local --alias beta.local "${PREFIX}-net" "$ID_A" 2>&1; then
        echo "[C-2] ✅ disconnect→reconnect(全alias付き) 成功"
    else
        echo "[C-2] ❌ reconnect 失敗"
    fi
else
    echo "[C-1] ❌ disconnect 失敗"
fi

echo
echo "--- 最終エイリアス状態 ---"
docker inspect "$ID_A" \
    --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} aliases={{$v.Aliases}}{{end}}'
