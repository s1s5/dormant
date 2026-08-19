import asyncio
import os
import socket

import websockets

# dormant 本体のコンテナ名 (環境に応じて DORMANT_CONTAINER で上書き可)
DORMANT_CONTAINER = os.environ.get("DORMANT_CONTAINER", "dormant-dormant-1")


async def main():
    # websockets は Host ヘッダーを URI のホスト部分から生成するため、
    # additional_headers で Host を指定しても上書きされてしまう。
    # socket を事前生成して接続先を固定し、URI のホスト部分 (= Host ヘッダー) を
    # テストコンテナ名 (dormant-e2e-wss) にして dormant にルーティングさせる。
    sock = socket.create_connection((DORMANT_CONTAINER, 18000))
    async with websockets.connect(
        "ws://dormant-e2e-wss:18000/",
        sock=sock,
    ) as ws:
        await ws.send("hello")
        resp = await asyncio.wait_for(ws.recv(), timeout=5)
        print("echo:", resp)


asyncio.run(main())
