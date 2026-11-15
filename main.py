import asyncio
import logging

from aiohttp import web

from api import build_status_app
from config import Config
from node import RaftNode
from rpc_client import RpcClient
from rpc_server import build_rpc_app


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s", #(asctime)s 
    )
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

    config = Config.from_env()
    node = RaftNode(config)

    rpc_client = RpcClient()
    node.rpc_client = rpc_client

    await rpc_client.start()
    await node.start()

    # RPC-сервер (межнодовые вызовы) — на основном порту
    rpc_app = build_rpc_app(node)
    rpc_runner = web.AppRunner(rpc_app)
    await rpc_runner.setup()
    rpc_site = web.TCPSite(rpc_runner, config.host, config.port)
    await rpc_site.start()

    # Status API — на порту +1000
    status_port = config.port + 1000
    status_app = build_status_app(node)
    status_runner = web.AppRunner(status_app)
    await status_runner.setup()
    status_site = web.TCPSite(status_runner, config.host, status_port)
    await status_site.start()

    print(f"Node {config.node_id} listening on {config.host}:{config.port}")
    print(f"Status API on http://{config.host}:{status_port}/status")

    try:
        await asyncio.Event().wait()
    finally:
        await node.stop()
        await rpc_client.stop()
        await rpc_runner.cleanup()
        await status_runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass