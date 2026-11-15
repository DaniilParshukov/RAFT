import asyncio
import logging

from .config import Config
from .server import NodeServer


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s", #(asctime)s
    )
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

    config = Config.from_env()
    server = NodeServer(config)
    await server.start()

    print(f"Node {config.node_id} listening on {config.host}:{config.port}")
    print(f"Status API on http://{config.host}:{config.status_port}/status")

    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
