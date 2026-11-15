from aiohttp import web

from .config import Config
from .node import RaftNode
from .rpc_client import RpcClient
from .rpc_server import build_rpc_app
from .status_api import build_status_app


class NodeServer:
    """Собирает ноду целиком: RaftNode + RPC-клиент + RPC-сервер + status API."""

    def __init__(self, config: Config):
        self.config = config
        self.node = RaftNode(config)
        self.rpc_client = RpcClient(timeout=config.rpc_timeout)
        self.node.rpc_client = self.rpc_client
        self._runners: list[web.AppRunner] = []

    async def start(self) -> None:
        await self.rpc_client.start()

        # Сервера поднимаем до старта таймеров, чтобы нода сразу могла отвечать
        # RPC-сервер (межнодовые вызовы) — на основном порту
        await self._serve(build_rpc_app(self.node), self.config.port)
        # Status API — на отдельном порту
        await self._serve(build_status_app(self.node), self.config.status_port)

        await self.node.start()

    async def stop(self) -> None:
        # Сначала перестаём принимать RPC, иначе входящий heartbeat
        # может перезапустить таймер уже остановленной ноды
        for runner in self._runners:
            await runner.cleanup()
        self._runners.clear()
        await self.node.stop()
        await self.rpc_client.stop()

    async def _serve(self, app: web.Application, port: int) -> None:
        runner = web.AppRunner(app)
        await runner.setup()
        self._runners.append(runner)
        await web.TCPSite(runner, self.config.host, port).start()
