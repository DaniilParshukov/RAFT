import asyncio
import inspect
import json
import socket
from typing import Any, Callable, Optional

from raft.config import Config
from raft.node import RaftNode
from raft.state import Role

# Ответ фейкового RPC: dict, None ("нода не ответила") или корутина, возвращающая одно из них
Handler = Callable[[str, dict], Any]


class FakeRpcClient:
    """Подменяет RpcClient: записывает вызовы и отвечает через подставляемые обработчики."""

    def __init__(self) -> None:
        self.vote_handler: Handler = lambda url, payload: None
        self.append_handler: Handler = lambda url, payload: None
        self.vote_calls: list[tuple[str, dict]] = []
        self.append_calls: list[tuple[str, dict]] = []

    async def request_vote(self, peer_url: str, payload: dict) -> Optional[dict]:
        self.vote_calls.append((peer_url, payload))
        return await _resolve(self.vote_handler(peer_url, payload))

    async def append_entries(self, peer_url: str, payload: dict) -> Optional[dict]:
        self.append_calls.append((peer_url, payload))
        return await _resolve(self.append_handler(peer_url, payload))


async def _resolve(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def peer_url(peer_id: str) -> str:
    return f"http://{peer_id}"


def make_config(
    node_id: str = "n1",
    peer_ids: tuple[str, ...] = ("n2", "n3"),
    election_timeout: tuple[float, float] = (10.0, 10.0),
    heartbeat_interval: float = 0.02,
    **extra,
) -> Config:
    """Конфиг для юнит-тестов. По умолчанию таймаут выборов огромный, чтобы таймер не мешал."""
    return Config(
        node_id=node_id,
        host="127.0.0.1",
        port=0,
        peers={pid: peer_url(pid) for pid in peer_ids},
        election_timeout_min=election_timeout[0],
        election_timeout_max=election_timeout[1],
        heartbeat_interval=heartbeat_interval,
        **extra,
    )


class LocalNetwork:
    """In-memory «сеть» между настоящими RaftNode: RPC — прямой вызов обработчика.

    Payload и ответ гоняются через JSON, как по проводу. Ноды можно изолировать
    (все RPC к ним и от них «теряются»), чтобы моделировать разделение сети.
    """

    def __init__(self) -> None:
        self.nodes: dict[str, Any] = {}
        self.isolated: set[str] = set()

    def attach(self, node) -> None:
        self.nodes[node.id] = node
        node.rpc_client = _LocalRpcClient(self, node.id)

    def detach(self, node_id: str) -> None:
        self.nodes.pop(node_id, None)

    def isolate(self, *node_ids: str) -> None:
        self.isolated.update(node_ids)

    def heal(self) -> None:
        self.isolated.clear()

    def _reachable(self, src: str, dst: str) -> bool:
        return dst in self.nodes and src not in self.isolated and dst not in self.isolated

    async def call(self, src: str, url: str, method: str, payload: dict) -> Optional[dict]:
        dst = url.removeprefix("http://")
        if not self._reachable(src, dst):
            return None
        await asyncio.sleep(0)  # как в сети: ответ не приходит синхронно
        if not self._reachable(src, dst):  # ноду могли убить/изолировать за это время
            return None
        resp = await getattr(self.nodes[dst], method)(**json.loads(json.dumps(payload)))
        if not self._reachable(src, dst):  # связь пропала, пока запрос был «в пути»
            return None
        return json.loads(json.dumps(resp))


class _LocalRpcClient:
    def __init__(self, network: LocalNetwork, node_id: str) -> None:
        self._network = network
        self._node_id = node_id

    async def request_vote(self, peer_url: str, payload: dict) -> Optional[dict]:
        return await self._network.call(self._node_id, peer_url, "handle_request_vote", payload)

    async def append_entries(self, peer_url: str, payload: dict) -> Optional[dict]:
        return await self._network.call(self._node_id, peer_url, "handle_append_entries", payload)


def grant_votes_from(*peer_ids: str, term: int = 0) -> Handler:
    """Обработчик RequestVote: голосуют только перечисленные пиры, остальные отказывают."""
    granted = {peer_url(pid) for pid in peer_ids}

    def handler(url: str, payload: dict) -> dict:
        return {"term": max(term, payload["term"]), "vote_granted": url in granted}

    return handler


async def wait_until(predicate: Callable[[], bool], timeout: float = 3.0,
                     interval: float = 0.01, message: str = "") -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"Условие не выполнилось за {timeout}s. {message}")
        await asyncio.sleep(interval)


def free_ports(count: int) -> list[int]:
    """Свободные локальные порты; сокеты держим открытыми до конца, чтобы порты не повторились."""
    socks = []
    try:
        for _ in range(count):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


class LocalCluster:
    """Кластер настоящих RaftNode на LocalNetwork с короткими таймингами."""

    def __init__(self, size: int, **config_kwargs) -> None:
        self.ids = [f"n{i}" for i in range(1, size + 1)]
        self.network = LocalNetwork()
        self.nodes: dict[str, RaftNode] = {}
        self._config_kwargs = {"election_timeout": (0.10, 0.20), "heartbeat_interval": 0.02,
                               **config_kwargs}

    async def start(self, node_id: str) -> RaftNode:
        """Запуск (или перезапуск с чистым состоянием) ноды."""
        peers = tuple(pid for pid in self.ids if pid != node_id)
        node = RaftNode(make_config(node_id, peers, **self._config_kwargs))
        self.network.attach(node)
        self.nodes[node_id] = node
        await node.start()
        return node

    async def start_all(self) -> None:
        for node_id in self.ids:
            await self.start(node_id)

    async def kill(self, node_id: str) -> None:
        self.network.detach(node_id)
        await self.nodes.pop(node_id).stop()

    async def stop_all(self) -> None:
        for node_id in list(self.nodes):
            await self.kill(node_id)

    def connected(self) -> list[RaftNode]:
        return [n for n in self.nodes.values() if n.id not in self.network.isolated]

    def others(self, *excluded: RaftNode) -> list[RaftNode]:
        return [n for n in self.nodes.values() if n not in excluded]

    async def wait_for_leader(self, among: Optional[list[RaftNode]] = None,
                              timeout: float = 5.0) -> RaftNode:
        """Ровно один лидер среди among (по умолчанию — неизолированные ноды), и все его признают."""
        def nodes() -> list[RaftNode]:
            return among if among is not None else self.connected()

        def stable() -> bool:
            leaders = [n for n in nodes() if n.role == Role.LEADER]
            return len(leaders) == 1 and all(
                n.leader_id == leaders[0].id and n.current_term == leaders[0].current_term
                for n in nodes())

        await wait_until(stable, timeout=timeout,
                         message=f"состояние: {[n.status() for n in nodes()]}")
        return next(n for n in nodes() if n.role == Role.LEADER)

    async def wait_converged(self, timeout: float = 5.0) -> None:
        """Логи неизолированных нод совпадают и полностью закоммичены."""
        def converged() -> bool:
            nodes = self.connected()
            logs = [n.log.to_list() for n in nodes]
            return (all(lg == logs[0] for lg in logs)
                    and all(n.commit_index == n.log.last_index for n in nodes))

        await wait_until(converged, timeout=timeout,
                         message=f"состояние: {[n.status() for n in self.connected()]}")


def commands(node: RaftNode) -> list[Any]:
    """Команды из лога без no-op записей лидеров."""
    return [e["command"] for e in node.log.to_list() if e["command"] is not None]
