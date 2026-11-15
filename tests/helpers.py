import asyncio
import inspect
import socket
from typing import Any, Callable, Optional

from raft.config import Config

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
    )


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
