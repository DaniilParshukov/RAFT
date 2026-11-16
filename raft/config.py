import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    node_id: str
    host: str
    port: int
    peers: dict[str, str]  # {node_id: "http://host:port"}

    election_timeout_min: float = 0.500
    election_timeout_max: float = 0.800
    heartbeat_interval: float = 0.050
    rpc_timeout: float = 0.100

    # Сколько записей лидер шлёт пиру в одном AppendEntries
    max_entries_per_rpc: int = 64
    # Сколько POST /log ждёт коммита записи, прежде чем ответить 504
    commit_timeout: float = 2.0

    # Порт клиентского REST API (status/log); по умолчанию port + 1000
    api_port: Optional[int] = None

    def __post_init__(self) -> None:
        if self.api_port is None:
            self.api_port = self.port + 1000

    @classmethod
    def from_env(cls) -> "Config":
        node_id = os.environ["NODE_ID"]
        host = os.environ.get("HOST", "127.0.0.1")
        port = int(os.environ["PORT"])
        api_port_raw = os.environ.get("API_PORT")
        api_port = int(api_port_raw) if api_port_raw else None

        return cls(
            node_id=node_id,
            host=host,
            port=port,
            peers=parse_peers(os.environ.get("PEERS", "")),
            api_port=api_port,
        )


def parse_peers(raw: str) -> dict[str, str]:
    """PEERS="node2=http://127.0.0.1:8002,node3=http://127.0.0.1:8003" -> dict."""
    peers = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        pid, addr = item.split("=", 1)
        peers[pid.strip()] = addr.strip()
    return peers
