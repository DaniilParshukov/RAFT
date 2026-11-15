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

    # Порт status API; по умолчанию port + 1000
    status_port: Optional[int] = None

    def __post_init__(self) -> None:
        if self.status_port is None:
            self.status_port = self.port + 1000

    @classmethod
    def from_env(cls) -> "Config":
        node_id = os.environ["NODE_ID"]
        host = os.environ.get("HOST", "127.0.0.1")
        port = int(os.environ["PORT"])
        status_port_raw = os.environ.get("STATUS_PORT")
        status_port = int(status_port_raw) if status_port_raw else None

        return cls(
            node_id=node_id,
            host=host,
            port=port,
            peers=parse_peers(os.environ.get("PEERS", "")),
            status_port=status_port,
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
