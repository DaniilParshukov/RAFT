import os
from dataclasses import dataclass


@dataclass
class Config:
    node_id: str
    host: str
    port: int
    peers: dict[str, str]  # {node_id: "http://host:port"}

    election_timeout_min: float = 0.500
    election_timeout_max: float = 0.800
    heartbeat_interval: float = 0.050

    @classmethod
    def from_env(cls) -> "Config":
        node_id = os.environ["NODE_ID"]
        host = os.environ.get("HOST", "127.0.0.1")
        port = int(os.environ["PORT"])

        # PEERS="node2=http://127.0.0.1:8002,node3=http://127.0.0.1:8003"
        peers_raw = os.environ.get("PEERS", "")
        peers = {}
        for item in peers_raw.split(","):
            if not item.strip():
                continue
            pid, addr = item.split("=", 1)
            peers[pid.strip()] = addr.strip()

        return cls(node_id=node_id, host=host, port=port, peers=peers)