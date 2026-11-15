"""Интеграционные тесты: настоящие ноды в одном процессе, общаются по HTTP через localhost."""
import asyncio
from collections import Counter

import aiohttp
import pytest

from raft.config import Config
from raft.server import NodeServer
from raft.state import Role
from tests.helpers import free_ports, wait_until

pytestmark = pytest.mark.integration

# Укороченные тайминги, чтобы тесты шли быстро; соотношения как в проде
FAST_TIMINGS = dict(
    election_timeout_min=0.15,
    election_timeout_max=0.30,
    heartbeat_interval=0.03,
    rpc_timeout=0.05,
)


class Cluster:
    def __init__(self, size: int):
        ports = free_ports(size * 2)
        self.ids = [f"node{i + 1}" for i in range(size)]
        self.rpc_ports = dict(zip(self.ids, ports[:size]))
        self.status_ports = dict(zip(self.ids, ports[size:]))
        self.servers: dict[str, NodeServer] = {}

    def config(self, node_id: str) -> Config:
        return Config(
            node_id=node_id,
            host="127.0.0.1",
            port=self.rpc_ports[node_id],
            status_port=self.status_ports[node_id],
            peers={pid: f"http://127.0.0.1:{self.rpc_ports[pid]}"
                   for pid in self.ids if pid != node_id},
            **FAST_TIMINGS,
        )

    async def start(self, node_id: str) -> None:
        server = NodeServer(self.config(node_id))
        await server.start()
        self.servers[node_id] = server

    async def start_all(self) -> None:
        for node_id in self.ids:
            await self.start(node_id)

    async def kill(self, node_id: str) -> None:
        await self.servers.pop(node_id).stop()

    async def stop_all(self) -> None:
        for node_id in list(self.servers):
            await self.kill(node_id)

    def nodes(self):
        return [s.node for s in self.servers.values()]

    def leaders(self):
        return [n for n in self.nodes() if n.role == Role.LEADER]

    def has_stable_leader(self) -> bool:
        """Ровно один лидер, и все живые ноды знают его и находятся в его терме."""
        leaders = self.leaders()
        if len(leaders) != 1:
            return False
        leader = leaders[0]
        return all(n.leader_id == leader.id and n.current_term == leader.current_term
                   for n in self.nodes())

    async def wait_for_leader(self, timeout: float = 5.0):
        await wait_until(self.has_stable_leader, timeout=timeout,
                         message=f"состояние: {[n.status() for n in self.nodes()]}")
        return self.leaders()[0]


@pytest.fixture
async def cluster_of():
    clusters = []

    async def factory(size: int) -> Cluster:
        cluster = Cluster(size)
        clusters.append(cluster)
        await cluster.start_all()
        return cluster

    yield factory
    for c in clusters:
        await c.stop_all()


@pytest.mark.parametrize("size", [1, 3, 5])
async def test_cluster_elects_single_leader(cluster_of, size):
    cluster = await cluster_of(size)

    leader = await cluster.wait_for_leader()

    roles = Counter(n.role for n in cluster.nodes())
    assert roles == Counter({Role.LEADER: 1, Role.FOLLOWER: size - 1})
    assert leader.current_term >= 1


async def test_leader_is_stable_while_alive(cluster_of):
    cluster = await cluster_of(3)
    leader = await cluster.wait_for_leader()
    term = leader.current_term

    await asyncio.sleep(1.0)  # несколько election timeout'ов

    assert cluster.has_stable_leader()
    assert cluster.leaders()[0] is leader
    assert leader.current_term == term


async def test_new_leader_after_leader_failure(cluster_of):
    cluster = await cluster_of(3)
    old_leader = await cluster.wait_for_leader()
    old_term = old_leader.current_term

    await cluster.kill(old_leader.id)
    new_leader = await cluster.wait_for_leader()

    assert new_leader.id != old_leader.id
    assert new_leader.current_term > old_term


async def test_restarted_node_rejoins_as_follower(cluster_of):
    cluster = await cluster_of(3)
    old_leader = await cluster.wait_for_leader()
    await cluster.kill(old_leader.id)
    new_leader = await cluster.wait_for_leader()

    await cluster.start(old_leader.id)
    await cluster.wait_for_leader()

    rejoined = cluster.servers[old_leader.id].node
    assert rejoined.role == Role.FOLLOWER
    assert rejoined.leader_id == cluster.leaders()[0].id
    assert cluster.leaders()[0].current_term >= new_leader.current_term


async def test_no_leader_without_quorum(cluster_of):
    cluster = await cluster_of(3)
    leader = await cluster.wait_for_leader()
    survivor = next(n for n in cluster.nodes() if n is not leader)
    for node_id in [n.id for n in cluster.nodes() if n is not survivor]:
        await cluster.kill(node_id)

    await asyncio.sleep(1.0)

    assert survivor.role == Role.CANDIDATE
    assert survivor.leader_id is None


async def test_status_api_over_http(cluster_of):
    cluster = await cluster_of(3)
    leader = await cluster.wait_for_leader()

    async with aiohttp.ClientSession() as session:
        for node_id, port in cluster.status_ports.items():
            async with session.get(f"http://127.0.0.1:{port}/status") as resp:
                body = await resp.json()
            assert body["id"] == node_id
            assert body["leader_id"] == leader.id
            assert body["role"] == ("leader" if node_id == leader.id else "follower")
