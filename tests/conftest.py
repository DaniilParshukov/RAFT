import pytest

from raft.node import RaftNode
from tests.helpers import FakeRpcClient, LocalCluster, make_config


@pytest.fixture
async def make_node():
    """Фабрика RaftNode с FakeRpcClient; все созданные ноды останавливаются после теста."""
    nodes: list[RaftNode] = []

    def factory(**config_kwargs) -> RaftNode:
        node = RaftNode(make_config(**config_kwargs))
        node.rpc_client = FakeRpcClient()
        nodes.append(node)
        return node

    yield factory

    for node in nodes:
        await node.stop()


@pytest.fixture
async def local_cluster():
    """Фабрика запущенных LocalCluster; останавливаются после теста."""
    clusters: list[LocalCluster] = []

    async def factory(size: int, **config_kwargs) -> LocalCluster:
        cluster = LocalCluster(size, **config_kwargs)
        clusters.append(cluster)
        await cluster.start_all()
        return cluster

    yield factory

    for cluster in clusters:
        await cluster.stop_all()
