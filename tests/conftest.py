import pytest

from raft.node import RaftNode
from tests.helpers import FakeRpcClient, make_config


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
