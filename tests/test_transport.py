"""HTTP-слой: RPC-сервер, status API и RpcClient поверх настоящего aiohttp."""
import asyncio

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from raft.rpc_client import RpcClient
from raft.rpc_server import build_rpc_app
from raft.status_api import build_status_app
from tests.helpers import free_ports


class StubNode:
    def __init__(self):
        self.calls = []

    async def handle_request_vote(self, term, candidate_id):
        self.calls.append(("vote", term, candidate_id))
        return {"term": term, "vote_granted": True}

    async def handle_append_entries(self, term, leader_id):
        self.calls.append(("append", term, leader_id))
        return {"term": term, "success": True}

    def status(self):
        return {"id": "n1", "role": "follower"}


@pytest.fixture
async def client_for():
    clients = []

    async def factory(app: web.Application) -> TestClient:
        client = TestClient(TestServer(app))
        await client.start_server()
        clients.append(client)
        return client

    yield factory
    for c in clients:
        await c.close()


# ---------- RPC-сервер ----------

async def test_rpc_request_vote_route(client_for):
    node = StubNode()
    client = await client_for(build_rpc_app(node))

    resp = await client.post("/raft/request_vote", json={"term": 3, "candidate_id": "n2"})

    assert resp.status == 200
    assert await resp.json() == {"term": 3, "vote_granted": True}
    assert node.calls == [("vote", 3, "n2")]


async def test_rpc_append_entries_route(client_for):
    node = StubNode()
    client = await client_for(build_rpc_app(node))

    resp = await client.post("/raft/append_entries", json={"term": 4, "leader_id": "n3"})

    assert resp.status == 200
    assert await resp.json() == {"term": 4, "success": True}
    assert node.calls == [("append", 4, "n3")]


async def test_rpc_routes_are_post_only(client_for):
    client = await client_for(build_rpc_app(StubNode()))

    resp = await client.get("/raft/request_vote")

    assert resp.status == 405


# ---------- Status API ----------

async def test_status_endpoint(client_for):
    client = await client_for(build_status_app(StubNode()))

    resp = await client.get("/status")

    assert resp.status == 200
    assert await resp.json() == {"id": "n1", "role": "follower"}


async def test_health_endpoint(client_for):
    client = await client_for(build_status_app(StubNode()))

    resp = await client.get("/health")

    assert await resp.json() == {"ok": True}


# ---------- RpcClient ----------

@pytest.fixture
async def rpc_client():
    client = RpcClient(timeout=0.1)
    await client.start()
    yield client
    await client.stop()


@pytest.fixture
async def peer(client_for):
    """Фейковый пир с разным поведением на разных путях."""
    app = web.Application()

    async def ok(request):
        return web.json_response({"echo": await request.json()})

    async def fail(request):
        return web.Response(status=500)

    async def slow(request):
        await asyncio.sleep(1)
        return web.json_response({})

    app.router.add_post("/raft/request_vote", ok)
    app.router.add_post("/raft/append_entries", ok)
    app.router.add_post("/fail/raft/request_vote", fail)
    app.router.add_post("/slow/raft/request_vote", slow)

    client = await client_for(app)
    return str(client.make_url("")).rstrip("/")


async def test_rpc_client_request_vote(rpc_client, peer):
    resp = await rpc_client.request_vote(peer, {"term": 1, "candidate_id": "n1"})

    assert resp == {"echo": {"term": 1, "candidate_id": "n1"}}


async def test_rpc_client_append_entries(rpc_client, peer):
    resp = await rpc_client.append_entries(peer, {"term": 1, "leader_id": "n1"})

    assert resp == {"echo": {"term": 1, "leader_id": "n1"}}


async def test_rpc_client_non_200_returns_none(rpc_client, peer):
    assert await rpc_client.request_vote(f"{peer}/fail", {"term": 1}) is None


async def test_rpc_client_timeout_returns_none(rpc_client, peer):
    loop = asyncio.get_running_loop()
    started = loop.time()

    assert await rpc_client.request_vote(f"{peer}/slow", {"term": 1}) is None
    assert loop.time() - started < 0.5


async def test_rpc_client_connection_refused_returns_none(rpc_client):
    (port,) = free_ports(1)  # порт свободен -> никто не слушает

    assert await rpc_client.request_vote(f"http://127.0.0.1:{port}", {"term": 1}) is None
