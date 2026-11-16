"""HTTP-слой: RPC-сервер, клиентский API и RpcClient поверх настоящего aiohttp."""
import asyncio
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from raft.client_api import build_client_app
from raft.node import NotLeaderError
from raft.rpc_client import RpcClient
from raft.rpc_server import build_rpc_app
from tests.helpers import free_ports


class StubNode:
    """Нода-заглушка: записывает вызовы, поведение клиентских методов настраивается."""

    def __init__(self):
        self.calls = []
        self.config = SimpleNamespace(commit_timeout=1.0)
        self.leader = True
        self.commit_result = True  # True / False / TimeoutError

    async def handle_request_vote(self, **kwargs):
        self.calls.append(("vote", kwargs))
        return {"term": kwargs["term"], "vote_granted": True}

    async def handle_append_entries(self, **kwargs):
        self.calls.append(("append", kwargs))
        return {"term": kwargs["term"], "success": True}

    async def append_command(self, command):
        if not self.leader:
            raise NotLeaderError("n2")
        self.calls.append(("command", command))
        return 7, 3

    async def wait_for_commit(self, index, term, timeout):
        self.calls.append(("wait", index, term, timeout))
        if self.commit_result is TimeoutError:
            raise TimeoutError
        return self.commit_result

    def status(self):
        return {"id": "n1", "role": "follower"}

    def log_view(self):
        return {"commit_index": 1, "last_applied": 1,
                "entries": [{"index": 1, "term": 1, "command": "x", "committed": True}]}


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
    body = {"term": 3, "candidate_id": "n2", "last_log_index": 5, "last_log_term": 2}

    resp = await client.post("/raft/request_vote", json=body)

    assert resp.status == 200
    assert await resp.json() == {"term": 3, "vote_granted": True}
    assert node.calls == [("vote", body)]


async def test_rpc_append_entries_route(client_for):
    node = StubNode()
    client = await client_for(build_rpc_app(node))
    body = {"term": 4, "leader_id": "n3", "prev_log_index": 2, "prev_log_term": 1,
            "entries": [{"term": 4, "command": {"set": 1}}], "leader_commit": 2}

    resp = await client.post("/raft/append_entries", json=body)

    assert resp.status == 200
    assert await resp.json() == {"term": 4, "success": True}
    assert node.calls == [("append", body)]


async def test_rpc_routes_are_post_only(client_for):
    client = await client_for(build_rpc_app(StubNode()))

    resp = await client.get("/raft/request_vote")

    assert resp.status == 405


# ---------- Клиентский API ----------

async def test_status_endpoint(client_for):
    client = await client_for(build_client_app(StubNode()))

    resp = await client.get("/status")

    assert resp.status == 200
    assert await resp.json() == {"id": "n1", "role": "follower"}


async def test_health_endpoint(client_for):
    client = await client_for(build_client_app(StubNode()))

    resp = await client.get("/health")

    assert await resp.json() == {"ok": True}


async def test_get_log(client_for):
    node = StubNode()
    client = await client_for(build_client_app(node))

    resp = await client.get("/log")

    assert resp.status == 200
    assert await resp.json() == node.log_view()


async def test_post_log_waits_for_commit(client_for):
    node = StubNode()
    client = await client_for(build_client_app(node))

    resp = await client.post("/log", json={"command": {"set": "x", "value": 1}})

    assert resp.status == 200
    assert await resp.json() == {"index": 7, "term": 3, "committed": True}
    assert node.calls == [("command", {"set": "x", "value": 1}), ("wait", 7, 3, 1.0)]


async def test_post_log_without_waiting(client_for):
    node = StubNode()
    client = await client_for(build_client_app(node))

    resp = await client.post("/log?wait=false", json={"command": "x"})

    assert resp.status == 202
    assert await resp.json() == {"index": 7, "term": 3, "committed": False}
    assert [c[0] for c in node.calls] == ["command"]


async def test_post_log_on_follower(client_for):
    node = StubNode()
    node.leader = False
    client = await client_for(build_client_app(node))

    resp = await client.post("/log", json={"command": "x"})

    assert resp.status == 421
    assert await resp.json() == {"error": "not_leader", "leader_id": "n2"}


@pytest.mark.parametrize("result, status, error", [
    (TimeoutError, 504, "commit_timeout"),
    (False, 503, "entry_lost"),
])
async def test_post_log_not_committed(client_for, result, status, error):
    node = StubNode()
    node.commit_result = result
    client = await client_for(build_client_app(node))

    resp = await client.post("/log", json={"command": "x"})

    assert resp.status == status
    assert await resp.json() == {"error": error, "index": 7, "term": 3}


@pytest.mark.parametrize("kwargs, error", [
    ({"data": "not json"}, "invalid_json"),
    ({"json": {"cmd": 1}}, "command_required"),
    ({"json": [1, 2]}, "command_required"),
    ({"json": {"command": None}}, "command_must_not_be_null"),
])
async def test_post_log_validation(client_for, kwargs, error):
    node = StubNode()
    client = await client_for(build_client_app(node))

    resp = await client.post("/log", **kwargs)

    assert resp.status == 400
    assert (await resp.json())["error"] == error
    assert node.calls == []


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
