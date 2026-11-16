import pytest

from raft.config import Config, parse_peers


def test_parse_peers():
    assert parse_peers("node2=http://127.0.0.1:8002, node3 = http://127.0.0.1:8003") == {
        "node2": "http://127.0.0.1:8002",
        "node3": "http://127.0.0.1:8003",
    }


@pytest.mark.parametrize("raw", ["", " ", ",", " , ,"])
def test_parse_peers_empty(raw):
    assert parse_peers(raw) == {}


def test_parse_peers_keeps_equals_in_url():
    assert parse_peers("n2=http://h:1/?a=b") == {"n2": "http://h:1/?a=b"}


def test_from_env(monkeypatch):
    monkeypatch.setenv("NODE_ID", "node1")
    monkeypatch.setenv("PORT", "8001")
    monkeypatch.setenv("PEERS", "node2=http://127.0.0.1:8002")
    monkeypatch.delenv("HOST", raising=False)
    monkeypatch.delenv("API_PORT", raising=False)

    cfg = Config.from_env()

    assert cfg.node_id == "node1"
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 8001
    assert cfg.api_port == 9001
    assert cfg.peers == {"node2": "http://127.0.0.1:8002"}


def test_from_env_overrides(monkeypatch):
    monkeypatch.setenv("NODE_ID", "node1")
    monkeypatch.setenv("PORT", "8001")
    monkeypatch.setenv("HOST", "0.0.0.0")
    monkeypatch.setenv("API_PORT", "7000")
    monkeypatch.delenv("PEERS", raising=False)

    cfg = Config.from_env()

    assert cfg.host == "0.0.0.0"
    assert cfg.api_port == 7000
    assert cfg.peers == {}


@pytest.mark.parametrize("missing", ["NODE_ID", "PORT"])
def test_from_env_requires_vars(monkeypatch, missing):
    monkeypatch.setenv("NODE_ID", "node1")
    monkeypatch.setenv("PORT", "8001")
    monkeypatch.delenv(missing)

    with pytest.raises(KeyError):
        Config.from_env()


def test_election_timeout_greater_than_heartbeat_and_rpc_timeout():
    # Иначе живой лидер не успеет отправить heartbeat до таймаута фолловера
    cfg = Config(node_id="n", host="h", port=1, peers={})
    assert cfg.heartbeat_interval + cfg.rpc_timeout < cfg.election_timeout_min
    assert cfg.election_timeout_min < cfg.election_timeout_max
