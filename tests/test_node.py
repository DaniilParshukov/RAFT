"""Юнит-тесты логики RaftNode. Сеть подменена FakeRpcClient."""
import asyncio

import pytest

from raft.state import Role
from tests.helpers import grant_votes_from, peer_url, wait_until


def peers(n: int) -> tuple[str, ...]:
    return tuple(f"n{i}" for i in range(2, n + 2))


# ---------- Кворум ----------

@pytest.mark.parametrize("n_peers, quorum", [(0, 1), (1, 2), (2, 2), (3, 3), (4, 3), (6, 4)])
async def test_quorum(make_node, n_peers, quorum):
    assert make_node(peer_ids=peers(n_peers)).quorum == quorum


# ---------- RequestVote ----------

async def test_vote_rejected_for_lower_term(make_node):
    node = make_node()
    node.current_term = 5

    resp = await node.handle_request_vote(term=4, candidate_id="n2")

    assert resp == {"term": 5, "vote_granted": False}
    assert node.voted_for is None


async def test_vote_granted_to_first_candidate(make_node):
    node = make_node()

    resp = await node.handle_request_vote(term=1, candidate_id="n2")

    assert resp == {"term": 1, "vote_granted": True}
    assert node.current_term == 1
    assert node.voted_for == "n2"
    assert node.role == Role.FOLLOWER


async def test_only_one_vote_per_term(make_node):
    node = make_node()
    await node.handle_request_vote(term=1, candidate_id="n2")

    resp = await node.handle_request_vote(term=1, candidate_id="n3")

    assert resp["vote_granted"] is False
    assert node.voted_for == "n2"


async def test_repeated_vote_request_from_same_candidate_granted(make_node):
    node = make_node()
    await node.handle_request_vote(term=1, candidate_id="n2")

    resp = await node.handle_request_vote(term=1, candidate_id="n2")

    assert resp["vote_granted"] is True


async def test_higher_term_allows_new_vote(make_node):
    node = make_node()
    await node.handle_request_vote(term=1, candidate_id="n2")

    resp = await node.handle_request_vote(term=2, candidate_id="n3")

    assert resp == {"term": 2, "vote_granted": True}
    assert node.voted_for == "n3"


async def test_candidate_does_not_vote_for_rival_in_same_term(make_node):
    node = make_node()
    await node._start_election()  # никто не отвечает -> остаёмся кандидатом
    assert node.role == Role.CANDIDATE

    resp = await node.handle_request_vote(term=node.current_term, candidate_id="n2")

    assert resp["vote_granted"] is False
    assert node.voted_for == node.id


async def test_leader_steps_down_on_higher_term_vote_request(make_node):
    node = make_node()
    node.rpc_client.vote_handler = grant_votes_from("n2", "n3")
    await node._start_election()
    assert node.role == Role.LEADER

    resp = await node.handle_request_vote(term=node.current_term + 1, candidate_id="n2")

    assert resp["vote_granted"] is True
    assert node.role == Role.FOLLOWER
    assert node.leader_id is None  # старый лидер не должен считать лидером себя
    await wait_until(lambda: node._heartbeat_timer is None or node._heartbeat_timer.done())


# ---------- AppendEntries ----------

async def test_append_entries_rejected_for_lower_term(make_node):
    node = make_node()
    node.current_term = 3

    resp = await node.handle_append_entries(term=2, leader_id="n2")

    assert resp == {"term": 3, "success": False}
    assert node.leader_id is None


async def test_append_entries_accepts_leader(make_node):
    node = make_node()

    resp = await node.handle_append_entries(term=2, leader_id="n2")

    assert resp == {"term": 2, "success": True}
    assert node.current_term == 2
    assert node.leader_id == "n2"
    assert node.role == Role.FOLLOWER


async def test_candidate_steps_down_on_append_entries_same_term(make_node):
    node = make_node()
    await node._start_election()
    term = node.current_term

    resp = await node.handle_append_entries(term=term, leader_id="n2")

    assert resp["success"] is True
    assert node.role == Role.FOLLOWER
    assert node.current_term == term
    assert node.leader_id == "n2"


async def test_candidate_keeps_its_vote_after_stepping_down_in_same_term(make_node):
    """Безопасность выборов: в одном терме нода голосует не более одного раза."""
    node = make_node()
    await node._start_election()
    term = node.current_term
    await node.handle_append_entries(term=term, leader_id="n2")

    resp = await node.handle_request_vote(term=term, candidate_id="n3")

    assert node.voted_for == node.id
    assert resp["vote_granted"] is False


async def test_heartbeats_prevent_election(make_node):
    node = make_node(election_timeout=(0.10, 0.15))
    await node.start()

    for _ in range(20):  # 20 * 0.03 = 0.6s, что в 4 раза больше таймаута
        await node.handle_append_entries(term=1, leader_id="n2")
        await asyncio.sleep(0.03)

    assert node.role == Role.FOLLOWER
    assert node.current_term == 1
    assert node.rpc_client.vote_calls == []


# ---------- Выборы ----------

async def test_follower_starts_election_after_timeout(make_node):
    node = make_node(election_timeout=(0.05, 0.08))
    await node.start()

    await wait_until(lambda: node.rpc_client.vote_calls, timeout=1.0)

    assert node.role == Role.CANDIDATE
    assert node.current_term >= 1
    assert node.voted_for == node.id
    assert node.leader_id is None


async def test_request_vote_sent_to_every_peer(make_node):
    node = make_node()

    await node._start_election()

    assert sorted(node.rpc_client.vote_calls, key=lambda c: c[0]) == [
        (peer_url("n2"), {"term": 1, "candidate_id": "n1"}),
        (peer_url("n3"), {"term": 1, "candidate_id": "n1"}),
    ]


@pytest.mark.parametrize("n_peers, granting, expected", [
    (2, ("n2", "n3"), Role.LEADER),
    (2, ("n2",), Role.LEADER),          # 2 из 3
    (2, (), Role.CANDIDATE),            # 1 из 3
    (4, ("n2", "n3"), Role.LEADER),     # 3 из 5
    (4, ("n2",), Role.CANDIDATE),       # 2 из 5
    (3, ("n2",), Role.CANDIDATE),       # 2 из 4 — не большинство
])
async def test_election_result_depends_on_majority(make_node, n_peers, granting, expected):
    node = make_node(peer_ids=peers(n_peers))
    node.rpc_client.vote_handler = grant_votes_from(*granting)

    await node._start_election()

    assert node.role == expected
    assert node.leader_id == (node.id if expected == Role.LEADER else None)


async def test_single_node_cluster_elects_itself(make_node):
    node = make_node(peer_ids=(), election_timeout=(0.05, 0.08))
    await node.start()

    await wait_until(lambda: node.role == Role.LEADER, timeout=1.0)

    assert node.current_term == 1
    assert node.leader_id == node.id


async def test_unreachable_peers_do_not_count(make_node):
    node = make_node()
    node.rpc_client.vote_handler = lambda url, payload: None

    await node._start_election()

    assert node.role == Role.CANDIDATE
    assert node.votes_received == {node.id}


async def test_candidate_steps_down_on_higher_term_in_vote_response(make_node):
    node = make_node()
    node.rpc_client.vote_handler = lambda url, payload: {"term": 7, "vote_granted": False}

    await node._start_election()

    assert node.role == Role.FOLLOWER
    assert node.current_term == 7
    assert node.voted_for is None


async def test_stale_vote_response_is_ignored(make_node):
    node = make_node()
    gate = asyncio.Event()

    async def delayed_grant(url, payload):
        await gate.wait()
        return {"term": payload["term"], "vote_granted": True}

    node.rpc_client.vote_handler = delayed_grant
    election = asyncio.create_task(node._start_election())
    await wait_until(lambda: len(node.rpc_client.vote_calls) == 2)

    # Пока голоса «в пути», появился лидер с более старшим термом
    await node.handle_append_entries(term=5, leader_id="n3")
    gate.set()
    await election

    assert node.role == Role.FOLLOWER
    assert node.current_term == 5
    assert node.leader_id == "n3"


async def test_candidate_retries_with_new_term(make_node):
    node = make_node(election_timeout=(0.03, 0.05))
    node.rpc_client.vote_handler = grant_votes_from()  # все отказывают
    await node.start()

    await wait_until(lambda: node.current_term >= 3, timeout=2.0)

    assert node.role == Role.CANDIDATE


# ---------- Лидер ----------

async def become_leader(node) -> None:
    node.rpc_client.vote_handler = grant_votes_from(*node.config.peers)
    await node._start_election()
    assert node.role == Role.LEADER


async def test_leader_sends_heartbeats_to_all_peers(make_node):
    node = make_node(heartbeat_interval=0.02)
    node.rpc_client.append_handler = lambda url, payload: {"term": payload["term"], "success": True}
    await become_leader(node)

    await asyncio.sleep(0.2)

    calls = node.rpc_client.append_calls
    for pid in ("n2", "n3"):
        sent = [p for url, p in calls if url == peer_url(pid)]
        assert len(sent) >= 3, f"мало heartbeat'ов к {pid}: {len(sent)}"
        assert all(p == {"term": node.current_term, "leader_id": "n1"} for p in sent)


async def test_leader_does_not_start_elections(make_node):
    node = make_node(election_timeout=(0.03, 0.05))
    node.rpc_client.append_handler = lambda url, payload: {"term": payload["term"], "success": True}
    await become_leader(node)
    term = node.current_term

    await asyncio.sleep(0.2)

    assert node.role == Role.LEADER
    assert node.current_term == term


async def test_leader_steps_down_on_higher_term_in_heartbeat_response(make_node):
    node = make_node(heartbeat_interval=0.02)
    await become_leader(node)
    node.rpc_client.append_handler = lambda url, payload: {"term": 9, "success": False}

    await wait_until(lambda: node.role == Role.FOLLOWER, timeout=1.0)

    assert node.current_term == 9
    assert node.leader_id is None
    await wait_until(lambda: node._heartbeat_timer is None or node._heartbeat_timer.done())


async def test_leader_stays_leader_when_peers_unreachable(make_node):
    node = make_node(heartbeat_interval=0.02)
    await become_leader(node)
    node.rpc_client.append_handler = lambda url, payload: None

    await asyncio.sleep(0.1)

    assert node.role == Role.LEADER


# ---------- Жизненный цикл / status ----------

async def test_stop_cancels_timers(make_node):
    node = make_node(heartbeat_interval=0.02)
    await become_leader(node)

    await node.stop()

    assert node._heartbeat_timer.done()
    assert node._election_timer is None or node._election_timer.done()


async def test_no_timers_after_stop(make_node):
    node = make_node(election_timeout=(0.03, 0.05))
    await node.start()
    await node.stop()

    # Запоздавший RPC после остановки не должен запускать выборы
    await node.handle_append_entries(term=1, leader_id="n2")
    await asyncio.sleep(0.1)

    assert node.rpc_client.vote_calls == []


async def test_status(make_node):
    node = make_node()
    node.rpc_client.vote_handler = grant_votes_from("n3")
    await node._start_election()

    assert node.status() == {
        "id": "n1",
        "role": "leader",
        "term": 1,
        "leader_id": "n1",
        "voted_for": "n1",
        "votes_received": ["n1", "n3"],
    }
