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

    resp = await node.handle_request_vote(term=node.current_term + 1, candidate_id="n2",
                                          last_log_index=node.log.last_index,
                                          last_log_term=node.log.last_term)

    assert resp["vote_granted"] is True
    assert node.role == Role.FOLLOWER
    assert node.leader_id is None  # старый лидер не должен считать лидером себя
    assert node._replication_tasks == {}


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


async def test_reset_reuses_single_timer_task(make_node):
    node = make_node()
    await node.start()
    timer = node._election_timer
    tasks_before = len(asyncio.all_tasks())

    for term in range(1, 101):
        await node.handle_append_entries(term=term, leader_id="n2")
        await node.handle_request_vote(term=term, candidate_id="n2")

    assert node._election_timer is timer
    assert not timer.done()
    assert len(asyncio.all_tasks()) == tasks_before


async def test_election_starts_not_earlier_than_min_timeout_after_last_heartbeat(make_node):
    node = make_node(election_timeout=(0.15, 0.25))
    await node.start()
    loop = asyncio.get_running_loop()

    for _ in range(10):
        await node.handle_append_entries(term=1, leader_id="n2")
        await asyncio.sleep(0.02)
    last_heartbeat = loop.time()
    await node.handle_append_entries(term=1, leader_id="n2")

    await wait_until(lambda: node.rpc_client.vote_calls, timeout=1.0, interval=0.005)
    elapsed = loop.time() - last_heartbeat

    # Нижняя граница строгая; верхняя — max + запас на точность таймеров (Windows ~15 мс)
    assert 0.15 <= elapsed <= 0.25 + 0.05


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

    expected = {"term": 1, "candidate_id": "n1", "last_log_index": 0, "last_log_term": 0}
    assert sorted(node.rpc_client.vote_calls, key=lambda c: c[0]) == [
        (peer_url("n2"), expected),
        (peer_url("n3"), expected),
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
        assert all(p["term"] == node.current_term and p["leader_id"] == "n1" for p in sent)
        # Первый запрос несёт no-op лидера, после подтверждения — пустые heartbeat'ы
        assert sent[0]["entries"] == [{"term": node.current_term, "command": None}]
        assert all(p["entries"] == [] and p["prev_log_index"] == 1 for p in sent[1:])


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
    assert node._replication_tasks == {}


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
    tasks = list(node._replication_tasks.values())
    assert len(tasks) == 2

    await node.stop()

    assert all(t.done() for t in tasks)
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
        "last_log_index": 1,  # no-op нового лидера
        "last_log_term": 1,
        "commit_index": 0,
        "last_applied": 0,
        "next_index": {"n2": 1, "n3": 1},
        "match_index": {"n2": 0, "n3": 0},
    }
