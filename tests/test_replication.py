"""Репликация лога: фолловер (AppendEntries), лидер (commit), и сценарии на in-memory кластере."""
import asyncio

import pytest

from raft.node import NotLeaderError
from raft.state import Role
from tests.helpers import commands, grant_votes_from, peer_url, wait_until


def entries(*terms: int, prefix: str = "c") -> list[dict]:
    return [{"term": t, "command": f"{prefix}{i}"} for i, t in enumerate(terms, start=1)]


def log_terms(node) -> list[int]:
    return [e["term"] for e in node.log.to_list()]


def ack(url, payload):
    return {"term": payload["term"], "success": True}


async def become_leader(node) -> None:
    node.rpc_client.vote_handler = grant_votes_from(*node.config.peers)
    await node._start_election()
    assert node.role == Role.LEADER


# ---------- Фолловер: AppendEntries ----------

async def test_follower_appends_entries(make_node):
    node = make_node()

    resp = await node.handle_append_entries(term=1, leader_id="n2", prev_log_index=0, prev_log_term=0,
                                            entries=entries(1, 1), leader_commit=0)

    assert resp == {"term": 1, "success": True}
    assert commands(node) == ["c1", "c2"]
    assert node.commit_index == 0


async def test_follower_rejects_when_prev_entry_missing(make_node):
    node = make_node()
    await node.handle_append_entries(term=1, leader_id="n2", entries=entries(1))

    resp = await node.handle_append_entries(term=1, leader_id="n2", prev_log_index=5, prev_log_term=1,
                                            entries=entries(1))

    assert resp == {"term": 1, "success": False, "conflict_index": 2}
    assert node.log.last_index == 1
    assert node.leader_id == "n2"  # лидера всё равно признаём


async def test_follower_rejects_on_prev_term_mismatch(make_node):
    node = make_node()
    await node.handle_append_entries(term=2, leader_id="n2", entries=entries(1, 2, 2))

    resp = await node.handle_append_entries(term=3, leader_id="n3", prev_log_index=3, prev_log_term=3,
                                            entries=entries(3))

    # Подсказка — начало блока конфликтующего терма 2
    assert resp == {"term": 3, "success": False, "conflict_index": 2}
    assert log_terms(node) == [1, 2, 2]


async def test_follower_overwrites_conflicting_suffix(make_node):
    node = make_node()
    await node.handle_append_entries(term=1, leader_id="n2", entries=entries(1, 1, 1))

    resp = await node.handle_append_entries(term=2, leader_id="n3", prev_log_index=1, prev_log_term=1,
                                            entries=entries(2, prefix="new"))

    assert resp["success"] is True
    assert log_terms(node) == [1, 2]
    assert commands(node) == ["c1", "new1"]


async def test_follower_ignores_stale_append_entries(make_node):
    node = make_node()
    await node.handle_append_entries(term=1, leader_id="n2", entries=entries(1, 1, 1))

    # Запоздавший дубликат более раннего запроса с одной записью
    resp = await node.handle_append_entries(term=1, leader_id="n2", entries=entries(1))

    assert resp["success"] is True
    assert node.log.last_index == 3


async def test_follower_advances_commit_index(make_node):
    node = make_node()

    await node.handle_append_entries(term=1, leader_id="n2", entries=entries(1, 1, 1), leader_commit=2)
    assert (node.commit_index, node.last_applied) == (2, 2)

    await node.handle_append_entries(term=1, leader_id="n2", prev_log_index=3, prev_log_term=1,
                                     leader_commit=5)
    assert (node.commit_index, node.last_applied) == (3, 3)  # не дальше своего лога


async def test_follower_does_not_commit_unverified_entries(make_node):
    """leader_commit применяется только к префиксу, совпадение которого подтвердил этот запрос."""
    node = make_node()
    await node.handle_append_entries(term=1, leader_id="n2", entries=entries(1, 1, 1))

    # Новый лидер подтвердил только запись #1; #2–#3 у нас от старого лидера и могут быть затёрты
    await node.handle_append_entries(term=2, leader_id="n3", prev_log_index=1, prev_log_term=1,
                                     leader_commit=3)

    assert node.commit_index == 1


# ---------- RequestVote с учётом лога ----------

async def follower_with_log(make_node, *terms):
    node = make_node()
    await node.handle_append_entries(term=max(terms), leader_id="n2", entries=entries(*terms))
    return node


@pytest.mark.parametrize("last_index, last_term, granted", [
    (5, 1, False),  # младший последний терм
    (1, 2, False),  # тот же терм, лог короче
    (2, 2, True),   # такой же лог
    (1, 3, True),   # старший последний терм
])
async def test_vote_depends_on_candidate_log(make_node, last_index, last_term, granted):
    node = await follower_with_log(make_node, 1, 2)

    resp = await node.handle_request_vote(term=5, candidate_id="n3",
                                          last_log_index=last_index, last_log_term=last_term)

    assert resp == {"term": 5, "vote_granted": granted}
    assert node.current_term == 5  # терм обновляем даже при отказе
    assert node.voted_for == ("n3" if granted else None)


# ---------- Лидер ----------

async def test_append_command_on_follower_raises(make_node):
    node = make_node()
    await node.handle_append_entries(term=1, leader_id="n2")

    with pytest.raises(NotLeaderError) as exc:
        await node.append_command("x")

    assert exc.value.leader_id == "n2"
    assert node.log.last_index == 0


async def test_leader_appends_noop_on_election(make_node):
    node = make_node()
    await become_leader(node)

    assert node.log.to_list() == [{"index": 1, "term": 1, "command": None}]
    assert node.next_index == {"n2": 1, "n3": 1}


async def test_leader_commits_with_majority(make_node):
    node = make_node()
    # n3 недоступен, n2 подтверждает — 2 из 3
    node.rpc_client.append_handler = lambda url, p: ack(url, p) if url == peer_url("n2") else None
    await become_leader(node)

    index, term = await node.append_command("x")

    assert await node.wait_for_commit(index, term, timeout=1.0) is True
    assert (node.commit_index, node.last_applied) == (2, 2)
    assert node.match_index == {"n2": 2, "n3": 0}


async def test_leader_does_not_commit_without_majority(make_node):
    node = make_node()
    await become_leader(node)  # append_handler по умолчанию — «нет ответа»

    index, term = await node.append_command("x")

    with pytest.raises(TimeoutError):
        await node.wait_for_commit(index, term, timeout=0.2)
    assert node.commit_index == 0


async def test_single_node_commits_immediately(make_node):
    node = make_node(peer_ids=(), election_timeout=(0.03, 0.05))
    await node.start()
    await wait_until(lambda: node.role == Role.LEADER)

    index, term = await node.append_command("x")

    assert node.commit_index == index


async def test_leader_does_not_commit_previous_term_entry_by_counting(make_node):
    """§5.4.2 (Figure 8): запись старого терма коммитится только вместе с записью текущего."""
    node = make_node()
    await node.handle_append_entries(term=1, leader_id="n2", entries=entries(1))
    await become_leader(node)  # терм 2, лог: [old(1), no-op(2)]

    async with node._lock:
        node.match_index.update(n2=1, n3=1)  # старая запись есть у всех
        node._advance_commit_locked()
    assert node.commit_index == 0

    async with node._lock:
        node.match_index["n2"] = 2  # no-op текущего терма на большинстве
        node._advance_commit_locked()
    assert node.commit_index == 2


async def test_new_entry_sent_without_waiting_for_heartbeat(make_node):
    node = make_node(heartbeat_interval=5.0)
    node.rpc_client.append_handler = ack
    await become_leader(node)
    await wait_until(lambda: node.commit_index == 1)  # no-op ушёл сразу после избрания

    index, term = await node.append_command("x")

    assert await node.wait_for_commit(index, term, timeout=0.5) is True


async def test_entries_are_batched(make_node):
    node = make_node(max_entries_per_rpc=3)
    await become_leader(node)
    for i in range(10):
        await node.append_command(i)

    node.rpc_client.append_handler = ack
    await wait_until(lambda: node.match_index == {"n2": 11, "n3": 11})

    sizes = [len(p["entries"]) for _, p in node.rpc_client.append_calls]
    assert max(sizes) == 3
    assert node.commit_index == 11


async def test_wait_for_commit_reports_overwritten_entry(make_node):
    node = make_node()
    await become_leader(node)  # терм 1, лог: [no-op]
    index, term = await node.append_command("lost")
    waiter = asyncio.create_task(node.wait_for_commit(index, term, timeout=2.0))
    await asyncio.sleep(0.01)

    # Новый лидер терма 2 затирает нашу незакоммиченную запись
    await node.handle_append_entries(term=2, leader_id="n2", prev_log_index=1, prev_log_term=1,
                                     entries=entries(2, prefix="new"))

    assert await waiter is False
    assert commands(node) == ["new1"]


# ---------- Сценарии на in-memory кластере ----------

async def test_logs_converge(local_cluster):
    cluster = await local_cluster(3)
    leader = await cluster.wait_for_leader()

    for i in range(20):
        index, term = await leader.append_command(i)
    assert await leader.wait_for_commit(index, term, timeout=2.0)

    await cluster.wait_converged()
    for node in cluster.nodes.values():
        assert commands(node) == list(range(20))


async def test_lagging_follower_catches_up(local_cluster):
    cluster = await local_cluster(3)
    leader = await cluster.wait_for_leader()
    lagging = cluster.others(leader)[0]

    cluster.network.isolate(lagging.id)
    for i in range(10):
        index, term = await leader.append_command(i)
    assert await leader.wait_for_commit(index, term, timeout=2.0)  # хватает лидера + второго
    assert commands(lagging) == []

    cluster.network.heal()
    await cluster.wait_converged()
    assert commands(lagging) == list(range(10))


async def test_partitioned_leader_entries_are_overwritten(local_cluster):
    cluster = await local_cluster(3)
    old = await cluster.wait_for_leader()

    cluster.network.isolate(old.id)
    for i in range(30):  # длинный конфликтующий хвост — next_index должен откатиться
        index, term = await old.append_command(f"lost{i}")
    lost = asyncio.create_task(old.wait_for_commit(index, term, timeout=5.0))

    new = await cluster.wait_for_leader(among=cluster.others(old))
    index, term = await new.append_command("kept")
    assert await new.wait_for_commit(index, term, timeout=2.0)

    cluster.network.heal()
    assert await lost is False
    await cluster.wait_converged()
    for node in cluster.nodes.values():
        assert commands(node) == ["kept"]


async def test_leader_in_minority_cannot_commit(local_cluster):
    cluster = await local_cluster(3)
    leader = await cluster.wait_for_leader()
    commit_before = leader.commit_index
    cluster.network.isolate(*[n.id for n in cluster.others(leader)])

    index, term = await leader.append_command("x")

    with pytest.raises(TimeoutError):
        await leader.wait_for_commit(index, term, timeout=0.3)
    assert leader.commit_index == commit_before


async def test_node_with_stale_log_cannot_become_leader(local_cluster):
    cluster = await local_cluster(3)
    leader = await cluster.wait_for_leader()
    stale, up_to_date = cluster.others(leader)

    cluster.network.isolate(stale.id)
    for i in range(5):
        index, term = await leader.append_command(i)
    assert await leader.wait_for_commit(index, term, timeout=2.0)

    await cluster.kill(leader.id)
    cluster.network.heal()

    new_leader = await cluster.wait_for_leader()
    assert new_leader is up_to_date  # у stale нет закоммиченных записей — голос ему не дадут
    await cluster.wait_converged()
    assert commands(stale) == list(range(5))


async def test_restarted_node_gets_full_log(local_cluster):
    cluster = await local_cluster(3)
    leader = await cluster.wait_for_leader()
    for i in range(5):
        index, term = await leader.append_command(i)
    assert await leader.wait_for_commit(index, term, timeout=2.0)
    follower = cluster.others(leader)[0]

    await cluster.kill(follower.id)
    restarted = await cluster.start(follower.id)  # состояние только в памяти — стартует с пустым логом

    await cluster.wait_converged()
    assert commands(restarted) == list(range(5))
