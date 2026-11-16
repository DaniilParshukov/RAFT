import pytest

from raft.log import LogEntry, RaftLog


def make_log(*terms: int) -> RaftLog:
    log = RaftLog()
    for i, term in enumerate(terms, start=1):
        log.append(LogEntry(term, f"c{i}"))
    return log


def terms(log: RaftLog) -> list[int]:
    return [e["term"] for e in log.to_list()]


def test_empty_log():
    log = RaftLog()
    assert log.last_index == 0
    assert log.last_term == 0
    assert log.term_at(0) == 0
    assert log.term_at(1) is None


def test_indexes_start_from_one():
    log = make_log(1, 1, 2)
    assert log.last_index == 3
    assert log.last_term == 2
    assert log.get(1) == LogEntry(1, "c1")
    assert [log.term_at(i) for i in range(5)] == [0, 1, 1, 2, None]
    with pytest.raises(IndexError):
        log.get(0)


def test_entries_from():
    log = make_log(1, 1, 2, 3)
    assert [e.command for e in log.entries_from(2)] == ["c2", "c3", "c4"]
    assert [e.command for e in log.entries_from(2, limit=2)] == ["c2", "c3"]
    assert log.entries_from(5) == []


@pytest.mark.parametrize("prev_index, prev_term, ok", [
    (0, 0, True),
    (2, 1, True),
    (3, 2, True),
    (3, 1, False),  # терм не совпал
    (4, 2, False),  # записи нет
])
def test_matches(prev_index, prev_term, ok):
    assert make_log(1, 1, 2).matches(prev_index, prev_term) is ok


def test_merge_appends_to_end():
    log = make_log(1)
    truncated = log.merge(1, [LogEntry(1, "a"), LogEntry(2, "b")])
    assert not truncated
    assert terms(log) == [1, 1, 2]


def test_merge_truncates_conflicting_suffix():
    log = make_log(1, 1, 2, 2)
    truncated = log.merge(2, [LogEntry(3, "new")])
    assert truncated
    assert terms(log) == [1, 1, 3]
    assert log.get(3).command == "new"


def test_merge_keeps_entries_on_stale_or_duplicate_request():
    log = make_log(1, 1, 1, 1)
    # Запоздавший AppendEntries с частью уже принятых записей не должен обрезать хвост
    truncated = log.merge(1, [LogEntry(1, "c2")])
    assert not truncated
    assert log.last_index == 4


def test_merge_skips_matching_prefix_then_appends():
    log = make_log(1, 1)
    log.merge(0, [LogEntry(1, "c1"), LogEntry(1, "c2"), LogEntry(2, "c3")])
    assert terms(log) == [1, 1, 2]


def test_first_index_of_term():
    log = make_log(1, 2, 2, 2, 3)
    assert log.first_index_of_term(4) == 2
    assert log.first_index_of_term(2) == 2
    assert log.first_index_of_term(1) == 1
    assert log.first_index_of_term(5) == 5


@pytest.mark.parametrize("cand_index, cand_term, ok", [
    (3, 2, True),   # тот же лог
    (5, 2, True),   # длиннее в том же терме
    (2, 2, False),  # короче в том же терме
    (1, 3, True),   # старший терм важнее длины
    (9, 1, False),  # младший терм — длина не спасает
])
def test_candidate_is_up_to_date(cand_index, cand_term, ok):
    assert make_log(1, 1, 2).candidate_is_up_to_date(cand_index, cand_term) is ok


def test_entry_roundtrip():
    entry = LogEntry(3, {"set": "x", "value": [1, 2]})
    assert LogEntry.from_dict(entry.to_dict()) == entry
