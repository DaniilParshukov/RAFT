from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: Any  # любое JSON-значение; None — служебная no-op запись лидера

    def to_dict(self) -> dict:
        return {"term": self.term, "command": self.command}

    @classmethod
    def from_dict(cls, data: dict) -> "LogEntry":
        return cls(term=data["term"], command=data.get("command"))


class RaftLog:
    """Журнал Raft. Индексы с 1, как в статье; индекс 0 — «пустой» префикс с термом 0."""

    def __init__(self) -> None:
        self._entries: list[LogEntry] = []

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def last_index(self) -> int:
        return len(self._entries)

    @property
    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def term_at(self, index: int) -> Optional[int]:
        """Терм записи по индексу; 0 для индекса 0; None, если записи нет."""
        if index == 0:
            return 0
        if 1 <= index <= len(self._entries):
            return self._entries[index - 1].term
        return None

    def get(self, index: int) -> LogEntry:
        if not 1 <= index <= len(self._entries):
            raise IndexError(index)
        return self._entries[index - 1]

    def append(self, entry: LogEntry) -> int:
        self._entries.append(entry)
        return len(self._entries)

    def entries_from(self, index: int, limit: Optional[int] = None) -> list[LogEntry]:
        """Записи начиная с index (включительно)."""
        start = max(index, 1) - 1
        end = None if limit is None else start + limit
        return self._entries[start:end]

    def matches(self, prev_index: int, prev_term: int) -> bool:
        """Проверка согласованности AppendEntries (§5.3, шаг 2)."""
        return self.term_at(prev_index) == prev_term

    def merge(self, prev_index: int, entries: list[LogEntry]) -> bool:
        """Вливает записи лидера после prev_index (§5.3, шаги 3–4).

        Хвост обрезается только при конфликте термов — повторный или запоздавший
        AppendEntries не должен стирать уже принятые записи. Возвращает True, если обрезали.
        """
        truncated = False
        for offset, entry in enumerate(entries):
            index = prev_index + 1 + offset
            existing = self.term_at(index)
            if existing == entry.term:
                continue
            if existing is not None:
                del self._entries[index - 1:]
                truncated = True
            self._entries.extend(entries[offset:])
            break
        return truncated

    def first_index_of_term(self, index: int) -> int:
        """Первый индекс непрерывного блока записей с тем же термом, что и у index."""
        term = self.term_at(index)
        while index > 1 and self.term_at(index - 1) == term:
            index -= 1
        return index

    def candidate_is_up_to_date(self, last_index: int, last_term: int) -> bool:
        """Лог кандидата не отстаёт от нашего (§5.4.1)."""
        if last_term != self.last_term:
            return last_term > self.last_term
        return last_index >= self.last_index

    def to_list(self) -> list[dict]:
        return [{"index": i, **e.to_dict()} for i, e in enumerate(self._entries, start=1)]
