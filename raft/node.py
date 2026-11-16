import asyncio
import logging
import random
from typing import Any, Optional

from .config import Config
from .log import LogEntry, RaftLog
from .state import Role

log = logging.getLogger(__name__)


class NotLeaderError(Exception):
    """Клиентская запись пришла не на лидера."""

    def __init__(self, leader_id: Optional[str]):
        super().__init__(f"not a leader (leader_id={leader_id})")
        self.leader_id = leader_id


class RaftNode:
    def __init__(self, config: Config):
        self.config = config
        self.id = config.node_id

        # --- Persistent state (пока только в памяти) ---
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log = RaftLog()

        # --- Volatile state ---
        self.role: Role = Role.FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes_received: set[str] = set()
        self.commit_index: int = 0
        self.last_applied: int = 0

        # --- Volatile state лидера (пересоздаётся на каждое лидерство) ---
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # --- Таймеры ---
        # Одна задача на всё время жизни ноды; сброс таймера лишь сдвигает дедлайн
        self._election_timer: Optional[asyncio.Task] = None
        self._election_deadline: float = 0.0  # по часам event loop (loop.time())
        # По задаче репликации на пира, пока мы лидер; wakeup будит её раньше heartbeat'а
        self._replication_tasks: dict[str, asyncio.Task] = {}
        self._replication_wakeup: dict[str, asyncio.Event] = {}

        # Срабатывает (и заменяется новым) при каждом росте commit_index или обрезке лога
        self._commit_changed = asyncio.Event()

        self._lock = asyncio.Lock()

        # Проставляется снаружи (NodeServer или тесты)
        self.rpc_client = None

    # ---------- Утилиты ----------

    @property
    def quorum(self) -> int:
        total = len(self.config.peers) + 1
        return total // 2 + 1

    @staticmethod
    async def _wait_event(event: asyncio.Event, timeout: float) -> None:
        try:
            async with asyncio.timeout(timeout):
                await event.wait()
        except TimeoutError:
            pass

    # ---------- Запуск/остановка ----------

    async def start(self) -> None:
        log.info("Node %s starting as %s (quorum=%d)", self.id, self.role.value, self.quorum)
        self._reset_election_timer()
        self._election_timer = asyncio.create_task(self._election_timer_task())

    async def stop(self) -> None:
        tasks = [t for t in (self._election_timer, *self._replication_tasks.values()) if t]
        self._replication_tasks.clear()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # ---------- Таймер выборов ----------

    def _random_election_timeout(self) -> float:
        return random.uniform(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        )

    def _reset_election_timer(self) -> None:
        """O(1): только переносит дедлайн, задачу не трогает."""
        loop = asyncio.get_running_loop()
        self._election_deadline = loop.time() + self._random_election_timeout()

    async def _election_timer_task(self) -> None:
        """Спит до дедлайна; если за это время дедлайн сдвинули — досыпает до нового.

        Сон не бывает длиннее election_timeout_max (дедлайн всегда ставится не дальше
        now + max), поэтому таймер срабатывает в интервале [min, max] после последнего
        сброса, даже если новый дедлайн оказался раньше того, до которого уже спим.
        """
        loop = asyncio.get_running_loop()
        while True:
            delay = self._election_deadline - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
                continue

            if self.role == Role.LEADER:
                # Лидеру выборы не нужны — просто взводим таймер заново
                self._reset_election_timer()
                continue

            try:
                await self._start_election()
            except Exception:
                # Не даём упасть единственной задаче таймера — иначе нода больше никогда не начнёт выборы
                log.exception("Node %s: election failed", self.id)
                self._reset_election_timer()

    # ---------- Выборы ----------

    async def _start_election(self) -> None:
        async with self._lock:
            self.role = Role.CANDIDATE
            self.current_term += 1
            self.voted_for = self.id
            self.votes_received = {self.id}
            self.leader_id = None
            term = self.current_term
            # Если голосов не хватит — по этому дедлайну начнём следующие выборы
            self._reset_election_timer()
            payload = {
                "term": term,
                "candidate_id": self.id,
                "last_log_index": self.log.last_index,
                "last_log_term": self.log.last_term,
            }

            log.info("Node %s starts election, term=%d", self.id, term)
            if len(self.votes_received) >= self.quorum:  # кластер из одной ноды
                await self._become_leader_locked(term)
                return

        if self.rpc_client is None:
            log.error("Node %s: rpc_client is None!", self.id)
            return

        tasks = [
            asyncio.create_task(self._send_request_vote(peer_id, url, payload))
            for peer_id, url in self.config.peers.items()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_request_vote(self, peer_id: str, peer_url: str, payload: dict) -> None:
        term = payload["term"]
        resp = await self.rpc_client.request_vote(peer_url, payload)
        if resp is None:
            return

        async with self._lock:
            if term != self.current_term or self.role != Role.CANDIDATE:
                return

            remote_term = resp.get("term", 0)
            if remote_term > self.current_term:
                await self._become_follower_locked(remote_term, None)
                return

            if resp.get("vote_granted"):
                self.votes_received.add(peer_id)
                votes = len(self.votes_received)
                log.info("Node %s got vote from %s (%d/%d), term=%d", self.id, peer_id, votes, self.quorum, term)

                if votes >= self.quorum:
                    await self._become_leader_locked(term)

    # ---------- Смена роли (вызывать только под self._lock) ----------

    async def _become_leader_locked(self, term: int) -> None:
        if self.role != Role.CANDIDATE or self.current_term != term:
            return
        self.role = Role.LEADER
        self.leader_id = self.id
        self.next_index = {pid: self.log.last_index + 1 for pid in self.config.peers}
        self.match_index = {pid: 0 for pid in self.config.peers}

        # No-op в начале терма (§8): записи прошлых термов коммитятся только
        # вместе с записью текущего терма, иначе они могут висеть незакоммиченными
        self.log.append(LogEntry(term=term, command=None))
        log.info("Node %s became LEADER, term=%d, log=%d", self.id, term, self.log.last_index)

        self._advance_commit_locked()  # кластер из одной ноды коммитит сразу

        # Таймер выборов не отменяем: у лидера он срабатывает вхолостую (см. _election_timer_task)
        for peer_id, url in self.config.peers.items():
            wakeup = asyncio.Event()
            wakeup.set()  # первый AppendEntries — сразу, чтобы фолловеры узнали о лидере
            self._replication_wakeup[peer_id] = wakeup
            self._replication_tasks[peer_id] = asyncio.create_task(
                self._replication_task(peer_id, url, term, wakeup))

    async def _become_follower_locked(self, term: int, leader_id: Optional[str]) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        was = self.role
        self.role = Role.FOLLOWER
        self.leader_id = leader_id
        self.votes_received.clear()
        self._stop_replication_locked()

        if was != Role.FOLLOWER:
            log.info("Node %s became FOLLOWER, term=%d, leader=%s", self.id, self.current_term, leader_id)

        self._reset_election_timer()

    def _stop_replication_locked(self) -> None:
        for task in self._replication_tasks.values():
            task.cancel()
        self._replication_tasks.clear()
        self._replication_wakeup.clear()
        self.next_index.clear()
        self.match_index.clear()

    # ---------- Репликация (лидер) ----------

    async def _replication_task(self, peer_id: str, peer_url: str, term: int,
                                wakeup: asyncio.Event) -> None:
        """Шлёт пиру AppendEntries, пока мы лидер этого терма.

        Если пир отстаёт — следующий запрос уходит сразу, иначе ждём новую запись
        (wakeup) или heartbeat_interval, смотря что наступит раньше.
        """
        while True:
            async with self._lock:
                if self.role != Role.LEADER or self.current_term != term:
                    return
                wakeup.clear()  # до сборки payload, чтобы не потерять append между ними
                payload = self._append_entries_payload_locked(peer_id)

            behind = False
            try:
                resp = await self.rpc_client.append_entries(peer_url, payload)
                if resp is not None:
                    async with self._lock:
                        behind = await self._handle_append_response_locked(peer_id, term, payload, resp)
            except Exception:
                log.exception("Node %s: replication to %s failed", self.id, peer_id)

            if not behind:
                await self._wait_event(wakeup, self.config.heartbeat_interval)

    def _append_entries_payload_locked(self, peer_id: str) -> dict:
        next_index = self.next_index[peer_id]
        prev_index = next_index - 1
        entries = self.log.entries_from(next_index, self.config.max_entries_per_rpc)
        return {
            "term": self.current_term,
            "leader_id": self.id,
            "prev_log_index": prev_index,
            "prev_log_term": self.log.term_at(prev_index),
            "entries": [e.to_dict() for e in entries],
            "leader_commit": self.commit_index,
        }

    async def _handle_append_response_locked(self, peer_id: str, term: int,
                                             payload: dict, resp: dict) -> bool:
        """Обрабатывает ответ на AppendEntries. True — пир отстаёт, слать ещё сразу."""
        if self.role != Role.LEADER or self.current_term != term:
            return False

        remote_term = resp.get("term", 0)
        if remote_term > self.current_term:
            await self._become_follower_locked(remote_term, None)
            return False

        prev_index = payload["prev_log_index"]
        if resp.get("success"):
            # max(): ответы на старые запросы могут прийти позже новых
            match = prev_index + len(payload["entries"])
            self.match_index[peer_id] = max(self.match_index[peer_id], match)
            self.next_index[peer_id] = max(self.next_index[peer_id], self.match_index[peer_id] + 1)
            self._advance_commit_locked()
            return self.next_index[peer_id] <= self.log.last_index

        # Лог пира не совпал на prev_index — откатываем next_index, пользуясь подсказкой пира.
        # Запросы к пиру идут строго по одному, так что этот отказ — ответ на последний запрос.
        # prev_index >= 1 (с prev_index = 0 отказа не бывает), значит next_index строго убывает.
        new_next = max(1, min(resp.get("conflict_index", prev_index), prev_index))
        if new_next <= self.match_index[peer_id]:
            # Пир потерял подтверждённые записи — возможно при рестарте без персистентности
            log.warning("Node %s: peer %s lost entries after #%d, resending",
                        self.id, peer_id, new_next - 1)
            self.match_index[peer_id] = new_next - 1
        self.next_index[peer_id] = new_next
        return True

    def _advance_commit_locked(self) -> None:
        """Коммитит максимальный N, реплицированный на большинство, если log[N] из текущего терма (§5.4.2)."""
        if self.role != Role.LEADER:
            return
        for n in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(n) != self.current_term:
                break  # термы в логе не убывают — дальше только старые
            replicated = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if replicated >= self.quorum:
                self._set_commit_index_locked(n)
                break

    def _set_commit_index_locked(self, index: int) -> None:
        if index <= self.commit_index:
            return
        self.commit_index = index
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry.command is not None:
                log.info("Node %s applied #%d (term %d): %r",
                         self.id, self.last_applied, entry.term, entry.command)
        self._notify_commit_waiters()

    def _notify_commit_waiters(self) -> None:
        changed, self._commit_changed = self._commit_changed, asyncio.Event()
        changed.set()

    # ---------- Обработка RPC ----------

    async def handle_request_vote(self, term: int, candidate_id: str,
                                  last_log_index: int = 0, last_log_term: int = 0) -> dict:
        async with self._lock:
            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if term > self.current_term:
                # Сбрасывает voted_for, leader_id и останавливает репликацию, если были лидером
                await self._become_follower_locked(term, None)

            up_to_date = self.log.candidate_is_up_to_date(last_log_index, last_log_term)
            vote_granted = up_to_date and self.voted_for in (None, candidate_id)
            if vote_granted:
                self.voted_for = candidate_id
                self._reset_election_timer()

            result = {"term": self.current_term, "vote_granted": vote_granted}

        if vote_granted:
            log.info("Node %s granted vote to %s, term=%d", self.id, candidate_id, term)
        else:
            log.info("Node %s denied vote to %s, term=%d (voted_for=%s, log_ok=%s)",
                     self.id, candidate_id, term, self.voted_for, up_to_date)
        return result

    async def handle_append_entries(self, term: int, leader_id: str,
                                    prev_log_index: int = 0, prev_log_term: int = 0,
                                    entries: Optional[list[dict]] = None,
                                    leader_commit: int = 0) -> dict:
        """Значения по умолчанию — пустой heartbeat от начала лога."""
        async with self._lock:
            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            if term > self.current_term or self.role != Role.FOLLOWER:
                # При равном терме voted_for сохраняется: кандидат уже голосовал
                # за себя в этом терме и не должен проголосовать второй раз
                await self._become_follower_locked(term, leader_id)

            self.leader_id = leader_id
            self._reset_election_timer()

            if not self.log.matches(prev_log_index, prev_log_term):
                return {
                    "term": self.current_term,
                    "success": False,
                    "conflict_index": self._conflict_index_locked(prev_log_index),
                }

            new_entries = [LogEntry.from_dict(e) for e in entries or ()]
            if self.log.merge(prev_log_index, new_entries):
                log.info("Node %s truncated conflicting log suffix after #%d", self.id, prev_log_index)
                self._notify_commit_waiters()  # ждущие клиенты должны узнать, что запись затёрта

            if leader_commit > self.commit_index:
                # Коммитим не дальше того, что точно совпадает с логом лидера
                self._set_commit_index_locked(min(leader_commit, prev_log_index + len(new_entries)))

            return {"term": self.current_term, "success": True}

    def _conflict_index_locked(self, prev_log_index: int) -> int:
        """Откуда лидеру стоит продолжить: конец нашего лога или начало конфликтующего терма."""
        if prev_log_index > self.log.last_index:
            return self.log.last_index + 1
        return self.log.first_index_of_term(prev_log_index)

    # ---------- Клиентский API ----------

    async def append_command(self, command: Any) -> tuple[int, int]:
        """Дописывает команду в лог лидера. Возвращает (index, term); NotLeaderError на не-лидере."""
        async with self._lock:
            if self.role != Role.LEADER:
                raise NotLeaderError(self.leader_id)
            term = self.current_term
            index = self.log.append(LogEntry(term=term, command=command))
            for wakeup in self._replication_wakeup.values():
                wakeup.set()
            self._advance_commit_locked()  # кластер из одной ноды
        return index, term

    async def wait_for_commit(self, index: int, term: int, timeout: float) -> bool:
        """True — запись (index, term) закоммичена; False — затёрта другим лидером.

        TimeoutError — за timeout не решилось (запись может закоммититься позже).
        """
        async with asyncio.timeout(timeout):
            while True:
                changed = self._commit_changed  # берём до проверки, чтобы не пропустить сигнал
                if self.log.term_at(index) != term:
                    return False
                if self.commit_index >= index:
                    return True
                await changed.wait()

    def log_view(self) -> dict:
        return {
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "entries": [
                {**e, "committed": e["index"] <= self.commit_index}
                for e in self.log.to_list()
            ],
        }

    # ---------- Status ----------

    def status(self) -> dict:
        return {
            "id": self.id,
            "role": self.role.value,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "voted_for": self.voted_for,
            "votes_received": sorted(self.votes_received),
            "last_log_index": self.log.last_index,
            "last_log_term": self.log.last_term,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "next_index": dict(self.next_index),
            "match_index": dict(self.match_index),
        }
