import asyncio
import logging
import random
from typing import Optional

from .config import Config
from .state import Role

log = logging.getLogger(__name__)


class RaftNode:
    def __init__(self, config: Config):
        self.config = config
        self.id = config.node_id

        # --- Persistent state ---
        self.current_term: int = 0
        self.voted_for: Optional[str] = None

        # --- Volatile state ---
        self.role: Role = Role.FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes_received: set[str] = set()

        # --- Таймеры ---
        # Одна задача на всё время жизни ноды; сброс таймера лишь сдвигает дедлайн
        self._election_timer: Optional[asyncio.Task] = None
        self._election_deadline: float = 0.0  # по часам event loop (loop.time())
        self._heartbeat_timer: Optional[asyncio.Task] = None

        self._lock = asyncio.Lock()
        self._shutdown = asyncio.Event()

        # Будет проставлен из main.py
        self.rpc_client = None

    # ---------- Утилиты ----------

    @property
    def quorum(self) -> int:
        total = len(self.config.peers) + 1
        return total // 2 + 1

    # ---------- Запуск/остановка ----------

    async def start(self) -> None:
        log.info("Node %s starting as %s (quorum=%d)", self.id, self.role.value, self.quorum)
        self._reset_election_timer()
        self._election_timer = asyncio.create_task(self._election_timer_task())

    async def stop(self) -> None:
        self._shutdown.set()
        for t in (self._election_timer, self._heartbeat_timer):
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    # ---------- Таймеры ----------

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

    async def _heartbeat_task(self) -> None:
        try:
            while self.role == Role.LEADER:
                await self._broadcast_append_entries()
                await asyncio.sleep(self.config.heartbeat_interval)
        except asyncio.CancelledError:
            pass

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

        log.info("Node %s starts election, term=%d", self.id, term)
        if self.rpc_client is None:
            log.error("Node %s: rpc_client is None!", self.id)
            return
        log.info("Node %s: broadcasting RequestVote to %s", self.id, list(self.config.peers.items()))

        if len(self.votes_received) >= self.quorum:
            await self._become_leader(term)
            return

        tasks = [
            asyncio.create_task(self._send_request_vote(peer_id, url, term))
            for peer_id, url in self.config.peers.items()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


    async def _send_request_vote(self, peer_id: str, peer_url: str, term: int) -> None:
        payload = {"term": term, "candidate_id": self.id}
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


    async def _become_leader(self, term: int) -> None:
        async with self._lock:
            await self._become_leader_locked(term)

    async def _become_leader_locked(self, term: int) -> None:
        """Вызывается, когда уже держим self._lock."""
        if self.role != Role.CANDIDATE or self.current_term != term:
            return
        self.role = Role.LEADER
        self.leader_id = self.id
        log.info("Node %s became LEADER, term=%d", self.id, term)

        # Таймер выборов не отменяем: у лидера он срабатывает вхолостую (см. _election_timer_task)
        if self._heartbeat_timer and not self._heartbeat_timer.done():
            self._heartbeat_timer.cancel()
        self._heartbeat_timer = asyncio.create_task(self._heartbeat_task())

        asyncio.create_task(self._broadcast_append_entries())


    async def _become_follower(self, term: int, leader_id: Optional[str]) -> None:
        async with self._lock:
            await self._become_follower_locked(term, leader_id)


    async def _become_follower_locked(self, term: int, leader_id: Optional[str]) -> None:
        """Вызывается, когда уже держим self._lock."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.role = Role.FOLLOWER
        self.leader_id = leader_id
        self.votes_received.clear()

        if self._heartbeat_timer and not self._heartbeat_timer.done():
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

        log.info("Node %s became FOLLOWER, term=%d, leader=%s", self.id, self.current_term, leader_id)

        self._reset_election_timer()

    # ---------- Heartbeat ----------

    async def _broadcast_append_entries(self) -> None:
        if self.role != Role.LEADER:
            return
        term = self.current_term
        tasks = [
            asyncio.create_task(self._send_append_entries(url, term))
            for peer_id, url in self.config.peers.items()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


    async def _send_append_entries(self, peer_url: str, term: int) -> None:
        payload = {"term": term, "leader_id": self.id}
        resp = await self.rpc_client.append_entries(peer_url, payload)
        if resp is None:
            return

        async with self._lock:
            if term != self.current_term or self.role != Role.LEADER:
                return
            remote_term = resp.get("term", 0)
            if remote_term > self.current_term:
                await self._become_follower_locked(remote_term, None)

    # ---------- Обработка RPC ----------

    async def handle_request_vote(self, term: int, candidate_id: str) -> dict:
        async with self._lock:
            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if term > self.current_term:
                # Сбрасывает voted_for, leader_id и останавливает heartbeat, если были лидером
                await self._become_follower_locked(term, None)

            vote_granted = False
            if self.voted_for in (None, candidate_id):
                self.voted_for = candidate_id
                vote_granted = True

            result = {"term": self.current_term, "vote_granted": vote_granted}

        if vote_granted:
            self._reset_election_timer()
            log.info("Node %s granted vote to %s, term=%d",
                     self.id, candidate_id, term)
        else:
            log.info("Node %s denied vote to %s, term=%d (voted_for=%s)",
                     self.id, candidate_id, term, self.voted_for)

        return result


    async def handle_append_entries(self, term: int, leader_id: str) -> dict:
        async with self._lock:
            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            if term > self.current_term or self.role != Role.FOLLOWER:
                # При равном терме voted_for сохраняется: кандидат уже голосовал
                # за себя в этом терме и не должен проголосовать второй раз
                await self._become_follower_locked(term, leader_id)

            self.leader_id = leader_id

        self._reset_election_timer()
        return {"term": self.current_term, "success": True}

    # ---------- Status ----------

    def status(self) -> dict:
        return {
            "id": self.id,
            "role": self.role.value,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "voted_for": self.voted_for,
            "votes_received": sorted(self.votes_received),
        }