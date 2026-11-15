import aiohttp
import logging

log = logging.getLogger(__name__)


class RpcClient:
    def __init__(self, timeout: float = 0.100):
        self.timeout = timeout
        self._session: aiohttp.ClientSession | None = None

    async def start(self):
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout)
        )

    async def stop(self):
        if self._session:
            await self._session.close()

    async def request_vote(self, peer_url: str, payload: dict) -> dict | None:
        return await self._post(f"{peer_url}/raft/request_vote", payload)

    async def append_entries(self, peer_url: str, payload: dict) -> dict | None:
        return await self._post(f"{peer_url}/raft/append_entries", payload)

    async def _post(self, url: str, payload: dict) -> dict | None:
        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    log.warning("RPC %s returned %d", url, resp.status)
                    return None
                return await resp.json()
        except Exception as e:
            log.debug("RPC %s failed: %s", url, e)
            return None