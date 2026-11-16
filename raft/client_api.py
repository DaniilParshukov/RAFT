"""Клиентский REST API ноды: status, health и работа с логом."""
import json

from aiohttp import web

from .node import NotLeaderError


def build_client_app(node) -> web.Application:
    app = web.Application()

    async def status(request: web.Request) -> web.Response:
        return web.json_response(node.status())

    async def health(request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def show_log(request: web.Request) -> web.Response:
        return web.json_response(node.log_view())

    async def add_entry(request: web.Request) -> web.Response:
        """POST /log {"command": <любой JSON>}[?wait=false]

        200 — закоммичено; 202 — принято лидером без ожидания (wait=false);
        421 — не лидер (в ответе leader_id); 503 — запись затёрта сменой лидера;
        504 — не дождались коммита (запись может закоммититься позже).
        """
        try:
            body = await request.json()
        except json.JSONDecodeError:
            return _error(400, "invalid_json")
        if not isinstance(body, dict) or "command" not in body:
            return _error(400, "command_required")
        if body["command"] is None:
            return _error(400, "command_must_not_be_null")  # null зарезервирован под no-op лидера

        try:
            index, term = await node.append_command(body["command"])
        except NotLeaderError as e:
            return _error(421, "not_leader", leader_id=e.leader_id)

        entry = {"index": index, "term": term}
        if request.query.get("wait", "true").lower() in ("false", "0", "no"):
            return web.json_response({**entry, "committed": False}, status=202)

        try:
            committed = await node.wait_for_commit(index, term, node.config.commit_timeout)
        except TimeoutError:
            return _error(504, "commit_timeout", **entry)
        if not committed:
            return _error(503, "entry_lost", **entry)
        return web.json_response({**entry, "committed": True})

    app.router.add_get("/status", status)
    app.router.add_get("/health", health)
    app.router.add_get("/log", show_log)
    app.router.add_post("/log", add_entry)
    return app


def _error(status: int, error: str, **extra) -> web.Response:
    return web.json_response({"error": error, **extra}, status=status)
