import logging
from aiohttp import web

log = logging.getLogger(__name__)


def build_rpc_app(node) -> web.Application:
    app = web.Application()

    async def request_vote(request: web.Request) -> web.Response:
        data = await request.json()
        result = await node.handle_request_vote(
            term=data["term"],
            candidate_id=data["candidate_id"],
            last_log_index=data["last_log_index"],
            last_log_term=data["last_log_term"],
        )
        return web.json_response(result)

    async def append_entries(request: web.Request) -> web.Response:
        data = await request.json()
        result = await node.handle_append_entries(
            term=data["term"],
            leader_id=data["leader_id"],
            prev_log_index=data["prev_log_index"],
            prev_log_term=data["prev_log_term"],
            entries=data["entries"],
            leader_commit=data["leader_commit"],
        )
        return web.json_response(result)

    app.router.add_post("/raft/request_vote", request_vote)
    app.router.add_post("/raft/append_entries", append_entries)
    return app
