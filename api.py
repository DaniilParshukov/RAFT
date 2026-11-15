from aiohttp import web


def build_status_app(node) -> web.Application:
    app = web.Application()

    async def status(request: web.Request) -> web.Response:
        return web.json_response(node.status())

    async def health(request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    app.router.add_get("/status", status)
    app.router.add_get("/health", health)
    return app