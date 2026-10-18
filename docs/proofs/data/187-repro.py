#!/usr/bin/env python3
"""plaita#187 repro / before-after probe.

Run with ``PYTHONPATH`` pointing at the tree under test::

    PYTHONPATH=/tmp/w35 /Users/jeff.jie/.venvs/plaita/bin/python \
        docs/proofs/data/187-repro.py

Three scenarios (local aiohttp client + stdlib HTTP server, no external net):

A. nested scope: a child flow's ``_flow_session_scoped`` must not close the
   parent's shared session (issue's exact probe);
B. concurrent branches: branch A ends while branch B's request is still in
   flight — B's request must succeed (before fix: "Session is closed");
C. root scope: when the root scope ends the session is closed (no leak).
"""
from __future__ import annotations

import asyncio
import http.server
import json
import threading
import time

from plaita.core.async_utils import _flow_session_scoped
from plaita.core.http_session import (
    close_flow_session,
    get_flow_session,
    open_flow_session,
)


class _SlowHandler(http.server.BaseHTTPRequestHandler):
    delay = 0.0

    def do_GET(self):  # noqa: N802 — stdlib handler API
        time.sleep(self.delay)
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # 静音
        pass


def _start_server(delay: float):
    handler = type("_Handler", (_SlowHandler,), {"delay": delay})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


async def _nested_scope() -> dict:
    created = await open_flow_session()
    outer = get_flow_session()
    await _flow_session_scoped(asyncio.sleep(0))
    still_same = get_flow_session() is outer
    closed = outer.closed
    await close_flow_session()
    await outer.close()
    return {
        "created": created,
        "contextvar_kept": still_same,
        "outer_closed_by_child": closed,
    }


async def _concurrent_branches() -> dict:
    server = _start_server(delay=0.6)
    url = f"http://127.0.0.1:{server.server_address[1]}/slow"
    try:
        async def branch_b():
            session = get_flow_session()
            async with session.get(url) as rsp:
                return {"status": rsp.status, "body": await rsp.json()}

        async def branch_a():
            await asyncio.sleep(0.15)  # let B's request get in flight
            await _flow_session_scoped(asyncio.sleep(0))
            return "short-branch-done"

        async def both():
            # 真实形态：父 flow 驱动已开 scope，Parallel 分支任务继承它
            return await asyncio.gather(
                _flow_session_scoped(branch_a()),
                _flow_session_scoped(branch_b()),
                return_exceptions=True,
            )

        outcome = {}
        results = await _flow_session_scoped(both())
        for name, value in zip(("A", "B"), results):
            if isinstance(value, BaseException):
                outcome[name] = f"{type(value).__name__}: {value}"
            else:
                outcome[name] = value
        return outcome
    finally:
        server.shutdown()
        server.server_close()


async def _root_scope() -> dict:
    await _flow_session_scoped(asyncio.sleep(0))
    return {"contextvar_after_root": get_flow_session() is None}


def run() -> dict:
    return {
        "plaita_file": __import__("plaita").__file__,
        "nested": asyncio.run(_nested_scope()),
        "concurrent": asyncio.run(_concurrent_branches()),
        "root": asyncio.run(_root_scope()),
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
