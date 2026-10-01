#!/usr/bin/env python3
"""bench_http_overhead.py — HTTP 节点连接策略开销对照。

两层输出（2026-10 BFF 评审要求，防止 loopback 稀释结论）：
- 层 1（loopback CPU 侧）：同 host 连续调用，作用域共享 session（新）vs
  一次性 session（≈旧行为）的纯框架开销差；
- 层 2（注入 RTT）：服务端延迟 50ms，对比两种策略的墙钟差——这是生产
  公网/跨机房收益的量级代表（每跳省 TCP+TLS 握手 + 1-RTT）。

声明：DNS-TTL 收益在 loopback 上不可测（无真实解析），由 keepalive 间接
覆盖，本脚本不直接测量。

用法：python benchmarks/bench_http_overhead.py
"""

from __future__ import annotations

import asyncio
import http.server
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import plaita  # noqa: E402

from plaita.core.http_session import (  # noqa: E402
    close_flow_session,
    get_flow_session,
    open_flow_session,
)
from plaita.node.http import HttpExecutor  # noqa: E402

print(f"plaita : {plaita.__file__}")

DELAY_SECS = float(os.environ.get("BENCH_HTTP_DELAY", "0"))


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    # macOS loopback：header/body 分次写会撞 delayed-ACK×Nagle ~40ms 驻点，
    # 把每调用的真实开销淹没——keepalive 路径必须禁 Nagle。
    disable_nagle_algorithm = True

    def do_GET(self):
        if DELAY_SECS:
            time.sleep(DELAY_SECS)
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def _executor(port: int) -> HttpExecutor:
    return HttpExecutor(url=f"http://127.0.0.1:{port}/bench", method="GET",
                        query=None, body=None, headers=None,
                        addressing=None, delegate=None)


async def bench_in_scope(executor: HttpExecutor, n: int) -> float:
    """flow 作用域共享 session（新行为）：一次 open，n 次调用复用。"""
    await open_flow_session()
    try:
        t0 = time.perf_counter()
        for _ in range(n):
            rsp, err = await executor.handle_request_async(None)
            assert err is None, err
        return time.perf_counter() - t0
    finally:
        await close_flow_session()


async def bench_oneshot(executor: HttpExecutor, n: int) -> float:
    """每调用一次性 session（≈旧行为）：脱离 flow 驱动时节点走兜底路径。"""
    t0 = time.perf_counter()
    for _ in range(n):
        session = get_flow_session()
        assert session is None
        rsp, err = await executor.handle_request_async(None)
        assert err is None, err
    return time.perf_counter() - t0


async def main_async() -> None:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = server.server_address[1]
    executor = _executor(port)
    n = 200
    try:
        # warmup
        await bench_in_scope(executor, 20)
        t_shared = await bench_in_scope(executor, n)
        t_oneshot = await bench_oneshot(executor, n)

        per_shared = t_shared / n * 1e3
        per_oneshot = t_oneshot / n * 1e3
        print(f"\n[层1 loopback CPU 侧] {n} 次同 host GET")
        print(f"  作用域共享 session : {per_shared:8.2f} ms/调用")
        print(f"  一次性 session     : {per_oneshot:8.2f} ms/调用")
        print(f"  每调用节省         : {per_oneshot - per_shared:8.2f} ms "
              f"(loopback 下限；公网 TCP+TLS 握手为 30~100ms 量级)")

        if DELAY_SECS:
            n2 = 30
            await bench_in_scope(executor, 5)
            t2_shared = await bench_in_scope(executor, n2)
            t2_oneshot = await bench_oneshot(executor, n2)
            print(f"\n[层2 注入 RTT {DELAY_SECS*1000:.0f}ms] {n2} 次调用墙钟")
            print(f"  作用域共享 session : {t2_shared:8.2f} s 总耗时")
            print(f"  一次性 session     : {t2_oneshot:8.2f} s 总耗时")
            print(f"  墙钟节省           : {(t2_oneshot - t2_shared) / n2 * 1e3:8.2f} ms/调用")
        else:
            print("\n(层2 未跑：设 BENCH_HTTP_DELAY=0.05 注入 50ms 服务端延迟)")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    asyncio.run(main_async())
