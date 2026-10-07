"""plaita#24 评审阻断项回归：分支执行体不得继承父 flow 的 HTTP 会话。

环境态跨执行体（节点池 / 分支池 / 进程池）走**显式白名单快照**
（``plaita.env_context``）。整份复制 context 会把 flow 级共享
``aiohttp.ClientSession``（绑定父 flow 的 event loop）一起带进分支线程与子进程：

1. 分支里的 HTTP 节点复用父 loop 的 session → aiohttp 硬约束报
   ``Timeout context manager should be used inside a task``；
2. 分支收尾的 ``close_flow_session()`` 关掉的是父 flow 的 session → 其后的
   HTTP 节点静默退回一次性会话（连接复用失效），跨 loop 的 close 本身也不安全。

两条都在这里钉住：thread / process 模式下分支内的 HTTP 节点必须成功，且父 flow
的 session 在分支跑完后对象不变、未关闭。全程本地 http.server，零外网。
"""

from __future__ import annotations

import asyncio
import http.server
import threading
import unittest
from typing import Any, ClassVar, Dict, List

import pytest

pytest.importorskip("aiohttp")

from plaita.core.flow import Flow  # noqa: E402
from plaita.core.http_session import get_flow_session  # noqa: E402
from plaita.node import Node, get_default_registry  # noqa: E402

_PROBES: List[Dict[str, Any]] = []


class _SessionProbe(Node):
    """异步探测节点：报告节点内看到的 flow 级 HTTP 会话身份与状态。

    只用异步节点探测——同步节点跑在工作线程里，本来就看不到父 flow 的 session。
    """

    node_type: ClassVar[str] = "session_probe_test"
    node_name: ClassVar[str] = "session probe"

    async def arun(self, execution):
        session = get_flow_session()
        _PROBES.append({
            "missing": session is None,
            "closed": None if session is None else session.closed,
            "sid": id(session),
        })
        return "probe-ok"


def _start_server():
    """本地 HTTP/1.1 服务：按请求计数。"""
    state = {"requests": 0}

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            state["requests"] += 1
            body = b'{"ok": true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # 静音
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state


def _inner_http_flow(flow_id: str, url: str) -> Dict[str, Any]:
    return {
        "flow_id": flow_id, "version": "0.1", "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "h"},
            {"type": "http", "id": "h", "method": "GET", "url": url, "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.h", "resultType": "success"},
        ],
    }


def _parallel_flow(mode: str, branch: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "flow_id": f"parallel-branch-{mode}", "version": "0.1", "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "p"},
            {"type": "parallel", "id": "p", "mode": mode, "join_branches": ["b1"],
             "branches": [{"name": "b1", "flow": branch}], "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.p", "resultType": "success"},
        ],
    }


def _probe_parallel_flow(mode: str, branch: Dict[str, Any]) -> Dict[str, Any]:
    """分支前后各放一个会话探测节点。

    探测节点写成 ``session_probe_test``（``_SessionProbe.node_type``）：被依赖的
    是节点类型，硬编码字面量能让"探针不在注册表里"这种错误在解析期就炸出来。
    """
    return {
        "flow_id": f"probe-parallel-{mode}", "version": "0.1", "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "before"},
            {"type": _SessionProbe.node_type, "id": "before", "next": "p"},
            {"type": "parallel", "id": "p", "mode": mode, "join_branches": ["b1"],
             "branches": [{"name": "b1", "flow": branch}], "next": "after"},
            {"type": _SessionProbe.node_type, "id": "after", "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.after", "resultType": "success"},
        ],
    }


class _BranchTestCase(unittest.TestCase):
    def setUp(self):
        self.server, self.state = _start_server()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/"
        get_default_registry().register(_SessionProbe)
        _PROBES.clear()

    def tearDown(self):
        get_default_registry().unregister(_SessionProbe.node_type)
        _PROBES.clear()
        self.server.shutdown()
        self.server.server_close()

    def _run_http_branch_flow(self, mode: str, *, async_driver: bool):
        flow = Flow.model_validate(
            _parallel_flow(mode, _inner_http_flow(f"inner-{mode}", self.url))
        )
        if async_driver:
            return asyncio.run(flow.arun())
        return flow.run()


class TestHttpNodeInsideParallelBranch(_BranchTestCase):
    """分支里的 HTTP 节点必须成功——复现阻断项的四种驱动组合。"""

    def test_thread_mode_async_driver(self):
        result = self._run_http_branch_flow("thread", async_driver=True)
        self.assertEqual(result, {"b1": {"ok": True}})
        self.assertEqual(self.state["requests"], 1)

    def test_thread_mode_sync_driver(self):
        self.assertEqual(
            self._run_http_branch_flow("thread", async_driver=False),
            {"b1": {"ok": True}},
        )

    def test_process_mode_async_driver(self):
        self.assertEqual(
            self._run_http_branch_flow("process", async_driver=True),
            {"b1": {"ok": True}},
        )

    def test_process_mode_sync_driver(self):
        self.assertEqual(
            self._run_http_branch_flow("process", async_driver=False),
            {"b1": {"ok": True}},
        )


class TestBranchDoesNotCloseParentSession(_BranchTestCase):
    """分支跑完，父 flow 的共享 session 必须原样可用。"""

    def test_thread_branch_keeps_parent_session_open(self):
        flow = Flow.model_validate(
            _probe_parallel_flow("thread", _inner_http_flow("inner-probe", self.url))
        )
        asyncio.run(flow.arun())

        self.assertEqual(len(_PROBES), 2, "两个探测节点都应执行")
        before, after = _PROBES
        self.assertFalse(before["missing"], "flow 驱动必须打开共享 session")
        self.assertFalse(before["closed"])
        self.assertEqual(after["sid"], before["sid"],
                         "分支替换了父 flow 的 session（跨 loop 的 session 被复用）")
        self.assertFalse(after["closed"], "分支收尾把父 flow 的 session 关掉了")
