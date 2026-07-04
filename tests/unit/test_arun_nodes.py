"""
测试各核心节点的 arun 方法，验证 asyncio 路径端到端可正常执行。

覆盖：
  - InlineFlow.arun / ReferenceFlow.arun
  - Loop.arun / Map.arun / Filter.arun / Find.arun / Reduce.arun
  - Parallel.arun  (coroutine 模式)
  - Flow.arun  (端到端)
  - HTTP.arun  (可选：需安装 aioresponses)
  - 各 arun 是协程函数（NodeRunner 路由检查）
"""
from __future__ import annotations

import asyncio
import unittest


from plaita.core.flow import Flow


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_flow(nodes: list, flow_id: str = "test") -> Flow:
    import json

    data = {"id": flow_id, "version": "0.1", "runtime": "python", "nodes": nodes}
    return Flow.from_string(json.dumps(data))


def _end(node_id: str = "end", output=None, next_node: str = None) -> dict:
    """Helper: build a success End node dict."""
    d: dict = {"type": "end", "id": node_id, "resultType": "success"}
    if output is not None:
        d["output"] = output
    if next_node:
        d["next"] = next_node
    return d


def _echo_child_flow(flow_id: str = "inner") -> dict:
    """Child flow: echo $INPUT.item back (loop/map pass {item, index} as $INPUT)."""
    return {
        "id": flow_id,
        "version": "0.1",
        "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            _end("e", "$INPUT.item"),
        ],
    }


def _always_true_child_flow() -> dict:
    """Child flow: always returns True (used to test Filter/Find arun path)."""
    return {
        "id": "always_true",
        "version": "0.1",
        "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            _end("e", True),
        ],
    }


def _index_zero_child_flow() -> dict:
    """Child flow: returns True only when index == 0 (used to test Find stops early)."""
    return {
        "id": "index_zero",
        "version": "0.1",
        "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            _end("e", {"$calc": {"left": "$INPUT.index", "op": "==", "right": 0}}),
        ],
    }


def _add_child_flow() -> dict:
    """Child flow for Reduce: echo second (accumulator pattern — simpler than add)."""
    return {
        "id": "add_inner",
        "version": "0.1",
        "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            _end("e", "$INPUT.second"),
        ],
    }


# ---------------------------------------------------------------------------
# arun coroutine check (sync test)
# ---------------------------------------------------------------------------

class TestArunAreCoroutines(unittest.TestCase):
    def test_arun_methods_exist_and_are_coroutines(self):
        from plaita.node.http import HTTP
        from plaita.node.child import InlineFlow, ReferenceFlow
        from plaita.node.loop import Loop, Map, Filter, Find, Reduce
        from plaita.node.concurrent import Parallel

        for cls in [HTTP, InlineFlow, ReferenceFlow, Loop, Map, Filter, Find, Reduce, Parallel]:
            with self.subTest(cls=cls.__name__):
                self.assertTrue(hasattr(cls, "arun"), f"{cls.__name__} 缺少 arun 方法")
                self.assertTrue(
                    asyncio.iscoroutinefunction(cls.arun),
                    f"{cls.__name__}.arun 不是协程函数",
                )


# ---------------------------------------------------------------------------
# Flow.arun — end-to-end
# ---------------------------------------------------------------------------

class TestFlowArun(unittest.IsolatedAsyncioTestCase):
    async def test_simple(self):
        flow = _make_flow([
            {"type": "start", "id": "start", "next": "end"},
            _end("end", "hello"),
        ])
        result = await flow.arun()
        self.assertEqual(result, "hello")

    async def test_with_input(self):
        flow = _make_flow([
            {"type": "start", "id": "start", "next": "end"},
            _end("end", "$INPUT.value"),
        ])
        result = await flow.arun({"value": 42})
        self.assertEqual(result, 42)


# ---------------------------------------------------------------------------
# Loop.arun
# ---------------------------------------------------------------------------

class TestLoopArun(unittest.IsolatedAsyncioTestCase):
    async def test_loop_returns_last_item(self):
        flow = _make_flow([
            {"type": "start", "id": "start", "next": "loop1"},
            {
                "type": "loop",
                "id": "loop1",
                "collection": "$INPUT.items",
                "child_flow": _echo_child_flow(),
                "next": "end",
            },
            _end("end", "$NODE.loop1"),
        ])
        result = await flow.arun({"items": [1, 2, 3]})
        # Loop returns the last iteration's result
        self.assertEqual(result, 3)


# ---------------------------------------------------------------------------
# Map.arun
# ---------------------------------------------------------------------------

class TestMapArun(unittest.IsolatedAsyncioTestCase):
    async def test_sequential(self):
        flow = _make_flow([
            {"type": "start", "id": "start", "next": "map1"},
            {
                "type": "map",
                "id": "map1",
                "collection": "$INPUT.items",
                "concurrent": False,
                "child_flow": _echo_child_flow(),
                "next": "end",
            },
            _end("end", "$NODE.map1"),
        ])
        result = await flow.arun({"items": [10, 20, 30]})
        self.assertEqual(result, [10, 20, 30])

    async def test_concurrent(self):
        flow = _make_flow([
            {"type": "start", "id": "start", "next": "map1"},
            {
                "type": "map",
                "id": "map1",
                "collection": "$INPUT.items",
                "concurrent": True,
                "max_concurrent": 4,
                "child_flow": _echo_child_flow(),
                "next": "end",
            },
            _end("end", "$NODE.map1"),
        ])
        result = await flow.arun({"items": [1, 2, 3, 4, 5]})
        self.assertEqual(sorted(result), [1, 2, 3, 4, 5])


# ---------------------------------------------------------------------------
# Filter.arun
# ---------------------------------------------------------------------------

class TestFilterArun(unittest.IsolatedAsyncioTestCase):
    async def test_filter_all_pass(self):
        """Filter.arun: always-true child flow keeps all items."""
        flow = _make_flow([
            {"type": "start", "id": "start", "next": "filter1"},
            {
                "type": "filter",
                "id": "filter1",
                "collection": "$INPUT.items",
                "child_flow": _always_true_child_flow(),
                "next": "end",
            },
            _end("end", "$NODE.filter1"),
        ])
        result = await flow.arun({"items": [10, 20, 30]})
        self.assertEqual(result, [10, 20, 30])


# ---------------------------------------------------------------------------
# Find.arun
# ---------------------------------------------------------------------------

class TestFindArun(unittest.IsolatedAsyncioTestCase):
    async def test_find_first_item(self):
        """Find.arun: always-true child → returns first item in collection."""
        flow = _make_flow([
            {"type": "start", "id": "start", "next": "find1"},
            {
                "type": "find",
                "id": "find1",
                "collection": "$INPUT.items",
                "child_flow": _always_true_child_flow(),
                "next": "end",
            },
            _end("end", "$NODE.find1"),
        ])
        result = await flow.arun({"items": [10, 20, 30]})
        self.assertEqual(result, 10)


# ---------------------------------------------------------------------------
# Reduce.arun
# ---------------------------------------------------------------------------

class TestReduceArun(unittest.IsolatedAsyncioTestCase):
    async def test_reduce_echo_last(self):
        """Reduce.arun: child echoes second → result equals last item in collection."""
        flow = _make_flow([
            {"type": "start", "id": "start", "next": "reduce1"},
            {
                "type": "reduce",
                "id": "reduce1",
                "collection": "$INPUT.items",
                "child_flow": _add_child_flow(),
                "next": "end",
            },
            _end("end", "$NODE.reduce1"),
        ])
        result = await flow.arun({"items": ["a", "b", "c"]})
        # Each iteration returns second (current item), so final result = last item
        self.assertEqual(result, "c")


# ---------------------------------------------------------------------------
# Parallel.arun  coroutine mode
# ---------------------------------------------------------------------------

class TestParallelArun(unittest.IsolatedAsyncioTestCase):
    async def test_coroutine_mode(self):
        def branch(name: str, val: int) -> dict:
            return {
                "name": name,
                "input": val,
                "flow": {
                    "id": f"f_{name}",
                    "version": "0.1",
                    "runtime": "python",
                    "nodes": [
                        {"type": "start", "id": "s", "next": "e"},
                        _end("e", "$INPUT"),
                    ],
                },
            }

        flow = _make_flow([
            {"type": "start", "id": "start", "next": "par1"},
            {
                "type": "parallel",
                "id": "par1",
                "mode": "coroutine",
                "join_branches": ["branchA", "branchB"],
                "branches": [branch("branchA", 10), branch("branchB", 20)],
                "next": "end",
            },
            _end("end", "$NODE.par1"),
        ])
        result = await flow.arun()
        self.assertEqual(result, {"branchA": 10, "branchB": 20})


# ---------------------------------------------------------------------------
# HTTP.arun — optional (requires aioresponses)
# ---------------------------------------------------------------------------

class TestHttpArun(unittest.IsolatedAsyncioTestCase):
    async def test_http_arun_with_mock(self):
        try:
            from aioresponses import aioresponses
        except ImportError:
            self.skipTest("aioresponses not installed")

        flow = _make_flow([
            {"type": "start", "id": "start", "next": "http1"},
            {
                "type": "http",
                "id": "http1",
                "method": "GET",
                "url": "http://test.example.com/api",
                "next": "end",
            },
            _end("end", "$NODE.http1"),
        ])

        with aioresponses() as m:
            m.get("http://test.example.com/api", payload={"msg": "ok"})
            result = await flow.arun()

        self.assertEqual(result, {"msg": "ok"})
