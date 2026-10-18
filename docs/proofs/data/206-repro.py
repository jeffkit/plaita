#!/usr/bin/env python3
"""plaita#206 repro / before-after probe (real Parallel thread branches).

    PYTHONPATH=/tmp/w35 /Users/jeff.jie/.venvs/plaita/bin/python \
        docs/proofs/data/206-repro.py

Scenario: ``Parallel`` with two thread branches.

* branch A: a blocking sync node with a 200 ms timeout;
* branch B: a real ``code`` node (subprocess sandbox) sleeping 1 s.

Before the fix both branches inherit the *same* ``threading.Event``
(``ExecutionContext(parent=...)``).  A's timeout sets it, B's sandbox wait loop
sees it and SIGKILLs B's process tree — B fails with
``cancelled via cancel_event``.  After the fix each branch owns its node-level
event and B completes normally while the parent-level ``cancel()`` still
reaches both branches (``cancel_requested``).
"""
from __future__ import annotations

import asyncio
import json
import time

from plaita.core import types
from plaita.core.context import ExecutionContext
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, Node, Start
from plaita.node.code import CodeNode
from plaita.node.concurrent import Parallel

BRANCH_B_CODE = """
def run(input):
    import time
    time.sleep(1.0)
    return "code-ok"
"""


class _SlowNode(Node):
    node_type = "w35_slow_node"
    node_name = "慢节点"
    seconds: float = 0.0

    def run(self, execution):
        time.sleep(self.seconds)
        return f"slept {self.seconds}"


def _branch_a() -> Flow:
    return Flow(
        flow_id="w35-206-a", version="1", runtime="python",
        output_type=Property(data_type=types.STRING),
        nodes=[
            Start(id="start", next="slow"),
            _SlowNode(id="slow", next="end", seconds=0.6, timeout="200"),
            End(id="end", **{"resultType": "success", "output": "$NODE.slow"}),
        ],
    )


def _branch_b() -> Flow:
    return Flow(
        flow_id="w35-206-b", version="1", runtime="python",
        output_type=Property(data_type=types.STRING),
        nodes=[
            Start(id="start", next="code"),
            CodeNode(id="code", next="end", language="python",
                     code=BRANCH_B_CODE, sandbox_backend="subprocess"),
            End(id="end", **{"resultType": "success", "output": "$NODE.code"}),
        ],
    )


def _parallel_flow() -> Flow:
    return Flow(
        flow_id="w35-206-parent", version="1", runtime="python",
        output_type=Property(data_type=types.OBJECT),
        nodes=[
            Start(id="start", next="par"),
            Parallel(
                id="par", name="par", mode="thread", join_branches=["A", "B"],
                branches=[
                    {"name": "A", "flow": _branch_a()},
                    {"name": "B", "flow": _branch_b()},
                ],
                next="end",
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.par"}),
        ],
    )


def _shared_event_probe() -> dict:
    parent = ExecutionContext()
    child = ExecutionContext(parent=parent)
    shared = parent.cancel_event is child.cancel_event
    child.cancel_event.set()
    sibling_visible = parent.cancel_event.is_set()
    return {"same_object": shared, "child_set_visible_in_parent": sibling_visible}


async def _parallel_probe() -> dict:
    execution = FlowExecution()
    try:
        result = await execution.arun_compatible(_parallel_flow(), False)
    except Exception as exc:  # noqa: BLE001 — 探针记录异常原文
        return {"raised": f"{type(exc).__name__}: {exc}"}
    return {
        "result": result,
        "cancel_requested_set": execution.cancel_requested.is_set(),
    }


async def _parent_cancel_probe() -> dict:
    """父执行 cancel() 仍须传播到所有分支（cancel_requested 路径）。"""
    flow = _parallel_flow()
    execution = FlowExecution()
    task = asyncio.ensure_future(execution.arun_compatible(flow, False))
    await asyncio.sleep(0.05)
    execution.cancel()
    try:
        result = await task
    except Exception as exc:  # noqa: BLE001
        return {"cancelled": True, "detail": f"{type(exc).__name__}: {exc}"}
    return {"cancelled": False, "result": result}


def run() -> dict:
    return {
        "plaita_file": __import__("plaita").__file__,
        "shared_event": _shared_event_probe(),
        "parallel": asyncio.run(_parallel_probe()),
        "parent_cancel": asyncio.run(_parent_cancel_probe()),
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
