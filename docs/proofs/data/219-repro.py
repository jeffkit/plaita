#!/usr/bin/env python3
"""plaita#219 repro / before-after probe.

    PYTHONPATH=/tmp/w44 .venvs/plaita/bin/python docs/proofs/data/219-repro.py

一个节点逻辑写两版（``execute`` / ``arun``），两版的 ``_validate_output`` 都恒抛
``ValueError("r7 invalid output")``：

- ``execute_only``：sync 驱动，run 路径一直会校验 ⇒ NodeExecutionError；
- ``arun``        ：async 驱动，修复前 **不校验** ⇒ 非法输出 ``"x"`` 原样返回；
- ``arun_budget`` ：带超时预算的 async 分支（wait_for 正常返回）同样必须校验；
- ``clean_async`` ：未覆写 ``_validate_output`` 的 async 节点行为不变（正控）。
"""
from __future__ import annotations

import asyncio
import json
import sys
from typing import ClassVar

from plaita.core.context import ExecutionContext
from plaita.core.errors import NodeExecutionError
from plaita.core.runner import NodeRunner
from plaita.node.basic import Node


class _Flow:
    flow_id = "proof219"


class _BadSync(Node):
    node_type: ClassVar[str] = "proof219_bad_sync"

    def execute(self, execution=None):
        return "x"

    def _validate_output(self, result):
        raise ValueError("r7 invalid output")


class _BadAsync(Node):
    node_type: ClassVar[str] = "proof219_bad_async"

    async def arun(self, execution=None):
        return "x"

    def _validate_output(self, result):
        raise ValueError("r7 invalid output")


class _CleanAsync(Node):
    node_type: ClassVar[str] = "proof219_clean_async"

    async def arun(self, execution=None):
        return {"ok": True}


async def _drive(node, **kwargs):
    ctx = ExecutionContext()
    ctx.clean()
    try:
        result, _branch = await NodeRunner(ctx).run_node(_Flow(), node, **kwargs)
        return {"ok": True, "result": result}
    except NodeExecutionError as exc:
        return {"ok": False, "exc": type(exc).__name__, "msg": str(exc)}


def run() -> dict:
    async def _all():
        return {
            "execute_only": await _drive(_BadSync(id="v")),
            "arun": await _drive(_BadAsync(id="v")),
            "arun_budget": await _drive(_BadAsync(id="v"), max_timeout_ms=5000),
            "clean_async": await _drive(_CleanAsync(id="ok")),
        }

    return {
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "results": asyncio.run(_all()),
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
