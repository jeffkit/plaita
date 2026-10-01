#!/usr/bin/env python3
"""bench_flow_overhead.py — 5 节点 mock flow 端到端引擎税基准。

BFF 场景总账叙事的落点（2026-10 评审）：组件基准（表达式/HTTP）之外，
用一个真实引擎驱动的 5 节点链测量「每节点框架税」：

- 流 A：5 个零成本 async 节点（无 I/O）——总墙钟 / 5 = 纯引擎税/节点，
  口径：小 payload、热缓存、空回调（评审要求的诚实口径）；
- 流 B：5 个 50ms async I/O 节点——I/O 占绝对主导，引擎税应不可见。

用法：python benchmarks/bench_flow_overhead.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Any, ClassVar, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import plaita  # noqa: E402

from plaita.core.executor import FlowExecution  # noqa: E402
from plaita.core.flow import Flow  # noqa: E402
from plaita.node.basic import Node  # noqa: E402
from plaita.node.end import End  # noqa: E402
from plaita.node.start import Start  # noqa: E402

print(f"plaita : {plaita.__file__}")

SLEEP_SECS = 0.050


class BenchNode(Node):
    """可配睡眠时长的 async 桩节点（I/O 密集 BFF 节点的最小替身）。"""
    node_type: ClassVar[str] = "bench"
    branching: ClassVar[bool] = False
    ms: int = 0

    async def arun(self, execution) -> Any:
        if self.ms:
            await asyncio.sleep(self.ms / 1000)
        return self.id


def build_flow(node_ms: int, count: int = 5) -> Flow:
    nodes: list = [Start(id="start", next="n0")]
    for i in range(count):
        nodes.append(BenchNode(id=f"n{i}", ms=node_ms,
                               next=f"n{i+1}" if i < count - 1 else "end"))
    nodes.append(End(id="end"))
    return Flow.model_validate({"nodes": nodes})


def run_flow(flow: Flow) -> float:
    execution = FlowExecution()
    execution.clean()
    t0 = time.perf_counter()
    execution.execute(flow, params={})
    return time.perf_counter() - t0


def main() -> None:
    for _ in range(3):  # warmup（表达式编译缓存、import、loop 预热）
        run_flow(build_flow(0))

    # 每节点边际税：用 5 节点与 50 节点链的墙钟差回归，把每 run 固定成本
    # （clean/setup_flow/CheckpointState 重建）从节点边际成本里剥离。
    t5 = best_run(0, 5, 30)
    t50 = best_run(0, 50, 20)
    per_node = (t50 - t5) / 45 * 1e6
    print(f"\n[流A 零 I/O] 边际引擎税 {per_node:8.1f} us/节点"
          f"（5/50 节点链回归剥离 per-run 固定成本；5 节点整链 {t5*1e6:.0f}us）")

    # 流 B：50ms I/O × 5
    tb = best_run(50, 5, 10) * 1e3
    io_total = 50 * 5
    print(f"[流B 50ms I/O] 5 节点链最好成绩 {tb:8.1f} ms/run"
          f"（I/O 合计 {io_total}ms，引擎税占比 {(tb - io_total) / io_total * 100:.2f}%）")

    ok = per_node < 500
    print(f"\n验收线（小 payload <0.5ms/节点）: {'PASS' if ok else 'FAIL'}"
          f"（实测边际 {per_node:.1f}us）")


def best_run(node_ms: int, count: int, reps: int) -> float:
    """reps 次整链运行，返回单次最优墙钟（秒）。"""
    flow = build_flow(node_ms, count)
    best = float("inf")
    for _ in range(reps):
        t = run_flow(flow)
        if t < best:
            best = t
    return best


if __name__ == "__main__":
    main()
