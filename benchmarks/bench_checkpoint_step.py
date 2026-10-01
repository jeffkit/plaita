#!/usr/bin/env python3
"""bench_checkpoint_step.py — distributed/generator 每步 checkpoint 成本量化。

2026-10 wave1 曾把「每步 context.to_dict() 全量状态」标记为 O(n²) 疑点；
本基准把成本拆到构成，给出增量 checkpoint 序列化是否值得做的数据判据：

- 引擎侧：CheckpointState.to_checkpoint_dict() + validate_checkpoint()
  —— 实现为**按引用浅拷贝**（payload 不复制），预期 O(键数)、与 payload
  大小无关；
- 消费侧：步骤快照的 json.dumps（console/bridge 持久化必须立即做——
  to_dict 是活引用，下一步节点结果写入会污染旧快照）—— 预期随状态
  线性增长，单次 run 累计 O(n²)。

用法：python benchmarks/bench_checkpoint_step.py
"""

from __future__ import annotations

import json
import os
import sys
import timeit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import plaita  # noqa: E402

from plaita.core.state import CheckpointState, validate_checkpoint  # noqa: E402

print(f"plaita : {plaita.__file__}")


def make_state(n_nodes: int, payload_kb: float) -> CheckpointState:
    """模拟跑到第 n 个节点的状态：$NODE 含 n 条节点结果，每条 ~payload_kb。"""
    state = CheckpointState.fresh(prefix="$", execution_id="bench", env={})
    state["$INPUT"] = {"goal": "bench" * 64}
    state["$GLOBAL"] = {"flow_id": "bench-flow"}
    node_results = {}
    for i in range(n_nodes):
        node_results[f"n{i}"] = {
            "status": "ok",
            "text": "".join("abcdefghij "[j % 11] for j in range(int(payload_kb * 512))),
            "usage": {"total_tokens": 100 + i, "input": 80, "output": 20},
            "model": "bench-model",
        }
    state["$NODE"] = node_results
    state["$LAST_NODE"] = f"n{n_nodes - 1}"
    return state


def best_us(fn, number: int) -> float:
    fn()
    return min(timeit.repeat(fn, number=number, repeat=5)) / number * 1e6


def main() -> None:
    print(f"\n{'规模':>16} | {'引擎 to_dict':>12} | {'validate':>10} | {'json.dumps(快照)':>16} | 单次 run 持久化累计")
    total_state = lambda n, kb: n * kb  # noqa: E731
    for n, kb in ((10, 10), (30, 10), (30, 50), (100, 50)):
        state = make_state(n, kb)
        engine_us = best_us(lambda: state.to_checkpoint_dict(), 2000)

        def full_step():
            data = state.to_checkpoint_dict()
            validate_checkpoint(data, "$")

        step_us = best_us(full_step, 2000)
        snap = state.to_checkpoint_dict()
        dumps_us = best_us(lambda: json.dumps(snap, default=str), 100)

        # 单次 run 累计持久化：第 i 步的状态 = i 条节点结果 → Σ i·kb
        cumulative_mb = kb * n * (n + 1) / 2 / 1024
        # 持久化 CPU 累计按实测 dumps 吞吐反推（单步 dumps_us / 单步 MB → GB/s）
        step_mb = len(snap) and (kb * n / 1024)
        throughput_gbps = (step_mb / 1024) / (dumps_us / 1e6) if dumps_us else 0
        cumulative_ms = cumulative_mb / 1024 / max(throughput_gbps, 1e-9) * 1000
        print(f"{n:>6} 节点×{kb:>3}KB | {engine_us:9.1f}µs | {step_us:8.1f}µs | "
              f"{dumps_us:13.1f}µs | ~{cumulative_mb:6.1f}MB json（≈{cumulative_ms:5.0f}ms CPU @ {throughput_gbps:.2f}GB/s）")

    print("\n判读口径：")
    print("  - 引擎侧 to_dict 为浅拷贝，成本 O(键数) 且与 payload 无关——引擎侧无可优化空间；")
    print("  - 消费侧 json.dumps 随状态线性增长、单 run 累计 O(n²)，但发生在")
    print("    console/bridge 持久化路径（engine 外）：distributed 步为分钟级 LLM 调用时")
    print("    占比 <0.1%；仅高频短步（HTTP 节点 ~200ms）+ 大状态时逼近 10%，")
    print("    且修复属于消费方契约（增量持久化/异步落盘），非 plaita 引擎改动。")


if __name__ == "__main__":
    main()
