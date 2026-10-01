#!/usr/bin/env python3
"""bench_json_payload.py — HTTP 节点 JSON 后端按 payload 分层对照。

2026-10 wave2 验收要求：序列化成本必须按 payload 大小分层报告——
stdlib json 在 ~120KB 实测 ~375µs/次，是 BFF 大 payload 档（LLM 响应转发）
节点税的主要构成；orjson（fast extra）是该档的唯一有效杠杆。

用法：python benchmarks/bench_json_payload.py
"""

from __future__ import annotations

import json
import os
import sys
import timeit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import plaita  # noqa: E402

from plaita.node.http import _loads_lenient  # noqa: E402

print(f"plaita : {plaita.__file__}")

try:
    import orjson
    HAVE_ORJSON = True
except ImportError:
    HAVE_ORJSON = False
    print("(orjson 未安装——仅测 stdlib 基线；pip install plaita[fast] 后重跑可得对照)")


def make_payload(size_kb: int) -> dict:
    item = {"id": 0, "name": "item-name", "tags": ["a", "b", "c"],
            "score": 0.98, "meta": {"ok": True, "path": "/x/y", "note": "n" * 40}}
    n = max(1, size_kb * 1024 // len(json.dumps(item)))
    return [{**item, "id": i} for i in range(n)]


def best_us(fn, obj, number: int) -> float:
    fn(obj)  # warm
    return min(timeit.repeat(lambda: fn(obj), number=number, repeat=5)) / number * 1e6


def main() -> None:
    print(f"\n{'payload':>10} | {'stdlib dumps':>12} | {'stdlib loads':>12} | "
          + ("orjson dumps | orjson loads | " if HAVE_ORJSON else "")
          + "loads提速")
    for kb in (8, 32, 128):
        payload = make_payload(kb)
        n = 2000 if kb <= 32 else 500
        sd = best_us(lambda o: json.dumps(o).encode("utf-8"), payload, n)
        text = json.dumps(payload)
        sl = best_us(json.loads, text, n)
        row = f"{kb:>8}KB | {sd:10.1f}µs | {sl:10.1f}µs | "
        if HAVE_ORJSON:
            od = best_us(orjson.dumps, payload, n)
            ol = best_us(orjson.loads, text, n)
            row += f"{od:10.1f}µs | {ol:10.1f}µs | {sl / ol:6.1f}x"
        else:
            row += "—"
        print(row)
    # 长度无关项：宽松回退层自身开销（NaN 不在热路径，仅验证兜底可用）
    assert _loads_lenient('{"a": 1}') == {"a": 1}


if __name__ == "__main__":
    main()
