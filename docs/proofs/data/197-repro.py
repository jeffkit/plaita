#!/usr/bin/env python3
"""plaita#197 repro / before-after probe (skeleton worker, no Redis/processes).

    PYTHONPATH=/tmp/w35 /Users/jeff.jie/.venvs/plaita/bin/python \
        docs/proofs/data/197-repro.py

Scenario 1 (issue's exact probe): register an orphan whose execution is
already over (``object()`` / no longer in ``_lease_watch``), then run three
``_publish_orphan_progress`` rounds.  Before the fix the entry and the
``_lease_lost`` marker stay forever and every round writes a heartbeat.

Scenario 2 (scale): register 5 orphans that all ended; both sets must return
to zero (bounded memory).

Scenario 3 (still alive): an execution that keeps a lease registration is
handed back to the normal path and not published twice.
"""
from __future__ import annotations

import json
import threading

from plaita.server.flow_worker import RedisFlowWorker


class _RunningExecution:
    """最小存活执行对象：模拟仍在推进（有节点在跑）。"""

    def __init__(self):
        self.cancel_requested = threading.Event()
        self._running = True


def _worker():
    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w._lease_watch_lock = threading.RLock()
    w._lease_watch = {}
    w._lease_orphan_watch = {}
    w._lease_lost = set()
    w._node_timings = {}
    w.calls = []
    w._publish_node_progress = lambda *a, **k: w.calls.append(a)
    return w


def _ended_scenario() -> dict:
    w = _worker()
    w._register_lease_orphan("e1", object(), "default", 1)
    for _ in range(3):
        w._publish_orphan_progress()
    return {
        "heartbeat_calls": len(w.calls),
        "orphan_watch_size": len(w._lease_orphan_watch),
        "lease_lost": sorted(w._lease_lost),
    }


def _scale_scenario() -> dict:
    w = _worker()
    for i in range(5):
        w._register_lease_orphan(f"s{i}", object(), "default", i)
    for _ in range(2):
        w._publish_orphan_progress()
    return {
        "orphan_watch_size": len(w._lease_orphan_watch),
        "lease_lost_size": len(w._lease_lost),
    }


def _alive_scenario() -> dict:
    w = _worker()
    execution = _RunningExecution()
    w._register_lease_orphan("live", execution, "default", 7)
    w._publish_orphan_progress()  # 仍在观测 → 发心跳
    calls_while_orphan = len(w.calls)
    # 新租约重新登记（重投 resume 重入）→ 交回正常路径，不重复发布
    w._register_lease_watch("live", "lease-2", execution)
    w._publish_orphan_progress()
    return {
        "calls_while_orphan": calls_while_orphan,
        "calls_after_handback": len(w.calls),
        "orphan_watch_size": len(w._lease_orphan_watch),
    }


def run() -> dict:
    return {
        "plaita_file": __import__("plaita").__file__,
        "ended": _ended_scenario(),
        "scale": _scale_scenario(),
        "alive": _alive_scenario(),
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
