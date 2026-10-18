"""plaita#197：失租 orphan 观测集必须可摘除（有界 + keeper 能收尸）.

历史缺陷：``_register_lease_orphan`` 把执行同时写进 ``_lease_orphan_watch`` 与
``_lease_lost``；``_publish_orphan_progress`` 的摘除条件是
``not still_active and execution_id not in _lease_lost`` —— 条目永远在
``_lease_lost`` 里，两个分支都不命中。已结束的执行因此永久驻留并每轮续写
``last_update_time``，keeper 的 stale 判据永远看不到停滞。

修复后摘除判据改为 **execution 存活**（``_running`` / 在跑节点采集器），
摘除时同时 ``_lease_lost.discard``；「失租期间继续发心跳」的初衷保留：仍在
推进的执行照旧每轮发布。
"""
from __future__ import annotations

import threading

import pytest

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.node_timings import NodeTimingCallback


class _LiveExecution:
    """仍在推进的执行对象（引擎 run 窗口内 ``_running=True``）。"""

    def __init__(self, running: bool = True):
        self._running = running
        self.cancel_requested = False

    def cancel(self):
        self.cancel_requested = True


def _worker():
    worker = RedisFlowWorker.__new__(RedisFlowWorker)
    worker._lease_watch_lock = threading.RLock()
    worker._lease_watch = {}
    worker._lease_orphan_watch = {}
    worker._lease_lost = set()
    worker._node_timings = {}
    worker.published = []
    worker._publish_node_progress = (
        lambda eid, **kw: worker.published.append(eid)
    )
    return worker


class TestOrphanWatchIsBounded:
    def test_ended_orphan_is_dropped_and_heartbeat_stops(self):
        """验收用例：register → 执行已结束 → 两轮后两个集合为空、心跳 0 次。"""
        worker = _worker()
        worker._register_lease_orphan("e1", object(), "default", 1)
        for _ in range(2):
            worker._publish_orphan_progress()

        assert worker._lease_orphan_watch == {}, "已结束的 orphan 必须被摘除"
        assert worker._lease_lost == set(), "失租标记不得永久驻留"
        assert worker.published == [], "已结束的执行不得再续写 last_update_time"

    def test_many_finished_orphans_return_to_zero(self):
        """规模用例：连续 register N 个已结束 orphan → 两个集合回到 0。"""
        worker = _worker()
        for i in range(5):
            worker._register_lease_orphan(f"s{i}", object(), "default", i)
        worker._publish_orphan_progress()

        assert len(worker._lease_orphan_watch) == 0
        assert len(worker._lease_lost) == 0

    def test_live_orphan_keeps_publishing(self):
        """仍活着（``_running``）⇒ 每轮继续发布，并在摘除前累计轮数。"""
        worker = _worker()
        worker._register_lease_orphan("live", _LiveExecution(), "default", 7)
        worker._publish_orphan_progress()
        worker._publish_orphan_progress()

        assert worker.published == ["live", "live"], "仍在跑的执行必须继续发心跳"
        assert "live" in worker._lease_orphan_watch

    def test_open_node_timing_counts_as_alive(self):
        """无 ``_running`` 但本 worker 有在跑节点（#123 生产形态）⇒ 仍活。"""
        worker = _worker()
        timing = NodeTimingCallback()
        timing.on_node_start(None, type("N", (), {"id": "impl"})())

        class _AbandonedExecution:
            # 引擎 run 已返回（_running=False），但 impl 节点仍在沙箱里跑
            _running = False

        worker._node_timings["impl-exec"] = timing
        try:
            worker._register_lease_orphan("impl-exec", _AbandonedExecution(), "default", 1)
            worker._publish_orphan_progress()
            assert worker.published == ["impl-exec"], "在跑节点仍是活证据"

            timing.on_node_end(None, type("N", (), {"id": "impl"})())
            worker._publish_orphan_progress()
            assert "impl-exec" not in worker._lease_orphan_watch, "节点结束后应可摘除"
        finally:
            assert "impl-exec" not in worker._lease_lost

    def test_handback_to_new_lease_does_not_publish_again(self):
        """验收用例：`_lease_watch` 重新登记 ⇒ 交回正常路径、不重复发布。"""
        worker = _worker()
        execution = _LiveExecution()
        worker._register_lease_orphan("e2", execution, "default", 3)
        worker._publish_orphan_progress()
        assert worker.published == ["e2"]

        worker._register_lease_watch("e2", "lease-2", execution)
        worker._publish_orphan_progress()
        assert worker.published == ["e2"], "交回后不得再走 orphan 发布"
        assert "e2" not in worker._lease_orphan_watch
        assert "e2" not in worker._lease_lost


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
