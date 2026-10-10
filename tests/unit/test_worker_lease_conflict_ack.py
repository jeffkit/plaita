"""租约冲突 → ack 释放（2026-10-06 第5缺陷修复回归）。

背景：原先「不 ack 留 pending」被对端按 claim_min_idle 反复 XCLAIM，每次让
Redis deliveries +1 → 烧到超限触发假死信（实测 22/31）。修法：租约冲突时
直接 ack（任务在持有者手里，没丢；持有者崩溃由 keeper reaper 重派兜底）。
"""
from __future__ import annotations

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("redis")

from plaita.server.execution_lease import ExecutionLeaseError  # noqa: E402
from plaita.server.flow_worker import RedisFlowWorker  # noqa: E402


class _FakeQueue:
    def __init__(self):
        self.acked = []
        self.conflicts = 0
    def ack(self, mid):
        self.acked.append(mid)
    def note_lease_conflict(self):
        self.conflicts += 1


def test_lease_conflict_acks_message():
    """冲突分支必须 ack（不再留 pending 烧 delivery）。"""
    import inspect
    src = inspect.getsource(RedisFlowWorker.run) if hasattr(RedisFlowWorker, "run") else ""
    # run() 在 RedisFlowWorker 上；若不可直接取源码，退化为检查模块源码
    if "被他人持租约" not in src:
        import plaita.server.flow_worker as m
        src = inspect.getsource(m)
    assert "queue.ack(task.message_id)" in src, "冲突分支缺 ack"
    assert "被他人持租约" in src, "冲突分支注释/日志缺失"
    # 且 acked=True 必须设置（统计正确性）
    #
    # 定位用**日志调用行**（`logger.xxx` 里带该文案）而非裸字符串：2026-10-10
    # 起 `finally` 里也有注释提到「他人持租约」，裸 find 会先命中注释，
    # seg 里自然没有 `acked = True` ⇒ 门禁误报。
    import re as _re

    m = _re.search(
        r"logger\.(?:info|warning|error)\([^)]*被他人持租约", src, _re.S
    )
    assert m, "未找到冲突分支的日志调用"
    seg = src[max(0, m.start() - 900):m.start() + 200]
    assert "acked = True" in seg, "ack 后未置 acked=True"


def test_affinity_path_still_hands_over():
    """对照：亲和闸路径仍走交接（ack+重入队），两条路径语义不同。"""
    import inspect
    import plaita.server.flow_worker as m
    src = inspect.getsource(m)
    assert "_handover_non_affine" in src
    assert "TaskNotForThisWorker" in src
