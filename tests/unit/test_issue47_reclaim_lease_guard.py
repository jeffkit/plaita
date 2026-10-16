"""plaita#47 — reclaim 不查活跃租约：排队中的长任务被「死信→重入队」循环冲洗。

生产实测（issue #47）：``plaita:flow:queue:v2:dlq`` 3 条全为
``reason=max_deliveries=5``，同一条 ``start`` 任务互为 source_id 链条
（死信→重入队新副本→再烧 5 次投递→再死信，周期 ≈6.4min）；对照租约证据
显示对应 exec ``status=running`` 且租约 TTL=86s（活）——即 exec 正常跑着，
其队列消息仍被 reclaim 按 idle 阈值反复 ``XCLAIM``：``_reclaim_one``
（task_queue.py）只看 ``idle >= claim_min_idle_ms``，租约/执行状态零查询，
``delivery_count`` 虚增触顶后守卫「非终态+租约空」分支再重入队——循环闭环。

修复契约：reclaim 在 ``XCLAIM`` **前**探测活跃租约（队列新参
``reclaim_lease_guard``，body → True=租约活跃必须跳过）：

- 租约活跃 → 不 XCLAIM、不计数、DLQ 零新增（循环在源头切断）；
- 租约消失（持有者崩溃，TTL 到期）→ reclaim 照常接管，恢复路径不被切断；
- 无租约消息（常态短任务）→ 存量 reclaim 行为零变化。

fakeredis 全覆盖，不需要真 Redis。
"""
import time
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.task_queue import RedisStreamTaskQueue

STREAM = "plaita:flow:queue:t47"
GROUP = "g"


def _lease_probe(redis):
    """与 RedisFlowWorker._reclaim_lease_guard 同规则（default 租户 → ``plaita`` ns）。"""

    def probe(body):
        execution_id = body.get("execution_id")
        if not execution_id:
            return False
        return bool(redis.exists(f"plaita:execution:lease:{execution_id}"))

    return probe


def _make_pending(redis, execution_id, *, deliveries=1, max_deliveries=5):
    """造一条已被 c1 派发（未 ack）的 pending 消息，留出 idle 窗口。

    deliveries>1 时用 XCLAIM 刷 ``times_delivered``（与生产虚增路径同形）。
    """
    writer = RedisStreamTaskQueue(
        redis, STREAM, group_name=GROUP, consumer_name="c1",
        claim_min_idle_ms=1, max_deliveries=max_deliveries,
    )
    mid = writer.enqueue({"type": "start", "flow_id": "f", "execution_id": execution_id})
    first = writer.read(block_ms=100)  # 派发 ≠ 处理完：不 ack
    assert first is not None and first.message_id == mid
    for _ in range(deliveries - 1):
        time.sleep(0.005)
        claimed = redis.xclaim(STREAM, GROUP, "c1", min_idle_time=1, message_ids=[mid])
        assert claimed
    time.sleep(0.02)  # idle > claim_min_idle_ms=1ms
    return mid


def _pending_entry(redis, mid):
    entries = redis.xpending_range(STREAM, GROUP, min=mid, max=mid, count=1)
    return entries[0] if entries else None


def _reader(redis, **kwargs):
    kwargs.setdefault("claim_min_idle_ms", 1)
    kwargs.setdefault("max_deliveries", 5)
    kwargs.setdefault("reclaim_lease_guard", _lease_probe(redis))
    return RedisStreamTaskQueue(redis, STREAM, group_name=GROUP, consumer_name="c2", **kwargs)


class TestReclaimChecksActiveLease(unittest.TestCase):
    def test_active_lease_reclaim_skips_and_counts_stay_frozen(self):
        """租约活跃：reclaim 跳过——不 XCLAIM、不投递、times_delivered 不增长。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _make_pending(redis, "e-queue")
        redis.set("plaita:execution:lease:e-queue", "worker-a:1", ex=120)
        r = _reader(redis)
        for _ in range(5):
            time.sleep(0.005)
            self.assertIsNone(
                r.read(block_ms=20), "租约活跃：reclaim 不得把该消息再投递"
            )
        entry = _pending_entry(redis, mid)
        self.assertIsNotNone(entry)
        self.assertEqual(int(entry["times_delivered"]), 1, "未 XCLAIM ⇒ 次数不虚增")
        self.assertEqual(redis.xlen(r.dlq_key), 0, "DLQ 零新增")
        self.assertGreaterEqual(r.stats()["reclaim_lease_skipped"], 1)

    def test_overdelivered_with_active_lease_never_dead_letters(self):
        """生产循环的终结判据：已触顶消息在租约活跃期不得进入死信。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _make_pending(redis, "e-cycle", deliveries=5, max_deliveries=5)
        redis.set("plaita:execution:lease:e-cycle", "worker-a:1", ex=120)
        r = _reader(redis)
        time.sleep(0.005)
        self.assertIsNone(r.read(block_ms=20))
        self.assertEqual(redis.xlen(r.dlq_key), 0, "租约活跃：死信→重入队循环必须切断")
        entry = _pending_entry(redis, mid)
        self.assertEqual(int(entry["times_delivered"]), 5, "跳过路径不得再刷次数")

    def test_lease_gone_reclaim_takes_over_recovery_path_intact(self):
        """持有者崩溃（租约消失）：reclaim 照常接管——修复不得切断恢复路径。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _make_pending(redis, "e-orphan")
        redis.set("plaita:execution:lease:e-orphan", "worker-a:1", ex=120)
        r = _reader(redis)
        time.sleep(0.005)
        self.assertIsNone(r.read(block_ms=20), "租约在 → 跳过")
        redis.delete("plaita:execution:lease:e-orphan")  # TTL 到期 = 持有者已死
        time.sleep(0.02)
        task = r.read(block_ms=50)
        self.assertIsNotNone(task, "租约消失后必须能 reclaim 接管")
        self.assertEqual(task.body["execution_id"], "e-orphan")

    def test_no_lease_reclaim_behavior_unchanged(self):
        """无租约消息（常态）：reclaim 行为与存量完全一致。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _make_pending(redis, "e-fresh")
        r = _reader(redis)
        task = r.read(block_ms=50)
        self.assertIsNotNone(task)
        self.assertEqual(task.message_id, mid)

    def test_guard_raising_skips_reclaim_conservatively(self):
        """探测抛错按「无法确认无租约」保守跳过：不 XCLAIM、不误伤。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _make_pending(redis, "e-boom")

        def boom(body):
            raise RuntimeError("lease probe down")

        r = _reader(redis, reclaim_lease_guard=boom)
        time.sleep(0.005)
        self.assertIsNone(r.read(block_ms=20))
        entry = _pending_entry(redis, mid)
        self.assertEqual(int(entry["times_delivered"]), 1)

    def test_no_guard_param_zero_behavior_change(self):
        """默认（未传 reclaim_lease_guard）恒跳过检查：存量行为零变化。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _make_pending(redis, "e-legacy")
        r = RedisStreamTaskQueue(
            redis, STREAM, group_name=GROUP, consumer_name="c2", claim_min_idle_ms=1
        )
        task = r.read(block_ms=50)
        self.assertIsNotNone(task)
        self.assertEqual(task.message_id, mid)


if __name__ == "__main__":
    unittest.main()
