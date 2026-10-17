"""#42（W10-a）全局调度层第一竖切：worker 容量上报 + 派发侧容量门。

## 现象（可执行证据）

issue-keeper#16 的配套单要求 console 承担「派发决策」——谁先跑、跑几个、
发给哪台机器。plaita 的分布式地基已齐（task_queue 抢单 / execution_lease
fencing / registry 心跳），唯独 registry 只报「**存在性**」：

- `plaita/server/registry.py` 的 ``ServiceInfo`` 只有 ``status`` /
  ``active_tasks`` / ``metadata``，**没有任何可用容量字段**（无盘、无在跑数
  上限、无上报时间）；
- `RedisFlowWorker` 注册时写的 metadata 是 ``queue_name`` / ``redis_url`` /
  ``cache_size`` / ``cache_ttl`` 四项配置，**不含运行时容量**；
- worker 有一个**本机**盘闸（#49 `_disk_guard_blocks_claim`，低盘不领取），
  但它只在自己进程里空转——**对派发方完全不可见**，健康机器无法代它接单。

后果：派发侧无法避开低盘机器（2026-10-06 实测本机盘触底 0 字节仍在被压单），
也无法按「在跑数/并发额度」做负载均衡。

## 本竖切（可独立落地、有守卫、不引入新基础设施）

1. **容量快照**（``plaita.server.capacity``）：worker 把「可用盘 GiB +
   盘守线 + 是否被盘闸拦下 + 在跑任务数 + 并发上限 + 采样时刻」规格化为
   一段 JSON，写进既有 registry metadata 的 ``worker_capacity`` 键
   （复用 `plaita:registry:flow_worker:<id>` 键与 TTL，零新基础设施）。
2. **新鲜度**：容量块带 ``sampled_at``；超过 ``CAPACITY_STALE_SECONDS`` 的
   快照按「不可信」处理——**不参与派发**（宁可少派，不可按陈旧容量误派）。
3. **派发容量门**：``plaita.server.capacity.filter_dispatchable`` 是**纯函数**
   （输入候选快照、输出可派候选 + 拒绝原因），供 intake flow / console 复用；
   **影子模式**（默认）只算不发，满足验收 5「只算不发、生产队列零新增」。

## 红线

- 容量缺失 / 探测失败 / 无 ``sampled_at`` → **一律放行**（与 #49 盘闸同口径：
  宁可白跑一次，不可因上报机制自身故障让整池 worker 停摆）；
- 默认零行为变化：worker 未启用上报时消息形状与本次改动前完全一致。
"""
from __future__ import annotations

import json
import time
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis  # noqa: E402

from plaita.server.capacity import (  # noqa: E402
    CAPACITY_METADATA_KEY,
    CAPACITY_SCHEMA_VERSION,
    CAPACITY_STALE_SECONDS,
    DispatchCandidate,
    build_capacity_block,
    capacity_from_metadata,
    filter_dispatchable,
    load_worker_capacity,
    parse_capacity_block,
    select_dispatch_target,
)
from plaita.server.registry import ServiceInfo, ServiceRegistry  # noqa: E402

GIB = 1024 ** 3


def _now() -> "datetime":
    """构造夹具时**当刻**取时间（而不是收集期快照一次）。

    W30 整合期修正：原先这里是模块级常量 ``NOW = datetime.now(...)``，在
    pytest **收集期**求值；本文件在整套 `tests/unit` 里约 5 分钟后才执行，
    早已超过 ``CAPACITY_STALE_SECONDS``（默认 90s）⇒ 夹具里「健康 worker」
    的容量块被判**陈旧**，按红线「陈旧 ⇒ 放行」，5 条容量门用例集体假红
    （`w-low` 被允许、`select_dispatch_target` 选到低盘机）。
    夹具的意图本来就是「一份**新鲜**的健康容量块」；陈旧面由
    ``test_stale_capacity_is_not_trusted_but_worker_stays_in_pool`` 用
    ``now - (CAPACITY_STALE_SECONDS + 5)`` 显式构造。**判据未改，断言未动。**
    """
    return datetime.now(timezone.utc)


def _block(**over):
    """一份健康 worker 的容量块（可覆盖任意字段）。"""
    base = dict(
        free_disk_gib=120.0,
        min_free_disk_gib=20.0,
        disk_blocked=False,
        active_tasks=0,
        concurrency=1,
        sampled_at=_now().isoformat(),
    )
    base.update(over)
    base.pop("schema_version", None)  # 由 build_capacity_block 落当前版本
    return build_capacity_block(**base)


# --------------------------------------------------------------- 红先证


class TestCapacityModuleExists(unittest.TestCase):
    def test_capacity_reporting_was_absent_before_this_slice(self):
        """红先证：本竖切前 registry metadata 里根本没有 ``worker_capacity``。

        基线（origin/main）实测：``RedisFlowWorker`` 注册 metadata 只有
        queue_name/redis_url/cache_size/cache_ttl 四项，`plaita.server.capacity`
        模块不存在。故「有容量块」本身就是本单新增的能力。
        """
        info = ServiceInfo(
            instance_id="i-1",
            service_type="flow_worker",
            host="h",
            metadata={"queue_name": "q", "cache_size": 100, "cache_ttl": 300},
        )
        self.assertIsNone(
            capacity_from_metadata(info.metadata),
            "存量 metadata 不含容量块 —— 这正是本单要补的缺口",
        )


# --------------------------------------------------------------- 快照契约


class TestCapacityBlock(unittest.TestCase):
    def test_block_roundtrips_and_declares_schema(self):
        raw = json.dumps(_block())
        parsed = parse_capacity_block(raw)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["schema_version"], CAPACITY_SCHEMA_VERSION)
        self.assertAlmostEqual(parsed["free_disk_gib"], 120.0, places=3)
        self.assertEqual(parsed["concurrency"], 1)

    def test_block_tolerates_bytes_and_embedded_dict(self):
        """registry 读回的 metadata 可能是 dict（fakeredis 不解码 JSON 值）。"""
        block = _block()
        self.assertIsNotNone(parse_capacity_block(block))
        self.assertIsNotNone(parse_capacity_block(json.dumps(block).encode()))
        self.assertIsNotNone(
            parse_capacity_block({CAPACITY_METADATA_KEY: json.dumps(block)})
        )

    def test_malformed_block_is_none_not_raise(self):
        for bad in (None, "", "   ", "{not json", "[]", "3", b"\xff\xfe"):
            with self.subTest(bad=bad):
                self.assertIsNone(parse_capacity_block(bad))

    def test_missing_sampled_at_is_tolerated(self):
        """无 ``sampled_at`` 的块仍可解析（新鲜度判定归 capacity_age_seconds）。"""
        block = dict(_block())
        block.pop("sampled_at")
        self.assertIsNotNone(parse_capacity_block(json.dumps(block)))

    def test_non_negative_clamp(self):
        parsed = parse_capacity_block(json.dumps(_block(free_disk_gib=-5.0, active_tasks=-2)))
        self.assertEqual(parsed["free_disk_gib"], 0.0)
        self.assertEqual(parsed["active_tasks"], 0)

    def test_from_metadata_absent_and_present(self):
        self.assertIsNone(capacity_from_metadata({}))
        self.assertIsNone(capacity_from_metadata(None))
        block = _block()
        got = capacity_from_metadata({CAPACITY_METADATA_KEY: json.dumps(block)})
        self.assertIsNotNone(got)


# --------------------------------------------------------------- 派发门


class TestDispatchGate(unittest.TestCase):
    def _cand(self, name, block, **over):
        raw = block if isinstance(block, str) else json.dumps(block)
        return DispatchCandidate(
            instance_id=name,
            host=f"{name}.host",
            service_type="flow_worker",
            status=over.pop("status", "running"),
            capacity_raw=raw,
            **over,
        )

    def test_low_disk_worker_is_not_dispatched(self):
        """验收 2：磁盘低于阈值的 worker 本轮零派单。"""
        healthy = self._cand("w-ok", _block(free_disk_gib=120.0))
        low = self._cand(
            "w-low", _block(free_disk_gib=3.2, min_free_disk_gib=20.0, disk_blocked=True)
        )
        allowed, rejected = filter_dispatchable([healthy, low])
        self.assertEqual([c.instance_id for c in allowed], ["w-ok"])
        self.assertEqual([r.instance_id for r in rejected], ["w-low"])
        self.assertIn("disk", rejected[0].reason)

    def test_saturated_worker_is_not_dispatched(self):
        """在跑数达到并发上限 → 不再派单（负载均衡的另一半）。"""
        free = self._cand("w-free", _block(active_tasks=0, concurrency=2))
        full = self._cand("w-full", _block(active_tasks=2, concurrency=2))
        allowed, rejected = filter_dispatchable([free, full])
        self.assertEqual([c.instance_id for c in allowed], ["w-free"])
        self.assertIn("saturat", rejected[0].reason)

    def test_capacity_higher_than_min_still_allowed(self):
        """负向对照：容量在阈值之上（未被盘闸拦下）必须照常派单。"""
        ok = self._cand("w-ok", _block(free_disk_gib=21.0, min_free_disk_gib=20.0))
        allowed, rejected = filter_dispatchable([ok])
        self.assertEqual([c.instance_id for c in allowed], ["w-ok"])
        self.assertEqual(rejected, [])

    def test_missing_capacity_is_allowed(self):
        """红线：没有容量块（存量 worker，未升级）→ 放行，不制造停摆。"""
        legacy = self._cand("w-legacy", "")
        allowed, rejected = filter_dispatchable([legacy])
        self.assertEqual([c.instance_id for c in allowed], ["w-legacy"])
        self.assertEqual(rejected, [])

    def test_malformed_capacity_is_allowed(self):
        broken = self._cand("w-broken", "{not json")
        allowed, _ = filter_dispatchable([broken])
        self.assertEqual([c.instance_id for c in allowed], ["w-broken"])

    def test_stale_capacity_is_not_trusted_but_worker_stays_in_pool(self):
        """陈旧快照不采信（不按陈旧容量误派），但不能因此把机器踢出池。"""
        stale_at = (datetime.now(timezone.utc) - timedelta(seconds=CAPACITY_STALE_SECONDS + 5)).isoformat()
        stale = self._cand("w-stale", _block(free_disk_gib=1.0, disk_blocked=True, sampled_at=stale_at))
        allowed, rejected = filter_dispatchable([stale])
        self.assertEqual([c.instance_id for c in allowed], ["w-stale"])
        self.assertEqual(rejected, [])
        self.assertTrue(allowed[0].capacity_stale)

    def test_undecodable_sampled_at_is_treated_as_stale(self):
        bad = self._cand("w-bad-ts", _block(sampled_at="yesterday-ish"))
        allowed, _ = filter_dispatchable([bad])
        self.assertTrue(allowed[0].capacity_stale)

    def test_draining_worker_is_rejected(self):
        draining = self._cand("w-drain", _block(), status="draining")
        allowed, rejected = filter_dispatchable([draining])
        self.assertEqual(allowed, [])
        self.assertEqual(rejected[0].reason, "draining")

    def test_disk_blocked_flag_alone_blocks_even_when_free_looks_fine(self):
        """盘闸自报被拦（active）优先于数字：worker 自己说不能接就别派。"""
        odd = self._cand("w-odd", _block(free_disk_gib=99.0, disk_blocked=True))
        allowed, rejected = filter_dispatchable([odd])
        self.assertEqual(allowed, [])
        self.assertIn("disk", rejected[0].reason)

    def test_select_target_is_deterministic_and_picks_roomiest(self):
        """验收 1 的确定性内核：同输入同输出，且选剩余盘最多的。"""
        a = self._cand("w-a", _block(free_disk_gib=50.0))
        b = self._cand("w-b", _block(free_disk_gib=180.0))
        c = self._cand("w-c", _block(free_disk_gib=180.0))
        first = select_dispatch_target([a, b, c])
        second = select_dispatch_target([c, a, b])
        self.assertEqual(first.instance_id, "w-b", "并列时按 instance_id 字典序稳定取首")
        self.assertEqual(first.instance_id, second.instance_id)

    def test_select_target_returns_none_when_all_rejected(self):
        low = self._cand("w-low", _block(free_disk_gib=1.0, min_free_disk_gib=20.0, disk_blocked=True))
        self.assertIsNone(select_dispatch_target([low]))

    def test_select_target_falls_back_to_legacy_worker(self):
        low = self._cand("w-low", _block(free_disk_gib=1.0, min_free_disk_gib=20.0, disk_blocked=True))
        legacy = self._cand("w-legacy", "")
        self.assertEqual(select_dispatch_target([low, legacy]).instance_id, "w-legacy")


# --------------------------------------------------------------- registry 读侧


class TestLoadWorkerCapacity(unittest.TestCase):
    def _registry(self):
        return ServiceRegistry(fakeredis.FakeRedis(decode_responses=True))

    def test_load_reads_capacity_block_from_registry(self):
        r = self._registry()
        r.register(ServiceInfo(
            instance_id="w-1",
            service_type="flow_worker",
            host="h",
            metadata={CAPACITY_METADATA_KEY: json.dumps(_block(free_disk_gib=42.5))},
        ))
        got = load_worker_capacity(r.redis_client)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0].instance_id, "w-1")
        self.assertAlmostEqual(got[0].capacity["free_disk_gib"], 42.5, places=3)

    def test_load_survives_redis_outage(self):
        class _Boom:
            def keys(self, *a, **k):
                raise RuntimeError("redis down")

        self.assertEqual(load_worker_capacity(_Boom()), [], "Redis 瞬断返回空而不是抛")

    def test_load_skips_non_worker_service_types(self):
        r = self._registry()
        r.register(ServiceInfo(
            instance_id="s-1",
            service_type="delay_service",
            host="h",
            metadata={CAPACITY_METADATA_KEY: json.dumps(_block())},
        ))
        self.assertEqual(load_worker_capacity(r), [])

    def test_shadow_dispatch_never_mutates_redis(self):
        """验收 5：影子模式只算不发 —— 生产队列零新增。"""
        from plaita.server.capacity import shadow_dispatch_plan

        r = self._registry()
        r.register(ServiceInfo(
            instance_id="w-1",
            service_type="flow_worker",
            host="h",
            metadata={CAPACITY_METADATA_KEY: json.dumps(_block())},
        ))
        before = set(r.redis_client.keys("*"))
        plan = shadow_dispatch_plan(r.redis_client, ["issue-a", "issue-b"])
        after = set(r.redis_client.keys("*"))
        self.assertEqual(before, after, "影子决策不得写任何 Redis 键")
        self.assertTrue(plan["shadow"])
        self.assertEqual(plan["target"], "w-1")
        self.assertIn("plaita:flow:queue", plan["queue"])
