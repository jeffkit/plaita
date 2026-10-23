"""plaita#326（E1 T0 第一切）：新 start 的租户状态闸 fail-closed 三格。

## 缺陷

``_dispatch_task`` 的租户状态闸（#27）走 ``tenant_is_active``：缺失记录/
无 Redis/读失败一律 fail-open——Redis 抖动一次，停用租户的 kill switch 对
**新 start** 失效（在飞 fail-open 是对的：准入闸拽飞中执行会放大爆炸半径）。

## 规格（正文 + A2 r60 评审三格边界）

1. **格 1（认识论性）**：状态/世代读失败 → 拒 start 且**载体不动**（不
   ack、不收容、不终局化，留 pending 重投自愈）；reason 复用 #312
   ``is_epistemic_reject_reason`` 词汇表，不造第二套。
2. **格 2（确认态）**：``tenant_disabled`` → 照既有拒绝语义（丢弃）。
3. **格 3（记录缺失切刀，A133-03 残留形态）**：status 缺 + 世代缺 =
   机制前租户，#237 fail-open 守恒；**status 缺 + 世代在** = 快照回滚/
   补偿残留（kill switch 被击败的状态）→ 具名 fail-closed
   ``status_missing_generation_present`` + 告警。

default/空租户与无 Redis 客户端：fail-open 守恒（兼容面不扩）。
纪元闸（A2R31-02）经 ``tenant_is_active`` 收口，语义不变。

## 负控（还原修复后必须精确红）

- 删 ``tenant_start_allowed`` 三格（start 回 ``tenant_is_active``）：
  格 1 两面由「抛 TenantGateEpistemicReject」变红（读失败照旧放行）；
  格 3 具名面由「具名拒绝+计数」变红（残留形态照旧 fail-open）。
- resume 读失败必须**不**抛（在飞豁免守恒）——误扩到 resume 时红。
- 消费循环格：start 读失败 ⇒ PEL 非零 + start 未执行；自愈后原载体重投
  过闸执行（two-delivery 形状，#312 格①同款）。
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker, TenantGateEpistemicReject
from plaita.server.tenant_status import (
    STATUS_MISSING_GENERATION_PRESENT,
    is_epistemic_reject_reason,
    tenant_start_allowed,
    tenant_status_missing_generation_total,
)
from plaita.tenant_context import TENANT_STATUS_KEY, TENANT_STATUS_META_KEY
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

META_KEY = TENANT_STATUS_META_KEY


def _make_worker(fake) -> RedisFlowWorker:
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue326-start-gate",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )
    # 自愈重投确定性：XCLAIM 不等 60s 默认 idle。
    worker._claim_min_idle_ms = 0
    return worker


def _start_body(exec_id="e1", tenant="acme"):
    return {
        "type": "start",
        "tenant_id": tenant,
        "flow_id": "f1",
        "execution_id": exec_id,
        "timestamp": datetime.now().isoformat(),
    }


def _poison_hget_once(fake, hash_key):
    """一次性注入：对指定 HASH 的下一次 hget 抛 ConnectionError（#312 同款）。"""
    original_hget = fake.hget
    state = {"armed": True}

    def hget(name, key):
        if state["armed"] and name == hash_key:
            state["armed"] = False
            raise ConnectionError("transient redis flap")
        return original_hget(name, key)

    fake.hget = hget  # type: ignore[method-assign]


def _poison_hget_manual(fake, hash_key):
    """可控注入：对指定 HASH 的 hget 抛错直到显式恢复（循环格窗口可控）。

    返回 state 句柄——``state["poisoned"] = False`` 即自愈。claim_min_idle_ms=0
    时循环会立即重投，一次性 poison 会被下一轮重投穿过，故循环格必须可控。
    """
    original_hget = fake.hget
    state = {"poisoned": True}

    def hget(name, key):
        if state["poisoned"] and name == hash_key:
            raise ConnectionError("transient redis flap")
        return original_hget(name, key)

    fake.hget = hget  # type: ignore[method-assign]
    return state


class TestTenantStartAllowedGrid:
    """三格判定矩阵（纯函数级）。"""

    def test_fail_open_faces_conserved(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        # default / 空租户 / 无客户端：#237/#27 兼容面不扩。
        assert tenant_start_allowed(fake, "default") == (True, None)
        assert tenant_start_allowed(fake, None) == (True, None)
        assert tenant_start_allowed(None, "acme") == (True, None)

    def test_cell2_disabled_confirmed_reject(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")
        allowed, reason = tenant_start_allowed(fake, "acme")
        assert (allowed, reason) == (False, "tenant_disabled")
        assert not is_epistemic_reject_reason(reason)

    def test_cell2_active_allows(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        assert tenant_start_allowed(fake, "acme") == (True, None)

    def test_cell1_status_read_failed_epistemic(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        _poison_hget_manual(fake, TENANT_STATUS_KEY)
        allowed, reason = tenant_start_allowed(fake, "acme")
        assert (allowed, reason) == (False, "status_read_failed")
        assert is_epistemic_reject_reason(reason), "读失败必须走 #312 词汇表"

    def test_cell1_generation_read_failed_epistemic(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        # status 记录缺失才读世代；此时 meta 读失败 = 认识论性。
        _poison_hget_manual(fake, META_KEY)
        allowed, reason = tenant_start_allowed(fake, "acme")
        assert (allowed, reason) == (False, "generation_read_failed")
        assert is_epistemic_reject_reason(reason)

    def test_cell3_missing_status_generation_present_named_closed(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(
            META_KEY, "acme", json.dumps({"tenant_generation": "g1"})
        )
        before = tenant_status_missing_generation_total()
        allowed, reason = tenant_start_allowed(fake, "acme")
        assert (allowed, reason) == (False, STATUS_MISSING_GENERATION_PRESENT)
        assert not is_epistemic_reject_reason(reason), (
            "读到了缺席（状态确认）不是认识论性拒绝"
        )
        assert tenant_status_missing_generation_total() == before + 1, (
            "具名 fail-closed 必须带告警计数"
        )

    def test_cell3_missing_both_fail_open_237_conserved(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        assert tenant_start_allowed(fake, "acme") == (True, None)

    def test_epoch_gate_still_enforced_via_tenant_is_active(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        # 机制前租户（status 缺 + 世代缺）本格放行，但纪元闸照收口：
        # Redis 无纪元键 + expected 非 legacy ⇒ 非 default 拒（A2R31-02）。
        allowed, reason = tenant_start_allowed(fake, "acme", expected_epoch="2")
        assert allowed is False
        assert reason == "tenant_epoch_stale"


class TestDispatchStartGate:
    """派发面：start 三格生效，resume 维持在飞豁免。"""

    def test_start_epistemic_raises_carrier_untouched(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        _poison_hget_once(fake, TENANT_STATUS_KEY)
        with pytest.raises(TenantGateEpistemicReject) as excinfo:
            worker._dispatch_task(_start_body())
        assert excinfo.value.reason == "status_read_failed"
        assert excinfo.value.tenant_id == "acme"
        assert calls == [], "读失败型拒绝不得执行 start"

    def test_start_missing_status_generation_present_dropped(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        fake.hset(META_KEY, "acme", json.dumps({"tenant_generation": "g1"}))
        worker._dispatch_task(_start_body())  # 正常返回 = 消费循环 ack 丢弃
        assert calls == [], "A133-03 残留形态必须具名 fail-closed"

    def test_start_disabled_drop_conserved(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")
        worker._dispatch_task(_start_body())
        assert calls == [], "确认态照既有拒绝语义（#27 守恒）"

    def test_start_healthy_passes(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        worker._dispatch_task(_start_body())
        assert calls == ["start"]

    def test_resume_stays_fail_open_inflight_exempt(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        calls = []
        worker.resume_flow = lambda *a, **k: calls.append("resume")
        _poison_hget_once(fake, TENANT_STATUS_KEY)
        # resume = 在飞载体：读失败**不**抛、不拒——kill switch 不拽在飞执行。
        worker._dispatch_task(
            {
                "type": "resume",
                "tenant_id": "acme",
                "execution_id": "e1",
                "timestamp": datetime.now().isoformat(),
            }
        )
        assert calls == ["resume"]


class TestConsumeLoopEpistemicPending:
    """消费循环格：读失败留 pending（不 ack），自愈后原载体重投过闸执行。"""

    def test_pending_then_self_heal_two_deliveries(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        # 测试旋钮：claim_min_idle_ms=0 下循环毫秒级重投，每轮 XCLAIM 烧一次
        # delivery——断言窗内 churn 远超生产速率（生产默认 60s claim 间隔），
        # 预算放到 1e5 让「烧穿死信」不在本格考纲（本格考两交付契约）。
        worker._max_deliveries = 100_000
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        queue = worker._get_task_queue()
        queue.ensure_group()

        poison = _poison_hget_manual(fake, TENANT_STATUS_KEY)
        # plaita#328：legacy 载体（无快照）——预算由上面的 worker._max_deliveries 旋钮裁判
        queue.enqueue(_start_body(exec_id="e326"), max_deliveries=None)
        worker._running = True
        loop = threading.Thread(
            target=worker._consume_loop, args=(queue,), daemon=True
        )
        loop.start()
        try:
            # 交付一（读失败）：载体必须留在 PEL，start 未执行。
            # claim_min_idle_ms=0 下循环会反复重投重撞闸，poison 持续期
            # 内每轮都留 pending——给足宽限再断言稳态。
            deadline = time.time() + 3
            while time.time() < deadline:
                pending = int(
                    fake.xpending("test:issue326-start-gate", "plaita-workers")[
                        "pending"
                    ]
                )
                if pending >= 1:
                    break
                time.sleep(0.02)
            time.sleep(0.2)
            assert calls == [], "读失败窗内 start 不得执行（fail-closed）"
            pending = int(
                fake.xpending("test:issue326-start-gate", "plaita-workers")[
                    "pending"
                ]
            )
            assert pending == 1, "认识论性拒绝必须载体不动（不 ack）"
        finally:
            worker._running = False
            loop.join(timeout=5)
        assert not loop.is_alive()

        # 交付二（自愈）：状态可读后，同一条 pending 载体重投过闸执行。
        poison["poisoned"] = False
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        worker._running = True
        loop = threading.Thread(
            target=worker._consume_loop, args=(queue,), daemon=True
        )
        loop.start()
        try:
            deadline = time.time() + 5
            while time.time() < deadline and calls == []:
                time.sleep(0.02)
            assert calls == ["start"], "瞬断自愈后原载体必须可救（重投过闸）"
            pending = int(
                fake.xpending("test:issue326-start-gate", "plaita-workers")[
                    "pending"
                ]
            )
            assert pending == 0, "过闸执行后消息被正常 ack"
        finally:
            worker._running = False
            loop.join(timeout=5)
        assert not loop.is_alive()
