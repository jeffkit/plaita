"""plaita#327（E2 第一切）：per-tenant DLQ 配额——闸序/两键成对/隔离流同闸。

## 缺陷

队列/DLQ 平台共享（``tenant_context`` 平台机制键不分区）：平台 DLQ 单
Stream、全局 xtrim（上限 1000 全租户共享）——单租户洪水可灌满全平台死信
配额，把其他租户的死信证据挤掉；租户隔离流（``{dlq}:quarantine:{tenant}``）
同样 ``maxlen=dlq_max_len`` 且 console 不可见，是天然的绕行暗面。

## 规格（正文 + A2 r60 评审）

- **满格行为**：拒新收容 + 留 pending + 具名 reason ``dlq_quota_full`` +
  告警；**不挤旧**（挤旧=静默销毁恢复载体）。满格 = 「无着陆点」，与 #312
  「无着陆点不 ack」同构；确认态撞满格的留 pending 必须**独立计数口径**
  （``dlq_quota_full_pending``），不与认识论性（``tenant_reject_epistemic_pending``）
  混数。
- **闸序**：purity（#277 写闸）→ quota → write，单一写入口；配额满格不得
  把消息引去另一条流。
- **两键成对**：``delete_tenant`` 的 DLQ purge 必须与计数清零成对（console
  侧 tests/console/test_issue327_dlq_quota_purge_pairing.py 复刻 A2 r58
  allowed=True 剧本）；本文件锁队列侧闸面。
- **隔离流同受配额**：同一计数键（绕行暗面封死）。
- 配额数值 env 可调（``PLAITA_TENANT_DLQ_MAX_ENTRIES``，0=不限），owner
  三选一后零成本切换。

## 负控（还原修复后必须精确红）

- 去掉平台 DLQ 配额闸：满格用例由「拒新留 pending」变红（照旧写入）；
- 去掉隔离流配额闸：确认态×满格用例由「不收容」变红（照旧收容+ack）；
- 闸序颠倒（quota 先于 purity）：直接调 ``dead_letter`` 的旧世代用例由
  「抛 TenantWriteRejected」变红（改抛 TenantDlqQuotaFull）。
"""
from __future__ import annotations

import json
from datetime import datetime

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server import tenant_dlq_quota as q
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import (
    PAYLOAD_FIELD,
    RedisStreamTaskQueue,
    StreamTask,
    enqueue_task,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.tenant_context import TENANT_STATUS_KEY

QUEUE = "test:a327:tasks"
GROUP = "plaita-workers"
DLQ = f"{QUEUE}:dlq"


def _worker(fake, storage):
    flow_def = {
        "flow_id": "f1",
        "version": "1",
        "nodes": [
            {"id": "start", "type": "start", "next": "end"},
            {"id": "end", "type": "end", "output": "ok"},
        ],
    }
    fs = MemoryFlowStorage()
    fs.save_flow(flow_def)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage,
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _queue(fake, worker, *, max_deliveries=1, with_guard=True):
    return RedisStreamTaskQueue(
        fake,
        QUEUE,
        group_name=GROUP,
        consumer_name="c1",
        max_deliveries=max_deliveries,
        dlq_key=DLQ,
        claim_min_idle_ms=0,
        dead_letter_guard=worker._dead_letter_guard if with_guard else None,
    )


def _terminal_state(exec_id):
    return ExecutionState(
        execution_id=exec_id,
        flow_id="f1",
        flow_version="1",
        status="completed",
        context={},
        end_time=datetime.now().isoformat(),
    )


def _body(exec_id, tenant):
    return {
        "type": "resume",
        "flow_id": "f1",
        "execution_id": exec_id,
        "resume_type": "continue",
        "tenant_id": tenant,
        "timestamp": datetime.now().isoformat(),
    }


def _enqueue_with_group(fake, body):
    """入队 + 建组（不预投递——预投递的 idle≈0 不可 reclaim，会造成假绿）。"""
    enqueue_task(fake, QUEUE, dict(body))
    fake.xgroup_create(QUEUE, GROUP, id="0")


def _pending_ids(fake):
    return [
        e["message_id"]
        for e in fake.xpending_range(QUEUE, GROUP, min="-", max="+", count=10)
    ]


def _pel_count(fake):
    return int(fake.xpending(QUEUE, GROUP)["pending"])


def _quarantine_len(fake, tenant):
    return int(fake.xlen(f"{DLQ}:quarantine:{tenant}") or 0)


def _fill_quota(fake, tenant, count):
    if count:
        fake.hset(q.TENANT_DLQ_COUNT_KEY, tenant, count)


class TestEnvParsing:
    def test_default_and_zero_and_invalid(self, monkeypatch):
        monkeypatch.delenv(q.TENANT_DLQ_MAX_ENTRIES_ENV, raising=False)
        assert q.tenant_dlq_max_entries() == 200
        monkeypatch.setenv(q.TENANT_DLQ_MAX_ENTRIES_ENV, "0")
        assert q.tenant_dlq_max_entries() is None, "0 = 不限（关闭配额）"
        monkeypatch.setenv(q.TENANT_DLQ_MAX_ENTRIES_ENV, "77")
        assert q.tenant_dlq_max_entries() == 77
        monkeypatch.setenv(q.TENANT_DLQ_MAX_ENTRIES_ENV, "bogus")
        assert q.tenant_dlq_max_entries() == 200, "非法 loud 回默认"

    def test_queue_reads_env_at_init(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        monkeypatch.setenv(q.TENANT_DLQ_MAX_ENTRIES_ENV, "7")
        queue = _queue(fake, _worker(fake, MemoryExecutionStorage()))
        assert queue.tenant_dlq_max_entries == 7


class TestPlatformDlqQuotaGate:
    def test_full_quota_rejects_new_stays_pending(self):
        """满格拒新：不写不 ack，留 pending 等水位回落（不挤旧）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-a327a", _terminal_state("exec-a327a"))
        queue = _queue(fake, _worker(fake, storage))
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        _enqueue_with_group(fake, _body("exec-a327a", "acme"))

        assert queue.read(block_ms=1) is not None  # 首投 → PEL
        _fill_quota(fake, "acme", 200)
        assert queue.read(block_ms=1) is None  # reclaim → 死信尝试被配额拒
        assert fake.xlen(DLQ) == 0, "满格不得写入平台 DLQ"
        assert _pel_count(fake) == 1, "满格 = 无着陆点，载体留 pending"
        assert queue.stats()["dlq_quota_full_pending"] == 1
        pending_ids = _pending_ids(fake)
        assert len(pending_ids) == 1 and pending_ids[0].endswith("-0")

    def test_under_quota_writes_and_bumps_count(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-a327b", _terminal_state("exec-a327b"))
        queue = _queue(fake, _worker(fake, storage))
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        _enqueue_with_group(fake, _body("exec-a327b", "acme"))

        assert queue.read(block_ms=1) is not None  # 首投 → PEL
        _fill_quota(fake, "acme", 3)
        assert queue.read(block_ms=1) is None  # reclaim → 死信落 DLQ
        assert fake.xlen(DLQ) == 1
        assert _pel_count(fake) == 0
        assert q.tenant_dlq_count(fake, "acme") == 4, "写路径计数 +1"

    def test_halfway_crossing_warns_once(self, caplog):
        """过半程告警：跨过 cap/2 的那次写具名告警（落点可辨）。"""
        import logging

        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-a327c", _terminal_state("exec-a327c"))
        queue = _queue(fake, _worker(fake, storage))
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        _enqueue_with_group(fake, _body("exec-a327c", "acme"))

        with caplog.at_level(logging.WARNING, logger="plaita.server.tenant_dlq_quota"):
            assert queue.read(block_ms=1) is not None  # 首投 → PEL
            _fill_quota(fake, "acme", 99)  # +1 == 200//2 → 跨越点
            assert queue.read(block_ms=1) is None  # reclaim → 死信落 DLQ
        assert fake.xlen(DLQ) == 1
        assert any(
            "tenant_dlq.quota_half" in rec.message and "platform_dlq" in rec.message
            for rec in caplog.records
        ), "过半程必须具名告警（含落点名）"

    def test_unattributed_carrier_ungated(self):
        """缺显式 tenant_id 的载体无法归属 → 不闸不计数（与 purge 同口径）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-a327d", _terminal_state("exec-a327d"))
        queue = _queue(fake, _worker(fake, storage))
        body = _body("exec-a327d", None)
        body.pop("tenant_id")
        _enqueue_with_group(fake, body)

        assert queue.read(block_ms=1) is not None  # 首投 → PEL
        _fill_quota(fake, "default", 200)
        assert queue.read(block_ms=1) is None  # reclaim → 死信落 DLQ
        assert fake.xlen(DLQ) == 1, "无法归属的载体不参与配额（不可归属即不可闸）"


class TestGateOrderPurityBeforeQuota:
    def test_dead_letter_raises_purity_not_quota_when_both_hit(self):
        """旧世代 payload × 满格：必须先撞 purity（TenantWriteRejected）。

        若配额闸先于纯净闸，同输入会改抛 TenantDlqQuotaFull——旧世代写面
        的收容分流（A125/#312 语义）被整体跳过，闸序回归即在此精确红。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        queue = _queue(fake, _worker(fake, MemoryExecutionStorage()), with_guard=False)
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        fake.hset(
            "plaita:tenant_status:meta", "acme",
            json.dumps({"tenant_generation": "g2"}),
        )
        _fill_quota(fake, "acme", 200)
        task = StreamTask(
            message_id="1-1",
            body=_body("exec-order", "acme"),
            delivery_count=5,
            tenant_generation="g1",  # 旧世代 → purity 拒
        )
        with pytest.raises(Exception) as excinfo:
            queue.dead_letter(task, reason="max_deliveries=5")
        assert type(excinfo.value).__name__ == "TenantWriteRejected"
        assert fake.xlen(DLQ) == 0
        assert _quarantine_len(fake, "acme") == 0


class TestQuarantineQuotaGate:
    """隔离流同受配额：绕行暗面封死；确认态撞满格独立计数。"""

    def _disabled_tenant_env(self, fake, exec_id):
        storage = MemoryExecutionStorage()
        storage.save_execution_state(exec_id, _terminal_state(exec_id))
        queue = _queue(fake, _worker(fake, storage))
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")  # purity 确认态拒
        return queue

    def test_quarantine_full_blocks_confirmed_quarantine(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        queue = self._disabled_tenant_env(fake, "exec-a327e")
        _enqueue_with_group(fake, _body("exec-a327e", "acme"))

        assert queue.read(block_ms=1) is not None  # 首投 → PEL
        _fill_quota(fake, "acme", 200)
        assert queue.read(block_ms=1) is None  # reclaim → 收容尝试被配额拒
        assert _quarantine_len(fake, "acme") == 0, "隔离流满格不得收容"
        assert fake.xlen(DLQ) == 0
        assert _pel_count(fake) == 1, "确认态×满格：留 pending（不挤旧）"
        stats = queue.stats()
        assert stats["dlq_quota_full_pending"] == 1, "满格独立计数口径"
        assert stats.get("tenant_reject_epistemic_pending", 0) == 0, (
            "确认态满格绝不混入认识论性计数（reason 可辨）"
        )

    def test_quarantine_under_cap_quarantines_and_counts(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        queue = self._disabled_tenant_env(fake, "exec-a327f")
        _enqueue_with_group(fake, _body("exec-a327f", "acme"))

        assert queue.read(block_ms=1) is not None  # 首投 → PEL
        _fill_quota(fake, "acme", 5)
        assert queue.read(block_ms=1) is None  # reclaim → 收容终局
        assert _quarantine_len(fake, "acme") == 1, "配额内收容照旧（A125 守恒）"
        assert _pel_count(fake) == 0
        assert q.tenant_dlq_count(fake, "acme") == 6, "隔离流落点同计数"
