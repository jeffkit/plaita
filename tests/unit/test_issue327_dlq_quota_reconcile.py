"""plaita#327（E2 wave 2）：配额观测面——周期扫描对账纠偏 + 告警线。

## 规格（正文 + A2 r60 评审③ + owner 裁决 2026-10-24 四点全定）

wave 1（B3 r100）已落：写路径配额闸（满格拒新留 pending 具名
``dlq_quota_full``）、隔离流同闸、闸序 purity→quota→write、两键成对、
env 旋钮。本轮按裁决补观测半边：

- **②d 计数分工**：闸门 = 写路径计数（HINCRBY 原子）；观测 = 周期扫描
  对账纠偏（:func:`reconcile_tenant_dlq_counts`，复用 #316 扫描器骨架——
  XRANGE 解析 payload ``tenant_id`` + 隔离流 XLEN，不新增进程）。计数只朝
  虚高漂（xtrim 不通知），**纠偏是满格 pending「等水位回落」的收敛半边**
  ——虚高不纠，满格载体永无回头日。
- **②b 分流名告警**：任一租户两流合计 >50% 上限 → 具名 WARNING（带
  ``platform_dlq``/``quarantine`` 落点名）。
- **②e trim 互挤**：明示可接受（模块 docstring）；总量告警线 = 平台 DLQ
  长度 > maxlen 20%（:data:`DLQ_TOTAL_ALERT_RATIO`）→ 具名早警。
- **②a/c/f 守恒**：满格告警文案、满格不引去另一条流（A2：拒收改道 =
  配额绕行 + 纯净性破面）、纠偏间隔 env 旋钮同款纪律。

## 负控（还原修复后必须精确红）

- 删纠偏写面：d-1 由「幻影计数回落、满格解闭」变红（虚高永存）；
- 删分流名/总量告警：b-1/e-1 由「具名 WARNING 在场」变红（静默）；
- 满格改道（把拒收改写成写隔离流腾额度）：c-1 由「隔离流空」变红。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server import tenant_dlq_quota as q
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import (
    RedisStreamTaskQueue,
    enqueue_task,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.tenant_context import TENANT_STATUS_KEY

QUEUE = "test:a327w2:tasks"
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


def _queue(fake, worker):
    return RedisStreamTaskQueue(
        fake,
        QUEUE,
        group_name=GROUP,
        consumer_name="c1",
        max_deliveries=1,
        dlq_key=DLQ,
        claim_min_idle_ms=0,
        dead_letter_guard=worker._dead_letter_guard,
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
    enqueue_task(fake, QUEUE, dict(body), max_deliveries=None)
    fake.xgroup_create(QUEUE, GROUP, id="0")


def _pel_count(fake):
    return int(fake.xpending(QUEUE, GROUP)["pending"])


def _dlq_entry(tenant, exec_id="e-seed"):
    """与 dead_letter 写面同形的信封（payload.body.tenant_id 归属链）。"""
    return {
        "payload": json.dumps(
            {"payload": {"tenant_id": tenant, "execution_id": exec_id}, "reason": "x"}
        )
    }


class TestFullRejectionAlertConserved:
    def test_quota_full_named_alert_and_no_reroute_to_quarantine(self, caplog):
        """②a 守恒 + ②c 引道面：满格拒收 = 留 pending + 具名
        ``dlq_quota_full`` 告警，且**绝不**把消息引去隔离流（改道 = 配额
        绕行暗面复活 + 纯净性破面）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-w2a", _terminal_state("exec-w2a"))
        queue = _queue(fake, _worker(fake, storage))
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        _enqueue_with_group(fake, _body("exec-w2a", "acme"))

        assert queue.read(block_ms=1) is not None  # 首投 → PEL
        fake.hset(q.TENANT_DLQ_COUNT_KEY, "acme", 200)
        with caplog.at_level(logging.WARNING, logger="plaita.server.task_queue"):
            assert queue.read(block_ms=1) is None  # reclaim → 满格拒收
        assert fake.xlen(DLQ) == 0
        assert fake.xlen(f"{DLQ}:quarantine:acme") == 0, "满格不得引去隔离流"
        assert _pel_count(fake) == 1, "无着陆点 = 留 pending"
        assert queue.stats()["dlq_quota_full_pending"] == 1
        assert "dlq_quota_full" in caplog.text, "满格拒绝必须具名告警"


class TestReconcileSplitSurfaceAlert:
    def test_half_cap_alert_names_both_surfaces(self, caplog):
        """②b：两流合计 >50% 上限 → 具名告警带落点分流名；≤50% 不告警。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        for i in range(60):
            fake.xadd(DLQ, _dlq_entry("acme", f"e-p{i}"))
        for i in range(50):
            fake.xadd(f"{DLQ}:quarantine:acme", _dlq_entry("acme", f"e-q{i}"))
        for i in range(40):
            fake.xadd(DLQ, _dlq_entry("calm", f"e-c{i}"))

        with caplog.at_level(logging.WARNING, logger="plaita.server.tenant_dlq_quota"):
            report = q.reconcile_tenant_dlq_counts(fake, DLQ, dlq_max_len=1000, cap=200)

        acme = report["per_tenant"]["acme"]
        assert acme == {"platform_dlq": 60, "quarantine": 50, "desired": 110}
        assert any(a["tenant"] == "acme" for a in report["alerts"])
        assert not any(a["tenant"] == "calm" for a in report["alerts"]), "40≤100 不告警"
        assert any(
            "tenant_dlq.quota_watermark" in rec.message
            and "platform_dlq:60" in rec.message
            and "quarantine:50" in rec.message
            for rec in caplog.records
        ), "分流名必须双双在场"


class TestReconcileCorrection:
    def test_phantom_high_count_corrected_and_quota_unsticks(self, caplog):
        """②d：xtrim 挤旧导致计数虚高（幻影 200）→ 纠偏回真实 1 → 满格
        解闭，载体重投可过闸落 DLQ（「等水位回落」的收敛半边）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-w2d", _terminal_state("exec-w2d"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "acme", "active")
        fake.xadd(DLQ, _dlq_entry("acme", "exec-w2d"))  # 真实水位 = 1
        fake.hset(q.TENANT_DLQ_COUNT_KEY, "acme", 200)  # 幻影满格
        _enqueue_with_group(fake, _body("exec-w2d", "acme"))

        assert queue.read(block_ms=1) is not None  # 首投 → PEL
        assert queue.read(block_ms=1) is None  # 满格拒收 → pending
        assert _pel_count(fake) == 1

        report = q.reconcile_tenant_dlq_counts(fake, DLQ, dlq_max_len=1000, cap=200)
        assert {"tenant": "acme", "from": 200, "to": 1} in report["corrections"]
        assert q.tenant_dlq_count(fake, "acme") == 1
        assert q.tenant_dlq_quota_allows(fake, "acme", 200)[0] is True

        assert queue.read(block_ms=1) is None  # 重投 → 过闸落 DLQ
        assert fake.xlen(DLQ) == 2, "种子 1 条 + 解闭后新落 1 条"
        assert _pel_count(fake) == 0

    def test_worker_hook_drives_reconcile_on_due(self, monkeypatch):
        """接线面：worker 周期 hook 首调即扫（到期语义同残留回收），
        纠偏经 hook 生效。"""
        monkeypatch.setenv(q.TENANT_DLQ_RECONCILE_INTERVAL_ENV, "600")
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake, MemoryExecutionStorage())
        fake.xadd(DLQ, _dlq_entry("acme", "e1"))
        fake.hset(q.TENANT_DLQ_COUNT_KEY, "acme", 50)

        fake_queue = MagicMock()
        fake_queue.redis = fake
        fake_queue.dlq_key = DLQ
        fake_queue.dlq_max_len = 1000
        fake_queue.tenant_dlq_max_entries = 200
        worker._reconcile_dlq_quota_if_due(fake_queue)

        assert q.tenant_dlq_count(fake, "acme") == 1, "hook 必须驱动纠偏"
        # 未到期（interval 内二调）：mock 面不再变化也无可断言差异，
        # 用锁内 last 时间戳证明走到期门。
        assert worker._last_dlq_reconcile is not None

    def test_missing_count_corrected_upwards(self):
        """②d 反向：bump 失败计数虚低（真实 2、计数缺）→ 纠偏上调——
        闸门不得因记账失败朝放任方向漂。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.xadd(DLQ, _dlq_entry("acme", "e1"))
        fake.xadd(DLQ, _dlq_entry("acme", "e2"))

        report = q.reconcile_tenant_dlq_counts(fake, DLQ, dlq_max_len=1000, cap=200)

        assert {"tenant": "acme", "from": 0, "to": 2} in report["corrections"]


class TestTotalWatermarkAlert:
    def test_platform_total_over_20pct_warns(self, caplog):
        """②e：平台 DLQ 总长 > maxlen 20% → 具名早警（trim 互挤明示可
        接受，但 collective fill 挤旧之前必须可见）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        for i in range(3):
            fake.xadd(DLQ, _dlq_entry("acme", f"e{i}"))

        with caplog.at_level(logging.WARNING, logger="plaita.server.tenant_dlq_quota"):
            report = q.reconcile_tenant_dlq_counts(fake, DLQ, dlq_max_len=10, cap=200)
        assert report["total_watermark"] is True
        assert any("tenant_dlq.total_watermark" in rec.message for rec in caplog.records)

        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="plaita.server.tenant_dlq_quota"):
            report = q.reconcile_tenant_dlq_counts(fake, DLQ, dlq_max_len=100, cap=200)
        assert report["total_watermark"] is False, "3 ≤ 20 不告警"
        assert not any("total_watermark" in rec.message for rec in caplog.records)


class TestReconcileIntervalKnob:
    def test_env_parsing_loud_invalid(self, monkeypatch):
        """②f：纠偏间隔 env 旋钮同款纪律（缺省 600 / 0=关 / 非法 loud 回默认）。"""
        monkeypatch.delenv(q.TENANT_DLQ_RECONCILE_INTERVAL_ENV, raising=False)
        assert q.tenant_dlq_reconcile_interval_seconds() == 600.0
        monkeypatch.setenv(q.TENANT_DLQ_RECONCILE_INTERVAL_ENV, "0")
        assert q.tenant_dlq_reconcile_interval_seconds() == 0.0, "0 = 关闭扫描纠偏"
        monkeypatch.setenv(q.TENANT_DLQ_RECONCILE_INTERVAL_ENV, "30")
        assert q.tenant_dlq_reconcile_interval_seconds() == 30.0
        monkeypatch.setenv(q.TENANT_DLQ_RECONCILE_INTERVAL_ENV, "-5")
        assert q.tenant_dlq_reconcile_interval_seconds() == -5.0, "负值 = 关闭（<=0）"
        monkeypatch.setenv(q.TENANT_DLQ_RECONCILE_INTERVAL_ENV, "bogus")
        assert q.tenant_dlq_reconcile_interval_seconds() == 600.0, "非法 loud 回默认"
