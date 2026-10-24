"""执行载体对账器 shadow（``scripts/reconciler_shadow.py``，plaita#321 A2 阶段 1）负控。

覆盖面（对照 #321 正文 + A2 r138 评审修正 + 任务书四格）：

- **四差异格**：丢消息（queued→start / running→continue / suspended×service→
  redispatch_service）、丢订阅（event 等待：未到期→rebuild_subscription〔enqueue〕/
  已过 deadline→finalize_pending_lost〔control〕）、丢定时器（timer 等待同两分支）、
  多余载体（终态挂载体→gc〔control〕）。每格：制造差异 → gauge 命中具名 kind
  + 意图具名 + 动作类具名 → 修复态 → 归零。
- **零差异正控格**：四类健康执行 → diffs 空、intents 空、互证三项全真。
- **同源互证格**：混合世界（含损坏行/老行 orphan/终态 orphan）→ shadow 按执行
  聚合逐格重放 #316 cells / missing / orphan_terminal 全部相等；并钉「双视互补」
  ——#316 missing=2 而 shadow 可重建差异=1（老行 orphan 不猜 expected）、
  #316 verdict=pel 的行 shadow 仍报 lost_subscription（期望载体视角）。
- **#320 读面复用**：老行无显式 pending、靠 ``$NODE.<id>.subscription_id`` 惰性
  推导 event 等待 → shadow 走 ``effective_pending`` 同口径。
- **custody 义务**：DLQ 在押 = 有载体不补（零 lost_message）、意图 custody_replay。
- **降级**：低版快照（无 per-execution 段）/ 过期快照 → sources_failed 具名、
  loss 类差异不进 gauge、意图记 unverifiable；快照文件缺失轮级上抛。
- **只读纪律**：shadow 轮前后 Redis 键集逐字节不变；落盘 tmp+rename；
  配置缺失 fail-visible（退出码 2）。

基建：fakeredis；执行行走真实 ``RedisExecutionStorage.save_execution_state``
（行形状与生产同源）；actual 侧快照由真实 ``carrier_audit.scan_round`` +
``render_snapshot`` 产出（单分类器，不手拼快照）。
"""
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.storage.base import ExecutionState
from plaita.storage.redis import RedisExecutionStorage
from plaita.tenant_context import (
    DEFAULT_TENANT_ID,
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# 先载 carrier_audit 再载 shadow：shadow 的 ``import carrier_audit`` 复用同一模块
# 对象（单分类器来源在测试进程内也唯一）。
carrier_audit = sys.modules.get("carrier_audit") or _load_script("carrier_audit")
shadow = _load_script("reconciler_shadow")

NS_DEFAULT = "plaita"  # tenant_namespace(DEFAULT_TENANT_ID)
STREAM = "plaita:flow:queue"
TIMER_QUEUE = "plaita:delay:queue"
SUB_PREFIX = "plaita:subscription:"
NOW = datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc).timestamp()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _seed(
    client,
    execution_id,
    status,
    *,
    pending=None,
    context=None,
    tenant=DEFAULT_TENANT_ID,
    age=300,
):
    """经真实存储种一行执行状态（可带 #320 pending 引用 / 老行 context）。"""
    token = set_current_tenant(tenant)
    try:
        store = RedisExecutionStorage(client=client, namespace=tenant_namespace(tenant))
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            tenant_id=tenant,
            context=context if context is not None else {},
            status=status,
            start_time=_iso(NOW - age),
            last_update_time=_iso(NOW - age / 2),
            pending=pending,
        )
        assert store.save_execution_state(execution_id, state) is True
    finally:
        reset_current_tenant(token)


def _message(client, execution_id, stream=STREAM):
    client.xadd(
        stream,
        {"payload": json.dumps({"execution_id": execution_id}), "schema_version": "1"},
    )


def _subscription(client, subscription_id, execution_id, event_type="evt"):
    client.set(
        f"{SUB_PREFIX}data:{subscription_id}",
        json.dumps(
            {
                "subscription_id": subscription_id,
                "event_type": event_type,
                "correlation_id": execution_id,
            }
        ),
    )


def _timer(client, execution_id):
    client.rpush(TIMER_QUEUE, json.dumps({"execution_id": execution_id}))


def _dlq(client, execution_id):
    client.xadd(
        f"{STREAM}:dlq",
        {"payload": json.dumps({"reason": "r", "payload": {"execution_id": execution_id}})},
    )


def _event_pending(subscription_id, *, deadline):
    return {
        "kind": "event",
        "ref_id": subscription_id,
        "event_type": "evt",
        "deadline": deadline,
        "node_id": "wait",
        "recorded_at": _iso(NOW - 100),
    }


def _timer_pending(*, due_at):
    return {"kind": "timer", "ref_id": None, "due_at": due_at, "node_id": "delay"}


def _service_pending():
    return {
        "kind": "service",
        "ref_id": None,
        "node_id": "svc",
        "payload": {"service_type": "delay", "service_config": {"seconds": 5}},
    }


def _audit_snapshot(client, tmp_path, *, now=NOW, tenants="default"):
    """真实 #316 扫描 → schema 2 快照 dict（actual 侧唯一来源）。"""
    config = carrier_audit.load_config(
        redis_url="redis://localhost:6379/9",
        tenants=tenants,
        streams=STREAM,
        timer_queues=TIMER_QUEUE,
        output_dir=str(tmp_path / "audit"),
    )
    result = carrier_audit.scan_round(carrier_audit.ReadOnlyRedis(client), config, now=now)
    return carrier_audit.render_snapshot(result, config)


def _shadow_config(tmp_path, **overrides):
    kwargs = dict(
        redis_url="redis://localhost:6379/9",
        snapshot=str(tmp_path / "audit" / "carrier_snapshot.json"),
        output_dir=str(tmp_path / "shadow"),
    )
    kwargs.update(overrides)
    return shadow.load_config(**kwargs)


def _shadow(snapshot, client, tmp_path, *, now=NOW, **overrides):
    return shadow.shadow_round(
        snapshot, carrier_audit.ReadOnlyRedis(client), _shadow_config(tmp_path, **overrides), now=now
    )


def _intent(result, execution_id):
    hits = [i for i in result.intents if i["execution_id"] == execution_id]
    assert len(hits) <= 1, hits
    return hits[0] if hits else None


def _assert_intent(result, execution_id, *, diffs, intent, action_class):
    record = _intent(result, execution_id)
    assert record is not None, f"{execution_id} 应有意图记录"
    assert record["diffs"] == diffs
    assert record["intent"] == intent
    assert record["action_class"] == action_class
    assert record["verifiable"] is True
    assert result.intent_counts.get((intent, action_class), 0) >= 1
    return record


class TestSnapshotSchema2:
    """#316 快照加法扩展：per-execution 段形状（shadow 候选集的数据前提）。"""

    def test_per_execution_section_shape_and_membership(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q-msg", "queued")
        _message(client, "q-msg")
        _seed(client, "r-none", "running")
        _seed(client, "done-sub", "completed")
        _subscription(client, "s-done", "done-sub")
        _seed(client, "done-clean", "completed")  # 无载体终态：不入段

        snap = _audit_snapshot(client, tmp_path)
        assert snap["schema"] == 2
        by_id = {r["execution_id"]: r for r in snap["executions"]}
        assert set(by_id) == {"q-msg", "r-none", "done-sub"}
        assert by_id["q-msg"] == {
            "execution_id": "q-msg",
            "tenant": "default",
            "status": "queued",
            "carriers": ["pel"],
            "messages": 1,
            "verdict": "pel",
        }
        assert by_id["r-none"]["carriers"] == [] and by_id["r-none"]["verdict"] == "none"
        assert by_id["done-sub"]["carriers"] == ["subscription"]
        # 既有键全保留（加法扩展）。
        for key in ("cells", "ages", "violations", "check_errors", "sources_failed", "missing_samples"):
            assert key in snap


class TestLostMessage:
    def test_queued_without_message_intent_start(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q1", "queued")

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {"lost_message": 1}
        assert result.sources_failed == []
        _assert_intent(result, "q1", diffs=["lost_message"], intent="start", action_class="enqueue")

        # 修复态：start 消息到位 → 差异熄灭、意图清空。
        _message(client, "q1")
        repaired = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert repaired.diffs == {}
        assert repaired.intents == []

    def test_running_without_message_intent_continue(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "r1", "running")  # mode 1：ack 后崩溃，在途消息消失

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {"lost_message": 1}
        _assert_intent(result, "r1", diffs=["lost_message"], intent="continue", action_class="enqueue")

        _message(client, "r1")
        assert _shadow(_audit_snapshot(client, tmp_path), client, tmp_path).diffs == {}

    def test_suspended_service_without_message_intent_redispatch(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "svc1", "suspended", pending=_service_pending())

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {"lost_message": 1}
        record = _assert_intent(
            result, "svc1", diffs=["lost_message"], intent="redispatch_service", action_class="enqueue"
        )
        assert record["pending_kind"] == "service"

    def test_dlq_custody_is_a_carrier_never_lost_message(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "c1", "suspended", pending=_service_pending())
        _dlq(client, "c1")  # 消息在 DLQ 在押：A2 规范性义务——有载体不补

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {}
        record = _assert_intent(result, "c1", diffs=[], intent="custody_replay", action_class="observe")
        assert "重放" in record["note"]


class TestLostSubscription:
    def test_armed_event_wait_without_subscription_intent_rebuild(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "e1", "suspended", pending=_event_pending("s-e1", deadline=_iso(NOW + 86400)))

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {"lost_subscription": 1}
        _assert_intent(
            result, "e1", diffs=["lost_subscription"], intent="rebuild_subscription", action_class="enqueue"
        )

        _subscription(client, "s-e1", "e1")
        repaired = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert repaired.diffs == {} and repaired.intents == []

    def test_expired_event_wait_without_subscription_intent_finalize(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "e2", "suspended", pending=_event_pending("s-e2", deadline=_iso(NOW - 60)))

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {"lost_subscription": 1}
        _assert_intent(
            result, "e2", diffs=["lost_subscription"], intent="finalize_pending_lost", action_class="control"
        )

        # 过 deadline 但订阅仍在场 = 仍 armed，不是 shadow 的差异（终局化由阶段 3 另判）。
        _subscription(client, "s-e2", "e2")
        assert _shadow(_audit_snapshot(client, tmp_path), client, tmp_path).diffs == {}

    def test_legacy_row_effective_pending_derives_event_wait(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        # #320 老行：无显式 pending，靠 $NODE.<$LAST_NODE>.subscription_id 惰性推导。
        _seed(
            client,
            "legacy",
            "suspended",
            context={
                "$LAST_NODE": "wait",
                "$NODE": {"wait": {"subscription_id": "s-legacy", "event_type": "evt"}},
            },
        )

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {"lost_subscription": 1}
        record = _assert_intent(
            result, "legacy", diffs=["lost_subscription"], intent="rebuild_subscription", action_class="enqueue"
        )
        assert record["pending_kind"] == "event"  # migrated 推导、deadline 缺省不编造 → 未到期分支


class TestLostTimer:
    def test_armed_timer_wait_without_timer_intent_rebuild(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "t1", "suspended", pending=_timer_pending(due_at=_iso(NOW + 600)))

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {"lost_timer": 1}
        _assert_intent(result, "t1", diffs=["lost_timer"], intent="rebuild_timer", action_class="enqueue")

        _timer(client, "t1")
        repaired = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert repaired.diffs == {} and repaired.intents == []

    def test_expired_timer_wait_without_timer_intent_finalize(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "t2", "suspended", pending=_timer_pending(due_at=_iso(NOW - 5)))

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.diffs == {"lost_timer": 1}
        _assert_intent(
            result, "t2", diffs=["lost_timer"], intent="finalize_pending_lost", action_class="control"
        )


class TestExtraCarrier:
    def test_terminal_with_carrier_intent_gc(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "done", "completed")
        _subscription(client, "s-done", "done")

        snap = _audit_snapshot(client, tmp_path)
        assert snap["violations"]["orphan_terminal"] == 1
        result = _shadow(snap, client, tmp_path)
        assert result.diffs == {"extra_carrier": 1}
        _assert_intent(result, "done", diffs=["extra_carrier"], intent="gc", action_class="control")
        assert result.candidates == {("default", "completed"): 1}
        assert result.mutual["orphan_terminal_matched"] is True

        client.delete(f"{SUB_PREFIX}data:s-done")
        repaired = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert repaired.diffs == {} and repaired.candidates == {}

    def test_expected_carrier_missing_while_other_carrier_present(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        # 双视互补：event 等待的订阅丢了，但 stream 里残留一条该执行的消息——
        # #316 存放处视角判 carrier=pel（非 missing）；shadow 期望视角仍报
        # lost_subscription（并把残留消息报 extra_carrier）。
        _seed(client, "mix", "suspended", pending=_event_pending("s-mix", deadline=_iso(NOW + 3600)))
        _message(client, "mix")

        snap = _audit_snapshot(client, tmp_path)
        assert snap["violations"]["missing"] == 0
        result = _shadow(snap, client, tmp_path)
        assert result.diffs == {"lost_subscription": 1, "extra_carrier": 1}
        _assert_intent(
            result,
            "mix",
            diffs=["lost_subscription", "extra_carrier"],
            intent="rebuild_subscription",
            action_class="enqueue",
        )


class TestZeroDiffPositive:
    def test_healthy_world_has_no_diffs_and_mutual_all_true(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q", "queued")
        _message(client, "q")
        _seed(client, "r", "running")
        _message(client, "r")
        _seed(client, "e", "suspended", pending=_event_pending("s-e", deadline=_iso(NOW + 3600)))
        _subscription(client, "s-e", "e")
        _seed(client, "t", "suspended", pending=_timer_pending(due_at=_iso(NOW + 60)))
        _timer(client, "t")

        result = _shadow(_audit_snapshot(client, tmp_path), client, tmp_path)
        assert result.sources_failed == []
        assert result.diffs == {}
        assert result.intents == [] and result.intents_total == 0
        assert result.check_errors == 0
        assert result.candidates == {
            ("default", "queued"): 1,
            ("default", "running"): 1,
            ("default", "suspended"): 2,
        }
        assert result.mutual == {
            "checked": True,
            "cells_reproduced": True,
            "missing_matched": True,
            "orphan_terminal_matched": True,
            "mismatch_count": 0,
            "mismatches": [],
        }


class TestMutualVerification:
    def test_mixed_world_reproduces_cells_missing_orphan(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q-none", "queued")  # #316 missing #1；shadow lost_message
        _seed(client, "r-pel", "running")
        _message(client, "r-pel")
        _seed(client, "s-sub", "suspended", pending=_event_pending("s1", deadline=_iso(NOW + 3600)))
        _subscription(client, "s1", "s-sub")
        _seed(client, "s-orphan", "suspended")  # #316 missing #2；老行 orphan，shadow 不猜 expected
        _seed(client, "done", "completed")
        _subscription(client, "s2", "done")  # orphan_terminal
        _seed(client, "bad", "running")
        client.set(f"{NS_DEFAULT}:execution:bad", "{not json")  # 损坏行 → unknown cell
        _seed(client, "acme-q", "queued", tenant="acme")  # 第二租户：cells 分租户重放

        snap = _audit_snapshot(client, tmp_path, tenants="default,acme")
        assert snap["violations"] == {
            "missing": 3,
            "duplicate": 0,
            "orphan_terminal": 1,
            "unbound_subscription": 0,
        }
        assert snap["check_errors"] == 1

        result = _shadow(snap, client, tmp_path)
        assert result.mutual["checked"] is True
        assert result.mutual["cells_reproduced"] is True, result.mutual
        assert result.mutual["missing_matched"] is True
        assert result.mutual["orphan_terminal_matched"] is True
        # 双视互补：#316 missing=3，shadow 可重建差异=2（老行 orphan 不猜 expected）。
        assert result.diffs == {"lost_message": 2, "extra_carrier": 1}
        assert _intent(result, "s-orphan") is None
        # 损坏行：shadow 侧状态行不可归一 → 单条错误计数、不出意图。
        assert result.check_errors == 1
        assert _intent(result, "bad") is None
        assert result.candidates[("default", "unknown")] == 1
        assert result.candidates[("acme", "queued")] == 1

    def test_mutual_flags_tampered_snapshot(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q", "queued")
        snap = _audit_snapshot(client, tmp_path)
        # 人为撕裂快照两视（模拟分类器分叉）：cells 说 none=2，段里只有 1 条。
        for cell in snap["cells"]:
            if cell["carrier"] == "none":
                cell["count"] = 2
        mutual = shadow.mutual_verify(snap)
        assert mutual["cells_reproduced"] is False
        assert mutual["mismatch_count"] == 1
        assert mutual["mismatches"] == ["cells[default/queued/none] snapshot=2 shadow=1"]


class TestDegraded:
    def test_schema1_snapshot_is_a_failed_source(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q", "queued")
        snap = _audit_snapshot(client, tmp_path)
        snap.pop("executions")
        snap["schema"] = 1

        result = _shadow(snap, client, tmp_path)
        assert result.sources_failed == ["snapshot_schema"]
        assert result.executions_seen == 0
        assert result.diffs == {} and result.intents == []
        assert result.mutual == {"checked": False, "reason": "snapshot_schema"}

    def test_stale_snapshot_suppresses_loss_diffs(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q", "queued")
        stale = _audit_snapshot(client, tmp_path, now=NOW - 3600)

        result = _shadow(stale, client, tmp_path)
        assert result.sources_failed == ["snapshot_stale"]
        assert result.snapshot_age_seconds == pytest.approx(3600.0, abs=1.0)
        # 缺席不可判：差异不进 gauge，意图记 unverifiable（报告可见、不翻页）。
        assert result.diffs == {}
        assert result.intents == []
        assert result.intents_total == 1 and result.unverifiable_intents == 1
        # 同一快照放宽陈旧度门限即恢复可判（阈值是部署面旋钮）。
        fresh_enough = _shadow(stale, client, tmp_path, snapshot_max_age_seconds="7200")
        assert fresh_enough.sources_failed == []
        assert fresh_enough.diffs == {"lost_message": 1}

    def test_missing_snapshot_file_raises_round_level(self, tmp_path):
        with pytest.raises(OSError):
            shadow.load_snapshot(tmp_path / "nope" / "carrier_snapshot.json")


class TestReadOnlyAndRender:
    def test_shadow_round_writes_no_redis_keys(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q", "queued")
        _seed(client, "e", "suspended", pending=_event_pending("s-e", deadline=_iso(NOW - 1)))
        _seed(client, "done", "completed")
        _message(client, "done")
        snap = _audit_snapshot(client, tmp_path)
        before = {k: client.dump(k) for k in sorted(client.keys("*"))}

        result = _shadow(snap, client, tmp_path)
        assert result.diffs == {"lost_message": 1, "lost_subscription": 1, "extra_carrier": 1}
        after = {k: client.dump(k) for k in sorted(client.keys("*"))}
        assert after == before

    def test_readonly_wrapper_rejects_writes(self):
        wrapped = carrier_audit.ReadOnlyRedis(fakeredis.FakeStrictRedis(decode_responses=True))
        with pytest.raises(carrier_audit.AuditCommandNotAllowed):
            wrapped.set("x", "1")

    def test_flush_writes_report_and_prom_with_explicit_zero_cells(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "done", "completed")
        _subscription(client, "s-done", "done")
        config = _shadow_config(tmp_path)
        result = shadow.shadow_round(
            _audit_snapshot(client, tmp_path), carrier_audit.ReadOnlyRedis(client), config, now=NOW
        )
        shadow.flush(result, config)

        report = json.loads((config.output_dir / "reconciler_shadow_report.json").read_text())
        assert report["schema"] == 1
        assert report["diffs"] == {
            "lost_message": 0,
            "lost_subscription": 0,
            "lost_timer": 0,
            "extra_carrier": 1,
        }
        assert report["intent_counts"] == [{"intent": "gc", "action_class": "control", "count": 1}]
        assert report["mutual_verification"]["orphan_terminal_matched"] is True
        assert report["snapshot"]["sources_failed"] == []
        assert not list(config.output_dir.glob("*.tmp"))

        prom = (config.output_dir / "reconciler_shadow_metrics.prom").read_text()
        assert "_total" not in prom
        assert 'plaita_reconciler_shadow_diffs{kind="extra_carrier"} 1' in prom
        assert 'plaita_reconciler_shadow_diffs{kind="lost_timer"} 0' in prom
        assert 'plaita_reconciler_shadow_intents{intent="gc",action_class="control"} 1' in prom
        assert 'plaita_reconciler_shadow_intents{intent="start",action_class="enqueue"} 0' in prom
        assert 'plaita_reconciler_shadow_candidates{status="queued",tenant="default"} 0' in prom
        assert 'plaita_reconciler_shadow_candidates{status="completed",tenant="default"} 1' in prom
        assert "plaita_reconciler_shadow_sources_failed 0" in prom
        assert "plaita_reconciler_shadow_check_errors 0" in prom
        assert "# TYPE plaita_reconciler_shadow_last_success_timestamp gauge" in prom
        # 全部意图×动作类格显式发（9×3），degraded/空轮也不蒸发。
        assert prom.count("plaita_reconciler_shadow_intents{") == len(shadow.INTENT_KINDS) * len(
            shadow.ACTION_CLASSES
        )

    def test_config_requires_redis_url_and_follows_audit_output_dir(self, monkeypatch, tmp_path):
        monkeypatch.delenv(shadow.ENV_REDIS_URL, raising=False)
        with pytest.raises(shadow.ShadowConfigError):
            shadow.load_config()
        assert shadow.main(["--once"]) == 2

        monkeypatch.setenv("PLAITA_CARRIER_AUDIT_OUTPUT_DIR", str(tmp_path / "audit-out"))
        config = shadow.load_config(redis_url="redis://localhost:6379/9")
        assert config.snapshot_path == tmp_path / "audit-out" / "carrier_snapshot.json"
        assert config.interval_seconds == 10.0
        assert config.snapshot_max_age_seconds == 120.0

        with pytest.raises(shadow.ShadowConfigError):
            shadow.load_config(redis_url="redis://x", snapshot_max_age_seconds="0")

    def test_main_once_end_to_end_over_audit_snapshot_file(self, monkeypatch, tmp_path):
        import time as _time

        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q", "queued")
        # #316 真实 flush 落盘（tmp+rename）→ shadow 以文件为唯一候选来源。
        audit_config = carrier_audit.load_config(
            redis_url="redis://localhost:6379/9",
            tenants="default",
            streams=STREAM,
            timer_queues=TIMER_QUEUE,
            output_dir=str(tmp_path / "audit"),
        )
        carrier_audit.flush(
            carrier_audit.scan_round(
                carrier_audit.ReadOnlyRedis(client), audit_config, now=_time.time()
            ),
            audit_config,
        )
        monkeypatch.setattr(shadow, "build_client", lambda _url: carrier_audit.ReadOnlyRedis(client))

        rc = shadow.main(
            [
                "--once",
                "--redis-url",
                "redis://localhost:6379/9",
                "--snapshot",
                str(tmp_path / "audit" / "carrier_snapshot.json"),
                "--output-dir",
                str(tmp_path / "shadow"),
            ]
        )
        assert rc == 0
        report = json.loads((tmp_path / "shadow" / "reconciler_shadow_report.json").read_text())
        assert report["snapshot"]["sources_failed"] == []
        assert report["diffs"]["lost_message"] == 1
        assert report["intents"][0]["intent"] == "start"
        assert report["mutual_verification"]["cells_reproduced"] is True

        # 快照文件缺失 → 轮级失败 fail-visible（rc=1），上一轮产物不被覆写。
        (tmp_path / "audit" / "carrier_snapshot.json").unlink()
        before = (tmp_path / "shadow" / "reconciler_shadow_report.json").read_bytes()
        assert (
            shadow.main(
                [
                    "--once",
                    "--redis-url",
                    "redis://localhost:6379/9",
                    "--snapshot",
                    str(tmp_path / "audit" / "carrier_snapshot.json"),
                    "--output-dir",
                    str(tmp_path / "shadow"),
                ]
            )
            == 1
        )
        assert (tmp_path / "shadow" / "reconciler_shadow_report.json").read_bytes() == before

    def test_expected_carriers_table(self):
        future = _iso(NOW + 10)
        past = _iso(NOW - 10)
        table = shadow.expected_carriers
        assert table("queued", None, now=NOW) == ({"message"}, False)
        assert table("running", None, now=NOW) == ({"message"}, False)
        assert table("running", _service_pending(), now=NOW) == ({"message"}, False)
        assert table("running", _event_pending("s", deadline=future), now=NOW) == ({"subscription"}, False)
        assert table("suspended", _event_pending("s", deadline=past), now=NOW) == ({"subscription"}, True)
        assert table("suspended", _timer_pending(due_at=future), now=NOW) == ({"timer"}, False)
        assert table("suspended", _timer_pending(due_at=past), now=NOW) == ({"timer"}, True)
        assert table("suspended", {"kind": "cancel"}, now=NOW) == ({"message"}, False)
        assert table("suspended", None, now=NOW) == (set(), False)
        assert table("completed", None, now=NOW) == (set(), False)
        assert table("weird", None, now=NOW) == (set(), False)
        # 不可解析 deadline 不编造到期（老行 migrated 口径）。
        assert table("suspended", _event_pending("s", deadline=None), now=NOW) == ({"subscription"}, False)
