"""A2R31 施工回归：配置代际 / 单一 resolver / 非法值口径（r31 B2 类）。

覆盖三条规格的验收与负控形态：

* ``B2R31-01`` — 三维代际（family/generation/budget）、首投/reclaim 同判、
  重投继承、旧 PEL 预算不立即死信、租户状态纪元；
* ``B2R31-02`` — queue/redis 单一 resolver + 空串统一拒绝 + 实例 id 对齐；
* ``B2R31-03`` — identity/security/resource fail-fast、optional WARNING+
  安全默认、禁止静默回退/钳制。

负控证据（pre-fix 精确红）见台账 r46：``/tmp/a2r31-r46/negcontrol/``。
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita import runtime_config as rc
from plaita.server import task_queue as tq
from plaita.server import tenant_status as ts
from plaita.server.task_queue import (
    LEGACY_GENERATION,
    RedisStreamTaskQueue,
    StreamTask,
    enqueue_task,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

REPO_ROOT = Path(__file__).resolve().parents[2]
CONSOLE_BACKEND = REPO_ROOT / "plaita-console" / "backend"
if str(CONSOLE_BACKEND) not in sys.path:
    sys.path.insert(0, str(CONSOLE_BACKEND))
STREAM = "test:r31-config"


# ---------------------------------------------------------------------------
# B2R31-01 — generation identity
# ---------------------------------------------------------------------------
def _generation(**kwargs):
    base = dict(
        queue_name="plaita:flow:queue",
        consumer_group="plaita-workers",
        dlq_key="plaita:flow:queue:dlq",
        deployment_epoch="epoch-1",
        tenant_status_epoch="epoch-1",
        lease_ttl_seconds=300,
        max_deliveries=5,
        task_max_age_seconds=3600,
        dlq_max_len=1000,
    )
    base.update(kwargs)
    return rc.build_generation(**base)


class TestGenerationCanonical:
    def test_same_semantics_same_ids(self):
        a = _generation(queue_name="  plaita:flow:queue  ")
        b = _generation(queue_name="plaita:flow:queue")
        assert a.family == b.family
        assert a.generation == b.generation
        assert a.budget == b.budget

    def test_observability_knobs_do_not_change_generation(self, monkeypatch):
        before = _generation()
        monkeypatch.setenv("PLAITA_LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("PLAITA_METRICS_PORT", "9999")
        after = _generation()
        assert before.generation == after.generation

    def test_queue_budget_epoch_change_ids(self):
        base = _generation()
        assert _generation(queue_name="other:queue").family != base.family
        assert _generation(max_deliveries=3).budget != base.budget
        assert _generation(max_deliveries=3).generation != base.generation
        assert _generation(deployment_epoch="epoch-2").generation != base.generation

    def test_producer_and_consumer_agree(self, monkeypatch):
        for name in (
            "PLAITA_DEPLOYMENT_EPOCH", "PLAITA_CONSUMER_GROUP", "PLAITA_DLQ_KEY",
            "PLAITA_MAX_DELIVERIES", "PLAITA_LEASE_TTL_SECONDS", "QUEUE_NAME",
            "PLAITA_QUEUE_NAME", "PLAITA_CONSOLE_TASK_QUEUE",
        ):
            monkeypatch.delenv(name, raising=False)
        envelope = rc.envelope_for_stream("plaita:flow:queue", role="console")
        local = rc.build_generation(queue_name="plaita:flow:queue")
        assert envelope["config_family"] == local.family
        assert envelope["budget_generation"] == local.budget

    def test_generation_config_legacy_disable_dialect(self):
        """DISABLE_* 仍只认精确 1；`true` 不改变安全语义，但必须 WARNING。"""
        legacy = rc.load_runtime_config(
            "worker",
            environ={"PLAITA_DISABLE_FENCING": "true"},
            strict=False,
        )
        assert legacy.fencing is True
        assert any("DISABLE_FENCING" in w for w in legacy.warnings)
        off = rc.load_runtime_config(
            "worker",
            environ={"PLAITA_DISABLE_FENCING": "1"},
            strict=False,
        )
        assert off.fencing is False
        assert off.generation.generation != legacy.generation.generation

    def test_malformed_generation_is_legacy_not_current(self):
        assert rc.normalize_generation("not-a-family", kind="family") == LEGACY_GENERATION
        assert rc.normalize_generation(None, kind="budget") == LEGACY_GENERATION
        valid = rc.build_generation(queue_name=STREAM).family
        assert rc.normalize_generation(valid, kind="family") == valid


def _queue(redis, **kwargs):
    kwargs.setdefault("group_name", "g")
    kwargs.setdefault("consumer_name", "c1")
    kwargs.setdefault("claim_min_idle_ms", 0)
    return RedisStreamTaskQueue(redis, STREAM, **kwargs)


class TestFamilyGate:
    def test_first_delivery_mismatch_leaves_pending(self, caplog):
        redis = fakeredis.FakeRedis(decode_responses=True)
        enqueue_task(
            redis, STREAM, {"type": "start", "flow_id": "f", "execution_id": "e1"},
            config_family="cf1-bbbbbbbbbbbb",
        )
        queue = _queue(redis, config_family="cf1-aaaaaaaaaaaa")
        with caplog.at_level("WARNING"):
            assert queue.read(block_ms=1) is None
        pending = redis.xpending_range(STREAM, "g", "-", "+", 10)
        assert pending and pending[0]["times_delivered"] == 1
        assert redis.xlen(STREAM) == 1
        assert queue.stats()["config_family_mismatch"] == 1
        assert "config.family_mismatch" in caplog.text

    def test_legacy_message_still_executes(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        enqueue_task(redis, STREAM, {"type": "start", "flow_id": "f", "execution_id": "e1"})
        queue = _queue(redis, config_family="cf1-aaaaaaaaaaaa")
        task = queue.read(block_ms=1)
        assert task is not None
        assert task.config_family == LEGACY_GENERATION

    def test_reclaim_mismatch_does_not_burn_delivery(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        msg = enqueue_task(
            redis, STREAM, {"type": "start", "flow_id": "f", "execution_id": "e1"},
            config_family="cf1-bbbbbbbbbbbb",
        )
        seed = _queue(redis, consumer_name="c1", config_family="cf1-bbbbbbbbbbbb")
        seed.ensure_group()
        assert seed.read(block_ms=1) is not None  # 首投进 PEL（times_delivered=1）
        reader = _queue(redis, consumer_name="c2", config_family="cf1-aaaaaaaaaaaa")
        assert reader.read(block_ms=1) is None
        pending = redis.xpending_range(STREAM, "g", "-", "+", 10)
        assert pending[0]["message_id"] == msg
        assert pending[0]["times_delivered"] == 1, "family 预读必须在 XCLAIM 之前"
        assert reader.stats()["config_family_mismatch"] == 1

    def test_family_mismatch_over_age_is_dead_lettered(self, monkeypatch):
        """A2R31-01：family 不匹配不能无限占 PEL——超过 task_max_age 进 DLQ。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        enqueue_task(
            redis, STREAM, {"type": "start", "flow_id": "f", "execution_id": "e1"},
            config_family="cf1-bbbbbbbbbbbb",
        )
        seed = _queue(redis, consumer_name="c1", config_family="cf1-bbbbbbbbbbbb")
        seed.ensure_group()
        assert seed.read(block_ms=1) is not None
        monkeypatch.setattr(
            tq, "stream_id_timestamp",
            lambda _mid: datetime.now() - timedelta(hours=2),
        )
        reader = _queue(
            redis, consumer_name="c2", config_family="cf1-aaaaaaaaaaaa",
            task_max_age_seconds=60,
        )
        assert reader.read(block_ms=1) is None
        assert redis.xlen(f"{STREAM}:dlq") == 1
        assert redis.xpending(STREAM, "g")["pending"] == 0
        import json
        envelope = json.loads(redis.xrange(f"{STREAM}:dlq")[0][1]["payload"])
        assert envelope["config_family"] == "cf1-bbbbbbbbbbbb"
        assert "config_family_mismatch_age" in envelope["reason"]


class TestBudgetStale:
    def test_old_pel_budget_not_immediately_dead_lettered(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        old_budget = "bg1-111111111111"
        local_budget = "bg1-222222222222"
        msg = enqueue_task(
            redis, STREAM, {"type": "start", "flow_id": "f", "execution_id": "e1"},
            budget_generation=old_budget,
        )
        seed = _queue(redis, consumer_name="c1", max_deliveries=5)
        seed.ensure_group()
        redis.xreadgroup("g", "c1", {STREAM: ">"}, count=1)
        for _ in range(3):
            redis.xclaim(STREAM, "g", "c1", min_idle_time=0, message_ids=[msg])
        pending = redis.xpending_range(STREAM, "g", "-", "+", 10)
        assert pending[0]["times_delivered"] == 4

        new = _queue(
            redis, consumer_name="c2", max_deliveries=3,
            budget_generation=local_budget,
        )
        assert new.read(block_ms=1) is None
        assert redis.xlen(f"{STREAM}:dlq") == 0, "新预算不得立即死信旧 PEL"
        assert redis.xpending(STREAM, "g")["pending"] == 1
        assert new.stats()["config_budget_stale"] == 1

    def test_same_budget_still_enforces_max_deliveries(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        budget = "bg1-333333333333"
        msg = enqueue_task(
            redis, STREAM, {"type": "start", "flow_id": "f", "execution_id": "e1"},
            budget_generation=budget,
        )
        seed = _queue(redis, consumer_name="c1", max_deliveries=5)
        seed.ensure_group()
        redis.xreadgroup("g", "c1", {STREAM: ">"}, count=1)
        for _ in range(3):
            redis.xclaim(STREAM, "g", "c1", min_idle_time=0, message_ids=[msg])
        new = _queue(
            redis, consumer_name="c2", max_deliveries=3, budget_generation=budget,
        )
        assert new.read(block_ms=1) is None
        assert redis.xlen(f"{STREAM}:dlq") == 1


class TestRequeueInheritance:
    def test_source_generation_inherited(self):
        from plaita.server.flow_worker import _requeue_envelope_kwargs

        task = StreamTask(
            message_id="1-0", body={}, schema_version=1,
            config_family="cf1-aaaaaaaaaaaa",
            config_generation="cfg1-aaaaaaaaaaaa",
            budget_generation="bg1-aaaaaaaaaaaa",
            config_effective_at="2026-10-20T00:00:00+00:00",
        )
        kwargs = _requeue_envelope_kwargs(task)
        assert kwargs["schema_version"] == 1
        assert kwargs["config_family"] == "cf1-aaaaaaaaaaaa"
        assert kwargs["config_generation"] == "cfg1-aaaaaaaaaaaa"
        assert kwargs["budget_generation"] == "bg1-aaaaaaaaaaaa"
        assert kwargs["config_effective_at"] == "2026-10-20T00:00:00+00:00"

    def test_legacy_source_does_not_fabricate_current(self):
        from plaita.server.flow_worker import _requeue_envelope_kwargs

        kwargs = _requeue_envelope_kwargs(StreamTask(message_id="1-0", body={}))
        assert kwargs == {"schema_version": 1}

    def test_dlq_envelope_preserves_generation(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        queue = _queue(redis)
        task = StreamTask(
            message_id="1-0", body={"x": 1}, delivery_count=1,
            config_family="cf1-aaaaaaaaaaaa",
            config_generation="cfg1-aaaaaaaaaaaa",
            budget_generation="bg1-aaaaaaaaaaaa",
        )
        queue.dead_letter(task, reason="test")
        raw = redis.xrange(f"{STREAM}:dlq")[0][1]["payload"]
        import json
        envelope = json.loads(raw)
        assert envelope["config_family"] == "cf1-aaaaaaaaaaaa"
        assert envelope["budget_generation"] == "bg1-aaaaaaaaaaaa"


class TestGroupRenameAndOrphan:
    def test_group_rename_start_id_dollar_does_not_replay(self, monkeypatch):
        redis = fakeredis.FakeRedis(decode_responses=True)
        enqueue_task(redis, STREAM, {"type": "start", "flow_id": "f1", "execution_id": "e1"})
        enqueue_task(redis, STREAM, {"type": "start", "flow_id": "f1", "execution_id": "e2"})
        monkeypatch.setenv("PLAITA_CONSUMER_GROUP_START_ID", "$")
        queue = RedisStreamTaskQueue(
            redis, STREAM, group_name="renamed-g", consumer_name="c1",
            claim_min_idle_ms=0,
        )
        assert queue.read(block_ms=1) is None, "新组从 $ 建立不得重放历史"
        assert redis.xlen(STREAM) == 2
        assert int(redis.xpending(STREAM, "renamed-g")["pending"]) == 0

    def test_orphan_stream_with_pending_warns(self, caplog):
        redis = fakeredis.FakeRedis(decode_responses=True)
        old_stream = f"{STREAM}-old"
        enqueue_task(redis, old_stream, {"type": "start", "flow_id": "f1", "execution_id": "e1"})
        redis.xgroup_create(old_stream, "g", id="0")
        redis.xreadgroup("g", "c1", {old_stream: ">"}, count=1)
        queue = RedisStreamTaskQueue(
            redis, f"{STREAM}-v2", group_name="g", consumer_name="c1",
        )
        with caplog.at_level("WARNING"):
            summary = queue.preflight_orphan_streams()
        assert summary["orphans"]
        assert summary["orphans"][0]["stream"] == old_stream
        assert "config.orphan_stream" in caplog.text


class TestDispatchFamily:
    def test_mixed_generation_candidate_is_rejected(self):
        from plaita.server.capacity import DispatchCandidate, decide_route

        a = DispatchCandidate(
            instance_id="a", metadata={"queue_name": STREAM, "config_family": "cf1-aaaaaaaaaaaa"},
        )
        b = DispatchCandidate(
            instance_id="b", metadata={"queue_name": STREAM, "config_family": "cf1-bbbbbbbbbbbb"},
        )
        decision = decide_route(
            [a, b], queue_name=STREAM, config_family="cf1-aaaaaaaaaaaa"
        )
        assert decision.target is a
        assert [r.instance_id for r in decision.rejected] == ["b"]
        assert "config: family mismatch" in decision.rejected[0].reason

    def test_legacy_candidate_is_not_filtered(self):
        from plaita.server.capacity import DispatchCandidate, decide_route

        legacy = DispatchCandidate(
            instance_id="old", metadata={"queue_name": STREAM},
        )
        decision = decide_route(
            [legacy], queue_name=STREAM, config_family="cf1-aaaaaaaaaaaa"
        )
        assert decision.target is legacy


# ---------------------------------------------------------------------------
# B2R31-01 — tenant status epoch
# ---------------------------------------------------------------------------
class TestTenantStatusEpoch:
    def test_restore_after_disable_is_rejected_non_default(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        ts.publish_tenant_status(fake, "acme", "disabled", epoch="100")
        # Redis-only restore of an old snapshot: status active, no epoch key.
        fake.hset(ts.TENANT_STATUS_KEY, "acme", "active")
        fake.delete(ts.TENANT_STATUS_EPOCH_KEY)
        before = ts.tenant_stale_epoch_total()
        assert ts.tenant_is_active(fake, "acme", expected_epoch="100") is False
        assert ts.tenant_stale_epoch_total() > before

    def test_default_keeps_fail_open(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.hset(ts.TENANT_STATUS_KEY, "default", "active")
        assert ts.tenant_is_active(fake, "default", expected_epoch="100") is True

    def test_newer_redis_epoch_rejects_old_worker(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        ts.publish_tenant_status(fake, "acme", "active", epoch="101")
        assert ts.tenant_is_active(fake, "acme", expected_epoch="100") is False
        assert ts.tenant_is_active(fake, "default", expected_epoch="100") is False

    def test_legacy_expected_epoch_keeps_old_behavior(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        ts.publish_tenant_status(fake, "acme", "active")
        assert ts.tenant_is_active(fake, "acme", expected_epoch="legacy") is True


# ---------------------------------------------------------------------------
# B2R31-02 — single resolver
# ---------------------------------------------------------------------------
class TestUnifiedResolvers:
    def test_console_queue_empty_rejected_everywhere(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "")
        monkeypatch.delenv("PLAITA_QUEUE_NAME", raising=False)
        monkeypatch.delenv("QUEUE_NAME", raising=False)
        from plaita.server.capacity import dispatch_queue_from_env

        with pytest.raises(rc.ConfigError):
            rc.resolve_queue_name("console")
        with pytest.raises(rc.ConfigError):
            dispatch_queue_from_env()
        with pytest.raises(rc.ConfigError):
            rc.resolve_dispatch_queue_name("schedule")

    def test_worker_queue_empty_rejected(self, monkeypatch):
        monkeypatch.setenv("PLAITA_QUEUE_NAME", "")
        with pytest.raises(rc.ConfigError):
            rc.resolve_queue_name("worker")

    def test_redis_alias_all_entrypoints(self, monkeypatch):
        monkeypatch.delenv("PLAITA_REDIS_URL", raising=False)
        monkeypatch.delenv("PLAITA_CONSOLE_REDIS_URL", raising=False)
        monkeypatch.setenv("REDIS_URL", "redis://real-host:6379/9")
        from plaita.server.event_filter import _event_filter_redis_url

        assert _event_filter_redis_url() == "redis://real-host:6379/9"
        for role in ("worker", "services", "event_filter", "schedule", "console"):
            assert rc.resolve_redis_url(role) == "redis://real-host:6379/9"

    def test_redis_empty_is_identity_error(self, monkeypatch):
        monkeypatch.setenv("PLAITA_REDIS_URL", "")
        with pytest.raises(rc.ConfigError):
            rc.resolve_redis_url("worker")

    def test_dlq_key_from_env_and_empty_rejected(self, monkeypatch):
        monkeypatch.setenv("PLAITA_DLQ_KEY", "custom:dead")
        assert rc.resolve_dlq_key("q") == "custom:dead"
        monkeypatch.setenv("PLAITA_DLQ_KEY", "")
        with pytest.raises(rc.ConfigError):
            rc.resolve_dlq_key("q")

    def test_instance_id_alignment(self, monkeypatch):
        monkeypatch.setenv("PLAITA_INSTANCE_ID", "worker-r31")
        fake = fakeredis.FakeRedis(decode_responses=True)
        from plaita.server.flow_worker import RedisFlowWorker

        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name=STREAM,
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            redis_client=fake,
            enable_registry=True,
            enable_redis_logging=False,
            lease_ttl_seconds=60,
        )
        assert worker.instance_id == "worker-r31"
        assert worker._service_info.instance_id == "worker-r31"


# ---------------------------------------------------------------------------
# B2R31-03 — invalid-value policy
# ---------------------------------------------------------------------------
class TestInvalidValuePolicy:
    def test_strict_bool_accepts_on(self):
        assert rc.parse_bool("on", "X") is True
        assert rc.parse_bool(" OFF ", "X") is False
        with pytest.raises(rc.ConfigError):
            rc.parse_bool("bogus", "X")

    def test_http_block_private_on_and_unknown_fail_closed(self, monkeypatch):
        import plaita.node.http as http_mod

        monkeypatch.setenv(http_mod.HTTP_BLOCK_PRIVATE_ENV, "on")
        assert http_mod.operator_block_private() is True
        monkeypatch.setenv(http_mod.HTTP_BLOCK_PRIVATE_ENV, "bogus")
        assert http_mod.operator_block_private() is True, "安全门未知值必须 fail-closed"

    def test_optional_values_warn_not_silent(self, monkeypatch, caplog):
        import plaita.node.http as http_mod
        from plaita.server.tenant_quota import tenant_max_running
        from plaita.server.task_queue import _dlq_max_len_from_env

        monkeypatch.setenv(http_mod.HTTP_MAX_RESPONSE_BYTES_ENV, "abc")
        with caplog.at_level("WARNING"):
            assert http_mod.http_max_response_bytes() == http_mod.DEFAULT_MAX_RESPONSE_BYTES
        assert "PLAITA_HTTP_MAX_RESPONSE_BYTES" in caplog.text

        monkeypatch.setenv("PLAITA_TENANT_MAX_RUNNING", "abc")
        fake = fakeredis.FakeRedis(decode_responses=True)
        assert tenant_max_running(fake, "acme") is None

        monkeypatch.setenv("PLAITA_DLQ_MAX_LEN", "0")
        assert _dlq_max_len_from_env() == 1

    def test_langfuse_unknown_is_false(self, monkeypatch):
        from services.obs_link import langfuse_enabled

        monkeypatch.setenv("PLAITA_CONSOLE_LANGFUSE", "bogus")
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-x")
        assert langfuse_enabled() is False

    def test_preflight_fail_fast_and_aggregate(self):
        from plaita.runtime_config import preflight_security_and_limits

        with pytest.raises(rc.ConfigError) as exc:
            preflight_security_and_limits(
                {
                    "PLAITA_SANDBOX_MEMORY_MB": "-1",
                    "PLAITA_TENANT_MAX_RUNNING": "abc",
                    "PLAITA_EVENT_RECONCILE_INTERVAL": "0",
                }
            )
        text = str(exc.value)
        assert "PLAITA_SANDBOX_MEMORY_MB" in text
        assert "PLAITA_TENANT_MAX_RUNNING" in text
        assert "PLAITA_EVENT_RECONCILE_INTERVAL" in text

    def test_preflight_accepts_on_and_rejects_unknown_enum(self):
        from plaita.runtime_config import preflight_security_and_limits

        preflight_security_and_limits({"PLAITA_HTTP_BLOCK_PRIVATE": "on"})
        with pytest.raises(rc.ConfigError):
            preflight_security_and_limits({"PLAITA_CONSOLE_LANGFUSE": "bogus"})

    def test_sandbox_negative_memory_fails_at_import(self):
        env = {**os.environ, "PLAITA_SANDBOX_MEMORY_MB": "-1"}
        env["PYTHONPATH"] = str(REPO_ROOT)
        proc = subprocess.run(
            [sys.executable, "-c", "import plaita.node.code"],
            cwd=str(REPO_ROOT), env=env, capture_output=True, text=True,
        )
        assert proc.returncode != 0
        assert "PLAITA_SANDBOX_MEMORY_MB" in proc.stderr

    def test_sandbox_docker_cpus_invalid_fails_at_import(self):
        env = {**os.environ, "PLAITA_SANDBOX_DOCKER_CPUS": "abc"}
        env["PYTHONPATH"] = str(REPO_ROOT)
        proc = subprocess.run(
            [sys.executable, "-c", "import plaita.node.code"],
            cwd=str(REPO_ROOT), env=env, capture_output=True, text=True,
        )
        assert proc.returncode != 0
        assert "PLAITA_SANDBOX_DOCKER_CPUS" in proc.stderr

    def test_empty_identity_env_fail_fast(self):
        from plaita.runtime_config import preflight_security_and_limits

        for name in ("PLAITA_INSTANCE_ID", "PLAITA_DELAY_QUEUE", "PLAITA_DLQ_KEY"):
            with pytest.raises(rc.ConfigError):
                preflight_security_and_limits({name: ""})

    def test_start_carrier_payload_shape_unchanged(self):
        """A69-04 回归：新增的是**信封**字段，payload 形状不变。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        body = {"type": "start", "flow_id": "f", "execution_id": "e1"}
        enqueue_task(redis, STREAM, dict(body))
        raw = redis.xrange(STREAM)[0][1]
        import json
        assert json.loads(raw["payload"]).keys() == body.keys() | {"timestamp"}
        assert "config_family" not in raw, "未接入代际的生产方不写增量字段"


def _iso_now():
    return datetime.now().isoformat()
