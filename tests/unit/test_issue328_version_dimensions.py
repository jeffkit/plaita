"""#328 F1 版本维度收敛（r101 B3）：信封 = schema_version + tenant_generation + max_deliveries 快照。

规格（jeffkit #328 正文四条 + #329-F1）：

* 只保留两个版本维度：wire schema + IR；config_family/config_generation/
  budget_generation/config_effective_at 四个哈希维度全部离开信封；
* ``config_family`` 收敛为 worker **启动校验**（queue/group/dlq 与部署 env
  不一致拒绝启动；namespace 空 env 拒绝）——不再有逐消息 family 闸；
* ``budget_generation`` 换成消息上的预算快照（``max_deliveries`` 一个整数随
  信封走；缺失 = legacy，按消费端本地预算）；
* ``tenant_generation`` 保留为身份字段，不参与版本预算；
* 终态：除 schema-too-new→DLQ 外，不再有永久 pending 路径。

负控（在 B2R31-01 旧实现上必须精确红）：

* 信封形状：旧实现写 config_* 四字段、不写 ``max_deliveries``；
* family 戳不匹配：旧实现 read() 返回 None（永久留 PEL）；
* budget 代际戳不匹配：旧实现 reclaim 跳过（永久留 PEL）；
* 预算快照：旧实现没有该字段，超快照预算的条目被继续执行而非按源预算死信；
* requeue/DLQ 继承面：旧实现继承 config_*、不带快照。
"""
from __future__ import annotations

import argparse
import inspect
import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.runtime_config import (
    resolve_consumer_group,
    resolve_dlq_key,
    resolve_queue_name,
)
from plaita.server import dlq_replay
from plaita.server import metrics as metrics_mod
from plaita.server.flow_worker import _cli_startup_problems, _requeue_envelope_kwargs
from plaita.server.task_queue import (
    PAYLOAD_FIELD,
    SCHEMA_FIELD,
    RedisStreamTaskQueue,
    StreamTask,
    enqueue_task,
)

STREAM = "test:i328-versions"


def _body(execution_id: str = "e1") -> dict:
    return {"type": "start", "flow_id": "f", "execution_id": execution_id}


# 旧实现（B2R31-01）的队列构造参数：红相用它构造「消费端代际 ≠ 消息戳」的
# 场景（两端都显式且不同才触发旧闸；缺省=legacy 会被旧闸放行，负控会假绿）。
# 按构造器签名探测：旧世界原样传入（精确红），新世界构造器已无这些参数则
# 丢弃（用例体只断言行为：消息被服务/死信、不留 pending）。
_LEGACY_QUEUE_KWARGS = frozenset({
    "config_family", "config_generation", "budget_generation", "config_effective_at",
})


def _queue(redis, **kwargs):
    kwargs.setdefault("group_name", "g")
    kwargs.setdefault("consumer_name", "c1")
    kwargs.setdefault("claim_min_idle_ms", 0)
    accepted = inspect.signature(RedisStreamTaskQueue.__init__).parameters
    for legacy in _LEGACY_QUEUE_KWARGS:
        if legacy not in accepted:
            kwargs.pop(legacy, None)
    return RedisStreamTaskQueue(redis, STREAM, **kwargs)


def _fields(redis, msg_id: str) -> dict:
    entries = redis.xrange(STREAM, min=msg_id, max=msg_id)
    return entries[0][1]


# ---------------------------------------------------------------------------
# 信封形状（验收②：信封只剩 schema_version + tenant_generation + 预算快照）
# ---------------------------------------------------------------------------
class TestEnvelopeShape:
    def test_envelope_is_schema_tenant_budget_only(self, monkeypatch):
        monkeypatch.setenv("PLAITA_MAX_DELIVERIES", "3")
        redis = fakeredis.FakeRedis(decode_responses=True)
        msg = enqueue_task(redis, STREAM, _body(), tenant_generation="t1")
        fields = _fields(redis, msg)
        assert set(fields) == {
            PAYLOAD_FIELD, SCHEMA_FIELD, "max_deliveries", "tenant_generation",
        }
        assert fields["schema_version"] == "1"
        assert fields["max_deliveries"] == "3"
        assert fields["tenant_generation"] == "t1"

    def test_explicit_none_writes_legacy_envelope(self):
        """重投路径显式 ``max_deliveries=None`` = 源是 legacy 载体，不得盖当前预算。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        msg = enqueue_task(
            redis, STREAM, _body(),
            schema_version=1, max_deliveries=None, tenant_generation=None,
        )
        assert set(_fields(redis, msg)) == {PAYLOAD_FIELD, SCHEMA_FIELD}

    def test_explicit_snapshot_travels(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        msg = enqueue_task(redis, STREAM, _body(), max_deliveries=2)
        assert _fields(redis, msg)["max_deliveries"] == "2"


# ---------------------------------------------------------------------------
# family 截不再逐消息拦截（验收①：队列改名滚动升级无消息卡 pending）
# ---------------------------------------------------------------------------
class TestFamilyStampNoLongerStrands:
    def test_foreign_family_stamp_is_served(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        redis.xadd(STREAM, {
            PAYLOAD_FIELD: json.dumps(_body()),
            SCHEMA_FIELD: "1",
            "config_family": "cf1-bbbbbbbbbbbb",
        })
        queue = _queue(redis, config_family="cf1-aaaaaaaaaaaa")
        task = queue.read(block_ms=1)
        assert task is not None, "family 戳不匹配的消息必须被消费，不得留 pending"
        assert task.body["execution_id"] == "e1"

    def test_foreign_family_stamp_reclaim_is_served(self):
        """reclaim 路径同判：外族戳不得跳过 XCLAIM（B2R31-01 旧行为=永久 pending）。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        msg = redis.xadd(STREAM, {
            PAYLOAD_FIELD: json.dumps(_body()),
            SCHEMA_FIELD: "1",
            "config_family": "cf1-bbbbbbbbbbbb",
        })
        seed = _queue(redis, consumer_name="c1", config_family="cf1-bbbbbbbbbbbb")
        seed.ensure_group()
        assert seed.read(block_ms=1) is not None
        reader = _queue(redis, consumer_name="c2", config_family="cf1-aaaaaaaaaaaa")
        task = reader.read(block_ms=1)
        assert task is not None

    def test_old_budget_generation_stamp_is_served(self):
        """budget 代际戳不匹配不得留 pending（#328 永久 pending 路径之二）。

        旧实现只在 reclaim（PEL）路径判 budget 代际——按 reclaim 形态构造。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        msg = redis.xadd(STREAM, {
            PAYLOAD_FIELD: json.dumps(_body()),
            SCHEMA_FIELD: "1",
            "budget_generation": "bg1-111111111111",
        })
        seed = _queue(redis, consumer_name="c1", budget_generation="bg1-111111111111")
        seed.ensure_group()
        assert seed.read(block_ms=1) is not None
        redis.xclaim(STREAM, "g", "c1", min_idle_time=0, message_ids=[msg])
        reader = _queue(redis, consumer_name="c2", budget_generation="bg1-222222222222")
        task = reader.read(block_ms=1)
        assert task is not None, "budget 代际不匹配的 PEL 条目必须被消费/死信，不得跳过"


# ---------------------------------------------------------------------------
# 预算快照（验收③：budget_generation → max_deliveries 整数随信封走）
# ---------------------------------------------------------------------------
class TestBudgetSnapshot:
    def _seed_delivered(self, redis, fields: dict, claims: int) -> str:
        msg = redis.xadd(STREAM, fields)
        seed = _queue(redis, consumer_name="c1")
        seed.ensure_group()
        assert seed.read(block_ms=1) is not None  # 首投（times_delivered=1）
        for _ in range(claims):
            redis.xclaim(STREAM, "g", "c1", min_idle_time=0, message_ids=[msg])
        return msg

    def test_snapshot_budget_dead_letters_at_source_budget(self):
        """消息自带快照 2、消费端本地 5：达 2 次投递即按源预算死信，不再执行。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        self._seed_delivered(
            redis,
            {
                PAYLOAD_FIELD: json.dumps(_body()),
                SCHEMA_FIELD: "1",
                "max_deliveries": "2",
            },
            claims=1,
        )
        reader = _queue(redis, consumer_name="c2", max_deliveries=5)
        task = reader.read(block_ms=1)
        assert task is None, "快照预算已耗尽必须死信，不得继续执行"
        assert redis.xlen(f"{STREAM}:dlq") == 1
        envelope = json.loads(redis.xrange(f"{STREAM}:dlq")[0][1][PAYLOAD_FIELD])
        assert envelope["reason"] == "max_deliveries=2"
        assert redis.xpending(STREAM, "g")["pending"] == 0

    def test_snapshot_under_budget_is_served(self):
        """快照预算未耗尽：消息照常执行（快照只收紧判据，不放大）。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        self._seed_delivered(
            redis,
            {
                PAYLOAD_FIELD: json.dumps(_body()),
                SCHEMA_FIELD: "1",
                "max_deliveries": "5",
            },
            claims=1,
        )
        reader = _queue(redis, consumer_name="c2", max_deliveries=5)
        assert reader.read(block_ms=1) is not None

    def test_legacy_message_without_snapshot_uses_local_budget(self):
        """旧格式在途消息（无快照字段）滚动升级：按消费端本地预算死信。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        self._seed_delivered(
            redis,
            {PAYLOAD_FIELD: json.dumps(_body()), SCHEMA_FIELD: "1"},
            claims=2,
        )
        reader = _queue(redis, consumer_name="c2", max_deliveries=3)
        assert reader.read(block_ms=1) is None
        assert redis.xlen(f"{STREAM}:dlq") == 1
        envelope = json.loads(redis.xrange(f"{STREAM}:dlq")[0][1][PAYLOAD_FIELD])
        assert envelope["reason"] == "max_deliveries=3"

    def test_malformed_snapshot_falls_back_to_local_budget(self):
        """畸形快照（非正整数）按缺失处理 = 本地预算，绝不猜着收紧。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        self._seed_delivered(
            redis,
            {
                PAYLOAD_FIELD: json.dumps(_body()),
                SCHEMA_FIELD: "1",
                "max_deliveries": "zero",
            },
            claims=1,
        )
        reader = _queue(redis, consumer_name="c2", max_deliveries=5)
        assert reader.read(block_ms=1) is not None


# ---------------------------------------------------------------------------
# 继承面：requeue kwargs / DLQ 信封 / replay（验收③+①：副本带源快照）
# ---------------------------------------------------------------------------
class TestInheritanceSurfaces:
    def test_requeue_kwargs_inherit_snapshot(self):
        task = StreamTask(
            message_id="1-0", body={}, schema_version=1,
            max_deliveries=2, tenant_generation="t9",
        )
        kwargs = _requeue_envelope_kwargs(task)
        assert kwargs == {
            "schema_version": 1, "max_deliveries": 2, "tenant_generation": "t9",
        }

    def test_requeue_kwargs_legacy_source_unchanged(self):
        kwargs = _requeue_envelope_kwargs(StreamTask(message_id="1-0", body={}))
        assert kwargs == {"schema_version": 1}

    def test_dlq_envelope_carries_snapshot_not_hashes(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        queue = _queue(redis)
        task = StreamTask(
            message_id="1-0", body={"x": 1}, delivery_count=1,
            max_deliveries=2, tenant_generation="t1",
        )
        queue.dead_letter(task, reason="test")
        envelope = json.loads(redis.xrange(f"{STREAM}:dlq")[0][1][PAYLOAD_FIELD])
        assert envelope["max_deliveries"] == 2
        for gone in (
            "config_family", "config_generation",
            "budget_generation", "config_effective_at",
        ):
            assert gone not in envelope

    def test_replay_has_no_family_parameters(self):
        params = inspect.signature(dlq_replay.replay_dead_letter).parameters
        for gone in (
            "target_config_family", "allow_family_mismatch",
            "target_consumer_group", "target_dlq_key",
        ):
            assert gone not in params

    def test_replay_inherits_snapshot(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        queue = _queue(redis)
        queue.dead_letter(
            StreamTask(
                message_id="1-0", body=_body("e-dlq"), delivery_count=3,
                schema_version=1, max_deliveries=2, tenant_generation="t1",
            ),
            reason="max_deliveries=2",
        )
        dlq_id = redis.xrange(f"{STREAM}:dlq")[0][0]
        result = dlq_replay.replay_dead_letter(
            redis,
            dlq_key=f"{STREAM}:dlq",
            dlq_id=dlq_id,
            target_stream=STREAM,
            actor="r101",
            tenant_id="acme",
        )
        assert result.replayed, result.reason
        entries = redis.xrange(STREAM)
        fields = entries[-1][1]
        assert fields["max_deliveries"] == "2"
        assert fields["tenant_generation"] == "t1"
        assert "config_family" not in fields


# ---------------------------------------------------------------------------
# 启动校验（验收①正控：连错队列/group/dlq 的 worker 启动失败；namespace 空 env 拒绝）
# ---------------------------------------------------------------------------
def _cli_args(**overrides):
    queue = resolve_queue_name("worker")
    values = {
        "queue_name": queue,
        "consumer_group": resolve_consumer_group(),
        "dlq_key": resolve_dlq_key(queue, empty_policy="default"),
        "lease_ttl_seconds": 120,
        "max_deliveries": 5,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class TestStartupValidationIsTheFamilyGate:
    def test_wrong_queue_worker_startup_fails(self):
        problems = _cli_startup_problems(_cli_args(queue_name="plaita:flow:queue:wrong"))
        assert any("--queue-name" in p for p in problems)

    def test_wrong_group_worker_startup_fails(self):
        problems = _cli_startup_problems(_cli_args(consumer_group="wrong-group"))
        assert any("--consumer-group" in p for p in problems)

    def test_wrong_dlq_worker_startup_fails(self):
        problems = _cli_startup_problems(_cli_args(dlq_key="plaita:flow:queue:wrong:dlq"))
        assert any("--dlq-key" in p for p in problems)


# ---------------------------------------------------------------------------
# 指标收敛：family/budget 代际计数器随闸门一起删除
# ---------------------------------------------------------------------------
class TestMetricsConverged:
    def test_generation_counters_removed_from_registry(self):
        names = {name for name, _op, _doc in metrics_mod.QUEUE_COUNTERS}
        for gone in ("config_family_mismatch", "config_budget_stale", "config_legacy"):
            assert gone not in names

    def test_queue_stats_dropped_generation_counters(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        stats = _queue(redis).stats()
        for gone in ("config_family_mismatch", "config_budget_stale", "config_legacy"):
            assert gone not in stats
