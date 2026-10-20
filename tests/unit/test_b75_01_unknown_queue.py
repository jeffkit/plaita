"""B75-01 / B74-02：unknown 队列三态（unknown ≠ default）。

## 规格（r75 B75-01）

- 只有「非空、非纯空白字符串」的 ``queue_name`` 是 known；缺失/空白/
  非字符串一律 unknown（``None``），**绝不**回落到默认队列；
- unknown 只拒绝**定向**，不拒绝**广播**——legacy default worker 仍可消费；
- ``PLAITA_DISPATCH_UNKNOWN_QUEUE=skip``（默认）/ ``assume-default``；
  后者仅在请求默认队列时把 unknown 当默认，并留 WARNING +
  ``queue_source=assumed-default``；
- 指标 ``dispatch_unknown_queue_total{queue}`` 与 plan/dispatch 同判据。

## 负控（pre-fix 精确红，逐组可复跑）

1. 缺失 queue_name → 仍被默认计划列为 allowed/target（本组断言 target None /
   ``queue: unknown`` / WARNING）；
2. 空白 + 非字符串 → 同上（pre-fix 全部按 default）；
3. assume-default → 无 ``queue_source`` 字段（pre-fix 无此概念）；
4. shadow plan 默认 skip → pre-fix target=unknown worker、无 queue_source；
5. plan/route 同判据 → pre-fix 两者都定向 unknown，断言 None 红。
"""
from __future__ import annotations

import json
import logging

import pytest

pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402

from plaita.server import capacity as cap  # noqa: E402
from plaita.server.capacity import DispatchCandidate, decide_route  # noqa: E402
from plaita.server.registry import ServiceInfo, ServiceRegistry  # noqa: E402

ENV = cap.UNKNOWN_QUEUE_POLICY_ENV


def _candidate(instance_id="w-unknown", metadata=None, status="running"):
    return DispatchCandidate(
        instance_id=instance_id,
        host=f"{instance_id}.host",
        status=status,
        metadata={} if metadata is None else dict(metadata),
    )


@pytest.fixture(autouse=True)
def _fresh_metrics():
    cap.reset_unknown_queue_counts()
    yield
    cap.reset_unknown_queue_counts()


class TestUnknownNeverEqualsDefault:
    """负控组 1/2：缺失、空白、纯空白、非字符串一律 unknown。"""

    @pytest.mark.parametrize(
        "metadata",
        [
            {},
            {"queue_name": ""},
            {"queue_name": "   "},
            {"queue_name": None},
            {"queue_name": 12},
            {"queue_name": []},
            {"queue_name": {}},
        ],
    )
    def test_unknown_metadata_is_not_targeted(self, metadata, caplog):
        cand = _candidate(metadata=metadata)
        assert cand.queue_name is None
        assert cand.queue_source == "unknown"

        with caplog.at_level(logging.WARNING, logger="plaita.server.capacity"):
            route = decide_route([cand], queue_name=cap.DEFAULT_DISPATCH_QUEUE)

        assert route.target is None
        assert route.allowed == []
        assert route.rejected[0].instance_id == cand.instance_id
        assert route.rejected[0].reason.startswith(cap.REASON_QUEUE_UNKNOWN)
        assert "worker metadata lacks queue_name" in route.rejected[0].reason
        assert cap.unknown_queue_counts() == {cap.DEFAULT_DISPATCH_QUEUE: 1}
        assert cand.instance_id in caplog.text
        assert cap.REASON_QUEUE_UNKNOWN in caplog.text

    def test_known_cross_queue_mismatch_keeps_r54_reason(self):
        """known 不匹配仍是 ``queue: mismatch``，不得混入 unknown 语义。"""
        cand = _candidate(metadata={"queue_name": "q:v2"})
        route = decide_route([cand], queue_name="q:v1")
        assert route.target is None
        assert route.rejected[0].reason.startswith(cap.REASON_QUEUE_MISMATCH)
        assert cap.REASON_QUEUE_UNKNOWN not in route.rejected[0].reason
        assert cap.unknown_queue_counts() == {}

    def test_unknown_candidate_cannot_shadow_known_worker(self):
        """unknown 不进 allowed：同队列的 known worker 才是目标。"""
        unknown = _candidate("w-unknown")
        known = _candidate("w-known", metadata={"queue_name": cap.DEFAULT_DISPATCH_QUEUE})
        # 让 unknown 的排序键更优也不得成为目标（它没有容量块，排序不分胜负；
        # 这里显式给 known 一个容量块以外的事实：判据只看队列声明）。
        route = decide_route([known, unknown], queue_name=cap.DEFAULT_DISPATCH_QUEUE)
        assert route.target is known
        assert route.allowed == [known]
        assert [r.instance_id for r in route.rejected] == ["w-unknown"]
        assert route.queue_source == cap.QUEUE_SOURCE_DECLARED


    def test_unknown_metric_is_exported_per_queue(self):
        cap.record_unknown_queue(cap.DEFAULT_DISPATCH_QUEUE)
        cap.record_unknown_queue(cap.DEFAULT_DISPATCH_QUEUE)
        cap.record_unknown_queue("q:canary")
        metrics = {m.labels.get("queue"): m for m in cap.collect_dispatch_metrics()}
        assert metrics[cap.DEFAULT_DISPATCH_QUEUE].name == "dispatch_unknown_queue_total"
        assert metrics[cap.DEFAULT_DISPATCH_QUEUE].value == 2
        assert metrics["q:canary"].value == 1
        assert metrics[cap.DEFAULT_DISPATCH_QUEUE].type == "counter"


class TestAssumeDefaultCompatibility:
    """负控组 3：显式兼容开关必须可审计（queue_source + WARNING）。"""

    def test_assume_default_targets_unknown_and_marks_source(
        self, monkeypatch, caplog,
    ):
        monkeypatch.setenv(ENV, cap.UNKNOWN_QUEUE_ASSUME_DEFAULT)
        cand = _candidate()
        with caplog.at_level(logging.WARNING, logger="plaita.server.capacity"):
            route = decide_route([cand], queue_name=cap.DEFAULT_DISPATCH_QUEUE)

        assert route.target is cand
        assert route.target_queue_name == cap.DEFAULT_DISPATCH_QUEUE
        assert route.queue_source == cap.QUEUE_SOURCE_ASSUMED_DEFAULT
        assert "assumed-default" in caplog.text
        assert cand.instance_id in caplog.text
        assert cap.unknown_queue_counts() == {cap.DEFAULT_DISPATCH_QUEUE: 1}

    def test_assume_default_does_not_leak_to_non_default_queue(self, monkeypatch):
        monkeypatch.setenv(ENV, cap.UNKNOWN_QUEUE_ASSUME_DEFAULT)
        route = decide_route([_candidate()], queue_name="q:canary")
        assert route.target is None
        assert route.rejected[0].reason.startswith(cap.REASON_QUEUE_UNKNOWN)

    def test_invalid_policy_falls_back_to_skip_with_warning(
        self, monkeypatch, caplog,
    ):
        monkeypatch.setenv(ENV, "assume_default!")  # 非法：不是 assume-default
        with caplog.at_level(logging.WARNING, logger="plaita.server.capacity"):
            route = decide_route(
                [_candidate()], queue_name=cap.DEFAULT_DISPATCH_QUEUE,
            )
        assert route.target is None
        assert route.rejected[0].reason.startswith(cap.REASON_QUEUE_UNKNOWN)
        assert "非法" in caplog.text


class TestPlanSharesCriteria:
    """负控组 4/5：shadow plan 与 route 同判据（不能 plan 拒、dispatch 放）。"""

    def _registry(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        reg = ServiceRegistry(redis)
        reg.register(ServiceInfo(
            instance_id="w-unknown",
            service_type="flow_worker",
            host="w-unknown.host",
            metadata={},
        ))
        return redis

    def test_shadow_plan_does_not_assume_default_by_default(self):
        redis = self._registry()
        plan = cap.shadow_dispatch_plan(redis, ["i-1"])
        assert plan["target"] is None
        assert plan["allowed"] == []
        assert plan["queue"] == cap.DEFAULT_DISPATCH_QUEUE
        assert plan["queue_source"] == cap.QUEUE_SOURCE_DECLARED
        assert plan["rejected"][0]["reason"].startswith(cap.REASON_QUEUE_UNKNOWN)
        assert cap.unknown_queue_counts() == {cap.DEFAULT_DISPATCH_QUEUE: 1}

    def test_shadow_plan_and_route_agree_with_assume_default(self, monkeypatch):
        monkeypatch.setenv(ENV, cap.UNKNOWN_QUEUE_ASSUME_DEFAULT)
        redis = self._registry()
        plan = cap.shadow_dispatch_plan(redis, ["i-1"])
        route = decide_route(
            cap.load_worker_capacity(redis), queue_name=cap.DEFAULT_DISPATCH_QUEUE,
        )
        assert plan["target"] == route.target_instance_id == "w-unknown"
        assert plan["queue_source"] == route.queue_source == cap.QUEUE_SOURCE_ASSUMED_DEFAULT
        assert plan["target"] == "w-unknown"


class TestRegistrationContract:
    """B75-01 §6：新 worker 注册必须声明队列契约版本（unknown 可区分）。"""

    def test_worker_metadata_declares_queue_contract_version(self):
        from plaita.event.memory import InMemoryEventBus
        from plaita.server.flow_worker import RedisFlowWorker
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        redis = fakeredis.FakeRedis(decode_responses=True)
        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/9",
            queue_name="plaita:flow:queue:b75",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            event_bus=InMemoryEventBus(),
            redis_client=redis,
            enable_registry=True,
            enable_redis_logging=False,
            min_free_disk_gib=20.0,
            disk_guard_path="/disk",
        )
        raw = redis.get(f"plaita:registry:flow_worker:{worker._service_info.instance_id}")
        data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        assert data["metadata"]["queue_name"] == "plaita:flow:queue:b75"
        assert data["metadata"]["queue_contract_version"] == 2
