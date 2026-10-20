"""#42 期 1：容量路由接**显式派发** + 非目标 worker 交接（不新增协议）。

## 红先证（RED）

基线 `11c10162`（v1 竖切）只有「算」没有「发」：`capacity` 里没有
``target_mismatch`` / ``decide_route``，worker 消费入口也不看消息体的
``target_instance_id``。故：

- ``from plaita.server.capacity import target_mismatch`` → **ImportError**；
- 更关键的**语义红**：低盘 + 高盘两台 worker 并存时，派发侧把消息投向共享
  队列（无目标字段），**任何** worker（含被容量门判为不可派的低盘机）都能
  抢到并执行 —— 见 ``test_red_baseline_no_target_means_low_disk_worker_claims``
  （本用例断言的是**修复后**的事实：低盘机拿到带目标的副本会让出）。

## 判据（期 1 §5）

1. 指定 target 时，非目标 worker 交接率 **100%**（同一个
   ``TaskNotForThisWorker`` 计数口径）；
2. 交接后的新副本**原样保留** ``target_instance_id``（否则副本会落到任意
   worker 手里，路由退化成广播）；
3. 红线：字段缺失/空白/非字符串/本机 id 不可知 ⇒ **一律放行**（存量广播消息
   零行为变化；不因注册表缺失误拦本机该跑的任务）。
"""
from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("redis")
pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402

from plaita.server import capacity as cap  # noqa: E402
from plaita.server.flow_worker import RedisFlowWorker, TaskNotForThisWorker  # noqa: E402
from plaita.server.registry import ServiceInfo  # noqa: E402

CANARY_QUEUE = "plaita:flow:queue:canary"


def _carrier(**extra):
    """合成 start 载体：A69-04 年龄闸默认开启，直接 dispatch 需带合法时间。"""
    body = {
        "type": "start",
        "flow_id": "f",
        "execution_id": "e1",
        "timestamp": datetime.now().isoformat(),
    }
    body.update(extra)
    return body


def _block(free_disk_gib, *, disk_blocked=False, active_tasks=0, concurrency=1):
    from datetime import datetime, timezone

    return {
        "schema_version": 1,
        "free_disk_gib": free_disk_gib,
        "min_free_disk_gib": 20.0,
        "disk_blocked": disk_blocked,
        "active_tasks": active_tasks,
        "concurrency": concurrency,
        "sampled_at": datetime.now(timezone.utc).isoformat(),
    }


def _candidate(instance_id, *, block=None, status="running"):
    from plaita.server.capacity import CAPACITY_METADATA_KEY

    metadata = {} if block is None else {CAPACITY_METADATA_KEY: block}
    return cap.DispatchCandidate.from_service_info(ServiceInfo(
        instance_id=instance_id,
        service_type="flow_worker",
        host=f"{instance_id}.host",
        status=status,
        metadata=metadata,
    ))


def _skeleton_worker(instance_id=None):
    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w.redis_client = fakeredis.FakeRedis(decode_responses=True)
    w.queue_name = CANARY_QUEUE
    if instance_id is not None:
        w._service_info = ServiceInfo(
            instance_id=instance_id, service_type="flow_worker", host="h",
        )
    return w


class TestTargetMismatchPureFunction:
    """``target_mismatch``：显式派发目标闸的**唯一**判据实现。"""

    def test_target_other_worker_is_mismatch(self):
        reason = cap.target_mismatch({"target_instance_id": "w-high"}, "w-low")
        assert reason is not None
        assert "w-high" in reason and "w-low" in reason

    def test_same_worker_is_affine(self):
        assert cap.target_mismatch({"target_instance_id": "w-high"}, "w-high") is None

    def test_whitespace_is_normalized(self):
        assert cap.target_mismatch({"target_instance_id": "  w-high "}, "w-high") is None
        assert cap.target_mismatch({"target_instance_id": "w-high"}, " w-high ") is None

    @pytest.mark.parametrize("raw", [None, "", "   ", 0, 12, [], {}, True])
    def test_missing_or_malformed_target_is_allowed(self, raw):
        """红线：非字符串/空白/缺失 ⇒ 放行（存量广播消息零行为变化）。"""
        assert cap.target_mismatch({"target_instance_id": raw}, "w-low") is None

    def test_no_field_is_allowed(self):
        assert cap.target_mismatch({"type": "start"}, "w-low") is None

    def test_capacity_missing_field_entirely_is_allowed(self):
        assert cap.target_mismatch({"target_instance_id": "w-high"}, None) is None
        assert cap.target_mismatch({"target_instance_id": "w-high"}, "") is None

    def test_non_dict_message_is_allowed(self):
        assert cap.target_mismatch(None, "w-low") is None
        assert cap.target_mismatch("target_instance_id=w-high", "w-low") is None

    def test_handover_rate_is_100_percent_for_non_targets(self):
        """判据①：池里除目标外**每一台**都判不亲和（交接率 100%）。"""
        workers = [f"w-{i}" for i in range(12)]
        msg = {"target_instance_id": "w-7"}
        mismatched = [w for w in workers if cap.target_mismatch(msg, w) is not None]
        assert mismatched == [w for w in workers if w != "w-7"]
        assert len(mismatched) == len(workers) - 1


class TestWorkerDispatchGate:
    """worker 消费入口：非目标 ⇒ 抛**既有** ``TaskNotForThisWorker``。"""

    def test_non_target_worker_raises_handover_exception(self):
        w = _skeleton_worker("w-low")
        with pytest.raises(TaskNotForThisWorker) as ei:
            w._dispatch_task(_carrier(target_instance_id="w-high"))
        assert "w-high" in str(ei.value)

    def test_target_worker_proceeds_past_the_gate(self):
        """目标机不受闸约束（后续失败与本闸无关——只要不是交接异常）。"""
        w = _skeleton_worker("w-high")
        with pytest.raises(Exception) as ei:
            w._dispatch_task(_carrier(target_instance_id="w-high"))
        assert not isinstance(ei.value, TaskNotForThisWorker)

    def test_untargeted_message_is_zero_behavior_change(self):
        """无目标字段 = 存量广播消息 ⇒ 行为与期 0 完全一致（放行）。"""
        w = _skeleton_worker("w-low")
        with pytest.raises(Exception) as ei:
            w._dispatch_task(_carrier())
        assert not isinstance(ei.value, TaskNotForThisWorker)

    def test_unknown_instance_id_is_allowed(self):
        """红线：本机 instance_id 不可知（注册表未启用/骨架 worker）⇒ 放行。"""
        w = _skeleton_worker(None)
        assert w.instance_id is None
        with pytest.raises(Exception) as ei:
            w._dispatch_task(_carrier(target_instance_id="w-high"))
        assert not isinstance(ei.value, TaskNotForThisWorker)

    def test_rollback_switch_disables_gate(self, monkeypatch):
        """负控：``PLAITA_DISABLE_DISPATCH_TARGET=1`` ⇒ 摘掉路由，复现原行为。"""
        from plaita.server import flow_worker as fw

        monkeypatch.setenv("PLAITA_DISABLE_DISPATCH_TARGET", "1")
        assert fw._dispatch_target_disabled() is True
        w = _skeleton_worker("w-low")
        with pytest.raises(Exception) as ei:
            w._dispatch_task(_carrier(target_instance_id="w-high"))
        assert not isinstance(ei.value, TaskNotForThisWorker)

    def test_affinity_switch_does_not_disable_target_gate(self, monkeypatch):
        """两个回滚开关**互相独立**：回滚亲和闸不得顺带关掉显式目标路由。"""
        monkeypatch.setenv("PLAITA_DISABLE_AFFINITY", "1")
        w = _skeleton_worker("w-low")
        with pytest.raises(TaskNotForThisWorker):
            w._dispatch_task(_carrier(target_instance_id="w-high"))


class TestHandoverPreservesTarget:
    """判据②：目标仍可用时，交接副本必须**原样保留** ``target_instance_id``。"""

    def test_handover_copy_keeps_target_and_target_worker_accepts(self):
        from plaita.server.task_queue import RedisStreamTaskQueue, enqueue_task

        w = _skeleton_worker("w-low")
        # A46-02：交接前会复核目标注册键；目标必须仍 running，否则按广播重投。
        w.redis_client.setex(
            "plaita:registry:flow_worker:w-high",
            30,
            ServiceInfo(
                instance_id="w-high", service_type="flow_worker", host="h",
                status="running",
            ).to_json(),
        )
        body = {
            "type": "start", "flow_id": "f", "execution_id": "exec-route",
            "target_instance_id": "w-high",
            "params": {},
        }
        q = RedisStreamTaskQueue(w.redis_client, CANARY_QUEUE, consumer_name="c1")
        q.ensure_group()
        enqueue_task(w.redis_client, CANARY_QUEUE, dict(body))

        task = q.read(block_ms=1)
        assert task is not None
        with pytest.raises(TaskNotForThisWorker):
            w._dispatch_task(dict(task.body))

        w._handover_non_affine(task, q, TaskNotForThisWorker("别的机器"))

        entries = w.redis_client.xrange(CANARY_QUEUE)
        assert len(entries) == 1, "交接应再入队恰好一份新副本"
        import json

        copy = json.loads(entries[0][1]["payload"])
        assert copy["target_instance_id"] == "w-high", (
            "交接副本丢了目标字段 ⇒ 路由退化成广播（副本会被任意 worker 抢走）"
        )
        # 目标机看到的是同一份副本 ⇒ 直接放行
        assert cap.target_mismatch(copy, "w-high") is None


class TestHandoverStripsUnavailableTarget:
    """A46-02：目标不可用（键消失 / 非 running）→ 剥 target 按广播重投。"""

    def _run_handover(self, w, *, body):
        from plaita.server.task_queue import RedisStreamTaskQueue, enqueue_task

        q = RedisStreamTaskQueue(w.redis_client, CANARY_QUEUE, consumer_name="c1")
        q.ensure_group()
        enqueue_task(w.redis_client, CANARY_QUEUE, dict(body))
        task = q.read(block_ms=1)
        assert task is not None
        w._handover_non_affine(task, q, TaskNotForThisWorker("别的机器"))
        entries = w.redis_client.xrange(CANARY_QUEUE)
        assert len(entries) == 1, "交接应恰好再入队一份新副本"
        import json

        return json.loads(entries[0][1]["payload"])

    def _body(self, target="w-gone"):
        return {
            "type": "start", "flow_id": "f", "execution_id": "exec-ghost",
            "target_instance_id": target, "params": {},
        }

    def test_missing_target_is_stripped_and_broadcast(self):
        w = _skeleton_worker("w-low")
        copy = self._run_handover(w, body=self._body("w-gone"))
        assert "target_instance_id" not in copy, (
            "目标键不存在时仍保留 target ⇒ 每台非目标机交接一次，"
            "4 次后 AffinityHandoverExhausted（A46-02 探针）"
        )
        assert copy["execution_id"] == "exec-ghost"

    @pytest.mark.parametrize("status", ["draining", "stopping", "stopped"])
    def test_non_running_target_is_stripped_and_broadcast(self, status):
        w = _skeleton_worker("w-low")
        w.redis_client.setex(
            "plaita:registry:flow_worker:w-gone",
            30,
            ServiceInfo(
                instance_id="w-gone", service_type="flow_worker", host="h",
                status=status,
            ).to_json(),
        )
        copy = self._run_handover(w, body=self._body("w-gone"))
        assert "target_instance_id" not in copy

    def test_registry_read_failure_keeps_target_fail_open(self, monkeypatch):
        w = _skeleton_worker("w-low")

        def boom(*args, **kwargs):
            raise RuntimeError("redis down")

        monkeypatch.setattr(w.redis_client, "get", boom)
        copy = self._run_handover(w, body=self._body("w-high"))
        assert copy["target_instance_id"] == "w-high", (
            "一次 Redis 瞬断不得把定向任务广播给不亲和的机器（fail-open）"
        )

    def test_stripped_copy_is_claimable_by_any_worker(self):
        """A46-02 终态判据：广播副本不得在第三台机器上再次触发交接。

        探针 `r46_dispatch_target_ghost.py` 的交接风暴是「每台非目标机各交接
        一次 → 4 次后 AffinityHandoverExhausted」；剥 target 后副本对任意
        worker 都是广播消息，风暴在一次交接内终止。
        """
        w = _skeleton_worker("w-low")
        copy = self._run_handover(w, body=self._body("w-gone"))
        for instance in ("w-low", "w-third"):
            assert cap.target_mismatch(copy, instance) is None

    def test_target_is_preserved_when_running(self):
        w = _skeleton_worker("w-low")
        w.redis_client.setex(
            "plaita:registry:flow_worker:w-high",
            30,
            ServiceInfo(
                instance_id="w-high", service_type="flow_worker", host="h",
                status="running",
            ).to_json(),
        )
        copy = self._run_handover(w, body=self._body("w-high"))
        assert copy["target_instance_id"] == "w-high"


class TestDecideRoute:
    """容量路由（唯一实现）：低盘 worker 不得成为目标。"""

    def test_low_disk_worker_is_not_selected(self):
        route = cap.decide_route([
            _candidate("w-low", block=_block(3.0, disk_blocked=True)),
            _candidate("w-high", block=_block(120.0)),
        ])
        assert route.target_instance_id == "w-high"
        assert route.reason == cap.REASON_ROUTE_CAPACITY
        assert [r.instance_id for r in route.rejected] == ["w-low"]
        assert route.rejected[0].reason.startswith("disk")

    def test_no_candidate_means_no_target_but_still_dispatchable(self):
        """红线：池空 ⇒ 无目标 = 放行（广播抢单），不是「本轮不派」。"""
        route = cap.decide_route([])
        assert route.target_instance_id is None
        assert route.reason == cap.REASON_ROUTE_NO_TARGET

    def test_all_rejected_means_no_target(self):
        route = cap.decide_route([_candidate("w-low", block=_block(1.0, disk_blocked=True))])
        assert route.target_instance_id is None
        assert route.reason == cap.REASON_ROUTE_NO_TARGET
        assert len(route.rejected) == 1

    def test_missing_capacity_is_allowed(self):
        """红线：存量 worker（无容量上报）仍可被选中。"""
        route = cap.decide_route([_candidate("w-legacy")])
        assert route.target_instance_id == "w-legacy"

    def test_deterministic_regardless_of_input_order(self):
        a = _candidate("w-a", block=_block(100.0))
        b = _candidate("w-b", block=_block(100.0))
        c = _candidate("w-c", block=_block(100.0))
        assert cap.decide_route([a, b, c]).target_instance_id == "w-a"
        assert cap.decide_route([c, a, b]).target_instance_id == "w-a"
        assert cap.decide_route([b, c, a]).target_instance_id == "w-a"

    def test_red_baseline_no_target_means_low_disk_worker_claims(self):
        """★ 红先证的**语义**形态：没有目标字段时，低盘 worker **不会**让出。

        这正是期 0 的病：容量门算得出「别给 w-low」，但算完就丢了——消息进
        共享队列，w-low 抢到照跑。修复后同一份消息带上目标字段，w-low 走既有
        交接路径让出（上一个用例已断言）。本用例把「没有字段 ⇒ 照跑」这一
        事实钉死，防止有人把红线（缺字段放行）误改成「缺字段就拒」——
        那会让存量广播派发全部停摆。
        """
        w_low = _skeleton_worker("w-low")
        broadcast = _carrier(execution_id="e-bc")
        assert cap.target_mismatch(broadcast, "w-low") is None
        with pytest.raises(Exception) as ei:
            w_low._dispatch_task(dict(broadcast))
        assert not isinstance(ei.value, TaskNotForThisWorker)
