"""Worker 落盘/派发守卫回归（2026-10-02 分布式深度评审 任务①②③）。

基线缺陷（任务①）：``RedisExecutionStorage.save_execution_state`` 吞掉一切
异常返回 False（plaita/storage/redis.py），而 FlowWorker 的终态/挂起/取消/
步进/错误保存与 resume 错误路径、resume 入口取消路径都不检查返回值——
is_end 落盘失败后消息照常 ack，节点副作用已发生而执行状态永远停在
running/suspended（僵尸执行）。

基线缺陷（任务②）：``_dispatch_service_task`` 内部 catch 一切只打日志——
rpush 失败被吞 → 执行已 suspended、消息被 ack → delay/approval 任务永无人
接，永久挂起。

修复契约：
- 任何 ``save_execution_state`` 返回 False → 抛 ``StatePersistError``
  （RuntimeError 子类），消息不 ack 走 at-least-once 重投；
- 有 redis_client 时服务任务派发失败 → 抛 ``ServiceDispatchError``，
  suspended 状态保留（**不**翻 error——翻了会被重投消息的 already_terminal
  短路 ack，执行永久卡死），重投后下轮 resume 重新执行挂起节点再派发；
- 无 redis_client（内存 worker / 单测）派发跳过 + warning（兼容红线）；
- fenced 世代失配走 ExecutionLeaseError（storage 层 raise，不经 False 路径），
  已有链路不受影响。

任务③（与 Track B 的契约）：``_get_task_queue`` 在队列类支持
``dead_letter_guard`` 参数时传入守卫；守卫判据 = **执行状态优先**——
缺失/终态放行死信；非终态 + 租约在（活 worker 处理中）跳过；非终态 +
租约空（持有者已死）重入队一份 delivery 归 1 的新消息再放行（XCLAIM
虚增的 delivery_count 不可逆，只跳过的话超限消息永不再被派发）。租约键
``{ns}:execution:lease:{id}`` 按消息体 tenant_id 路由。
"""
import threading
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

import plaita.server.flow_worker as flow_worker_module
from plaita.core.flow import Flow
from plaita.server.flow_worker import (
    FlowWorker,
    RedisFlowWorker,
    ServiceDispatchError,
    StatePersistError,
)
from plaita.server.task_queue import StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


class FalseSaveStorage(MemoryExecutionStorage):
    """save 永远返回 False 的桩：模拟 Redis 后端吞异常后的失败契约。

    照常写入底层 dict（load 可用），只把返回值变成 False——与
    RedisExecutionStorage.save_execution_state 的失败形态逐字对齐。
    """

    def save_execution_state(self, execution_id, state) -> bool:
        super().save_execution_state(execution_id, state)
        return False


def _base_worker(storage=None) -> FlowWorker:
    return FlowWorker(
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
    )


def _redis_worker(fake_redis, storage=None, flow_storage=None) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:worker-save-guard",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _flow() -> Flow:
    return Flow.model_validate(TEST_FLOW)


def _state(execution_id="exec-1", status="running", context=None) -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="1",
        status=status,
        context=context if context is not None else {"$LAST_NODE": "start", "$NODE": {}},
    )


# ---------- 任务①：save_execution_state 返回 False 必须炸出，不许静默 ----------


class TestSaveFailureRaises:
    def test_save_false_at_is_end_raises(self):
        """is_end 落盘失败 → 抛错（修复前静默 break = 消息 ack = 僵尸执行）。"""
        worker = _base_worker(FalseSaveStorage())
        result = {"execution_id": "exec-1", "is_end": True, "context": {"answer": 42}}
        with pytest.raises(RuntimeError, match="保存执行状态失败"):
            worker._process_execution_result(_flow(), result, _state())

    def test_save_false_at_is_end_raises_state_persist_error(self):
        """抛的是 StatePersistError——供 resume_flow / 步进循环定向放行。"""
        worker = _base_worker(FalseSaveStorage())
        result = {"execution_id": "exec-1", "is_end": True, "context": {}}
        with pytest.raises(StatePersistError):
            worker._process_execution_result(_flow(), result, _state())

    def test_save_false_at_suspend_raises(self):
        """suspended 落盘失败 → 抛错（修复前静默挂起 = 消息 ack = 无人 resume）。"""
        worker = _base_worker(FalseSaveStorage())
        result = {
            "execution_id": "exec-1",
            "is_suspend": True,
            "context": {"$LAST_NODE": "a"},
        }
        with pytest.raises(StatePersistError, match="suspended"):
            worker._process_execution_result(_flow(), result, _state())

    def test_save_false_at_cancelled_boundary_raises(self):
        """步界取消落盘失败 → 抛错（修复前静默终态化 = 消息 ack = 取消丢失）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _base_worker(FalseSaveStorage())
        worker.redis_client = fake  # 注入取消标志检查的 redis 客户端
        fake.set("plaita:execution:cancel:exec-1", "ts", ex=60)
        result = {"execution_id": "exec-1", "is_end": False, "is_suspend": False, "context": {}}
        with pytest.raises(StatePersistError, match="cancelled"):
            worker._process_execution_result(_flow(), result, _state())

    def test_save_false_at_step_persist_raises(self):
        """步进 persist 落盘失败 → 抛错，消息重投从 checkpoint 重入。

        修复前：persist 失败被吞 → 继续下一轮（副作用重复发生却无 checkpoint
        记录），最终 StopIteration 落错误分支也静默 break。
        """
        worker = _base_worker(FalseSaveStorage())
        step_results = [
            {"execution_id": "exec-1", "is_end": False, "is_suspend": False, "context": {"step": 1}},
            {"execution_id": "exec-1", "is_end": False, "is_suspend": False, "context": {"step": 2}},
        ]
        execution = MagicMock()
        execution.run_distributed.side_effect = step_results
        with pytest.raises(StatePersistError, match="step_persist"):
            worker._process_execution_result(_flow(), step_results[0], _state(), execution=execution)

    def test_save_false_at_error_state_save_raises(self):
        """错误态落盘失败 → 抛错（修复前静默 break = 消息 ack = 错误无痕）。"""
        worker = _base_worker(FalseSaveStorage())
        execution = MagicMock()
        execution.run_distributed.side_effect = ValueError("boom")
        result = {"execution_id": "exec-1", "is_end": False, "is_suspend": False, "context": {}}
        with pytest.raises(StatePersistError, match="error_state"):
            worker._process_execution_result(_flow(), result, _state(), execution=execution)

    def test_save_false_in_resume_error_path_raises(self):
        """resume 错误处理路径的 error 态落盘失败 → 抛错（:461 调用点）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = FalseSaveStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)
        storage.save_execution_state("exec-1", _state())  # 照常写入（返回 False 而已）

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = ValueError("boom")
            with pytest.raises(RuntimeError, match="保存执行状态失败"):
                worker.resume_flow("f1", "exec-1", "continue")

    def test_save_false_at_resume_cancel_entry_raises(self):
        """resume 入口取消路径落盘失败 → 抛错（:412 调用点，评审清单外的新发现）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = FalseSaveStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)
        storage.save_execution_state("exec-1", _state())
        fake.set("plaita:execution:cancel:exec-1", "ts", ex=60)

        with pytest.raises(RuntimeError, match="保存执行状态失败"):
            worker.resume_flow("f1", "exec-1", "continue")

    def test_save_success_still_returns_normally(self):
        """守卫不误伤：save 正常返回 True 时行为零变化。"""
        worker = _base_worker(MemoryExecutionStorage())
        result = {"execution_id": "exec-1", "is_end": True, "context": {"answer": 42}}
        final = worker._process_execution_result(_flow(), result, _state())
        assert final["is_end"] is True


# ---------- 任务②：挂起派发失败不许吞 ----------


class TestDispatchFailureRaises:
    def test_rpush_failure_raises_when_redis_client_present(self):
        """有 redis_client 且 rpush 失败 → 抛错（修复前吞掉 = 消息 ack = 永久挂起）。"""
        worker = _base_worker(MemoryExecutionStorage())
        worker.redis_client = MagicMock()
        worker.redis_client.rpush.side_effect = ConnectionError("redis down")
        result = {
            "execution_id": "exec-1",
            "is_suspend": True,
            "service_config": {"type": "delay", "delay_ms": 1000},
            "context": {"$LAST_NODE": "a"},
        }
        with pytest.raises(ServiceDispatchError, match="挂起任务投递失败"):
            worker._process_execution_result(_flow(), result, _state())

    def test_rpush_failure_preserves_suspended_state_via_resume(self):
        """resume 链路端到端：派发失败抛错且 suspended 状态不被翻成 error。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)

        def boom(*args, **kwargs):
            raise ConnectionError("redis down during rpush")

        fake.rpush = boom
        storage.save_execution_state("exec-1", _state())

        suspend_result = {
            "execution_id": "exec-1",
            "is_suspend": True,
            "service_config": {"type": "delay", "delay_ms": 1000},
            "context": {"$LAST_NODE": "a", "$NODE": {}},
        }
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.return_value = suspend_result
            with pytest.raises(ServiceDispatchError):
                worker.resume_flow("f1", "exec-1", "continue")

        # 派发失败绝不把 suspended 翻成 error——否则重投消息命中
        # already_terminal 短路被 ack，执行永久卡死
        state = storage.load_execution_state("exec-1")
        assert state.status == "suspended"

    def test_dispatch_skipped_without_redis_client(self):
        """无 redis_client（内存 worker / 单测）派发跳过 + warning（兼容红线）。"""
        worker = _base_worker(MemoryExecutionStorage())  # 基类无 redis_client 属性
        result = {
            "execution_id": "exec-1",
            "is_suspend": True,
            "service_config": {"type": "delay", "delay_ms": 1000},
            "context": {"$LAST_NODE": "a"},
        }
        final = worker._process_execution_result(_flow(), result, _state())
        assert final["is_suspend"] is True

    def test_dispatch_not_service_node_still_silent(self):
        """非服务节点挂起（无 service_config）不派发、不抛错。"""
        worker = _base_worker(MemoryExecutionStorage())
        worker.redis_client = MagicMock()
        result = {
            "execution_id": "exec-1",
            "is_suspend": True,
            "context": {"$LAST_NODE": "a"},
        }
        final = worker._process_execution_result(_flow(), result, _state())
        assert final["is_suspend"] is True
        worker.redis_client.rpush.assert_not_called()


# ---------- 任务③：死信守卫（与 Track B 的队列参数契约） ----------


class TestDeadLetterGuard:
    def _guard_worker(self, fake, storage=None):
        return _redis_worker(fake, storage=storage)

    def _task(self, body):
        return StreamTask(message_id="msg-1", body=body, delivery_count=5)

    def _seed_running(self, worker, execution_id="exec-1"):
        """非终态执行状态（守卫判据 = 状态优先）。"""
        worker.execution_storage.save_execution_state(
            execution_id, _state(execution_id=execution_id, status="running")
        )

    def test_guard_allows_start_task_without_execution_id(self):
        """start 任务入队时还没有 execution_id → 放行死信。"""
        worker = self._guard_worker(fakeredis.FakeRedis(decode_responses=True))
        assert worker._dead_letter_guard(self._task({"type": "start", "flow_id": "f1"})) is True

    def test_guard_allows_when_state_missing(self):
        """执行状态缺失（无可恢复对象）→ 放行死信，租约无需检查。"""
        worker = self._guard_worker(fakeredis.FakeRedis(decode_responses=True))
        fake = worker.redis_client
        fake.set("plaita:execution:lease:exec-1", "resume:abc:3", ex=60)
        task = self._task({"type": "resume", "execution_id": "exec-1", "tenant_id": "default"})
        assert worker._dead_letter_guard(task) is True

    def test_guard_allows_when_terminal(self):
        """执行已终态 → 放行死信（already_terminal 短路本会 ack），不重入队。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._guard_worker(fake)
        worker.execution_storage.save_execution_state(
            "exec-1", _state(execution_id="exec-1", status="completed")
        )
        task = self._task({"type": "resume", "execution_id": "exec-1", "tenant_id": "default"})
        assert worker._dead_letter_guard(task) is True
        assert fake.xlen("test:worker-save-guard") == 0

    def test_guard_blocks_when_lease_held(self):
        """非终态 + 租约在（活 worker 正处理长步骤）→ 跳过死信。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._guard_worker(fake)
        self._seed_running(worker)
        fake.set("plaita:execution:lease:exec-1", "resume:abc:3", ex=60)
        task = self._task({"type": "resume", "execution_id": "exec-1", "tenant_id": "default"})
        assert worker._dead_letter_guard(task) is False
        assert fake.xlen("test:worker-save-guard") == 0

    def test_guard_requeues_when_holder_dead(self):
        """非终态 + 租约空（持有者已死）→ 重入队恢复路径后放行死信。

        delivery_count 被 XCLAIM 虚增不可逆：只跳过死信的话超限消息永不再
        被派发，执行照样僵尸——必须重入队一份 delivery 归 1 的新消息。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._guard_worker(fake)
        self._seed_running(worker)
        body = {"type": "resume", "execution_id": "exec-1", "tenant_id": "default",
                "resume_type": "event"}
        task = self._task(body)
        assert worker._dead_letter_guard(task) is True
        assert fake.xlen("test:worker-save-guard") == 1
        import json as _json

        entry = fake.xrange("test:worker-save-guard")
        payload = entry[0][1]["payload"] if isinstance(entry[0][1], dict) else entry[0][1][b"payload"]
        assert _json.loads(payload) == body

    def test_guard_skips_when_reenqueue_fails(self):
        """重入队失败（Redis 瞬断）→ 保守跳过死信，原消息留 pending。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._guard_worker(fake)
        self._seed_running(worker)

        def blip(*a, **kw):
            raise ConnectionError("connection reset")

        fake.xadd = blip
        task = self._task({"type": "resume", "execution_id": "exec-1", "tenant_id": "default"})
        assert worker._dead_letter_guard(task) is False

    def test_guard_routes_lease_key_by_tenant(self):
        """租约键按消息体 tenant_id 路由（守卫运行时 ContextVar 已复位）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._guard_worker(fake)
        self._seed_running(worker, execution_id="exec-9")
        # 只在 acme 租户 namespace 有租约 → 跳过死信
        fake.set("plaita:acme:execution:lease:exec-9", "resume:abc:3", ex=60)
        task_acme = self._task({"type": "resume", "execution_id": "exec-9", "tenant_id": "acme"})
        assert worker._dead_letter_guard(task_acme) is False
        # 同名执行在 default namespace 无租约 → 持有者已死路径：重入队后放行
        task_default = self._task({"type": "resume", "execution_id": "exec-9", "tenant_id": "default"})
        assert worker._dead_letter_guard(task_default) is True
        assert fake.xlen("test:worker-save-guard") == 1

    def test_guard_skips_dead_letter_when_lease_query_fails(self):
        """非终态下租约查询异常（瞬断）→ 保守跳过死信，不误杀活执行。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._guard_worker(fake)
        self._seed_running(worker)

        def blip(key):
            raise ConnectionError("connection reset")

        fake.exists = blip
        task = self._task({"type": "resume", "execution_id": "exec-1", "tenant_id": "default"})
        assert worker._dead_letter_guard(task) is False

    def test_guard_skips_when_state_load_raises(self):
        """执行状态加载失败（瞬断/损坏）→ 保守跳过死信。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._guard_worker(fake)

        class BlipStorage(MemoryExecutionStorage):
            def load_execution_state(self, execution_id):
                raise RuntimeError("redis blip")

        worker = self._guard_worker(fake, storage=BlipStorage())
        task = self._task({"type": "resume", "execution_id": "exec-1", "tenant_id": "default"})
        assert worker._dead_letter_guard(task) is False

    def test_guard_tolerates_non_dict_body(self):
        """body 非 dict（防御）→ 放行（死信兜底不被畸形消息卡死）。"""
        worker = self._guard_worker(fakeredis.FakeRedis(decode_responses=True))
        assert worker._dead_letter_guard(StreamTask(message_id="m", body=None)) is True


class TestTaskQueueGuardWiring:
    def test_get_task_queue_passes_guard_when_supported(self, monkeypatch):
        """队列类支持 dead_letter_guard 参数（Track B 合入后）→ 传入守卫。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._worker(fake)
        captured = {}

        class GuardAcceptingQueue:
            # 镜像 Track B 契约的显式签名（**kwargs 不产生具名参数，
            # 签名探测以具名参数为准）
            def __init__(
                self,
                redis_client,
                stream_key,
                *,
                group_name=None,
                consumer_name=None,
                claim_min_idle_ms=None,
                max_deliveries=None,
                dlq_key=None,
                dead_letter_guard=None,
            ):
                captured.update({
                    "group_name": group_name,
                    "consumer_name": consumer_name,
                    "claim_min_idle_ms": claim_min_idle_ms,
                    "max_deliveries": max_deliveries,
                    "dlq_key": dlq_key,
                    "dead_letter_guard": dead_letter_guard,
                })
                self.consumer_name = consumer_name or "c"

        monkeypatch.setattr(flow_worker_module, "RedisStreamTaskQueue", GuardAcceptingQueue)
        worker._get_task_queue()
        # 绑定方法每次属性访问都是新对象，用 == 比较（同函数同实例即等价）
        assert captured.get("dead_letter_guard") == worker._dead_letter_guard
        assert callable(captured.get("dead_letter_guard"))

    def test_get_task_queue_compatible_with_current_queue_class(self):
        """当前 main 的 RedisStreamTaskQueue 尚无该参数——接线必须向后兼容。

        Track B 合入前本测试证明 worker 构造队列不炸；合入后由集成联测
        验证守卫真正生效（等合并后联测）。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = self._worker(fake)
        queue = worker._get_task_queue()
        from plaita.server.task_queue import RedisStreamTaskQueue

        assert isinstance(queue, RedisStreamTaskQueue)

    def _worker(self, fake):
        return _redis_worker(fake)


# ---------- 守卫不影响 fenced / 租约既有链路 ----------


class TestLeaseChainUnaffected:
    def test_lease_conflict_still_raises_execution_lease_error(self):
        """fenced/租约失配路径走 ExecutionLeaseError（raise 而非 False），不受守卫影响。"""
        from plaita.server.execution_lease import ExecutionLeaseError

        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)

        # 先让另一 worker 持租约
        lease = worker.execution_lease
        holder_a = "resume:aaaa:1"
        assert lease.try_acquire("exec-1", holder_a, 60)
        storage.save_execution_state("exec-1", _state())

        with pytest.raises(ExecutionLeaseError):
            worker.resume_flow("f1", "exec-1", "continue")
