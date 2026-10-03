"""start 幂等键 dedup_key（2026-10 第二波 Track P1 任务③）。

基线缺陷：start 消息重投（worker 崩溃/保存失败）→ ``start_flow`` 新建
execution_id 从头跑，首节点副作用双份（文档已标注首节点须幂等，第一波只
补了文档）。

可行性核实（本任务的前提）：execution_id 在 ``FlowExecution`` 构造时即生成
——``FlowExecution.__init__`` 构造 ``ExecutionContext``（core/executor.py:105），
后者在 ``__init__`` 里 ``uuid4().hex`` 写入 ``$EXECUTION_ID``
（core/context.py:220），构造后、``run_distributed`` 前即可读
``execution.execution_id``。因此 worker 能在首节点执行前认领幂等键。

修复契约：
- 消息体可选 ``dedup_key``：``start_flow`` 在 run_distributed **之前**以
  SET NX + EX 7d 原子认领 ``{ns}:start-dedup:{key}``（租户路由，与租约键
  同规则）；
- 命中 → 读映射的执行状态：已终态 → 返回 already 形状；非终态 running →
  重入队一份 resume 消息接续执行（只返回现状会让执行无人推进永久卡
  running）再 ack start；非终态 suspended → 只返回现状（挂起执行自有
  delay/approval 的 resume 链路，重入队 continue 会被 pending 校验拒绝）。
  **任何分支都绝不二次 start**；
- 孤儿映射（认领后首次执行从未落盘，如 crash 在 claim 与 save 之间）→
  GET==id 才 DEL 释放，重新认领后按新启动继续（首节点可能重跑——首节点
  须幂等是既有约定）；
- 不传 dedup_key / 无 redis 客户端 / 认领瞬断 → 行为与存量完全一致；
- 不自动生成键（如 body hash）：同参数定时任务（cron 每小时跑同一 flow）
  会被误判为重复启动而永不执行，键必须调用方显式声明「同键=同逻辑 start」。
"""
import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import (
    FlowWorker,
    RedisFlowWorker,
    ServiceDispatchError,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

QUEUE = "test:worker-start-dedup"


def _flow_storage():
    fs = MemoryFlowStorage()
    fs.save_flow(TEST_FLOW)
    return fs


def _redis_worker(fake, storage=None, flow_storage=None) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else _flow_storage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _patched_execution(result_by_call=None, final=None, execution_id="exec-1"):
    """patch FlowExecution：execution_id 可预读（任务③前提），run_distributed
    依次返回 result_by_call 各步。"""
    decorator = patch("plaita.server.flow_worker.FlowExecution")

    def wrap(fn):
        def inner(*args, **kwargs):
            with decorator as FE:
                inst = MagicMock()
                FE.return_value = inst
                inst.execution_id = execution_id
                if result_by_call is not None:
                    inst.run_distributed.side_effect = list(result_by_call)
                else:
                    inst.run_distributed.return_value = final
                return fn(inst)
        return inner

    return wrap


class TestClaimSemantics:
    def test_claim_writes_mapping_with_ttl(self):
        """认领成功写 {ns}:start-dedup:{key} = execution_id，带 7 天内 TTL。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)

        @_patched_execution(
            final={"execution_id": "exec-1", "is_end": True, "context": {}}
        )
        def run(inst):
            return worker.start_flow("f1", {}, version="1", dedup_key="k1",
                                     execution_id="exec-1")

        run()
        assert fake.get("plaita:start-dedup:k1") == "exec-1"
        assert 0 < fake.ttl("plaita:start-dedup:k1") <= 7 * 86400

    def test_tenant_routed_key(self):
        """键按租户 namespace 路由（与租约键同规则）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        from plaita.server.tenant_context import (
            reset_current_tenant,
            set_current_tenant,
        )

        @_patched_execution(
            final={"execution_id": "exec-a", "is_end": True, "context": {}},
            execution_id="exec-a",
        )
        def run(inst):
            token = set_current_tenant("acme")
            try:
                return worker.start_flow("f1", {}, version="1", dedup_key="k1",
                                         execution_id="exec-a")
            finally:
                reset_current_tenant(token)

        run()
        assert fake.get("plaita:acme:start-dedup:k1") == "exec-a"
        assert fake.get("plaita:start-dedup:k1") is None

    def test_no_dedup_key_no_key_written(self):
        """不传 dedup_key → 不写任何键，行为与存量一致。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)

        @_patched_execution(
            final={"execution_id": "exec-1", "is_end": True, "context": {}}
        )
        def run(inst):
            return worker.start_flow("f1", {}, version="1")

        run()
        assert fake.xlen(QUEUE) == 0
        assert not fake.keys("plaita:start-dedup*")

    def test_base_worker_without_redis_starts_normally(self):
        """无 redis 客户端（内存 worker）dedup_key 静默不启用（兼容红线）。"""
        worker = FlowWorker(
            execution_storage=MemoryExecutionStorage(),
            flow_storage=_flow_storage(),
        )
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-1"
            inst.run_distributed.return_value = {
                "execution_id": "exec-1", "is_end": True, "context": {},
            }
            result = worker.start_flow("f1", {}, version="1", dedup_key="k1")
        assert result["is_end"] is True


class TestDedupHit:
    def test_hit_terminal_returns_already_shape_without_restart(self):
        """命中 + 已终态 → already 形状，绝不二次 start（run 只调一次）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage, _flow_storage())

        calls = []

        @_patched_execution(
            final={"execution_id": "exec-1", "is_end": True, "context": {"r": 1}},
            execution_id="exec-1",
        )
        def first(inst):
            calls.append(1)
            return worker.start_flow("f1", {}, version="1", dedup_key="k1",
                                     execution_id="exec-1")

        first()
        assert len(calls) == 1

        # 模拟重投：再次 start 同键 → 命中已完成执行
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-2"  # 若二次 start 会生成新执行
            result = worker.start_flow("f1", {}, version="1", dedup_key="k1",
                                       execution_id="exec-2")
        assert result["execution_id"] == "exec-1"
        assert result["already_terminal"] is True
        assert result["deduplicated"] is True
        inst.run_distributed.assert_not_called()
        # 组合语义（G1 先行落行）：命中在落新行**之前**返回，不产生新执行行
        assert storage.load_execution_state("exec-2") is None

    def test_hit_running_reenqueues_resume_and_never_restarts(self):
        """命中 + 非终态 running（崩溃/重试搁浅）→ 重入队 resume 接续，
        不二次 start。这让任务①的重试在 start 消息上也能收敛到 checkpoint。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage, _flow_storage())
        storage.save_execution_state(
            "exec-1",
            __import__("plaita.storage.base", fromlist=["ExecutionState"]).ExecutionState(
                execution_id="exec-1", flow_id="f1", status="running",
                context={"$LAST_NODE": "start", "$NODE": {}},
            ),
        )
        fake.set("plaita:start-dedup:k1", "exec-1")

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-2"
            result = worker.start_flow("f1", {}, version="1", dedup_key="k1")

        assert result["execution_id"] == "exec-1"
        assert result["deduplicated"] is True
        assert result["resume_requeued"] is True
        inst.run_distributed.assert_not_called()
        # 队列里有一份 resume 消息指向原执行
        entry = fake.xrange(QUEUE)
        assert len(entry) == 1
        fields = entry[0][1]
        payload = fields["payload"] if "payload" in fields else fields[b"payload"]
        msg = json.loads(payload)
        assert msg["type"] == "resume"
        assert msg["execution_id"] == "exec-1"
        assert msg["resume_type"] == "continue"

    def test_hit_suspended_returns_current_state_without_requeue(self):
        """命中 + suspended → 只返回现状形状不重入队：挂起执行自有
        delay/approval resume 链路，重入队 continue 会被 pending 校验拒绝。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage, _flow_storage())
        storage.save_execution_state(
            "exec-1",
            __import__("plaita.storage.base", fromlist=["ExecutionState"]).ExecutionState(
                execution_id="exec-1", flow_id="f1", status="suspended",
                context={"$LAST_NODE": "wait", "$NODE": {}},
            ),
        )
        fake.set("plaita:start-dedup:k1", "exec-1")

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-2"
            result = worker.start_flow("f1", {}, version="1", dedup_key="k1")

        assert result == {
            "execution_id": "exec-1",
            "status": "suspended",
            "deduplicated": True,
        }
        inst.run_distributed.assert_not_called()
        assert fake.xlen(QUEUE) == 0

    def test_hit_orphan_mapping_releases_and_starts_fresh(self):
        """孤儿映射（认领后首次执行从未落盘）→ 释放并重新认领，按新启动继续。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake, MemoryExecutionStorage(), _flow_storage())
        fake.set("plaita:start-dedup:k1", "exec-ghost")  # 指向不存在的执行

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-new"
            inst.run_distributed.return_value = {
                "execution_id": "exec-new", "is_end": True, "context": {},
            }
            result = worker.start_flow("f1", {}, version="1", dedup_key="k1",
                                       execution_id="exec-new")

        assert result["is_end"] is True
        inst.run_distributed.assert_called_once()  # 确实启动了
        # 重新认领成功：映射指向本次执行
        assert fake.get("plaita:start-dedup:k1") == "exec-new"

    def test_reenqueue_failure_raises_service_dispatch_error(self):
        """命中 running 但重入队失败（瞬断）→ 抛 ServiceDispatchError 让
        start 消息重投（不吞：吞了执行无人接续）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _redis_worker(fake, storage, _flow_storage())
        storage.save_execution_state(
            "exec-1",
            __import__("plaita.storage.base", fromlist=["ExecutionState"]).ExecutionState(
                execution_id="exec-1", flow_id="f1", status="running",
                context={"$LAST_NODE": "start", "$NODE": {}},
            ),
        )
        fake.set("plaita:start-dedup:k1", "exec-1")

        def blip(*a, **kw):
            raise ConnectionError("redis down")

        fake.xadd = blip
        with pytest.raises(ServiceDispatchError):
            worker.start_flow("f1", {}, version="1", dedup_key="k1")


class TestDispatchPassthrough:
    def test_dispatch_task_passes_dedup_key(self):
        """消息体 dedup_key 透传进 start_flow。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        seen = {}
        original = worker.start_flow

        def spy(flow_id, params, version=None, execution_id=None,
                dedup_key=None, delivery_count=None):
            seen["dedup_key"] = dedup_key
            seen["execution_id"] = execution_id
            return {"is_end": True}

        worker.start_flow = spy
        worker._dispatch_task(
            {"type": "start", "flow_id": "f1", "params": {}, "dedup_key": "abc"},
            delivery_count=1,
        )
        assert seen["dedup_key"] == "abc"
        assert seen["execution_id"] is None  # 消息未带预铸 id


class TestConsoleBFF:
    def test_start_request_accepts_dedup_key(self):
        """BFF StartFlowRequest additive 字段：缺省 None（旧客户端零变化）。"""
        pytest.importorskip("fastapi")
        pytest.importorskip("pydantic")
        # api.executions 链上 import sse_starlette（console backend 依赖，
        # 不在 plaita dev extras 内——无此包的环境优雅跳过）
        pytest.importorskip("sse_starlette")
        import sys
        from pathlib import Path

        console_backend = Path(__file__).resolve().parents[2] / "plaita-console" / "backend"
        if str(console_backend) not in sys.path:
            sys.path.insert(0, str(console_backend))
        from api.executions import StartFlowRequest

        req = StartFlowRequest(flow_id="f1")
        assert req.dedup_key is None
        req2 = StartFlowRequest(flow_id="f1", dedup_key="order-123")
        assert req2.dedup_key == "order-123"
