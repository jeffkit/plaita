"""
FlowWorker 测试场景补充
覆盖更多边界情况和业务场景

## 「假守卫」判定标准（plaita#154，改测试时必须照此自检）

一条守卫在满足**任一条**时即为**假守卫**，比没有守卫更糟——名字承诺了覆盖，
实际永远绿灯：

1. **零断言**：用例只证明「没抛异常」，没有任何可证伪的对象；
2. **断言不可达**：断言写在 ``try`` 里，被同函数的 ``except Exception: pass``
   吞掉（异常路径上连「没抛异常」都不成立）；
3. **断言了不可区分的东西**：如只断言 ``result is not None`` / ``state is
   not None``——把实现改成返回任何非空垃圾都照样绿。

**真守卫必须能红**：写完后必须对**被守卫的实现做变异**（改坏它，如把事件数据
丢掉、把 resume_type 透传写死成 continue），确认守卫失败；再还原为绿。没有
跑过变异的守卫按假守卫对待。

本文件 `TestFlowWorkerResumeScenarios` 的三条恢复用例按此标准修复：先厘清
worker resume 的契约（见各用例 docstring），再断言恢复后的**状态/结果本体**。
"""
import pytest
import json
import os
import asyncio
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution, ExecutionMode
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.base import ExecutionState

pytest.importorskip("cachetools")
pytest.importorskip("redis")

from plaita.server.flow_worker import FlowWorker, ResumeProtocolError
from plaita.event.memory import InMemoryEventBus


class TestFlowWorkerBasicScenarios:
    """FlowWorker 基础场景测试"""
    
    @pytest.fixture
    def setup_worker(self):
        """设置测试环境"""
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        event_bus = InMemoryEventBus()
        
        # 创建简单测试流程
        test_flow = {
            "flow_id": "test_flow_basic",
            "name": "基础测试流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "success"}
            ]
        }
        flow_storage.save_flow(test_flow)
        
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=event_bus,
            cache_size=10,
            cache_ttl=60
        )
        
        return {
            "worker": worker,
            "execution_storage": execution_storage,
            "flow_storage": flow_storage,
            "event_bus": event_bus,
            "test_flow": test_flow
        }
    
    def test_start_simple_flow(self, setup_worker):
        """测试启动简单流程"""
        worker = setup_worker["worker"]
        test_flow = setup_worker["test_flow"]
        
        result = worker.start_flow(
            flow_id=test_flow["flow_id"],
            params={"key": "value"},
            version=test_flow["version"]
        )
        
        assert "execution_id" in result
        assert result.get("is_end", False) or result.get("is_suspend", False)
    
    def test_flow_with_multiple_nodes(self, setup_worker):
        """测试包含多个节点的流程"""
        flow_storage = setup_worker["flow_storage"]
        worker = setup_worker["worker"]
        
        # 创建多节点流程
        multi_node_flow = {
            "flow_id": "multi_node_flow",
            "name": "多节点流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "step1"},
                {
                    "id": "step1", 
                    "type": "assignment",
                    "result": {"step": 1},
                    "next": "step2"
                },
                {
                    "id": "step2",
                    "type": "assignment", 
                    "result": {"step": 2},
                    "next": "end"
                },
                {"id": "end", "type": "end", "output": "$NODE.step2"}
            ]
        }
        flow_storage.save_flow(multi_node_flow)
        
        result = worker.start_flow(
            flow_id="multi_node_flow",
            params={},
            version="1.0.0"
        )
        
        assert "execution_id" in result
    
    def test_flow_definition_cache(self, setup_worker):
        """测试流程定义缓存功能"""
        worker = setup_worker["worker"]
        test_flow = setup_worker["test_flow"]
        
        # 第一次获取
        flow1 = worker.get_flow_definition(
            test_flow["flow_id"], 
            test_flow["version"]
        )
        
        # 第二次获取应该从缓存
        flow2 = worker.get_flow_definition(
            test_flow["flow_id"], 
            test_flow["version"]
        )
        
        assert flow1 is flow2  # 应该是同一个对象（来自缓存）
        
        cache_key = f"{test_flow['flow_id']}:{test_flow['version']}"
        assert f"default:{cache_key}" in worker.flow_definition_cache
    
    def test_get_latest_version(self, setup_worker):
        """测试获取最新版本流程"""
        worker = setup_worker["worker"]
        flow_storage = setup_worker["flow_storage"]
        
        # 保存多个版本
        for version in ["1.0.0", "1.1.0", "2.0.0"]:
            flow_storage.save_flow({
                "flow_id": "versioned_flow",
                "name": f"版本流程 {version}",
                "version": version,
                "nodes": [
                    {"id": "start", "type": "start", "next": "end"},
                    {"id": "end", "type": "end"}
                ]
            })
        
        # 获取最新版本
        flow = worker.get_flow_definition("versioned_flow")
        assert flow is not None


class TestFlowWorkerResumeScenarios:
    """FlowWorker 恢复场景测试"""
    
    @pytest.fixture
    def setup_suspended_flow(self):
        """设置**真实形态**的挂起 checkpoint。

        plaita#154：旧 fixture 写的是 ``{"last_node_id": "wait_event"}`` —— 键名
        不是引擎实际读取的 ``$LAST_NODE``、``$NODE`` 里也没有 ``status=pending``
        的节点结果，于是 ``resume_flow`` 在任何 resume_type 下都先撞
        ``ResumeGuardError("No suspended node found for resume")``。三条用例又
        各自 ``except Exception: pass``，所以它们从头到尾没有测到「恢复」这条
        路径。这里按 ``DistributedStrategy._handle_resume`` 的读取口径构造：
        ``$LAST_NODE`` 指向挂起节点、``$NODE.<id>.status == "pending"``。
        """
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        event_bus = InMemoryEventBus()
        
        # 创建事件流程
        event_flow = {
            "flow_id": "event_test_flow",
            "name": "事件测试流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "wait_event"},
                {
                    "id": "wait_event", 
                    "type": "event",
                    "event_type": "test.event",
                    "next": "end"
                },
                {"id": "end", "type": "end", "output": "completed"}
            ]
        }
        flow_storage.save_flow(event_flow)
        
        # 创建挂起状态
        execution_id = "test-suspended-001"
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="event_test_flow",
            flow_version="1.0.0",
            context={
                "$LAST_NODE": "wait_event",
                "$INPUT": {},
                # 挂起节点结果：EventNode.execute 挂起时写 status=pending。
                "$NODE": {
                    "wait_event": {
                        "status": "pending",
                        "event_type": "test.event",
                        "event_id": "evt-1",
                    }
                },
            },
            status="suspended",
            start_time=datetime.now().isoformat(),
            invoker="test"
        )
        execution_storage.save_execution_state(execution_id, state)
        
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=event_bus
        )
        
        return {
            "worker": worker,
            "execution_storage": execution_storage,
            "execution_id": execution_id,
            "state": state
        }
    
    def test_resume_with_event_data(self, setup_suspended_flow):
        """事件数据必须**真正送达**被唤醒的挂起节点，流程继续推进到 end。

        契约（worker resume / event 路径）：
        - ``resume_type=event`` 的 ``data`` 原样交给 ``EventNode.on_event``；
        - 挂起节点结果 ``$NODE.wait_event.status`` 由 ``pending`` → ``completed``，
          且 ``event_data`` == 投递的事件数据（事件数据不丢、不串）；
        - 流程推进到 ``end`` 并终态化 completed。

        真守卫判据（变异必须红）：把 ``EventNode.on_event`` 里
        ``state["event_data"] = event_data`` 删掉 / 改成常量，或让 worker 丢
        掉 ``data`` —— 本用例必须失败。旧的「``assert result is not None`` 写在
        ``try`` 里 + ``except: pass``」对这两种变异都不红，是假守卫。
        """
        worker = setup_suspended_flow["worker"]
        execution_storage = setup_suspended_flow["execution_storage"]
        execution_id = setup_suspended_flow["execution_id"]
        
        event_data = {
            "source": "test",
            "payload": {"key": "value"}
        }
        
        result = worker.resume_flow(
            flow_id="event_test_flow",
            execution_id=execution_id,
            resume_type="event",
            data=event_data
        )
        
        assert result["is_end"] is True
        assert result["is_suspend"] is False
        state = execution_storage.load_execution_state(execution_id)
        assert state.status == "completed"
        wait_node = state.context["$NODE"]["wait_event"]
        assert wait_node["status"] == "completed"
        assert wait_node["event_data"] == event_data
        assert state.context["$LAST_NODE"] == "end"
    
    def test_resume_with_cancel(self, setup_suspended_flow):
        """``resume_type=cancel`` 必须把挂起节点判成 cancelled 并真实推进。

        契约（节点级取消）：取消决议送达挂起节点 → ``EventNode.on_cancel`` 把
        ``$NODE.wait_event.status`` 置为 ``cancelled``，流程按节点的 ``next``
        继续（要不要因取消而终止由流程分支按该状态决定）。**执行级**的取消
        终态化走另一条路（console 先写 cancelled 状态 / 取消标志键），由
        ``test_resume_cancelled_is_terminal`` 与取消监听用例覆盖。
        本用例钉住的是「cancel 决议被应用且流程真的推进」，不是「没抛异常」。

        真守卫判据（变异必须红）：把 ``EventNode.on_cancel`` 的状态写成
        非 cancelled（如 completed），或让 worker 把 cancel 当 continue 幂等
        短路 —— 本用例必须失败。
        """
        worker = setup_suspended_flow["worker"]
        execution_storage = setup_suspended_flow["execution_storage"]
        execution_id = setup_suspended_flow["execution_id"]
        
        result = worker.resume_flow(
            flow_id="event_test_flow",
            execution_id=execution_id,
            resume_type="cancel",
            data={}
        )
        
        assert result["is_end"] is True
        state = execution_storage.load_execution_state(execution_id)
        wait_node = state.context["$NODE"]["wait_event"]
        assert wait_node["status"] == "cancelled"
        # 流程真的推进了（不是「受理后无动作」的假恢复）。
        assert state.context["$LAST_NODE"] == "end"
        assert state.status == "completed"
    
    def test_resume_with_timeout(self, setup_suspended_flow):
        """``resume_type=timeout`` 必须①原样下发给引擎 ②把挂起节点判成 timeout。

        契约（worker resume / timeout 路径）：worker 不解释 timeout，只把它当
        决议透传 —— ``run_distributed(resume_type="timeout", resume_data=...)``；
        引擎侧 ``EventNode.on_timeout`` 把节点状态置为 ``timeout`` 后按 ``next``
        推进。本用例用 spy 钉住透传（防止「受理后无动作」），再断言节点状态。

        真守卫判据（变异必须红）：① 把 worker 里 ``resume_type=resume_type``
        写死成 ``"continue"`` → spy 断言失败；② 把 ``on_timeout`` 的状态改成
        非 timeout → 节点态断言失败。旧的零断言版本对两者都不红。
        """
        worker = setup_suspended_flow["worker"]
        execution_storage = setup_suspended_flow["execution_storage"]
        execution_id = setup_suspended_flow["execution_id"]

        # spy：包住真实的 run_distributed，记录 worker 实际下发的 resume 载体。
        # 记**全部**调用：resume 那一步之后 worker 还会以 ``resume_type=continue``
        # 续推后续节点（见 _process_execution_result），验收的是**第一步**。
        calls = []
        real_run_distributed = FlowExecution.run_distributed

        def _spy(self, flow, **kwargs):
            calls.append(kwargs)
            return real_run_distributed(self, flow, **kwargs)

        with patch.object(FlowExecution, "run_distributed", _spy):
            result = worker.resume_flow(
                flow_id="event_test_flow",
                execution_id=execution_id,
                resume_type="timeout",
                data={}
            )

        assert calls, "resume 必须真实下发到引擎（禁止「受理后无动作」）"
        assert calls[0]["resume_type"] == "timeout", (
            "resume 消息携带的 timeout 必须原样下发给引擎，"
            f"实际 {calls[0].get('resume_type')!r}"
        )
        assert calls[0]["resume_data"] == {}
        assert result["is_end"] is True
        state = execution_storage.load_execution_state(execution_id)
        assert state.context["$NODE"]["wait_event"]["status"] == "timeout"

    def test_negative_control_non_pending_checkpoint_is_protocol_error(
        self, setup_suspended_flow
    ):
        """负向对照：checkpoint 不是「挂起节点 pending」时，event resume 必须
        **响亮**报协议错误且执行保持原状（不终态化、不假推进）。

        它证明上面三条守卫的绿灯来自「决议被真实应用 + 流程真的推进」，而不是
        「无论输入如何都绿」：把 ``$NODE.wait_event.status`` 从 pending 改成
        completed，同一个 ``resume_type=event`` 立刻抛 ``ResumeProtocolError``。
        """
        worker = setup_suspended_flow["worker"]
        execution_storage = setup_suspended_flow["execution_storage"]
        execution_id = setup_suspended_flow["execution_id"]

        state = execution_storage.load_execution_state(execution_id)
        state.context["$NODE"]["wait_event"]["status"] = "completed"
        execution_storage.save_execution_state(execution_id, state)

        with pytest.raises(ResumeProtocolError):
            worker.resume_flow(
                flow_id="event_test_flow",
                execution_id=execution_id,
                resume_type="event",
                data={"source": "test"},
            )

        kept = execution_storage.load_execution_state(execution_id)
        assert kept.status == "suspended", "协议错误不得把挂起执行终态化/打封"
        assert kept.context["$NODE"]["wait_event"]["status"] == "completed"

    def test_resume_cancelled_is_terminal(self, setup_suspended_flow):
        """cancelled 是终态：重复投递的 cancel/resume 消息幂等跳过，不再推进。

        console cancel 端点先写 cancelled 再投 resume_type=cancel 消息；
        若终态短路不认 cancelled，挂起执行会被 on_cancel 续跑到 end 翻成
        completed（「已取消」跳回「已完成」）。E2E cancel 回归用例钉同一语义。
        """
        worker = setup_suspended_flow["worker"]
        execution_storage = setup_suspended_flow["execution_storage"]
        execution_id = setup_suspended_flow["execution_id"]

        # 模拟 console cancel 端点已写入的终态
        state = execution_storage.load_execution_state(execution_id)
        state.status = "cancelled"
        execution_storage.save_execution_state(execution_id, state)

        result = worker.resume_flow(
            flow_id="event_test_flow",
            execution_id=execution_id,
            resume_type="cancel",
            data={},
        )
        assert result["already_terminal"] is True
        assert result["status"] == "cancelled"
        # 状态未被改写
        assert execution_storage.load_execution_state(execution_id).status == "cancelled"

    def test_resume_invalid_execution_id(self, setup_suspended_flow):
        """测试无效执行ID恢复"""
        worker = setup_suspended_flow["worker"]
        
        with pytest.raises(ValueError) as exc_info:
            worker.resume_flow(
                flow_id="event_test_flow",
                execution_id="non-existent-id",
                resume_type="event"
            )
        
        assert "找不到执行状态" in str(exc_info.value)
    
    def test_resume_flow_id_mismatch(self, setup_suspended_flow):
        """测试流程ID不匹配"""
        worker = setup_suspended_flow["worker"]
        execution_id = setup_suspended_flow["execution_id"]
        
        with pytest.raises(ValueError) as exc_info:
            worker.resume_flow(
                flow_id="wrong_flow_id",
                execution_id=execution_id,
                resume_type="event"
            )
        
        assert "不匹配" in str(exc_info.value)


class TestFlowWorkerErrorHandling:
    """FlowWorker 错误处理测试"""
    
    @pytest.fixture
    def setup_worker(self):
        """设置测试环境"""
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage
        )
        
        return {
            "worker": worker,
            "execution_storage": execution_storage,
            "flow_storage": flow_storage
        }
    
    def test_start_non_existent_flow(self, setup_worker):
        """测试启动不存在的流程"""
        worker = setup_worker["worker"]
        
        with pytest.raises((ValueError, RuntimeError)):
            worker.start_flow(
                flow_id="non_existent_flow",
                params={}
            )
    
    def test_get_non_existent_flow_definition(self, setup_worker):
        """测试获取不存在的流程定义"""
        worker = setup_worker["worker"]
        
        with pytest.raises(ValueError) as exc_info:
            worker.get_flow_definition("non_existent_flow")
        
        assert "找不到流程定义" in str(exc_info.value)
    
    def test_invalid_flow_definition(self, setup_worker):
        """测试无效的流程定义"""
        flow_storage = setup_worker["flow_storage"]
        worker = setup_worker["worker"]
        
        # 保存无效的流程定义 - 使用无法解析的节点
        invalid_flow = {
            "flow_id": "invalid_flow",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "invalid_type_that_does_not_exist", "next": "end"}
            ]
        }
        flow_storage.save_flow(invalid_flow)
        
        # 由于 pydantic 可能仍能解析，我们测试是否能获取到定义
        # 如果能获取到，验证它是有效的 Flow 对象
        try:
            flow = worker.get_flow_definition("invalid_flow", "1.0.0")
            # 如果成功获取，验证基本属性
            assert flow.flow_id == "invalid_flow"
        except ValueError as e:
            # 如果解析失败，验证错误消息
            assert "解析流程定义失败" in str(e) or "找不到流程定义" in str(e)


class TestFlowWorkerExecutionResultProcessing:
    """FlowWorker 执行结果处理测试"""
    
    @pytest.fixture
    def setup_worker_with_flow(self):
        """设置带流程的工作器"""
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        
        test_flow = {
            "flow_id": "result_test_flow",
            "name": "结果测试流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "done"}
            ]
        }
        flow_storage.save_flow(test_flow)
        
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage
        )
        
        flow = worker.get_flow_definition("result_test_flow", "1.0.0")
        
        return {
            "worker": worker,
            "execution_storage": execution_storage,
            "flow": flow
        }
    
    def test_process_completed_result(self, setup_worker_with_flow):
        """测试处理完成状态结果"""
        worker = setup_worker_with_flow["worker"]
        execution_storage = setup_worker_with_flow["execution_storage"]
        flow = setup_worker_with_flow["flow"]
        
        execution_id = "test-complete-001"
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="result_test_flow",
            flow_version="1.0.0",
            context={},
            status="running",
            start_time=datetime.now().isoformat(),
            invoker="test"
        )
        
        result = {
            "execution_id": execution_id,
            "is_end": True,
            "is_suspend": False,
            "context": {"result": "success"}
        }
        
        final_result = worker._process_execution_result(flow, result, state)
        
        assert state.status == "completed"
        assert state.end_time is not None
    
    def test_process_suspended_result(self, setup_worker_with_flow):
        """测试处理挂起状态结果"""
        worker = setup_worker_with_flow["worker"]
        flow = setup_worker_with_flow["flow"]
        
        execution_id = "test-suspend-001"
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="result_test_flow",
            flow_version="1.0.0",
            context={},
            status="running",
            start_time=datetime.now().isoformat(),
            invoker="test"
        )
        
        result = {
            "execution_id": execution_id,
            "is_end": False,
            "is_suspend": True,
            "context": {"waiting": True}
        }
        
        final_result = worker._process_execution_result(flow, result, state)
        
        assert state.status == "suspended"


class TestFlowWorkerConcurrency:
    """FlowWorker 并发场景测试"""
    
    @pytest.fixture
    def setup_concurrent_worker(self):
        """设置并发测试环境"""
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        
        concurrent_flow = {
            "flow_id": "concurrent_test_flow",
            "name": "并发测试流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "done"}
            ]
        }
        flow_storage.save_flow(concurrent_flow)
        
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage
        )
        
        return {
            "worker": worker,
            "execution_storage": execution_storage,
            "flow_storage": flow_storage
        }
    
    def test_multiple_flow_starts(self, setup_concurrent_worker):
        """测试同时启动多个流程"""
        worker = setup_concurrent_worker["worker"]
        execution_storage = setup_concurrent_worker["execution_storage"]
        
        execution_ids = []
        for i in range(5):
            result = worker.start_flow(
                flow_id="concurrent_test_flow",
                params={"index": i},
                version="1.0.0"
            )
            execution_ids.append(result.get("execution_id"))
        
        # 验证每个执行都有唯一ID
        assert len(set(execution_ids)) == 5
    
    def test_cache_thread_safety(self, setup_concurrent_worker):
        """测试缓存线程安全性"""
        worker = setup_concurrent_worker["worker"]
        
        # 多次获取流程定义
        flows = []
        for _ in range(10):
            flow = worker.get_flow_definition("concurrent_test_flow", "1.0.0")
            flows.append(flow)
        
        # 所有获取应该返回相同对象
        assert all(f is flows[0] for f in flows)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

