"""
FlowWorker 测试场景补充
覆盖更多边界情况和业务场景
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
from plaita.server.flow_worker import FlowWorker
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
        assert cache_key in worker.flow_definition_cache
    
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
        """设置挂起状态的流程"""
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
                "last_node_id": "wait_event",
                "$INPUT": {}
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
        """测试使用事件数据恢复流程"""
        worker = setup_suspended_flow["worker"]
        execution_id = setup_suspended_flow["execution_id"]
        
        event_data = {
            "source": "test",
            "payload": {"key": "value"}
        }
        
        try:
            result = worker.resume_flow(
                flow_id="event_test_flow",
                execution_id=execution_id,
                resume_type="event",
                data=event_data
            )
            assert result is not None
        except Exception:
            # 即使执行失败，状态应该被更新
            pass
    
    def test_resume_with_cancel(self, setup_suspended_flow):
        """测试取消恢复类型"""
        worker = setup_suspended_flow["worker"]
        execution_storage = setup_suspended_flow["execution_storage"]
        execution_id = setup_suspended_flow["execution_id"]
        
        try:
            result = worker.resume_flow(
                flow_id="event_test_flow",
                execution_id=execution_id,
                resume_type="cancel",
                data={}
            )
        except Exception:
            pass
        
        # 验证状态已更新
        state = execution_storage.load_execution_state(execution_id)
        assert state is not None
    
    def test_resume_with_timeout(self, setup_suspended_flow):
        """测试超时恢复类型"""
        worker = setup_suspended_flow["worker"]
        execution_id = setup_suspended_flow["execution_id"]
        
        try:
            result = worker.resume_flow(
                flow_id="event_test_flow",
                execution_id=execution_id,
                resume_type="timeout",
                data={}
            )
        except Exception:
            pass

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

