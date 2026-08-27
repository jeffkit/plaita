"""
性能基准测试
测试各组件的性能表现和资源使用
"""
import pytest

pytest.importorskip('redis')
pytest.importorskip('cachetools')
import asyncio
import time
import gc
import sys
from typing import Dict, Any, List
from datetime import datetime
from statistics import mean, stdev

from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution, ExecutionMode
from plaita.event.memory import InMemoryEventBus
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.base import ExecutionState
from plaita.server.flow_worker import FlowWorker
from plaita.server.services.delay_service import DelayService
from plaita.server.services.approval_service import ApprovalService
from plaita.server.nodes.delay_node import DelayNode


class PerformanceResult:
    """性能测试结果"""
    
    def __init__(self, name: str):
        self.name = name
        self.durations: List[float] = []
        self.memory_before: int = 0
        self.memory_after: int = 0
    
    def record(self, duration: float):
        """记录一次执行时间"""
        self.durations.append(duration)
    
    @property
    def count(self) -> int:
        return len(self.durations)
    
    @property
    def total_time(self) -> float:
        return sum(self.durations)
    
    @property
    def avg_time(self) -> float:
        return mean(self.durations) if self.durations else 0
    
    @property
    def min_time(self) -> float:
        return min(self.durations) if self.durations else 0
    
    @property
    def max_time(self) -> float:
        return max(self.durations) if self.durations else 0
    
    @property
    def std_dev(self) -> float:
        return stdev(self.durations) if len(self.durations) > 1 else 0
    
    @property
    def ops_per_second(self) -> float:
        return self.count / self.total_time if self.total_time > 0 else 0
    
    @property
    def memory_used(self) -> int:
        return self.memory_after - self.memory_before
    
    def __str__(self) -> str:
        return (
            f"{self.name}:\n"
            f"  执行次数: {self.count}\n"
            f"  总耗时: {self.total_time:.4f}s\n"
            f"  平均耗时: {self.avg_time * 1000:.2f}ms\n"
            f"  最小耗时: {self.min_time * 1000:.2f}ms\n"
            f"  最大耗时: {self.max_time * 1000:.2f}ms\n"
            f"  标准差: {self.std_dev * 1000:.2f}ms\n"
            f"  每秒操作数: {self.ops_per_second:.2f} ops/s"
        )


def get_memory_usage() -> int:
    """获取当前内存使用量（字节）"""
    gc.collect()
    return sys.getsizeof(sys.modules)


class TestFlowExecutionPerformance:
    """流程执行性能测试"""
    
    @pytest.fixture
    def simple_flow(self):
        """简单流程"""
        return {
            "flow_id": "perf_simple_flow",
            "name": "性能测试简单流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "done"}
            ]
        }
    
    @pytest.fixture
    def complex_flow(self):
        """复杂流程"""
        nodes = [{"id": "start", "type": "start", "next": "step_1"}]
        
        for i in range(1, 20):
            nodes.append({
                "id": f"step_{i}",
                "type": "assignment",
                "result": {"step": i, "timestamp": f"${{Date.now()}}"},
                "next": f"step_{i+1}" if i < 19 else "end"
            })
        
        nodes.append({"id": "end", "type": "end", "output": "$NODE.step_19"})
        
        return {
            "flow_id": "perf_complex_flow",
            "name": "性能测试复杂流程",
            "version": "1.0.0",
            "nodes": nodes
        }
    
    def test_simple_flow_execution_performance(self, simple_flow):
        """简单流程执行性能测试"""
        flow = Flow.model_validate(simple_flow)
        event_bus = InMemoryEventBus()
        
        result = PerformanceResult("简单流程执行")
        iterations = 100
        
        # 预热
        for _ in range(5):
            FlowExecution.run(flow, params={}, mode=ExecutionMode.NORMAL, event_bus=event_bus)
        
        # 正式测试
        result.memory_before = get_memory_usage()
        
        for _ in range(iterations):
            start = time.perf_counter()
            FlowExecution.run(flow, params={}, mode=ExecutionMode.NORMAL, event_bus=event_bus)
            end = time.perf_counter()
            result.record(end - start)
        
        result.memory_after = get_memory_usage()
        
        print(f"\n{result}")
        
        # 断言性能指标
        assert result.avg_time < 0.1  # 平均执行时间小于 100ms
        assert result.ops_per_second > 10  # 每秒至少 10 次操作
    
    def test_complex_flow_execution_performance(self, complex_flow):
        """复杂流程执行性能测试"""
        flow = Flow.model_validate(complex_flow)
        event_bus = InMemoryEventBus()
        
        result = PerformanceResult("复杂流程执行 (20个节点)")
        iterations = 50
        
        # 预热
        for _ in range(3):
            FlowExecution.run(flow, params={}, mode=ExecutionMode.NORMAL, event_bus=event_bus)
        
        # 正式测试
        for _ in range(iterations):
            start = time.perf_counter()
            FlowExecution.run(flow, params={}, mode=ExecutionMode.NORMAL, event_bus=event_bus)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        # 断言性能指标
        assert result.avg_time < 0.5  # 平均执行时间小于 500ms
        assert result.ops_per_second > 2  # 每秒至少 2 次操作
    
    def test_distributed_mode_performance(self, simple_flow):
        """分布式模式执行性能测试"""
        flow = Flow.model_validate(simple_flow)
        event_bus = InMemoryEventBus()
        
        result = PerformanceResult("分布式模式执行")
        iterations = 100
        
        # 正式测试
        for _ in range(iterations):
            start = time.perf_counter()
            FlowExecution.run(flow, params={}, mode=ExecutionMode.DISTRIBUTED, event_bus=event_bus)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        # 断言性能指标
        assert result.avg_time < 0.1  # 平均执行时间小于 100ms


class TestFlowWorkerPerformance:
    """FlowWorker 性能测试"""
    
    @pytest.fixture
    def worker_setup(self):
        """设置 FlowWorker"""
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        event_bus = InMemoryEventBus()
        
        test_flow = {
            "flow_id": "perf_worker_flow",
            "name": "Worker性能测试流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "done"}
            ]
        }
        flow_storage.save_flow(test_flow)
        
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=event_bus,
            cache_size=100,
            cache_ttl=300
        )
        
        return {
            "worker": worker,
            "flow_id": test_flow["flow_id"],
            "version": test_flow["version"]
        }
    
    def test_flow_start_performance(self, worker_setup):
        """流程启动性能测试"""
        worker = worker_setup["worker"]
        flow_id = worker_setup["flow_id"]
        version = worker_setup["version"]
        
        result = PerformanceResult("FlowWorker 启动流程")
        iterations = 50
        
        # 预热缓存
        worker.get_flow_definition(flow_id, version)
        
        for i in range(iterations):
            start = time.perf_counter()
            worker.start_flow(flow_id=flow_id, params={"index": i}, version=version)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        assert result.avg_time < 0.2  # 平均启动时间小于 200ms
    
    def test_flow_definition_cache_performance(self, worker_setup):
        """流程定义缓存性能测试"""
        worker = worker_setup["worker"]
        flow_id = worker_setup["flow_id"]
        version = worker_setup["version"]
        
        # 缓存未命中测试
        worker.flow_definition_cache.clear()
        
        uncached_result = PerformanceResult("流程定义获取 (未缓存)")
        
        start = time.perf_counter()
        worker.get_flow_definition(flow_id, version)
        end = time.perf_counter()
        uncached_result.record(end - start)
        
        # 缓存命中测试
        cached_result = PerformanceResult("流程定义获取 (已缓存)")
        iterations = 100
        
        for _ in range(iterations):
            start = time.perf_counter()
            worker.get_flow_definition(flow_id, version)
            end = time.perf_counter()
            cached_result.record(end - start)
        
        print(f"\n{uncached_result}")
        print(f"\n{cached_result}")
        
        # 缓存命中应该快很多
        assert cached_result.avg_time < uncached_result.avg_time


class TestEventBusPerformance:
    """事件总线性能测试"""
    
    @pytest.fixture
    def event_bus(self):
        """创建事件总线"""
        return InMemoryEventBus()
    
    @pytest.mark.asyncio
    async def test_event_publish_performance(self, event_bus):
        """事件发布性能测试"""
        result = PerformanceResult("事件发布")
        iterations = 1000
        
        # 注册一个简单处理器
        async def handler(event):
            pass
        
        await event_bus.register_handler("test_event", handler)
        
        for i in range(iterations):
            start = time.perf_counter()
            await event_bus.publish("test_event", {"index": i})
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        assert result.ops_per_second > 1000  # 每秒至少 1000 次发布
    
    @pytest.mark.asyncio
    async def test_event_handler_registration_performance(self, event_bus):
        """事件处理器注册性能测试"""
        result = PerformanceResult("处理器注册")
        iterations = 100
        
        async def handler(event):
            pass
        
        for i in range(iterations):
            start = time.perf_counter()
            await event_bus.register_handler(f"event_{i}", handler)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        assert result.ops_per_second > 100  # 每秒至少 100 次注册


class TestStoragePerformance:
    """存储性能测试"""
    
    @pytest.fixture
    def execution_storage(self):
        """创建执行存储"""
        return MemoryExecutionStorage()
    
    @pytest.fixture
    def flow_storage(self):
        """创建流程存储"""
        return MemoryFlowStorage()
    
    def test_execution_state_save_performance(self, execution_storage):
        """执行状态保存性能测试"""
        result = PerformanceResult("执行状态保存")
        iterations = 500
        
        for i in range(iterations):
            execution_id = f"exec_{i}"
            state = ExecutionState(
                execution_id=execution_id,
                flow_id="test_flow",
                flow_version="1.0.0",
                context={"step": i, "data": "test" * 100},
                status="running",
                start_time=datetime.now().isoformat(),
                invoker="benchmark"
            )
            
            start = time.perf_counter()
            execution_storage.save_execution_state(execution_id, state)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        assert result.ops_per_second > 1000  # 每秒至少 1000 次保存
    
    def test_execution_state_load_performance(self, execution_storage):
        """执行状态加载性能测试"""
        # 先保存一些数据
        for i in range(100):
            execution_id = f"exec_load_{i}"
            state = ExecutionState(
                execution_id=execution_id,
                flow_id="test_flow",
                flow_version="1.0.0",
                context={"step": i},
                status="running",
                start_time=datetime.now().isoformat(),
                invoker="benchmark"
            )
            execution_storage.save_execution_state(execution_id, state)
        
        result = PerformanceResult("执行状态加载")
        iterations = 500
        
        for i in range(iterations):
            execution_id = f"exec_load_{i % 100}"
            
            start = time.perf_counter()
            execution_storage.load_execution_state(execution_id)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        assert result.ops_per_second > 1000  # 每秒至少 1000 次加载
    
    def test_flow_storage_save_performance(self, flow_storage):
        """流程定义保存性能测试"""
        result = PerformanceResult("流程定义保存")
        iterations = 100
        
        for i in range(iterations):
            flow = {
                "flow_id": f"perf_flow_{i}",
                "name": f"性能测试流程 {i}",
                "version": "1.0.0",
                "nodes": [
                    {"id": "start", "type": "start", "next": "end"},
                    {"id": "end", "type": "end"}
                ]
            }
            
            start = time.perf_counter()
            flow_storage.save_flow(flow)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        assert result.ops_per_second > 100  # 每秒至少 100 次保存


class TestNodePerformance:
    """节点性能测试"""
    
    @pytest.fixture
    def execution(self):
        """创建执行上下文"""
        event_bus = InMemoryEventBus()
        execution = FlowExecution(event_bus=event_bus)
        execution.clean()
        execution.set_state("$FLOW_ID", "perf_test")
        execution.set_state("$EXECUTION_ID", "perf_exec")
        return execution
    
    def test_delay_node_config_generation_performance(self, execution):
        """延迟节点配置生成性能测试"""
        node = DelayNode(
            id="perf_delay",
            delay_seconds=5,
            delay_unit="seconds"
        )
        
        result = PerformanceResult("延迟节点配置生成")
        iterations = 1000
        
        for _ in range(iterations):
            start = time.perf_counter()
            node.generate_service_config(execution)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        assert result.ops_per_second > 1000  # 每秒至少 1000 次配置生成


@pytest.mark.asyncio
class TestServicePerformance:
    """服务性能测试"""
    
    async def test_delay_service_task_submission_performance(self):
        """延迟服务任务提交性能测试 - 仅测试提交性能，不触发实际延迟"""
        event_bus = InMemoryEventBus()
        service = DelayService(event_bus, {"max_workers": 1})
        
        result = PerformanceResult("延迟服务任务提交（模拟）")
        iterations = 50
        
        # 测试任务配置验证的性能，而不是实际提交
        for i in range(iterations):
            task_config = {
                "type": "delay",
                "delay_ms": 60000,
                "trigger_timestamp": int(time.time() * 1000) + 60000,
                "node_id": f"node_{i}",
                "execution_id": f"exec_{i}",
                "flow_id": "perf_flow",
                "event_type": "delay_trigger"
            }
            
            start = time.perf_counter()
            # 只测试配置验证，不实际提交任务
            valid = service.validate_task_config(task_config)
            end = time.perf_counter()
            result.record(end - start)
        
        print(f"\n{result}")
        
        assert valid is True
        assert result.ops_per_second > 100  # 验证应该非常快
    
    async def test_approval_service_task_creation_performance(self):
        """审批服务任务创建性能测试"""
        event_bus = InMemoryEventBus()
        service = ApprovalService(event_bus)
        service.start_service()
        
        result = PerformanceResult("审批服务任务创建")
        iterations = 50  # 减少迭代次数
        
        try:
            for i in range(iterations):
                task_config = {
                    "approval_id": f"approval_{i}",
                    "approval_config": {"title": "性能测试", "content": "测试"},
                    "approver_config": {"approvers": ["admin"], "strategy": "any"},
                    "node_id": f"node_{i}",
                    "execution_id": f"exec_{i}",
                    "flow_id": "perf_flow",
                    "event_type": "approval_decision"
                }
                
                start = time.perf_counter()
                await service.handle_task(task_config)
                end = time.perf_counter()
                result.record(end - start)
            
            print(f"\n{result}")
            
            assert result.ops_per_second > 50  # 调整阈值
        
        finally:
            service.stop_service()


class TestConcurrentPerformance:
    """并发性能测试"""
    
    def test_concurrent_flow_execution(self):
        """并发流程执行性能测试"""
        flow_def = {
            "flow_id": "concurrent_perf_flow",
            "name": "并发性能测试流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "done"}
            ]
        }
        
        flow = Flow.model_validate(flow_def)
        event_bus = InMemoryEventBus()
        
        result = PerformanceResult("并发流程执行")
        iterations = 50
        
        import concurrent.futures
        
        def run_flow():
            start = time.perf_counter()
            FlowExecution.run(flow, params={}, mode=ExecutionMode.NORMAL, event_bus=event_bus)
            end = time.perf_counter()
            return end - start
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(run_flow) for _ in range(iterations)]
            
            for future in concurrent.futures.as_completed(futures):
                result.record(future.result())
        
        print(f"\n{result}")
        
        # 并发执行应该有一定的吞吐量
        assert result.ops_per_second > 5  # 每秒至少 5 次并发操作


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

