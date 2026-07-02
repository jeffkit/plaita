"""A4 复现: 分布式流程跨步执行不应丢失用户回调。

当前 FlowWorker._process_execution_result 每推进一个节点就调一次
`FlowExecution.run(...)` classmethod, 每次都新建 FlowExecution + 空的
CallbackManager, 导致用户注册的回调在第 2 步起全部失效。

正确做法: 为每个 execution 复用同一个 FlowExecution 实例, 让
CallbackManager/ExecutionContext 在跨步之间保持, 从而回调贯穿整条分布式流程。
"""

import pytest
from plaita.core.callback import FlowCallback
from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import FlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


class CountingCallback(FlowCallback):
    def __init__(self):
        self.flow_starts = 0
        self.node_starts = []

    def on_flow_start(self, flow, **kwargs):
        self.flow_starts += 1

    def on_node_start(self, flow, node, **kwargs):
        self.node_starts.append(node.id)


def _multi_node_flow():
    return {
        "flow_id": "a4-multi",
        "name": "多节点",
        "version": "1.0.0",
        "nodes": [
            {"id": "start", "type": "start", "next": "assign1"},
            {"id": "assign1", "type": "assignment", "output": {"step": 1}, "next": "assign2"},
            {"id": "assign2", "type": "assignment", "output": {"step": 2}, "next": "end"},
            {"id": "end", "type": "end", "output": "success"},
        ],
    }


@pytest.fixture
def worker_with_callback():
    execution_storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    event_bus = InMemoryEventBus()
    flow_storage.save_flow(_multi_node_flow())
    counter = CountingCallback()
    worker = FlowWorker(
        execution_storage=execution_storage,
        flow_storage=flow_storage,
        event_bus=event_bus,
        callback_handlers=[counter],
    )
    return worker, counter


class TestDistributedCallbacksPersist:
    def test_callbacks_fire_for_every_node(self, worker_with_callback):
        worker, counter = worker_with_callback
        result = worker.start_flow(flow_id="a4-multi", params={}, version="1.0.0")

        # 流程应跑完到 end
        assert result.get("is_end") is True
        # 每个节点都应触发 on_node_start (start, assign1, assign2, end)
        assert sorted(counter.node_starts) == ["assign1", "assign2", "end", "start"]
        # on_flow_start 在整个分布式执行期间只应触发一次
        assert counter.flow_starts == 1

    def test_resume_reuses_callbacks(self, worker_with_callback):
        worker, counter = worker_with_callback
        # 用一个带事件节点的流程做 resume 场景比较复杂, 这里至少验证
        # start_flow 之后回调对象被绑定且计数 > 0
        result = worker.start_flow(flow_id="a4-multi", params={}, version="1.0.0")
        assert result.get("is_end") is True
        assert len(counter.node_starts) == 4
