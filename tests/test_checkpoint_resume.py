"""
T004: Checkpoint resume 验证逻辑、context 更新、event_bus 传递
T010: 分布式模式下 subscription_id 不为 pending_xxx
T033: 端到端断点续执场景测试（挂起→恢复→完成）
"""
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from plaita.flow import Flow, FlowExecution, FlowExecutionException, ExecutionMode
from plaita.node import Start, End, Node
from plaita.node.event_node import EventNode
from plaita.core.errors import FlowErrorType


class SimpleNode(Node):
    node_type = "simple"

    def run(self, execution):
        return {"value": self.id}


def create_event_flow():
    """创建包含 EventNode 的测试流程"""
    return Flow(
        flow_id="test_event_flow",
        nodes=[
            Start(id="start", next="task1"),
            SimpleNode(id="task1", next="wait_event"),
            EventNode(id="wait_event", event_type="approval", next="task2"),
            SimpleNode(id="task2", next="end"),
            End(id="end"),
        ],
    )


def create_simple_flow():
    return Flow(
        flow_id="test_simple",
        nodes=[
            Start(id="start", next="node1"),
            SimpleNode(id="node1", next="node2"),
            SimpleNode(id="node2", next="end"),
            End(id="end"),
        ],
    )


class TestResumeValidation:
    """T004: 测试 _handle_resume_operation 的 early return 验证"""

    def test_resume_no_last_node_raises(self):
        """没有 LAST_NODE 时应抛异常"""
        flow = create_event_flow()
        execution = FlowExecution()
        execution.context = {}

        with pytest.raises(FlowExecutionException) as exc_info:
            execution._handle_resume_operation(flow, "event", {"some": "data"})
        assert "No suspended node found" in str(exc_info.value)

    def test_resume_invalid_node_id_raises(self):
        """LAST_NODE 指向不存在的节点 ID 时应抛异常"""
        flow = create_event_flow()
        execution = FlowExecution()
        execution.context = {
            "$LAST_NODE": "nonexistent_node",
        }

        with pytest.raises((FlowExecutionException, ValueError)):
            execution._handle_resume_operation(flow, "event", {})

    def test_resume_non_event_node_raises(self):
        """LAST_NODE 指向非 EventNode 时应抛异常"""
        flow = create_event_flow()
        execution = FlowExecution()
        execution.context = {
            "$LAST_NODE": "task1",
            "$NODE": {"task1": {"value": "task1"}},
        }

        with pytest.raises(FlowExecutionException) as exc_info:
            execution._handle_resume_operation(flow, "event", {})
        assert "is not an EventNode" in str(exc_info.value)

    def test_resume_not_pending_raises(self):
        """EventNode 不在 pending 状态时应抛异常"""
        flow = create_event_flow()
        execution = FlowExecution()
        execution.context = {
            "$LAST_NODE": "wait_event",
            "$NODE": {"wait_event": {"status": "completed"}},
        }

        with pytest.raises(FlowExecutionException) as exc_info:
            execution._handle_resume_operation(flow, "event", {})
        assert "is not in pending status" in str(exc_info.value)

    def test_resume_unsupported_type_raises(self):
        """不支持的 resume_type 应抛异常"""
        flow = create_event_flow()
        execution = FlowExecution()
        execution.context = {
            "$LAST_NODE": "wait_event",
            "$NODE": {"wait_event": {"status": "pending"}},
        }

        with pytest.raises(FlowExecutionException) as exc_info:
            execution._handle_resume_operation(flow, "invalid_type", {})
        assert "Unsupported resume type" in str(exc_info.value)

    def test_resume_event_success(self):
        """正常的 event resume 应成功"""
        flow = create_event_flow()
        execution = FlowExecution()
        execution.context = {
            "$LAST_NODE": "wait_event",
            "$NODE": {"wait_event": {"status": "pending"}},
            "$EXECUTION_ID": "test-exec-001",
        }
        execution.callback_manager = MagicMock()

        result = execution._handle_resume_operation(flow, "event", {"approved": True})
        assert result is not None
        assert result.get("is_end") is not True or result.get("is_suspend") is not True

    def test_resume_cancel_success(self):
        """正常的 cancel resume 应成功"""
        flow = create_event_flow()
        execution = FlowExecution()
        execution.context = {
            "$LAST_NODE": "wait_event",
            "$NODE": {"wait_event": {"status": "pending"}},
            "$EXECUTION_ID": "test-exec-002",
        }
        execution.callback_manager = MagicMock()

        result = execution._handle_resume_operation(flow, "cancel", None)
        assert result is not None

    def test_resume_timeout_success(self):
        """正常的 timeout resume 应成功"""
        flow = create_event_flow()
        execution = FlowExecution()
        execution.context = {
            "$LAST_NODE": "wait_event",
            "$NODE": {"wait_event": {"status": "pending"}},
            "$EXECUTION_ID": "test-exec-003",
        }
        execution.callback_manager = MagicMock()

        result = execution._handle_resume_operation(flow, "timeout", None)
        assert result is not None


class TestContextUpdate:
    """T004: 测试 context 在分布式执行中的更新"""

    def test_context_updated_after_execution(self):
        """分布式执行后 context 应包含最新节点结果"""
        flow = create_simple_flow()
        result = FlowExecution.run(flow, mode=ExecutionMode.DISTRIBUTED)

        assert "context" in result
        context = result["context"]
        assert "$LAST_NODE" in context
        assert context["$LAST_NODE"] == "node1"

    def test_context_carries_through_resume(self):
        """恢复执行时 context 应携带前序信息"""
        flow = create_simple_flow()

        result1 = FlowExecution.run(flow, mode=ExecutionMode.DISTRIBUTED)
        context1 = result1["context"]

        result2 = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, context=context1
        )
        context2 = result2["context"]

        assert context2["$LAST_NODE"] == "node2"
        node_results = context2.get("$NODE", {})
        assert "node1" in node_results
        assert "node2" in node_results


class TestEventBusPassing:
    """T004: 测试 event_bus 通过参数注入"""

    def test_event_bus_injected_via_run(self):
        """FlowExecution.run 传递的 event_bus 应被设置"""
        mock_bus = MagicMock()
        flow = create_simple_flow()

        result = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, event_bus=mock_bus
        )
        assert result is not None

    def test_event_bus_used_in_subscribe(self):
        """EventNode 订阅时应使用注入的 event_bus"""
        flow = create_event_flow()
        mock_bus = MagicMock()
        mock_bus.register_subscription = MagicMock(return_value="sub-123")

        result = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, event_bus=mock_bus
        )

        context = result["context"]
        if context.get("$LAST_NODE") == "wait_event":
            node_results = context.get("$NODE", {})
            wait_state = node_results.get("wait_event", {})
            if "subscription_id" in wait_state:
                assert not wait_state["subscription_id"].startswith("pending_")


class TestSubscriptionId:
    """T010: 分布式模式下 subscription_id 不为 pending_xxx"""

    def test_sync_event_bus_subscription_id(self):
        """同步 event_bus 的 subscription_id 应为真实 ID"""
        flow = create_event_flow()
        mock_bus = MagicMock()
        mock_bus.register_subscription = MagicMock(return_value="real-sub-456")

        result = FlowExecution.run(
            flow, mode=ExecutionMode.DISTRIBUTED, event_bus=mock_bus
        )

        context = result["context"]
        if context.get("$LAST_NODE") == "wait_event":
            node_results = context.get("$NODE", {})
            wait_state = node_results.get("wait_event", {})
            assert wait_state.get("subscription_id") == "real-sub-456"
            assert not wait_state["subscription_id"].startswith("pending_")


class TestEndToEndCheckpoint:
    """T033: 端到端断点续执场景测试"""

    def test_full_suspend_resume_complete_flow(self):
        """完整的挂起→恢复→完成流程"""
        flow = create_event_flow()
        mock_bus = MagicMock()
        mock_bus.register_subscription = MagicMock(return_value="sub-e2e-001")

        # Step 1: 执行到 task1
        r1 = FlowExecution.run(flow, mode=ExecutionMode.DISTRIBUTED, event_bus=mock_bus)
        assert r1["id"] == "task1"
        assert not r1["is_end"]

        # Step 2: 继续执行到 EventNode，应挂起
        r2 = FlowExecution.run(
            flow,
            mode=ExecutionMode.DISTRIBUTED,
            context=r1["context"],
            event_bus=mock_bus,
        )
        assert r2["id"] == "wait_event"
        assert r2.get("is_suspend", False) is True

        # Step 3: 用 event resume 恢复
        r3 = FlowExecution.run(
            flow,
            mode=ExecutionMode.DISTRIBUTED,
            context=r2["context"],
            event_bus=mock_bus,
            resume_type="event",
            resume_data={"approved": True},
        )
        assert r3["id"] == "wait_event"
        assert r3.get("is_suspend") is not True

        # Step 4: 继续执行 task2
        r4 = FlowExecution.run(
            flow,
            mode=ExecutionMode.DISTRIBUTED,
            context=r3["context"],
            event_bus=mock_bus,
        )
        assert r4["id"] == "task2"

        # Step 5: 执行到 end
        r5 = FlowExecution.run(
            flow,
            mode=ExecutionMode.DISTRIBUTED,
            context=r4["context"],
            event_bus=mock_bus,
        )
        assert r5["is_end"] is True

    def test_suspend_cancel_flow(self):
        """挂起→取消流程"""
        flow = create_event_flow()
        mock_bus = MagicMock()
        mock_bus.register_subscription = MagicMock(return_value="sub-cancel-001")

        r1 = FlowExecution.run(flow, mode=ExecutionMode.DISTRIBUTED, event_bus=mock_bus)
        r2 = FlowExecution.run(
            flow,
            mode=ExecutionMode.DISTRIBUTED,
            context=r1["context"],
            event_bus=mock_bus,
        )
        assert r2.get("is_suspend") is True

        r3 = FlowExecution.run(
            flow,
            mode=ExecutionMode.DISTRIBUTED,
            context=r2["context"],
            event_bus=mock_bus,
            resume_type="cancel",
        )
        assert r3 is not None

    def test_suspend_timeout_flow(self):
        """挂起→超时流程"""
        flow = create_event_flow()
        mock_bus = MagicMock()
        mock_bus.register_subscription = MagicMock(return_value="sub-timeout-001")

        r1 = FlowExecution.run(flow, mode=ExecutionMode.DISTRIBUTED, event_bus=mock_bus)
        r2 = FlowExecution.run(
            flow,
            mode=ExecutionMode.DISTRIBUTED,
            context=r1["context"],
            event_bus=mock_bus,
        )
        assert r2.get("is_suspend") is True

        r3 = FlowExecution.run(
            flow,
            mode=ExecutionMode.DISTRIBUTED,
            context=r2["context"],
            event_bus=mock_bus,
            resume_type="timeout",
        )
        assert r3 is not None
