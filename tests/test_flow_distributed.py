import json
import pytest
from unittest.mock import MagicMock

from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution
from plaita.core.errors import FlowExecutionException
from plaita.node import Start, End, Node, Switch
from plaita.core.errors import FlowErrorType


class SimpleNode(Node):
    node_type = "simple"

    def run(self, execution):
        return self.id


class ErrorNode(Node):
    node_type = "error"

    def run(self, execution):
        raise Exception("Test error")


def create_test_flow():
    """创建测试用的流程"""
    return Flow(
        flow_id="test_flow",
        nodes=[
            Start(id="start", next="node1"),
            SimpleNode(id="node1", next="node2"),
            SimpleNode(id="node2", next="end"),
            End(id="end")
        ]
    )


def create_branch_flow():
    """创建带分支的测试流程"""
    return Flow(
        flow_id="branch_flow",
        nodes=[
            Start(id="start", next="branch"),
            Switch(
                id="branch",
                branches=[
                    {"name": "branch1", "next": "node1", "condition": {"field": "1", "operator": "eq", "value": "1"}},
                    {"name": "branch2", "next": "node2", "condition": {"field": "1", "operator": "eq", "value": "2"}}
                ]
            ),
            SimpleNode(id="node1", next="end"),
            SimpleNode(id="node2", next="end"),
            End(id="end")
        ]
    )


def create_error_flow():
    """创建会产生错误的测试流程"""
    return Flow(
        flow_id="error_flow",
        nodes=[
            Start(id="start", next="error_node"),
            ErrorNode(id="error_node", next="end"),
            End(id="end")
        ]
    )


class TestFlowDistributed:
    def setup_method(self):
        self.execution = FlowExecution()
        # Mock callback methods to avoid actual callbacks
        self.execution.callback_manager = MagicMock()

    def test_new_flow_execution(self):
        """测试新流程执行"""
        flow = create_test_flow()
        result = self.execution._run_distributed(flow)
        
        assert result["id"] == "node1"
        assert result["result"] == "node1"
        assert not result["is_end"]
        assert "context" in result
        
        # 验证context中包含了必要的信息
        context = result["context"]
        assert f"{self.execution.express_prefix}LAST_NODE" in context
        assert context[f"{self.execution.express_prefix}LAST_NODE"] == "node1"

    def test_resume_flow_execution(self):
        """测试从上下文恢复执行"""
        flow = create_test_flow()
        
        # 首先执行到第一个节点
        first_result = self.execution._run_distributed(flow)
        context = first_result["context"]
        
        # 使用上下文恢复执行
        new_execution = FlowExecution()
        resume_result = new_execution._run_distributed(flow, context=context)
        
        assert resume_result["id"] == "node2"
        assert resume_result["result"] == "node2"
        assert not resume_result["is_end"]

    def test_branch_flow_execution(self):
        """测试分支流程执行"""
        flow = create_branch_flow()
        
        # 执行到分支节点
        result = self.execution._run_distributed(flow)
        assert result["id"] == "branch"
        assert result["branch"] == "node1"  # Switch节点返回下一个节点的ID
        
        # 使用上下文继续执行
        new_execution = FlowExecution()
        resume_result = new_execution._run_distributed(flow, context=result["context"])
        
        assert resume_result["id"] == "node1"
        assert resume_result["result"] == "node1"

    def test_flow_completion(self):
        """测试流程完整执行到结束"""
        flow = create_test_flow()
        
        # 执行第一个节点
        result1 = self.execution._run_distributed(flow)
        assert not result1["is_end"]
        
        # 执行第二个节点
        result2 = self.execution._run_distributed(flow, context=result1["context"])
        assert not result2["is_end"]
        
        # 执行到结束节点
        result3 = self.execution._run_distributed(flow, context=result2["context"])
        assert result3["is_end"]
        assert result3["type"] == End.node_type

    def test_error_handling(self):
        """测试错误处理"""
        flow = create_error_flow()
        
        with pytest.raises(FlowExecutionException) as exc_info:
            self.execution._run_distributed(flow)
        
        assert exc_info.value.error_type == FlowErrorType.FLOW_ERROR
        assert exc_info.value.code == -500

    def test_timeout_handling(self):
        """测试超时设置"""
        flow = create_test_flow()
        flow.timeout = "PT0.1S"  # 设置100ms超时
        
        result = self.execution._run_distributed(flow, timeout=200)  # 使用更大的超时时间
        assert result["id"] == "node1"
        
        # 验证使用了较小的超时时间
        assert self.execution._parse_timeout(flow.timeout) == 100

    def test_empty_flow(self):
        """测试空流程"""
        flow = Flow(flow_id="empty_flow", nodes=[])
        
        result = self.execution._run_distributed(flow)
        assert result["is_end"]
        assert result["result"] == {}

    def test_invalid_context(self):
        """测试无效的上下文"""
        flow = create_test_flow()
        invalid_context = {"invalid": "context"}
        
        # 即使上下文无效，也应该从头开始执行
        result = self.execution._run_distributed(flow, context=invalid_context)
        assert result["id"] == "node1"
        assert result["result"] == "node1" 