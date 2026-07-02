import os
import time
from typing import ClassVar
from unittest import TestCase
from unittest.mock import Mock

from plaita.core import types
from plaita.core.errors import FlowErrorType, FlowExecutionException
from plaita.core.flow import Flow, parse_and_run
from plaita.core.executor import FlowExecution
from plaita.io import Property, match
from plaita.node import Assignment, End, Node, Start


class FlowTestCase(TestCase):

    def test_echo(self):
        flow = """
{
    "id": "echo",
    "version": "0.1",
    "runtime": "python",
    "inputType": {
        "name": "name",
        "label": "姓名",
        "dataType": "object",
        "default": "",
        "required": true
    },
    "outputType": {
        "dataType": "string"
    },
    "nodes": [
        {
            "type": "start",
            "label": "开始",
            "id": "start",
            "next": "end"
        },
        {
            "type": "end",
            "label": "结束",
            "id": "end",
            "resultType": "success",
            "output": "$INPUT.name"
        }
    ]
}
        """
        result = parse_and_run(flow, {"name": "KongJie"})
        self.assertEqual(result, "KongJie")

    def test_property_match_simple(self):
        # 判断值是否符合结构，结构允许空时，值可以没有。
        flow = Flow(
            flow_id="test",
            version="00",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                name="user",
                is_required=True,
                children={
                    "name": Property(name="name", data_type=types.STRING, is_required=True),
                    "age": Property(name="age", data_type=types.INTEGER),
                },
            ),
            output_type=Property(data_type=types.INTEGER, name="code", is_required=True),
            nodes=[
                Start(id="start", name="开始", next="end"),
                End(id="end", **{"resultType": "success", "output": 0}, label="结束"),
            ],
        )
        self.assertFalse(match(flow.input_type, 1))
        self.assertTrue(match(flow.input_type, {"name": "kongJie", "age": 19}))
        self.assertTrue(match(flow.input_type, {"name": "kongJie"}))  # age为可选值，允许空

        self.assertTrue(match(flow.output_type, 0))
        self.assertFalse(match(flow.output_type, False))
        self.assertFalse(match(flow.output_type, "OK"))

    def test_property_match_array(self):
        # 值类型检测，数组
        flow = Flow(
            flow_id="test1",
            version="01",
            runtime="python",
            input_type=Property(
                data_type=types.ARRAY,
                name="users",
                is_required=True,
                item_type=Property(
                    data_type=types.OBJECT,
                    name="item",
                    children={
                        "name": Property(name="name", data_type=types.STRING, is_required=True),
                        "age": Property(name="age", data_type=types.INTEGER),
                    },
                ),
            ),
            output_type=Property(
                data_type=types.ARRAY,
                name="vip_info",
                is_required=True,
                children=[
                    Property(name="name", data_type=types.STRING, is_required=True),
                    Property(name="age", data_type=types.INTEGER),
                ],
            ),
            nodes=[
                Start(id="start", name="开始", next="end"),
                End(id="end", **{"resultType": "success", "output": ["kongJie", 28]}, label="结束"),
            ],
        )
        self.assertFalse(match(flow.input_type, 2))
        self.assertTrue(match(flow.input_type, [{"name": "kongJie", "age": 19}]))
        self.assertTrue(match(flow.input_type, [{"name": "kongJie"}]))  # age为可选值，允许空

        self.assertFalse(match(flow.output_type, 0))
        self.assertFalse(match(flow.output_type, False))
        self.assertFalse(match(flow.output_type, "OK"))
        self.assertTrue(match(flow.output_type, ["kongJie", None]))
        self.assertTrue(match(flow.output_type, ["kongJie", 28]))

    def test_global_context(self):
        # 全局上下文
        flow = Flow(
            flow_id="test2",
            version="02",
            runtime="python",
            global_context={"name": "kongjie", "age": 19},
            output_type=Property(data_type=types.INTEGER, name="code", is_required=True),
            nodes=[
                Start(id="start", name="开始", next="end"),
                End(id="end", **{"resultType": "success", "output": "$GLOBAL.name"}, label="结束"),
            ],
        )
        self.assertEqual(flow.run(), "kongjie")

    def test_debug_flow(self):
        # 全局上下文
        flow = Flow(
            flow_id="test3",
            version="03",
            runtime="python",
            global_context={"name": "kongjie", "age": 19},
            output_type=Property(data_type=types.STRING, name="name", is_required=True),
            nodes=[
                Start(id="start", name="开始", next="assign"),
                Assignment(
                    id="assign",
                    name="赋值",
                    next="end",
                    outputType=Property(data_type=types.STRING),
                    output="$GLOBAL.name",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.assign"}, label="结束"),
            ],
        )
        gen = flow.debug()
        start_node = next(gen)
        self.assertEqual(start_node["type"], Start.node_type)
        self.assertEqual(start_node["id"], "start")
        assign_node = next(gen)
        self.assertEqual(assign_node["type"], Assignment.node_type)
        self.assertEqual(assign_node["id"], "assign")
        self.assertEqual(assign_node["result"], "kongjie")
        end_node = next(gen)
        self.assertEqual(end_node["type"], End.node_type)
        self.assertEqual(end_node["id"], "end")
        self.assertEqual(end_node["result"], "kongjie")

    def test_environment_variable(self):
        # ``$ENV`` 默认空（2026-07 安全模型），flow 必须显式 ``expose_env``
        # 才能读到环境变量。
        flow = Flow(
            flow_id="test4",
            version="04",
            runtime="python",
            output_type=Property(data_type=types.INTEGER, name="code", is_required=True),
            expose_env=["HOME"],
            nodes=[
                Start(id="start", name="开始", next="end"),
                End(id="end", **{"resultType": "success", "output": "$ENV.HOME"}, label="结束"),
            ],
        )
        self.assertEqual(flow.run(), os.environ.get("HOME"))


class ProcessNodeTestCase(TestCase):
    def setUp(self):
        self.flow = Flow(flow_id="test", version="1.0", runtime="python")
        self.execution = FlowExecution()
        self.mock_node = Mock(spec=Node)
        self.mock_node.id = "test_node"
        self.mock_node.name = "Test Node"
        self.mock_node.branching = False
        self.mock_node.timeout = None
        self.mock_node.error_handler = None
        self.mock_node.timeout_handler = None

    def test_normal_execution(self):
        """测试正常执行情况"""
        self.mock_node.run.return_value = "success"
        result, branch = self.execution._process_node(self.flow, self.mock_node, False)

        self.assertEqual(result, "success")
        self.assertIsNone(branch)
        self.mock_node.run.assert_called_once()

    def test_timeout_execution(self):
        """测试执行超时情况"""

        def slow_run(execution):
            time.sleep(2)
            return "success"

        self.mock_node.run.side_effect = slow_run
        self.mock_node.timeout = "1000"  # 设置1秒超时

        with self.assertRaises(FlowExecutionException) as context:
            self.execution._process_node(self.flow, self.mock_node, False)

        self.assertEqual(context.exception.error_type, FlowErrorType.NODE_ERROR)
        self.assertEqual(context.exception.code, -1)
        self.assertTrue("timeout" in str(context.exception))

    def test_timeout_with_handler(self):
        """测试执行超时但有超时处理器的情况"""

        def slow_run(execution):
            time.sleep(2)
            return "success"

        self.mock_node.run.side_effect = slow_run
        self.mock_node.timeout = "PT1S"
        self.mock_node.timeout_handler = Mock()
        self.mock_node.timeout_handler.handle.return_value = "timeout_handled"

        result, branch = self.execution._process_node(self.flow, self.mock_node, False)

        self.assertEqual(result, "timeout_handled")
        self.mock_node.timeout_handler.handle.assert_called_once()

    def test_timeout_with_abort_handler(self):
        """测试执行超时且超时处理器策略为abort的情况"""

        def slow_run(execution):
            time.sleep(2)
            return "success"

        self.mock_node.run.side_effect = slow_run
        self.mock_node.timeout = "1000"  # 1秒超时
        self.mock_node.timeout_handler = Mock()
        self.mock_node.timeout_handler.handle.side_effect = TimeoutError()

        with self.assertRaises(FlowExecutionException) as context:
            self.execution._process_node(self.flow, self.mock_node, False)

        self.assertEqual(context.exception.error_type, FlowErrorType.NODE_ERROR)
        self.assertEqual(context.exception.code, -1)
        self.assertTrue("abort" in str(context.exception))
        self.mock_node.timeout_handler.handle.assert_called_once()

    def test_retry_on_error(self):
        """测试执行失败重试机制"""
        self.mock_node.error_handler = Mock()
        self.mock_node.error_handler.retry_times = 2
        self.mock_node.error_handler.strategy = "abort"

        # 模拟前两次失败，第三次成功
        self.mock_node.run.side_effect = [Exception("First failure"), Exception("Second failure"), "success"]

        result, branch = self.execution._process_node(self.flow, self.mock_node, False)

        self.assertEqual(result, "success")
        self.assertEqual(self.mock_node.run.call_count, 3)

    def test_retry_exhausted(self):
        """测试重试次数耗尽的情况"""
        self.mock_node.error_handler = Mock()
        self.mock_node.error_handler.retry_times = 2
        self.mock_node.error_handler.strategy = "abort"
        self.mock_node.error_handler.error_code = -520
        self.mock_node.error_handler.error_message = "All retries failed"

        # 模拟所有尝试都失败
        self.mock_node.run.side_effect = Exception("Persistent failure")

        with self.assertRaises(FlowExecutionException) as context:
            self.execution._process_node(self.flow, self.mock_node, False)

        self.assertEqual(context.exception.error_type, FlowErrorType.NODE_ERROR)
        self.assertEqual(context.exception.code, -520)
        self.assertEqual(self.mock_node.run.call_count, 3)  # 初始尝试 + 2次重试

    def test_error_continue_strategy(self):
        """测试错误继续策略"""
        self.mock_node.error_handler = Mock()
        self.mock_node.error_handler.retry_times = 0
        self.mock_node.error_handler.strategy = "continue"

        self.mock_node.run.side_effect = Exception("Failure")

        result, branch = self.execution._process_node(self.flow, self.mock_node, False)

        self.assertIsNone(result)
        self.mock_node.run.assert_called_once()

    def test_error_continue_with_strategy(self):
        """测试错误继续并返回默认值策略"""
        self.mock_node.error_handler = Mock()
        self.mock_node.error_handler.retry_times = 0
        self.mock_node.error_handler.strategy = "continue-with"
        self.mock_node.error_handler.default_value = "default_result"

        self.mock_node.run.side_effect = Exception("Failure")

        result, branch = self.execution._process_node(self.flow, self.mock_node, False)

        self.assertEqual(result, "default_result")
        self.mock_node.run.assert_called_once()

    def test_branching_node(self):
        """测试分支节点"""
        self.mock_node.branching = True
        self.mock_node.run.return_value = "branch_a"

        result, branch = self.execution._process_node(self.flow, self.mock_node, False)

        self.assertEqual(result, "branch_a")
        self.assertEqual(branch, "branch_a")


class DelayNode(Node):
    """用于测试的延时节点"""

    node_type: ClassVar[str] = "delay"
    node_name: ClassVar[str] = "延时"
    execute_time: float = 0

    def run(self, execution):
        time.sleep(self.execute_time)
        return f"executed after {self.execute_time}s"


class FlowTimeoutTestCase(TestCase):
    def test_flow_timeout(self):
        """测试整个流程的超时控制"""
        flow = Flow(
            flow_id="test_timeout",
            version="1.0",
            runtime="python",
            timeout="1000",  # 1秒超时
            output_type=Property(data_type=types.STRING, name="result", is_required=True),
            nodes=[
                Start(id="start", name="开始", next="slow"),
                DelayNode(id="slow", name="慢节点", next="end", execute_time=2.0),  # 该节点会执行2秒
                End(id="end", name="结束", **{"resultType": "success", "output": "$NODE.slow"}),
            ],
        )

        with self.assertRaises(FlowExecutionException) as context:
            flow.run()

        self.assertEqual(context.exception.error_type, FlowErrorType.FLOW_ERROR)
        self.assertEqual(context.exception.code, -1)
        self.assertTrue("timeout" in str(context.exception).lower())

    def test_flow_timeout_with_multiple_nodes(self):
        """测试多节点场景下的流程超时控制"""
        flow = Flow(
            flow_id="test_timeout_multiple",
            version="1.0",
            runtime="python",
            timeout="1000",  # 1秒超时
            output_type=Property(data_type=types.STRING, name="result", is_required=True),
            nodes=[
                Start(id="start", name="开始", next="node1"),
                DelayNode(id="node1", name="节点1", next="node2", execute_time=0.5),
                DelayNode(id="node2", name="节点2", next="node3", execute_time=0.5),
                DelayNode(id="node3", name="节点3", next="end", execute_time=0.5),
                End(id="end", name="结束", **{"resultType": "success", "output": "$NODE.node3"}),
            ],
        )

        with self.assertRaises(FlowExecutionException) as context:
            flow.run()

        self.assertEqual(context.exception.error_type, FlowErrorType.FLOW_ERROR)
        self.assertEqual(context.exception.code, -1)
        self.assertTrue("timeout" in str(context.exception).lower())

    def test_flow_completes_within_timeout(self):
        """测试在超时时间内完成的流程"""
        flow = Flow(
            flow_id="test_timeout_success",
            version="1.0",
            runtime="python",
            timeout="1000",  # 1秒超时
            output_type=Property(data_type=types.STRING, name="result", is_required=True),
            nodes=[
                Start(id="start", name="开始", next="quick"),
                DelayNode(id="quick", name="快节点", next="end", execute_time=0.1),
                End(id="end", name="结束", **{"resultType": "success", "output": "$NODE.quick"}),
            ],
        )

        result = flow.run()
        self.assertTrue("executed after 0.1s" in result)

    def test_flow_timeout_with_iso_duration(self):
        """测试使用 ISO 8601 持续时间格式的超时设置"""
        flow = Flow(
            flow_id="test_iso_timeout",
            version="1.0",
            runtime="python",
            timeout="PT0.5S",  # 0.5秒超时
            output_type=Property(data_type=types.STRING, name="result", is_required=True),
            nodes=[
                Start(id="start", name="开始", next="slow"),
                DelayNode(id="slow", name="慢节点", next="end", execute_time=1.0),
                End(id="end", name="结束", **{"resultType": "success", "output": "$NODE.slow"}),
            ],
        )

        with self.assertRaises(FlowExecutionException) as context:
            flow.run()

        self.assertEqual(context.exception.error_type, FlowErrorType.FLOW_ERROR)
        self.assertEqual(context.exception.code, -1)


class ExecuteTestCase(TestCase):
    """Test cases for FlowExecution.execute method"""

    def setUp(self):
        self.execution = FlowExecution()

    def test_execute_with_dict_params(self):
        """Test execute method with dictionary parameters"""
        flow = Flow(
            flow_id="test-execute",
            version="1.0",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                is_required=True,
                children={
                    "name": Property(data_type=types.STRING, is_required=True),
                    "age": Property(data_type=types.INTEGER, is_required=True),
                },
            ),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="end"),
                End(id="end", **{"resultType": "success", "output": "$INPUT.name"}),
            ],
        )

        params = {"name": "KongJie", "age": 28}
        result = self.execution.execute(flow, params)
        self.assertEqual(result, "KongJie")

    def test_execute_with_none_params(self):
        """Test execute method with None parameters"""
        flow = Flow(
            flow_id="test-execute-none",
            version="1.0",
            runtime="python",
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="end"),
                End(id="end", **{"resultType": "success", "output": "success"}),
            ],
        )

        result = self.execution.execute(flow)
        self.assertEqual(result, "success")

    def test_execute_with_lazy_mode(self):
        """Test execute method in lazy (debug) mode"""
        flow = Flow(
            flow_id="test-execute-lazy",
            version="1.0",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, children={"input": Property(data_type=types.STRING)}),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="end"),
                End(id="end", **{"resultType": "success", "output": "$INPUT.input"}),
            ],
        )

        # 使用字典传入参数
        params = {"input": "test-input"}
        gen = self.execution.execute(flow, params, lazy=True)
        
        # Check start node
        start_node = next(gen)
        self.assertEqual(start_node["type"], "start")
        self.assertEqual(start_node["id"], "start")
        
        # Check end node
        end_node = next(gen)
        self.assertEqual(end_node["type"], "end")
        self.assertEqual(end_node["id"], "end")
        self.assertEqual(end_node["result"], "test-input")

    def test_execute_with_non_dict_params(self):
        """Test execute method with non-dictionary parameters"""
        flow = Flow(
            flow_id="test-execute-type",
            version="1.0",
            runtime="python",
            input_type=Property(data_type=types.STRING),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="end"),
                End(id="end", **{"resultType": "success", "output": "$INPUT"}),
            ],
        )

        # 测试非字典参数
        with self.assertRaises(TypeError) as cm:
            self.execution.execute(flow, "test-input")
        self.assertEqual(str(cm.exception), "params must be a dictionary or None")

    def test_execute_with_error_result(self):
        """Test execute method with error result"""
        flow = Flow(
            flow_id="test-execute-error",
            version="1.0",
            runtime="python",
            nodes=[
                Start(id="start", next="end"),
                End(id="end", **{"resultType": "error", "error": {"code": -400, "message": "test error"}}),
            ],
        )

        with self.assertRaises(FlowExecutionException) as cm:
            self.execution.execute(flow)
        self.assertEqual(cm.exception.error_type, FlowErrorType.ERROR_RESULT)
