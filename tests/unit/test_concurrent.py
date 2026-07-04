import json
from unittest import TestCase

from plaita.core.flow import Flow
from plaita.io import Property, types
from plaita.node import End, Start
from plaita.node.assignment import Assignment  # 添加这行导入
from plaita.node.concurrent import Parallel


class TestParallel(TestCase):

    def setUp(self):
        # Create a flow, which echoes the input
        self.flow_data = Flow(
            flow_id="test-echo",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.STRING, is_required=True),
            output_type=Property(data_type=types.STRING, is_required=True),
            nodes=[Start(id="start", next="end"), End(id="end", resultType="success", output="$INPUT")],
        )

        self.flow_data_str = """
{
    "id": "test-echo2",
    "version": "1",
    "runtime": "python",
    "inputType": {
        "data_type": "string",
        "required": true
    },
    "outputType": {
        "data_type": "string"
    },
    "nodes": [
        {
            "type": "start",
            "id": "start",
            "next": "end"
        },
        {
            "type": "end",
            "id": "end",
            "resultType": "success",
            "output": "$INPUT"
        }
    ]
}
"""

        self.flow_data_dict = json.loads(self.flow_data_str)

    def _test_pool_execute(self, pool_type="thread"):
        # Create a flow with the parallel node
        flow = Flow(
            flow_id="test-parallel",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel",
                    name="parallel",
                    branches=[
                        {"name": "branch0", "flow": self.flow_data, "input": "branch0"},
                        {"name": "branch1", "flow": self.flow_data_dict, "input": "branch1"},
                        {
                            "name": "branch2",
                            "flow": self.flow_data_str,
                            "input": "branch2",
                        },
                        {
                            "name": "branch3",
                            "flow": self.flow_data,
                            "input": "branch3",
                        },
                        {
                            "name": "branch4",
                            "flow": self.flow_data,
                            "input": "branch4",
                            "condition": {
                                "field": "branch4",
                                "operator": "in",
                                "value": ["branch1", "branch2", "branch3"],
                            },
                        },
                    ],
                    mode=pool_type,
                    join_branches=["branch0", "branch1", "branch2"],
                    next="end",
                    is_conditional=True,
                ),
                End(id="end", resultType="success", output="$NODE.parallel"),
            ],
        )
        # Run the flow
        result = flow.run()
        # Check the result
        self.assertEqual({"branch0": "branch0", "branch1": "branch1", "branch2": "branch2"}, result)

    # 测试兼容驼峰命名变量
    def _test_concurrent_in_camel_case(self, mode):
        flow = Flow(
            flow_id="test-parallel-camel-case",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel",
                    name="parallel",
                    branches=[
                        {"name": "branch1", "flow": self.flow_data, "input": "branch1"},
                        {
                            "name": "branch2",
                            "flow": self.flow_data,
                            "input": "branch2",
                            "condition": {"field": "branch2", "operator": "in", "value": ["branch1", "branch2"]},
                        },
                    ],
                    mode=mode,
                    joinBranches=["branch2"],
                    next="end",
                    isConditional=True,
                ),
                End(id="end", resultType="success", output="$NODE.parallel"),
            ],
        )
        result = flow.run()
        self.assertEqual({"branch2": "branch2"}, result)

    # 测试并行节点子流程的上下文
    def _test_concurrent_with_context(self, mode):
        flow = Flow(
            flow_id="test-parallel-camel-case",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.STRING, is_required=True),
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel",
                    name="parallel",
                    branches=[
                        {"name": "branch1", "flow": self.flow_data, "input": "$INPUT.input"},
                        {
                            "name": "branch2",
                            "flow": Flow(
                                flow_id="test-echo2",
                                version="1",
                                runtime="python",
                                input_type=Property(data_type=types.STRING, is_required=True),
                                output_type=Property(data_type=types.STRING, is_required=True),
                                nodes=[
                                    Start(id="start", next="end"),
                                    End(id="end", resultType="success", output="$PARENT.$INPUT.input"),
                                ],
                            ),
                        },
                    ],
                    mode=mode,
                    join_branches=["branch1", "branch2"],
                    next="end",
                    is_conditional=True,
                ),
                End(id="end", resultType="success", output="$NODE.parallel"),
            ],
        )
        result = flow.run({"input": "input_value"})
        self.assertEqual({"branch1": "input_value", "branch2": "input_value"}, result)

    def test_execute(self):
        # coroutine 模式已下线（sync 桥接里嵌套 asyncio.run_until_complete，
        # 任何 running 事件循环下必崩），不再覆盖。
        for mode in ["thread", "process"]:
            with self.subTest(mode=mode):
                self.setUp()
                self._test_pool_execute(mode)

    def test_context(self):
        for mode in ["thread", "process"]:
            with self.subTest(mode=mode):
                self.setUp()
                self._test_concurrent_with_context(mode)

    def test_concurrent_in_camel_case(self):
        for mode in ["thread", "process"]:
            with self.subTest(mode=mode):
                self.setUp()
                self._test_concurrent_in_camel_case(mode)

    def test_coroutine_mode_works_via_arun(self):
        """coroutine 模式通过 Parallel.arun() 正常工作。

        0.5.0+ 为 Parallel 添加了 arun() 方法：runner 检测到 arun 是协程函数时
        自动使用 arun 路径，因此 mode='coroutine' 在 flow.run()（内部走 asyncio 驱
        动器）也能成功执行，不再抛 ValueError。
        同步路径显式拒绝的历史行为已被 arun 路径取代。
        """
        flow = Flow(
            flow_id="test-parallel-coroutine-works",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel",
                    name="parallel",
                    branches=[{"name": "b1", "flow": self.flow_data, "input": "x"}],
                    mode="coroutine",
                    join_branches=["b1"],
                    next="end",
                ),
                End(id="end", resultType="success", output="$NODE.parallel"),
            ],
        )
        # coroutine 模式现在通过 Parallel.arun() 执行，flow.run() 应成功。
        result = flow.run()
        self.assertIn("b1", result)

    def test_parallel_with_assignment_nodes(self):
        # 创建只包含 Assignment 节点的子流程
        sub_flow = Flow(
            flow_id="sub-flow",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.STRING, is_required=True),
            nodes=[
                Start(id="start", next="assign1"),
                Assignment(
                    id="assign1",
                    name="assign1",
                    output_type=Property(data_type=types.STRING),
                    output="Value 1",
                    next="assign2",
                ),
                Assignment(
                    id="assign2",
                    name="assign2",
                    output_type=Property(data_type=types.INTEGER),
                    output=42,
                    next="assign3",
                ),
                Assignment(id="assign3", name="assign3", output_type=Property(data_type=types.STRING), output="$INPUT.value"),
            ],
        )

        # 创建包含并发节点的主流程
        main_flow = Flow(
            flow_id="main-flow",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel",
                    name="parallel",
                    branches=[{"name": "branch1", "flow": sub_flow}, {"name": "branch2", "flow": sub_flow}],
                    mode="thread",
                    join_branches=["branch1", "branch2"],
                    next="end",
                ),
                End(id="end", result_type="success", output="$NODE.parallel"),
            ],
        )

        # 运行流程
        result = main_flow.run({"value": "Main flow input"})

        # 检查结果（sub_flow 显式加了 Start 节点，故 $NODE 子流程结果含 'start': None）
        expected_result = {
            "branch1": {"start": None, "assign1": "Value 1", "assign2": 42, "assign3": "Main flow input"},
            "branch2": {"start": None, "assign1": "Value 1", "assign2": 42, "assign3": "Main flow input"},
        }
        self.assertEqual(result, expected_result)

    # 在子流程中调用parent的全局变量
    def test_call_parent_global_var_in_child_flow(self):
        echo_global_var_flow = Flow(
            flow_id="test-echo",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.STRING, is_required=True),
            output_type=Property(data_type=types.STRING, is_required=True),
            nodes=[Start(id="start", next="end"), End(id="end", result_type="success", output="$GLOBAL.name")],
        )

        main_flow = Flow(
            flow_id="main-flow",
            version="1",
            runtime="python",
            global_context={"name": "kongjie", "age": 19},
            input_type=Property(data_type=types.STRING, is_required=True),
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel",
                    name="parallel",
                    branches=[
                        {"name": "branch1", "flow": echo_global_var_flow},
                        {"name": "branch2", "flow": echo_global_var_flow},
                    ],
                    mode="thread",
                    join_branches=["branch1", "branch2"],
                    next="end",
                ),
                End(id="end", result_type="success", output="$NODE.parallel"),
            ],
        )
        result = main_flow.run()
        self.assertEqual(result, {"branch1": "kongjie", "branch2": "kongjie"})

    # 子流程中同时调用自身的变量和主流程的变量
    def test_call_mixed_var_in_child_flow(self):
        echo_mixed_var_flow = Flow(
            flow_id="test-mixed-var",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="assign1"),
                Assignment(
                    id="assign1",
                    name="assign1",
                    output_type=Property(data_type=types.STRING),
                    output="$INPUT.value",
                    next="assign2",
                ),
                Assignment(
                    id="assign2",
                    name="assign2",
                    output_type=Property(data_type=types.STRING),
                    output="$NODE.assign1",
                    next="assign3",
                ),
                Assignment(
                    id="assign3",
                    name="assign3",
                    output_type=Property(data_type=types.OBJECT, is_required=True),
                    output={
                        "parent_input": "$INPUT.value",
                        "self_node1": "$NODE.assign1",
                        "self_node2": "$NODE.assign2",
                        "parent_global": "$GLOBAL.name",
                    },
                ),
            ],
        )

        main_flow = Flow(
            flow_id="main-flow",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.OBJECT, is_required=True),
            global_context={"name": "kongjie", "age": 19},
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel",
                    name="parallel",
                    branches=[{"name": "branch1", "flow": echo_mixed_var_flow}],
                    mode="thread",
                    join_branches=["branch1"],
                    next="end",
                ),
                End(id="end", result_type="success", output="$NODE.parallel"),
            ],
        )
        result = main_flow.run({"value": "hello"})
        self.assertEqual(
            result,
            {
                "branch1": {
                    "assign1": "hello",
                    "assign2": "hello",
                    "start": None,
                    "assign3": {
                        "parent_global": "kongjie",
                        "parent_input": "hello",
                        "self_node1": "hello",
                        "self_node2": "hello",
                    },
                }
            },
        )
