from unittest import TestCase
import json
import threading
import time
import unittest
from typing import ClassVar

from plaita.core import types
from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution
from plaita.io import Property
from plaita.node import End, Start, decide
from plaita.node.basic import Node
from plaita.node.loop import Filter, Find, Loop, Map
from plaita.node.code import CodeNode

user = Property(
    data_type=types.OBJECT,
    children={"name": Property(data_type=types.STRING, is_required=True), "age": Property(data_type=types.INTEGER)},
)


class _ConcurrencyProbe:
    """并发宽度探针：记录「同时在跑」的峰值。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def reset(self) -> None:
        with self._lock:
            self.active = 0
            self.peak = 0

    def hold(self, seconds: float) -> None:
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            time.sleep(seconds)
        finally:
            with self._lock:
                self.active -= 1


MAP_PROBE = _ConcurrencyProbe()


class _MapProbeNode(Node):
    """探针节点：进出各记一次，peak 即 Map 实际并发宽度。"""

    node_type: ClassVar[str] = "map-probe"

    def execute(self, execution=None):
        MAP_PROBE.hold(0.05)
        return "ok"


class LoopTestCase(TestCase):

    def setUp(self) -> None:
        self.child_flow = Flow(
            flow_id="child-loop",
            version="1",
            runtime="python",
            # 输入格式是被注入的？ item, index。
            input_type=Property(
                data_type=types.OBJECT, children={"item": user, "index": Property(data_type=types.INTEGER)}
            ),
            output_type=Property(data_type=types.INTEGER),
            nodes=[
                Start(id="child-start", next="bool"),
                decide.Bool(id="bool", condition={"field": "$INPUT.item.name", "operator": "eq", "value": "KongJie"}),
                End(id="true", **{"resultType": "success", "output": 1}),
                End(id="false", **{"resultType": "success", "output": 0}),
            ],
        )

    def create_flow(self, type_defs, collection):
        flow = Flow(
            flow_id="loop",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=user,
        )
        nodes = [
            Start(id="start", next="loop"),
            Loop(
                id="loop",
                item_type=type_defs,
                collection=collection,
                child_flow=self.child_flow,
                condition=decide.Condition(
                    field="$LOOP-RESULT", operator=decide.CONDITION_OP_NE, value=1
                ),  # 条件可以引用父流程上下文，循环Collection的Item @LOOP-ITEM， childFlow返回的值@LOOP-RESULT。
                next="end",
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.loop"}),
        ]
        flow.nodes = nodes
        return flow

    def test_loop(self):

        flow = self.create_flow(user, "$INPUT.items")
        self.assertEqual(
            1,
            flow.run(
                {"items": [
                    {"name": "jie", "age": 22},
                    {"name": "Kong", "age": 10},
                    {"name": "KongJie", "age": 29},
                    {"name": "kongJie", "age": 40},
                ]}
            ),
        )

    def test_loop_collection(self):
        flow = self.create_flow(
            user,
            [
                {"name": "jie", "age": 22},
                {"name": "Kong", "age": 10},
                {"name": "KongJie", "age": 29},
                {"name": "kongJie", "age": 40},
            ],
        )
        self.assertEqual(1, flow.run())


class TestLoopConditionIsolation(unittest.TestCase):
    """2026-07 整改: Loop condition 求值用的 ctx 必须与 execution.context 隔离。

    历史上 loop.py 用 ``dict(execution.context)`` shallow copy, 注释自安慰
    "condition.match is read-only"。但表达式引擎里有 ``$F.set`` / ``$F.pop``
    等 mutate 函数 (见 plaita/core/expression.py), 一旦 condition 引用它们
    就会污染原 context 里 value 对象, 跨迭代累积。deepcopy 把隔离做实。
    """

    def test_condition_using_set_function_does_not_mutate_original(self):
        from plaita.node.decide import Condition, CONDITION_OP_EQ

        # child_flow 把 item 原样返回; condition 用 $F.set 写一个 key (mutate)
        # 再判等"counter != LOOP-RESULT"——永远为真, 让 loop 跑完所有 iteration
        # 不 break, 同时每次 condition 求值都会触发 mutate。
        # 关键: condition 求值用的 ctx 是 deepcopy 出来的, 不应污染原 context。
        child = Flow(
            flow_id="cond-isolation-child",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                children={"item": Property(data_type=types.INTEGER),
                          "index": Property(data_type=types.INTEGER)},
            ),
            output_type=Property(data_type=types.INTEGER),
            nodes=[
                Start(id="cs", next="ce"),
                End(id="ce", **{"resultType": "success", "output": "$INPUT.item"}),
            ],
        )

        flow = Flow(
            flow_id="cond-isolation",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            global_context={"counter": 0},
            nodes=[
                Start(id="start", next="loop"),
                Loop(
                    id="loop",
                    collection=[1, 2, 3],
                    child_flow=child,
                    # condition 故意 mutate $GLOBAL.counter, 然后用一条永远为真
                    # 的判断 (counter != -1) 让 loop 跑完。
                    condition=Condition(
                        field="$F.set($GLOBAL, 'counter', $LOOP-RESULT)",
                        operator=CONDITION_OP_EQ,
                        value=0,
                    ),
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.loop"}),
            ],
        )

        execution = FlowExecution()
        # condition 第一轮: $F.set 返回 None, None == 0 → False → break。
        # 所以 loop 只跑第一个 item, result = 1。
        # 但 mutate 在 condition 求值中已经发生 ($GLOBAL.counter 被改成 1)。
        # 关键是: 这个 mutate 不应泄漏到原 execution.context。
        result = execution.run_compatible(flow, lazy=False, input=None)
        self.assertEqual(result, 1)
        # deepcopy 隔离生效时 counter 仍是 0; shallow copy 时 counter 会被改成
        # condition 写入的值 (1)。
        self.assertEqual(
            execution.context.get("$GLOBAL", {}).get("counter"),
            0,
            "condition 求值不应污染原 execution.context",
        )


class MapTestCase(TestCase):
    def setUp(self) -> None:
        self.child_flow = Flow(  # 如果age大于35，则映射为old ...
            flow_id="child-map",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT, children={"item": user, "index": Property(data_type=types.INTEGER)}
            ),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="child-start", next="bool"),
                decide.Bool(id="bool", condition={"field": "$INPUT.item.age", "operator": "gte", "value": 35}),
                End(id="true", **{"resultType": "success", "output": "old"}),
                End(id="false", **{"resultType": "success", "output": "young"}),
            ],
        )

        # 创建一个带延时的子流程用于性能测试
        self.slow_child_flow = Flow(
            flow_id="child-map-slow",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT, children={"item": user, "index": Property(data_type=types.INTEGER)}
            ),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="child-start", next="sleep"),
                CodeNode(
                    id="sleep",
                    next="bool",
                    language="python",
                    code="def run(input):\n    import time\n    time.sleep(0.1)\n    return input",
                    input="$INPUT",
                    sandbox_backend="unsafe",
                ),
                decide.Bool(id="bool", condition={"field": "$INPUT.item.age", "operator": "gte", "value": 35}),
                End(id="true", **{"resultType": "success", "output": "old"}),
                End(id="false", **{"resultType": "success", "output": "young"}),
            ],
        )

        # 并发宽度探针用的子流程（test_map_max_concurrent）：节点进出记录同时在跑的数量
        self.probe_child_flow = Flow(
            flow_id="child-map-probe",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT, children={"item": user, "index": Property(data_type=types.INTEGER)}
            ),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="child-start", next="probe"),
                _MapProbeNode(id="probe", next="end"),
                End(id="end", **{"resultType": "success", "output": "ok"}),
            ],
        )

    def create_flow(self, type_defs, collection):
        flow = Flow(
            flow_id="map",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=user,
        )
        nodes = [
            Start(id="start", next="map"),
            Map(
                id="map", item_type=type_defs, flow=flow, collection=collection, child_flow=self.child_flow, next="end"
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=flow),
        ]
        flow.nodes = nodes
        return flow

    def test_map(self):
        self.assertEqual(
            ["young", "young", "young", "old"],
            self.create_flow(None, "$INPUT.items").run(
                {"items": [
                    {"name": "jie", "age": 22},
                    {"name": "Kong", "age": 10},
                    {"name": "KongJie", "age": 29},
                    {"name": "kongJie", "age": 40},
                ]}
            ),
        )

    def test_map_concurrent(self):
        # Create a flow with concurrent execution enabled
        flow = Flow(
            flow_id="map",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=user,
        )
        nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=flow,
                collection="$INPUT.items",
                child_flow=self.child_flow,
                next="end",
                concurrent=True  # Enable concurrent execution
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=flow),
        ]
        flow.nodes = nodes

        # Test with the same input data
        result = flow.run(
            {"items": [
                {"name": "jie", "age": 22},
                {"name": "Kong", "age": 10},
                {"name": "KongJie", "age": 29},
                {"name": "kongJie", "age": 40},
            ]}
        )
        
        # Verify the results are the same as sequential execution
        self.assertEqual(["young", "young", "young", "old"], result)

    def test_map_concurrent_performance(self):
        # 准备测试数据 - 10个元素
        test_data = [{"name": f"user{i}", "age": 20 + i} for i in range(10)]
        
        # 创建顺序执行的流程
        sequential_flow = Flow(
            flow_id="map-sequential",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=user,
        )
        sequential_flow.nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=sequential_flow,
                collection="$INPUT.items",
                child_flow=self.slow_child_flow,
                next="end",
                concurrent=False
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=sequential_flow),
        ]

        # 创建并发执行的流程
        concurrent_flow = Flow(
            flow_id="map-concurrent",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=user,
        )
        concurrent_flow.nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=concurrent_flow,
                collection="$INPUT.items",
                child_flow=self.slow_child_flow,
                next="end",
                concurrent=True
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=concurrent_flow),
        ]

        # 测量顺序执行时间
        import time
        sequential_start = time.time()
        sequential_result = sequential_flow.run({"items": test_data})
        sequential_time = time.time() - sequential_start

        # 测量并发执行时间
        concurrent_start = time.time()
        concurrent_result = concurrent_flow.run({"items": test_data})
        concurrent_time = time.time() - concurrent_start

        # 验证结果相同
        self.assertEqual(sequential_result, concurrent_result)
        
        # 验证并发执行显著快于顺序执行
        # 理论上，顺序执行时间应该约为 10 * 0.1 = 1秒
        # 而并发执行时间应该约为 0.1秒
        print(f"\nSequential execution time: {sequential_time:.2f}s")
        print(f"Concurrent execution time: {concurrent_time:.2f}s")
        
        # 并发执行时间应该显著小于顺序执行时间
        self.assertLess(concurrent_time, sequential_time / 2)

    def _max_concurrent_flow(self, max_concurrent: int, child_flow: Flow) -> Flow:
        flow = Flow(
            flow_id=f"map-max-concurrent-{max_concurrent}",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=Property(data_type=types.STRING),
        )
        flow.nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=flow,
                collection="$INPUT.items",
                child_flow=child_flow,
                next="end",
                concurrent=True,
                max_concurrent=max_concurrent,
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=flow),
        ]
        return flow

    def test_map_max_concurrent(self):
        """max_concurrent 真的把并发宽度卡在指定值上。

        旧版靠墙钟计时断言「4 并发的耗时约为 2 并发的一半」：10 项 / mc=2 是 5 批、
        /mc=4 是 3 批，比值本就该是 3/5 而非 1/2；且单批耗时随宿主机负载在
        0.11~0.26s 抖动（实测），紧的计时断言在共享机器上不可复现（本仓 CI/本地
        反复假红）。改为直接观测同时在跑的子流程数峰值：语义等价，且确定性。
        """
        test_data = [{"name": f"user{i}", "age": 20 + i} for i in range(10)]
        for max_concurrent in (2, 4):
            MAP_PROBE.reset()
            flow = self._max_concurrent_flow(max_concurrent, self.probe_child_flow)

            start = time.time()
            result = flow.run({"items": test_data})
            elapsed = time.time() - start

            # 结果与并发宽度无关
            self.assertEqual(["ok"] * len(test_data), result)
            # 并发宽度生效：峰值既不能高于 max_concurrent（semaphore 失效），
            # 也不能低于它（降级串行 / 共享池饥饿）
            self.assertEqual(
                max_concurrent,
                MAP_PROBE.peak,
                f"max_concurrent={max_concurrent} 实测并发峰值 {MAP_PROBE.peak}",
            )
            print(f"Max concurrent={max_concurrent} execution time: {elapsed:.2f}s, peak={MAP_PROBE.peak}")


class FilterTestCase(TestCase):
    def setUp(self) -> None:
        self.child_flow = Flow(  # 只要age小于35的
            flow_id="child-filter",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT, children={"item": user, "index": Property(data_type=types.INTEGER)}
            ),
            output_type=Property(data_type=types.BOOL),
            nodes=[
                Start(id="child-start", next="bool"),
                decide.Bool(id="bool", condition={"field": "$INPUT.item.age", "operator": "lt", "value": 35}),
                End(id="true", **{"resultType": "success", "output": True}),
                End(id="false", **{"resultType": "success", "output": False}),
            ],
        )

    def create_flow(self, type_defs, collection):
        flow = Flow(
            flow_id="filter",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=user,
        )
        nodes = [
            Start(id="start", next="filter"),
            Filter(
                id="filter",
                item_type=type_defs,
                flow=flow,
                collection=collection,
                child_flow=self.child_flow,
                next="end",
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.filter"}, flow=flow),
        ]
        flow.nodes = nodes
        return flow

    def test_filter(self):
        self.assertEqual(
            [{"name": "jie", "age": 22}, {"name": "Kong", "age": 10}, {"name": "KongJie", "age": 29}],
            self.create_flow(None, "$INPUT.items").run(
                {"items": [
                    {"name": "jie", "age": 22},
                    {"name": "Kong", "age": 10},
                    {"name": "KongJie", "age": 29},
                    {"name": "kongJie", "age": 40},
                ]}
            ),
        )


class FindTestCase(TestCase):
    def setUp(self) -> None:
        self.child_flow = Flow(  # 找到第一个年纪小于30岁的
            flow_id="child-find",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT, children={"item": user, "index": Property(data_type=types.INTEGER)}
            ),
            output_type=Property(data_type=types.BOOL),
            nodes=[
                Start(id="child-start", next="bool"),
                decide.Bool(id="bool", condition={"field": "$INPUT.item.age", "operator": "lt", "value": 30}),
                End(id="true", **{"resultType": "success", "output": True}),
                End(id="false", **{"resultType": "success", "output": False}),
            ],
        )

    def create_flow(self, type_defs, collection):
        flow = Flow(
            flow_id="find",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=user,
        )
        nodes = [
            Start(id="start", next="find"),
            Find(id="find", collection=collection, child_flow=self.child_flow, next="end"),
            End(id="end", **{"resultType": "success", "output": "$NODE.find"}),
        ]
        flow.nodes = nodes
        return flow

    def test_find(self):
        self.assertEqual(
            {"name": "jie", "age": 22},
            self.create_flow(None, "$INPUT.items").run(
                {"items": [
                    {"name": "jie", "age": 22},
                    {"name": "Kong", "age": 10},
                    {"name": "KongJie", "age": 29},
                    {"name": "kongJie", "age": 40},
                ]}
            ),
        )


class TestMapFlow(unittest.TestCase):
    def test_map_flow(self):
        # Flow definition
        flow_json = '''{"id":"tsdk4iNm","inputType":{"dataType":"object","name":"","properties":{"input":{"dataType":"array","label":"输入","name":"input","default":"","required":false,"ref":"","itemType":{"dataType":"string"}}}},"outputType":{"dataType":"object","name":"","properties":{"text":{"dataType":"array","label":"回复","name":"text","default":"","required":false,"ref":"","itemType":{"dataType":"string"}}}},"nodes":[{"type":"start","name":"开始","id":"node_SDAYzyDD","next":"node_b5uU9mdh"},{"type":"map","name":"MAP 循环","collection":"$INPUT.input","itemType":{"dataType":"string","name":"item","label":"循环元素","ref":""},"resultItemType":{"dataType":"string","name":"result","label":"循环结果","ref":""},"childFlow":{"id":"iI8WVu1-","inputType":{"dataType":"object","name":"","properties":{"item":{"dataType":"string","name":"item","label":"循环元素","ref":""},"index":{"dataType":"number","name":"index","label":"索引","ref":""}}},"outputType":{"dataType":"string","name":"result","label":"循环结果","ref":""},"nodes":[{"type":"start","name":"开始","id":"node_ZUyy547D","next":"node_5zo4WjXo"},{"type":"end","name":"结束","output":"$INPUT.item","resultType":"success","id":"node_5zo4WjXo"}],"external":{"plaitaVersion":"2.1.0","envProperty":{"dataType":"object","name":"global","label":"全局变量","properties":{}}},"context":{"global":{"__ENVCONF__":{"formal":{}},"__ENVNAME__":"formal"}}},"async":true,"id":"node_b5uU9mdh","next":"node_BAVCx96m"},{"type":"end","name":"结束","resultType":"success","id":"node_BAVCx96m","output":{"text":"$NODE.node_b5uU9mdh"}}],"external":{"plaitaVersion":"2.1.0","envProperty":{"dataType":"object","name":"global","label":"全局变量","properties":{"trace_id":{"dataType":"string","label":"trace_id","name":"trace_id","default":"","required":false},"edan_context":{"dataType":"object","label":"edan_context","name":"edan_context","default":"","required":false,"ref":"","properties":{"user_name":{"dataType":"string","label":"user_name","name":"user_name","default":"","required":false}}}}}},"context":{"global":{"__ENVCONF__":{"formal":{"trace_id":"","edan_context":{"user_name":""}}},"__ENVNAME__":"formal"}}}'''
        
        # Parse flow JSON
        flow_def = json.loads(flow_json)
        flow = Flow.model_validate(flow_def)
        
        # Test input data
        test_input = {
            "input": ["test1", "test2", "test3"]
        }
        
        # Create execution context and run flow
        execution = FlowExecution()
        result = execution.run_compatible(flow, False, **test_input)
        
        # Verify results
        self.assertIn('text', result)
        self.assertEqual(result['text'], test_input['input'])
        self.assertEqual(len(result['text']), 3)

if __name__ == '__main__':
    unittest.main()
