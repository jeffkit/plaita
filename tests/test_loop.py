from unittest import TestCase
import json
import unittest

from plaita.core import types
from plaita.flow import Flow, FlowExecution
from plaita.io import Property
from plaita.node import End, Start, decide
from plaita.node.loop import Filter, Find, Loop, Map
from plaita.node.code import CodeNode

user = Property(
    data_type=types.OBJECT,
    children={"name": Property(data_type=types.STRING, is_required=True), "age": Property(data_type=types.INTEGER)},
)


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
            input_type=Property(data_type=types.ARRAY, item_type=user),
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

        flow = self.create_flow(user, "$INPUT")
        self.assertEqual(
            1,
            flow.run(
                *[
                    {"name": "jie", "age": 22},
                    {"name": "Kong", "age": 10},
                    {"name": "KongJie", "age": 29},
                    {"name": "kongJie", "age": 40},
                ]
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
                    input="$INPUT"
                ),
                decide.Bool(id="bool", condition={"field": "$INPUT.item.age", "operator": "gte", "value": 35}),
                End(id="true", **{"resultType": "success", "output": "old"}),
                End(id="false", **{"resultType": "success", "output": "young"}),
            ],
        )

    def create_flow(self, type_defs, collection):
        flow = Flow(
            flow_id="map",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.ARRAY, item_type=user),
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
            self.create_flow(None, "$INPUT").run(
                *[
                    {"name": "jie", "age": 22},
                    {"name": "Kong", "age": 10},
                    {"name": "KongJie", "age": 29},
                    {"name": "kongJie", "age": 40},
                ]
            ),
        )

    def test_map_concurrent(self):
        # Create a flow with concurrent execution enabled
        flow = Flow(
            flow_id="map",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.ARRAY, item_type=user),
            output_type=user,
        )
        nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=flow,
                collection="$INPUT",
                child_flow=self.child_flow,
                next="end",
                concurrent=True  # Enable concurrent execution
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=flow),
        ]
        flow.nodes = nodes

        # Test with the same input data
        result = flow.run(
            *[
                {"name": "jie", "age": 22},
                {"name": "Kong", "age": 10},
                {"name": "KongJie", "age": 29},
                {"name": "kongJie", "age": 40},
            ]
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
            input_type=Property(data_type=types.ARRAY, item_type=user),
            output_type=user,
        )
        sequential_flow.nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=sequential_flow,
                collection="$INPUT",
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
            input_type=Property(data_type=types.ARRAY, item_type=user),
            output_type=user,
        )
        concurrent_flow.nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=concurrent_flow,
                collection="$INPUT",
                child_flow=self.slow_child_flow,
                next="end",
                concurrent=True
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=concurrent_flow),
        ]

        # 测量顺序执行时间
        import time
        sequential_start = time.time()
        sequential_result = sequential_flow.run(*test_data)
        sequential_time = time.time() - sequential_start

        # 测量并发执行时间
        concurrent_start = time.time()
        concurrent_result = concurrent_flow.run(*test_data)
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

    def test_map_max_concurrent(self):
        # 准备测试数据 - 10个元素
        test_data = [{"name": f"user{i}", "age": 20 + i} for i in range(10)]
        import time
        # 创建 max_concurrent=2 的并发流程（理论约0.5秒）
        concurrent_flow_2 = Flow(
            flow_id="map-max-concurrent-2",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.ARRAY, item_type=user),
            output_type=user,
        )
        concurrent_flow_2.nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=concurrent_flow_2,
                collection="$INPUT",
                child_flow=self.slow_child_flow,
                next="end",
                concurrent=True,
                max_concurrent=2,
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=concurrent_flow_2),
        ]
        start_2 = time.time()
        result_2 = concurrent_flow_2.run(*test_data)
        time_2 = time.time() - start_2

        # 创建 max_concurrent=4 的并发流程（理论约0.25秒）
        concurrent_flow_4 = Flow(
            flow_id="map-max-concurrent-4",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.ARRAY, item_type=user),
            output_type=user,
        )
        concurrent_flow_4.nodes = [
            Start(id="start", next="map"),
            Map(
                id="map",
                item_type=None,
                flow=concurrent_flow_4,
                collection="$INPUT",
                child_flow=self.slow_child_flow,
                next="end",
                concurrent=True,
                max_concurrent=4,
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.map"}, flow=concurrent_flow_4),
        ]
        start_4 = time.time()
        result_4 = concurrent_flow_4.run(*test_data)
        time_4 = time.time() - start_4

        # 验证结果相同
        self.assertEqual(result_2, result_4)

        print(f"Max concurrent=2 execution time: {time_2:.2f}s")
        print(f"Max concurrent=4 execution time: {time_4:.2f}s")

        self.assertLess(time_4, time_2)
        self.assertAlmostEqual(time_4, time_2 / 2, delta=0.15)

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
            input_type=Property(data_type=types.ARRAY, item_type=user),
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
            self.create_flow(None, "$INPUT").run(
                *[
                    {"name": "jie", "age": 22},
                    {"name": "Kong", "age": 10},
                    {"name": "KongJie", "age": 29},
                    {"name": "kongJie", "age": 40},
                ]
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
            input_type=Property(data_type=types.ARRAY, item_type=user),
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
            self.create_flow(None, "$INPUT").run(
                *[
                    {"name": "jie", "age": 22},
                    {"name": "Kong", "age": 10},
                    {"name": "KongJie", "age": 29},
                    {"name": "kongJie", "age": 40},
                ]
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
