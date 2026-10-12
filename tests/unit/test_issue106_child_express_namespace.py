"""plaita#106：子执行必须继承父级的 ``express_*`` 命名空间配置。

``FlowExecution`` 的子执行入口（``get_child_execution``，被 ``child`` 内联子逻辑、
``parallel`` 分支、``map``/``filter``/``find``/``reduce`` 迭代体共用）历史上只复制
``mode``，六个命名空间参数全部回落默认值。父级按 ``docs-site/docs/guide/expressions.md``
自定义命名空间后，子流程表达式按自定义名寻址会硬
``KeyError: '$PARAMS' not found in expression context``。

本文件钉住：六个属性逐一继承、子执行的 CheckpointState 键前缀/名表同源、以及
child / parallel / map 三条生产路径的端到端求值。
"""

import json
import unittest

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow

EXPRESS_FIELDS = (
    "express_prefix", "express_input_name", "express_parent_name",
    "express_node_name", "express_global_name", "express_environment_variable",
)

CUSTOM = {
    "express_prefix": "#",
    "express_input_name": "PARAMS",
    "express_parent_name": "UP",
    "express_node_name": "STEPS",
    "express_global_name": "GLOB",
    "express_environment_variable": "VARS",
}


def _flow(payload):
    return Flow.model_validate(json.loads(json.dumps(payload)))


SUB_READS_INPUT = {
    "flow_id": "sub",
    "nodes": [
        {"id": "s", "type": "start", "next": "e"},
        {"id": "e", "type": "end", "resultType": "success", "output": "$PARAMS.x"},
    ],
}

MAIN_WITH_CHILD = {
    "flow_id": "main-child",
    "nodes": [
        {"id": "s", "type": "start", "next": "c"},
        {"id": "c", "type": "child", "childFlow": SUB_READS_INPUT, "next": "e"},
        {"id": "e", "type": "end", "resultType": "success", "output": "$NODE.c"},
    ],
}

MAIN_WITH_PARALLEL = {
    "flow_id": "main-parallel",
    "nodes": [
        {"id": "s", "type": "start", "next": "p"},
        {
            "id": "p", "type": "parallel",
            "joinBranches": ["bg"],
            "branches": [{"name": "bg", "flow": SUB_READS_INPUT}],
            "next": "e",
        },
        {"id": "e", "type": "end", "resultType": "success", "output": "$NODE.p.bg"},
    ],
}

MAIN_WITH_MAP = {
    "flow_id": "main-map",
    "nodes": [
        {"id": "s", "type": "start", "next": "m"},
        {
            "id": "m", "type": "map",
            "collection": ["a", "b"],
            "child_flow": {
                "flow_id": "map-body",
                "nodes": [
                    {"id": "s", "type": "start", "next": "e"},
                    {"id": "e", "type": "end", "resultType": "success", "output": "$PARAMS.item"},
                ],
            },
            "next": "e",
        },
        {"id": "e", "type": "end", "resultType": "success", "output": "$NODE.m"},
    ],
}


class TestChildExecutionInheritsExpressNamespace(unittest.TestCase):
    def test_get_child_execution_inherits_all_six_settings(self):
        parent = FlowExecution()
        for name, value in CUSTOM.items():
            setattr(parent, name, value)

        child = parent.get_child_execution()

        for name in EXPRESS_FIELDS:
            self.assertEqual(
                getattr(child, name), getattr(parent, name),
                f"{name} 必须从父执行继承，而不是回落默认值",
            )

    def test_child_state_model_uses_inherited_names_before_first_run(self):
        """首个 CheckpointState 就要用继承来的前缀/名表建键（晚 setattr 会漏）。"""
        parent = FlowExecution()
        for name, value in CUSTOM.items():
            setattr(parent, name, value)

        child = parent.get_child_execution()

        state = child._ctx._state
        self.assertEqual(state.prefix, "#")
        self.assertEqual(state.input_name, "PARAMS")
        self.assertEqual(state.node_name, "STEPS")

    def test_default_namespace_unchanged(self):
        """六个参数全默认时，子执行与父执行逐一相同（对生产链路零行为变更）。"""
        parent = FlowExecution()
        child = parent.get_child_execution()
        for name in EXPRESS_FIELDS:
            self.assertEqual(getattr(child, name), getattr(parent, name))

    def test_child_expression_reads_custom_namespace_e2e(self):
        """child 内联子流程按自定义命名空间求值，且与子流程当根跑结果一致。"""
        sub = _flow(SUB_READS_INPUT)
        as_root = FlowExecution.run(
            sub, params={"x": "HELLO"}, express_input_name="PARAMS"
        )
        self.assertEqual(as_root, "HELLO")

        main = _flow(MAIN_WITH_CHILD)
        as_child = FlowExecution.run(
            main, params={"x": "HELLO"}, express_input_name="PARAMS"
        )
        self.assertEqual(as_child, "HELLO")

    def test_parallel_branch_reads_custom_namespace_e2e(self):
        main = _flow(MAIN_WITH_PARALLEL)
        result = FlowExecution.run(
            main, params={"x": "HELLO"}, express_input_name="PARAMS"
        )
        self.assertEqual(result, "HELLO")

    def test_map_iteration_reads_custom_namespace_e2e(self):
        main = _flow(MAIN_WITH_MAP)
        result = FlowExecution.run(
            main, params={}, express_input_name="PARAMS"
        )
        self.assertEqual(result, ["a", "b"])

    def test_child_custom_prefix_keys_e2e(self):
        """自定义 prefix 时子执行上下文键也换前缀（#PARAMS 而非 $INPUT）。"""
        sub = {
            "flow_id": "sub-hash",
            "nodes": [
                {"id": "s", "type": "start", "next": "e"},
                {"id": "e", "type": "end", "resultType": "success", "output": "#PARAMS.x"},
            ],
        }
        main = {
            "flow_id": "main-hash",
            "nodes": [
                {"id": "s", "type": "start", "next": "c"},
                {"id": "c", "type": "child", "childFlow": sub, "next": "e"},
                {"id": "e", "type": "end", "resultType": "success", "output": "#NODE.c"},
            ],
        }
        result = FlowExecution.run(
            _flow(main), params={"x": "HELLO"}, express_prefix="#", express_input_name="PARAMS"
        )
        self.assertEqual(result, "HELLO")


if __name__ == "__main__":
    unittest.main()
