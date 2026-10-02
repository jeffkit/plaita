"""review-fix B2 回归：``Flow.global_context`` 不得被 run 内写穿。

历史行为：``ExecutionContext.setup_flow`` 对 ``flow.global_context`` 只做浅
``copy()``——run1 内节点写 ``$GLOBAL`` 嵌套值（``$GLOBAL.cfg.timeout = 30``）
会直接污染 Flow 定义本体，同一 Flow 实例的下一次 run 继承脏值（跨 run 状态
泄漏）。review-fix B2 起改为 ``copy.deepcopy``。
"""
from __future__ import annotations

import unittest

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow


_FLOW_DEF = {
    "flow_id": "b2-regression",
    "global_context": {"cfg": {"nested": "orig"}, "top": 1},
    "nodes": [
        {"id": "s", "type": "start", "name": "start", "next": "e"},
        {"id": "e", "type": "end", "name": "end",
         "resultType": "success", "output": "$GLOBAL.cfg.nested"},
    ],
}


class TestGlobalContextDeepCopy(unittest.TestCase):
    def test_run1_mutation_does_not_leak_into_flow_object(self):
        """run1 变异 $GLOBAL 嵌套 dict 后，Flow 本体的 global_context 不变。"""
        flow = Flow.model_validate(_FLOW_DEF)
        execution = FlowExecution()
        result = execution.run_compatible(flow, False, {})
        self.assertEqual(result, "orig")

        gkey = f"{execution.express_prefix}{execution.express_global_name}"
        g = execution.get_state(gkey)
        self.assertIsInstance(g, dict)
        g["cfg"]["nested"] = "MUTATED_BY_RUN1"

        self.assertEqual(flow.global_context["cfg"]["nested"], "orig",
                         "run1 的 $GLOBAL 写入不得穿透进 Flow 定义对象")

    def test_same_flow_instance_second_run_reads_original_value(self):
        """同一 Flow 实例两连跑：run2 必须读到原始值而非 run1 的脏值。"""
        flow = Flow.model_validate(_FLOW_DEF)

        ex1 = FlowExecution()
        self.assertEqual(ex1.run_compatible(flow, False, {}), "orig")
        gkey = f"{ex1.express_prefix}{ex1.express_global_name}"
        ex1.get_state(gkey)["cfg"]["nested"] = "MUTATED_BY_RUN1"

        ex2 = FlowExecution()
        result2 = ex2.run_compatible(flow, False, {})
        self.assertEqual(result2, "orig",
                         "run2 读到 run1 泄漏的 $GLOBAL 脏值")

    def test_top_level_shallow_write_also_isolated(self):
        """顶层键替换（$GLOBAL 顶层整体赋值）同样不污染 Flow 本体。"""
        flow = Flow.model_validate(_FLOW_DEF)
        ex1 = FlowExecution()
        ex1.run_compatible(flow, False, {})
        gkey = f"{ex1.express_prefix}{ex1.express_global_name}"
        ex1.set_state(gkey, {"brand": "new"})
        self.assertEqual(flow.global_context.get("top"), 1)
        self.assertNotIn("brand", flow.global_context)


if __name__ == "__main__":
    unittest.main()
