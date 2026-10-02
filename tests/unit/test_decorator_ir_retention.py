"""@flow 装饰器保留编译期 IR（__plaita_ir__）——部署方追加规则的复检入口。

装饰器路径没有源字符串；不保留 IR 的话，部署方规则（如
forbid_node_types_in_childflow）只能各自对 Flow 对象重写遍历，规则的
单一实现会被稀释。recursive 接线（self_improve_flow_v2）依赖本能力。
"""
import unittest

from plaita.dsl.codeflow import flow
from plaita.dsl.ir_validate import (
    FlowIRValidationError,
    forbid_node_types_in_childflow,
    validate_flow_ir,
)


class TestDecoratorIRRetention(unittest.TestCase):
    def test_decorator_retains_ir(self):
        @flow("ir_retention_demo")
        def demo(INPUT):
            a = 1
            return a

        ir = getattr(demo, "__plaita_ir__", None)
        self.assertIsNotNone(ir)
        self.assertIn("nodes", ir)
        self.assertIn("flow_id", ir)

    def test_deployment_rule_recheck_on_retained_ir(self):
        @flow("ir_retention_rule_demo")
        def demo(INPUT):
            a = 1
            return a

        ir = demo.__plaita_ir__
        # 当前 flow 无 childflow，部署规则应通过
        validate_flow_ir(ir, rules=[forbid_node_types_in_childflow({"F"})])

    def test_rule_catches_f_in_childflow_via_retained_ir_shape(self):
        # 与部署侧同一调用形态：对 IR 复检时 F 进 childflow 子树被拦
        bad_ir = {
            "flow_id": "bad_demo",
            "nodes": [
                {"type": "start", "id": "s", "next": "c"},
                {"type": "child", "id": "c", "input": {},
                 "childFlow": {"nodes": [
                     {"type": "start", "id": "cs", "next": "f"},
                     {"type": "F", "id": "f", "next": "ce"},
                     {"type": "end", "id": "ce"},
                 ]}, "next": "e"},
                {"type": "end", "id": "e"},
            ],
        }
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                bad_ir, rules=[forbid_node_types_in_childflow({"F"})])
        self.assertIn("F", cm.exception.message)


if __name__ == "__main__":
    unittest.main()
