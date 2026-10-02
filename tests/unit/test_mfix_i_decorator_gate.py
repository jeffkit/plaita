"""M 修复包 I：@flow / @childflow 装饰器路径统一走 validate_flow_ir 硬门禁（MI3）。

历史双标：``flow_from_source`` / ``FlowBuilder.build`` / ``parse_sexpr`` 走
``build_flow`` 硬门禁，而装饰器模式只走 ``Flow.model_validate``（非致命
warning 版拓扑检查），``@childflow`` 完全无校验。本文件用可控的坏 IR
（monkeypatch 编译产物）验证两条装饰器入口的校验行为与源码模式一致。
"""
from __future__ import annotations

import unittest
from unittest import mock

from plaita.core.flow import Flow
from plaita.dsl.codeflow import F, compile_source, flow_from_source  # noqa: F401
from plaita.dsl.codeflow._common import _ChildFlowMarker
from plaita.dsl.codeflow._source import childflow, flow
from plaita.dsl.ir_validate import FlowIRValidationError

_GOOD_SRC = '''
@flow("good_flow")
def good_flow(INPUT):
    return F.upper(INPUT.name)
'''

_GOOD_CHILD_SRC = '''
@childflow
def sub(INPUT):
    return INPUT.x
'''


def _bad_ir():
    """悬空 next 的 IR——build_flow 必拦，历史装饰器路径放行。"""
    return {
        "runtime": "python",
        "flow_id": "bad_deco",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "ghost"},
            {"type": "end", "id": "end", "output": "x"},
        ],
    }


class TestFlowDecoratorGate(unittest.TestCase):
    def test_decorator_passes_hard_gate(self):
        """正常 @flow 仍走 build_flow 并产出可执行 Flow。"""
        @flow("deco_ok")
        def deco_ok(INPUT):
            return F.upper(INPUT.name)

        self.assertIsInstance(deco_ok, Flow)
        self.assertEqual(deco_ok.run(name="ab"), "AB")

    def test_decorator_rejects_invalid_ir(self):
        """装饰器路径对坏 IR 硬失败——与 flow_from_source 同门。"""
        with mock.patch(
            "plaita.dsl.codeflow._source._compile_func", return_value=_bad_ir()
        ):
            with self.assertRaises(FlowIRValidationError) as cm:
                @flow("bad_deco")
                def bad_deco(INPUT):
                    return INPUT.x
        self.assertIn("ghost", str(cm.exception))

    def test_source_mode_and_decorator_mode_same_gate(self):
        """同一坏 IR：build_flow 层对两条入口一致拒绝（校验双标消除）。"""
        from plaita.dsl.ir_validate import build_flow

        with self.assertRaises(FlowIRValidationError):
            build_flow(_bad_ir())
        # 源码模式出口 flow_from_source 同门（正常源码可过，坏 IR 由 build_flow 拦）
        fl = flow_from_source(_GOOD_SRC)
        self.assertIsInstance(fl, Flow)

    def test_childflow_decorator_rejects_invalid_ir(self):
        """@childflow 历史完全无校验——现在装饰期即拦。"""
        bad_child_ir = {
            "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "missing_node"},
                {"type": "end", "id": "end"},
            ],
        }

        def bad_sub(INPUT):
            return INPUT.x

        with mock.patch(
            "plaita.dsl.codeflow._source._compile_childflow_fdef",
            return_value=bad_child_ir,
        ):
            with self.assertRaises(FlowIRValidationError) as cm:
                childflow()(bad_sub)
        self.assertIn("missing_node", str(cm.exception))

    def test_childflow_decorator_returns_marker_for_good_ir(self):
        def good_sub(INPUT):
            return INPUT.x

        with mock.patch(
            "plaita.dsl.codeflow._source._compile_childflow_fdef",
            return_value={
                "runtime": "python",
                "inputType": {"dataType": "object"},
                "nodes": [
                    {"type": "start", "id": "start", "next": "end"},
                    {"type": "end", "id": "end", "output": "$INPUT.x"},
                ],
            },
        ):
            marker = childflow()(good_sub)
        self.assertIsInstance(marker, _ChildFlowMarker)

    def test_real_childflow_and_flow_via_source_still_work(self):
        """端到端：@childflow + @flow 源码模式在新门禁下行为不变。"""
        src = _GOOD_CHILD_SRC + "\n" + '''
@flow("uses_child")
def uses_child(INPUT):
    r = CHILD(flow=sub, input={"x": INPUT.x})
    return r
'''
        fl = flow_from_source(src)
        self.assertEqual(fl.run(x="v"), "v")

    def test_compile_source_still_returns_raw_ir(self):
        """compile_source 是审查/序列化出口，不做硬校验（行为保持）。"""
        data = compile_source(_GOOD_SRC, "good_flow")
        self.assertEqual(data["flow_id"], "good_flow")


if __name__ == "__main__":
    unittest.main()
