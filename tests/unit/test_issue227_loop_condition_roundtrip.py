"""#227 回归：``@flow`` 发射/编译必须保留 ``loop.condition``。

修前：``_emit_collection`` 的 LOOP 分支只发 ``id=``（及 MAP/REDUCE 参数），
``_compile_for`` 也不读 ``condition=`` ⇒ **JSON→@flow→JSON 往返后条件静默消失**，
循环从「按 condition 控制」退化为无条件遍历（无报错）。sexpr 链（``:when``）
一直是好的，本模块同时钉住它不回归。

验收点（#227）：
1. 含 condition 的 loop 经 ``emit_source`` → ``compile_source`` 后 condition
   逐字段相等（单条件与 and/or 组都覆盖）；
2. 发射产物显式携带 ``condition=``（不是「编译侧恰好兜住」）；
3. 条件表达式里的 ``item`` / ``index`` / ``result`` 与 ``$LOOP-*`` 对称还原，
   外层已赋值变量（``$NODE.x``）按父侧引用还原；
4. 无 condition 的 loop 行为不变（不多发 ``condition=``）；
5. 无法逆映射的条件 **fail-closed**：``EmitError``，不产出丢字段的源码；
6. 运行期语义：条件为假即停止（往返前后一致）。

负控（#227，还原发射/编译两处读取后本文件必红）：见
``docs/proofs/data/227-negative-control.txt``。
"""
from __future__ import annotations

import copy

import pytest

from plaita import Flow
from plaita.dsl.codeflow import EmitError, compile_source, emit_source
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr


def _ir(condition=None):
    loop = {
        "type": "loop",
        "id": "n",
        "collection": "$INPUT.items",
        "childFlow": {
            "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "cs", "next": "ce"},
                {"type": "end", "id": "ce", "resultType": "success"},
            ],
        },
        "next": "e",
    }
    if condition is not None:
        loop["condition"] = condition
    return {
        "nodes": [
            {"type": "start", "id": "s", "next": "n"},
            loop,
            {"type": "end", "id": "e", "resultType": "success"},
        ]
    }


def _loop_node(ir):
    return next(n for n in ir["nodes"] if n["type"] == "loop")


def test_emit_source_carries_condition():
    ir = _ir({"field": "$INPUT.y", "operator": "eq", "value": 1})
    source = emit_source(copy.deepcopy(ir))
    assert "condition=(" in source
    assert "INPUT.y == 1" in source


def test_emit_compile_roundtrip_keeps_simple_condition():
    ir = _ir({"field": "$INPUT.y", "operator": "eq", "value": 1})
    back = compile_source(emit_source(copy.deepcopy(ir)))
    assert _loop_node(back).get("condition") == {
        "field": "$INPUT.y",
        "operator": "eq",
        "value": 1,
    }


def test_emit_compile_roundtrip_keeps_condition_group():
    condition = {
        "relation": "and",
        "conditions": [
            {"field": "$LOOP-ITEM.n", "operator": "lt", "value": 3},
            {"field": "$LOOP-INDEX", "operator": "ne", "value": 1},
        ],
    }
    ir = _ir(condition)
    back = compile_source(emit_source(copy.deepcopy(ir)))
    assert _loop_node(back).get("condition") == condition


def test_roundtrip_keeps_loop_result_condition():
    condition = {"field": "$LOOP-RESULT", "operator": "lt", "value": 8}
    ir = _ir(condition)
    back = compile_source(emit_source(copy.deepcopy(ir)))
    assert _loop_node(back).get("condition") == condition


def test_compile_source_reads_condition_kwarg():
    source = """
@flow('f')
def f(INPUT):
    x = INPUT.n
    for item in LOOP(INPUT.items, id='n', condition=(x > 1 and item.v < 3)):
        return item
    return NODE.n
"""
    ir = compile_source(source)
    assert _loop_node(ir).get("condition") == {
        "relation": "and",
        "conditions": [
            {"field": "$NODE.x", "operator": "gt", "value": 1},
            {"field": "$LOOP-ITEM.v", "operator": "lt", "value": 3},
        ],
    }
    # 再发射一遍仍逐字段相等（父侧变量名 → 裸名 → $NODE.x 对称）
    back = compile_source(emit_source(copy.deepcopy(ir)))
    assert _loop_node(back).get("condition") == _loop_node(ir)["condition"]


def test_no_condition_loop_stays_unconditioned():
    ir = _ir()
    source = emit_source(copy.deepcopy(ir))
    assert "condition=" not in source
    assert "condition" not in _loop_node(compile_source(source))


def test_unrepresentable_condition_fails_closed():
    ir = _ir({"field": "$BOGUS.x", "operator": "eq", "value": 1})
    with pytest.raises(EmitError):
        emit_source(copy.deepcopy(ir))


def test_sexpr_chain_still_keeps_condition():
    ir = _ir({"field": "$INPUT.y", "operator": "eq", "value": 1})
    back = compile_sexpr(flow_to_sexpr(copy.deepcopy(ir)))
    assert _loop_node(back).get("condition") == {
        "field": "$INPUT.y",
        "operator": "eq",
        "value": 1,
    }


def test_runtime_semantics_survive_roundtrip():
    """条件为假即停止：nums=[1,2,3,4] 停在 3；往返前后结果一致。"""
    source = """
@flow('f')
def f(INPUT):
    for item in LOOP(INPUT.nums, id='lp', condition=(item < 3)):
        return item
    return NODE.lp
"""
    ir = compile_source(source)
    back = compile_source(emit_source(copy.deepcopy(ir)))
    for candidate in (ir, back):
        flow = Flow.model_validate(candidate)
        assert flow.run(nums=[1, 2, 3, 4]) == 3
        assert flow.run(nums=[5, 6]) == 5
