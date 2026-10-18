"""plaita#220-L1：``@flow`` 集合节点 id 未进入名字作用域。

``emit_source`` 把 ``$NODE.<id>`` 的**下游引用**发射成裸变量名（``return agg``），
而集合节点在源码里是 ``for item in MAP(..., id="agg")``——没有 ``agg = MAP(...)``
这种赋值语句可依赖。``_compile_for`` 此前不登记该 id ⇒ 任何
``JSON -> @flow -> JSON`` 往返在集合节点被下游 ``$NODE.<id>`` 消费时以
``_CodeflowError: 未知名字 'agg'`` **硬失败**（map/filter/find/loop/reduce 5/5）；
命名 WHILE 的 for-head（``for item in WHILE(..., id="w")``）同族同病。

验收（#220-L1）：
- 5 类集合节点 ``IR -> emit_source -> compile_source`` 后下游 ``$NODE.<id>`` 保真；
- 命名 WHILE for-head 同链保真；
- 发射器实际发的是裸变量名（``return agg``），即修复必须发生在编译侧；
- 集合节点 id 只在本节点**之后**进作用域：循环体里引用自身 id 仍按未知名字拒绝
  （与「父侧结果此刻尚未写回」的既有契约一致）；
- 未登记裸名字仍被拒绝（修复不放宽未知名字解析）；
- 显式 ``NODE.<id>`` 形态与 sexpr 链在基线即绿，作为正控一起钉住；
- 编译后可真实执行（MAP 下游拿到的就是集合结果，不只是 IR 文本相等）。
"""
from __future__ import annotations

import pytest

from plaita.dsl.codeflow import compile_source, emit_source, flow_from_source
from plaita.dsl.codeflow._common import _CodeflowError
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr

_COLLECTION_KINDS = ("map", "filter", "find", "loop", "reduce")


def _child(output: str = "$INPUT.item") -> dict:
    return {
        "runtime": "python",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "cs", "next": "ce"},
            {"type": "end", "id": "ce", "output": output, "resultType": "success"},
        ],
    }


def _collection_ir(kind: str, node_id: str = "agg", flow_id: str = "l1probe") -> dict:
    node = {
        "type": kind,
        "id": node_id,
        "collection": "$INPUT.items",
        "childFlow": _child("$INPUT[0]" if kind == "reduce" else "$INPUT.item"),
        "next": "e",
    }
    if kind == "reduce":
        node["initial"] = 0
    return {
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "s", "next": node_id},
            node,
            {"type": "end", "id": "e", "output": f"$NODE.{node_id}", "resultType": "success"},
        ],
    }


def _while_ir(node_id: str = "w", flow_id: str = "l1while") -> dict:
    return {
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "s", "next": node_id},
            {
                "type": "while",
                "id": node_id,
                "condition": {"field": "$INPUT.n", "operator": "gt", "value": 0},
                "child_flow": _child(),
                "next": "e",
            },
            {"type": "end", "id": "e", "output": f"$NODE.{node_id}", "resultType": "success"},
        ],
    }


def _pick(ir: dict, node_id: str) -> dict:
    return [n for n in ir["nodes"] if n.get("id") == node_id][0]


def _top_end(ir: dict) -> dict:
    """主流程 end（源码直编译的 end id 是 slug，不保证叫 ``e``）。"""
    return [n for n in ir["nodes"] if n["type"] == "end"][-1]


@pytest.mark.parametrize("kind", _COLLECTION_KINDS)
def test_collection_node_ref_survives_flow_roundtrip(kind):
    """IR -> @flow -> IR：下游 ``$NODE.<id>`` 必须可重编译且逐字段保真。"""
    original = _collection_ir(kind)
    emitted = emit_source(original)

    # 发射形状：下游引用是裸变量名（修复点必须落在编译侧，不是发射器改名）
    assert emitted.splitlines()[-1] == "    return agg"
    assert "id='agg'" in emitted

    back = compile_source(emitted)
    end = _top_end(back)
    assert end["output"] == "$NODE.agg"
    node = _pick(back, "agg")
    assert node["type"] == kind
    assert node["collection"] == "$INPUT.items"
    assert node["next"] == end["id"]
    assert node["childFlow"]["inputType"] == {
        "dataType": "array" if kind == "reduce" else "object"}


def test_named_while_for_head_ref_survives_flow_roundtrip():
    """命名 WHILE 的 for-head 与集合节点同族：id 也必须进名字作用域。"""
    original = _while_ir()
    emitted = emit_source(original)
    assert "for item in WHILE(" in emitted
    assert "id='w'" in emitted
    assert emitted.splitlines()[-1] == "    return w"

    back = compile_source(emitted)
    assert _top_end(back)["output"] == "$NODE.w"
    node = _pick(back, "w")
    assert node["type"] == "while"
    assert node["child_flow"]["nodes"]


def test_direct_source_downstream_bare_name_binds_node_ref():
    """手写源码（不经发射器）同样按语言契约解析集合节点 id。"""
    ir = compile_source(
        "def f(INPUT):\n"
        "    for item in MAP(INPUT.items, id='agg'):\n"
        "        return item\n"
        "    return agg"
    )
    assert _top_end(ir)["output"] == "$NODE.agg"


def test_collection_id_is_not_visible_inside_its_own_body():
    """作用域边界：id 在本节点之后才登记，循环体引用自身 id 仍按未知名字拒绝。"""
    with pytest.raises(_CodeflowError) as ctx:
        compile_source(
            "def f(INPUT):\n"
            "    for item in MAP(INPUT.items, id='agg'):\n"
            "        return agg\n"
            "    return 'done'"
        )
    assert "未知名字 'agg'" in str(ctx.value)


def test_while_id_is_not_visible_inside_its_own_body():
    """WHILE 同族的作用域边界：体内引用自身 id 仍拒绝（父侧结果尚未写回）。"""
    with pytest.raises(_CodeflowError) as ctx:
        compile_source(
            "def f(INPUT):\n"
            "    for item in WHILE(item.n > 0, id='w'):\n"
            "        return w\n"
            "    return 'done'"
        )
    assert "未知名字 'w'" in str(ctx.value)


def test_unregistered_bare_name_still_rejected():
    """负控护栏：修复只登记节点 id，不把任意未知名字放行。"""
    with pytest.raises(_CodeflowError) as ctx:
        compile_source(
            "def f(INPUT):\n"
            "    for item in MAP(INPUT.items, id='agg'):\n"
            "        return item\n"
            "    return missing"
        )
    assert "未知名字 'missing'" in str(ctx.value)


def test_explicit_node_namespace_still_compiles():
    """正控：显式 ``NODE.<id>`` 在基线即绿，修复不得改变其语义。"""
    ir = compile_source(
        "def f(INPUT):\n"
        "    for item in MAP(INPUT.items, id='agg'):\n"
        "        return item\n"
        "    return NODE.agg"
    )
    assert _top_end(ir)["output"] == "$NODE.agg"


def test_sexpr_collection_node_ref_already_roundtrips():
    """正控：sexpr 链在基线即保留下游 ``$NODE.<id>``（本面是 @flow-only）。"""
    back = compile_sexpr(flow_to_sexpr(_collection_ir("map")))
    assert _pick(back, "e")["output"] == "$NODE.agg"


def test_collection_result_is_live_at_runtime():
    """端到端语义：重编译后的下游引用拿到真实集合结果，不只是文本相等。"""
    flow = flow_from_source(emit_source(_collection_ir("map")))
    assert flow.run({"items": [1, 2, 3]}) == [1, 2, 3]
