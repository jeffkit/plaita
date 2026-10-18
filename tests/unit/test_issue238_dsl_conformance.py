"""#238 DSL 往返 conformance：全节点类型 × 全字段 × 双链。

背景
====
JSON IR / ``@flow`` / sexpr 三表示互转的 round-trip 缺口从 #75 起逐点修补
（#76/#88/#89/#127/#136-#150/#200/#207/#220/#227），每轮 issue 都会再挖出新的
静默丢字段面。本模块把「往返保真」从逐面补丁升级为**系统性 conformance**：

1. **registry 是权威清单**：从 ``NodeRegistry()`` 取全部内置节点类型，逐个
   节点类反射 ``model_fields``——注册表新增字段而本表未覆盖时直接红。
2. **全字段实例**：每个节点类型一份把模型字段填满的 IR fixture（含子流程、
   flow 级字段），逐字段断言两条链的往返结果。
3. **双向深等价**：``JSON → @flow → JSON`` 与 ``JSON → sexpr → JSON`` 归一化
   后整树相等（剥派生注解、规范化非语义 id 与 snake/camel 拼写别名）。
4. **丢失面显式登记**：确实无法表达/无运行期语义的字段进入 ``MATRIX`` 的
   ``LOST``/``INERT``/``DERIVED`` 状态，并**反向断言它们确实仍丢失**——将来
   某个链补上支持后，本表不更新就会红（防止白名单腐烂）。
5. **负控**：``test_harness_detects_unregistered_new_field`` 动态注册一个
   带新字段的节点类，断言覆盖检查会报红（证明 harness 不是自证空转）。

状态语义
========
- ``OK``      字段必须逐值往返（嵌套结构按归一化口径深比较）。
- ``LOST``    字段确有语义但该链无法表达；fixture 里给出非默认值，并断言
              往返后**确实不同**（若将来支持了，本断言会红，提示升级为 OK）。
- ``INERT``   字段存在于模型但对本节点类型没有运行期/作者面（如集合节点从
              ``FlowNode`` 继承的 ``input``、终态 end 的 ``next``）；fixture
              不要求赋值。
- ``DERIVED`` pydantic validator 合成的字段（Bool/Case 的 ``branches``）或
              编译期派生的注解（``name``/``desc``/``source_line``），不作为
              作者输入比对。
- ``TYPE_LOST`` 整个节点类型在该链不可表达（@flow 的 switch/case 合成节点），
              该类型下所有字段按类型级登记，链调用必须 fail-closed 抛错。
"""
from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest

from plaita.dsl.codeflow import EmitError, compile_source, emit_source
from plaita.dsl.sexpr import SexprEmitError, compile_sexpr, flow_to_sexpr
from plaita.node import NodeRegistry
from plaita.node.concurrent import ParallelBranch
from plaita.node.decide import Branch

# ---------------------------------------------------------------------------
# 状态与视图
# ---------------------------------------------------------------------------

OK = "ok"
LOST = "lost"
INERT = "inert"
DERIVED = "derived"
TYPE_LOST = "type-lost"

FLOW = "flow"
SEXPR = "sexpr"

#: (flow 状态, sexpr 状态)
V = Tuple[str, str]

REGISTRY = NodeRegistry()
NODE_TYPES: Tuple[str, ...] = tuple(REGISTRY.list_types())


def _status(statuses: V, field: str, chain: str) -> str:
    """取 (flow, sexpr) 状态对里当前链的一项；``field`` 仅用于调用点可读性。"""
    return statuses[0 if chain == FLOW else 1]


# ---------------------------------------------------------------------------
# 节点字段矩阵（全节点类型 × 全字段 × 双链）
# ---------------------------------------------------------------------------

def _v(value: Any) -> V:
    """允许单状态（双链相同）或 ``(flow, sexpr)`` 状态对。"""
    if isinstance(value, tuple):
        assert len(value) == 2, value
        return value
    return (value, value)


def _base(
    *,
    output: str = OK,
    next: str = OK,
    timeout: str = OK,
    timeout_handler: str = OK,
    error_handler: str = OK,
    **extra: Any,
) -> Dict[str, V]:
    """Node 基类字段的默认状态 + 子类增量。

    ``name``/``desc``/``source_line`` 是编译期派生注解：@flow 的语句形态无法
    从源码反推原始注解（发射器按语义重算），sexpr 也不携带它们。两者都按
    ``DERIVED`` 登记，并在 fixture 里给出**不同的**非默认值以证明「确实不保真」。
    """
    row: Dict[str, V] = {
        "id": (OK, OK),
        "name": (DERIVED, DERIVED),
        "desc": (DERIVED, DERIVED),
        "source_line": (DERIVED, DERIVED),
        "output": _v(output),
        "next": _v(next),
        "timeout": _v(timeout),
        "timeout_handler": _v(timeout_handler),
        "error_handler": _v(error_handler),
    }
    row.update({key: _v(value) for key, value in extra.items()})
    return row


#: 每行 ``字段 -> (flow, sexpr)``。键必须覆盖 registry 每个节点的
#: ``model_fields``（``test_every_registry_field_is_classified`` 强制）。
MATRIX: Dict[str, Dict[str, V]] = {
    # start 由 @flow 隐式生成：源码里没有语句位可挂 timeout/handler/output。
    "start": _base(
        output=LOST,
        timeout=(LOST, OK),
        timeout_handler=(LOST, OK),
        error_handler=(LOST, OK),
    ),
    # end 现在由 `end(...)` 一等出口承载基类字段（#238）。
    # `next` 是终态边：引擎永不跟随，作者面也不存在——按 INERT 登记。
    "end": _base(next=(INERT, INERT), result_type=(OK, OK), error=(OK, OK)),
    "assignment": _base(
        output_type=(OK, OK),
        upstream_output=(OK, OK),
    ),
    # if 是合成节点：@flow 的 `if` 语句没有节点级 kwarg 位。
    # `output_type` 从 Switch 继承但对 Bool.execute 无意义。
    "if": _base(
        output=LOST,
        timeout=(LOST, OK),
        timeout_handler=(LOST, OK),
        error_handler=(LOST, OK),
        else_next=(OK, OK),
        condition=(OK, OK),
        branches=(DERIVED, DERIVED),
        output_type=(LOST, LOST),
    ),
    # switch/case 是 builder 专属合成节点：@flow 无语句形态，编译侧也不接受
    # 同名占位调用 ⇒ 类型级 fail-closed（两链均登记类型级，sexpr 实际支持）。
    "switch": _base(
        output=LOST,
        output_type=(TYPE_LOST, OK),
        branches=(TYPE_LOST, OK),
    ),
    "case": _base(
        output=LOST,
        output_type=(TYPE_LOST, OK),
        branches=(TYPE_LOST, DERIVED),
        target=(TYPE_LOST, OK),
        cases=(TYPE_LOST, OK),
        default=(TYPE_LOST, OK),
    ),
    "child": _base(output=LOST, input=(OK, OK), child_flow=(OK, OK)),
    "reference": _base(
        output=LOST,
        input=(OK, OK),
        child_flow=(OK, OK),
        # 命名引用（flow_id/flow_version）是调度器注入形状，@flow/sexpr 都
        # 只能表达内联 childflow；这两个字段是作者 API 决策项（白名单承载）。
        flow_id=(LOST, LOST),
        flow_version=(LOST, LOST),
    ),
    # 集合节点从 FlowNode 继承 input，但 execute 用 collection，不读它。
    "loop": _base(
        output=LOST,
        input=(LOST, LOST),
        collection=(OK, OK),
        item_type=(OK, OK),
        child_flow=(OK, OK),
        condition=(OK, OK),
    ),
    "map": _base(
        output=LOST,
        input=(LOST, LOST),
        collection=(OK, OK),
        item_type=(OK, OK),
        child_flow=(OK, OK),
        concurrent=(OK, OK),
        max_concurrent=(OK, OK),
    ),
    "filter": _base(
        output=LOST,
        input=(LOST, LOST),
        collection=(OK, OK),
        item_type=(OK, OK),
        child_flow=(OK, OK),
    ),
    "find": _base(
        output=LOST,
        input=(LOST, LOST),
        collection=(OK, OK),
        item_type=(OK, OK),
        child_flow=(OK, OK),
    ),
    "reduce": _base(
        output=LOST,
        input=(LOST, LOST),
        collection=(OK, OK),
        item_type=(OK, OK),
        child_flow=(OK, OK),
        initial=(OK, OK),
    ),
    "while": _base(
        output=LOST,
        input=(LOST, LOST),
        condition=(OK, OK),
        child_flow=(OK, OK),
        max_iterations=(OK, OK),
    ),
    "parallel": _base(
        output=LOST,
        branches=(OK, OK),
        mode=(OK, OK),
        join_branches=(OK, OK),
        is_conditional=(OK, OK),
    ),
    "http": _base(
        method=(OK, OK),
        content_type=(OK, OK),
        request_timeout=(OK, OK),
        allowed_hosts=(OK, OK),
        denied_hosts=(OK, OK),
        block_private_networks=(OK, OK),
        max_redirects=(OK, OK),
        url=(OK, OK),
        query=(OK, OK),
        headers=(OK, OK),
        body=(OK, OK),
        addressing=(OK, OK),
        delegate=(OK, OK),
    ),
    "event": _base(
        output=LOST,
        event_type=(OK, OK),
        event_filter=(OK, OK),
        subscription_timeout=(OK, OK),
    ),
    "mock": _base(
        value=(OK, OK),
    ),
}

#: CodeNode 不在默认 registry（执行任意代码，需显式 opt-in），但 @flow/sexpr
#: 都有 CODE/code 面。作为**额外覆盖**登记，不参与 registry 完整性检查。
EXTRA_NODE_TYPES: Tuple[str, ...] = ("code",)
CODE_MATRIX: Dict[str, V] = _base(
    output=LOST,
    language=(OK, OK),
    code=(OK, OK),
    input=(OK, OK),
    sandbox_backend=(OK, OK),
)

#: 归一化/覆盖检查统一的权威表（registry 全量 + CodeNode 额外覆盖）。
ALL_MATRIX: Dict[str, Dict[str, V]] = {**MATRIX, "code": CODE_MATRIX}

#: 该链整体不可表达的节点类型（@flow 没有 switch/case 语句形态）。
TYPE_UNEMITTABLE: Dict[str, Set[str]] = {
    FLOW: {"switch", "case"},
    SEXPR: set(),
}

#: Switch 分支（Branch）字段：sexpr 全支持；@flow 因 switch 类型级不可表达。
SWITCH_BRANCH_MATRIX: Dict[str, V] = {
    "name": (TYPE_LOST, OK),
    "condition": (TYPE_LOST, OK),
    "priority": (TYPE_LOST, OK),
    "label": (TYPE_LOST, OK),
    "desc": (TYPE_LOST, OK),
    "next": (TYPE_LOST, OK),
    "is_default": (TYPE_LOST, OK),
}

#: Parallel 分支字段：引擎只读 name/flow/input/condition；其余 Branch 继承
#: 字段在 Parallel.execute 里没有消费点（priority 排序只属于 Switch）。
PARALLEL_BRANCH_MATRIX: Dict[str, V] = {
    "name": (OK, OK),
    "flow": (OK, OK),
    "input": (OK, OK),
    "condition": (OK, OK),
    "priority": (LOST, LOST),
    "label": (LOST, LOST),
    "desc": (LOST, LOST),
    "is_default": (LOST, LOST),
    "next": (LOST, LOST),
}

#: 丢失/派生原因（矩阵行的可读解释；测试只要求存在条目，不解析文本）。
REASONS: Dict[Tuple[str, str, str], str] = {
    ("start", "timeout", FLOW): "@flow 的 start 隐式生成，无语句位",
    ("start", "timeout_handler", FLOW): "@flow 的 start 隐式生成，无语句位",
    ("start", "error_handler", FLOW): "@flow 的 start 隐式生成，无语句位",
    ("end", "next", FLOW): "终态节点，引擎不跟随 next",
    ("end", "next", SEXPR): "终态节点，引擎不跟随 next",
    ("if", "timeout", FLOW): "if 是语句，无节点级 kwarg 位",
    ("if", "timeout_handler", FLOW): "if 是语句，无节点级 kwarg 位",
    ("if", "error_handler", FLOW): "if 是语句，无节点级 kwarg 位",
    ("if", "output", FLOW): "Bool.execute 返回分支目标，不读 output",
    ("if", "output", SEXPR): "Bool.execute 返回分支目标，不读 output",
    ("if", "output_type", FLOW): "继承自 Switch，Bool 无输出类型语义",
    ("if", "output_type", SEXPR): "继承自 Switch，Bool 无输出类型语义",
    ("switch", "output", FLOW): "Switch.execute 返回分支名，不读 output",
    ("switch", "output", SEXPR): "Switch.execute 返回分支名，不读 output",
    ("case", "output", FLOW): "SwitchLegacy.execute 返回分支名，不读 output",
    ("case", "output", SEXPR): "SwitchLegacy.execute 返回分支名，不读 output",
    ("reference", "flow_id", FLOW): "命名引用是调度器注入形状，@flow 只表达内联子流程",
    ("reference", "flow_id", SEXPR): "命名引用是调度器注入形状，sexpr 只表达内联子流程",
    ("reference", "flow_version", FLOW): "命名引用是调度器注入形状，@flow 只表达内联子流程",
    ("reference", "flow_version", SEXPR): "命名引用是调度器注入形状，sexpr 只表达内联子流程",
    ("parallel", "branches.priority", FLOW): "Parallel 不消费 priority（无排序）",
    ("parallel", "branches.priority", SEXPR): "Parallel 不消费 priority（无排序）",
    ("parallel", "branches.label", FLOW): "展示元数据，Parallel 分支无语义",
    ("parallel", "branches.label", SEXPR): "展示元数据，Parallel 分支无语义",
    ("parallel", "branches.desc", FLOW): "展示元数据，Parallel 分支无语义",
    ("parallel", "branches.desc", SEXPR): "展示元数据，Parallel 分支无语义",
    ("parallel", "branches.is_default", FLOW): "Parallel 分支没有 default 语义",
    ("parallel", "branches.is_default", SEXPR): "Parallel 分支没有 default 语义",
    ("parallel", "branches.next", FLOW): "Parallel 分支没有 next 语义",
    ("parallel", "branches.next", SEXPR): "Parallel 分支没有 next 语义",
    ("code", "output", FLOW): "CodeNode.execute 返回代码结果，不读 output",
    ("code", "output", SEXPR): "CodeNode.execute 返回代码结果，不读 output",
}

#: 所有 ``output`` 在非输出承载节点上都是惰性字段（execute 不读）。
for _nt in ("start", "child", "reference", "loop", "map", "filter", "find",
            "reduce", "while", "parallel", "event", "code"):
    REASONS.setdefault((_nt, "output", FLOW), "节点 execute 不读 Node.output")
    REASONS.setdefault((_nt, "output", SEXPR), "节点 execute 不读 Node.output")
for _nt in ("loop", "map", "filter", "find", "reduce", "while"):
    REASONS.setdefault((_nt, "input", FLOW), "集合节点 execute 只读 collection")
    REASONS.setdefault((_nt, "input", SEXPR), "集合节点 execute 只读 collection")
for _nt in NODE_TYPES + EXTRA_NODE_TYPES:
    for _fld in ("name", "desc", "source_line"):
        REASONS.setdefault((_nt, _fld, FLOW), "编译期派生注解，不从源码反推")
        REASONS.setdefault((_nt, _fld, SEXPR), "编译期派生注解，sexpr 不携带")
for _nt in ("if", "case"):
    REASONS.setdefault((_nt, "branches", FLOW), "pydantic validator 合成字段")
    REASONS.setdefault((_nt, "branches", SEXPR), "pydantic validator 合成字段")

#: 类型级不可表达的统一理由：一条覆盖该类型下全部 @flow 单元。
REASONS[("switch", "__type__", FLOW)] = (
    "@flow 没有 switch 语句形态（builder 专属合成节点，发射 fail-closed）")
REASONS[("case", "__type__", FLOW)] = (
    "@flow 没有 case 语句形态（builder 专属合成节点，发射 fail-closed）")


# ---------------------------------------------------------------------------
# Fixtures：每个节点类型一份全字段 IR
# ---------------------------------------------------------------------------

#: 业务值刻意选「类型敏感」的拼写（"true"/"1"/"$..."），任何走裸原子/丢引号
#: 的发射路径都会在重读时漂移成 bool/int，从而被字段断言抓住。
_STR_TRUE = "true"
_STR_ONE = "1"

_HANDLER = {
    "strategy": "continue_with",
    "retryTimes": 3,
    "defaultValue": "$INPUT.seed",
    "errorCode": 9,
    "errorMessage": _STR_TRUE,
}
_TIMEOUT_HANDLER = {
    "strategy": "continue",
    "retryTimes": 2,
    "defaultValue": _STR_ONE,
    "errorCode": 7,
    "errorMessage": "timeout",
}


def _child(output: str = "$INPUT.item", array: bool = False,
           flow_id: str = "cf238") -> Dict[str, Any]:
    """内联子流程：同时覆盖 childflow 的 flow 级字段。"""
    return {
        "runtime": "python",
        "flow_id": flow_id,
        "inputType": {"dataType": "array" if array else "object"},
        "outputType": {"dataType": "object"},
        "desc": "子流程238",
        "nodes": [
            {"type": "start", "id": "cs", "next": "ce"},
            {"type": "end", "id": "ce", "output": output,
             "resultType": "success"},
        ],
    }


def _node(ntype: str, nid: str, **kw: Any) -> Dict[str, Any]:
    """全字段节点：基类可控字段一律给非默认值。"""
    node: Dict[str, Any] = {
        "type": ntype,
        "id": nid,
        "name": "节点名238",
        "desc": "节点描述238",
        "source_line": 20238,
        "output": "$INPUT.out",
        "timeout": "PT5S",
        "timeoutHandler": copy.deepcopy(_TIMEOUT_HANDLER),
        "errorHandler": copy.deepcopy(_HANDLER),
    }
    node.update(kw)
    return node


def _flow(nodes: List[Dict[str, Any]], **kw: Any) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "runtime": "python",
        "flow_id": "flow238",
        "inputType": {"dataType": "object"},
        "outputType": {"dataType": "object"},
        "desc": "根流程238",
        "version": "2.38",
        "author": "conformance",
        "timeout": "PT30S",
        "globalContext": {"g": _STR_TRUE, "n": 1},
        "metadata": {"m": _STR_ONE, "nested": {"k": _STR_TRUE}},
    }
    data.update(kw)
    data["nodes"] = nodes
    return data


def _start(nxt: str) -> Dict[str, Any]:
    return _node("start", "start", next=nxt)


def _end(nxt: Optional[str] = None) -> Dict[str, Any]:
    # output 是**表达式槽位**：用 $ 表达式而非外观像原子的字符串（"true"
    # 在表达式语义里就是 bool，不是数据漂移；数据字段才用 _STR_* 探针）。
    node = _node("end", "ret_done", output="$INPUT.out", resultType="success")
    node.pop("next", None)
    if nxt is not None:
        node["next"] = nxt
    return node


def _tail(nid: str) -> Dict[str, Any]:
    """目标节点之后的下游：一个引用它、再走到 end 的赋值。"""
    return _node("assignment", "after", output=f"$NODE.{nid}", next="ret_done")


def _fixtures() -> Dict[str, Dict[str, Any]]:
    f: Dict[str, Dict[str, Any]] = {}

    # start：显式全字段（timeout/handler 在 @flow 是登记丢失面）。
    f["start"] = _flow([_start("ret_done"), _end()])

    # end：error 出口 + 基类字段（end(...) 一等出口承载）。
    end_node = _node(
        "end", "ret_done", output="$INPUT.out", resultType="error",
        error={"code": 4001, "message": _STR_TRUE, _STR_ONE: "$literal"},
    )
    end_node.pop("next", None)
    f["end"] = _flow([_start("ret_done"), end_node])

    f["assignment"] = _flow([
        _start("x"),
        _node("assignment", "x", output="$INPUT.seed", next="a"),
        _node("assignment", "a", output="$INPUT.seed", next="ret_done",
              outputType={"dataType": "object", "name": "out", "label": "L",
                          "desc": "D", "isRequired": True,
                          "defaultValue": _STR_TRUE,
                          "itemType": {"dataType": "integer"},
                          "children": [{"dataType": "string", "name": "c"}]},
              upstreamOutput=[{"upstream": "x", "value": "$NODE.x"},
                              {"upstream": "y", "value": _STR_ONE}]),
        _end(),
    ])

    f["if"] = _flow([
        _start("i"),
        _node("if", "i",
              condition={"relation": "and", "conditions": [
                  {"field": "$INPUT.name", "operator": "contains",
                   "value": _STR_TRUE},
                  {"field": "$INPUT.name", "operator": "notContains",
                   "value": _STR_ONE},
                  {"field": "$INPUT.n", "operator": "gte", "value": 1},
              ]},
              next="yes", else_next="no", output_type={"dataType": "string"}),
        _node("assignment", "yes", output="yes", next="ret_done"),
        _node("assignment", "no", output="no", next="ret_done"),
        _end(),
    ])

    f["switch"] = _flow([
        _start("sw"),
        _node("switch", "sw", next="ret_done",
              output_type={"dataType": "string", "name": "branch"},
              branches=[
                  {"name": "a", "next": "a", "priority": 5,
                   "condition": {"field": "$INPUT.x", "operator": "eq",
                                 "value": _STR_ONE},
                   "label": "A", "desc": "da", "isDefault": False},
                  {"name": "d", "next": "ret_done", "priority": 1,
                   "label": "D", "desc": "dd", "isDefault": True},
              ]),
        _node("assignment", "a", output="A", next="ret_done"),
        _end(),
    ])

    f["case"] = _flow([
        _start("c"),
        _node("case", "c", next="ret_done", target="$INPUT.kind",
              output_type={"dataType": "string"},
              cases=[
                  {"id": "one", "value": _STR_ONE, "name": "ONE"},
                  {"id": "two", "value": 2},
              ], default="ret_done"),
        _node("assignment", "one", output="one", next="ret_done"),
        _node("assignment", "two", output="two", next="ret_done"),
        _end(),
    ])

    f["child"] = _flow([
        _start("ch"),
        _node("child", "ch", input="$INPUT.seed", childFlow=_child(),
              next="ret_done"),
        _end(),
    ])

    f["reference"] = _flow([
        _start("rf"),
        _node("reference", "rf", input="$INPUT.seed", childFlow=_child(),
              flow_id="external238", flow_version="v238", next="ret_done"),
        _end(),
    ])

    f["loop"] = _flow([
        _start("lp"),
        _node("loop", "lp", input="$PARENT.INPUT.x",
              collection="$INPUT.items",
              item_type={"dataType": "string", "label": "item"},
              condition={"relation": "or", "conditions": [
                  {"field": "$LOOP-ITEM.v", "operator": "gt", "value": 0},
                  {"field": "$LOOP-RESULT", "operator": "lt", "value": 8},
              ]}, childFlow=_child(), next="ret_done"),
        _end(),
    ])

    f["map"] = _flow([
        _start("m"),
        _node("map", "m", input="$PARENT.INPUT.x", collection="$INPUT.items",
              item_type={"dataType": "string"}, concurrent=True,
              maxConcurrent=4, childFlow=_child(), next="ret_done"),
        _end(),
    ])

    for ntype in ("filter", "find"):
        f[ntype] = _flow([
            _start(ntype),
            _node(ntype, ntype, input="$PARENT.INPUT.x",
                  collection="$INPUT.items",
                  item_type={"dataType": "string"},
                  childFlow=_child("$INPUT.item.v"), next="ret_done"),
            _end(),
        ])

    f["reduce"] = _flow([
        _start("r"),
        _node("reduce", "r", input="$PARENT.INPUT.x",
              collection="$INPUT.items",
              item_type={"dataType": "string"}, initial=0,
              childFlow=_child("$INPUT[0]", array=True), next="ret_done"),
        _end(),
    ])

    f["while"] = _flow([
        _start("w"),
        _node("while", "w", input="$PARENT.INPUT.x",
              condition={"field": "$LOOP-ITEM", "operator": "lt", "value": 6},
              maxIterations=9, childFlow=_child("$INPUT.item"),
              next="ret_done"),
        _end(),
    ])

    f["parallel"] = _flow([
        _start("p"),
        _node("parallel", "p", mode="thread",
              joinBranches=["a", "b"], isConditional=True,
              branches=[
                  {"name": "a", "flow": _child(), "input": "$INPUT.seed",
                   "condition": {"field": "$INPUT.flag", "operator": "truthy",
                                 "value": True},
                   "priority": 1, "label": "A", "desc": "da",
                   "isDefault": False, "next": "ret_done"},
                  {"name": "b", "input": _STR_TRUE,
                   "flow": _child("$F.add($INPUT.item, 1)", flow_id="cf238b")},
              ], next="ret_done"),
        _end(),
    ])

    f["http"] = _flow([
        _start("h"),
        _node("http", "h", method="PUT", url="https://example.com/x",
              content_type="text/plain", requestTimeout=12.5,
              allowedHosts=["example.com", "*.ok.test", "10.0.0.0/8"],
              deniedHosts=["bad.test"], blockPrivateNetworks=True,
              maxRedirects=3,
              query={"q": _STR_ONE, "flag": True},
              headers={"X-N": "42", "X-B": _STR_TRUE},
              body={"k": _STR_TRUE, "n": 7},
              addressing={"name": "domain"},
              delegate={"name": "proxy", "params": {"p": 1}},
              output="$INPUT.pick", next="ret_done"),
        _end(),
    ])

    f["event"] = _flow([
        _start("ev"),
        _node("event", "ev", eventType="order.paid",
              eventFilter={"data.status": _STR_TRUE, "n": 1, "b": True},
              subscriptionTimeout=7.5, next="ret_done"),
        _end(),
    ])

    f["mock"] = _flow([
        _start("mk"),
        _node("mock", "mk", value={"a": 1, "b": _STR_TRUE,
                                   "c": _STR_ONE}, output="$INPUT.seed",
              next="ret_done"),
        _end(),
    ])

    f["code"] = _flow([
        _start("cd"),
        _node("code", "cd", language="python", code=_STR_ONE,
              input="$INPUT.seed", sandbox_backend="restricted",
              next="ret_done"),
        _end(),
    ])

    return f


FIXTURES: Dict[str, Dict[str, Any]] = _fixtures()


# ---------------------------------------------------------------------------
# 归一化比较器
# ---------------------------------------------------------------------------

_KEY_ALIASES: Dict[str, str] = {
    "child_flow": "childFlow",
    "max_iterations": "maxIterations",
    "timeout_handler": "timeoutHandler",
    "error_handler": "errorHandler",
    "result_type": "resultType",
    "output_type": "outputType",
    "upstream_output": "upstreamOutput",
    "event_type": "eventType",
    "event_filter": "eventFilter",
    "subscription_timeout": "subscriptionTimeout",
    "allowed_hosts": "allowedHosts",
    "denied_hosts": "deniedHosts",
    "block_private_networks": "blockPrivateNetworks",
    "request_timeout": "requestTimeout",
    "max_redirects": "maxRedirects",
    "is_conditional": "isConditional",
    "join_branches": "joinBranches",
    "content_type": "contentType",
    "item_type": "itemType",
    "is_default": "isDefault",
}

_IR_KEY: Dict[str, str] = {field: alias for field, alias in _KEY_ALIASES.items()}

_DERIVED_KEYS = ("name", "desc", "source_line")
_NODE_REF_RE = re.compile(r"\$NODE\.([A-Za-z_]\w*)")
_SCOPE_KEYS = ("childFlow", "child_flow")


def _ir_key(field: str) -> str:
    return _IR_KEY.get(field, field)


def _raw_get(node: Dict[str, Any], field: str) -> Any:
    """读原始（未归一化）节点字段：canonical 拼写优先，fixture 常见 snake 兜底。"""
    for key in (_ir_key(field), field):
        if key in node:
            return node[key]
    return node.get(_ir_key(field))


def _canonical_node(ntype: str, node: Dict[str, Any], chain: str,
                    registry: Dict[str, Dict[str, V]]) -> None:
    """就地去掉该链登记为 LOST/DERIVED 的字段并规范化键拼写。"""
    for key in list(node):
        if key in _DERIVED_KEYS:
            node.pop(key, None)
            continue
        canonical = _KEY_ALIASES.get(key, key)
        if canonical != key:
            node[canonical] = node.pop(key)
            key = canonical
        if node[key] is None:
            node.pop(key)
    row = registry.get(ntype, {})
    for field, statuses in row.items():
        if _status(statuses, field, chain) in (LOST, DERIVED):
            node.pop(_ir_key(field), None)
    # case.cases[].name 的默认值 = str(value)：读侧总会补，写侧按「等于默认
    # 就不发」发射。两侧都按默认归一，避免把派生值当成用户字段。
    if ntype == "case":
        for case in node.get("cases") or []:
            if isinstance(case, dict) and case.get("name") == str(case.get("value")):
                case.pop("name", None)
    # switch/parallel 分支的 isDefault=False 与缺席等价。
    for branch in node.get("branches") or []:
        if isinstance(branch, dict) and branch.get("isDefault") is False:
            branch.pop("isDefault", None)


def _canonical_ids(scope: Dict[str, Any]) -> Dict[str, Any]:
    """非语义 id 按 BFS 图序规范化（start/if/return 的 slug id 允许重编）。

    ``$NODE.<id>`` 引用到的 id 是语义 id，保持原名——这也钉住了「下游引用
    的节点 id 必须可复现」这一条。
    """
    nodes = {n["id"]: n for n in scope.get("nodes", [])}
    referenced: Set[str] = set()

    def _scan(value: Any) -> None:
        if isinstance(value, str):
            referenced.update(_NODE_REF_RE.findall(value))
        elif isinstance(value, dict):
            for v in value.values():
                _scan(v)
        elif isinstance(value, list):
            for v in value:
                _scan(v)

    for node in scope.get("nodes", []):
        for key, value in node.items():
            if key in ("id", "next", "else_next", "type"):
                continue
            _scan(value)

    # 分支/跳转表里的节点 id 也是语义引用：switch branch.next、case 的
    # cases[].id / default 必须和普通 next 一样参与规范化（否则首跳目标会被
    # 当成「无人引用」重编，两侧对不上）。
    for node in scope.get("nodes", []):
        ntype = node.get("type")
        if ntype == "switch":
            for branch in node.get("branches") or []:
                if isinstance(branch, dict):
                    target = branch.get("next") or branch.get("name")
                    if isinstance(target, str):
                        referenced.add(target)
        elif ntype == "case":
            for case in node.get("cases") or []:
                if isinstance(case, dict) and isinstance(case.get("id"), str):
                    referenced.add(case["id"])
            if isinstance(node.get("default"), str):
                referenced.add(node["default"])

    def _targets(node: Dict[str, Any]) -> List[str]:
        out = []
        for arc in (node.get("next"), node.get("else_next")):
            if arc:
                out.append(arc)
        ntype = node.get("type")
        if ntype == "switch":
            for branch in node.get("branches") or []:
                if isinstance(branch, dict):
                    target = branch.get("next") or branch.get("name")
                    if isinstance(target, str):
                        out.append(target)
        elif ntype == "case":
            for case in node.get("cases") or []:
                if isinstance(case, dict) and isinstance(case.get("id"), str):
                    out.append(case["id"])
            if isinstance(node.get("default"), str):
                out.append(node["default"])
        return out

    remap: Dict[str, str] = {}
    counter = 0
    start = next((n for n in nodes.values() if n.get("type") == "start"), None)
    queue = [start["id"]] if start else []
    while queue:
        nid = queue.pop(0)
        if nid in remap or nid not in nodes:
            continue
        if nid in referenced:
            remap[nid] = nid
        else:
            remap[nid] = f"c{counter}"
            counter += 1
        node = nodes[nid]
        queue.extend(a for a in _targets(node) if a)
    for node in scope.get("nodes", []):
        node["id"] = remap.get(node["id"], node["id"])
        if node.get("next"):
            node["next"] = remap.get(node["next"], node["next"])
        if node.get("else_next"):
            node["else_next"] = remap.get(node["else_next"], node["else_next"])
        ntype = node.get("type")
        if ntype == "switch":
            for branch in node.get("branches") or []:
                if isinstance(branch, dict) and branch.get("next"):
                    branch["next"] = remap.get(branch["next"], branch["next"])
        elif ntype == "case":
            for case in node.get("cases") or []:
                if isinstance(case, dict) and case.get("id"):
                    case["id"] = remap.get(case["id"], case["id"])
            if node.get("default"):
                node["default"] = remap.get(node["default"], node["default"])
        for scope_key in _SCOPE_KEYS:
            inner = node.get(scope_key)
            if isinstance(inner, dict):
                node[scope_key] = _canonical_ids(inner)
        for branch in node.get("branches") or []:
            if isinstance(branch, dict) and isinstance(branch.get("flow"), dict):
                branch["flow"] = _canonical_ids(branch["flow"])
    scope["nodes"] = sorted(scope["nodes"], key=lambda n: n["id"])
    return scope


def _child_flow_fields(owner: Optional[str]) -> Dict[str, V]:
    """内联子流程的 flow 级字段视图。

    ``CHILD``/``REFERENCE``/``PARALLEL`` 用 ``@childflow(flow_id, desc=...)``
    命名块发射，flow_id/desc 有表达面；集合节点/while 的子流程是**内联在
    for 语句体里**的语句序列，没有装饰器位（flow_id/desc 在 @flow 链丢失，
    sexpr 链仍保留）。
    """
    if owner in ("child", "reference", "parallel"):
        return CHILD_FLOW_FIELDS_REF
    return CHILD_FLOW_FIELDS_INLINE


def _normalize(scope: Dict[str, Any], chain: str,
               registry: Optional[Dict[str, Dict[str, V]]] = None,
               flow_fields: Optional[Dict[str, V]] = None) -> Dict[str, Any]:
    """整树归一化：剥派生注解、别名拼写、登记丢失字段、非语义 id。"""
    registry = ALL_MATRIX if registry is None else registry
    flow_fields = ROOT_FIELDS if flow_fields is None else flow_fields
    scope = copy.deepcopy(scope)
    for field, statuses in flow_fields.items():
        if _status(statuses, field, chain) != OK:
            scope.pop(field, None)
    for node in scope.get("nodes", []):
        ntype = node.get("type")
        _canonical_node(ntype, node, chain, registry)
        child_fields = _child_flow_fields(ntype)
        for scope_key in _SCOPE_KEYS:
            inner = node.get(scope_key)
            if isinstance(inner, dict):
                node[scope_key] = _normalize(
                    inner, chain, registry, child_fields)
        for branch in node.get("branches") or []:
            if isinstance(branch, dict) and isinstance(branch.get("flow"), dict):
                branch["flow"] = _normalize(
                    branch["flow"], chain, registry, CHILD_FLOW_FIELDS_REF)
            if isinstance(branch, dict):
                _canonical_branch(ntype, branch, chain)
    return _canonical_ids(scope)


def _canonical_branch(ntype: str, branch: Dict[str, Any], chain: str) -> None:
    if ntype == "parallel":
        for field, statuses in PARALLEL_BRANCH_MATRIX.items():
            if _status(statuses, field, chain) in (LOST, DERIVED):
                branch.pop(_ir_key(field), None)
    elif ntype == "switch":
        for field, statuses in SWITCH_BRANCH_MATRIX.items():
            if _status(statuses, field, chain) in (LOST, DERIVED):
                branch.pop(_ir_key(field), None)
    if branch.get("isDefault") is False:
        branch.pop("isDefault", None)


# ---------------------------------------------------------------------------
# 双链执行
# ---------------------------------------------------------------------------

def _emit_and_compile(ir: Dict[str, Any], chain: str) -> Dict[str, Any]:
    if chain == FLOW:
        return compile_source(emit_source(copy.deepcopy(ir)))
    return compile_sexpr(flow_to_sexpr(copy.deepcopy(ir)))


def _node_of(ir: Dict[str, Any], ntype: str) -> Dict[str, Any]:
    matches = [n for n in ir.get("nodes", []) if n.get("type") == ntype]
    assert matches, f"往返后找不到节点类型 {ntype!r}: {ir}"
    return matches[0]


# ---------------------------------------------------------------------------
# 测试：矩阵完整性 / 字段覆盖 / 双链深等价
# ---------------------------------------------------------------------------

def _missing_classifications(
    registry: Optional[NodeRegistry] = None,
    matrix: Optional[Dict[str, Dict[str, V]]] = None,
) -> List[str]:
    """harness 的权威检查本体：registry 每个 model_field 必须有矩阵条目。

    抽成函数是为了让负控**真正调用**这条链路，而不是复写一遍判断。
    """
    reg = REGISTRY if registry is None else registry
    table = ALL_MATRIX if matrix is None else matrix
    missing: List[str] = []
    for ntype in reg.list_types():
        cls = reg.get(ntype)
        row = table.get(ntype, {})
        for field in cls.model_fields:
            if field not in row:
                missing.append(f"{ntype}.{field}")
    return missing


class TestMatrixCompleteness:
    """registry 是权威清单：新节点/新字段必须显式进入矩阵。"""

    def test_every_registry_node_type_has_a_fixture(self):
        missing = sorted(set(NODE_TYPES) - set(FIXTURES))
        assert missing == [], f"registry 节点类型缺 fixture: {missing}"

    def test_every_fixture_node_type_is_known(self):
        unknown = sorted(set(FIXTURES) - set(NODE_TYPES) - set(EXTRA_NODE_TYPES))
        assert unknown == [], f"fixture 含未登记节点类型: {unknown}"

    def test_every_registry_field_is_classified(self):
        """反射每个节点类的 ``model_fields``，逐字段要求矩阵条目。"""
        missing = _missing_classifications()
        code_cls = __import__("plaita.node", fromlist=["CodeNode"]).CodeNode
        missing += [f"code.{field}" for field in code_cls.model_fields
                    if field not in CODE_MATRIX]
        assert missing == [], (
            "registry 新增字段未进入 #238 conformance 矩阵（先分类为 OK/LOST/"
            f"INERT/DERIVED 再实现或登记）: {missing}")

    def test_matrix_has_no_stale_fields(self):
        stale: List[str] = []
        for ntype, row in ALL_MATRIX.items():
            cls = (REGISTRY.get(ntype) if ntype in NODE_TYPES
                   else __import__("plaita.node", fromlist=["CodeNode"]).CodeNode)
            for field in row:
                if field not in cls.model_fields:
                    stale.append(f"{ntype}.{field}")
        assert stale == [], f"矩阵含模型已不存在的字段: {stale}"

    def test_branch_models_are_classified(self):
        assert set(Branch.model_fields) == set(SWITCH_BRANCH_MATRIX)
        assert set(ParallelBranch.model_fields) == set(PARALLEL_BRANCH_MATRIX)
        assert "flow" not in Branch.model_fields
        assert "flow" in ParallelBranch.model_fields

    def test_every_non_ok_cell_has_a_reason(self):
        cells: List[Tuple[str, str, str, str]] = []
        for ntype, row in ALL_MATRIX.items():
            for field, (flow_st, sexpr_st) in row.items():
                if flow_st != OK:
                    cells.append((ntype, field, FLOW, flow_st))
                if sexpr_st != OK:
                    cells.append((ntype, field, SEXPR, sexpr_st))
        for field, (flow_st, sexpr_st) in PARALLEL_BRANCH_MATRIX.items():
            if flow_st != OK:
                cells.append(("parallel", f"branches.{field}", FLOW, flow_st))
            if sexpr_st != OK:
                cells.append(("parallel", f"branches.{field}", SEXPR, sexpr_st))
        for field, (flow_st, sexpr_st) in SWITCH_BRANCH_MATRIX.items():
            if flow_st != OK:
                cells.append(("switch", f"branches.{field}", FLOW, flow_st))
            if sexpr_st != OK:
                cells.append(("switch", f"branches.{field}", SEXPR, sexpr_st))

        def _has_reason(ntype: str, field: str, chain: str, status: str) -> bool:
            if (ntype, field, chain) in REASONS:
                return True
            # 类型级不可表达：一条类型级理由覆盖全部字段单元。
            return (status == TYPE_LOST
                    and (ntype, "__type__", chain) in REASONS)

        missing = [c for c in cells if not _has_reason(*c)]
        assert missing == [], f"非 OK 单元缺原因说明: {missing}"

    def test_harness_detects_unregistered_new_field(self):
        """负控：动态给节点模型加字段，覆盖检查必须报红。

        用一个真实注册的临时节点类（而不是只测辅助函数），证明 registry
        → 矩阵的反射链路没有空转。
        """
        from plaita.node import Node

        class Issue238ProbeNode(Node):
            node_type = "issue238_probe"
            extra_field: str = "x"

        reg = NodeRegistry()
        reg.register(Issue238ProbeNode)
        missing = _missing_classifications(reg, ALL_MATRIX)
        assert "issue238_probe.extra_field" in missing, (
            "负控失败：新增字段没有触发矩阵缺失（harness 会静默放过新字段）")


class TestNodeFieldConformance:
    """每个节点类型 × 每个字段：OK 必保真、LOST 必仍丢失。"""

    @pytest.mark.parametrize("ntype", sorted(FIXTURES))
    def test_all_fields(self, ntype):
        fixture = FIXTURES[ntype]
        expected_node = _node_of(fixture, ntype)
        row = ALL_MATRIX[ntype]
        for chain in (FLOW, SEXPR):
            # 类型级不可表达：发射必须 fail-closed（不是产出丢字段的源码）。
            if ntype in TYPE_UNEMITTABLE[chain]:
                with pytest.raises((EmitError, SexprEmitError)):
                    _emit_and_compile(fixture, chain)
                continue
            actual_ir = _emit_and_compile(fixture, chain)
            # OK 字段在**归一化视图**上比较：id/next 允许按图序重编，别名拼写
            # 统一，childFlow 递归归一——否则「end 的 slug id 变了」会把
            # start.next 误报成字段漂移。
            expected_norm = _node_of(_normalize(fixture, chain), ntype)
            actual_norm = _node_of(_normalize(actual_ir, chain), ntype)
            actual_raw = _node_of(actual_ir, ntype)
            for field, statuses in row.items():
                status = _status(statuses, field, chain)
                if status == INERT:
                    continue
                ir_key = _ir_key(field)
                if status == OK:
                    expected = expected_norm.get(ir_key)
                    actual = actual_norm.get(ir_key)
                    assert actual == expected, (
                        f"{ntype}.{field} [{chain}] 往返丢失/漂移\n"
                        f"expected={expected!r}\nactual={actual!r}")
                elif status == DERIVED:
                    continue
                elif status == LOST:
                    expected = _raw_get(expected_node, field)
                    actual = _raw_get(actual_raw, field)
                    if expected is None:
                        # fixture 没给该字段（如只有分支 a 设 priority）：
                        # 没有「必须丢失」的可观测值。
                        continue
                    assert actual != expected, (
                        f"{ntype}.{field} [{chain}] 登记为 {status}，但往返后"
                        f"值仍然相等（{expected!r}）——该链已支持此字段，"
                        "请把矩阵状态升级为 OK 并加回归断言")

    def test_flow_switch_and_case_fail_closed(self):
        """switch/case 在 @flow 没有语句形态：必须是 EmitError，而不是坏源码。"""
        for ntype in ("switch", "case"):
            with pytest.raises(EmitError):
                emit_source(copy.deepcopy(FIXTURES[ntype]))

    def test_sexpr_while_is_supported(self):
        ir = FIXTURES["while"]
        back = compile_sexpr(flow_to_sexpr(copy.deepcopy(ir)))
        node = _node_of(back, "while")
        assert node["condition"] == _node_of(ir, "while")["condition"]
        assert node["maxIterations"] == 9
        assert node.get("next") == "ret_done"


class TestDeepEquality:
    """整树深等价：归一化（剥派生/别名/登记丢失/非语义 id）后逐树相等。"""

    @pytest.mark.parametrize("ntype", sorted(FIXTURES))
    def test_json_flow_json_and_json_sexpr_json(self, ntype):
        fixture = FIXTURES[ntype]
        for chain in (FLOW, SEXPR):
            if ntype in TYPE_UNEMITTABLE[chain]:
                continue
            actual = _emit_and_compile(fixture, chain)
            left = _normalize(fixture, chain)
            right = _normalize(actual, chain)
            assert left == right, (
                f"{ntype} [{chain}] JSON→DSL→JSON 深等价失败\n"
                f"left={left!r}\nright={right!r}")


# ---------------------------------------------------------------------------
# Flow 级字段（根流程 / 内联子流程）
# ---------------------------------------------------------------------------

ROOT_FIELDS: Dict[str, V] = {
    "flow_id": (OK, OK),
    "runtime": (OK, OK),
    "inputType": (OK, OK),
    # @flow 的 output_type 是显式废弃并忽略的装饰器参数（_source.py
    # DeprecationWarning），sexpr 是唯一可表达链——作者 API 决策项。
    "outputType": (LOST, OK),
    "desc": (OK, OK),
    "version": (OK, OK),
    "author": (OK, OK),
    "timeout": (OK, OK),
    "globalContext": (OK, OK),
    "metadata": (OK, OK),
}

#: `CHILD`/`REFERENCE`/`PARALLEL` 的命名 `@childflow` 块：flow_id/desc 有面。
CHILD_FLOW_FIELDS_REF: Dict[str, V] = {
    "flow_id": (OK, OK),
    "runtime": (OK, OK),
    "inputType": (OK, OK),
    # @childflow(...) 只接受 flow_id / desc；输出类型声明没有作者面。
    "outputType": (LOST, OK),
    "desc": (OK, OK),
    # @childflow 装饰器与 sexpr childflow 都不接受这些根流程字段。
    "version": (INERT, INERT),
    "author": (INERT, INERT),
    "timeout": (INERT, INERT),
    "globalContext": (INERT, INERT),
    "metadata": (INERT, INERT),
}

#: 集合节点/while 的内联子流程：整段内联进 for 语句体，没有装饰器位——
#: flow_id/desc 在 @flow 链同样丢失（sexpr 的 `(childflow …)` 仍保留）。
CHILD_FLOW_FIELDS_INLINE: Dict[str, V] = {
    **CHILD_FLOW_FIELDS_REF,
    "flow_id": (LOST, OK),
    "desc": (LOST, OK),
}

#: 兼容别名（TestFlowLevelConformance 的 CHILD/REFERENCE 视图）。
CHILD_FLOW_FIELDS = CHILD_FLOW_FIELDS_REF


class TestFlowLevelConformance:
    def test_root_flow_fields(self):
        ir = FIXTURES["http"]
        for chain in (FLOW, SEXPR):
            actual = _emit_and_compile(ir, chain)
            for field, (flow_st, sexpr_st) in ROOT_FIELDS.items():
                status = flow_st if chain == FLOW else sexpr_st
                expected = ir.get(field)
                got = actual.get(field)
                if status == OK:
                    assert got == expected, (
                        f"根流程 {field} [{chain}] 往返不等: "
                        f"expected={expected!r} actual={got!r}")
                elif status == LOST:
                    assert got != expected, (
                        f"根流程 {field} [{chain}] 登记为 LOST，但往返后仍保真"
                        "（升级矩阵为 OK）")

    def _assert_child_flow_fields(self, ntype: str,
                                  fields: Dict[str, V]) -> None:
        ir = FIXTURES[ntype]
        child = _node_of(ir, ntype)["childFlow"]
        for chain in (FLOW, SEXPR):
            actual = _emit_and_compile(ir, chain)
            # 归一化后统一是 camel `childFlow`（while 的编译产物用 snake）。
            got_child = _node_of(_normalize(actual, chain), ntype)["childFlow"]
            for field, (flow_st, sexpr_st) in fields.items():
                status = flow_st if chain == FLOW else sexpr_st
                if status == INERT:
                    continue
                expected = child.get(field)
                got = got_child.get(field)
                if status == OK:
                    assert got == expected, (
                        f"{ntype} 子流程 {field} [{chain}] 往返不等: "
                        f"expected={expected!r} actual={got!r}")
                elif status == LOST:
                    if expected is None:
                        continue
                    assert got != expected, (
                        f"{ntype} 子流程 {field} [{chain}] 登记为 LOST，"
                        "但往返后仍保真（升级矩阵为 OK）")

    def test_child_flow_fields(self):
        self._assert_child_flow_fields("child", CHILD_FLOW_FIELDS_REF)

    def test_inline_child_flow_fields(self):
        """集合/while 的内联子流程：@flow 没有装饰器位，flow_id/desc 丢失；
        sexpr 的 `(childflow …)` 仍可表达。"""
        for ntype in ("loop", "map", "filter", "find", "reduce", "while"):
            self._assert_child_flow_fields(ntype, CHILD_FLOW_FIELDS_INLINE)


# ---------------------------------------------------------------------------
# 分支字段 conformance
# ---------------------------------------------------------------------------

class TestBranchConformance:
    def test_switch_branch_fields(self):
        ir = FIXTURES["switch"]
        expected = _node_of(ir, "switch")["branches"]
        actual_ir = _emit_and_compile(ir, SEXPR)
        actual = _node_of(actual_ir, "switch")["branches"]
        assert len(actual) == len(expected)
        for exp, got in zip(expected, actual):
            for field, (flow_st, sexpr_st) in SWITCH_BRANCH_MATRIX.items():
                key = _ir_key(field)
                if field == "is_default":
                    # False 与缺席等价（reader 只写 true）。
                    assert bool(got.get(key)) == bool(exp.get(key)), (
                        f"switch 分支 is_default [sexpr] 往返不等")
                elif sexpr_st == OK:
                    assert got.get(key) == exp.get(key), (
                        f"switch 分支 {field} [sexpr] 往返不等: "
                        f"expected={exp.get(key)!r} actual={got.get(key)!r}")
                else:
                    assert got.get(key) != exp.get(key), (
                        f"switch 分支 {field} [sexpr] 应仍丢失")

    def test_parallel_branch_fields(self):
        ir = FIXTURES["parallel"]
        expected = _node_of(ir, "parallel")["branches"]
        for chain in (FLOW, SEXPR):
            actual_ir = _emit_and_compile(ir, chain)
            actual = _node_of(actual_ir, "parallel")["branches"]
            assert len(actual) == len(expected)
            for exp, got in zip(expected, actual):
                for field, (flow_st, sexpr_st) in PARALLEL_BRANCH_MATRIX.items():
                    status = flow_st if chain == FLOW else sexpr_st
                    key = _ir_key(field)
                    if status == OK:
                        if field == "flow":
                            assert (_normalize(exp[key], chain)
                                    == _normalize(got[key], chain)), (
                                f"parallel 分支 flow [{chain}] 往返不等")
                        else:
                            assert got.get(key) == exp.get(key), (
                                f"parallel 分支 {field} [{chain}] 往返不等: "
                                f"expected={exp.get(key)!r} actual={got.get(key)!r}")
                    elif status == LOST:
                        exp_value = _raw_get(exp, field)
                        if exp_value is None:
                            continue  # fixture 未给该分支设值
                        assert _raw_get(got, field) != exp_value, (
                            f"parallel 分支 {field} [{chain}] 应仍丢失")


# ---------------------------------------------------------------------------
# 负控：证明具体修复有区分力（恢复旧行为会红）
# ---------------------------------------------------------------------------

class TestNegativeControls:
    """每个夹具都钉住一个已修的真实缺口；这里用「链路断点」复现红。"""

    def test_while_base_fields_would_be_lost_without_common_kwargs(self):
        """负控：`_emit_while` 不调用 `_common_kwargs` 时 timeout 必丢。"""
        from plaita.dsl.codeflow import _emit as emit_mod

        original = emit_mod._BodyEmitter._common_kwargs

        def drop_common(self, spec):
            return []

        emit_mod._BodyEmitter._common_kwargs = drop_common
        try:
            back = compile_source(emit_source(copy.deepcopy(FIXTURES["while"])))
            assert _node_of(back, "while").get("timeout") is None
        finally:
            emit_mod._BodyEmitter._common_kwargs = original

    def test_http_content_type_would_be_lost_without_dual_read(self):
        """负控：HTTP 发射器不读 content_type 时字段必丢。"""
        from plaita.dsl.codeflow import _emit as emit_mod

        original = emit_mod._BodyEmitter._call_http
        emitter = emit_mod._BodyEmitter

        def stripped_call_http(self, node):
            node = dict(node)
            node.pop("content_type", None)
            node.pop("contentType", None)
            return original(self, node)

        emit_mod._BodyEmitter._call_http = stripped_call_http
        try:
            back = compile_source(emit_source(copy.deepcopy(FIXTURES["http"])))
            assert _node_of(back, "http").get("content_type") is None
        finally:
            emit_mod._BodyEmitter._call_http = original

    def test_sexpr_timeout_handler_would_be_lost_without_on_timeout(self):
        """负控：sexpr 写侧不发 `:on-timeout` 时超时策略必丢。"""
        from plaita.dsl import sexpr as sexpr_mod

        original = sexpr_mod._common_kw_to_src

        def no_timeout(n):
            return [p for p in original(n) if not p.startswith(":on-timeout")]

        sexpr_mod._common_kw_to_src = no_timeout
        try:
            back = compile_sexpr(flow_to_sexpr(copy.deepcopy(FIXTURES["http"])))
            assert _node_of(back, "http").get("timeoutHandler") is None
        finally:
            sexpr_mod._common_kw_to_src = original

    def test_contains_condition_would_fail_without_f_contains_mapping(self):
        """负控：`_invert_cond` 不认 contains 时发射直接 EmitError。"""
        from plaita.dsl.codeflow import _emit as emit_mod

        original = emit_mod._invert_cond

        def old_ops(cond, ns):
            if isinstance(cond, dict) and cond.get("operator") in (
                    "contains", "notContains"):
                raise emit_mod.EmitError(
                    f"不支持的条件算子 {cond.get('operator')!r}")
            return original(cond, ns)

        emit_mod._invert_cond = old_ops
        try:
            with pytest.raises(EmitError):
                emit_source(copy.deepcopy(FIXTURES["if"]))
        finally:
            emit_mod._invert_cond = original
