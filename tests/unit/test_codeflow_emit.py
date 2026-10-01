"""IR → ``@flow`` 源码反向发射器（``emit_source``）。

核心口径：``compile_source(emit_source(ir))`` 与 ``ir`` **语义等价**——

- 严格口径（源码产物 IR）：剥派生注解（name/desc/source_line）后**逐字段相等**。
  节点 id 全部可复现：变量名 id 与显式 ``id=`` 钉住的 id 恒定，if/return 的
  slug id 由同形表达式再生，无名节点 ``_n{k}`` 计数序与编译序一致。
- 宽松口径（任意来源 JSON IR）：非语义 id（未被 ``$NODE.`` 引用、非变量名）
  按 BFS 图序规范化后相等。手写 JSON 的裸节点 id 无从钉源，只保语义。

负面：switch/bool 合成节点、非 object inputType、空真分支 if → ``EmitError``。
"""
from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Set

from plaita import Node
from plaita.dsl.codeflow import EmitError, compile_source, emit_source
from plaita.node import NodeRegistry, get_default_registry
from typing import Any as _Any, ClassVar, Optional


# ---------------------------------------------------------------------------
# 归一化比较器
# ---------------------------------------------------------------------------

_DERIVED_KEYS = ("name", "desc", "source_line")
_NODE_REF_RE = re.compile(r"\$NODE\.([A-Za-z_]\w*)")
_INLINE_SCOPE_KEYS = ("childFlow", "child_flow")


def _strip_derived(scope: Dict[str, Any]) -> None:
    """递归剥派生注解（节点级 name/desc/source_line）。"""
    for node in scope.get("nodes", []):
        for key in _DERIVED_KEYS:
            node.pop(key, None)
        for scope_key in _INLINE_SCOPE_KEYS:
            inner = node.get(scope_key)
            if isinstance(inner, dict):
                _strip_derived(inner)
        for branch in node.get("branches") or []:
            if isinstance(branch.get("flow"), dict):
                _strip_derived(branch["flow"])


def _referenced_vars(scope: Dict[str, Any]) -> Set[str]:
    """本作用域表达式里被 ``$NODE.<v>`` 引用的变量名集合（语义 id，不许重命名）。"""
    found: Set[str] = set()

    def _scan(value: Any) -> None:
        if isinstance(value, str):
            found.update(_NODE_REF_RE.findall(value))
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
    return found


def _canonicalize(scope: Dict[str, Any]) -> Dict[str, Any]:
    """非语义 id 规范化：未被 ``$NODE.`` 引用的 id（含 start/end 的命名 id）按
    BFS 图序重编为 ``c0..cN``；childFlow 子作用域独立规范化。返回深拷贝。"""
    scope = copy.deepcopy(scope)
    _strip_derived(scope)
    nodes: Dict[str, Dict[str, Any]] = {n["id"]: n for n in scope.get("nodes", [])}
    referenced = _referenced_vars(scope)
    remap: Dict[str, str] = {}
    counter = 0
    start = next((n for n in nodes.values() if n.get("type") == "start"), None)
    queue = [start["id"]] if start else []
    while queue:
        nid = queue.pop(0)
        if nid in remap or nid not in nodes:
            continue
        node = nodes[nid]
        if nid in referenced:
            remap[nid] = nid
        else:
            remap[nid] = f"c{counter}"
            counter += 1
        queue.extend(a for a in (node.get("next"), node.get("else_next")) if a)
    for node in scope.get("nodes", []):
        node["id"] = remap.get(node["id"], node["id"])
        if node.get("next"):
            node["next"] = remap.get(node["next"], node["next"])
        if node.get("else_next"):
            node["else_next"] = remap.get(node["else_next"], node["else_next"])
        for scope_key in _INLINE_SCOPE_KEYS:
            inner = node.get(scope_key)
            if isinstance(inner, dict):
                node[scope_key] = _canonicalize(inner)
        for branch in node.get("branches") or []:
            if isinstance(branch.get("flow"), dict):
                branch["flow"] = _canonicalize(branch["flow"])
    scope["nodes"] = sorted(scope["nodes"], key=lambda n: n["id"])
    return scope


def assert_roundtrip(src: str, **opts: Any) -> Dict[str, Any]:
    """源码 → IR → 源码 → IR：归一化口径（剥派生注解 + 非语义 id 规范化）。

    语义 id（变量名、显式 ``id=``、``$NODE.`` 引用到的 id）在规范化中保持原名，
    故本口径仍然钉死语义引用；仅 ``_n{k}`` 计数 id 允许按图序重编——图同构但
    语句形态不同（fall-through return vs 显式 else）的编译序差异不属语义差异。
    """
    ir1 = compile_source(src, **opts)
    emitted = emit_source(ir1)
    ir2 = compile_source(emitted)
    left = _canonicalize(ir1)
    right = _canonicalize(ir2)
    assert left == right, f"round-trip 不等\n--- emitted ---\n{emitted}\n--- left ---\n{left}\n--- right ---\n{right}"
    return emitted


# ---------------------------------------------------------------------------
# 测试用自定义节点
# ---------------------------------------------------------------------------

class EmitEchoNode(Node):
    node_type: ClassVar[str] = "codeflow_emit_echo"
    text: Optional[str] = None

    def execute(self, execution):
        return str(execution.evaluate(self.text) if self.text else "")


class EmitSumNode(Node):
    node_type: ClassVar[str] = "codeflow_emit_sum"
    a: Optional[_Any] = None
    b: Optional[_Any] = None

    def execute(self, execution):
        return (execution.evaluate(self.a) or 0) + (execution.evaluate(self.b) or 0)


_reg = get_default_registry()
_reg.register(EmitEchoNode)
_reg.register(EmitSumNode)


# ---------------------------------------------------------------------------
# round-trip 语料
# ---------------------------------------------------------------------------

def test_roundtrip_linear_http():
    emitted = assert_roundtrip('''
@flow("demo", desc="实验")
def demo(INPUT):
    r = HTTP.get(url="https://api.example.com")
    return F.concat(INPUT.name, ":", r.status)
''')
    assert "@flow('demo', desc='实验')" in emitted


def test_roundtrip_linear_exact_ir_including_ids():
    """线性流：无分支重排空间，round-trip 连节点 id 都逐字段还原
    （仅派生注解 name/desc/source_line 例外——desc 内嵌源码行号必变）。"""
    ir1 = compile_source('''
@flow("demo")
def demo(INPUT):
    r = HTTP.get(url="https://api.example.com")
    return F.concat(INPUT.name, ":", r.status)
''')
    ir2 = compile_source(emit_source(ir1))
    for side in (ir1, ir2):
        for node in side["nodes"]:
            for key in _DERIVED_KEYS:
                node.pop(key, None)
    assert ir1 == ir2


def test_roundtrip_if_elif_else_and_operators():
    assert_roundtrip('''
@flow("grade")
def grade(INPUT):
    if INPUT.score >= 90 and INPUT.vip:
        return "A"
    elif INPUT.score >= 60 or INPUT.name in INPUT.whitelist:
        return "B"
    else:
        return "C"
''')


def test_roundtrip_bare_truth_and_not():
    assert_roundtrip('''
@flow("flags")
def flags(INPUT):
    if INPUT.flag:
        return "on!"
    if not INPUT.mute:
        return "loud"
    return "quiet"
''')


def test_roundtrip_notin_and_ne():
    assert_roundtrip('''
@flow("ops")
def ops(INPUT):
    if INPUT.name not in INPUT.blocked:
        if INPUT.count != 0:
            return "ok"
    return "blocked"
''')


def test_roundtrip_arithmetic_and_builtins():
    assert_roundtrip('''
@flow("calc")
def calc(INPUT):
    x = INPUT.a + INPUT.b * 2
    y = len(INPUT.name) - INPUT.loss
    return F.ifelse(x > y, "big", "small")
''')


def test_roundtrip_str_and_dict_list_literal():
    assert_roundtrip('''
@flow("shapes")
def shapes(INPUT):
    d = {"k": INPUT.v, "n": [1, 2, INPUT.w]}
    return F.concat(str(INPUT.num), d.k)
''')


def test_roundtrip_multiline_string_escape():
    assert_roundtrip('''
@flow("mlines")
def mlines(INPUT):
    return F.concat("line1\\nline2\\t\\\"q\\\"", INPUT.x)
''')


def test_roundtrip_while_for_form():
    assert_roundtrip('''
@flow("poll")
def poll(INPUT):
    for item in WHILE(rounds < INPUT.limit, max_iterations=9, id="poll_loop"):
        return rounds + 1
    return "done"
''')


def test_roundtrip_while_first_round_bootstrap():
    assert_roundtrip('''
@flow("bootstrap")
def bootstrap(INPUT):
    for item in WHILE(item == None or item.keep, id="boot"):
        return {"keep": False}
    return "end"
''')


def test_roundtrip_collections():
    assert_roundtrip('''
@flow("cols")
def cols(INPUT):
    for x, i in MAP(INPUT.items, id="m1"):
        out = F.concat("", x, i)
        return out
    for x in FILTER(INPUT.items, id="f1"):
        return x
    for x in FIND(INPUT.items, id="fd1"):
        return x
    for x in LOOP(INPUT.items, id="l1"):
        return x
    for acc, x in REDUCE(INPUT.items, initial=0, id="r1"):
        return F.add(acc, 1)
    return "done"
''')


def test_roundtrip_concurrent_map():
    assert_roundtrip('''
@flow("fan")
def fan(INPUT):
    for x in MAP(INPUT.items, concurrent=True, max_concurrent=4, id="m"):
        return x
    return "done"
''')


def test_roundtrip_http_full():
    assert_roundtrip('''
@flow("api")
def api(INPUT):
    r = HTTP(
        method="POST",
        url=INPUT.endpoint,
        headers={"Authorization": "Bearer t"},
        body={"q": INPUT.q},
        timeout=30,
        on_error=ErrorHandler(strategy="continue_with", default_value={"fallback": True}, retry_times=2),
    )
    return r.status
''')


def test_roundtrip_code_and_event():
    assert_roundtrip('''
@flow("glue")
def glue(INPUT):
    c = CODE(code="return {'x': 1}", lang="python")
    e = EVENT(type="deploy.done", filter=INPUT.env)
    return F.concat(c, e)
''')


def test_roundtrip_childflow_child_reference():
    assert_roundtrip('''
@childflow()
def double_each(INPUT):
    return F.mul(INPUT.item, 2)

@childflow(desc="三倍")
def triple_each(INPUT):
    return F.mul(INPUT.item, 3)

@flow("via_child")
def via_child(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=double_each)
    s = REFERENCE(input={"item": INPUT.payload}, flow=triple_each)
    return F.concat(r, s)
''')


def test_roundtrip_nested_childflow():
    """孙辈 childflow：主流程 → child_1 → child_2，发射顺序须先孙后子。"""
    assert_roundtrip('''
@childflow()
def leaf(INPUT):
    return INPUT.item

@childflow()
def middle(INPUT):
    r = CHILD(input={"item": INPUT.item}, flow=leaf)
    return r

@flow("nested_child")
def nested_child(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=middle)
    return r
''')


def test_roundtrip_parallel():
    assert_roundtrip('''
@childflow()
def sub_a(INPUT):
    return "a"

@childflow()
def sub_b(INPUT):
    return "b"

@flow("fan_out")
def fan_out(INPUT):
    r = PARALLEL(branches={"a": sub_a, "b": sub_b}, join=["a", "b"])
    return r
''')


def test_roundtrip_custom_nodes():
    assert_roundtrip('''
@flow("biz")
def biz(INPUT):
    echo = CODEFLOW_EMIT_ECHO(text=INPUT.x)
    s1 = CODEFLOW_EMIT_SUM(a=INPUT.a, b=2)
    CODEFLOW_EMIT_ECHO(text="fire-and-forget")
    return F.concat(echo, s1)
''')


def test_roundtrip_expr_statement_bare_call():
    assert_roundtrip('''
@flow("side_effects")
def side_effects(INPUT):
    HTTP.get(url="https://hooks.example.com")
    return "fired"
''')


def test_roundtrip_flow_opts():
    assert_roundtrip('''
@flow("full_opts", desc="全量装饰器参数", version="1.2.0", author="kongjie", timeout="60s",
      global_context={"region": "cn", "retries": 3}, metadata={"team": "infra"})
def full_opts(INPUT):
    return INPUT.x
''')


def test_roundtrip_nested_loop_in_while():
    assert_roundtrip('''
@flow("matrix")
def matrix(INPUT):
    for outer in WHILE(item == None, id="w0"):
        for x, i in MAP(INPUT.rows, id="m0"):
            return F.concat("", x, i)
        return None
    return "done"
''')


def test_roundtrip_deep_if_nesting():
    assert_roundtrip('''
@flow("deep")
def deep(INPUT):
    if INPUT.a:
        if INPUT.b:
            return "ab"
        return "a"
    if INPUT.c:
        return "c"
    return "none"
''')


# ---------------------------------------------------------------------------
# 宽松口径：手写 JSON IR（非源码产物）
# ---------------------------------------------------------------------------

def test_emit_from_handwritten_json_ir():
    hand_ir = {
        "runtime": "python",
        "flow_id": "legacy_flow",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "begin", "next": "fetch"},
            {"type": "http", "id": "fetch", "method": "GET",
             "url": "https://api.example.com/x", "next": "ret"},
            {"type": "end", "id": "ret", "output": "$NODE.fetch.status",
             "resultType": "success"},
        ],
    }
    emitted = emit_source(hand_ir)
    assert "fetch = HTTP" in emitted
    rebuilt = compile_source(emitted)
    left = _canonicalize(hand_ir)
    right = _canonicalize(rebuilt)
    assert left == right


# ---------------------------------------------------------------------------
# 负面路径
# ---------------------------------------------------------------------------

def test_emit_rejects_switch_node():
    ir = {
        "runtime": "python", "flow_id": "sw", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "sw"},
            {"type": "switch", "id": "sw", "expression": "$INPUT.x",
             "branches": [{"target": "end1", "value": "1", "priority": 0}],
             "next": "end1"},
            {"type": "end", "id": "end1", "resultType": "success"},
        ],
    }
    try:
        emit_source(ir)
    except EmitError as e:
        assert "switch" in str(e)
    else:
        raise AssertionError("switch 节点应抛 EmitError")


def test_emit_rejects_non_object_input_type():
    ir = {
        "runtime": "python", "flow_id": "arr", "inputType": {"dataType": "array"},
        "nodes": [{"type": "start", "id": "start", "next": "e"},
                  {"type": "end", "id": "e", "resultType": "success"}],
    }
    try:
        emit_source(ir)
    except EmitError as e:
        assert "inputType" in str(e)
    else:
        raise AssertionError("array inputType 应抛 EmitError")


def test_emit_rejects_if_same_target_branches():
    ir = {
        "runtime": "python", "flow_id": "deg", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "cond"},
            {"type": "if", "id": "cond", "condition": {"field": "$INPUT.x", "operator": "ne", "value": False},
             "next": "e", "else_next": "e"},
            {"type": "end", "id": "e", "output": "ok", "resultType": "success"},
        ],
    }
    try:
        emit_source(ir)
    except EmitError as e:
        assert "同址" in str(e)
    else:
        raise AssertionError("两分支同址应抛 EmitError")


def test_emit_rejects_malformed_ir():
    for bad in (None, [], {"flow_id": "x"}, {"nodes": "nope"}):
        try:
            emit_source(bad)
        except EmitError:
            continue
        raise AssertionError(f"应抛 EmitError: {bad!r}")


def test_emit_unmappable_expression():
    ir = {
        "runtime": "python", "flow_id": "bad", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "e"},
            {"type": "end", "id": "e", "output": "$MYSTERY.x", "resultType": "success"},
        ],
    }
    try:
        emit_source(ir)
    except EmitError as e:
        assert "$MYSTERY" in str(e)
    else:
        raise AssertionError("未知命名空间表达式应抛 EmitError")
