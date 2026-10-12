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
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Set

from plaita import Node
from plaita.dsl.builder import cond
from plaita.dsl.codeflow import EmitError, compile_source, emit_source, flow_from_source
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


def test_roundtrip_literal_eq_true_stays_equality():
    """源码字面 ``x == True`` 的 IR（``eq``/``True``）必须逆映射回 ``== True``。

    历史缺陷（#74）：``eq``+``True`` 被无条件当成 ``not x`` 的遗留形态还原成
    ``not x``，分支静默反转。现行编译器的 ``not x`` 产物是 ``falsy``（由 falsy
    分支单独接管），``eq``+``True`` 只来自字面相等比较。
    """
    src = '''
@flow("flag_check")
def flag_check(INPUT):
    if INPUT.flag == True:
        return "TAKEN_TRUE_BRANCH"
    else:
        return "TAKEN_FALSE_BRANCH"
'''
    emitted = assert_roundtrip(src)
    assert "INPUT.flag == True" in emitted
    assert "not INPUT.flag" not in emitted


def test_emit_literal_eq_true_preserves_runtime_branches():
    src = '''
@flow("flag_check_rt")
def flag_check_rt(INPUT):
    if INPUT.flag == True:
        return "TAKEN_TRUE_BRANCH"
    else:
        return "TAKEN_FALSE_BRANCH"
'''
    original = flow_from_source(src)
    roundtrip = flow_from_source(emit_source(compile_source(src)))
    assert original.run({"flag": True}) == roundtrip.run({"flag": True}) == "TAKEN_TRUE_BRANCH"
    assert original.run({"flag": False}) == roundtrip.run({"flag": False}) == "TAKEN_FALSE_BRANCH"


def test_emit_builder_eq_true_condition_stays_equality():
    """builder ``cond(field, "==", True)`` 直达同一形态，同样按比较还原。"""
    ir = {
        "runtime": "python", "flow_id": "builder_eq_true", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c"},
            {"type": "if", "id": "c", "condition": cond("$INPUT.ok", "==", True),
             "next": "b", "else_next": "e"},
            {"type": "assignment", "id": "b", "output": "yes", "next": "e"},
            {"type": "end", "id": "e", "output": "no", "resultType": "success"},
        ],
    }
    emitted = emit_source(ir)
    assert "INPUT.ok == True" in emitted
    rebuilt = compile_source(emitted)
    conds = [n["condition"] for n in rebuilt["nodes"] if n["type"] == "if"]
    assert conds == [{"field": "$INPUT.ok", "operator": "eq", "value": True}]


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


_EVENT_FIXTURE = Path(__file__).resolve().parents[1] / "integration" / "server" / "test_flow.json"


def test_emit_consumes_snake_case_event_keys():
    """存量 JSON flow 的事件节点用引擎权威的 snake_case 键（event_type/event_filter）。

    emit 只认 camelCase 时会产出 ``EVENT(type=None)``、``event_filter`` 静默消失，
    随后 round-trip 编译必抛「EVENT 需要 type」——JSON→@flow 迁移在事件节点上是死路。
    """
    with _EVENT_FIXTURE.open(encoding="utf-8") as fh:
        ir = json.load(fh)
    emitted = emit_source(ir)
    assert "EVENT(type=INPUT.event_type, filter={'source': 'test'})" in emitted
    assert "type=None" not in emitted
    compile_source(emitted)


def test_emit_camel_and_snake_event_keys_converge():
    """两种键名（编译器自产 camelCase / 存量 snake_case）emit 出同一份源码。"""
    snake_ir = {
        "runtime": "python", "flow_id": "ev", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "wait"},
            {"type": "event", "id": "wait", "event_type": "$INPUT.kind",
             "event_filter": {"source": "test"}, "next": "done"},
            {"type": "end", "id": "done", "output": "ok", "resultType": "success"},
        ],
    }
    camel_ir = copy.deepcopy(snake_ir)
    node = camel_ir["nodes"][1]
    node["eventType"] = node.pop("event_type")
    node["eventFilter"] = node.pop("event_filter")
    emitted = emit_source(snake_ir)
    assert "EVENT(type=INPUT.kind, filter={'source': 'test'})" in emitted
    assert emit_source(camel_ir) == emitted


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


# ---------------------------------------------------------------------------
# #76：end 的 resultType=error/nop 与 error 载荷必须活过 round-trip
# ---------------------------------------------------------------------------

def _end_by_output(ir: Dict[str, Any], output: Any) -> Dict[str, Any]:
    return next(n for n in ir["nodes"] if n["type"] == "end" and n.get("output") == output)


def test_emit_end_error_result_type_preserved():
    """resultType=error + error 载荷经 emit→compile 后原样保留。

    修复前：``_emit_return`` 只读 output，error 出口发射成裸 ``return 'no'``，
    重编译的 end 被 ``_stmt`` 硬编码回 ``resultType='success'``、error 载荷消失。
    """
    ir = {
        "runtime": "python", "flow_id": "errflow", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c"},
            {"type": "if", "id": "c",
             "condition": {"field": "$INPUT.ok", "operator": "truthy", "value": True},
             "next": "good", "else_next": "bad"},
            {"type": "end", "id": "good", "output": "yes", "resultType": "success"},
            {"type": "end", "id": "bad", "output": "no", "resultType": "error",
             "error": {"code": 4001, "message": "bad"}},
        ],
    }
    emitted = emit_source(ir)
    assert "result_type='error'" in emitted
    assert "error={'code': 4001, 'message': 'bad'}" in emitted
    bad = _end_by_output(compile_source(emitted), "no")
    assert bad["resultType"] == "error"
    assert bad["error"] == {"code": 4001, "message": "bad"}
    # 归一化口径整体等价（不只单看两个字段）
    assert _canonicalize(ir) == _canonicalize(compile_source(emitted))


def test_emit_end_nop_result_type_preserved():
    """resultType=nop 经 emit→compile 后保留（静默结束不降级为带值 success）。"""
    ir = {
        "runtime": "python", "flow_id": "nopflow", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "e"},
            {"type": "end", "id": "e", "output": "x", "resultType": "nop"},
        ],
    }
    emitted = emit_source(ir)
    assert "result_type='nop'" in emitted
    assert _end_by_output(compile_source(emitted), "x")["resultType"] == "nop"
    assert _canonicalize(ir) == _canonicalize(compile_source(emitted))


def test_emit_end_error_result_type_runtime_equivalence():
    """端到端：error 出口在 emit→compile→run 后仍抛 ERROR_RESULT，而非正常返回。"""
    from plaita.core.errors import FlowErrorType, FlowExecutionException

    ir = {
        "runtime": "python", "flow_id": "errflow", "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c"},
            {"type": "if", "id": "c",
             "condition": {"field": "$INPUT.ok", "operator": "truthy", "value": True},
             "next": "good", "else_next": "bad"},
            {"type": "end", "id": "good", "output": "yes", "resultType": "success"},
            {"type": "end", "id": "bad", "output": "no", "resultType": "error",
             "error": {"code": 4001, "message": "bad"}},
        ],
    }
    flow = flow_from_source(emit_source(ir))
    try:
        flow.run(ok=False)
    except FlowExecutionException as exc:
        assert exc.error_type == FlowErrorType.ERROR_RESULT
        assert exc.code == 4001
    else:
        raise AssertionError("error 出口 round-trip 后应仍抛 FlowExecutionException")
    assert flow.run(ok=True) == "yes"


def test_roundtrip_end_error_and_nop_from_source():
    """源码里的 ``end(...)`` 终止语句：源码 → IR → 源码 → IR 归一化相等。"""
    emitted = assert_roundtrip('''
@flow("endreasons")
def endreasons(INPUT):
    if INPUT.ok:
        return "yes"
    end(output="no", result_type="error", error={"code": 4001, "message": "bad"})
''')
    assert "result_type='error'" in emitted
    assert_roundtrip('''
@flow("nopflow")
def nopflow(INPUT):
    end(result_type="nop")
''')


def test_emit_end_error_payload_stays_literal_data():
    """``error`` 是纯数据：含字面 ``$`` 的字符串/键名必须原样往返。

    引擎 ``End.execute`` 直接 ``self.error.get(...)``，从不求值 error——
    按表达式逆映射时 ``{"message": "$100 fee missing"}`` / ``{"$ref": "x"}``
    会直接 ``EmitError``（无法逆映射），载荷无法迁移。
    """
    for payload in ({"message": "$100 fee missing", "hint": "x$y"}, {"$ref": "x"}):
        ir = {
            "runtime": "python", "flow_id": "errdata", "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "bad"},
                {"type": "end", "id": "bad", "output": "no", "resultType": "error",
                 "error": payload},
            ],
        }
        emitted = emit_source(ir)
        assert "end(" in emitted
        bad = _end_by_output(compile_source(emitted), "no")
        assert bad["error"] == payload, emitted
        assert _canonicalize(ir) == _canonicalize(compile_source(emitted))


def test_emit_end_error_payload_runtime_equivalence_for_dollar_data():
    """含字面 ``$`` 的 error 载荷运行期仍是同一失败出口（类型/code/message 一致）。"""
    from plaita.core.errors import FlowErrorType, FlowExecutionException
    from plaita.dsl.ir_validate import build_flow

    payload = {"code": 4001, "message": "$100 fee missing", "hint": "x$y"}
    ir = {
        "runtime": "python", "flow_id": "errdata", "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "bad"},
            {"type": "end", "id": "bad", "resultType": "error", "error": payload},
        ],
    }
    seen = []
    for flow in (build_flow(ir), flow_from_source(emit_source(ir))):
        try:
            flow.run()
        except FlowExecutionException as exc:
            seen.append((type(exc).__name__, exc.error_type, exc.code, exc.message))
        else:
            raise AssertionError("error 出口应抛 FlowExecutionException")
    assert seen[0] == seen[1]
    assert seen[0] == ("ErrorResultException", FlowErrorType.ERROR_RESULT, 4001,
                       "$100 fee missing")


def test_emit_normalizes_non_domain_result_type_to_engine_semantics():
    """非 success/error/nop 的 resultType 按发动机口径归一成 success 再发射。

    非字符串（``1``/``True``）或未知字面量（``"failure"``/``"ERROR"``/``""``）
    在引擎里都会被 ``End.setup_end`` 归一为 success；发射器若原样发
    ``result_type=1``，产物就**无法被 compile_source 吃回去**——破掉「发射器输出
    永远可重编译」这条硬性质。本用例对每个畸形值断言产物可重编译且语义为 success。
    """
    for bad in (1, True, "failure", "ERROR", "", None):
        ir = {
            "runtime": "python", "flow_id": "badrt", "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "e"},
                {"type": "end", "id": "e", "output": "x", "resultType": bad},
            ],
        }
        emitted = emit_source(ir)
        assert "result_type=" not in emitted, (bad, emitted)
        end = _end_by_output(compile_source(emitted), "x")
        assert end["resultType"] == "success", (bad, emitted)


def test_emit_normalizes_non_domain_result_type_with_error_payload():
    """归一后仍走 ``end(...)`` 通道（error 载荷不能丢），且 result_type 省略即 success。"""
    ir = {
        "runtime": "python", "flow_id": "badrterr", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "e"},
            {"type": "end", "id": "e", "output": "no", "resultType": 1,
             "error": {"code": 4001, "message": "bad"}},
        ],
    }
    emitted = emit_source(ir)
    assert "end(" in emitted
    assert "result_type=" not in emitted
    end = _end_by_output(compile_source(emitted), "no")
    assert end["resultType"] == "success"
    assert end["error"] == {"code": 4001, "message": "bad"}


# ---------------------------------------------------------------------------
# 发动机双键读取：resultType/result_type（发射器必须 camel 优先、snake 兜底）
# ---------------------------------------------------------------------------

_ENGINE_KEY_ALIASES = {
    "result_type": "resultType", "event_type": "eventType", "event_filter": "eventFilter",
    "child_flow": "childFlow", "else_next": "elseNext", "is_conditional": "isConditional",
    "join_branches": "joinBranches",
}


def _normalize_engine_keys(obj: Any) -> Any:
    """把发动机的 snake 键归一到编译器正典的 camel 键（仅用于跨前端等价比较）。"""
    if isinstance(obj, dict):
        return {_ENGINE_KEY_ALIASES.get(k, k): _normalize_engine_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize_engine_keys(x) for x in obj]
    return obj


def _flow_terminal(ir: Dict[str, Any]):
    """跑一遍 IR：正常返回 ('ok', 值)，否则 ('异常类', code, error_type, message)。"""
    from plaita.core.errors import FlowExecutionException
    from plaita.dsl.ir_validate import build_flow

    try:
        return ("ok", build_flow(ir).run())
    except FlowExecutionException as exc:
        return (type(exc).__name__, exc.code, exc.error_type, exc.message)


def test_emit_end_reads_snake_result_type():
    """只写 snake ``result_type`` 的存量 IR 必须还原 error/nop 出口。

    发动机 ``End.setup_end`` 两个键都收（camel 优先、snake 兜底），而
    ``End(...).model_dump()`` 与仓内存盘 flow 出的是 snake 键；发射器只读 camel
    时 snake 的 error/nop 出口被静默洗成 success（实测：snake error 直跑抛
    ``ErrorResultException``，emit→compile 后正常返回 'x'）。
    """
    for result_type, error in (("error", {"code": 7, "message": "m"}),
                               ("nop", None),
                               ("error", None)):
        node: Dict[str, Any] = {"type": "end", "id": "e", "output": "x",
                                "result_type": result_type}
        if error is not None:
            node["error"] = error
        ir = {
            "runtime": "python", "flow_id": "snake_rt", "version": "1",
            "inputType": {"dataType": "object"},
            "nodes": [{"type": "start", "id": "start", "next": "e"}, node],
        }
        emitted = emit_source(ir)
        if result_type != "success":
            assert f"result_type={result_type!r}" in emitted, emitted
        back = compile_source(emitted)
        end = _end_by_output(back, "x")
        assert end["resultType"] == result_type, emitted
        if error is not None:
            assert end["error"] == error
        original = _flow_terminal(ir)
        recompiled = _flow_terminal(compile_source(emitted))
        assert recompiled[:3] == original[:3], (original, recompiled, emitted)
        if error is not None or result_type == "nop":
            assert recompiled == original, (original, recompiled, emitted)


def test_emit_integration_fixture_snake_keys_roundtrip():
    """仓内存盘 flow（``tests/integration/server/test_flow.json``）作为回归语料。

    **口径：语义等价，非逐字节**——发射器写编译器正典的 camelCase 键
    （``resultType``/``eventType``/``eventFilter``），且 ``@flow`` 无语法表达
    ``input_type``/``output_type`` 的 properties 细节。断言：snake 取值被消费、
    flow 级元数据保留、以及「引擎键名归一 + 非语义 id 规范化」后逐节点相等。
    """
    with _EVENT_FIXTURE.open(encoding="utf-8") as fh:
        ir = json.load(fh)
    emitted = emit_source(ir)
    back = compile_source(emitted)
    end = _end_by_output(back, "$INPUT.message")
    assert end["resultType"] == "success", emitted          # snake result_type 被消费
    event = next(n for n in back["nodes"] if n["type"] == "event")
    assert event["eventType"] == "$INPUT.event_type", emitted   # snake event_type
    assert event["eventFilter"] == {"source": "test"}            # snake event_filter
    for key in ("flow_id", "desc", "version", "timeout", "metadata"):
        assert back[key] == ir[key], (key, back.get(key), ir.get(key))
    assert (_normalize_engine_keys(_canonicalize(ir)["nodes"])
            == _normalize_engine_keys(_canonicalize(back)["nodes"]))


def test_emit_end_empty_error_dict_is_preserved():
    """空 ``error={}`` 也是数据：error 出口丢它会把 ERROR_RESULT 变成 NODE_ERROR。

    手搓变异 ``error is not None`` → ``bool(error)`` 在 success 出口是运行时等价
    （引擎用不到 error），但在 **error 出口** 不是：``{}`` 被丢掉后发动机
    ``End.execute`` 走 ``self.error.get(...)`` 的 None 分支 ⇒
    ``NodeExecutionError(-9527)``，而原 IR 抛 ``ErrorResultException(None)``。
    """
    ir = {
        "runtime": "python", "flow_id": "emptyerr", "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "e"},
            {"type": "end", "id": "e", "output": "x", "resultType": "error", "error": {}},
        ],
    }
    emitted = emit_source(ir)
    assert "error={}" in emitted, emitted
    end = _end_by_output(compile_source(emitted), "x")
    assert end["error"] == {}
    assert _flow_terminal(ir)[:3] == _flow_terminal(compile_source(emitted))[:3]


def test_emit_end_error_payload_node_refs_survive_roundtrip():
    """``error`` 是数据：载荷里引用任意节点 id（含 end 自身）不得判悬空。

    发射器把 end 节点重命名为语义 slug（``e`` → ``ret_x``），若校验器把载荷当
    表达式扫，``$NODE.e`` 会变成悬空引用 ⇒ ``flow_from_source`` 直接抛
    ``FlowIRValidationError``（载荷里恰好引用 end 自身 id 时命中）。
    """
    payload = {"m": "$NODE.e.output", "ghost": "$NODE.nowhere"}
    ir = {
        "runtime": "python", "flow_id": "errref", "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "e"},
            {"type": "end", "id": "e", "output": "x", "resultType": "error",
             "error": payload},
        ],
    }
    emitted = emit_source(ir)
    flow_from_source(emitted)  # 校验门必须放行（不抛 FlowIRValidationError）
    end = _end_by_output(compile_source(emitted), "x")
    assert end["error"] == payload
    assert _flow_terminal(ir)[:3] == _flow_terminal(compile_source(emitted))[:3]


# ---------------------------------------------------------------------------
# 发动机双键读取：errorHandler/error_handler、childFlow/child_flow
# ---------------------------------------------------------------------------

def test_emit_reads_snake_error_handler():
    """``error_handler``（snake）与 ``errorHandler``（camel）发射出同一份源码。

    发动机 ``Node.setup_error_handler`` 两个键都收；发射器只读 camel 会把 snake
    存盘 flow 的错误策略静默丢掉（``on_error=`` 消失，引擎退回默认 abort）。
    """
    snake = {
        "runtime": "python", "flow_id": "snake_eh", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "h"},
            {"type": "http", "id": "h", "method": "GET", "url": "https://x.example",
             "error_handler": {"strategy": "continue_with", "defaultValue": {"data": None}},
             "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.h.status", "resultType": "success"},
        ],
    }
    camel = copy.deepcopy(snake)
    camel["nodes"][1]["errorHandler"] = camel["nodes"][1].pop("error_handler")
    emitted = emit_source(snake)
    assert "on_error=ErrorHandler(" in emitted, emitted
    assert emit_source(camel) == emitted
    http = next(n for n in compile_source(emitted)["nodes"] if n["type"] == "http")
    assert http["errorHandler"]["strategy"] == "continue_with"
    assert http["errorHandler"]["defaultValue"] == {"data": None}


_CHILD_FLOW_IR = {
    "runtime": "python", "inputType": {"dataType": "object"},
    "nodes": [
        {"type": "start", "id": "start", "next": "ret"},
        {"type": "end", "id": "ret", "output": "$INPUT.x", "resultType": "success"},
    ],
}


def test_emit_reads_snake_child_flow():
    """``child_flow``（snake）与 ``childFlow``（camel）的 child 节点都能发射。

    发动机 ``FlowNode.setup_child_flow`` 两个键都收；只读 camel 会把 snake 存盘
    flow 的整个内联子图丢掉。
    """
    snake = {
        "runtime": "python", "flow_id": "snake_child", "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c"},
            {"type": "child", "id": "c", "input": {"x": "$INPUT.x"},
             "child_flow": copy.deepcopy(_CHILD_FLOW_IR), "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.c", "resultType": "success"},
        ],
    }
    camel = copy.deepcopy(snake)
    camel["nodes"][1]["childFlow"] = camel["nodes"][1].pop("child_flow")
    emitted = emit_source(snake)
    assert emit_source(camel) == emitted
    assert "@childflow" in emitted, emitted
    child = next(n for n in compile_source(emitted)["nodes"] if n["type"] == "child")
    inner_ends = [n for n in child["childFlow"]["nodes"] if n["type"] == "end"]
    assert inner_ends and inner_ends[0]["output"] == "$INPUT.x"


# ---------------------------------------------------------------------------
# errorHandler 的发动机口径：真值判定 + 缺 strategy 不裸 KeyError（第 4 轮）
# ---------------------------------------------------------------------------

def _http_ir(error_handler: Any, key: str = "errorHandler") -> Dict[str, Any]:
    return {
        "runtime": "python", "flow_id": "eh_edge", "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "h"},
            {"type": "http", "id": "h", "method": "GET", "url": "https://x.example",
             key: error_handler, "next": "e"},
            {"type": "end", "id": "e", "output": "ok", "resultType": "success"},
        ],
    }


def _parsed_error_handler(ir: Dict[str, Any]) -> Dict[str, Any]:
    """按发动机解析 IR，返回 http 节点的 error_handler 正典 dict。"""
    from plaita.dsl.ir_validate import build_flow

    flow = build_flow(ir)
    node = next(n for n in flow.nodes if getattr(n, "node_type", None) == "http")
    return node.error_handler.model_dump(mode="json")


def test_emit_error_handler_falsy_shapes_match_engine_default():
    """``{}`` / ``[]`` / ``""`` 在发动机是**真值判定**下被当没配（退回默认 handler）。

    发射器若用 ``is not None`` 判定，``{}`` 会走进取值路径并在 ``eh['strategy']``
    上抛裸 ``KeyError``（本轮 0caa34c 引入的崩溃）。
    """
    for falsy in ({}, [], ""):
        ir = _http_ir(falsy)
        emitted = emit_source(ir)
        assert "on_error=" not in emitted, (falsy, emitted)
        assert _parsed_error_handler(compile_source(emitted)) == _parsed_error_handler(ir)


def test_emit_error_handler_missing_strategy_defaults_to_abort():
    """``{'retryTimes': 2}``：发动机接受（strategy 默认 abort），发射器不得抛 KeyError。"""
    ir = _http_ir({"retryTimes": 2})
    emitted = emit_source(ir)
    assert "on_error=ErrorHandler(strategy='abort', retry_times=2)" in emitted, emitted
    assert _parsed_error_handler(compile_source(emitted)) == _parsed_error_handler(ir)


def test_emit_error_handler_null_strategy_normalises_like_engine():
    """``strategy: None`` 发动机归一为 abort——发射器同口径，不产出不可重编译源码。"""
    ir = _http_ir({"strategy": None})
    emitted = emit_source(ir)
    assert "strategy='abort'" in emitted, emitted
    assert _parsed_error_handler(compile_source(emitted)) == _parsed_error_handler(ir)


def test_emit_error_handler_truthy_non_dict_is_emit_error():
    """真值但非 dict（发动机也 TypeError 拒绝）→ 干净 ``EmitError``，不是裸 TypeError。"""
    for bad in ("x", [1]):
        try:
            emit_source(_http_ir(bad))
        except EmitError as exc:
            assert str(exc) == (
                "节点 'h' 的 errorHandler 不是 dict，无法发射为可重编译源码"), exc
        else:
            raise AssertionError(f"真值非 dict 的 errorHandler 应抛 EmitError: {bad!r}")


def test_emit_error_handler_out_of_domain_strategy_is_emit_error():
    """域外 strategy（发动机 ValidationError 拒绝）→ EmitError，不产出不可重编译源码。"""
    try:
        emit_source(_http_ir({"strategy": "bogus"}))
    except EmitError as exc:
        assert str(exc) == (
            "errorHandler 的 strategy 'bogus' 不可表达为 @flow 源码"
            "（可用：abort/continue/continue_with/continue-with）"), exc
    else:
        raise AssertionError("域外 strategy 应抛 EmitError")


def test_emit_child_flow_json_string_parsed_like_engine():
    """``childFlow`` 传 JSON 字符串：发动机能跑（``Flow.model_validate_json``）。

    发射器此前直接把 str 交给只吃 dict 的接口 ⇒ 裸
    ``AttributeError: 'str' object has no attribute 'get'``；现按发动机口径解析，
    非法 JSON 才报 EmitError。
    """
    payload = json.dumps(_CHILD_FLOW_IR)
    ir = {
        "runtime": "python", "flow_id": "child_str", "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c"},
            {"type": "child", "id": "c", "input": {"x": "$INPUT.x"},
             "childFlow": payload, "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.c", "resultType": "success"},
        ],
    }
    emitted = emit_source(ir)
    assert "@childflow" in emitted, emitted
    child = next(n for n in compile_source(emitted)["nodes"] if n["type"] == "child")
    inner_ends = [n for n in child["childFlow"]["nodes"] if n["type"] == "end"]
    assert inner_ends and inner_ends[0]["output"] == "$INPUT.x"

    bad = copy.deepcopy(ir)
    bad["nodes"][1]["childFlow"] = "{not json"
    try:
        emit_source(bad)
    except EmitError as exc:
        assert str(exc) == "子流程 childFlow/flow 是字符串但不是合法 JSON", exc
    else:
        raise AssertionError("非法 JSON 的 childFlow 应抛 EmitError")


def test_emit_end_error_expression_output_uses_namespace():
    """error/nop 出口的 output 是 ``$NODE`` 表达式时，逆映射必须带上命名空间。

    ``_invert_value(output, ...)`` 的 ns 丢失在字面量 output 上看不出来，只有
    ``$NODE.`` 表达式才暴露（``ns=None`` ⇒ TypeError）。
    """
    ir = {
        "runtime": "python", "flow_id": "end_expr_out", "version": "1",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "r"},
            {"type": "assignment", "id": "r", "output": "$INPUT.x", "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.r", "resultType": "error",
             "error": {"code": 1, "message": "boom"}},
        ],
    }
    emitted = emit_source(ir)
    assert "end(output=r, result_type='error'" in emitted, emitted
    end = _end_by_output(compile_source(emitted), "$NODE.r")
    assert end["resultType"] == "error"
    assert end["error"] == {"code": 1, "message": "boom"}


def test_emit_end_error_in_loop_body_keeps_scope_namespace():
    """循环体内 error 出口的 output 必须按**该作用域**的 ns 逆映射，不能落回默认值。

    `_invert_value(output)` 省略 ns 时用 `_BASE_NS`（顶层同名）；在 while/集合体内
    ns 含 `$PARENT.NODE.*` 映射，省略会把 `$PARENT.NODE.r` 当普通表达式或直接
    无法逆映射。
    """
    ir = compile_source('''
@flow("wloop")
def wloop(INPUT):
    r = INPUT.x
    while INPUT.go:
        end(output=r, result_type="error", error={"code": 1})
    return r
''')
    emitted = emit_source(ir)
    assert "end(output=r, result_type='error'" in emitted, emitted
    back = compile_source(emitted)
    while_node = next(n for n in back["nodes"] if n["type"] == "while")
    inner_end = next(n for n in while_node["child_flow"]["nodes"] if n["type"] == "end")
    assert inner_end["output"] == "$PARENT.NODE.r", inner_end
    assert inner_end["resultType"] == "error"
    assert inner_end["error"] == {"code": 1}


def test_emit_success_return_in_loop_body_keeps_scope_namespace():
    """成功 `return`（`_emit_return` 的 return 分支）同样必须带作用域 ns。"""
    ir = compile_source('''
@flow("wloop2")
def wloop2(INPUT):
    r = INPUT.x
    while INPUT.go:
        return r
    return r
''')
    emitted = emit_source(ir)
    back = compile_source(emitted)
    while_node = next(n for n in back["nodes"] if n["type"] == "while")
    inner_end = next(n for n in while_node["child_flow"]["nodes"] if n["type"] == "end")
    assert inner_end["output"] == "$PARENT.NODE.r", inner_end
    assert inner_end["resultType"] == "success"
