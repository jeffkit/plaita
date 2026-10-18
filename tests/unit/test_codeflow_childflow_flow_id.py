"""#145: ``@childflow`` 的位置参数 = 子流程 ``flow_id``。

维护者决策（issue #145 评论）：**位置 id**，形如 ``@childflow("cf")``；
``desc`` 保持纯关键字。不给 id ⇒ ``flow_id is None``，**不回落函数名**
（内联子流程没有函数名可用，回落会让同名子流程撞 id）。

``flow_id`` **不参与节点寻址 / 查找**（``flow.py`` 的四处引用全在
``logger.warning`` 里），但**会被写进执行上下文**（``context.py:314`` →
``$GLOBAL.flow_id``；``context.py:331`` / ``state.py:632`` → ``$FLOW_ID``，
别名 ``$FLOW``），**因此子流程内的表达式能读到它**——这不是"零影响"，
`TestFlowIdIsObservable` 就是钉这一条的回归（#145 评审必修 1）。

本文件钉八件事：
1. ``@childflow("cf")`` ⇒ 子 IR ``flow_id == "cf"``；
2. ``emit_source`` → ``compile_source`` 往返保真（IR 层与 ``Flow`` 对象层）；
3. 裸 ``@childflow`` / ``@childflow(desc=…)`` 行为与改动前一致；
4. 非法 id 类型（非 str 位置参数）抛 ``_CodeflowError``，不静默退化；
5. ``flow_id`` 在执行上下文里**可观测**（``$FLOW_ID`` / ``$GLOBAL.flow_id``，
   含"不给 id ⇒ 回落父 id"这条实测口径）；
6. id 闸门**只按类型把关**：``str`` 一律收（含 ``""`` / 纯空白），非 ``str`` 拒；
7. 发射侧把非 str 的 JSON-IR id 按 ``str()`` 归一（#76：发射产物必须可重编译）；
8. 同一文档内两个**不同**子流程撞 id ⇒ ``UserWarning``；同一子流程被引用多次
   （共享同一 IR 对象）不告警。
"""
from __future__ import annotations

import json
import re
import warnings
from typing import Any, Dict, Optional

import pytest

from plaita.dsl.codeflow import (
    CHILD,
    _CodeflowError,
    childflow,
    compile_source,
    emit_source,
    flow,
    flow_from_source,
)


# ---------------------------------------------------------------------------
# 模块级真实装饰器（``childflow()`` 走 ``inspect.getsource``，必须是真实源文件）
# ---------------------------------------------------------------------------

@childflow("cf_mod", desc="模块级带 id")
def _mod_child_with_id(INPUT):
    return F.mul(INPUT.item, 2)


@childflow(desc="模块级仅 desc")
def _mod_child_desc_only(INPUT):
    return F.add(INPUT.item, 1)


@childflow()
def _mod_child_empty(INPUT):
    return F.add(INPUT.item, 1)


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _child_ir(ir: Dict[str, Any]) -> Dict[str, Any]:
    """取 IR 里第一个 child 节点的 childFlow 子图（dict 或 JSON 字符串都接）。"""
    for node in ir["nodes"]:
        if node.get("type") == "child":
            child = node["childFlow"]
            return json.loads(child) if isinstance(child, str) else child
    raise AssertionError("IR 里没有 child 节点")


def _child_flow_id_at_flow_level(flow) -> Optional[str]:
    """``Flow`` 对象层：取内联子流程的 ``flow_id``。"""
    for node in flow.nodes:
        child_flow = getattr(node, "child_flow", None)
        if child_flow is not None:
            return child_flow.flow_id
    raise AssertionError("Flow 里没有内联子流程节点")


_SRC_WITH_ID = '''
@childflow("cf")
def some_child(INPUT):
    return F.mul(INPUT.item, 2)

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=some_child)
    return r
'''

_SRC_BARE = '''
@childflow
def some_child(INPUT):
    return F.mul(INPUT.item, 2)

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=some_child)
    return r
'''

_SRC_DESC_ONLY = '''
@childflow(desc="只有说明")
def some_child(INPUT):
    return F.mul(INPUT.item, 2)

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=some_child)
    return r
'''

_SRC_ID_AND_DESC = '''
@childflow("cf", desc="两个都给")
def some_child(INPUT):
    return F.mul(INPUT.item, 2)

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=some_child)
    return r
'''


# ---------------------------------------------------------------------------
# 1. 位置 id 进入子 IR
# ---------------------------------------------------------------------------

class TestPositionalIdInIR:
    def test_positional_id_lands_in_child_ir(self):
        """准则 1：``@childflow("cf")`` ⇒ 子 IR ``flow_id == "cf"``。"""
        child = _child_ir(compile_source(_SRC_WITH_ID))
        assert child["flow_id"] == "cf"

    def test_positional_id_coexists_with_desc(self):
        child = _child_ir(compile_source(_SRC_ID_AND_DESC))
        assert child["flow_id"] == "cf"
        assert child["desc"] == "两个都给"

    def test_positional_string_is_not_swallowed_as_desc(self):
        """准则 6：位置串是 id，不是 desc。"""
        child = _child_ir(compile_source(_SRC_WITH_ID))
        assert child.get("desc") is None

    def test_bare_childflow_has_no_flow_id_key(self):
        """准则 3：裸 ``@childflow`` ⇒ 无 id（不写键，也不回落函数名）。"""
        child = _child_ir(compile_source(_SRC_BARE))
        assert child.get("flow_id") is None
        assert "flow_id" not in child

    def test_desc_only_childflow_unchanged(self):
        """准则 6：``@childflow(desc=…)`` 与改动前一致。"""
        child = _child_ir(compile_source(_SRC_DESC_ONLY))
        assert child.get("flow_id") is None
        assert child["desc"] == "只有说明"

    def test_decorator_positional_id_is_not_desc(self):
        """真实装饰器路径：位置参数进 flow_id，desc 保持关键字专属。"""
        ir = _mod_child_with_id.__codeflow_ir__
        assert ir["flow_id"] == "cf_mod"
        assert ir["desc"] == "模块级带 id"

    def test_decorator_desc_only_keeps_flow_id_none(self):
        ir = _mod_child_desc_only.__codeflow_ir__
        assert ir.get("flow_id") is None
        assert ir["desc"] == "模块级仅 desc"

    def test_decorator_empty_call_keeps_flow_id_none(self):
        ir = _mod_child_empty.__codeflow_ir__
        assert ir.get("flow_id") is None
        assert "desc" not in ir


# ---------------------------------------------------------------------------
# 2. 往返保真
# ---------------------------------------------------------------------------

class TestRoundTrip:
    def test_emitted_source_declares_positional_id(self):
        emitted = emit_source(compile_source(_SRC_WITH_ID))
        assert "@childflow('cf')" in emitted

    def test_emit_recompile_preserves_child_flow_id(self):
        """准则 2：IR → 源码 → IR，``flow_id`` 不变。"""
        ir = compile_source(_SRC_WITH_ID)
        emitted = emit_source(ir)
        child = _child_ir(compile_source(emitted))
        assert child["flow_id"] == "cf"

    def test_flow_object_child_flow_id_survives_roundtrip(self):
        """准则 2（Flow 对象层）：``child_flow.flow_id == 'cf'`` 往返后仍成立。"""
        flow = flow_from_source(_SRC_WITH_ID)
        assert _child_flow_id_at_flow_level(flow) == "cf"
        roundtripped = flow_from_source(emit_source(compile_source(_SRC_WITH_ID)))
        assert _child_flow_id_at_flow_level(roundtripped) == "cf"

    def test_positional_id_with_desc_survives_roundtrip(self):
        emitted = emit_source(compile_source(_SRC_ID_AND_DESC))
        child = _child_ir(compile_source(emitted))
        assert child["flow_id"] == "cf"
        assert child["desc"] == "两个都给"

    def test_bare_childflow_roundtrips_to_none_without_name_fallback(self):
        """准则 3：往返后仍是 ``None``——发射器不把函数名当 id 回填。

        子函数声明名 ``some_child`` 与发射名 ``child_1`` 都不得成为 id：
        发射文本里除引用表达式 ``flow=child_1`` 外不出现任何 id 形参。
        """
        ir = compile_source(_SRC_BARE)
        emitted = emit_source(ir)
        assert "@childflow\n" in emitted
        assert "some_child" not in emitted
        child = _child_ir(compile_source(emitted))
        assert child.get("flow_id") is None

    def test_desc_only_roundtrip_unchanged(self):
        emitted = emit_source(compile_source(_SRC_DESC_ONLY))
        child = _child_ir(compile_source(emitted))
        assert child.get("flow_id") is None
        assert child["desc"] == "只有说明"


# ---------------------------------------------------------------------------
# 3. 手写 JSON IR（引擎/存量产物路径）也能发射出位置 id
# ---------------------------------------------------------------------------

def _json_ir_child(flow_id: Any) -> Dict[str, Any]:
    child: Dict[str, Any] = {
        "runtime": "python",
        "nodes": [
            {"type": "start", "id": "start", "next": "ret"},
            {"type": "end", "id": "ret", "output": "$INPUT.x", "resultType": "success"},
        ],
        "inputType": {"dataType": "object"},
    }
    if flow_id is not None:
        child["flow_id"] = flow_id
    return {
        "runtime": "python",
        "flow_id": "parent",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c"},
            {"type": "child", "id": "c", "input": {"x": "$INPUT.x"},
             "childFlow": child, "next": "ret"},
            {"type": "end", "id": "ret", "output": "$NODE.c.result", "resultType": "success"},
        ],
    }


class TestEmitFromJsonIR:
    def test_json_child_flow_id_is_emitted(self):
        emitted = emit_source(_json_ir_child("cf"))
        assert "@childflow('cf')" in emitted
        child = _child_ir(compile_source(emitted))
        assert child["flow_id"] == "cf"

    def test_json_child_without_flow_id_stays_anonymous(self):
        emitted = emit_source(_json_ir_child(None))
        assert "@childflow" in emitted
        assert "@childflow(" not in emitted
        child = _child_ir(compile_source(emitted))
        assert child.get("flow_id") is None


# ---------------------------------------------------------------------------
# 4. 非法 id 类型必须响亮失败
# ---------------------------------------------------------------------------

_SRC_INVALID_ID_TEMPLATE = '''
@childflow(@@ID@@)
def some_child(INPUT):
    return F.mul(INPUT.item, 2)

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=some_child)
    return r
'''


def _invalid_id_source(value: str) -> str:
    return _SRC_INVALID_ID_TEMPLATE.replace("@@ID@@", value)


class TestInvalidIdType:
    @pytest.mark.parametrize("literal", ["123", "1.5", "True"])
    def test_non_string_positional_id_raises(self, literal):
        """准则 7：非 str 位置参数 ⇒ ``_CodeflowError``，不静默退化。"""
        with pytest.raises(_CodeflowError) as excinfo:
            compile_source(_invalid_id_source(literal))
        message = str(excinfo.value)
        assert "flow_id" in message
        # 报行号：校验处必须把 AST 节点交给 _CodeflowError（``第 N 行``，
        # 不是 ``第 ? 行``）——否则作者定位不到出错的装饰器。
        assert re.search(r"第 \d+ 行", message), message

    def test_error_message_names_the_offending_type(self):
        with pytest.raises(_CodeflowError) as excinfo:
            compile_source(_invalid_id_source("123"))
        assert "int" in str(excinfo.value)

    def test_decorator_rejects_non_string_positional_id(self):
        """真实装饰器路径同样拒绝——校验是单一闸门（``_compile_childflow_fdef``）。"""
        with pytest.raises(_CodeflowError) as excinfo:
            @childflow(123)
            def _bad(INPUT):
                return F.mul(INPUT.item, 2)

        message = str(excinfo.value)
        assert "flow_id" in message
        assert re.search(r"第 \d+ 行", message), message


# ---------------------------------------------------------------------------
# 5. flow_id 在执行上下文里可观测（评审必修 1）
# ---------------------------------------------------------------------------
#
# 实测（base = 1503553，fix = #145）：
#   子 end output = "$FLOW_ID"
#     base（子 IR 无 flow_id）      -> 'parent'   ← 父上下文回落
#     fix，@childflow("cf")         -> 'cf'
#     fix，裸 @childflow            -> 'parent'   ← 同上回落
# 因此"给不给 id"会改变 $FLOW_ID / $GLOBAL.flow_id 的取值，**按它分支的表达式
# 会走到不同分支**——这是 #145 引入的语义变化，不是纯保真修复。

_OBSERVABLE_SRC = '''
@childflow("cf")
def child(INPUT):
    return "$FLOW_ID"

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": 1}, flow=child)
    return r
'''

_OBSERVABLE_GLOBAL_SRC = '''
@childflow("cf")
def child(INPUT):
    return GLOBAL.flow_id

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": 1}, flow=child)
    return r
'''

_OBSERVABLE_BARE_SRC = _OBSERVABLE_GLOBAL_SRC.replace('@childflow("cf")', "@childflow")

_OBSERVABLE_BRANCH_SRC = '''
@childflow("cf")
def child(INPUT):
    if GLOBAL.flow_id == "cf":
        return "child-id"
    return "parent-id"

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": 1}, flow=child)
    return r
'''


class TestFlowIdIsObservable:
    """``flow_id`` 会被写进执行上下文，子流程表达式读得到（实测口径）。"""

    def test_quoted_flow_id_string_is_literal_not_expression(self):
        """#231：``"$FLOW_ID"`` 是**字面量**（引号内以 ``$`` 开头不再当表达式）。

        动态读取子流程 id 用下一例的 ``GLOBAL.flow_id``（不带引号）。
        """
        assert flow_from_source(_OBSERVABLE_SRC).run() == "$FLOW_ID"

    def test_child_flow_id_is_visible_as_global_flow_id(self):
        """``context.py:314`` 把 ``flow_id`` 塞进 ``$GLOBAL``。"""
        assert flow_from_source(_OBSERVABLE_GLOBAL_SRC).run() == "cf"

    def test_anonymous_child_falls_back_to_parent_flow_id(self):
        """不给 id ⇒ 子作用域求值为 None，父上下文回落（``context.py:137-148``）。

        这是**实测的**裸 ``@childflow`` 行为，也是 base 下 ``@childflow("cf")``
        的行为——把它钉住，避免"零影响"的口径再次被写进文档。
        """
        assert flow_from_source(_OBSERVABLE_BARE_SRC).run() == "parent"

    def test_flow_id_can_change_which_branch_runs(self):
        """可观测 ⇒ 能改变执行路径：按 ``GLOBAL.flow_id`` 分支的子流程走不同出口。"""
        assert flow_from_source(_OBSERVABLE_BRANCH_SRC).run() == "child-id"
        assert flow_from_source(
            _OBSERVABLE_BRANCH_SRC.replace('@childflow("cf")', "@childflow")).run() == "parent-id"

    def test_observability_survives_emit_roundtrip(self):
        """往返后 `GLOBAL.flow_id` 仍取到子 id（发射 → 重编译不丢可观测性）。

        这里用不带引号的 ``GLOBAL.flow_id``（动态表达式）而不是字面量
        ``"$FLOW_ID"``：后者按 #231 是字面量，不再是表达式。
        """
        emitted = emit_source(compile_source(_OBSERVABLE_GLOBAL_SRC))
        assert "@childflow('cf')" in emitted
        assert flow_from_source(emitted).run() == "cf"


# ---------------------------------------------------------------------------
# 6. id 闸门只按类型把关（空串 / 纯空白照收）
# ---------------------------------------------------------------------------

_SRC_ANY_STR_ID_TEMPLATE = '''
@childflow(@@ID@@)
def some_child(INPUT):
    return F.mul(INPUT.item, 2)

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=some_child)
    return r
'''


class TestIdGateIsTypeOnly:
    """``flow_id`` 只校验类型：任何 ``str`` 都收，不做非空 / 去空白校验。"""

    @pytest.mark.parametrize("literal, value", [
        ('""', ""),
        ('"   "', "   "),
        ('"\\t"', "\t"),
        ('"  cf  "', "  cf  "),
    ])
    def test_any_string_id_is_accepted_verbatim(self, literal, value):
        src = _SRC_ANY_STR_ID_TEMPLATE.replace("@@ID@@", literal)
        child = _child_ir(compile_source(src))
        assert "flow_id" in child
        assert child["flow_id"] == value

    @pytest.mark.parametrize("literal, value", [('""', ""), ('"   "', "   ")])
    def test_blank_string_id_roundtrips_verbatim(self, literal, value):
        src = _SRC_ANY_STR_ID_TEMPLATE.replace("@@ID@@", literal)
        emitted = emit_source(compile_source(src))
        assert f"@childflow({value!r})" in emitted
        assert _child_ir(compile_source(emitted))["flow_id"] == value

    def test_non_string_is_still_rejected(self):
        """类型闸门不放宽：空串收，非 str 依旧抛错。"""
        with pytest.raises(_CodeflowError):
            compile_source(_invalid_id_source("0"))


# ---------------------------------------------------------------------------
# 7. 发射侧：非 str 的 JSON-IR id 按 str() 归一（#76 不变量）
# ---------------------------------------------------------------------------

class TestEmitNormalisesNonStringJsonId:
    """存量 JSON 产物里的 ``flow_id`` 可能不是 str；发射器必须能自洽地重编译。"""

    @pytest.mark.parametrize("raw", [5, 5.5, True, ["a"], {"k": 1}])
    def test_non_string_json_id_is_str_normalised_and_recompiles(self, raw):
        emitted = emit_source(_json_ir_child(raw))
        assert f"@childflow({str(raw)!r})" in emitted
        child = _child_ir(compile_source(emitted))
        assert child["flow_id"] == str(raw)
        # 二次往返：发射产物自身也必须可再发射（#76「发射器输出永远可重编译」）。
        emitted_again = emit_source(compile_source(emitted))
        assert f"@childflow({str(raw)!r})" in emitted_again


# ---------------------------------------------------------------------------
# 8. 重名 id：只告警不报错；共享同一 IR 对象的合法复用不得误报
# ---------------------------------------------------------------------------

_DUP_SIBLING_SRC = '''
@childflow("dup")
def a(INPUT):
    return 1

@childflow("dup")
def b(INPUT):
    return 2

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": 1}, flow=a)
    s = CHILD(input={"r": r}, flow=b)
    return s
'''

_DUP_NESTED_SRC = '''
@childflow("dup")
def leaf(INPUT):
    return 1

@childflow("dup")
def middle(INPUT):
    r = CHILD(input={"item": 1}, flow=leaf)
    return r

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": 1}, flow=middle)
    return r
'''

_SHARED_CHILD_SRC = '''
@childflow("shared")
def a(INPUT):
    return 1

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": 1}, flow=a)
    s = CHILD(input={"r": r}, flow=a)
    return s
'''

_DISTINCT_IDS_SRC = '''
@childflow("one")
def a(INPUT):
    return 1

@childflow("two")
def b(INPUT):
    return 2

@flow("parent")
def parent(INPUT):
    r = CHILD(input={"item": 1}, flow=a)
    s = CHILD(input={"r": r}, flow=b)
    return s
'''


def _dup_warnings(src: str):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ir = compile_source(src)
    return ir, [w for w in caught if issubclass(w.category, UserWarning)]


# 真实装饰器路径的重名用例：两个**模块级** ``@childflow`` 撞 id（单独装饰不告警，
# 告警发生在父 ``@flow`` 装配时），父函数留到测试里再套 ``flow(...)``——这样
# 告警落在测试进程内、可被 ``pytest.warns`` 捕获，而不是在 import 期流失。
@childflow("dup_mod")
def _dup_mod_a(INPUT):
    return 1


@childflow("dup_mod")
def _dup_mod_b(INPUT):
    return 2


def _dup_mod_parent(INPUT):
    r = CHILD(input={"item": 1}, flow=_dup_mod_a)
    s = CHILD(input={"r": r}, flow=_dup_mod_b)
    return s


class TestDuplicateChildFlowIds:
    """撞 id 只告警（不破坏存量）；共享对象不算重复声明。"""

    def test_sibling_duplicate_ids_warn(self):
        _, caught = _dup_warnings(_DUP_SIBLING_SRC)
        assert any("flow_id 重复" in str(w.message) for w in caught), [str(w.message) for w in caught]

    def test_nested_duplicate_ids_warn(self):
        _, caught = _dup_warnings(_DUP_NESTED_SRC)
        assert any("flow_id 重复" in str(w.message) for w in caught), [str(w.message) for w in caught]

    def test_decorator_path_duplicate_ids_warn(self):
        """真实装饰器路径（父 ``@flow`` 装配时）同样告警。"""
        with pytest.warns(UserWarning, match="flow_id 重复"):
            compiled = flow("dup_parent")(_dup_mod_parent)
        assert compiled.flow_id == "dup_parent"
    def test_duplicate_warning_does_not_block_compilation(self):
        """告警不是错误：文档照常装配（存量 flow 不中断）。"""
        ir, caught = _dup_warnings(_DUP_SIBLING_SRC)
        assert caught
        assert [n["childFlow"].get("flow_id") for n in ir["nodes"] if n.get("type") == "child"] == ["dup", "dup"]

    def test_same_child_referenced_twice_does_not_warn(self):
        """父流程多处引用**同一个**子流程是合法复用，不得误报。

        前提：两处嵌入的是同一个 dict 对象（去重就是按身份做的）——本用例把
        前提与结论一起钉住。
        """
        ir, caught = _dup_warnings(_SHARED_CHILD_SRC)
        assert not caught, [str(w.message) for w in caught]
        embedded = [n["childFlow"] for n in ir["nodes"] if n.get("type") == "child"]
        assert len(embedded) == 2
        assert embedded[0] is embedded[1]

    def test_distinct_ids_do_not_warn(self):
        _, caught = _dup_warnings(_DISTINCT_IDS_SRC)
        assert not caught, [str(w.message) for w in caught]

    def test_anonymous_children_do_not_warn(self):
        """多个匿名子流程（``flow_id is None``）天然合法，不告警。"""
        src = _DUP_SIBLING_SRC.replace('@childflow("dup")', "@childflow")
        _, caught = _dup_warnings(src)
        assert not caught, [str(w.message) for w in caught]
