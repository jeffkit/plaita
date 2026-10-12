"""#77：MAP/WHILE 子作用域的 ``_CompileCtx`` 必须继承 ``childflows`` 注册表。

源码字符串模式（``compile_source`` / ``flow_from_source``）下，``@childflow``
子流程被收集进 ``_CompileCtx.childflows``；但 while 条件、while 体、集合体
三处子作用域只继承了 ``module_globals``，``childflows`` 走缺省空 dict。于是
循环体内的 ``CHILD/PARALLEL/REFERENCE(flow=<name>)`` 先查注册表落空、再回退
``module_globals``（源码字符串模式下为空 ``{}``）也落空，最终抛出与事实相反的
「'inner' 不是 @childflow 装饰的子流程」——而同一句移出循环体即可编译。

主作用域与 if 体不受影响（if 体复用主 ctx，不新建 ``_CompileCtx``），本文件的
主作用域/if 体用例即回归对照。
"""
from __future__ import annotations

import ast
import sys

import pytest

from plaita.dsl.codeflow import compile_source
from plaita.dsl.codeflow import _stmt as _stmt_mod
from plaita.dsl.codeflow._common import _CodeflowError, _CompileCtx

_CHILDFLOW_HEAD = '''
@childflow()
def inner(INPUT):
    return INPUT.x
'''


def _compile_with_childflow(body: str) -> dict:
    return compile_source(_CHILDFLOW_HEAD + body)


def _child_scope_nodes(node: dict) -> list:
    scope = node.get("childFlow") or node.get("child_flow") or {}
    return scope.get("nodes", [])


def _only_node_of_type(ir: dict, node_type: str) -> dict:
    matches = [n for n in ir["nodes"] if n["type"] == node_type]
    assert len(matches) == 1, f"期望恰好一个 {node_type} 节点，实得 {matches!r}"
    return matches[0]


def test_child_reference_inside_map_body_compiles():
    """MAP 体内 ``CHILD(flow=inner)`` 编译成功，且子作用域里确有 child 节点。"""
    ir = _compile_with_childflow('''
@flow("m")
def m(INPUT):
    for it in MAP(INPUT.items, id="m"):
        CHILD(flow=inner, input=it)
        return it
    return INPUT
''')
    scope_nodes = _child_scope_nodes(_only_node_of_type(ir, "map"))
    assert any(n["type"] == "child" for n in scope_nodes), scope_nodes


def test_child_reference_inside_while_body_compiles():
    """while 体内 ``CHILD(flow=inner)`` 编译成功（此前抛「不是 @childflow」）。"""
    ir = _compile_with_childflow('''
@flow("m")
def m(INPUT):
    while INPUT.go:
        CHILD(flow=inner, input=INPUT)
        return INPUT
    return INPUT
''')
    scope_nodes = _child_scope_nodes(_only_node_of_type(ir, "while"))
    assert any(n["type"] == "child" for n in scope_nodes), scope_nodes


def test_parallel_and_reference_inside_map_body_compile():
    """PARALLEL / REFERENCE 走同一个 ``_eval_childflow_arg``，同样受此缺陷影响。"""
    ir = _compile_with_childflow('''
@flow("m")
def m(INPUT):
    for it in MAP(INPUT.items, id="m"):
        PARALLEL(branches={"a": inner})
        return it
    return INPUT
''')
    scope_nodes = _child_scope_nodes(_only_node_of_type(ir, "map"))
    parallel = next(n for n in scope_nodes if n["type"] == "parallel")
    branch_flow = parallel["branches"][0]["flow"]
    assert any(
        n["type"] == "end" and n.get("output") == "$INPUT.x"
        for n in branch_flow["nodes"]
    ), branch_flow

    ir2 = _compile_with_childflow('''
@flow("m")
def m(INPUT):
    for it in MAP(INPUT.items, id="m"):
        REFERENCE(flow=inner, input={"item": it})
        return it
    return INPUT
''')
    scope_nodes2 = _child_scope_nodes(_only_node_of_type(ir2, "map"))
    reference = next(n for n in scope_nodes2 if n["type"] == "reference")
    assert any(
        n["type"] == "end" and n.get("output") == "$INPUT.x"
        for n in reference["childFlow"]["nodes"]
    ), reference


def test_child_reference_inside_filter_and_loop_bodies_compile():
    """FILTER/LOOP 与 MAP 共用 ``_compile_for`` 的同一处子 ctx 构造。"""
    ir = _compile_with_childflow('''
@flow("m")
def m(INPUT):
    for it in FILTER(INPUT.items, id="f"):
        CHILD(flow=inner, input=it)
        return True
    return INPUT
''')
    scope_nodes = _child_scope_nodes(_only_node_of_type(ir, "filter"))
    assert any(n["type"] == "child" for n in scope_nodes), scope_nodes


def test_child_reference_outside_loop_still_compiles():
    """回归对照：循环外（主作用域 / if 体）本就可用，修复不得改变其行为。"""
    main_ir = _compile_with_childflow('''
@flow("m")
def m(INPUT):
    CHILD(flow=inner, input=INPUT)
    return INPUT
''')
    assert any(n["type"] == "child" for n in main_ir["nodes"])

    if_ir = _compile_with_childflow('''
@flow("m")
def m(INPUT):
    if INPUT.go:
        CHILD(flow=inner, input=INPUT)
    return INPUT
''')
    assert any(n["type"] == "child" for n in if_ir["nodes"])


def test_unregistered_childflow_name_still_raises():
    """负面：名字确实没注册时，报错语义保持（修复没有放宽判定）。"""
    with pytest.raises(_CodeflowError) as excinfo:
        _compile_with_childflow('''
@flow("m")
def m(INPUT):
    for it in MAP(INPUT.items, id="m"):
        CHILD(flow=missing, input=it)
        return it
    return INPUT
''')
    assert "不是 @childflow 装饰的子流程" in str(excinfo.value)


def test_while_condition_scope_inherits_childflows(monkeypatch):
    """while 条件子作用域继承 childflows，且不丢原有的 module_globals。

    条件表达式里放 CHILD/PARALLEL/REFERENCE 会先被「节点调用只能作语句或赋值
    右侧」的语法门拒掉——那是另一条规则，与本缺陷不同根，故该继承在当前文法下
    没有可观测行为。这里以白盒断言钉住构造参数，避免再次漏传（issue #77 边界
    原文：「其可触发面未在本单确证，验收断言以体作用域为主」）。
    """
    seen: dict = {}
    real_ctx_cls = _stmt_mod._CompileCtx

    def spy(**kwargs):
        seen.update(kwargs)
        return real_ctx_cls(**kwargs)

    monkeypatch.setattr(_stmt_mod, "_CompileCtx", spy)
    childflows = {"inner": {"runtime": "python"}}
    module_globals = {"SENTINEL": object()}
    ctx = _CompileCtx(childflows=childflows, module_globals=module_globals)
    _stmt_mod._while_cond(ast.parse("True", mode="eval").body, ctx)
    assert seen.get("childflows") == childflows
    # 装饰器模式回退靠 module_globals 里的 _ChildFlowMarker——补 childflows 时不能丢它。
    assert seen.get("module_globals") == module_globals


_DECORATOR_MODULE_SRC = '''\
from plaita.dsl.codeflow import CHILD, MAP, childflow, flow


@childflow()
def inner(INPUT):
    return INPUT.x


@flow("deco_map")
def deco_map(INPUT):
    for it in MAP(INPUT.items, id="m"):
        CHILD(flow=inner, input=it)
        return it
    return INPUT


@flow("deco_while")
def deco_while(INPUT):
    while INPUT.go:
        CHILD(flow=inner, input=INPUT)
        return INPUT
    return INPUT
'''


def _has_node_type(scope: dict, node_type: str) -> bool:
    for node in scope.get("nodes", []):
        if node.get("type") == node_type:
            return True
        for key in ("childFlow", "child_flow"):
            inner = node.get(key)
            if isinstance(inner, dict) and _has_node_type(inner, node_type):
                return True
        for branch in node.get("branches") or []:
            if isinstance(branch.get("flow"), dict) and _has_node_type(branch["flow"], node_type):
                return True
    return False


def test_decorator_mode_childflow_in_loop_bodies(tmp_path, monkeypatch):
    """装饰器入口（真实模块 + ``@flow``）循环体内 CHILD 仍可用。

    #77 只给子作用域补 ``childflows``；若顺手丢掉了 ``module_globals``，
    装饰器模式下的 ``_ChildFlowMarker`` 回退路径会被切断（该模式 childflows
    注册表为空、名字只能从 module_globals 解析）——本用例钉住这条回退。
    """
    import importlib

    module_name = "deco_childflow_loop_mod"
    (tmp_path / f"{module_name}.py").write_text(_DECORATOR_MODULE_SRC, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        mod = importlib.import_module(module_name)
        for name in ("deco_map", "deco_while"):
            ir = getattr(mod, name).__plaita_ir__
            assert _has_node_type(ir, "child"), (name, ir["nodes"])
    finally:
        sys.modules.pop(module_name, None)
