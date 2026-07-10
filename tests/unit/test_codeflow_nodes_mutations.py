"""针对 plaita/dsl/codeflow/_nodes.py 的 mutation killing 测试。"""
from __future__ import annotations

import ast

import pytest

from plaita.dsl.codeflow._common import (
    _ChildFlowMarker,
    _CodeflowError,
    _CompileCtx,
)
from plaita.dsl.codeflow._nodes import (
    _compile_custom_node,
    _compile_node_call,
    _eval_childflow_arg,
    _eval_error_handler,
    _eval_join,
    _eval_parallel_branches,
    _parallel_branch,
)


def _call(code: str) -> ast.Call:
    node = ast.parse(code, mode="eval").body
    assert isinstance(node, ast.Call)
    if not getattr(node, "lineno", None):
        node.lineno = 7  # type: ignore[attr-defined]
    return node


def _ctx(**kwargs) -> _CompileCtx:
    return _CompileCtx(**kwargs)


_CHILD_IR = {"flow_id": "child", "nodes": [{"type": "end", "id": "e1"}]}


# ---------------------------------------------------------------------------
# _compile_node_call — dispatch + builtin nodes
# ---------------------------------------------------------------------------

class TestCompileNodeCallMutations:
    def test_http_minimal_spec(self):
        spec = _compile_node_call(_call('HTTP(url="http://x.com")'), _ctx(), "resp")
        assert spec["type"] == "http"
        assert spec["id"] == "resp"
        assert spec["method"] == "POST"
        assert spec["url"] == "http://x.com"

    def test_http_post_method_from_attribute(self):
        spec = _compile_node_call(_call('HTTP.get(url="http://x.com")'), _ctx(), None)
        assert spec["method"] == "GET"

    def test_http_method_kw_uppercased(self):
        spec = _compile_node_call(
            _call('HTTP(url="http://x.com", method="patch")'), _ctx(), None
        )
        assert spec["method"] == "PATCH"

    def test_http_optional_fields_use_ctx(self):
        ctx = _ctx()
        ctx.names["hdrs"] = "$NODE.hdrs"
        spec = _compile_node_call(
            _call('HTTP(url="http://x.com", headers=hdrs, body="x", timeout=5, input=INPUT.x)'),
            ctx,
            None,
        )
        assert spec["headers"] == "$NODE.hdrs"
        assert spec["body"] == "x"
        assert spec["timeout"] == 5
        assert spec["input"] == "$INPUT.x"

    def test_http_missing_url_message_and_lineno(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_node_call(_call("HTTP()"), _ctx(), None)
        msg = str(exc.value)
        assert "HTTP 需要 url" in msg
        assert "XXHTTP" not in msg
        assert "第 ?" not in msg

    def test_http_on_error_handler_spec(self):
        node = _call('HTTP(url="http://x.com", on_error=ErrorHandler("continue"))')
        spec = _compile_node_call(node, _ctx(), None)
        assert spec["errorHandler"]["strategy"] == "continue"

    def test_code_python_attribute_lang(self):
        spec = _compile_node_call(_call('CODE.python("print(1)")'), _ctx(), None)
        assert spec["type"] == "code"
        assert spec["language"] == "python"
        assert spec["code"] == "print(1)"

    def test_code_missing_fields_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_node_call(_call("CODE()"), _ctx(), None)
        assert "CODE 需要 code 和 lang" in str(exc.value)
        assert "第 ?" not in str(exc.value)

    def test_event_type_from_kw(self):
        spec = _compile_node_call(_call('EVENT(type="user.login")'), _ctx(), None)
        assert spec["eventType"] == "user.login"

    def test_event_type_snake_case_kw_alias(self):
        spec = _compile_node_call(_call('EVENT(event_type="user.login")'), _ctx(), None)
        assert spec["eventType"] == "user.login"

    def test_event_type_camel_case_kw_alias(self):
        spec = _compile_node_call(_call('EVENT(eventType="user.login")'), _ctx(), None)
        assert spec["eventType"] == "user.login"

    def test_event_positional_bound_name_uses_ctx(self):
        ctx = _ctx()
        ctx.names["etype"] = "$NODE.etype"
        spec = _compile_node_call(_call("EVENT(etype)"), ctx, None)
        assert spec["eventType"] == "$NODE.etype"

    def test_event_missing_type_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_node_call(_call("EVENT()"), _ctx(), None)
        assert "EVENT 需要 type" in str(exc.value)
        assert "第 ?" not in str(exc.value)

    def test_event_filter_field(self):
        spec = _compile_node_call(
            _call('EVENT("evt", filter=INPUT.cond)'), _ctx(), None
        )
        assert spec["eventFilter"] == "$INPUT.cond"

    def test_child_resolves_childflow_from_registry(self):
        ctx = _ctx(childflows={"cf": _CHILD_IR})
        flow_name = ast.Name(id="cf", ctx=ast.Load())
        flow_name.lineno = 3
        node = _call("CHILD(INPUT.x, flow=cf)")
        spec = _compile_node_call(node, ctx, "result")
        assert spec["type"] == "child"
        assert spec["childFlow"] == _CHILD_IR
        assert spec["input"] == "$INPUT.x"

    def test_reference_kind(self):
        ctx = _ctx(childflows={"cf": _CHILD_IR})
        spec = _compile_node_call(_call("REFERENCE(flow=cf)"), ctx, None)
        assert spec["type"] == "reference"

    def test_code_positional_bound_code_uses_ctx(self):
        ctx = _ctx()
        ctx.names["snippet"] = "$NODE.snippet"
        spec = _compile_node_call(_call("CODE.python(snippet)"), ctx, None)
        assert spec["code"] == "$NODE.snippet"
        assert spec["language"] == "python"

    def test_child_positional_input_uses_ctx(self):
        ctx = _ctx(childflows={"cf": _CHILD_IR})
        ctx.names["payload"] = "$NODE.payload"
        spec = _compile_node_call(_call("CHILD(payload, flow=cf)"), ctx, "out")
        assert spec["input"] == "$NODE.payload"

    def test_child_missing_flow_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_node_call(_call("CHILD(INPUT.x)"), _ctx(), None)
        msg = str(exc.value)
        assert "CHILD 需要 flow=" in msg
        assert "第 ?" not in msg

    def test_parallel_dict_branches(self):
        ctx = _ctx(childflows={"cf_a": _CHILD_IR, "cf_b": _CHILD_IR})
        branches = ast.Dict(
            keys=[ast.Constant("a"), ast.Constant("b")],
            values=[ast.Name("cf_a", ast.Load()), ast.Name("cf_b", ast.Load())],
        )
        branches.lineno = 5
        node = ast.Call(
            func=ast.Name("PARALLEL", ast.Load()),
            args=[],
            keywords=[ast.keyword("branches", branches)],
        )
        node.lineno = 4
        spec = _compile_node_call(node, ctx, "par")
        assert spec["type"] == "parallel"
        assert len(spec["branches"]) == 2
        assert spec["branches"][0]["name"] == "a"

    def test_parallel_missing_branches_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_node_call(_call("PARALLEL()"), _ctx(), None)
        assert "PARALLEL 需要 branches" in str(exc.value)
        assert "第 ?" not in str(exc.value)

    def test_parallel_mode_and_join_and_conditional(self):
        ctx = _ctx(childflows={"cf": _CHILD_IR})
        node = _call(
            'PARALLEL(branches={"a": cf}, mode="process", join=["a"], conditional=True)'
        )
        spec = _compile_node_call(node, ctx, None)
        assert spec["mode"] == "process"
        assert spec["joinBranches"] == ["a"]
        assert spec["isConditional"] is True

    def test_unsupported_node_call_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_node_call(_call("obj.method()"), _ctx(), None)
        msg = str(exc.value)
        assert "不支持的节点调用" in msg
        assert "XX不支持的节点调用XX" not in msg
        assert "第 ?" not in msg

    def test_http_on_error_handler_kw_alias(self):
        spec = _compile_node_call(
            _call('HTTP(url="http://x.com", on_error_handler=ErrorHandler("abort"))'),
            _ctx(),
            None,
        )
        assert spec["errorHandler"]["strategy"] == "abort"

    def test_http_method_ignores_non_constant_kw(self):
        ctx = _ctx()
        ctx.names["meth"] = "$NODE.meth"
        spec = _compile_node_call(
            _call('HTTP(url="http://x.com", method=meth)'), ctx, None
        )
        assert spec["method"] == "POST"

    def test_http_positional_url_constant(self):
        spec = _compile_node_call(_call('HTTP("http://y.com")'), _ctx(), None)
        assert spec["url"] == "http://y.com"

    def test_builtin_auto_id_without_assign_name(self):
        spec = _compile_node_call(_call('HTTP(url="http://z.com")'), _ctx(), None)
        assert spec["id"].startswith("_n")

    def test_unregistered_uppercase_goes_to_custom_error(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_node_call(_call("UNREGISTERED_NODE(x=1)"), _ctx(), None)
        assert "未注册的自定义节点" in str(exc.value)

    def test_custom_node_dispatch(self):
        ctx = _ctx(known_node_types={"mynode"})
        spec = _compile_node_call(_call("MYNODE(text=INPUT.msg)"), ctx, "out")
        assert spec["type"] == "mynode"
        assert spec["id"] == "out"
        assert spec["text"] == "$INPUT.msg"


# ---------------------------------------------------------------------------
# _compile_custom_node
# ---------------------------------------------------------------------------

class TestCompileCustomNodeMutations:
    def test_positional_args_error_message(self):
        node = _call("MYNODE(1)")
        with pytest.raises(_CodeflowError) as exc:
            _compile_custom_node(node, _ctx(known_node_types={"mynode"}), None, "mynode")
        msg = str(exc.value)
        assert "只接受关键字参数" in msg
        assert "第 ?" not in msg

    def test_id_kw_must_be_string_constant(self):
        node = _call("MYNODE(id=42, text=INPUT.x)")
        with pytest.raises(_CodeflowError) as exc:
            _compile_custom_node(node, _ctx(known_node_types={"mynode"}), None, "mynode")
        assert "id= 必须是字符串常量" in str(exc.value)

    def test_assign_and_id_kw_conflict(self):
        node = _call('MYNODE(id="x", text=INPUT.msg)')
        with pytest.raises(_CodeflowError) as exc:
            _compile_custom_node(node, _ctx(known_node_types={"mynode"}), "a", "mynode")
        msg = str(exc.value)
        assert "不要同时传 id=" in msg
        assert "第 ?" not in msg

    def test_custom_field_uses_ctx_bound_name(self):
        ctx = _ctx(known_node_types={"mynode"})
        ctx.names["val"] = "$NODE.val"
        node = _call("MYNODE(text=val)")
        spec = _compile_custom_node(node, ctx, "out", "mynode")
        assert spec["text"] == "$NODE.val"

    def test_explicit_id_kw_claimed(self):
        node = _call('MYNODE(id="node1", text="hi")')
        spec = _compile_custom_node(
            node, _ctx(known_node_types={"mynode"}), None, "mynode"
        )
        assert spec["id"] == "node1"

    def test_auto_id_when_no_assign(self):
        node = _call('MYNODE(text="hi")')
        spec = _compile_custom_node(
            node, _ctx(known_node_types={"mynode"}), None, "mynode"
        )
        assert spec["id"].startswith("_n")

    def test_timeout_and_error_handler_fields(self):
        node = _call(
            'MYNODE(text="hi", timeout=30, on_error=ErrorHandler("abort", error_message="oops"))'
        )
        spec = _compile_custom_node(
            node, _ctx(known_node_types={"mynode"}), None, "mynode"
        )
        assert spec["timeout"] == 30
        assert spec["errorHandler"]["strategy"] == "abort"
        assert spec["errorHandler"]["errorMessage"] == "oops"

    def test_star_kwargs_unpack_error(self):
        node = ast.Call(
            func=ast.Name("MYNODE", ast.Load()),
            args=[],
            keywords=[ast.keyword(None, ast.Name("extra", ast.Load()))],
        )
        node.lineno = 6
        with pytest.raises(_CodeflowError) as exc:
            _compile_custom_node(node, _ctx(known_node_types={"mynode"}), None, "mynode")
        assert "不支持 **kwargs 解包" in str(exc.value)


# ---------------------------------------------------------------------------
# _eval_error_handler
# ---------------------------------------------------------------------------

class TestEvalErrorHandlerMutations:
    def test_abort_default_strategy(self):
        node = _call("ErrorHandler()")
        spec = _eval_error_handler(node, _ctx())
        assert spec["strategy"] == "abort"

    def test_continue_with_retry_and_default_value(self):
        ctx = _ctx()
        ctx.names["fallback"] = "$NODE.fallback"
        node = _call(
            'ErrorHandler("continue_with", retry_times=2, default_value=fallback, error_code=500)'
        )
        spec = _eval_error_handler(node, ctx)
        assert spec["strategy"] == "continue_with"
        assert spec["retryTimes"] == 2
        assert spec["defaultValue"] == "$NODE.fallback"
        assert spec["errorCode"] == 500

    def test_retry_times_camel_case_kw(self):
        node = _call('ErrorHandler("abort", retryTimes=5)')
        spec = _eval_error_handler(node, _ctx())
        assert spec["retryTimes"] == 5

    def test_default_kw_aliases(self):
        for kw in ("default", "default_value", "defaultValue"):
            ctx = _ctx()
            node = _call(f'ErrorHandler("continue_with", {kw}=INPUT.x)')
            spec = _eval_error_handler(node, ctx)
            assert spec["defaultValue"] == "$INPUT.x"

    def test_error_code_and_message_aliases(self):
        node = _call(
            'ErrorHandler("abort", errorCode=404, errorMessage="not found")'
        )
        spec = _eval_error_handler(node, _ctx())
        assert spec["errorCode"] == 404
        assert spec["errorMessage"] == "not found"

    def test_retry_times_ignores_non_constant(self):
        ctx = _ctx()
        ctx.names["n"] = "$NODE.n"
        node = _call('ErrorHandler("abort", retry_times=n)')
        spec = _eval_error_handler(node, ctx)
        assert "retryTimes" not in spec

    def test_reference_missing_flow_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _compile_node_call(_call("REFERENCE()"), _ctx(), None)
        msg = str(exc.value)
        assert "REFERENCE 需要 flow=" in msg
        assert "第 ?" not in msg

    def test_invalid_strategy_message(self):
        node = _call('ErrorHandler("bad_strategy")')
        with pytest.raises(_CodeflowError) as exc:
            _eval_error_handler(node, _ctx())
        msg = str(exc.value)
        assert "unknown error handler strategy" in msg
        assert "bad_strategy" in msg
        assert "第 ?" not in msg

    def test_non_error_handler_call_message(self):
        node = _call('F.upper("x")')
        with pytest.raises(_CodeflowError) as exc:
            _eval_error_handler(node, _ctx())
        assert "on_error 必须是 ErrorHandler(...) 调用" in str(exc.value)
        assert "第 ?" not in str(exc.value)


# ---------------------------------------------------------------------------
# _eval_childflow_arg
# ---------------------------------------------------------------------------

class TestEvalChildflowArgMutations:
    def test_from_childflows_registry(self):
        ctx = _ctx(childflows={"cf": _CHILD_IR})
        name = ast.Name(id="cf", ctx=ast.Load())
        name.lineno = 2
        assert _eval_childflow_arg(name, ctx) == _CHILD_IR

    def test_from_module_globals_marker(self):
        def _dummy_cf(INPUT):  # noqa: ARG001
            return INPUT

        ctx = _ctx(module_globals={"cf": _ChildFlowMarker(_CHILD_IR, _dummy_cf)})
        name = ast.Name(id="cf", ctx=ast.Load())
        name.lineno = 2
        assert _eval_childflow_arg(name, ctx) == _CHILD_IR

    def test_unknown_childflow_message(self):
        name = ast.Name(id="missing_cf", ctx=ast.Load())
        name.lineno = 4
        with pytest.raises(_CodeflowError) as exc:
            _eval_childflow_arg(name, _ctx())
        msg = str(exc.value)
        assert "missing_cf" in msg
        assert "不是 @childflow 装饰的子流程" in msg
        assert "第 ?" not in msg

    def test_unsupported_childflow_arg_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _eval_childflow_arg(ast.Constant("literal"), _ctx())
        assert "不支持的 childflow 参数" in str(exc.value)


# ---------------------------------------------------------------------------
# _eval_parallel_branches / _parallel_branch
# ---------------------------------------------------------------------------

class TestEvalParallelBranchesMutations:
    def test_dict_form(self):
        ctx = _ctx(childflows={"cf": _CHILD_IR})
        d = ast.Dict(
            keys=[ast.Constant("branch_a")],
            values=[ast.Name("cf", ast.Load())],
        )
        d.lineno = 3
        branches = _eval_parallel_branches(d, ctx)
        assert branches == [{"name": "branch_a", "flow": _CHILD_IR}]

    def test_list_of_tuples_form(self):
        ctx = _ctx(childflows={"cf": _CHILD_IR})
        lst = ast.List(
            elts=[
                ast.Tuple(
                    elts=[ast.Constant("a"), ast.Name("cf", ast.Load())],
                    ctx=ast.Load(),
                )
            ],
            ctx=ast.Load(),
        )
        lst.lineno = 3
        branches = _eval_parallel_branches(lst, ctx)
        assert branches[0]["name"] == "a"
        assert branches[0]["flow"] == _CHILD_IR

    def test_dict_non_string_key_message(self):
        d = ast.Dict(keys=[ast.Constant(1)], values=[ast.Name("cf", ast.Load())])
        d.lineno = 2
        with pytest.raises(_CodeflowError) as exc:
            _eval_parallel_branches(d, _ctx())
        msg = str(exc.value)
        assert "PARALLEL 分支名必须是字符串常量" in msg
        assert "XX" not in msg
        assert "第 ?" not in msg

    def test_list_tuple_name_not_string_message(self):
        lst = ast.List(
            elts=[
                ast.Tuple(
                    elts=[ast.Constant(1), ast.Name("cf", ast.Load())],
                    ctx=ast.Load(),
                )
            ],
            ctx=ast.Load(),
        )
        lst.lineno = 2
        with pytest.raises(_CodeflowError) as exc:
            _eval_parallel_branches(lst, _ctx(childflows={"cf": _CHILD_IR}))
        assert "PARALLEL 分支名必须是字符串常量" in str(exc.value)

    def test_list_invalid_tuple_message(self):
        lst = ast.List(elts=[ast.Constant(1)], ctx=ast.Load())
        lst.lineno = 2
        with pytest.raises(_CodeflowError) as exc:
            _eval_parallel_branches(lst, _ctx())
        assert "PARALLEL 分支需要 (name, flow) 元组" in str(exc.value)

    def test_unsupported_branches_shape_message(self):
        with pytest.raises(_CodeflowError) as exc:
            _eval_parallel_branches(ast.Constant("bad"), _ctx())
        assert "PARALLEL branches 需要是 dict 字面量或 (name, flow) 元组列表" in str(
            exc.value
        )

    def test_parallel_branch_wraps_childflow(self):
        ctx = _ctx(childflows={"cf": _CHILD_IR})
        flow = ast.Name("cf", ast.Load())
        spec = _parallel_branch("x", flow, ctx)
        assert spec == {"name": "x", "flow": _CHILD_IR}


# ---------------------------------------------------------------------------
# _eval_join
# ---------------------------------------------------------------------------

class TestEvalJoinMutations:
    def test_string_list(self):
        join = ast.List(
            elts=[ast.Constant("a"), ast.Constant("b")],
            ctx=ast.Load(),
        )
        assert _eval_join(join, _ctx()) == ["a", "b"]

    def test_non_constant_element_message(self):
        join = ast.List(elts=[ast.Name("x", ast.Load())], ctx=ast.Load())
        join.lineno = 2
        with pytest.raises(_CodeflowError) as exc:
            _eval_join(join, _ctx())
        msg = str(exc.value)
        assert "join 列表元素必须是字符串常量" in msg
        assert "XXjoin" not in msg
        assert "第 ?" not in msg

    def test_join_numeric_constants_stringified(self):
        join = ast.List(elts=[ast.Constant(1), ast.Constant(2)], ctx=ast.Load())
        assert _eval_join(join, _ctx()) == ["1", "2"]

    def test_not_list_message(self):
        node = ast.Constant("a")
        node.lineno = 3
        with pytest.raises(_CodeflowError) as exc:
            _eval_join(node, _ctx())
        msg = str(exc.value)
        assert "join 必须是列表字面量" in msg
        assert "第 ?" not in msg
