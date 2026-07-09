"""Expression and condition compilation for @flow AST."""
from __future__ import annotations

import ast
from typing import Any, Dict, Optional

from plaita.dsl.codeflow._common import (
    _BINOP_TO_F,
    _BUILTIN_TO_F,
    _COLLECTION_CALL_NAMES,
    _COMPARE_OP,
    _FOOTGUN_HINTS,
    _NEGATE_OP,
    _NS_PREFIX,
    _CodeflowError,
    _CompileCtx,
    _custom_node_type,
    _describe_call,
    _node_call_kind,
    _raise_if_unregistered_custom,
)

def _compile_expr(node: ast.AST, ctx: _CompileCtx) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return _resolve_name(node.id, node, ctx)
    if isinstance(node, ast.Attribute):
        base = _compile_expr(node.value, ctx)
        if not isinstance(base, str):
            raise _CodeflowError(f"属性访问的基底必须是命名空间/节点引用，得到 {base!r}", node)
        return f"{base}.{node.attr}"
    if isinstance(node, ast.Call):
        return _compile_call_expr(node, ctx)
    if isinstance(node, ast.BinOp):
        fname = _BINOP_TO_F.get(type(node.op))
        if fname is None:
            raise _CodeflowError(f"不支持的二元运算 {type(node.op).__name__}", node)
        left = _compile_expr(node.left, ctx)
        right = _compile_expr(node.right, ctx)
        return f"$F.{fname}({_render_arg(left)}, {_render_arg(right)})"
    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.USub):
            operand = _compile_expr(node.operand, ctx)
            return f"$F.sub(0, {_render_arg(operand)})"
        if isinstance(node.op, ast.Not):
            raise _CodeflowError("not 要用在条件位置（if/while 判断），不能出现在表达式里", node)
        raise _CodeflowError(f"不支持的一元运算 {type(node.op).__name__}", node)
    if isinstance(node, ast.Subscript):
        base = _compile_expr(node.value, ctx)
        idx = _eval_subscript_index(node.slice, ctx)
        if not isinstance(base, str):
            raise _CodeflowError("下标访问的基底必须是命名空间/节点引用", node)
        return f"{base}[{idx}]"
    if isinstance(node, ast.Dict):
        d: Dict[Any, Any] = {}
        for k, v in zip(node.keys, node.values):
            if k is None:
                raise _CodeflowError("不支持 ** 解包", node)
            if not isinstance(k, ast.Constant):
                raise _CodeflowError("dict 的 key 必须是常量", node)
            d[k.value] = _compile_expr(v, ctx)
        return d
    if isinstance(node, (ast.List, ast.Tuple)):
        return [_compile_expr(e, ctx) for e in node.elts]
    if isinstance(node, ast.BoolOp) or isinstance(node, ast.Compare):
        raise _CodeflowError("比较/and/or 只能出现在条件位置（if 判断）", node)
    # 常见 Python 写法在 @flow 里不支持——给重写提示, 而不是抛 ast.dump
    _footgun_hint = _FOOTGUN_HINTS.get(type(node))
    if _footgun_hint:
        raise _CodeflowError(_footgun_hint, node)
    raise _CodeflowError(
        f"不支持的表达式 {type(node).__name__}（@flow 只支持字面量/变量/属性/下标/"
        f"函数调用/算术/字典与列表字面量, 详见 code-dsl 文档「表达式语义边界」）",
        node,
    )


def _resolve_name(name: str, node: ast.Name, ctx: _CompileCtx) -> str:
    if name in _NS_PREFIX:
        return _NS_PREFIX[name]
    if name in ctx.names:
        return ctx.names[name]
    if name in _BUILTIN_TO_F:
        # 单独出现的 len(x) 走 Call 分支；裸 len 名字无意义
        raise _CodeflowError(f"内置函数 {name} 必须以调用形式使用", node)
    raise _CodeflowError(
        f"未知名字 {name!r}：可用 INPUT/NODE/GLOBAL/PARENT/ENV/F 或已赋值的节点变量", node)


def _eval_subscript_index(sl: ast.AST, ctx: _CompileCtx) -> str:
    # 仅支持常量下标（数字）；负数也支持（引擎认 [-?\d+]）
    if isinstance(sl, ast.Index):  # py<3.9 兼容
        sl = sl.value
    if isinstance(sl, ast.Constant) and isinstance(sl.value, int):
        return str(sl.value)
    raise _CodeflowError("下标只支持整数常量", sl)


def _render_arg(v: Any) -> str:
    """把一个编译后的参数渲染进 ``$F.func(...)`` 的参数串。"""
    if isinstance(v, str):
        if v.startswith("$"):
            return v
        return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    if isinstance(v, (int, float)):
        return str(v)
    raise _CodeflowError(f"无法作为 $F 参数渲染的值: {v!r}")


def _compile_call_expr(node: ast.Call, ctx: _CompileCtx) -> Any:
    func = node.func
    # F.func(...) -> "$F.func(args)"
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) \
            and func.value.id == "F":
        args = [_compile_expr(a, ctx) for a in node.args]
        rendered = ", ".join(_render_arg(a) for a in args)
        return f"$F.{func.attr}({rendered})"
    # 节点调用出现在表达式位置：不允许（节点调用只能作为语句或赋值右侧）
    node_kind = _node_call_kind(func)
    if node_kind is not None or (
        isinstance(func, ast.Name) and func.id in _COLLECTION_CALL_NAMES
    ):
        bad = node_kind or (func.id if isinstance(func, ast.Name) else "节点")
        raise _CodeflowError(
            f"{bad}(...) 是节点调用，只能作为语句或赋值右侧，不能嵌在表达式里", node)
    # 自定义节点调用同样只能作语句/赋值右侧
    if _custom_node_type(func, ctx) is not None:
        raise _CodeflowError(
            f"{func.id}(...) 是节点调用，只能作为语句或赋值右侧，不能嵌在表达式里", node)
    # 内置 len/abs/round -> $F.len(...)
    if isinstance(func, ast.Name) and func.id in _BUILTIN_TO_F:
        args = [_compile_expr(a, ctx) for a in node.args]
        rendered = ", ".join(_render_arg(a) for a in args)
        return f"$F.{_BUILTIN_TO_F[func.id]}({rendered})"
    # 大写占位名但未注册 → 给可读错误（供 AI 自纠）
    _raise_if_unregistered_custom(func, ctx)
    # 其它调用都不支持——给可读的调用名而不是 ast.dump
    call_name = _describe_call(func)
    raise _CodeflowError(
        f"不支持的调用 {call_name}：@flow 表达式里只能调 F.xxx(...) 或内置 len/abs/round/str",
        node,
    )


# ---------------------------------------------------------------------------
# 条件编译：ast.expr -> Condition / ConditionGroup dict
# ---------------------------------------------------------------------------

def _compile_condition(node: ast.AST, ctx: _CompileCtx) -> Dict[str, Any]:
    if isinstance(node, ast.BoolOp):
        relation = "and" if isinstance(node.op, ast.And) else "or"
        return {"relation": relation,
                "conditions": [_compile_condition(v, ctx) for v in node.values]}
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return _negate_condition(_compile_condition(node.operand, ctx), node)
    if isinstance(node, ast.Compare):
        return _compile_compare(node, ctx)
    # 裸表达式真值测试 -> (expr != False)
    expr = _compile_expr(node, ctx)
    return {"field": expr, "operator": "ne", "value": False}


def _compile_compare(node: ast.Compare, ctx: _CompileCtx) -> Dict[str, Any]:
    left = _compile_expr(node.left, ctx)
    # 链式比较 a < b < c -> and(a<b, b<c)
    conds: List[Dict[str, Any]] = []
    cur_left = left
    for op, comp in zip(node.ops, node.comparators):
        op_name = _COMPARE_OP.get(type(op))
        if op_name is None:
            raise _CodeflowError(f"不支持的比较运算 {type(op).__name__}", node)
        right = _compile_expr(comp, ctx)
        conds.append({"field": cur_left, "operator": op_name, "value": right})
        cur_left = right
    if len(conds) == 1:
        return conds[0]
    return {"relation": "and", "conditions": conds}


def _negate_condition(cond: Dict[str, Any], node: ast.AST) -> Dict[str, Any]:
    if "operator" in cond:
        op = cond["operator"]
        if op not in _NEGATE_OP:
            raise _CodeflowError(f"not 无法翻转运算符 {op!r}", node)
        return {"field": cond["field"], "operator": _NEGATE_OP[op], "value": cond["value"]}
    rel = cond.get("relation")
    if rel == "and":
        return {"relation": "or", "conditions": [_negate_condition(c, node) for c in cond["conditions"]]}
    if rel == "or":
        return {"relation": "and", "conditions": [_negate_condition(c, node) for c in cond["conditions"]]}
    raise _CodeflowError("not 的操作数不合法", node)
