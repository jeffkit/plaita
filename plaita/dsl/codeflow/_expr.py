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
from plaita.logger import logger


class _CompiledExpr(str):
    """编译器自己产出的 ``$``-表达式串（``$INPUT.x`` / ``$F.concat(...)``）。

    plaita#231：``_compile_expr(ast.Constant)`` 返回的是用户字面量，与编译器
    产出的表达式同为 plain str，``_render_arg`` 无法区分——以 ``$`` 开头的
    字面量会被原样塞进 ``$F.func(...)``，运行期当变量求值（KeyError 或静默
    串值）。用子类标记表达式；只有标记过的值才允许裸渲染。
    """


def _quote_literal(v: str) -> str:
    """把字面量渲染成文法可还原的双引号字符串（转义 ``\\ " \\n \\r \\t``）。"""
    escaped = (v.replace("\\", "\\\\")
                .replace('"', '\\"')
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t"))
    return '"' + escaped + '"'


class _LiteralIR(str):
    """以 ``$`` 开头的**字面量**的 IR 安全形态。

    字面量值本身以 ``$`` 开头时，直接写进 IR 会被运行期求值器当表达式。
    本类的字符串值渲染为 ``$F.concat("<literal>")``（运行期得到原字面量）：
    - IR 序列化（JSON/Flow.model_validate）自动使用该安全串；
    - 作为 ``$F`` 参数时 ``_render_arg`` 用 ``literal`` 原值加引号，不会嵌套。
    """

    def __new__(cls, literal: str) -> "_LiteralIR":
        obj = super().__new__(cls, f"$F.concat({_quote_literal(literal)})")
        obj.literal = literal
        return obj


def _compile_expr(node: ast.AST, ctx: _CompileCtx) -> Any:
    if isinstance(node, ast.Constant):
        # 字面量恰好以 $ 开头：IR 里必须是可还原的字面量，不能裸写（#231）。
        if isinstance(node.value, str) and node.value.startswith("$"):
            return _LiteralIR(node.value)
        return node.value
    if isinstance(node, ast.Name):
        return _resolve_name(node.id, node, ctx)
    if isinstance(node, ast.Attribute):
        base = _compile_expr(node.value, ctx)
        if not isinstance(base, str):
            raise _CodeflowError(f"属性访问的基底必须是命名空间/节点引用，得到 {base!r}", node)
        return _CompiledExpr(f"{base}.{node.attr}")
    if isinstance(node, ast.Call):
        return _compile_call_expr(node, ctx)
    if isinstance(node, ast.BinOp):
        fname = _BINOP_TO_F.get(type(node.op))
        if fname is None:
            raise _CodeflowError(f"不支持的二元运算 {type(node.op).__name__}", node)
        left = _compile_expr(node.left, ctx)
        right = _compile_expr(node.right, ctx)
        return _CompiledExpr(f"$F.{fname}({_render_arg(left)}, {_render_arg(right)})")
    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.USub):
            operand = _compile_expr(node.operand, ctx)
            return _CompiledExpr(f"$F.sub(0, {_render_arg(operand)})")
        if isinstance(node.op, ast.Not):
            operand = _compile_expr(node.operand, ctx)
            return _CompiledExpr(f"$F.not({_render_arg(operand)})")
        raise _CodeflowError(f"不支持的一元运算 {type(node.op).__name__}", node)
    if isinstance(node, ast.BoolOp):
        # and/or 落到注册表同名逻辑函数（or 返回第一个真值，与 Python 真值语义一致）。
        # and 链左折叠嵌套——$F.and 是二元函数，a and b and c 的 Python 语义是
        # (a and b) and c（返回第一个假值本身，而非布尔化）。
        if isinstance(node.op, ast.And):
            values = node.values
            compiled = _compile_expr(values[0], ctx)
            for v in values[1:]:
                compiled = _CompiledExpr(
                    f"$F.and({_render_arg(compiled)}, {_render_arg(_compile_expr(v, ctx))})"
                )
            return compiled
        args = [_compile_expr(v, ctx) for v in node.values]
        rendered = ", ".join(_render_arg(a) for a in args)
        return _CompiledExpr(f"$F.or({rendered})")
    if isinstance(node, ast.Compare):
        # 比较落到注册表同名比较函数（函数名 == if 节点条件算子名，见 _COMPARE_OP）。
        # 链式比较 a < b < c -> $F.and($F.lt(a,b), $F.lt(b,c))。
        conds: List[str] = []
        cur = _compile_expr(node.left, ctx)
        for op, comp in zip(node.ops, node.comparators):
            fname = _COMPARE_OP.get(type(op))
            if fname is None:
                raise _CodeflowError(f"不支持的比较运算 {type(op).__name__}", node)
            right = _compile_expr(comp, ctx)
            conds.append(_CompiledExpr(
                f"$F.{fname}({_render_arg(cur)}, {_render_arg(right)})"
            ))
            cur = right
        if len(conds) == 1:
            return _CompiledExpr(conds[0])
        rendered = ", ".join(_render_arg(c) for c in conds)
        return _CompiledExpr(f"$F.and({rendered})")
    if isinstance(node, ast.IfExp):
        # 三元 -> $F.ifelse(c, a, b)。急切求值：两支都会被求值（$F 函数皆纯，无副作用风险）。
        cond = _compile_expr(node.test, ctx)
        then = _compile_expr(node.body, ctx)
        other = _compile_expr(node.orelse, ctx)
        return _CompiledExpr(
            f"$F.ifelse({_render_arg(cond)}, {_render_arg(then)}, {_render_arg(other)})"
        )
    if isinstance(node, ast.Subscript):
        base = _compile_expr(node.value, ctx)
        idx = _eval_subscript_index(node.slice, ctx)
        if not isinstance(base, str):
            raise _CodeflowError("下标访问的基底必须是命名空间/节点引用", node)
        return _CompiledExpr(f"{base}[{idx}]")
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
        return _CompiledExpr(_NS_PREFIX[name])
    if name in ctx.names:
        return _CompiledExpr(ctx.names[name])
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
    if isinstance(v, _LiteralIR):
        # 用户字面量恰好以 $ 开头：按原字面量加引号（不能裸写，也不能把
        # IR 安全壳 "$F.concat(...)" 再嵌一层）。
        return _quote_literal(v.literal)
    if isinstance(v, _CompiledExpr):
        # 编译器产出的表达式才允许裸渲染。
        return str(v)
    if isinstance(v, str):
        # 普通字面量（包括未经 _compile_expr 的 $ 前缀串）一律按字面量引号。
        return _quote_literal(v)
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
        return _CompiledExpr(f"$F.{func.attr}({rendered})")
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
        return _CompiledExpr(f"$F.{_BUILTIN_TO_F[func.id]}({rendered})")
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
    # #238：`F.contains(left, right)` 是条件算子 contains 的唯一 @flow 拼写
    # （Python 的 `in` 表示的是 in/notIn，语义方向不同：contains(a, b) 是 b in a）。
    # 识别成算子形态后，发射器 `_invert_cond` 才能把它还原回 `F.contains(...)`，
    # conditioned 往返不再因「不支持的条件算子」直接 EmitError。非条件位置
    # 的 F.contains(...) 仍是普通 $F 表达式，不受影响。
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "contains"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "F"
        and len(node.args) == 2
        and not node.keywords
    ):
        return {
            "field": _compile_expr(node.args[0], ctx),
            "operator": "contains",
            "value": _compile_expr(node.args[1], ctx),
        }
    # 裸表达式真值测试 -> truthy 算子（Python bool(x) 语义）。历史实现降级为
    # (expr != False)，空列表/空串/None 会误走真分支——`if items:` 语义错误
    # （2026-10 评审修复包 A1）。过渡期打编译警告，引导作者写显式比较。
    expr = _compile_expr(node, ctx)
    logger.warning(
        "[codeflow] 第 %s 行: 裸真值条件 %r 按 Python 真值语义（truthy）编译；"
        "建议改为显式比较（如 len(INPUT.items) > 0、INPUT.flag == true）以避免歧义",
        getattr(node, "lineno", "?"),
        ast.unparse(node) if hasattr(ast, "unparse") else expr,
    )
    return {"field": expr, "operator": "truthy", "value": True}


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
