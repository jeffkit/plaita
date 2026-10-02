"""IR → ``@flow`` 源码反向发射器（``compile_source`` 的逆操作）。

把 JSON/YAML/builder 产出的 Flow IR dict 重构为可读的 ``@flow`` Python 源码，
使存量 JSON flow 能迁移到 codeflow DSL。与 ``compile_source`` 构成双向桥：

    compile_source(emit_source(ir))  ≡  ir   （语义等价）

**语义等价而非逐字节等价**：if/return 的 slug 派生 id、无名节点的 ``_n{k}``
计数 id 由重编译时的源码形态决定，发射器无法从 IR 反推原始源码文本，这两类
id 在 round-trip 后可能变化（变量名 id、显式 ``id=`` 钉住的 id 恒定）。验收
口径是「归一化 IR 相等」（剥 name/desc/source_line 派生注解 + 非语义 id 按
图序规范化），见 ``tests/unit/test_codeflow_emit.py``。

**不支持的构造**（抛 ``EmitError``，不产出错误代码）：``switch``/``bool``
合成节点（builder 专属）、空真分支或两分支同址的 if、非 object 的
``inputType``。IR 里以 ``$`` 开头的字符串一律按表达式逆映射——原始字面量
恰以 ``$`` 开头的场景编译期已丢失区分，属已知边界。
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

__all__ = ["EmitError", "emit_source"]

_AUTO_ID_RE = re.compile(r"_n\d+")

# 由 _BodyEmitter 专属分支处理的类型；出现在节点调用位置即不可表达
_UNEMITTABLE_TYPES = {"switch", "bool", "start"}


class EmitError(ValueError):
    """IR 含 @flow 源码无法表达的构造。"""


# ---------------------------------------------------------------------------
# 表达式逆映射：$-表达式串 → Python 源码
# ---------------------------------------------------------------------------

# 前缀逆映射表（最长前缀优先）。$NODE.<v> 落回裸变量名（编译期 ctx.names 会
# 重新登记 $NODE.<v>）；循环体内 $PARENT.NODE.<v> 同理落回外层变量名。
_BASE_NS: List[Tuple[str, str]] = [
    ("$NODE.", ""),
    ("$INPUT", "INPUT"),
    ("$GLOBAL", "GLOBAL"),
    ("$PARENT", "PARENT"),
    ("$ENV", "ENV"),
]
# while 条件运行在父侧 loop_ctx：item/rounds 由引擎注入键携带
_COND_NS: List[Tuple[str, str]] = [
    ("$LOOP-ITEM", "item"),
    ("$LOOP-INDEX", "rounds"),
    *_BASE_NS,
]


def _loop_body_ns(index_name: str) -> List[Tuple[str, str]]:
    """循环体命名空间：循环目标名 → 子流程输入；外层变量 → 裸名（重编译时
    由 ``$PARENT.NODE.`` 映射还原）。REDUCE 子流程用 array 输入按下标取。"""
    return [
        ("$PARENT.NODE.", ""),
        ("$INPUT.item", "item"),
        ("$INPUT.index", index_name),
        ("$INPUT[0]", "first"),
        ("$INPUT[1]", "second"),
        *_BASE_NS,
    ]


def _split_args(text: str) -> List[str]:
    """按顶层逗号拆 ``$F.fn(...)`` 的参数串（尊重括号与双引号字符串）。"""
    parts: List[str] = []
    depth = 0
    in_str = False
    esc = False
    cur: List[str] = []
    for ch in text:
        if in_str:
            cur.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            cur.append(ch)
        elif ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def _invert_f_arg(arg: str, ns: List[Tuple[str, str]]) -> str:
    if arg.startswith('"'):
        # 文法字符串转义（\n \t \" \\）与 Python 字面量转义同形，逐字保留
        return arg
    if arg.startswith("$"):
        return _invert_expr(arg, ns)
    if arg == "true":
        return "True"
    if arg == "false":
        return "False"
    if arg == "null":
        return "None"
    return arg


def _invert_expr(s: str, ns: List[Tuple[str, str]] = _BASE_NS) -> str:
    """把编译后的 ``$``-表达式串逆映射为 Python 表达式源码。"""
    s = s.strip()
    if s.startswith("$F."):
        close = s.rfind(")")
        if close < 0:
            raise EmitError(f"无法解析 $F 调用: {s!r}")
        head = s[3:close]  # 如 'concat($INPUT.x, "a")'
        lparen = head.find("(")
        if lparen < 0:
            raise EmitError(f"无法解析 $F 调用: {s!r}")
        fn = head[:lparen]
        args = _split_args(head[lparen + 1 :])
        rendered = ", ".join(_invert_f_arg(a, ns) for a in args)
        return f"F.{fn}({rendered})"
    for prefix, replacement in ns:
        if s.startswith(prefix):
            return replacement + s[len(prefix) :]
    raise EmitError(f"无法逆映射表达式 {s!r}")


def _invert_value(v: Any, ns: List[Tuple[str, str]] = _BASE_NS) -> str:
    """IR 字段值 → Python 源码：$-串按表达式逆映射，容器递归，标量 repr。"""
    if isinstance(v, str) and v.startswith("$"):
        return _invert_expr(v, ns)
    if isinstance(v, dict):
        items = ", ".join(f"{_invert_value(k, ns)}: {_invert_value(x, ns)}" for k, x in v.items())
        return "{" + items + "}"
    if isinstance(v, list):
        return "[" + ", ".join(_invert_value(x, ns) for x in v) + "]"
    if v is True:
        return "True"
    if v is False:
        return "False"
    if v is None:
        return "None"
    return repr(v)


_CMP_PY: Dict[str, str] = {
    "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
    "eq": "==", "ne": "!=", "in": "in", "notIn": "not in",
}


def _invert_cond(cond: Dict[str, Any], ns: List[Tuple[str, str]]) -> str:
    """Condition/ConditionGroup dict → Python 条件表达式。"""
    if "relation" in cond:
        joiner = f" {cond['relation']} "
        parts = []
        for c in cond.get("conditions", []):
            text = _invert_cond(c, ns)
            if "relation" in c:
                text = f"({text})"
            parts.append(text)
        return joiner.join(parts)
    field = _invert_value(cond.get("field"), ns)
    op = cond.get("operator")
    value = cond.get("value")
    if op == "ne" and value is False:
        # 裸真值测试的规范形态（编译器对 `if x:` 的产物）——还原为裸写法
        return field
    if op == "eq" and value is True:
        # `not x` 的产物（ne 取反成 eq+True）——还原为 not 形态，slug 才能对上
        return f"not {field}"
    py_op = _CMP_PY.get(op)
    if py_op is None:
        raise EmitError(f"不支持的条件算子 {op!r}")
    return f"{field} {py_op} {_invert_value(value, ns)}"


# ---------------------------------------------------------------------------
# 结构图重构：nodes[] → 语句序列
# ---------------------------------------------------------------------------

class _ChildCollector:
    """收集被引用的 childFlow IR，按引用顺序命名 ``child_1..N``（同对象复用）。"""

    def __init__(self) -> None:
        self.entries: List[Tuple[str, Dict[str, Any]]] = []

    def register(self, child_ir: Dict[str, Any]) -> str:
        for name, ir in self.entries:
            if ir is child_ir:
                return name
        name = f"child_{len(self.entries) + 1}"
        self.entries.append((name, child_ir))
        return name


class _BodyEmitter:
    """单个流程作用域（主流程或 childflow / 循环子流程）的语句发射器。"""

    def __init__(
        self,
        ir: Dict[str, Any],
        ns: List[Tuple[str, str]],
        collector: _ChildCollector,
    ) -> None:
        self.nodes: Dict[str, Dict[str, Any]] = {n["id"]: n for n in ir.get("nodes", [])}
        self.ns = ns
        self.childflows = collector
        self._reach_cache: Dict[str, Set[str]] = {}
        self._term_cache: Dict[str, bool] = {}

    # -- 图工具 -----------------------------------------------------------

    def _reachable(self, nid: Optional[str]) -> Set[str]:
        """从 nid 沿 next/else_next 可达的节点 id 集（不下钻 childFlow 作用域）。"""
        if nid is None:
            return set()
        if nid in self._reach_cache:
            return self._reach_cache[nid]
        seen: Set[str] = set()
        stack = [nid]
        while stack:
            cur = stack.pop()
            if cur in seen or cur not in self.nodes:
                continue
            seen.add(cur)
            node = self.nodes[cur]
            for arc in (node.get("next"), node.get("else_next")):
                if arc and arc not in seen:
                    stack.append(arc)
        self._reach_cache[nid] = seen
        return seen

    def _find_join(self, t_id: str, f_id: str) -> Optional[str]:
        """两分支的汇合点：共同可达集中「不被其他共同可达节点可达」的唯一入口。"""
        common = self._reachable(t_id) & self._reachable(f_id)
        if not common:
            return None
        for cand in sorted(common):
            others = common - {cand}
            if not any(cand in self._reachable(o) for o in others):
                return cand
        # 互不可达的多入口（异常图）：退化取最小，语义由重编译校验兜底
        return sorted(common)[0]

    # -- 主入口 -----------------------------------------------------------

    def emit_chain(self, entry: Optional[str], stop: Set[str]) -> List[str]:
        """从 entry 线性走链发射语句；遇 stop 中的节点 id（汇合点）即停。"""
        lines: List[str] = []
        cur = entry
        while cur is not None and cur not in stop:
            node = self.nodes.get(cur)
            if node is None:
                raise EmitError(f"链引用了不存在的节点 {cur!r}")
            ntype = node["type"]
            if ntype == "end":
                lines.append(self._emit_return(node))
                return lines
            if ntype == "assignment":
                lines.extend(self._emit_assignment(node))
                cur = node.get("next")
                continue
            if ntype == "if":
                block_lines, join = self._emit_if(node, stop)
                lines.extend(block_lines)
                cur = join
                continue
            if ntype == "while":
                # while/collection 自带 next 续链，整段（含后续）由它们发射
                return lines + self._emit_while(node, stop)
            if ntype in ("map", "filter", "find", "loop", "reduce"):
                return lines + self._emit_collection(node, stop)
            lines.extend(self._emit_node_call(node))
            cur = node.get("next")
        return lines

    # -- 各语句形态 --------------------------------------------------------

    def _emit_return(self, node: Dict[str, Any]) -> str:
        output = node.get("output")
        if output is None:
            return "return"
        return f"return {_invert_value(output, self.ns)}"

    def _emit_assignment(self, node: Dict[str, Any]) -> List[str]:
        expr = _invert_value(node.get("output"), self.ns)
        if _AUTO_ID_RE.fullmatch(node["id"]):
            # 表达式语句（无名赋值）：保持语句形态，不引入变量名
            return [expr]
        return [f"{node['id']} = {expr}"]

    def _all_paths_return(self, nid: Optional[str], stop: Set[str]) -> bool:
        """从 nid 出发的所有路径是否都在触及 stop（或图尾）之前 return。"""
        if nid is None or nid in stop:
            return False
        memo = self._term_cache
        if nid in memo:
            return memo[nid]
        node = self.nodes.get(nid)
        if node is None:
            memo[nid] = False
            return False
        ntype = node["type"]
        if ntype == "end":
            memo[nid] = True
            return True
        if ntype == "if":
            result = (self._all_paths_return(node.get("next"), stop)
                      and self._all_paths_return(node.get("else_next"), stop))
        else:
            result = self._all_paths_return(node.get("next"), stop)
        memo[nid] = result
        return result

    def _emit_if(self, node: Dict[str, Any], outer_stop: Set[str]) -> Tuple[List[str], Optional[str]]:
        t_id, f_id = node.get("next"), node.get("else_next")
        if t_id is None or f_id is None:
            raise EmitError(f"if 节点 {node['id']!r} 缺分支出口")
        if t_id == f_id:
            raise EmitError(f"if 节点 {node['id']!r} 两分支同址（空体），@flow 无法表达")
        join = self._find_join(t_id, f_id)
        arm_stop = set(outer_stop) | ({join} if join else set())

        cond = _invert_cond(node.get("condition", {}), self.ns)
        true_lines = self.emit_chain(t_id, arm_stop)
        if not true_lines:
            raise EmitError(f"if 节点 {node['id']!r} 真分支为空，@flow 无法表达")
        false_lines = self.emit_chain(f_id, arm_stop)

        # 卫语句形态：无汇合点、真分支全路径 return、假分支续行——拍平成顺序
        # `if cond: return ...`，假分支入口交还外层链。嵌套 else 会把后续语句
        # 卷进假分支，打乱重编译的节点顺序，计数 id 随之错位。双分支都 return
        # 时保持 if/else 嵌套（与编译序 body→orelse 一致）。
        if join is None:
            t_term = self._all_paths_return(t_id, outer_stop)
            f_term = self._all_paths_return(f_id, outer_stop)
            if t_term and not f_term:
                return [f"if {cond}:"] + [f"    {ln}" for ln in true_lines], f_id
            if not t_term and not f_term:
                raise EmitError(
                    f"if 节点 {node['id']!r} 两分支均续行且无汇合点，无法结构化为 @flow")

        lines = [f"if {cond}:"]
        lines.extend(f"    {ln}" for ln in true_lines)
        if false_lines:
            lines.append("else:")
            lines.extend(f"    {ln}" for ln in false_lines)
        return lines, join

    def _emit_while(self, node: Dict[str, Any], outer_stop: Set[str]) -> List[str]:
        child_ir = node.get("child_flow") or {}
        cond = _invert_cond(node.get("condition", {}), _COND_NS)
        body = self._emit_inline_child(child_ir, _loop_body_ns("rounds"))
        # 恒用 for-head 形态：它是唯一能显式 id= 钉住节点 id 的写法（statement
        # 形态的 id 由条件 slug 派生，源码层不可控）
        args = [cond, f"id={node['id']!r}"]
        if "max_iterations" in node:
            args.append(f"max_iterations={node['max_iterations']}")
        lines = [f"for item in WHILE({', '.join(args)}):"]
        lines.extend(f"    {ln}" for ln in body)
        return self._emit_tail(lines, node, outer_stop)

    def _emit_collection(self, node: Dict[str, Any], outer_stop: Set[str]) -> List[str]:
        kind = node["type"].upper()
        child_ir = node.get("childFlow") or {}
        coll = _invert_value(node.get("collection"), self.ns)

        if kind == "REDUCE":
            target = "first, second"
        else:
            target = "item, index" if self._child_uses(child_ir, "$INPUT.index") else "item"

        kwargs = [f"id={node['id']!r}"]
        if kind == "MAP":
            if node.get("concurrent"):
                kwargs.append("concurrent=True")
            if node.get("maxConcurrent") is not None:
                kwargs.append(f"max_concurrent={node['maxConcurrent']}")
        if kind == "REDUCE" and node.get("initial") is not None:
            kwargs.append(f"initial={_invert_value(node['initial'], self.ns)}")

        body = self._emit_inline_child(child_ir, _loop_body_ns("index"))
        lines = [f"for {target} in {kind}({', '.join([coll] + kwargs)}):"]
        lines.extend(f"    {ln}" for ln in body)
        return self._emit_tail(lines, node, outer_stop)

    def _emit_tail(self, lines: List[str], node: Dict[str, Any], outer_stop: Set[str]) -> List[str]:
        nxt = node.get("next")
        if nxt is not None and nxt not in outer_stop:
            lines.extend(self.emit_chain(nxt, outer_stop))
        return lines

    def _child_uses(self, child_ir: Dict[str, Any], token: str) -> bool:
        def _scan(value: Any) -> bool:
            if isinstance(value, str):
                return token in value
            if isinstance(value, dict):
                return any(_scan(v) for v in value.values())
            if isinstance(value, list):
                return any(_scan(v) for v in value)
            return False

        return _scan(child_ir)

    def _emit_inline_child(self, child_ir: Dict[str, Any], ns: List[Tuple[str, str]]) -> List[str]:
        """while / 集合节点内联子流程体的语句发射。"""
        start = next((n for n in child_ir.get("nodes", []) if n.get("type") == "start"), None)
        if start is None:
            raise EmitError("childFlow 缺 start 节点")
        sub = _BodyEmitter(child_ir, ns, self.childflows)
        return sub.emit_chain(start.get("next"), set())

    # -- 节点调用 -----------------------------------------------------------

    def _emit_error_handler(self, eh: Dict[str, Any]) -> str:
        kwargs = [f"strategy={eh['strategy']!r}"]
        if eh.get("retryTimes") is not None:
            kwargs.append(f"retry_times={eh['retryTimes']}")
        if eh.get("defaultValue") is not None:
            kwargs.append(f"default_value={_invert_value(eh['defaultValue'], self.ns)}")
        if eh.get("errorCode") is not None:
            kwargs.append(f"error_code={eh['errorCode']}")
        if eh.get("errorMessage") is not None:
            kwargs.append(f"error_message={eh['errorMessage']!r}")
        return f"ErrorHandler({', '.join(kwargs)})"

    def _common_kwargs(self, spec: Dict[str, Any]) -> List[str]:
        kwargs: List[str] = []
        if spec.get("timeout") is not None:
            kwargs.append(f"timeout={_invert_value(spec['timeout'], self.ns)}")
        if spec.get("errorHandler") is not None:
            kwargs.append(f"on_error={self._emit_error_handler(spec['errorHandler'])}")
        return kwargs

    def _emit_node_call(self, node: Dict[str, Any]) -> List[str]:
        ntype = node["type"]
        if ntype in _UNEMITTABLE_TYPES:
            raise EmitError(f"节点类型 {ntype!r} 不可表达为 @flow 源码")
        builder = getattr(self, f"_call_{ntype}", None)
        call = builder(node) if builder is not None else self._call_custom(node)
        if _AUTO_ID_RE.fullmatch(node["id"]):
            # 无名节点调用：id 由重编译自动分配（归一化比较口径）
            return [call]
        return [f"{node['id']} = {call}"]

    def _call_http(self, node: Dict[str, Any]) -> str:
        kwargs: List[str] = []
        if node.get("method", "POST") != "POST":
            kwargs.append(f"method={node['method']!r}")
        kwargs.append(f"url={_invert_value(node.get('url'), self.ns)}")
        for ir_key in ("headers", "body", "timeout", "input"):
            if node.get(ir_key) is not None:
                kwargs.append(f"{ir_key}={_invert_value(node[ir_key], self.ns)}")
        kwargs.extend(self._common_kwargs(node))
        return f"HTTP({', '.join(kwargs)})"

    def _call_code(self, node: Dict[str, Any]) -> str:
        kwargs = [f"code={_invert_value(node.get('code'), self.ns)}"]
        if node.get("language") is not None:
            kwargs.append(f"lang={node['language']!r}")
        if node.get("input") is not None:
            kwargs.append(f"input={_invert_value(node['input'], self.ns)}")
        if node.get("sandbox_backend") is not None:
            kwargs.append(f"sandbox_backend={node['sandbox_backend']!r}")
        kwargs.extend(self._common_kwargs(node))
        return f"CODE({', '.join(kwargs)})"

    def _call_event(self, node: Dict[str, Any]) -> str:
        kwargs = [f"type={_invert_value(node.get('eventType'), self.ns)}"]
        if node.get("eventFilter") is not None:
            kwargs.append(f"filter={_invert_value(node['eventFilter'], self.ns)}")
        kwargs.extend(self._common_kwargs(node))
        return f"EVENT({', '.join(kwargs)})"

    def _call_child_ref(self, node: Dict[str, Any], placeholder: str) -> str:
        child_name = self.childflows.register(node.get("childFlow") or {})
        kwargs: List[str] = []
        if node.get("input") is not None:
            kwargs.append(f"input={_invert_value(node['input'], self.ns)}")
        kwargs.append(f"flow={child_name}")
        kwargs.extend(self._common_kwargs(node))
        return f"{placeholder}({', '.join(kwargs)})"

    def _call_child(self, node: Dict[str, Any]) -> str:
        return self._call_child_ref(node, "CHILD")

    def _call_reference(self, node: Dict[str, Any]) -> str:
        return self._call_child_ref(node, "REFERENCE")

    def _call_parallel(self, node: Dict[str, Any]) -> str:
        branch_items = []
        for branch in node.get("branches", []):
            child_name = self.childflows.register(branch.get("flow") or {})
            branch_items.append(f"{branch['name']!r}: {child_name}")
        kwargs = ["branches={" + ", ".join(branch_items) + "}"]
        if node.get("mode", "thread") != "thread":
            kwargs.append(f"mode={node['mode']!r}")
        if node.get("joinBranches"):
            kwargs.append(f"join={node['joinBranches']!r}")
        if node.get("isConditional"):
            kwargs.append("conditional=True")
        kwargs.extend(self._common_kwargs(node))
        return f"PARALLEL({', '.join(kwargs)})"

    def _call_custom(self, node: Dict[str, Any]) -> str:
        skip = {"type", "id", "next", "source_line", "name", "desc",
                "timeout", "errorHandler"}
        placeholder = node["type"].upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", placeholder):
            raise EmitError(
                f"node_type {node['type']!r} 大写化后不是合法占位符，无法表达为 @flow")
        kwargs: List[str] = []
        for key, value in node.items():
            if key in skip:
                continue
            kwargs.append(f"{key}={_invert_value(value, self.ns)}")
        kwargs.extend(self._common_kwargs(node))
        return f"{placeholder}({', '.join(kwargs)})"


# ---------------------------------------------------------------------------
# childflow / 主流程拼装
# ---------------------------------------------------------------------------

def _emit_childflow_blocks(name: str, child_ir: Dict[str, Any]) -> List[str]:
    """发射一个 ``@childflow`` 函数；孙辈 childflow 先于引用它的函数输出
    （compile_source 按源码顺序收集，被引用者必须先定义）。"""
    collector = _ChildCollector()
    emitter = _BodyEmitter(child_ir, list(_BASE_NS), collector)
    start = next((n for n in child_ir.get("nodes", []) if n.get("type") == "start"), None)
    if start is None:
        raise EmitError("childFlow 缺 start 节点")
    body = emitter.emit_chain(start.get("next"), set())

    blocks: List[str] = []
    for inner_name, inner_ir in collector.entries:
        blocks.extend(_emit_childflow_blocks(inner_name, inner_ir))
    deco = "@childflow"
    if child_ir.get("desc") is not None:
        deco = f"@childflow(desc={child_ir['desc']!r})"
    lines = [deco, f"def {name}(INPUT):"]
    lines.extend(f"    {ln}" for ln in body)
    blocks.append("\n".join(lines))
    return blocks


def _emit_flow_decorator(ir: Dict[str, Any]) -> str:
    input_type = ir.get("inputType")
    if input_type is not None and input_type != {"dataType": "object"}:
        raise EmitError(
            f"inputType {input_type!r} 非 object，@flow 源码无法表达（@flow 恒为 object 输入）")
    flow_id = ir.get("flow_id") or "emitted_flow"
    args = [repr(str(flow_id))]
    for key in ("desc", "version", "author", "timeout"):
        if ir.get(key) is not None:
            args.append(f"{key}={ir[key]!r}")
    if ir.get("globalContext") is not None:
        args.append(f"global_context={_pyliteral(ir['globalContext'])}")
    if ir.get("metadata") is not None:
        args.append(f"metadata={_pyliteral(ir['metadata'])}")
    return "@flow(" + ", ".join(args) + ")"


def _pyliteral(value: Any) -> str:
    """flow 级字面量（globalContext/metadata）→ Python 源码（纯 repr，容器递归）。"""
    if isinstance(value, dict):
        items = ", ".join(f"{k!r}: {_pyliteral(v)}" for k, v in value.items())
        return "{" + items + "}"
    if isinstance(value, list):
        return "[" + ", ".join(_pyliteral(v) for v in value) + "]"
    return repr(value)


def _flow_def_name(flow_id: str) -> str:
    name = re.sub(r"\W", "_", flow_id).strip("_") or "emitted_flow"
    if name[0].isdigit():
        name = f"flow_{name}"
    return name


def emit_source(ir: Dict[str, Any]) -> str:
    """Flow IR dict → 完整 ``@flow`` 源码（含被引用的 ``@childflow`` 函数）。

    ``compile_source(emit_source(ir))`` 与 ``ir`` 语义等价（口径见模块
    docstring）。ir 形态不符或含不可表达构造时抛 ``EmitError``。
    """
    if not isinstance(ir, dict) or not isinstance(ir.get("nodes"), list):
        raise EmitError("ir 必须是含 nodes 列表的 Flow IR dict")

    collector = _ChildCollector()
    emitter = _BodyEmitter(ir, list(_BASE_NS), collector)
    start = next((n for n in ir["nodes"] if n.get("type") == "start"), None)
    if start is None:
        raise EmitError("ir 缺 start 节点")
    body = emitter.emit_chain(start.get("next"), set())

    blocks: List[str] = []
    for child_name, child_ir in collector.entries:
        blocks.extend(_emit_childflow_blocks(child_name, child_ir))
    blocks.append("\n".join([
        _emit_flow_decorator(ir),
        f"def {_flow_def_name(str(ir.get('flow_id') or 'emitted_flow'))}(INPUT):",
        *(f"    {ln}" for ln in body),
    ]))
    return "\n\n".join(blocks)
