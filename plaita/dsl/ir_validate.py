"""plaita.dsl.ir_validate — 共享 Flow IR 拓扑校验。

三条作者前端（builder / codeflow / sexpr）最终都产出同一形态的 dict IR，
再经 ``Flow.model_validate``。历史上拓扑校验在 builder / sexpr 各写一份，
且 codeflow / AI 路径完全跳过——本模块是唯一真相源。

校验内容：
- 节点 id 唯一
- next / else_next / switch·case 分支目标存在
- if 必须有真/假分支
- switch 必须有 isDefault
- parallel 分支 next（若有）目标存在
- ``recursive=True``（默认）时递归 ``childFlow`` 与 ``parallel.branches[].flow``

规则钩子（M 修复包 I）
----------------------

硬编码检查之外，``validate_flow_ir`` 接受 ``rules``：一组随现有递归**逐节点**
回调的规则。规则签名::

    def rule(node: dict, path: str, graph: FlowIRGraph) -> str | None

- ``node`` 当前节点 dict；``path`` 该节点的定位路径（如 ``nodes[c].childFlow.nodes[ce]``）；
  ``graph`` 当前（子）图上下文（``ids`` / ``host`` / ``ancestors`` / 可达性缓存）。
- 返回 ``str`` → 按该 ``path`` 抛 ``FlowIRValidationError``（首个非 None 即失败，
  与硬编码检查的 first-error-wins 语义一致）；返回 ``None`` → 通过。
- 图级结论（缺 start / 不可达 / 子图无 end）希望以**图路径**而非节点路径定位时，
  规则可自行 ``raise FlowIRValidationError(msg, path=graph.path)``。

内置规则见 ``DEFAULT_RULES``；部署方约定类规则（如 ``forbid_node_types_in_childflow``）
不进默认集，由调用方按需组合。
"""
from __future__ import annotations

import re
from difflib import get_close_matches
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Optional, Tuple


class FlowIRValidationError(ValueError):
    """Flow IR 拓扑校验失败。

    ``path`` 指向出错子图（如 ``nodes[map1].childFlow``），供 AI/工具定位。
    """

    def __init__(self, message: str, *, path: str = "") -> None:
        self.path = path
        self.message = message
        prefix = f"[{path}] " if path else ""
        super().__init__(f"{prefix}{message}")


# 这些节点类型必须有 childFlow（reference 由调度器注入，除外）
_CHILD_FLOW_REQUIRED = frozenset({
    "child", "loop", "while", "map", "filter", "find", "reduce",
})


# ---------------------------------------------------------------------------
# 规则钩子
# ---------------------------------------------------------------------------

class FlowIRGraph:
    """单个（子）图的静态上下文，规则回调第三参。

    ``host`` 是承载本图的节点 dict（根图为 ``None``）；``ancestors`` 是自根到
    直接宿主的宿主链，用于「childflow 子树」类作用域判断。``ids`` 与
    ``reachable_from_start`` 惰性计算并按图缓存（规则逐节点回调，避免 O(n²)）。
    """

    def __init__(
        self,
        *,
        path: str,
        nodes: List[Any],
        host: Optional[Dict[str, Any]],
        ancestors: Tuple[Dict[str, Any], ...],
    ) -> None:
        self.path = path
        self.nodes = nodes
        self.host = host
        self.ancestors = ancestors
        self._cache: Dict[str, Any] = {}

    @property
    def host_type(self) -> Optional[str]:
        t = self.host.get("type") if isinstance(self.host, dict) else None
        return t if isinstance(t, str) else None

    @property
    def ids(self) -> FrozenSet[str]:
        cached = self._cache.get("ids")
        if cached is None:
            cached = frozenset(
                n.get("id") for n in self.nodes
                if isinstance(n, dict) and isinstance(n.get("id"), str) and n.get("id")
            )
            self._cache["ids"] = cached
        return cached

    @property
    def start_node(self) -> Optional[Dict[str, Any]]:
        for n in self.nodes:
            if isinstance(n, dict) and n.get("type") == "start":
                return n
        return None

    @property
    def reachable_from_start(self) -> Optional[FrozenSet[str]]:
        """从（首个）start BFS 可达的 id 集；无 start 时 ``None``。

        边集取运行期 ``Flow.next_node`` 会走的全部超集：next / else_next /
        ``branches[].next`` / ``branches[].name``（Switch/Bool 的 name-as-target
        回退契约）/ case 的 ``cases[].id``·``cases[].next``·``default``。偏保守
        （多算边只会少报不可达），把误报让位给漏报。
        """
        cached = self._cache.get("reachable")
        if cached is None:
            start = self.start_node
            if start is None:
                cached = frozenset()
                self._cache["has_start"] = False
            else:
                self._cache["has_start"] = True
                seen: set = set()
                stack = [start.get("id")]
                while stack:
                    cur = stack.pop()
                    if not isinstance(cur, str) or cur in seen:
                        continue
                    seen.add(cur)
                    node = self._node_by_id(cur)
                    if node is not None:
                        stack.extend(_iter_edge_targets(node))
                cached = frozenset(seen)
            self._cache["reachable"] = cached
        return cached

    @property
    def has_start(self) -> bool:
        self.reachable_from_start  # 触发缓存填充
        return bool(self._cache.get("has_start"))

    def _node_by_id(self, nid: str) -> Optional[Dict[str, Any]]:
        cache = self._cache.setdefault("by_id", {})
        if nid in cache:
            return cache[nid]
        for n in self.nodes:
            if isinstance(n, dict) and n.get("id") == nid:
                cache[nid] = n
                return n
        cache[nid] = None
        return None

    def in_childflow_subtree(self) -> bool:
        """本图是否处于某个 childflow 宿主（child/loop/while/map/...）子树内。

        parallel.branches[].flow 本身不算 childflow 子树，但嵌在 childflow
        子树里的 parallel 分支图算（宿主链线性，逐环检查即可）。
        """
        return any(
            isinstance(a, dict) and a.get("type") in _CHILD_FLOW_REQUIRED
            for a in self.ancestors
        )


def _iter_edge_targets(node: Dict[str, Any]) -> Iterable[str]:
    """收集节点声明的全部后继 id（含 name-as-target 回退），用于可达性 BFS。"""
    for key in ("next", "else_next", "elseNext", "default"):
        t = node.get(key)
        if isinstance(t, str):
            yield t
    for b in node.get("branches") or []:
        if not isinstance(b, dict):
            continue
        t = b.get("next")
        if isinstance(t, str):
            yield t
        name = b.get("name")
        if isinstance(name, str):
            yield name
    for c in node.get("cases") or []:
        if not isinstance(c, dict):
            continue
        t = c.get("id") or c.get("next")
        if isinstance(t, str):
            yield t


def _node_path(graph_data_path: str, node: Any, index: int) -> str:
    """规则回调的节点级定位路径：``nodes[a]`` / ``nodes[c].childFlow.nodes[ce]``。"""
    nid = node.get("id") if isinstance(node, dict) else None
    label = nid if isinstance(nid, str) and nid else str(index)
    return f"{graph_data_path}.nodes[{label}]" if graph_data_path else f"nodes[{label}]"


def _fmt_close_matches(bad: str, candidates: Iterable[str], limit: int = 3) -> str:
    hits = get_close_matches(bad, list(candidates), n=limit, cutoff=0.6)
    return "，你是否想引用 %s？" % " 或 ".join(repr(h) for h in hits) if hits else ""


# --- 内置规则 ---------------------------------------------------------------

def check_flow_entry_and_reachability(node, path, graph) -> Optional[str]:
    """R1：图必须存在 ``type=start``，且从 start 可达所有节点。

    运行期复核：缺 start 抛 ``FlowStartMissingError``（builder/JSON 作者原在
    解析期/运行期各处爆）；不可达节点运行期**静默跳过**（实证 run 正常返回、
    无人执行）。codeflow 前端自动补 start 不受影响。
    """
    if not graph.nodes:
        return None
    verdict = graph._cache.get("entry_verdict")
    if verdict is None:
        if not graph.has_start:
            verdict = (
                f"流程缺少 type=start 入口节点（共 {len(graph.nodes)} 个节点）。"
                "运行期这将抛 FlowStartMissingError——请添加 start 节点"
                "（FlowBuilder 用 start_with，@flow 前端自动补）。"
            )
        else:
            reached = graph.reachable_from_start
            unreachable = sorted(graph.ids - reached)
            if unreachable:
                verdict = (
                    "以下节点从 start 不可达，运行期会被静默跳过（结果无声丢失）: "
                    f"{unreachable}。请把它们接入主流程或删除。"
                )
            else:
                verdict = ""
        graph._cache["entry_verdict"] = verdict
    if verdict:
        raise FlowIRValidationError(verdict, path=graph.path)
    return None


def check_childflow_reachable_end(node, path, graph) -> Optional[str]:
    """R3：子图必须存在从 start 可达的 end 节点（只查子图，不查根图）。

    运行期复核：child/loop/parallel 分支等子流程尾节点缺 next 且非 End 时，
    默认抛 ``FlowExecutionException``（strategies._handle_missing_next）；
    codeflow 前端靠块尾强制 return 已保证，sexpr/builder/JSON 不保证。
    start 缺失时本规则让位给 R1，避免同图双报。
    """
    if graph.host is None or not graph.nodes:
        return None
    verdict = graph._cache.get("end_verdict")
    if verdict is None:
        if not graph.has_start:
            verdict = ""  # R1 已报，不重复
        else:
            reached = graph.reachable_from_start
            host_label = graph.host.get("id") if graph.host else "?"
            end_reached = any(
                isinstance(n, dict) and n.get("type") == "end" and n.get("id") in reached
                for n in graph.nodes
            )
            verdict = (
                f"子流程（宿主节点 {host_label!r}）从 start 可达的节点中没有 end 节点，"
                "子流程无法正常收尾，运行期将抛 FlowExecutionException"
                "（'has no next and is not an End node'）。"
                "请给子图补一条到 end 节点的路径。"
            ) if not end_reached else ""
        graph._cache["end_verdict"] = verdict
    if verdict:
        raise FlowIRValidationError(verdict, path=graph.path)
    return None


def check_parallel_join_branches(node, path, graph) -> Optional[str]:
    """R2：parallel 的 joinBranches 必须 ⊆ branches[].name。

    运行期复核：``Parallel._split_branches`` 用 ``b.name in self.join_branches``
    过滤（concurrent.py），拼错的分支名静默落入 fire-and-forget 后台分支，
    结果无声丢失（实证 join 结果为 ``{}``）。
    """
    if node.get("type") != "parallel":
        return None
    joins = node.get("joinBranches") or node.get("join_branches")
    if not joins or not isinstance(joins, (list, tuple)):
        return None
    branches = [b for b in node.get("branches") or [] if isinstance(b, dict)]
    names = [b.get("name") for b in branches if isinstance(b.get("name"), str)]
    known = set(names)
    bad = [j for j in joins if isinstance(j, str) and j not in known]
    if not bad:
        return None
    hints = "；".join(
        f"{j!r}{_fmt_close_matches(j, names)}" for j in bad
    )
    return (
        f"joinBranches 引用了不存在的分支名: {hints}。"
        "运行期这些分支会静默降级为后台分支（fire-and-forget），join 结果无声丢失。"
        f"已知分支名: {names}。"
    )


# $NODE/$F 引用扫描：负向后顾排除 ``$PARENT.$NODE.x`` 与 ``$NODEX.`` 这类
# 非根引用/相似前缀；id 段只取标识符字符（``$NODE.$INPUT.k`` 动态 id 不匹配
# ——静态判不了，属诚实边界，不算误报）。
_NODE_REF_RE = re.compile(r"(?<![A-Za-z0-9_.$])\$NODE\.([A-Za-z0-9_\-]+)")
_FUNC_REF_RE = re.compile(r"(?<![A-Za-z0-9_.$])\$F\.([A-Za-z0-9_]+)\s*\(")

# 不参与引用扫描的键：
# - ``code``：CODE 节点源码是 Python/JS，不是 plaita 表达式；
# - ``childFlow``/``child_flow``/``flow``：嵌套子图是独立作用域，随递归按
#   各自 graph.ids 单独校验——留在父节点扫描里会把子图引用错记到父图头上；
# - ``desc``/``name`` 等文档/结构字段：散文里的 "$NODE.foo" 不是引用。
_UNSCANNED_KEYS = frozenset({
    "code", "desc", "description", "name", "title", "label", "node_name",
    "id", "type", "next", "else_next", "elseNext", "default", "flow_id",
    "flowId", "source_line", "language",
    "childFlow", "child_flow", "flow",
})


def _iter_strings(obj: Any, *, skip_keys: FrozenSet[str]) -> Iterable[str]:
    """递归产出语义字符串（跳过 skip_keys 键下的子树）。"""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k in skip_keys:
                continue
            yield from _iter_strings(v, skip_keys=skip_keys)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            yield from _iter_strings(v, skip_keys=skip_keys)


def _collect_refs(node: Dict[str, Any], regex) -> List[str]:
    refs: List[str] = []
    for s in _iter_strings(node, skip_keys=_UNSCANNED_KEYS):
        refs.extend(regex.findall(s))
    return refs


def check_node_refs(node, path, graph) -> Optional[str]:
    """R5：``$NODE.<id>`` 引用的节点必须存在于本（子）图。

    运行期复核：未命中静默求值为 ``None``（下游拿到空值继续跑，典型沉默损坏）。
    $NODE 按执行作用域解析——子图内引用父图节点 id 同样为 None（实证），
    故按 graph.ids 差集判定；difflib 给近似 id 提示。
    CODE 节点 code 体与 desc/name 等文档字段不扫（见 _UNSCANNED_KEYS）。
    """
    refs = _collect_refs(node, _NODE_REF_RE)
    if not refs:
        return None
    known = graph.ids
    unknown = sorted({r for r in refs if r not in known})
    if not unknown:
        return None
    hints = "；".join(
        f"$NODE.{r}{_fmt_close_matches(r, known)}" for r in unknown
    )
    return (
        f"引用了本图不存在的节点 id: {hints}。"
        "运行期 $NODE.<未知id> 静默求值为 None，下游会拿到空值继续执行。"
        "（注意 $NODE 按子流程作用域解析，父图节点 id 在子图内同样不可见。）"
    )


def _default_function_names() -> FrozenSet[str]:
    """默认表达式注册表的函数名集（惰性导入，避免模块加载环）。"""
    from plaita.core.expression import get_default_expression_registry

    return frozenset(get_default_expression_registry().names())


def check_expression_functions(node, path, graph) -> Optional[str]:
    """R6：``$F.<fn>(...)`` 调用的函数必须已注册（默认表达式注册表）。

    运行期复核：未注册函数**不报错**，调用求值为字符串 ``'undefined'``
    （实证），下游拿到伪值继续执行。scoped registry 场景用
    ``unknown_expression_functions(registry)`` 替换本规则（见 build_flow 文档）。
    """
    return _expression_function_rule(node, graph, None)


def unknown_expression_functions(registry: Any = None) -> "FlowIRRule":
    """R6 工厂：用指定 registry（``ExpressionRegistry`` 或 dict 代理）校验 $F 引用。

    ``registry=None`` 等价默认注册表。替换默认规则的组合方式::

        rules = [r for r in DEFAULT_RULES if r is not check_expression_functions]
        rules.append(unknown_expression_functions(my_registry))
    """

    def _rule(node, path, graph) -> Optional[str]:
        return _expression_function_rule(node, graph, registry)

    return _rule


def _expression_function_rule(node, graph, registry: Any) -> Optional[str]:
    refs = _collect_refs(node, _FUNC_REF_RE)
    if not refs:
        return None
    names = (
        frozenset(registry.names())
        if hasattr(registry, "names")
        else frozenset(registry)
        if registry is not None
        else _default_function_names()
    )
    unknown = sorted({r for r in refs if r not in names})
    if not unknown:
        return None
    hints = "；".join(
        f"$F.{r}{_fmt_close_matches(r, names)}" for r in unknown
    )
    return (
        f"调用了未注册的表达式函数: {hints}。"
        "运行期 $F.<未注册函数>(...) 求值为字符串 'undefined'，下游会拿到伪值继续执行。"
        "请检查函数名拼写，或先在表达式注册表注册（scoped registry 请用 "
        "unknown_expression_functions(registry) 替换默认规则）。"
    )


def _is_static_value(v: Any) -> bool:
    """True 表示该 IR 值在编译期即最终值（不含表达式/模板标记）。

    字符串含 ``$``（表达式）或 ``{%``（模板插值）即视为动态；容器递归要求
    全部叶子静态。这是 outputType 预检的诚实边界：表达式 output 运行期才
    求值，静态判不了。
    """
    if isinstance(v, str):
        return "$" not in v and "{%" not in v
    if isinstance(v, bool) or v is None or isinstance(v, (int, float)):
        return True
    if isinstance(v, dict):
        return all(_is_static_value(x) for x in v.values())
    if isinstance(v, (list, tuple)):
        return all(_is_static_value(x) for x in v)
    return False


def check_output_type_literal(node, path, graph) -> Optional[str]:
    """R7：output 为字面量且声明了 outputType 时，用 io.match 预检。

    运行期复核：``Assignment.execute`` 对求值结果做 ``match``，不符抛
    ValueError——字面量 output 的求值结果就是字面量本身，可前移到编译期。
    表达式/模板 output 不检（运行期才求值）。
    """
    declared = node.get("outputType") or node.get("output_type")
    if not declared or "output" not in node:
        return None
    out = node.get("output")
    if out is None or not _is_static_value(out):
        return None
    try:
        from plaita.io import Property, match

        prop = declared if isinstance(declared, Property) else Property.model_validate(declared)
        ok = match(prop, out)
    except Exception:  # noqa: BLE001 - outputType 本身非法时交还节点 model_validate 报
        import logging

        logging.getLogger(__name__).debug(
            "outputType precheck skipped (invalid Property or match failure)",
            exc_info=True,
        )
        return None
    if ok:
        return None
    return (
        f"output 字面量 {out!r} 不符合声明的 outputType"
        f"（data_type={getattr(prop, 'data_type', '?')!r}）——运行期节点执行将抛"
        " ValueError，这里提前到编译期。"
    )


# 规则类型： ``(node, path, graph) -> str | None``
FlowIRRule = Callable[[Dict[str, Any], str, "FlowIRGraph"], Optional[str]]

# 默认规则集：结构性真理（运行期必炸或静默坏），所有前端构建路径生效。
# 部署方约定类规则（forbid_node_types_in_childflow）不在其中，按需组合。
DEFAULT_RULES: Tuple[FlowIRRule, ...] = (
    check_flow_entry_and_reachability,
    check_childflow_reachable_end,
    check_parallel_join_branches,
    check_node_refs,
    check_expression_functions,
    check_output_type_literal,
)


def forbid_node_types_in_childflow(
    forbidden: Iterable[str],
    *,
    message: Optional[str] = None,
) -> FlowIRRule:
    """R4 工厂：禁止 childflow 子树内出现指定类型的节点。

    机制化「childflow 子树禁含 F 节点」这类部署方约定（如 recursive 门禁
    修复环曾在运行期首炸）。默认**不启用**——这是约定不是引擎普适真理；
    部署方一行接入::

        validate_flow_ir(data, rules=[*DEFAULT_RULES,
                                      forbid_node_types_in_childflow({"F"})])

    ``parallel.branches[].flow`` 本身不算 childflow 子树，但嵌在 childflow
    子树里的 parallel 分支图受约束（按宿主链判定，见 ``FlowIRGraph``）。
    """
    forbidden_set = frozenset(forbidden)

    def _rule(node, path, graph) -> Optional[str]:
        if not graph.in_childflow_subtree():
            return None
        ntype = node.get("type")
        if ntype not in forbidden_set:
            return None
        chain = " > ".join(
            f"{a.get('type')}({a.get('id')})" for a in graph.ancestors if isinstance(a, dict)
        ) or "<root>"
        return (
            message
            or (
                f"节点类型 {ntype!r} 不允许出现在 childflow 子树内"
                f"（宿主链: {chain}）。这是部署方约定"
                "（forbid_node_types_in_childflow），请改用其他节点类型或将该逻辑上提。"
            )
        )

    return _rule


# ---------------------------------------------------------------------------
# 校验入口
# ---------------------------------------------------------------------------

def validate_flow_ir(
    data: Dict[str, Any],
    *,
    recursive: bool = True,
    path: str = "",
    rules: Optional[Iterable[FlowIRRule]] = None,
) -> None:
    """对 Flow IR dict 做构建期拓扑校验。失败抛 ``FlowIRValidationError``。

    ``rules=None`` 用 ``DEFAULT_RULES``；传空元组/列表可只跑硬编码检查；
    传自定义列表完全替换。规则随递归作用于每个（子）图。
    """
    _validate_ir(
        data, recursive=recursive, path=path,
        rules=rules, host=None, ancestors=(),
    )


def _validate_ir(
    data: Dict[str, Any],
    *,
    recursive: bool,
    path: str,
    rules: Optional[Iterable[FlowIRRule]],
    host: Optional[Dict[str, Any]],
    ancestors: Tuple[Dict[str, Any], ...],
) -> None:
    if not isinstance(data, dict):
        raise FlowIRValidationError(
            f"Flow IR 必须是 dict，得到 {type(data).__name__}",
            path=path or "<root>",
        )

    nodes = data.get("nodes") or []
    if not isinstance(nodes, list):
        raise FlowIRValidationError("nodes 必须是 list", path=path or "nodes")

    active_rules = tuple(DEFAULT_RULES) if rules is None else tuple(rules)

    _validate_nodes(
        nodes, path=path, rules=active_rules,
        host=host, ancestors=ancestors,
    )

    if recursive:
        for n in nodes:
            if not isinstance(n, dict):
                continue
            nid = n.get("id") or "?"
            ntype = n.get("type")
            child = n.get("childFlow") or n.get("child_flow")
            if isinstance(child, dict):
                child_path = f"{path + '.' if path else ''}nodes[{nid}].childFlow"
                _validate_ir(
                    child, recursive=True, path=child_path,
                    rules=active_rules, host=n, ancestors=(*ancestors, n),
                )
            if ntype == "parallel":
                for i, branch in enumerate(n.get("branches") or []):
                    if not isinstance(branch, dict):
                        continue
                    flow = branch.get("flow")
                    if isinstance(flow, dict):
                        bpath = (
                            f"{path + '.' if path else ''}nodes[{nid}].branches[{i}].flow"
                        )
                        _validate_ir(
                            flow, recursive=True, path=bpath,
                            rules=active_rules, host=n, ancestors=(*ancestors, n),
                        )


def _validate_nodes(
    nodes: List[Any],
    *,
    path: str,
    rules: Tuple[FlowIRRule, ...],
    host: Optional[Dict[str, Any]],
    ancestors: Tuple[Dict[str, Any], ...],
) -> None:
    graph_path = path or "nodes"  # 图级错误（id 重复/悬空目标/缺 start）锚点
    ids = [n.get("id") for n in nodes if isinstance(n, dict) and n.get("id")]
    seen: Dict[str, int] = {}
    dupes: List[str] = []
    for nid in ids:
        seen[nid] = seen.get(nid, 0) + 1
        if seen[nid] == 2:
            dupes.append(nid)
    if dupes:
        raise FlowIRValidationError(f"节点 id 重复: {dupes}", path=graph_path)

    id_set = set(ids)

    def _check_target(target: Optional[str], owner: str, field: str) -> None:
        if target is None:
            return
        if target not in id_set:
            raise FlowIRValidationError(
                f"节点 {owner!r} 的 {field} 指向不存在的节点 id {target!r}",
                path=graph_path,
            )

    graph = FlowIRGraph(path=graph_path, nodes=nodes, host=host, ancestors=ancestors)

    for idx, n in enumerate(nodes):
        if not isinstance(n, dict):
            continue
        nid = n.get("id")
        ntype = n.get("type")
        # 子流程宿主节点必须有 childFlow（R5-2 差分评审 P1-2：filter 缺
        # childFlow 在共享校验器里完全静默，各前端暴露时机/类型各异）。
        # reference 除外——它由调度器按 flow_id 注入。
        if ntype in _CHILD_FLOW_REQUIRED and not (
            n.get("childFlow") or n.get("child_flow")
        ):
            raise FlowIRValidationError(
                f"节点 {nid!r}（{ntype}）缺少 childFlow/child_flow",
                path=path,
            )
        if ntype != "end":
            # end 节点无后继语义，结构性检查跳过；规则钩子（$NODE 引用 /
            # outputType 预检）对 end 照常生效——end.output 恰是 $NODE 引用重灾区。
            _check_target(n.get("next"), nid, "next")

        if ntype == "if":
            if n.get("next") is None:
                raise FlowIRValidationError(
                    f"if 节点 {nid!r} 缺少真分支目标（next/then）",
                    path=path,
                )
            if n.get("else_next") is None:
                raise FlowIRValidationError(
                    f"if 节点 {nid!r} 缺少假分支目标（else_next/else_）",
                    path=path,
                )
            _check_target(n.get("next"), nid, "next")
            _check_target(n.get("else_next"), nid, "else_next")
        elif ntype == "switch":
            has_default = False
            for b in n.get("branches") or []:
                if not isinstance(b, dict):
                    continue
                _check_target(b.get("next"), nid, "branches[].next")
                if b.get("isDefault"):
                    has_default = True
            if not has_default:
                raise FlowIRValidationError(
                    f"switch 节点 {nid!r} 缺少 isDefault 分支，"
                    "全部条件不命中时行为未定义",
                    path=path,
                )
        elif ntype == "case":
            for c in n.get("cases") or []:
                if not isinstance(c, dict):
                    continue
                _check_target(c.get("id") or c.get("next"), nid, "cases[].target")
            _check_target(n.get("default"), nid, "default")
        elif ntype == "parallel":
            for b in n.get("branches") or []:
                if not isinstance(b, dict):
                    continue
                _check_target(b.get("next"), nid, "branches[].next")

        # 规则钩子：首个非 None 返回即失败（与硬编码检查同语义）；
        # 图级规则（R1/R3）在规则内部自行以图路径 raise。
        if rules:
            node_path = _node_path(path, n, idx)
            for rule in rules:
                verdict = rule(n, node_path, graph)
                if verdict:
                    raise FlowIRValidationError(verdict, path=node_path)


def build_flow(
    data: Dict[str, Any],
    *,
    recursive: bool = True,
    rules: Optional[Iterable[FlowIRRule]] = None,
) -> "Flow":
    """``validate_flow_ir`` → ``Flow.model_validate`` 单入口。

    ``rules`` 透传 :func:`validate_flow_ir`（None = ``DEFAULT_RULES``）。
    """
    from plaita.core.flow import Flow

    validate_flow_ir(data, recursive=recursive, rules=rules)
    return Flow.model_validate(data)
