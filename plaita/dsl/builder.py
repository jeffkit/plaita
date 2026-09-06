"""
plaita.dsl.builder — FlowBuilder 与节点工厂实现。

每个节点工厂返回一个 ``NodeSpec``（带 ``type`` 的 dict + 可选的待解析
child builder）。``FlowBuilder.add`` 收集这些 spec，``build()`` 时：

1. 把 child builder 递归展开成 dict；
2. 做构建期校验（id 唯一、next 指向存在、switch 有默认分支…）；
3. 调 ``Flow.model_validate`` 产出真正的 ``Flow``。

字段名沿用 JSON 里的驼峰形式（``next``/``else_next``/``resultType``/
``inputType``…），与 ``references/nodes.md`` 文档一致，降低心智负担。
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Union

from plaita.core.flow import Flow

# ---------------------------------------------------------------------------
# 运算符别名：允许用熟悉的符号代替 operator 字符串
# ---------------------------------------------------------------------------

_OP_ALIASES: Dict[str, str] = {
    "==": "eq",
    "!=": "ne",
    ">": "gt",
    ">=": "gte",
    "<": "lt",
    "<=": "lte",
    "in": "in",
    "not in": "notIn",
    "contains": "contains",
    "not contains": "notContains",
}


def _normalize_op(op: str) -> str:
    return _OP_ALIASES.get(op, op)


# ---------------------------------------------------------------------------
# 条件 / 错误处理 helper
# ---------------------------------------------------------------------------

def cond(
    field: Any,
    operator: str,
    value: Any,
) -> Dict[str, Any]:
    """单个分支条件，等价于 ``{ field, operator, value }``。

    ``operator`` 支持符号写法：``>=`` / ``==`` / ``!=`` / ``in`` …
    也会被规范化成 ``gte`` / ``eq`` / ``ne`` / ``in``。
    """
    return {"field": field, "operator": _normalize_op(operator), "value": value}


def cond_group(
    relation: str,
    conditions: List[Any],
) -> Dict[str, Any]:
    """条件组，``relation`` 为 ``and``/``or``，``conditions`` 元素可为
    ``cond(...)`` 或嵌套的 ``cond_group(...)``。"""
    if relation not in ("and", "or"):
        raise ValueError(f"relation 必须是 'and' 或 'or'，得到 {relation!r}")
    return {"relation": relation, "conditions": list(conditions)}


def error_handler(
    strategy: str = "abort",
    retry_times: Optional[int] = None,
    default_value: Any = None,
    error_code: Optional[int] = None,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """构造 ``errorHandler``。``strategy`` ∈ abort/continue/continue_with。"""
    if strategy not in ("abort", "continue", "continue_with", "continue-with"):
        # 连字符 "continue-with" 是 ErrorStrategy 的规范枚举值 (core 层两种拼写
        # 都收并归一化); DSL 层历史上只收下划线, 用户从 enum 取 .value 填进来会被拒。
        strategy = "continue_with" if strategy == "continue-with" else strategy
        raise ValueError(f"unknown error handler strategy: {strategy!r}")
    spec: Dict[str, Any] = {"strategy": strategy}
    if retry_times is not None:
        spec["retryTimes"] = retry_times
    if default_value is not None:
        spec["defaultValue"] = default_value
    if error_code is not None:
        spec["errorCode"] = error_code
    if error_message is not None:
        spec["errorMessage"] = error_message
    return spec


# ---------------------------------------------------------------------------
# NodeSpec：节点工厂的产物
# ---------------------------------------------------------------------------

class NodeSpec(dict):
    """一个节点的原始描述。``FlowBuilder`` 会把它收进 ``nodes`` 列表。

    子流程类节点（child/loop/map/…）的 ``childFlow`` 可以直接传一个
    ``FlowBuilder``，``build()`` 时会自动展开成 dict。
    """

    __slots__ = ()


def _node(node_type: str, **fields: Any) -> NodeSpec:
    spec = NodeSpec()
    spec["type"] = node_type
    spec.update(fields)
    return spec


# --- 节点工厂 -------------------------------------------------------------

def start(id: str = "start", next: Optional[str] = None, **extra: Any) -> NodeSpec:
    """流程起点。"""
    return _node("start", id=id, next=next, **extra)


def end(
    id: str = "end",
    output: Any = None,
    result_type: str = "success",
    error: Optional[Dict[str, Any]] = None,
    **extra: Any,
) -> NodeSpec:
    """流程终点。

    ``result_type`` ∈ success/nop/error；``error`` 仅在 error 时使用，
    形如 ``{"code": 4001, "message": "参数非法"}``。
    """
    spec = _node("end", id=id, output=output, resultType=result_type, **extra)
    if error is not None:
        spec["error"] = error
    return spec


def assignment(
    id: str,
    output: Any,
    next: Optional[str] = None,
    output_type: Optional[Dict[str, Any]] = None,
    **extra: Any,
) -> NodeSpec:
    """求值节点，求值 ``output`` 作为节点结果。"""
    spec = _node("assignment", id=id, output=output, next=next, **extra)
    if output_type is not None:
        spec["outputType"] = output_type
    return spec


def if_(
    id: str,
    condition: Any,
    next: Optional[str] = None,
    else_next: Optional[str] = None,
    *,
    then: Optional[str] = None,
    else_: Optional[str] = None,
    **extra: Any,
) -> NodeSpec:
    """二分支：真走 ``next``（别名 ``then``），假走 ``else_next``（别名 ``else_``）。

    ``condition`` 为 ``cond(...)`` / ``cond_group(...)`` 或原始 dict。
    ``then`` / ``else_`` 是为隐式 next 的 ``linear`` 写法准备的语义化别名。
    """
    next = next if next is not None else then
    else_next = else_next if else_next is not None else else_
    return _node("if", id=id, condition=condition, next=next, else_next=else_next, **extra)


def branch(
    name: str,
    next: str,
    condition: Any = None,
    priority: int = 0,
    is_default: bool = False,
) -> Dict[str, Any]:
    """``switch`` 的一条分支。"""
    spec: Dict[str, Any] = {
        "name": name,
        "next": next,
        "priority": priority,
    }
    if condition is not None:
        spec["condition"] = condition
    if is_default:
        spec["isDefault"] = True
    return spec


def switch(
    id: str,
    branches: List[Dict[str, Any]],
    **extra: Any,
) -> NodeSpec:
    """多路条件跳转。``branches`` 用 ``branch(...)`` 构造，至少一条
    ``is_default=True``。"""
    return _node("switch", id=id, branches=list(branches), **extra)


def case(
    id: str,
    target: Any,
    cases: List[Dict[str, Any]],
    default: Optional[str] = None,
    **extra: Any,
) -> NodeSpec:
    """等值匹配。

    ``cases`` 元素形如 ``{"name": ..., "value": ..., "next": ...}``。
    注意运行时实际用每条 case 的 ``id`` 作为跳转目标，这里做了归一化：
    若未显式给 ``id``，则用 ``next`` 兜底，让你按直觉写 ``next`` 即可。
    """
    normalized = []
    for c in cases:
        c = dict(c)
        if not c.get("id"):
            target_next = c.get("next")
            if not target_next:
                raise ValueError("case 每条分支需要 next（或 id）作为跳转目标")
            c["id"] = target_next
        normalized.append(c)
    spec = _node("case", id=id, target=target, cases=normalized, **extra)
    if default is not None:
        spec["default"] = default
    return spec


def _collection_node(
    node_type: str,
    id: str,
    collection: Any,
    child_flow: Union["FlowBuilder", Dict[str, Any]],
    next: Optional[str] = None,
    **extra: Any,
) -> NodeSpec:
    if child_flow is None:
        raise ValueError(f"{node_type} 节点 {id!r} 必须提供 child_flow")
    return _node(
        node_type,
        id=id,
        collection=collection,
        childFlow=child_flow,
        next=next,
        **extra,
    )


def loop(
    id: str,
    collection: Any,
    child_flow: Union["FlowBuilder", Dict[str, Any]],
    next: Optional[str] = None,
    condition: Any = None,
    **extra: Any,
) -> NodeSpec:
    """遍历集合执行子流程，``condition`` 不满足时中止。"""
    spec = _collection_node("loop", id, collection, child_flow, next=next, **extra)
    if condition is not None:
        spec["condition"] = condition
    return spec


def map(
    id: str,
    collection: Any,
    child_flow: Union["FlowBuilder", Dict[str, Any]],
    next: Optional[str] = None,
    concurrent: bool = False,
    max_concurrent: Optional[int] = None,
    **extra: Any,
) -> NodeSpec:
    """映射集合，``concurrent=True`` 并发执行。"""
    spec = _collection_node("map", id, collection, child_flow, next=next, **extra)
    if concurrent:
        spec["concurrent"] = True
        if max_concurrent is not None:
            spec["maxConcurrent"] = max_concurrent
    return spec


def filter(
    id: str,
    collection: Any,
    child_flow: Union["FlowBuilder", Dict[str, Any]],
    next: Optional[str] = None,
    **extra: Any,
) -> NodeSpec:
    """按子流程返回 bool 过滤集合。"""
    return _collection_node("filter", id, collection, child_flow, next=next, **extra)


def find(
    id: str,
    collection: Any,
    child_flow: Union["FlowBuilder", Dict[str, Any]],
    next: Optional[str] = None,
    **extra: Any,
) -> NodeSpec:
    """返回首个子流程返回真的元素。"""
    return _collection_node("find", id, collection, child_flow, next=next, **extra)


def reduce(
    id: str,
    collection: Any,
    child_flow: Union["FlowBuilder", Dict[str, Any]],
    next: Optional[str] = None,
    initial: Any = None,
    **extra: Any,
) -> NodeSpec:
    """归约集合。"""
    spec = _collection_node("reduce", id, collection, child_flow, next=next, **extra)
    if initial is not None:
        spec["initial"] = initial
    return spec


def child(
    id: str,
    input: Any,
    child_flow: Union["FlowBuilder", Dict[str, Any]],
    next: Optional[str] = None,
    **extra: Any,
) -> NodeSpec:
    """内联子流程，可共享父上下文。"""
    return _node("child", id=id, input=input, childFlow=child_flow, next=next, **extra)


def reference(
    id: str,
    input: Any,
    child_flow: Union["FlowBuilder", Dict[str, Any]],
    next: Optional[str] = None,
    **extra: Any,
) -> NodeSpec:
    """引用子流程，不共享父上下文。"""
    return _node("reference", id=id, input=input, childFlow=child_flow, next=next, **extra)


def parallel_branch(
    name: str,
    flow: Union["FlowBuilder", Dict[str, Any]],
    input: Any = None,
    condition: Any = None,
) -> Dict[str, Any]:
    """``parallel`` 的一条分支。"""
    spec: Dict[str, Any] = {"name": name, "flow": flow}
    if input is not None:
        spec["input"] = input
    if condition is not None:
        spec["condition"] = condition
    return spec


def parallel(
    id: str,
    branches: List[Dict[str, Any]],
    next: Optional[str] = None,
    mode: str = "thread",
    join_branches: Optional[List[str]] = None,
    is_conditional: bool = False,
    **extra: Any,
) -> NodeSpec:
    """多分支并行。``mode`` ∈ thread/process/coroutine。"""
    spec = _node(
        "parallel",
        id=id,
        branches=list(branches),
        mode=mode,
        next=next,
        **extra,
    )
    if join_branches is not None:
        spec["joinBranches"] = list(join_branches)
    if is_conditional:
        spec["isConditional"] = True
    return spec


def code(
    id: str,
    language: str,
    code: str,
    input: Any = None,
    next: Optional[str] = None,
    **extra: Any,
) -> NodeSpec:
    """执行用户代码，需 ``code`` extra。``language`` ∈ js/python。"""
    spec = _node("code", id=id, language=language, code=code, next=next, **extra)
    if input is not None:
        spec["input"] = input
    return spec


def http(
    id: str,
    method: str,
    url: str,
    next: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    body: Any = None,
    timeout: Optional[str] = None,
    error_handler: Any = None,
    **extra: Any,
) -> NodeSpec:
    """发 HTTP 请求，需 ``http`` extra。"""
    spec = _node("http", id=id, method=method, url=url, next=next, **extra)
    if headers is not None:
        spec["headers"] = headers
    if body is not None:
        spec["body"] = body
    if timeout is not None:
        spec["timeout"] = timeout
    if error_handler is not None:
        spec["errorHandler"] = error_handler
    return spec


def event(
    id: str,
    event_type: str,
    event_filter: Optional[Dict[str, Any]] = None,
    **extra: Any,
) -> NodeSpec:
    """事件节点，用于断点续执。"""
    spec = _node("event", id=id, eventType=event_type, **extra)
    if event_filter is not None:
        spec["eventFilter"] = event_filter
    return spec


# ---------------------------------------------------------------------------
# FlowBuilder
# ---------------------------------------------------------------------------

def _input_type_spec(value: Any) -> Optional[Dict[str, Any]]:
    """把简化的 input_type 入参规范成 ``{ dataType: ... }``。"""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    # 简写：传 "object" / "array" / "string" 等字符串
    return {"dataType": value}


class FlowBuilder:
    """链式累积节点，最终产出 ``Flow`` / JSON / YAML。

    既可以由 ``build(...)`` 创建顶层 flow，也可以由 ``child_flow`` 装饰器
    创建子流程。``.add()`` 返回 self，便于链式调用。
    """

    def __init__(
        self,
        flow_id: Optional[str] = None,
        input_type: Any = None,
        output_type: Any = None,
        desc: Optional[str] = None,
        version: Optional[str] = None,
        author: Optional[str] = None,
        timeout: Optional[str] = None,
        global_context: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        runtime: str = "python",
    ) -> None:
        self.flow_id = flow_id
        self.input_type = _input_type_spec(input_type)
        self.output_type = _input_type_spec(output_type)
        self.desc = desc
        self.version = version
        self.author = author
        self.timeout = timeout
        self.global_context = dict(global_context) if global_context else None
        self.metadata = dict(metadata) if metadata else None
        self.runtime = runtime
        self._nodes: List[NodeSpec] = []

    # -- 收集 / 修改 / 删除节点 -----------------------------------------

    def add(self, node: NodeSpec) -> "FlowBuilder":
        """追加一个节点，返回 self。"""
        if not isinstance(node, NodeSpec) and not isinstance(node, dict):
            raise TypeError(f"add() 只接受节点工厂的产物，得到 {type(node).__name__}")
        self._nodes.append(NodeSpec(node))  # type: ignore[arg-type]
        return self

    def remove_node(self, node_id: str) -> "FlowBuilder":
        """按 id 删除节点。若 id 不存在则抛 KeyError。"""
        before = len(self._nodes)
        self._nodes = [n for n in self._nodes if n.get("id") != node_id]
        if len(self._nodes) == before:
            raise KeyError(f"节点 id {node_id!r} 不存在")
        return self

    def update_node(self, node_id: str, **fields: Any) -> "FlowBuilder":
        """修改已有节点的字段。若 id 不存在则抛 KeyError。

        示例：
            builder.update_node("greet", output="$F.concat('Hi, ', $INPUT.name)")
        """
        for node in self._nodes:
            if node.get("id") == node_id:
                node.update(fields)
                return self
        raise KeyError(f"节点 id {node_id!r} 不存在")

    def reroute(
        self,
        node_id: str,
        *,
        next: Optional[str] = None,
        else_next: Optional[str] = None,
    ) -> "FlowBuilder":
        """修改节点的跳转目标。

        - ``next``：普通节点的后继，或 if 节点的真分支目标。
        - ``else_next``：if 节点的假分支目标。

        只传入想改的字段，其余保持不变。
        """
        for node in self._nodes:
            if node.get("id") == node_id:
                if next is not None:
                    node["next"] = next
                if else_next is not None:
                    node["else_next"] = else_next
                return self
        raise KeyError(f"节点 id {node_id!r} 不存在")

    # -- 从外部数据加载 --------------------------------------------------

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FlowBuilder":
        """从 ``Flow.model_dump()`` / JSON 解析出的 dict 加载为 FlowBuilder。

        常用于「加载已有流程，修改部分节点，再 build」：

        ::

            import json
            builder = FlowBuilder.from_dict(json.load(open("my_flow.json")))
            builder.update_node("greet", output="'hello'")
            new_flow = builder.build()

        字段名同时接受 camelCase（手写 JSON 常用 ``inputType``）与 snake_case
        （``Flow.model_dump()`` 产出 ``input_type``）——历史上只读 camelCase,
        roundtrip ``from_dict(flow.model_dump())`` 会静默丢 input/output/global_context。
        """
        def _pick(*keys: str) -> Any:
            for key in keys:
                if data.get(key) is not None:
                    return data[key]
            return None

        builder = cls(
            flow_id=_pick("flow_id", "flowId"),
            input_type=_pick("inputType", "input_type"),
            output_type=_pick("outputType", "output_type"),
            desc=_pick("desc", "description"),
            version=data.get("version"),
            author=data.get("author"),
            timeout=data.get("timeout"),
            global_context=_pick("globalContext", "global_context"),
            metadata=data.get("metadata"),
            runtime=data.get("runtime", "python"),
        )
        for raw_node in data.get("nodes", []):
            builder._nodes.append(NodeSpec(raw_node))
        return builder

    # -- 序列化 ----------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """展开成可被 ``Flow.model_validate`` 接受的 dict。"""
        data: Dict[str, Any] = {"runtime": self.runtime}
        if self.flow_id is not None:
            data["flow_id"] = self.flow_id
        if self.input_type is not None:
            data["inputType"] = self.input_type
        if self.output_type is not None:
            data["outputType"] = self.output_type
        if self.desc:
            data["desc"] = self.desc
        if self.version:
            data["version"] = self.version
        if self.author:
            data["author"] = self.author
        if self.timeout:
            data["timeout"] = self.timeout
        if self.global_context is not None:
            data["globalContext"] = self.global_context
        if self.metadata is not None:
            data["metadata"] = self.metadata
        data["nodes"] = [self._expand_node(n) for n in self._nodes]
        return data

    def _expand_node(self, node: NodeSpec) -> Dict[str, Any]:
        expanded: Dict[str, Any] = {}
        for k, v in node.items():
            expanded[k] = self._expand_value(v)
        return expanded

    def _expand_value(self, value: Any) -> Any:
        if isinstance(value, FlowBuilder):
            return value.to_dict()
        if isinstance(value, list):
            return [self._expand_value(v) for v in value]
        if isinstance(value, dict):
            return {k: self._expand_value(v) for k, v in value.items()}
        return value

    def to_json(self, indent: int = 2) -> str:
        import json

        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    def to_yaml(self) -> str:
        from plaita.io_format import dump_yaml

        return dump_yaml(self.to_dict())

    # -- 构建期校验 + 产出 Flow -----------------------------------------

    def validate(self) -> None:
        """构建期静态校验（委托共享 ``validate_flow_ir``，含递归子流程）。"""
        from plaita.dsl.ir_validate import validate_flow_ir

        validate_flow_ir(self.to_dict(), recursive=True)

    def build(self) -> Flow:
        """做构建期校验并产出可执行的 ``Flow``。"""
        from plaita.dsl.ir_validate import build_flow

        return build_flow(self.to_dict())

    # -- 便捷执行 --------------------------------------------------------

    def run(self, *args, **kwargs):
        return self.build().run(*args, **kwargs)

    async def arun(self, *args, **kwargs):
        return await self.build().arun(*args, **kwargs)

    def __enter__(self) -> "FlowBuilder":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # 配合 ``with build(...) as f:`` 写法，本身不需要清理
        return None


# ---------------------------------------------------------------------------
# 顶层入口与子流程装饰器
# ---------------------------------------------------------------------------

def build(
    flow_id: Optional[str] = None,
    *,
    input_type: Any = None,
    output_type: Any = None,
    desc: Optional[str] = None,
    version: Optional[str] = None,
    author: Optional[str] = None,
    timeout: Optional[str] = None,
    global_context: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> FlowBuilder:
    """创建一个顶层 ``FlowBuilder``。"""
    return FlowBuilder(
        flow_id=flow_id,
        input_type=input_type,
        output_type=output_type,
        desc=desc,
        version=version,
        author=author,
        timeout=timeout,
        global_context=global_context,
        metadata=metadata,
    )


def child_flow(
    input_type: Any = None,
    output_type: Any = None,
    desc: Optional[str] = None,
) -> Callable[[Callable[["FlowBuilder"], None]], "FlowBuilder"]:
    """装饰器：把一个函数体变成子流程 ``FlowBuilder``。

    ::

        @child_flow(input_type="object")
        def double_each(c):
            c.add(start(next="e"))
            c.add(end("e", output="$F.mul($INPUT.item, 2)"))

        f.add(map(id="double_all", collection="$INPUT.numbers",
                  child_flow=double_each, next="end"))
    """

    def decorator(fn: Callable[["FlowBuilder"], None]) -> "FlowBuilder":
        builder = FlowBuilder(input_type=input_type, output_type=output_type, desc=desc)
        fn(builder)
        return builder

    return decorator


# ---------------------------------------------------------------------------
# LinearBuilder：隐式 next，节点按声明顺序自动串接
# ---------------------------------------------------------------------------

# 这些节点类型的 next 是「真分支目标」而非「顺序下一个」，不参与自动串接
_BRANCH_SKIP_AUTO_NEXT = {"if", "switch", "case"}
# 终端节点：没有 next
_TERMINAL_TYPES = {"end", "event"}


class LinearBuilder:
    """隐式 ``next`` 的链式 builder。

    与 ``FlowBuilder`` 的区别：

    - 节点 ``id`` 可省略 —— 未提供时自动生成 ``_n1`` / ``_n2`` …；
      只在需要被分支引用（``then``/``else_``/``cases``）或被 ``$NODE.<id>``
      引用时才显式给出。
    - 非分支节点的 ``next`` 可省略 —— 按声明顺序自动指向下一个节点。
    - ``if_`` 的真分支 ``then`` 省略时也默认指向下一个声明节点（「条件成立则继续」）；
      假分支 ``else_`` 仍需显式给出。

    分支节点（``if_``/``switch``/``case``）的跳转目标仍是显式 label，
    因为它们语义上不是「顺序下一个」。

    ::

        from plaita.dsl import linear, cond

        flow = (
            linear("adult_check", input_type="object", desc="判断成年")
            .start()
            .if_(condition=cond("$INPUT.age", ">=", 18),
                 then="adult", else_="minor")
            .end("adult", output="成年")
            .end("minor", output="未成年")
            .build()
        )
    """

    def __init__(self, **flow_kwargs: Any) -> None:
        self._builder = FlowBuilder(**flow_kwargs)
        self._counter = 0
        self._claimed: set = set()

    # -- id 分配 ---------------------------------------------------------

    def _auto_id(self) -> str:
        while True:
            self._counter += 1
            candidate = f"_n{self._counter}"
            if candidate not in self._claimed:
                self._claimed.add(candidate)
                return candidate

    def _ensure_id(self, node_id: Optional[str]) -> str:
        if node_id:
            if node_id in self._claimed:
                raise ValueError(f"节点 id 重复: {node_id!r}")
            self._claimed.add(node_id)
            return node_id
        return self._auto_id()

    # -- 节点方法（每个返回 self，链式）----------------------------------

    def start(self, id: Optional[str] = None, **extra: Any) -> "LinearBuilder":
        return self._append(start(id=self._ensure_id(id), **extra))

    def end(
        self,
        id: Optional[str] = None,
        output: Any = None,
        result_type: str = "success",
        error: Optional[Dict[str, Any]] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(end(
            id=self._ensure_id(id), output=output,
            result_type=result_type, error=error, **extra,
        ))

    def assignment(
        self,
        output: Any,
        id: Optional[str] = None,
        output_type: Optional[Dict[str, Any]] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(assignment(
            id=self._ensure_id(id), output=output,
            output_type=output_type, **extra,
        ))

    def if_(
        self,
        condition: Any,
        *,
        then: Optional[str] = None,
        else_: Optional[str] = None,
        next: Optional[str] = None,
        else_next: Optional[str] = None,
        id: Optional[str] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(if_(
            id=self._ensure_id(id), condition=condition,
            next=next, else_next=else_next, then=then, else_=else_, **extra,
        ))

    def switch(
        self,
        branches: List[Dict[str, Any]],
        id: Optional[str] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(switch(id=self._ensure_id(id), branches=branches, **extra))

    def case(
        self,
        target: Any,
        cases: List[Dict[str, Any]],
        default: Optional[str] = None,
        id: Optional[str] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(case(
            id=self._ensure_id(id), target=target, cases=cases,
            default=default, **extra,
        ))

    def _collection(
        self,
        factory: Callable[..., NodeSpec],
        collection: Any,
        child_flow: Union[FlowBuilder, Dict[str, Any]],
        id: Optional[str],
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(factory(
            id=self._ensure_id(id), collection=collection,
            child_flow=child_flow, **extra,
        ))

    def loop(
        self,
        collection: Any,
        child_flow: Union[FlowBuilder, Dict[str, Any]],
        id: Optional[str] = None,
        condition: Any = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._collection(loop, collection, child_flow, id,
                                condition=condition, **extra)

    def map(
        self,
        collection: Any,
        child_flow: Union[FlowBuilder, Dict[str, Any]],
        id: Optional[str] = None,
        concurrent: bool = False,
        max_concurrent: Optional[int] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._collection(map, collection, child_flow, id,
                                concurrent=concurrent,
                                max_concurrent=max_concurrent, **extra)

    def filter(
        self,
        collection: Any,
        child_flow: Union[FlowBuilder, Dict[str, Any]],
        id: Optional[str] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._collection(filter, collection, child_flow, id, **extra)

    def find(
        self,
        collection: Any,
        child_flow: Union[FlowBuilder, Dict[str, Any]],
        id: Optional[str] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._collection(find, collection, child_flow, id, **extra)

    def reduce(
        self,
        collection: Any,
        child_flow: Union[FlowBuilder, Dict[str, Any]],
        id: Optional[str] = None,
        initial: Any = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._collection(reduce, collection, child_flow, id,
                                initial=initial, **extra)

    def child(
        self,
        input: Any,
        child_flow: Union[FlowBuilder, Dict[str, Any]],
        id: Optional[str] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(child(
            id=self._ensure_id(id), input=input, child_flow=child_flow, **extra,
        ))

    def reference(
        self,
        input: Any,
        child_flow: Union[FlowBuilder, Dict[str, Any]],
        id: Optional[str] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(reference(
            id=self._ensure_id(id), input=input, child_flow=child_flow, **extra,
        ))

    def parallel(
        self,
        branches: List[Dict[str, Any]],
        id: Optional[str] = None,
        mode: str = "thread",
        join_branches: Optional[List[str]] = None,
        is_conditional: bool = False,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(parallel(
            id=self._ensure_id(id), branches=branches, mode=mode,
            join_branches=join_branches, is_conditional=is_conditional, **extra,
        ))

    def code(
        self,
        language: str,
        code: str,
        id: Optional[str] = None,
        input: Any = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(code(
            id=self._ensure_id(id), language=language, code=code,
            input=input, **extra,
        ))

    def http(
        self,
        method: str,
        url: str,
        id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        body: Any = None,
        timeout: Optional[str] = None,
        error_handler: Any = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(http(
            id=self._ensure_id(id), method=method, url=url, headers=headers,
            body=body, timeout=timeout, error_handler=error_handler, **extra,
        ))

    def event(
        self,
        event_type: str,
        id: Optional[str] = None,
        event_filter: Optional[Dict[str, Any]] = None,
        **extra: Any,
    ) -> "LinearBuilder":
        return self._append(event(
            id=self._ensure_id(id), event_type=event_type,
            event_filter=event_filter, **extra,
        ))

    # -- 通用 add：兼容 FlowBuilder 的节点工厂产物 ----------------------

    def add(self, node: NodeSpec) -> "LinearBuilder":
        """追加一个由节点工厂构造的节点（自动补 id 与 next）。"""
        if not isinstance(node, (NodeSpec, dict)):
            raise TypeError(f"add() 只接受节点工厂的产物，得到 {type(node).__name__}")
        node = NodeSpec(node)
        if not node.get("id"):
            node["id"] = self._auto_id()
        else:
            self._ensure_id(node["id"])
        self._builder._nodes.append(node)
        return self

    def _append(self, node: NodeSpec) -> "LinearBuilder":
        self._builder._nodes.append(node)
        return self

    # -- 自动串接 + 构建 --------------------------------------------------

    def _auto_chain(self) -> None:
        """按声明顺序填充缺失的 ``next``。"""
        nodes = self._builder._nodes
        for i, n in enumerate(nodes):
            ntype = n.get("type")
            if ntype in _TERMINAL_TYPES:
                continue
            if ntype in _BRANCH_SKIP_AUTO_NEXT:
                # if 的真分支（next）若未指定，默认走下一个声明节点
                if ntype == "if" and n.get("next") is None and i + 1 < len(nodes):
                    n["next"] = nodes[i + 1].get("id")
                continue
            if n.get("next") is None and i + 1 < len(nodes):
                n["next"] = nodes[i + 1].get("id")

    def build(self) -> Flow:
        self._auto_chain()
        return self._builder.build()

    def validate(self) -> None:
        self._auto_chain()
        self._builder.validate()

    def to_dict(self) -> Dict[str, Any]:
        self._auto_chain()
        return self._builder.to_dict()

    def to_json(self, indent: int = 2) -> str:
        self._auto_chain()
        return self._builder.to_json(indent=indent)

    def to_yaml(self) -> str:
        self._auto_chain()
        return self._builder.to_yaml()

    def run(self, *args, **kwargs):
        return self.build().run(*args, **kwargs)

    async def arun(self, *args, **kwargs):
        return await self.build().arun(*args, **kwargs)

    def __enter__(self) -> "LinearBuilder":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def linear(
    flow_id: Optional[str] = None,
    *,
    input_type: Any = None,
    output_type: Any = None,
    desc: Optional[str] = None,
    version: Optional[str] = None,
    author: Optional[str] = None,
    timeout: Optional[str] = None,
    global_context: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> LinearBuilder:
    """创建一个隐式 ``next`` 的 ``LinearBuilder``。

    节点按声明顺序自动串接，``id`` 按需给出（被分支/``$NODE`` 引用时），
    非分支节点的 ``next`` 完全省略。等价于 ``build()`` 的「无脑线性版」。
    """
    return LinearBuilder(
        flow_id=flow_id,
        input_type=input_type,
        output_type=output_type,
        desc=desc,
        version=version,
        author=author,
        timeout=timeout,
        global_context=global_context,
        metadata=metadata,
    )
