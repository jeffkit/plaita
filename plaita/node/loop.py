import asyncio
import json
import logging
import re
from typing import Annotated, Any, ClassVar, Dict, List, Optional, Tuple, Union

from pydantic import Field, model_validator

from plaita.core.parallel_executor import (
    ParallelExecutor,
    SequentialExecutor,
    ThreadParallelExecutor,
    in_plaita_pool_thread,
)

from ..io import Property
from .basic import Expression
from .child import InlineFlow
from .decide import Condition, ConditionGroup
from plaita.core.expression_parser import (
    _EngineCopyRefused,
    _engine_copy,
    freshness_scope,
    note_fresh_closure,
)
from plaita.core.state import _LazyRootSnapshot, _snapshot_value

_logger = logging.getLogger(__name__)

#: 每个被条件引用的根键的隔离拷贝（plaita#69 r7）。
#:
#: **兼容别名**：这个名字历史上是 ``copy.deepcopy``（ML1 性能修复引入）。r7 起它
#: 指向**唯一**的数据模型拷贝入口 ``_engine_copy`` —— 绝不再调用宿主的
#: ``__deepcopy__``/``__copy__``（那是 verifier Finding 1 的活对象通道）。保留模块级
#: 名字是为了让 ML1-C 的性能钉（``tests/unit/test_mfix_l_loop_context.py`` 对
#: ``loop_mod.deepcopy`` 打 spy）继续生效：它约束的是"未引用的大负载不被拷贝"，
#: 与实现无关。
deepcopy = _engine_copy


def _coerce_collection(value: Any) -> List[Any]:
    """把 ``collection`` 表达式的求值结果归一成可迭代列表。

    字符串按 JSON 数组字面量解析（字段描述宣称"也可为字面量数组"，而表达式
    引擎不解析数组字面量、原样返回字符串——历史上 ``"[1,2,3]"`` 会被 ``list()``
    拆成 3 个单字符"元素"，子流程对每个字符各跑一遍，结果集完全错误）；不是
    JSON 数组的普通字符串按单元素集合处理，同样不再逐字符迭代。
    """
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, (list, tuple)):
                return list(parsed)
        _logger.warning(
            "collection expression evaluated to a plain string; treating it as a "
            "single-element collection instead of iterating characters: %r",
            value if len(value) < 80 else value[:77] + "...",
        )
        return [value]
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return list(value)


# ---------------------------------------------------------------------------
# 循环条件求值上下文（ML1 热路径修复）
# ---------------------------------------------------------------------------
_UNSET = object()  # 哨兵：「未提供 result」（区别于 result=None 的合法值）

# 历史：Loop/While 每轮 ``deepcopy(execution.context)`` 评条件 —— 生产上下文
# 携带全部上游节点输出（AGENTRUN 大文本），千次迭代 = 千次全量深拷贝（实测
# 500 轮 × ~0.9MB 上下文 ≈ 13.5s，96% 墙钟时间在 deepcopy）。
#
# 现策略：顶层浅拷贝 + 注入 LOOP-* 键；再把「条件文本静态引用到的根键」的值
# 逐个 deepcopy 替换。条件求值对 context 只读（expression_parser 的求值 thunk
# 只做 ``context[root_key]`` 读；历史上的线程栈 _push_frame 已不在热路径），
# 未引用的根共享读零成本。被引用的根保持与全量 deepcopy 等价的写隔离——
# 表达式引擎有 ``$F.set`` / ``$F.pop`` / ``$F.clear`` 等 mutate 函数（见
# core/expression.py，has_side_effects=True），条件引用它们时只能改到副本，
# 不污染原 context（契约钉在 tests/unit/test_loop.py::TestLoopConditionIsolation）。

# prefix -> 编译好的根 token 正则（express_prefix 可配置，按前缀缓存）
_ROOT_TOKEN_PATTERNS: Dict[str, "re.Pattern"] = {}


def _root_token_pattern(prefix: str) -> "re.Pattern":
    """匹配表达式中「根键 token」的正则。

    文法对齐 expression_parser._build_grammar 的 root 规则：
    ``root = prefix + Optional(name_token)``，``name_token = alphanums + "_-$"``。
    ``$F.`` 是函数命名空间不是上下文根，负前瞻排除（``$FLOW``/``$FOO`` 不受影响）。
    """
    pattern = _ROOT_TOKEN_PATTERNS.get(prefix)
    if pattern is None:
        pattern = re.compile(re.escape(prefix) + r"(?!F\.)[\w\-$]*")
        _ROOT_TOKEN_PATTERNS[prefix] = pattern
    return pattern


def _collect_condition_strings(value: Any, out: List[str]) -> None:
    """递归收集条件树里的所有字符串。

    镜像 evaluate 的递归形状：list/tuple 逐元素、dict 只走 value（表达式引擎
    对 dict 也是 ``{key: eval(val)}``，键不参与求值）。非字符串叶子（int/bool/
    None 等）被 evaluate 原样返回，无根引用。
    """
    if isinstance(value, str):
        out.append(value)
    elif isinstance(value, Condition):
        _collect_condition_strings(value.field, out)
        _collect_condition_strings(value.value, out)
    elif isinstance(value, ConditionGroup):
        for cond in value.conditions:
            _collect_condition_strings(cond, out)
    elif isinstance(value, dict):
        for v in value.values():
            _collect_condition_strings(v, out)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _collect_condition_strings(v, out)


def _isolated_root_keys(condition: Any, pfx: str) -> Tuple[str, ...]:
    """静态扫描条件，返回条件求值可能触碰的顶层上下文键。

    安全性关键在「绝不漏扫」：表达式文法里根键只能是字面 ``prefix+name`` token
    （无动态拼根、thunk 只捕获结构），所以任何引用路径都会出现在条件文本里；
    字符串字面量里恰好长得像根 token 会被多拷（无害，只是多做一次深拷贝）。
    """
    strings: List[str] = []
    _collect_condition_strings(condition, strings)
    pattern = _root_token_pattern(pfx)
    keys: List[str] = []
    for s in strings:
        for token in pattern.findall(s):
            if token == f"{pfx}FLOW":  # $FLOW 是 $FLOW_ID 的文法别名
                token = f"{pfx}FLOW_ID"
            if token and token not in keys:
                keys.append(token)
    return tuple(keys)


class _IsolatedConditionContext(dict):
    """循环条件求值用的隔离上下文（plaita#69 的 mutate 门按这个标记放行）。

    它是 live ``CheckpointState`` 的顶层浅拷贝、被引用的根键已由**引擎自有结构
    拷贝**（``_engine_copy``）替换 ⇒ 条件里的就地 mutate 只改副本。标记让
    ``expression_parser`` 的 #69 门**不需要 import loop** 就能区分「隔离副本」与
    「持久化数据容器」。
    """

    _plaita_isolated_copy = True


def _condition_context(
    execution,
    condition: Any,
    *,
    item: Any,
    index: int,
    result: Any = _UNSET,
) -> Dict[str, Any]:
    """构造循环条件求值用的上下文。

    顶层浅拷贝 + 注入 LOOP-ITEM/LOOP-INDEX（Loop 另注入 LOOP-RESULT）；
    随后仅对条件引用到的根键做引擎自有结构拷贝替换（写隔离），其余键与
    execution.context 共享对象（只读）。

    plaita#69 r7：拷贝走**唯一**的数据模型拷贝入口 ``_engine_copy``（r6 起
    ``copy.deepcopy`` 已从数据路径移除——它会调用宿主类的 ``__deepcopy__``，
    是活对象混进 freshness 作用域的通道）。不可拷贝的根（含真正的非数据模型
    宿主对象）**保留活引用且不登记** fresh：就地改写因此被 default-deny 具名
    拒绝（零写穿），只读条件语义不变——fail-closed，比"悄悄共享活对象"安全。
    """
    pfx = execution.express_prefix
    loop_ctx = _IsolatedConditionContext(execution.context)
    loop_ctx[f"{pfx}LOOP-ITEM"] = item
    loop_ctx[f"{pfx}LOOP-INDEX"] = index
    if result is not _UNSET:
        loop_ctx[f"{pfx}LOOP-RESULT"] = result
    for key in _isolated_root_keys(condition, pfx):
        try:
            value = loop_ctx[key]
        except KeyError:
            continue  # 条件引用了上下文中不存在的根：与全量 deepcopy 一样缺席，求值期按原语义报 KeyError
        if isinstance(value, _LazyRootSnapshot):
            # 引擎自己的惰性 $PARENT 视图：先物化为 plain dict 再拷贝。
            value = value.materialize()
        try:
            loop_ctx[key] = deepcopy(value)
        except _EngineCopyRefused:
            # 非数据模型载荷：无法造出私有副本 ⇒ 保留活引用且**不登记**，
            # 就地改写落到 default-deny（具名拒绝），读仍可用。
            continue
        # plaita#69 r5/r7：隔离副本是引擎新建的（_engine_copy 的产出必然是精确
        # 类型的容器/标量）——登记为 fresh，循环条件里的就地 mutate（owner 钉住的
        # 唯一合法 in-place 现场）才能通过 default-deny。登记面是精确类型判据，
        # 因此这里不可能登记到活对象。
        note_fresh_closure(loop_ctx[key])
    return loop_ctx


class BaseCollectionNode(InlineFlow):
    """Shared base for all collection-processing nodes.

    Provides the ``collection`` and ``item_type`` fields plus the validator
    that normalises legacy camelCase aliases.  Concrete subclasses only need
    to implement ``execute``; none of them should reuse each other as a base
    just because they share these two fields.
    """

    item_type: Optional[Property] = None
    collection: Annotated[
        Expression,
        Field(description="待处理的集合，通常为表达式（$INPUT.xxx / $NODE.xxx），也可为字面量数组"),
    ] = None

    # _setup_item_type 消费的遗留键（item_type 字段无 alias）
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"typeDefs", "itemType"})

    @model_validator(mode="before")
    @classmethod
    def _setup_item_type(cls, values: Dict) -> Dict:
        type_defs = values.get("typeDefs") or values.get("itemType")
        if type_defs:
            values["item_type"] = Property.from_json(type_defs)
        return values

    def _eval_collection(self, execution) -> List[Any]:
        # plaita#69 r4（结构半边 1）：集合在**边界**物化——集合元素会作为
        # item/index 带进每个子执行的 ``$INPUT``；集合若取自活根（``$INPUT.items`` /
        # ``$NODE.x``），元素就是活持久化图里的对象，子流程里任何就地改写都会写穿。
        # 物化后子流程只拿到私有副本（与 ``update_node_result`` 同口径）。
        # plaita#69 r7：**不再**在此登记 fresh。r5/r6 的登记调用点在任何求值作用域
        # 之外，是 no-op（每轮求值的 fresh ≡ 本次求值由引擎新建）；而把整个子执行包进
        # 一个作用域会让同一次子执行里"求值 A 造出的对象在求值 B 里仍 fresh"（C2 反例）
        # 重新成立。故删除死登记：子执行对副本的就地改写具名拒绝（零写穿不变）。
        snapshot = _snapshot_value(execution.evaluate(self.collection))
        return _coerce_collection(snapshot)


class Loop(BaseCollectionNode):
    """
    重复执行，即循环节点。
    其内部流程的输入为：
    - item, 格式为collection的item-type.
    - index, 循环索引
    输出格式自定义
    """

    node_type: ClassVar[str] = "loop"
    node_name: ClassVar[str] = "重复"

    condition: Optional[Union[Condition, ConditionGroup]] = None

    def execute(self, execution):
        collection = self._eval_collection(execution)
        # ML2: 输出语义是「最后一次迭代的结果」（空集合为 None），不再保留
        # 全部迭代结果列表——大集合场景避免内存无谓翻倍。
        last_result = None
        index = 0
        for item in collection:
            item_execution = execution.get_child_execution()
            last_result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            if self.condition:
                # plaita#69 r6：条件求值是**一次**逻辑求值——隔离副本的物化
                # （`_condition_context` 的 `_engine_copy` + note_fresh_closure）与条件
                # 自身的 match 必须在同一个 freshness 作用域里，副本才算"本次求值
                # 由引擎新建"；否则 match 会另开作用域，副本变为非 fresh 而被拒。
                with freshness_scope():
                    matched = self.condition.match(
                        _condition_context(execution, self.condition, item=item,
                                           index=index, result=last_result),
                        execution.express_prefix,
                    )
                if not matched:
                    break
            index += 1
        return last_result

    async def arun(self, execution):
        collection = self._eval_collection(execution)
        last_result = None
        index = 0
        for item in collection:
            item_execution = execution.get_child_execution()
            last_result = await item_execution.arun_compatible(self.child_flow, False, item=item, index=index)
            if self.condition:
                # r6：同 sync 路径——物化副本与条件 match 同属一次逻辑求值。
                with freshness_scope():
                    matched = self.condition.match(
                        _condition_context(execution, self.condition, item=item,
                                           index=index, result=last_result),
                        execution.express_prefix,
                    )
                if not matched:
                    break
            index += 1
        return last_result


class Map(BaseCollectionNode):
    """
    映射节点，对集合中每个元素执行子流程并返回所有结果。
    支持 concurrent=True 并发执行。

    并发与非并发两条路径统一走 ``ParallelExecutor`` 协议 (见
    ``plaita.core.parallel_executor``): concurrent=True 时用
    ``ThreadParallelExecutor`` (复用模块级单例池, ``max_concurrent`` 用 semaphore
    gate, 不再每次 ``with ThreadPoolExecutor()`` 自起池); concurrent=False 时用
    ``SequentialExecutor``, 让两条路径共用同一套 ``executor.map`` 控制流。
    """

    node_type: ClassVar[str] = "map"
    node_name: ClassVar[str] = "映射"

    concurrent: bool = False
    max_concurrent: Optional[int] = None

    # "async" 是 concurrent 的历史别名（保留字做 dict 键易混淆，仅兼容存量 flow）
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"async"})

    @model_validator(mode="before")
    @classmethod
    def _normalize_concurrent(cls, values: Dict) -> Dict:
        # "async" was a historical field alias for "concurrent"; using a Python
        # reserved word as a dict key is confusing — normalise it here.
        if not values.get("concurrent"):
            values["concurrent"] = bool(values.get("async", False))
        return values

    def _build_executor(self) -> ParallelExecutor:
        if self.concurrent:
            if in_plaita_pool_thread():
                # 已在共享线程池 worker 上: 再向同一池提交会 starving 死锁
                # (见 parallel_executor 模块头), 降级串行。
                _logger.debug(
                    "map %s: on a plaita pool worker thread; degrading to sequential",
                    self.id,
                )
                return SequentialExecutor()
            return ThreadParallelExecutor(max_workers=self.max_concurrent or None)
        return SequentialExecutor()

    def execute(self, execution):
        collection = self._eval_collection(execution)

        # 并发路径在主线程预先创建子执行体, 避免 worker 线程并发调
        # ``get_child_execution`` 的潜在竞态; 串行路径改为逐元素懒创建——
        # 10k 元素曾一次性预建 10k 个 FlowExecution（实测 RSS +71MB）。
        if self.concurrent:
            triples = [
                (execution.get_child_execution(), item, index)
                for index, item in enumerate(collection)
            ]
        else:
            triples = (
                (execution.get_child_execution(), item, index)
                for index, item in enumerate(collection)
            )

        def run_one(triple: tuple) -> Any:
            child, item, index = triple
            return child.run_compatible(self.child_flow, False, item=item, index=index)

        executor = self._build_executor()
        return executor.map(run_one, triples)

    async def arun(self, execution):
        """异步 Map：concurrent=True 时用 asyncio.gather 并发，否则顺序执行。"""
        collection = self._eval_collection(execution)
        triples = [
            (execution.get_child_execution(), item, index)
            for index, item in enumerate(collection)
        ]

        async def run_one_async(child, item, index):
            return await child.arun_compatible(self.child_flow, False, item=item, index=index)

        if self.concurrent:
            sem = asyncio.Semaphore(self.max_concurrent or len(triples) or 1)

            async def run_one_gated(child, item, index):
                async with sem:
                    return await run_one_async(child, item, index)

            return list(await asyncio.gather(*(run_one_gated(c, i, idx) for c, i, idx in triples)))
        else:
            results = []
            for child, item, index in triples:
                results.append(await run_one_async(child, item, index))
            return results


class Filter(BaseCollectionNode):
    """
    过滤节点，对集合中的数据进行过滤，返回值为collection子集。
    子流程规范：
    - 输入：item（collection的元素）、index（集合顺序）
    - 输出：bool
    """

    node_type: ClassVar[str] = "filter"
    node_name: ClassVar[str] = "过滤"

    def execute(self, execution):
        collection = self._eval_collection(execution)
        results = []
        for index, item in enumerate(collection):
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            if result:
                results.append(item)
        return results

    async def arun(self, execution):
        collection = self._eval_collection(execution)
        results = []
        for index, item in enumerate(collection):
            item_execution = execution.get_child_execution()
            result = await item_execution.arun_compatible(self.child_flow, False, item=item, index=index)
            if result:
                results.append(item)
        return results


class Find(BaseCollectionNode):
    """
    查找节点，返回集合中第一个使子流程返回真值的元素。
    子流程规范：
    - 输入：item（collection的元素）、index（集合顺序）
    - 输出：bool
    """

    node_type: ClassVar[str] = "find"
    node_name: ClassVar[str] = "查找"

    def execute(self, execution):
        collection = self._eval_collection(execution)
        for index, item in enumerate(collection):
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            if result:
                return item
        return None

    async def arun(self, execution):
        collection = self._eval_collection(execution)
        for index, item in enumerate(collection):
            item_execution = execution.get_child_execution()
            result = await item_execution.arun_compatible(self.child_flow, False, item=item, index=index)
            if result:
                return item
        return None


class Reduce(BaseCollectionNode):
    """
    归纳节点，对集合中的元素逐个进行计算。
    子流程规范：
    - 输入：first（当前累积值）、second（当前元素）
    - 输出：新的累积值（与 collection 元素同类型）
    """

    node_type: ClassVar[str] = "reduce"
    node_name: ClassVar[str] = "归纳"
    initial: Annotated[
        Expression,
        Field(description="归纳的初始累积值，通常为表达式或字面量（如 0 / \"\"）；未设置时以首元素为初始值"),
    ] = None

    def _eval_initial(self, execution):
        """``Reduce.initial`` 的边界物化（plaita#69 r5）。

        r4 的结构半边 1 漏了这条：``execution.evaluate(self.initial)`` 的结果**按引用**
        作为 ``$INPUT[0]`` / ``first`` 交给子执行，活对象（如 ``$INPUT.cfg`` 的某个子对象）
        因此能进子流程，并在子流程的嵌套表达式串作用域里被就地写穿（verifier X2，
        写穿发生在随后 KeyError 之前）。物化：子执行只看私有副本。

        plaita#69 r7：**不再**登记 fresh（该调用点在求值作用域之外，r6 起就是 no-op；
        包住整个子执行会重开 C2 通道）。子执行对副本的就地改写具名拒绝，父活 state 与
        checkpoint 零写穿。
        """
        return _snapshot_value(execution.evaluate(self.initial))

    def execute(self, execution):
        collection = self._eval_collection(execution)
        result = self._eval_initial(execution) if self.initial is not None else collection[0]
        items = collection[1:] if self.initial is None else collection
        array_input = self._child_is_array_input()
        for item in items:
            item_execution = execution.get_child_execution()
            if array_input:
                # @flow DSL 编译时选择 array 输入，$INPUT[0]=first, $INPUT[1]=second
                result = item_execution.run_compatible(self.child_flow, False, [result, item])
            else:
                # 历史兼容路径：object 输入，$INPUT.first / $INPUT.second
                result = item_execution.run_compatible(
                    self.child_flow, False, first=result, second=item
                )
        return result

    async def arun(self, execution):
        collection = self._eval_collection(execution)
        result = self._eval_initial(execution) if self.initial is not None else collection[0]
        items = collection[1:] if self.initial is None else collection
        array_input = self._child_is_array_input()
        for item in items:
            item_execution = execution.get_child_execution()
            if array_input:
                result = await item_execution.arun_compatible(self.child_flow, False, [result, item])
            else:
                result = await item_execution.arun_compatible(
                    self.child_flow, False, first=result, second=item
                )
        return result

    def _child_is_array_input(self) -> bool:
        """Return True when the child flow declares array input (DSL convention)."""
        if self.child_flow is None:
            return False
        input_type = getattr(self.child_flow, "inputType", None) or getattr(
            self.child_flow, "input_type", None
        )
        if input_type is None:
            return False
        if isinstance(input_type, dict):
            return input_type.get("dataType") == "array"
        data_type = getattr(input_type, "dataType", None) or getattr(
            input_type, "data_type", None
        )
        return data_type == "array"


class While(InlineFlow):
    """条件循环节点（while 型）。

    以 ``condition`` 为继续条件反复执行子流程：条件满足则继续下一轮，
    不满足即退出；``max_iterations`` 为迭代上限保护，达到上限强制停止并告警。
    子流程输入：item（上一轮结果，首轮为 None）、index（轮次，从 0 起）。
    条件上下文可引用 $LOOP-ITEM（上一轮结果）/ $LOOP-INDEX（当前轮次）。
    节点输出为最后一轮子流程结果（未执行任何轮次时为 None）。
    """

    node_type: ClassVar[str] = "while"
    node_name: ClassVar[str] = "条件循环"

    condition: Optional[Union[Condition, ConditionGroup]] = None
    max_iterations: int = 1000

    # 基类 `InlineFlow.LEGACY_KEYS` 声明了 ``childFlow``（canonical 正典把
    # while 的 ``child_flow`` 改名成它，`_canonical.py`）——本类此前只声明
    # ``maxIterations``，把基类那份**覆写掉**了，于是 canonical 产物每次
    # `Flow.model_validate` 都误报 `unknown config keys ['childFlow'] will be
    # IGNORED`，诱导作者去"修"正确的流程（plaita#89）。
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"maxIterations", "childFlow"})

    @model_validator(mode="before")
    @classmethod
    def _setup_max_iterations(cls, values: Dict) -> Dict:
        if values.get("maxIterations") is not None and values.get("max_iterations") is None:
            values["max_iterations"] = values["maxIterations"]
        return values

    def _should_continue(self, execution, index: int, result: Any) -> bool:
        """condition 为 None 时不循环（仅执行一轮）；否则条件满足才继续。"""
        if self.condition is None:
            return index == 0
        # While 语义不注入 LOOP-RESULT（条件可引用 $LOOP-ITEM / $LOOP-INDEX）
        # r6：物化副本与条件 match 同属一次逻辑求值（见 Loop.execute 的同款说明）。
        with freshness_scope():
            loop_ctx = _condition_context(execution, self.condition, item=result, index=index)
            return bool(self.condition.match(loop_ctx, execution.express_prefix))

    def execute(self, execution):
        index = 0
        result = None
        stopped_by_limit = False
        while index < self.max_iterations and self._should_continue(execution, index, result):
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=result, index=index)
            index += 1
            stopped_by_limit = index >= self.max_iterations
        if stopped_by_limit:
            _logger.warning(
                "While 节点 %s 达到 max_iterations=%s 上限，已强制停止", self.id, self.max_iterations
            )
        return result

    async def arun(self, execution):
        index = 0
        result = None
        stopped_by_limit = False
        while index < self.max_iterations and self._should_continue(execution, index, result):
            item_execution = execution.get_child_execution()
            result = await item_execution.arun_compatible(self.child_flow, False, item=result, index=index)
            index += 1
            stopped_by_limit = index >= self.max_iterations
        if stopped_by_limit:
            _logger.warning(
                "While 节点 %s 达到 max_iterations=%s 上限，已强制停止", self.id, self.max_iterations
            )
        return result
