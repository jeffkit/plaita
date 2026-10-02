import asyncio
import json
import logging
import re
from copy import deepcopy
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

_logger = logging.getLogger(__name__)


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
    随后仅对条件引用到的根键做 deepcopy 替换（写隔离），其余键与
    execution.context 共享对象（只读）。
    """
    pfx = execution.express_prefix
    loop_ctx = dict(execution.context)
    loop_ctx[f"{pfx}LOOP-ITEM"] = item
    loop_ctx[f"{pfx}LOOP-INDEX"] = index
    if result is not _UNSET:
        loop_ctx[f"{pfx}LOOP-RESULT"] = result
    for key in _isolated_root_keys(condition, pfx):
        try:
            loop_ctx[key] = deepcopy(loop_ctx[key])
        except KeyError:
            pass  # 条件引用了上下文中不存在的根：与全量 deepcopy 一样缺席，求值期按原语义报 KeyError
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
        return _coerce_collection(execution.evaluate(self.collection))


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
            if self.condition and not self.condition.match(
                _condition_context(execution, self.condition, item=item, index=index, result=last_result),
                execution.express_prefix,
            ):
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
            if self.condition and not self.condition.match(
                _condition_context(execution, self.condition, item=item, index=index, result=last_result),
                execution.express_prefix,
            ):
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

    def execute(self, execution):
        collection = self._eval_collection(execution)
        result = execution.evaluate(self.initial) if self.initial is not None else collection[0]
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
        result = execution.evaluate(self.initial) if self.initial is not None else collection[0]
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

    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"maxIterations"})

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
