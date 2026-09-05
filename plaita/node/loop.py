import asyncio
import json
import logging
from copy import deepcopy
from typing import Annotated, Any, ClassVar, Dict, List, Optional, Union

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
        results = []
        pfx = execution.express_prefix
        index = 0
        for item in collection:
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            results.append(result)
            if self.condition:
                loop_ctx = deepcopy(execution.context)
                loop_ctx[f"{pfx}LOOP-ITEM"] = item
                loop_ctx[f"{pfx}LOOP-INDEX"] = index
                loop_ctx[f"{pfx}LOOP-RESULT"] = result
                if not self.condition.match(loop_ctx, pfx):
                    break
            index += 1
        return results[-1] if len(results) > 0 else None

    async def arun(self, execution):
        collection = self._eval_collection(execution)
        results = []
        pfx = execution.express_prefix
        index = 0
        for item in collection:
            item_execution = execution.get_child_execution()
            result = await item_execution.arun_compatible(self.child_flow, False, item=item, index=index)
            results.append(result)
            if self.condition:
                loop_ctx = deepcopy(execution.context)
                loop_ctx[f"{pfx}LOOP-ITEM"] = item
                loop_ctx[f"{pfx}LOOP-INDEX"] = index
                loop_ctx[f"{pfx}LOOP-RESULT"] = result
                if not self.condition.match(loop_ctx, pfx):
                    break
            index += 1
        return results[-1] if len(results) > 0 else None


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

        # 子执行体在主线程预先创建 (与历史并发路径行为一致), 避免 worker 线程
        # 并发调 ``get_child_execution`` 触发 ``callback_manager.child()`` 的潜在
        # 竞态。非并发路径也走同一路径, 代价是预先创建 N 个 child 对象 (可接受)。
        triples = [
            (execution.get_child_execution(), item, index)
            for index, item in enumerate(collection)
        ]

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
        loop_ctx = deepcopy(execution.context)
        pfx = execution.express_prefix
        loop_ctx[f"{pfx}LOOP-ITEM"] = result
        loop_ctx[f"{pfx}LOOP-INDEX"] = index
        return bool(self.condition.match(loop_ctx, pfx))

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
