from copy import deepcopy
from typing import Any, ClassVar, Dict, Optional, Union

from pydantic import model_validator

from plaita.core.parallel_executor import (
    ParallelExecutor,
    SequentialExecutor,
    ThreadParallelExecutor,
)

from ..io import Property
from .child import InlineFlow
from .decide import Condition, ConditionGroup


class BaseCollectionNode(InlineFlow):
    """Shared base for all collection-processing nodes.

    Provides the ``collection`` and ``item_type`` fields plus the validator
    that normalises legacy camelCase aliases.  Concrete subclasses only need
    to implement ``execute``; none of them should reuse each other as a base
    just because they share these two fields.
    """

    item_type: Optional[Property] = None
    collection: Any = None

    @model_validator(mode="before")
    @classmethod
    def _setup_item_type(cls, values: Dict) -> Dict:
        type_defs = values.get("typeDefs") or values.get("itemType")
        if type_defs:
            values["item_type"] = Property.from_json(type_defs)
        return values


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
        collection = execution.evaluate(self.collection)
        results = []
        pfx = execution.express_prefix
        index = 0
        for item in collection:
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            results.append(result)
            if self.condition:
                # 表达式引擎里有 ``$F.set`` / ``$F.pop`` / ``$F.setListItem`` 等
                # mutate 操作 (见 plaita/core/expression.py), condition.match 通过
                # evaluate 调用它们时会改 context 内 value 对象。``dict(...)``
                # shallow copy 只防 top-level key 增删, 不防 value 对象被改——
                # 历史上注释自安慰 "match is read-only" 是错的。每次迭代 deepcopy
                # 把隔离做实, O(context_size) 的成本对 loop 而言可接受 (condition
                # 已经是少数路径, 不在 hot loop)。
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
            return ThreadParallelExecutor(max_workers=self.max_concurrent or None)
        return SequentialExecutor()

    def execute(self, execution):
        collection = list(execution.evaluate(self.collection))

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
        collection = execution.evaluate(self.collection)
        results = []
        index = 0
        for item in collection:
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            if result:
                results.append(item)
            index += 1
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
        collection = execution.evaluate(self.collection)
        index = 0
        for item in collection:
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            if result:
                return item
            index += 1
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
    initial: Optional[Any] = None

    def execute(self, execution):
        collection = execution.evaluate(self.collection)
        result = execution.evaluate(self.initial) if self.initial is not None else collection[0]

        items = collection[1:] if self.initial is None else collection
        for item in items:
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, first=result, second=item)
        return result
