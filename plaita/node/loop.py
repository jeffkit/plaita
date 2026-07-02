from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar, Dict, Optional, Union

from pydantic import model_validator

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
                # Shallow-merge is sufficient: condition.match is read-only.
                # A full deepcopy here is an O(context_size) allocation per iteration
                # with zero benefit — avoid it.
                loop_ctx = dict(execution.context)
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
    支持 concurrent=True 并发执行（线程池）。
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

    def execute(self, execution):
        collection = execution.evaluate(self.collection)
        index = 0

        if self.concurrent:
            max_workers = self.max_concurrent or None
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        execution.get_child_execution().run_compatible,
                        self.child_flow, False, item=item, index=i,
                    )
                    for i, item in enumerate(collection)
                ]
                return [f.result() for f in futures]

        results = []
        for item in collection:
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            results.append(result)
            index += 1
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
