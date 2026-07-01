from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Any, ClassVar, Dict, Optional, Union

from pydantic import model_validator

from ..io import Property
from .child import InlineFlow
from .decide import Condition, ConditionGroup


class Loop(InlineFlow):
    """
    重复执行，即循环节点。
    其内部流程的输入为：
    - item, 格式为collection的item-type.
    - index, 循环索引
    输出格式自定义
    """

    node_type: ClassVar[str] = "loop"
    node_name: ClassVar[str] = "重复"

    item_type: Optional[Property] = None
    collection: Any = None
    condition: Optional[Union[Condition, ConditionGroup]] = None

    @model_validator(mode="before")
    @classmethod
    def setup_item_type(cls, values: Dict) -> Dict:
        type_defs = values.get("typeDefs") or values.get("itemType")
        if type_defs:
            values["item_type"] = Property.from_json(type_defs)
        values["condition"] = values.get("condition")
        return values

    def execute(self, execution):
        collection = execution.evaluate(self.collection)
        results = []
        context = deepcopy(execution.context)
        index = 0
        for item in collection:
            # 使用了execution来执行
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
            results.append(result)
            context[f"{execution.express_prefix}LOOP-ITEM"] = item
            context[f"{execution.express_prefix}LOOP-INDEX"] = index
            context[f"{execution.express_prefix}LOOP-RESULT"] = results[-1]
            if self.condition and not self.condition.match(context, execution.express_prefix):
                break
            index += 1
        return results[-1] if len(results) > 0 else None


class Map(Loop):
    node_type: ClassVar[str] = "map"
    node_name: ClassVar[str] = "映射"

    concurrent: bool = False
    max_concurrent: Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def setup_output(cls, values: Dict) -> Dict:
        # values["output"] = Property(data_type=types.ARRAY, item_type=values.get("child_flow").output_property)
        if values.get("concurrent"):
            values["concurrent"] = values.get("concurrent", False)
        else:
            values["concurrent"] = values.get("async", False)
        return values

    def execute(self, execution):
        collection = execution.evaluate(self.collection)
        results = []
        index = 0
        
        if self.concurrent:
            max_workers = self.max_concurrent if self.max_concurrent else None
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for item in collection:
                    item_execution = execution.get_child_execution()
                    future = executor.submit(
                        item_execution.run_compatible,
                        self.child_flow,
                        False,
                        item=item,
                        index=index
                    )
                    futures.append(future)
                    index += 1
                # Get results in order
                results = [f.result() for f in futures]
        else:
            # Sequential execution
            for item in collection:
                item_execution = execution.get_child_execution()
                result = item_execution.run_compatible(self.child_flow, False, item=item, index=index)
                results.append(result)
                index += 1
                
        return results

class Filter(Loop):
    """
    过滤节点，对集合中的数据进行过滤.返回值为collection子集
    子流程规范：
    输入：
    - item， collection的元素
    - index，集合顺序

    输出：
    - bool

    """

    node_type: ClassVar[str] = "filter"
    node_name: ClassVar[str] = "过滤"

    @model_validator(mode="before")
    @classmethod
    def setup_output(cls, values: Dict) -> Dict:
        # values["output"] = Property(data_type=types.ARRAY, item_type=values.get("child_flow").output_property)
        return values

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


class Find(Filter):
    """
    查找节点，对集合中的数据进行过滤，返回第一个符合条件的.
    子流程规范：
    输入：
    - item， collection的元素
    - index，集合顺序

    输出：
    - bool
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


class Reduce(Loop):
    """
    归纳节点，对集合中的元素逐个进行计算
    子流程规范：
    输入：
    - first: collection item 类型
    - second: collection item 类型
    输出：
    - collection item类型
    """

    node_type: ClassVar[str] = "reduce"
    node_name: ClassVar[str] = "归纳"
    initial: Optional[Any] = None

    def execute(self, execution):
        collection = execution.evaluate(self.collection)
        result = execution.evaluate(self.initial) if self.initial else collection[0]

        if not self.initial:
            collection = collection[1:]
        for item in collection:
            item_execution = execution.get_child_execution()
            result = item_execution.run_compatible(self.child_flow, False, result, item)
        return result
