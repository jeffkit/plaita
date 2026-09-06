from typing import Annotated, Any, ClassVar, Dict, Optional

from pydantic import Field, model_validator

from ..io import match
from .basic import Expression, Node
from plaita.core.strategies import ExecutionMode


class FlowNode(Node):
    input: Annotated[
        Expression,
        Field(description="注入子流程的输入，通常为表达式（$INPUT.xxx / $NODE.xxx），也可为字面量"),
    ] = None
    child_flow: Optional[Any] = None

    @model_validator(mode="before")
    def setup_child_flow(cls, values: Dict) -> Dict:
        from plaita.core.flow import Flow

        child_flow = values.get("childFlow") or values.get("child_flow")
        if isinstance(child_flow, str):
            values["child_flow"] = Flow.model_validate_json(child_flow)
        elif isinstance(child_flow, dict):
            values["child_flow"] = Flow.model_validate(child_flow)
        if not values.get("child_flow"):
            raise RuntimeError("child_flow is required")
        return values

    def execute(self, execution):
        pass


class InlineFlow(FlowNode):
    """
    内联子逻辑，子逻辑内的节点可共享父级及更高级别的数据，性质相当于调用一个匿名函数。
    """

    node_type: ClassVar[str] = "child"
    node_name: ClassVar[str] = "内联子逻辑"

    # setup_child_flow 消费的 camelCase 键（child_flow 字段无 alias）——
    # 不登记会误报 unknown-key warning，诱导 AI 作者"修复"正确的流程
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"childFlow"})

    def execute(self, execution):
        if self.child_flow.input_property:
            assert match(self.child_flow.input_property, self.input), "input not match required"
        child_execution = execution.get_child_execution()
        lazy = execution.mode == ExecutionMode.GENERATOR
        return child_execution.run_compatible(self.child_flow, lazy, execution.evaluate(self.input))

    async def arun(self, execution):
        if self.child_flow.input_property:
            assert match(self.child_flow.input_property, self.input), "input not match required"
        child_execution = execution.get_child_execution()
        lazy = execution.mode == ExecutionMode.GENERATOR
        return await child_execution.arun_compatible(self.child_flow, lazy, execution.evaluate(self.input))


class ReferenceFlow(FlowNode):
    """
    引用逻辑节点，该节点内的逻辑不能使用父逻辑上下文的数据。性质相当于调用另一个独立函数。
    本运行时的引用逻辑节点不去获取子逻辑，子逻辑依赖上层的调度器注入，以减轻节点设计的复杂度。
    """

    node_type: ClassVar[str] = "reference"
    node_name: ClassVar[str] = "引用逻辑"

    # 调度器据此解析并注入 child_flow；若不声明为字段，pydantic 会按
    # extra=ignore 把 validator 写入的值静默丢弃，定义里就存不下引用信息。
    flow_id: Optional[str] = Field(None, description="引用的流程定义 ID（调度器据此注入子流程）")
    flow_version: Optional[str] = Field(None, description="引用的流程版本；未设置时由调度器取最新/默认版本")

    # setup_child_flow 消费的 camelCase 遗留键
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"flowID", "flowVersion"})

    @model_validator(mode="before")
    @classmethod
    def setup_child_flow(cls, values: Dict) -> Dict:
        # 只在 camelCase 别名存在时迁移，避免把已按规范键存好的值覆盖回 None
        if values.get("flowID") is not None:
            values["flow_id"] = values["flowID"]
        if values.get("flowVersion") is not None:
            values["flow_version"] = values["flowVersion"]
        return values

    def execute(self, execution):
        if self.child_flow is None:
            raise ValueError(
                f"reference 节点 {self.id!r} 缺少 child_flow（调度器尚未注入引用的子流程）。"
                "请确认 flow_id/flow_version 指向存在的流程定义。"
            )
        assert match(self.child_flow.input_property, self.input), "input not match required"
        child_execution = execution.get_child_execution()
        lazy = execution.mode == ExecutionMode.GENERATOR
        return child_execution.run_compatible(self.child_flow, lazy, execution.evaluate(self.input))

    async def arun(self, execution):
        assert self.child_flow is not None, "child_flow for reference flow node is required"
        assert match(self.child_flow.input_property, self.input), "input not match required"
        child_execution = execution.get_child_execution()
        lazy = execution.mode == ExecutionMode.GENERATOR
        return await child_execution.arun_compatible(self.child_flow, lazy, execution.evaluate(self.input))
