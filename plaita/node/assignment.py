from typing import ClassVar, Optional

from pydantic import Field, model_validator

from ..io import Property, match
from ..node.basic import Node


class Assignment(Node):
    """变量赋值节点。

    按 ``assignments`` 列表依次将表达式求值结果写入上下文变量，
    供下游节点以 ``$NODE.<本节点id>.<变量名>`` 引用。
    """

    node_type: ClassVar[str] = "assignment"
    node_name: ClassVar[str] = "赋值"

    output_type: Optional[Property] = None
    upstream_output: list[dict] = Field(default_factory=list)

    # setup_assignment 消费的 camelCase 遗留键
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"outputType", "upstreamOutput"})

    @model_validator(mode="before")
    @classmethod
    def setup_assignment(cls, values) -> "Assignment":
        # Handle output_type/outputType
        output_type = values.get("outputType") or values.get("output_type")
        if output_type:
            if isinstance(output_type, dict):
                values["output_type"] = Property.model_validate(output_type)
            else:
                values["output_type"] = output_type

        # Handle upstream_output/upstreamOutput
        upstream_output = values.get("upstreamOutput") or values.get("upstream_output")
        if upstream_output:
            values["upstream_output"] = upstream_output

        return values

    def validate(self):
        assert self.output_type is not None, "assignment node required outputType."
        assert self.output is not None or len(self.upstream_output) >= 0, "Assignment Node required output."

    def execute(self, execution):
        if len(self.upstream_output) == 1:
            value = self.upstream_output[0]["value"]
        elif len(self.upstream_output) > 1:
            upstream = execution.state.last_node_id
            value = [out for out in self.upstream_output if out["upstream"] == upstream]
            if value:
                value = value[0]["value"]
        elif self.output:  # 只有一个,不用管upstream
            value = self.output
        else:
            return None
        if self.output_type:
            if match(self.output_type, value):
                return execution.evaluate(value)
        else:
            return execution.evaluate(value)
