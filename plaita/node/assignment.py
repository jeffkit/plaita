from typing import ClassVar, Optional

from pydantic import Field, model_validator

from ..io import Property, match
from ..node.basic import Node


class Assignment(Node):
    node_type: ClassVar[str] = "assignment"
    node_name: ClassVar[str] = "赋值"

    output_type: Optional[Property] = None
    upstream_output: list[dict] = Field(default_factory=list)

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
            upstream = execution.last_node_id
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
