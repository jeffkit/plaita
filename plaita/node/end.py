from typing import ClassVar, Dict, Optional

from pydantic import ConfigDict, Field, model_validator

import plaita.core.errors as plaita_errors

from .basic import Node

END_TYPE_NORMAL = "success"
END_TYPE_NOP = "nop"
END_TYPE_ERROR = "error"


class End(Node):
    """流程结束节点。

    汇总流程输出：``output`` 表达式的结果作为流程返回值；
    ``result_type`` 支持 success（正常结束，默认）/ error（以错误结束，配 ``error``）/ nop（静默结束）。
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    node_type: ClassVar[str] = "end"
    node_name: ClassVar[str] = "结束"

    result_type: Optional[str] = Field(None, description="Result type of the end node")
    error: Optional[Dict] = None

    @model_validator(mode="before")
    @classmethod
    def setup_end(cls, values: Dict) -> Dict:
        # Handle both resultType and result_type
        result_type = values.get("resultType") or values.get("result_type")
        if result_type:
            values["result_type"] = result_type
        return values

    def execute(self, execution):
        if self.result_type == END_TYPE_NOP:
            return None
        elif self.result_type == END_TYPE_NORMAL:
            return execution.evaluate(self.output)
        elif self.result_type == END_TYPE_ERROR:
            raise plaita_errors.FlowResultError(self.error.get("code", None), self.error.get("message", None))
