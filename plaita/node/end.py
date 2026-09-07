from typing import ClassVar, Dict, Literal, Optional

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
    )

    node_type: ClassVar[str] = "end"
    node_name: ClassVar[str] = "结束"

    # setup_end 消费的遗留键（result_type 字段无 alias）
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"resultType"})

    # Literal 生成 schema enum（console 表单渲染下拉）；非法历史值由 setup_end
    # before-validator 先归一为 success（带 warning），Literal 校验其结果
    result_type: Optional[
        Literal["success", "error", "nop"]
    ] = Field(
        END_TYPE_NORMAL,
        description="结束类型: success（正常结束，默认）/ error（以错误结束，配 error）/ nop（静默结束）",
    )
    error: Optional[Dict] = None

    @model_validator(mode="before")
    @classmethod
    def setup_end(cls, values: Dict) -> Dict:
        # Handle both resultType and result_type; 缺省即 success——历史上默认
        # None, execute() 三个分支全不命中而静默返回 None, 与 docstring
        # "success（正常结束，默认）"自相矛盾（FlowBuilder/S-expr 前端一直
        # 默认 success, 手写 JSON 是唯一踩坑路径）。
        result_type = values.get("resultType") or values.get("result_type") or END_TYPE_NORMAL
        values["result_type"] = result_type
        if result_type not in (END_TYPE_NORMAL, END_TYPE_NOP, END_TYPE_ERROR):
            import logging

            logging.getLogger(__name__).warning(
                "end node %s: unknown resultType %r (valid: success/error/nop); "
                "treating it as 'success'",
                values.get("id", "?"), result_type,
            )
            values["result_type"] = END_TYPE_NORMAL
        return values

    def execute(self, execution):
        if self.result_type == END_TYPE_NOP:
            return None
        elif self.result_type == END_TYPE_NORMAL:
            return execution.evaluate(self.output)
        elif self.result_type == END_TYPE_ERROR:
            raise plaita_errors.FlowResultError(self.error.get("code", None), self.error.get("message", None))
