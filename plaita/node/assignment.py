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
        # 显式 raise 而非 assert：assert 在 ``python -O`` 下整体消失，校验形同虚设；
        # 且同仓节点校验（decide.py 对缺字段的处理）的既有风格是带节点 id 的 ValueError。
        if self.output_type is None:
            raise ValueError(f"assignment node {self.id!r}: 缺少必填的 output_type（outputType）。")
        # 曾经第二个条件是 ``len(self.upstream_output) >= 0``——恒真，于是文案宣称的
        # 「必须有 output」从未生效：缺 output 时构建期静默通过，运行期落入 execute()
        # 的 else 分支静默产出 None。改成 ``== 0``（等价于 ``> 0`` 的取反）后，
        # 「output 与 upstream_output 至少有一个」才真正成立。注意 output=0/False/""
        # 等假值字面量是合法赋值，这里必须以 ``is None`` 判断而非真值判断。
        if self.output is None and len(self.upstream_output) == 0:
            raise ValueError(
                f"assignment node {self.id!r}: 必须提供 output 或 upstream_output 之一，"
                "两者都缺失时节点无法产出任何值。"
            )

    def execute(self, execution):
        if len(self.upstream_output) == 1:
            value = self.upstream_output[0]["value"]
        elif len(self.upstream_output) > 1:
            upstream = execution.state.last_node_id
            value = [out for out in self.upstream_output if out["upstream"] == upstream]
            if value:
                value = value[0]["value"]
        elif self.output is not None:  # 只有一个,不用管upstream；is not None——
            # 假值字面量（0/False/""）也是合法赋值，曾因真值判断被静默吞成 None
            value = self.output
        # 无任何来源时保持 ``return None``（历史行为）：validate() 现在会在构建期
        # 抛 ValueError 拦下这种配置，但 JSON 直载路径经 Flow._validate_nodes 是
        # warning 降级（try/except 吞掉），且仓内多处既有 fixture 用无 output 的
        # assignment 只做「推进节点」用途——运行期改成抛错是破坏性语义变化
        # （实测撞红 3 个既有用例，波及 7+ 个使用无 output assignment 的测试文件），
        # 故按 issue「若担心兼容性，至少保证构建期可见」取最小修法，不动运行期。
        else:
            return None
        # 先求值再校验类型：output 常是 "$F.xxx(...)" 表达式串，曾先 match 原始
        # 字符串——数值/布尔 output_type 必然 miss，静默返回 None。
        evaluated = execution.evaluate(value)
        if self.output_type and not match(self.output_type, evaluated):
            raise ValueError(
                f"assignment node {self.id!r}: 求值结果 {evaluated!r} 不符合声明的 "
                f"output_type {self.output_type.data_type!r}"
            )
        return evaluated
