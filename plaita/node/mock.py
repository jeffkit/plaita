from typing import Any, ClassVar, Optional

from ..node.basic import Node


class Mock(Node):
    """数据固定（pin）占位节点：原样返回 ``value`` 字段作为节点输出。

    主要服务于编排台调试：把某次试运行的节点输出「固定」下来，后续试跑
    直接以固定值作为该节点结果（console 侧会把被固定的节点替换成本类型），
    从而跳过真实执行（如 HTTP 调用）反复调试下游。也可手工作为测试桩。
    """

    node_type: ClassVar[str] = "mock"
    node_name: ClassVar[str] = "数据固定"

    value: Optional[Any] = None

    def execute(self, execution):
        return self.value
