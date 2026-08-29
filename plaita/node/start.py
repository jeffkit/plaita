from typing import ClassVar

from .basic import Node


class Start(Node):
    """流程入口节点。

    每个流程（含子流程）必须有且仅有一个 start，流程入参即 ``$INPUT``。
    无可配置项，执行时直接放行到 ``next`` 指向的下游节点。
    """

    node_name: ClassVar[str] = "开始"
    node_type: ClassVar[str] = "start"

    def execute(self, execution):
        pass
