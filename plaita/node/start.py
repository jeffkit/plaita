from typing import ClassVar

from .basic import Node


class Start(Node):
    node_name: ClassVar[str] = "开始"
    node_type: ClassVar[str] = "start"

    def execute(self, execution):
        pass
