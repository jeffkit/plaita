"""
plaita.server.nodes — Server-specific extension nodes.

These nodes are discovered by the core ``NodeRegistry`` via
``importlib.metadata`` entry_points (group ``plaita.nodes``) configured
in ``pyproject.toml``.  They do **not** need to be imported by
``plaita.node.__init__``.
"""

from .delay_node import DelayNode
from .redis_queue_node import RedisQueueNode
from .kafka_queue_node import KafkaQueueNode
from .http_callback_node import HttpCallbackNode
from .approval_node import ApprovalNode

__all__ = [
    "DelayNode",
    "RedisQueueNode",
    "KafkaQueueNode",
    "HttpCallbackNode",
    "ApprovalNode",
]
