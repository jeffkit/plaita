"""
plaita.core.callback — Flow callback system.

Provides FlowCallback (concrete base with no-op defaults), CallbackManager
for multi-callback dispatch, and LoggerCallback for debug logging.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from plaita.node.basic import Node

logger = logging.getLogger("plaita.core.callback")


class FlowEvent(Enum):
    """Flow lifecycle event types."""
    FLOW_START = "flow_start"
    FLOW_END = "flow_end"
    NODE_START = "node_start"
    NODE_END = "node_end"


class FlowCallback:
    """Base callback with default no-op implementations.

    Subclass and override only the hooks you need.
    """

    def on_flow_start(self, flow, **kwargs) -> None:
        pass

    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs) -> None:
        pass

    def on_flow_suspend(self, flow, **kwargs) -> None:
        pass

    def on_flow_resume(self, flow, **kwargs) -> None:
        pass

    def on_node_start(self, flow, node, **kwargs) -> None:
        pass

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        pass

    def on_node_suspend(self, flow, node, **kwargs) -> None:
        pass

    def on_node_resume(self, flow, node, **kwargs) -> None:
        pass


class BaseCallbackManager(ABC):
    """Abstract base for callback managers."""

    @abstractmethod
    def on_flow_start(self, flow, **kwargs) -> None: ...

    @abstractmethod
    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs) -> None: ...

    @abstractmethod
    def on_flow_suspend(self, flow, **kwargs) -> None: ...

    @abstractmethod
    def on_flow_resume(self, flow, **kwargs) -> None: ...

    @abstractmethod
    def on_node_start(self, flow, node, **kwargs) -> None: ...

    @abstractmethod
    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None: ...

    @abstractmethod
    def on_node_suspend(self, flow, node, **kwargs) -> None: ...

    @abstractmethod
    def on_node_resume(self, flow, node, **kwargs) -> None: ...


class CallbackManager(BaseCallbackManager):
    """Callback manager that dispatches to multiple handlers."""

    def __init__(
        self,
        handlers: List[FlowCallback],
        parent: Optional[CallbackManager] = None,
        inherit_handlers: bool = False,
    ) -> None:
        self.handlers = list(handlers)
        self.parent = parent
        if parent and inherit_handlers:
            self.handlers.extend(parent.handlers)

    def add_handler(self, handler: FlowCallback) -> None:
        self.handlers.append(handler)

    def remove_handler(self, handler: FlowCallback) -> None:
        self.handlers.remove(handler)

    def _call_handlers(self, handler_method: str, *args, **kwargs) -> None:
        for handler in self.handlers:
            try:
                getattr(handler, handler_method)(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Error in {handler_method} callback: {e}", exc_info=True)

    def on_flow_start(self, flow, **kwargs) -> None:
        self._call_handlers("on_flow_start", flow, **kwargs)

    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs) -> None:
        self._call_handlers("on_flow_end", flow, result, error, exception, **kwargs)

    def on_node_start(self, flow, node, **kwargs) -> None:
        self._call_handlers("on_node_start", flow, node, **kwargs)

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        self._call_handlers("on_node_end", flow, node, result, error, exception, **kwargs)

    def on_flow_suspend(self, flow, **kwargs) -> None:
        self._call_handlers("on_flow_suspend", flow, **kwargs)

    def on_flow_resume(self, flow, **kwargs) -> None:
        self._call_handlers("on_flow_resume", flow, **kwargs)

    def on_node_suspend(self, flow, node, **kwargs) -> None:
        self._call_handlers("on_node_suspend", flow, node, **kwargs)

    def on_node_resume(self, flow, node, **kwargs) -> None:
        self._call_handlers("on_node_resume", flow, node, **kwargs)

    def child(self, handlers: Optional[List[FlowCallback]] = None) -> CallbackManager:
        return CallbackManager(handlers=handlers or [], parent=self, inherit_handlers=True)


class LoggerCallback(FlowCallback):
    """Callback that logs flow/node lifecycle events."""

    def on_flow_start(self, flow, **kwargs) -> None:
        logger.info(f"[flow start] {flow.flow_id}")

    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs) -> None:
        logger.info(f"[flow end] {flow.flow_id} with result: {result}, error: {error}, exception: {exception}")

    def on_node_start(self, flow, node, **kwargs) -> None:
        logger.info(f"[node start] {node.id} @ flow {flow.flow_id}")

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        logger.info(
            f"[node end] {node.id} @ flow {flow.flow_id} with result: {result}, error: {error}, exception: {exception}"
        )

    def on_flow_suspend(self, flow, **kwargs) -> None:
        logger.info(f"[flow suspend] {flow.flow_id}")

    def on_flow_resume(self, flow, **kwargs) -> None:
        logger.info(f"[flow resume] {flow.flow_id}")

    def on_node_suspend(self, flow, node, **kwargs) -> None:
        logger.info(f"[node suspend] {node.id} @ flow {flow.flow_id}")

    def on_node_resume(self, flow, node, **kwargs) -> None:
        logger.info(f"[node resume] {node.id} @ flow {flow.flow_id}")
