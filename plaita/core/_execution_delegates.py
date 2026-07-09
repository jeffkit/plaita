"""Explicit ExecutionContext delegates for the FlowExecution facade.

Kept in a separate module so ``FlowExecution`` itself stays under the SC-003
LOC budget. Nodes still see the same named properties/methods on
``execution`` — there is no ``__getattr__`` catch-all.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from plaita.core.strategies import _StateView


class _ExecutionContextDelegates:
    """Named property/method surface that forwards to ``self._ctx``.

    Subclasses must set ``self._ctx`` to an ``ExecutionContext`` before any
    of these accessors are used.
    """

    # -- context fields --

    @property
    def context(self) -> Dict[str, Any]:
        return self._ctx.context

    @context.setter
    def context(self, value: Dict[str, Any]) -> None:
        self._ctx.context = value

    @property
    def execution_id(self) -> str:
        return self._ctx.execution_id

    @property
    def event_bus(self):
        return self._ctx.event_bus

    @event_bus.setter
    def event_bus(self, value) -> None:
        self._ctx.event_bus = value

    @property
    def cancel_event(self):
        return self._ctx.cancel_event

    @property
    def express_prefix(self) -> str:
        return self._ctx.express_prefix

    @express_prefix.setter
    def express_prefix(self, value: str) -> None:
        self._ctx.express_prefix = value

    @property
    def express_input_name(self) -> str:
        return self._ctx.express_input_name

    @express_input_name.setter
    def express_input_name(self, value: str) -> None:
        self._ctx.express_input_name = value

    @property
    def express_parent_name(self) -> str:
        return self._ctx.express_parent_name

    @express_parent_name.setter
    def express_parent_name(self, value: str) -> None:
        self._ctx.express_parent_name = value

    @property
    def express_node_name(self) -> str:
        return self._ctx.express_node_name

    @express_node_name.setter
    def express_node_name(self, value: str) -> None:
        self._ctx.express_node_name = value

    @property
    def express_global_name(self) -> str:
        return self._ctx.express_global_name

    @express_global_name.setter
    def express_global_name(self, value: str) -> None:
        self._ctx.express_global_name = value

    @property
    def express_environment_variable(self) -> str:
        return self._ctx.express_environment_variable

    @express_environment_variable.setter
    def express_environment_variable(self, value: str) -> None:
        self._ctx.express_environment_variable = value

    @property
    def state(self) -> _StateView:
        """Typed, ``None``-normalized view over context state.

        ``execution.state.flow_id`` / ``.last_node_id`` / ``.last_branch``.
        Replaces the former ``execution.flow_id`` etc. bare-attribute
        pass-throughs (break change — see MIGRATION.md).
        """
        return _StateView(self._ctx)

    # -- state helpers --

    def set_state(self, key: str, value: Any) -> None:
        return self._ctx.set_state(key, value)

    def get_state(self, key: str, default: Any = None) -> Any:
        return self._ctx.get_state(key, default)

    def evaluate(self, value: Any) -> Any:
        return self._ctx.evaluate(value)

    def get_global_variable(self, key: str, default: Any = None) -> Any:
        return self._ctx.get_global_variable(key, default)

    def get_or_create_event_bus(self):
        return self._ctx.get_or_create_event_bus()

    def update_node_result(self, node, result: Any) -> None:
        return self._ctx.update_node_result(node, result)

    def clean(self) -> None:
        return self._ctx.clean()

    def setup_flow(self, flow, args: tuple, kwargs: dict) -> None:
        return self._ctx.setup_flow(flow, args, kwargs)
