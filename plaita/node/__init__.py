"""
plaita.node — Node definitions and the NodeRegistry.

The ``NodeRegistry`` class provides a scoped, dict-backed registry for
node types.  A module-level default registry is created at import time
and populated with all built-in nodes plus any nodes discovered via
``importlib.metadata`` entry_points (group ``plaita.nodes``).

Backward-compatible helpers (``nodes``, ``node_register``, ``parse_node``)
are preserved but emit ``DeprecationWarning``.
"""

from __future__ import annotations

import logging
import warnings
from importlib.metadata import entry_points
from typing import Dict, List, Optional, Type

from .assignment import Assignment
from .basic import Node
from .child import InlineFlow, ReferenceFlow
from .code import CodeNode
from .concurrent import Parallel
from .decide import Bool, Switch, SwitchLegacy
from .end import End
from .event_node import EventNode
from .loop import BaseCollectionNode, Filter, Find, Loop, Map, Reduce
from .start import Start
from .http import HTTP

_logger = logging.getLogger(__name__)

_BUILTIN_NODES: list[Type[Node]] = [
    Start,
    End,
    Switch,
    Assignment,
    Bool,
    SwitchLegacy,
    InlineFlow,
    Loop,
    Map,
    Filter,
    Find,
    Reduce,
    ReferenceFlow,
    # CodeNode is intentionally excluded from the default registry.
    # It executes arbitrary user-supplied code (Python exec / PyExecJS JS) without
    # a sandbox. To opt in, call register_code_node() or
    # get_default_registry().register(CodeNode) explicitly.
    Parallel,
    HTTP,
    EventNode,
    # BaseCollectionNode is intentionally excluded: it is abstract and has no
    # node_type, so it should never appear in the registry directly.
]


class NodeRegistry:
    """Scoped registry for Plaita node types.

    Args:
        auto_discover: If True, scan ``importlib.metadata`` entry_points
            in the ``plaita.nodes`` group to load plugin nodes.
        parent: If supplied, the new registry is initialised with a copy
            of the parent's registered nodes.
    """

    def __init__(
        self,
        *,
        auto_discover: bool = False,
        parent: Optional[NodeRegistry] = None,
    ) -> None:
        self._nodes: Dict[str, Type[Node]] = {}
        self._discovered = False
        if parent is not None:
            self._nodes.update(parent._nodes)
        self._register_builtins()
        # 默认不在构造期扫描 entry_points, 避免 import plaita.node 时耦合到
        # server 等插件依赖。需要插件发现时显式传 auto_discover=True,
        # 或调用 discover() / get_default_registry()。
        if auto_discover:
            self.discover()

    # -- public API ---------------------------------------------------------

    def register(self, node_cls: Type[Node]) -> Type[Node]:
        """Register a node class. Returns *node_cls* so it can be used as a decorator."""
        self._nodes[node_cls.node_type] = node_cls
        return node_cls

    def unregister(self, node_type: str) -> None:
        self._nodes.pop(node_type, None)

    def get(self, node_type: str) -> Optional[Type[Node]]:
        return self._nodes.get(node_type)

    def parse_node(self, node_dict) -> Node:
        """Create a Node instance from a dict (or return an existing Node)."""
        if isinstance(node_dict, Node):
            return node_dict

        node_type = node_dict.get("type", None)
        if not node_type:
            raise RuntimeError(f"not specific node type: {node_dict}")
        node_cls = self._nodes.get(node_type)
        if not node_cls:
            hint = ""
            if node_type == "code":
                hint = (
                    " CodeNode was moved out of the default registry in 0.4.0; "
                    "call register_code_node() at startup to opt in."
                )
            raise RuntimeError(f"unRecognized node type: {node_type}.{hint}")
        content = node_dict.copy()
        del content["type"]
        return node_cls.model_validate(content)

    def list_types(self) -> List[str]:
        return list(self._nodes.keys())

    def copy(self) -> NodeRegistry:
        """Return an independent shallow copy of this registry."""
        new = NodeRegistry.__new__(NodeRegistry)
        new._nodes = dict(self._nodes)
        return new

    def __contains__(self, node_type: str) -> bool:
        return node_type in self._nodes

    def __len__(self) -> int:
        return len(self._nodes)

    # -- internal -----------------------------------------------------------

    def _register_builtins(self) -> None:
        for cls in _BUILTIN_NODES:
            self._nodes[cls.node_type] = cls

    def _discover_entry_points(self) -> None:
        for ep in entry_points(group="plaita.nodes"):
            try:
                node_cls = ep.load()
                self._nodes[node_cls.node_type] = node_cls
            except Exception:
                _logger.warning("Failed to load node plugin: %s", ep.name)

    def discover(self) -> "NodeRegistry":
        """Lazily discover node plugins via ``plaita.nodes`` entry points.

        Idempotent: only scans on the first call for this registry instance.
        """
        if self._discovered:
            return self
        self._discovered = True
        self._discover_entry_points()
        return self


# ---------------------------------------------------------------------------
# Default global registry
# ---------------------------------------------------------------------------

_default_registry = NodeRegistry()


def get_default_registry() -> NodeRegistry:
    """Return the process-wide default ``NodeRegistry``.

    Plugin discovery via ``plaita.nodes`` entry points happens lazily on the
    first call, not at import time, so importing ``plaita.node`` does not pull
    in optional server/plugin dependencies.
    """
    _default_registry.discover()
    return _default_registry


# ---------------------------------------------------------------------------
# Backward-compatible dict proxy
# ---------------------------------------------------------------------------

class _RegistryDictProxy:
    """Dict-like wrapper around a ``NodeRegistry`` for backward compatibility."""

    def __init__(self, registry: NodeRegistry) -> None:
        self._registry = registry

    def __getitem__(self, key: str) -> Type[Node]:
        val = self._registry.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: str, value: Type[Node]) -> None:
        self._registry._nodes[key] = value

    def __delitem__(self, key: str) -> None:
        del self._registry._nodes[key]

    def __contains__(self, key: object) -> bool:
        return key in self._registry

    def __len__(self) -> int:
        return len(self._registry)

    def __iter__(self):
        return iter(self._registry._nodes)

    def get(self, key: str, default=None):
        val = self._registry.get(key)
        return val if val is not None else default

    def keys(self):
        return self._registry._nodes.keys()

    def values(self):
        return self._registry._nodes.values()

    def items(self):
        return self._registry._nodes.items()

    def __repr__(self) -> str:
        return f"_RegistryDictProxy({dict(self._registry._nodes)})"


nodes = _RegistryDictProxy(_default_registry)


# ---------------------------------------------------------------------------
# CodeNode opt-in helper
# ---------------------------------------------------------------------------


def register_code_node(registry: Optional[NodeRegistry] = None) -> None:
    """Register :class:`CodeNode` for use in flows.

    ``CodeNode`` executes **arbitrary user-supplied code** (Python ``exec``
    and/or PyExecJS JS) without a sandbox.  It is therefore excluded from the
    default registry and must be enabled explicitly.

    Args:
        registry: The :class:`NodeRegistry` to register into.  If *None*, the
            process-wide default registry is used.

    Example::

        from plaita.node import register_code_node
        register_code_node()   # enables CodeNode in the default registry

    .. warning::
        Only call this if you trust all flow definitions that will be executed
        in this process. A ``CodeNode`` in a flow JSON allows any code the flow
        author chooses to run, including arbitrary file-system and network
        access.
    """
    target = registry if registry is not None else get_default_registry()
    target.register(CodeNode)


# ---------------------------------------------------------------------------
# Deprecated module-level helpers
# ---------------------------------------------------------------------------

def node_register(node_cls: Type[Node]) -> None:
    """Register a node class on the default registry.

    .. deprecated::
        Use ``get_default_registry().register(node_cls)`` instead.
    """
    warnings.warn(
        "node_register() is deprecated. "
        "Use get_default_registry().register(node_cls) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    _default_registry.register(node_cls)


def parse_node(node_dict) -> Node:
    """Parse a node dict using the default registry.

    .. deprecated::
        Use ``get_default_registry().parse_node(node_dict)`` instead.
    """
    warnings.warn(
        "Module-level parse_node() is deprecated. "
        "Use get_default_registry().parse_node(node_dict) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _default_registry.parse_node(node_dict)
