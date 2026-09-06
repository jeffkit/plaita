"""
plaita.node — Node definitions and the NodeRegistry.

The ``NodeRegistry`` class provides a scoped, dict-backed registry for
node types.  A module-level default registry is created at import time
and populated with all built-in nodes plus any nodes discovered via
``importlib.metadata`` entry_points (group ``plaita.nodes``).

Backward-compatible helpers (``node_register``, ``parse_node``) emit
``DeprecationWarning``; the ``nodes`` dict proxy is still silent.
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
from .loop import BaseCollectionNode, Filter, Find, Loop, Map, Reduce, While
from .mock import Mock
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
    While,
    ReferenceFlow,
    # CodeNode is intentionally excluded from the default registry.
    # It executes arbitrary user-supplied code (Python exec / PyExecJS JS) without
    # a sandbox. To opt in, call register_code_node() or
    # get_default_registry().register(CodeNode) explicitly.
    Parallel,
    HTTP,
    Mock,
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
        """Register a node class. Returns *node_cls* so it can be used as a decorator.

        覆盖已注册的**不同**类时发 warning——历史上静默顶掉（包括劫持内置
        ``start``/``end`` 等类型），排查时完全无线索。同类型重复注册（如
        importlib 重复加载场景）不告警。
        """
        existing = self._nodes.get(node_cls.node_type)
        if existing is not None and existing is not node_cls:
            _logger.warning(
                "NodeRegistry.register: node_type %r is being overridden: "
                "%s -> %s. If this is unintentional, pick a different node_type.",
                node_cls.node_type,
                f"{existing.__module__}.{existing.__qualname__}",
                f"{node_cls.__module__}.{node_cls.__qualname__}",
            )
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
        # entry_points(group=...) requires Python 3.10+.
        # Python 3.9 returns a dict-like SelectableGroups from entry_points().
        # Test mocks may patch entry_points to return a flat list directly.
        import sys
        if sys.version_info >= (3, 10):
            eps = entry_points(group="plaita.nodes")
        else:
            raw = entry_points()
            if hasattr(raw, "get"):
                # Python 3.9 SelectableGroups / dict API
                eps = raw.get("plaita.nodes", [])
            else:
                # Flat iterable (e.g. test mock returns [ep, ...] directly)
                eps = raw
        for ep in eps:
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
# 标记默认 registry 是否经 ``init_default_registry`` 显式初始化。``False`` 时
# ``get_default_registry()`` 的首次调用走"隐式 discover"并 debug 提示, 鼓励
# 用户在启动脚本里显式调一次 ``init_default_registry``——把"进程级单例可变状态"
# 从隐式 import 期副作用变成显式、可读、可复现的启动步骤。
_default_registry_explicitly_initialized = False


def init_default_registry(*extra_nodes: Type[Node], auto_discover: bool = True) -> NodeRegistry:
    """显式 (重新) 初始化进程级默认 ``NodeRegistry``。

    历史上默认 registry 是模块级 ``_default_registry = NodeRegistry()`` 在 import
    期静默创建, 插件发现 (``plaita.nodes`` entry points) 在首次
    ``get_default_registry()`` 时隐式触发——"隐式可变单例 + import 期副作用"是
    典型反模式: 测试间状态污染、插件加载顺序敏感、复现困难。本入口把它变成
    **显式启动步骤**:

    - 清空并重建默认 registry (重注册内置节点);
    - ``auto_discover=True`` 时扫描 ``plaita.nodes`` entry points 装载插件节点;
    - 把 ``extra_nodes`` (节点类) 逐个注册进去;
    - 标记为"已显式初始化", 之后 ``get_default_registry()`` 不再发隐式初始化提示。

    建议在应用启动脚本顶部调用一次::

        from plaita.node import init_default_registry, register_code_node
        init_default_registry()              # 显式装载内置 + 插件节点
        register_code_node(default_backend="docker")  # 按需 opt-in CodeNode

    Args:
        *extra_nodes: 额外要注册的 ``Node`` 子类 (例如自定义节点)。
        auto_discover: 是否扫描 entry points 装载插件节点, 默认 True。

    Returns:
        初始化后的默认 ``NodeRegistry`` (即 ``get_default_registry()`` 将返回者)。
    """
    global _default_registry_explicitly_initialized
    reg = _default_registry
    reg._nodes.clear()
    reg._discovered = False
    reg._register_builtins()
    if auto_discover:
        reg.discover()
    for node_cls in extra_nodes:
        reg.register(node_cls)
    _default_registry_explicitly_initialized = True
    _logger.debug(
        "init_default_registry: %d node types registered (%s)",
        len(reg._nodes), sorted(reg._nodes.keys()),
    )
    return reg


def get_default_registry() -> NodeRegistry:
    """Return the process-wide default ``NodeRegistry``.

    Plugin discovery via ``plaita.nodes`` entry points happens lazily on the
    first call, not at import time, so importing ``plaita.node`` does not pull
    in optional server/plugin dependencies.

    若未先调 ``init_default_registry``, 首次调用走"隐式 discover"并 ``debug``
    提示——建议在启动脚本显式调 ``init_default_registry()`` 一次, 把进程级
    单例状态变成显式、可复现的启动步骤。
    """
    global _default_registry_explicitly_initialized
    if not _default_registry_explicitly_initialized and not _default_registry._discovered:
        _logger.debug(
            "get_default_registry() called without explicit init_default_registry(); "
            "doing implicit plugin discovery. Call init_default_registry() at startup "
            "for an explicit, reproducible node set."
        )
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


def register_code_node(registry: Optional[NodeRegistry] = None,
                       default_backend: Optional[str] = None,
                       allowed_backends: Optional[list] = None) -> None:
    """Register :class:`CodeNode` for use in flows.

    ``CodeNode`` executes **arbitrary user-supplied code** (Python ``exec``
    and/or PyExecJS JS).  It is therefore excluded from the default registry
    and must be enabled explicitly.

    Args:
        registry: The :class:`NodeRegistry` to register into.  If *None*, the
            process-wide default registry is used.
        default_backend: Python 沙箱后端默认值, 写入 ``code._DEFAULT_SANDBOX_BACKEND``。
            未指定时沿用模块默认 (0.5.0 起 ``"docker"``)。
        allowed_backends: 沙箱后端白名单（2026-09 安全评审 P0）。设置后，流程 JSON
            里声明的 ``sandbox_backend`` 不在白名单内即**解析期硬失败**——没有它，
            流程作者可把运营者选定的默认后端逐节点覆盖为 ``"unsafe"``（宿主任意
            代码执行）。多租户部署务必显式设置，且不要发放 ``unsafe``/``subprocess``::

                register_code_node(default_backend="docker",
                                   allowed_backends=("docker",))

            **安全 gate**: 若生效后端 (``default_backend`` 或模块默认) 为 ``"docker"``
            但当前环境 docker daemon 不可用, **拒绝注册**并抛 ``RuntimeError``, 指明
            三种降级路径——装 Docker / ``default_backend="subprocess"`` /
            ``default_backend="unsafe"``。不允许静默降级到 ``"restricted"`` (其 AST
            沙箱有已知绕过向量, 不该作为"对用户透明"的兜底)。

    Example::

        from plaita.node import register_code_node
        register_code_node()                      # 默认 docker, 需 daemon
        register_code_node(default_backend="subprocess")  # 半信任, 无需 docker

    .. warning::
        Only call this if you trust all flow definitions that will be executed
        in this process. A ``CodeNode`` in a flow JSON allows any code the flow
        author chooses to run; ``"unsafe"`` 后端连文件系统/网络访问都不限制。
    """
    from . import code as _code_module
    effective = default_backend if default_backend is not None else _code_module._DEFAULT_SANDBOX_BACKEND
    if effective == "docker" and not _code_module._docker_available():
        raise RuntimeError(
            "register_code_node: default sandbox_backend is 'docker' but the "
            "Docker daemon is not available. Either install/start Docker, or "
            "explicitly choose a weaker backend: "
            "register_code_node(default_backend='subprocess') (process-level, "
            "no FS/network isolation) or "
            "register_code_node(default_backend='unsafe') (no sandbox, only "
            "for fully-trusted flow authors). 'restricted' is no longer the "
            "default because RestrictedPython has known AST bypass vectors."
        )
    if default_backend is not None:
        _code_module._DEFAULT_SANDBOX_BACKEND = default_backend
    if allowed_backends is not None:
        invalid = [b for b in allowed_backends if b not in _code_module._PYTHON_BACKENDS]
        if invalid:
            raise ValueError(
                f"register_code_node: unknown sandbox backend(s) {invalid}; "
                f"supported: {sorted(_code_module._PYTHON_BACKENDS)}"
            )
        _code_module._ALLOWED_SANDBOX_BACKENDS = frozenset(allowed_backends)
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
