"""
plaita.core.context — ExecutionContext for flow runtime state management.

Extracted from FlowExecution: state storage, variable scoping ($INPUT,
$NODE, $GLOBAL, $PARENT, $ENV), parent-child chain, env var filtering,
and expression evaluation delegation.
"""

from __future__ import annotations

import os
import uuid
import logging
import threading
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from plaita.core import types
from plaita.core.expression import ExpressionEvaluator

if TYPE_CHECKING:
    from plaita.node.basic import Node

logger = logging.getLogger("plaita.core.context")

_SENSITIVE_ENV_PREFIXES = (
    "AWS_SECRET", "AWS_SESSION", "DATABASE_", "DB_PASSWORD",
    "SECRET", "TOKEN", "API_KEY", "PRIVATE_KEY", "CREDENTIAL",
    "PASSWORD", "PASS_", "REDIS_PASSWORD",
)


def _safe_environment() -> Dict[str, str]:
    """Return a filtered copy of ``os.environ`` with sensitive keys removed.

    Used by ``ExecutionContext.clean`` to populate ``$ENV``.  Kept at module
    level so the class body stays small and the rule is unit-testable in
    isolation.
    """
    return {
        k: v for k, v in os.environ.items()
        if not any(k.upper().startswith(p) for p in _SENSITIVE_ENV_PREFIXES)
    }


def _coerce_input_value(in_format, args: tuple, kwargs: dict) -> Any:
    """Resolve flow invocation arguments into the value stored at ``$INPUT``.

    Used by both the public ``Flow.run`` path and internal child-flow
    invocations (InlineFlow / parallel branches / loops), which legitimately
    pass a single non-dict value as the child's ``$INPUT``.

    - ``run(key=value, ...)``         → ``{key: value, ...}``
    - ``run({...})`` / ``run(scalar)``→ that value as-is (single positional)
    - ``run()``                       → ``{}``
    - multiple positional args        → ``TypeError`` (array splat no longer
      supported; wrap in a dict, e.g. ``run({"items": [...]})``)
    """
    if kwargs:
        if args:
            if len(args) == 1:
                if isinstance(args[0], dict):
                    return {**args[0], **kwargs}
                raise TypeError(
                    "flow.run() cannot mix a non-dict positional argument with "
                    f"keyword arguments; got args={args!r}"
                )
            raise TypeError(
                "flow.run() accepts a single dict and/or keyword arguments; "
                f"got positional args={args!r}"
            )
        return kwargs

    if not args:
        return {}

    if len(args) == 1:
        return args[0]

    raise TypeError(
        "flow.run() accepts a single dict and/or keyword arguments; "
        f"got positional args={args!r}. Wrap scalar/array input in a dict, "
        "e.g. flow.run({'items': [...]}) and reference $INPUT.items."
    )

# 依赖反转: core 不直接 import plaita.event, 而是持有一个由上层 (plaita 顶层包)
# 注册的 "默认 event bus provider" 可调用对象。这样 core → event 的反向依赖
# 被消除, 同时保留 "未注入 event_bus 时自动取默认总线" 的旧行为。
_default_event_bus_provider: Optional["Callable[[], Any]"] = None


def set_default_event_bus_provider(provider) -> None:
    """Register the callable used to lazily obtain a default event bus.

    Intended to be called once by the top-level ``plaita`` package (which may
    depend on ``plaita.event``).  Keeping this indirection means ``plaita.core``
    never imports ``plaita.event`` — preserving the ``core → event`` layering.
    """
    global _default_event_bus_provider
    _default_event_bus_provider = provider


class ExecutionContext:
    """Manages runtime state for a flow execution."""

    def __init__(
        self,
        parent: Optional[ExecutionContext] = None,
        *,
        express_prefix: str = "$",
        express_input_name: str = "INPUT",
        express_parent_name: str = "PARENT",
        express_node_name: str = "NODE",
        express_global_name: str = "GLOBAL",
        express_environment_variable: str = "ENV",
        event_bus=None,
        evaluator: Optional[ExpressionEvaluator] = None,
    ) -> None:
        self._context: Dict[str, Any] = {}
        self.parent = parent
        self.event_bus = event_bus
        # 协作取消令牌: 超时/取消时由 NodeRunner 设置, 节点可 poll 此事件提前退出。
        # 注: 并行分支经 get_child_execution 拿到独立子 context (各自 _context dict),
        # 不共享父级 $NODE, 故 update_node_result 无需加锁; 加 threading.Lock 反而会
        # 破坏 process 模式对 execution 的 pickle (见 test_concurrent)。
        self.cancel_event = threading.Event()
        self.express_prefix = express_prefix
        self.express_input_name = express_input_name
        self.express_parent_name = express_parent_name
        self.express_node_name = express_node_name
        self.express_global_name = express_global_name
        self.express_environment_variable = express_environment_variable
        # Eagerly generate an execution ID so it is available before clean() or
        # setup_flow() is called.  clean() replaces this with a fresh ID for
        # each new run; from_dict() overwrites it with the persisted value.
        self._context[f"{express_prefix}EXECUTION_ID"] = uuid.uuid4().hex
        self._evaluator = evaluator or ExpressionEvaluator()

    # -- dict-like access for backward compat (FlowExecution.context) --

    @property
    def context(self) -> Dict[str, Any]:
        return self._context

    @context.setter
    def context(self, value: Dict[str, Any]) -> None:
        self._context = value

    # -- state management --

    def set_state(self, key: str, value: Any) -> None:
        self._context[key] = value

    def get_state(self, key: str, default: Any = None) -> Any:
        return self._context.get(key, default)

    def clean(self) -> None:
        """Reset context and populate safe environment variables.

        Also recreates ``cancel_event`` so that a cancel signal set during a
        previous run does not bleed into the next one.  Note: cooperative
        cancellation is thread-local and best-effort; cross-process cancel
        is not supported (a child process gets a fresh, unset event — see
        ``__getstate__``).

        A fresh ``execution_id`` is generated here (not lazily on first read)
        so that the ID is stable for the entire run and does not change if
        ``execution_id`` is read multiple times or before ``setup_flow``.
        """
        self._context = {}
        self.cancel_event = threading.Event()
        self.set_state(f"{self.express_prefix}EXECUTION_ID", uuid.uuid4().hex)
        self.set_state(f"{self.express_prefix}{self.express_environment_variable}", _safe_environment())

    def __getstate__(self):
        # threading.Event 不可 pickle; 进程模式下子进程会得到一个全新的(未触发)事件,
        # 跨进程取消本就不适用。
        state = self.__dict__.copy()
        state.pop("cancel_event", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if not getattr(self, "cancel_event", None):
            self.cancel_event = threading.Event()

    # -- flow setup --

    def setup_flow(self, flow, args: tuple, kwargs: dict) -> None:
        """Initialize context with flow-level variables."""
        context_value = _coerce_input_value(flow.input_type, args, kwargs)
        self.set_state(f"{self.express_prefix}{self.express_input_name}", context_value)
        self.set_state(
            f"{self.express_prefix}{self.express_parent_name}",
            self.parent.context if self.parent else {},
        )
        global_context = flow.global_context.copy() if flow.global_context else {}
        global_context.update({"flow_id": flow.flow_id})
        self.set_state(f"{self.express_prefix}{self.express_global_name}", global_context)
        self.set_state("EXPRESS_PREFIX", self.express_prefix)
        self.set_state(f"{self.express_prefix}FLOW_ID", flow.flow_id)

    # -- expression evaluation --

    def evaluate(self, value: Any) -> Any:
        """Recursively resolve expressions via the ExpressionEvaluator."""
        if isinstance(value, list):
            return [self.evaluate(item) for item in value]
        if isinstance(value, dict):
            return {k: self.evaluate(v) for k, v in value.items()}
        return self._evaluate_string(value)

    def _evaluate_string(self, value: Any) -> Any:
        result = self._evaluator.evaluate(value, self._context, self.express_prefix)
        if (
            result is None
            and isinstance(value, str)
            and value.startswith(self.express_prefix)
            and self.parent is not None
        ):
            return self.parent.evaluate(value)
        return result

    # -- global variable lookup --

    def get_global_variable(self, key: str, default: Any = None) -> Any:
        global_dict = self.get_state(f"{self.express_prefix}{self.express_global_name}", {})
        if key in global_dict:
            return global_dict[key]
        if self.parent:
            return self.parent.get_global_variable(key, default)
        return default

    # -- node result tracking --

    def update_node_result(self, node, result: Any) -> None:
        node_results = self.get_state(f"{self.express_prefix}{self.express_node_name}", {})
        node_results[node.id] = result
        self.set_state(f"{self.express_prefix}{self.express_node_name}", node_results)

    # -- execution id --

    @property
    def execution_id(self) -> str:
        """Return the current execution ID (pure read-only).

        The ID is initialised in ``clean()`` for a fresh run, or restored from
        the persisted context dict in distributed resume scenarios.  Child
        contexts that were never ``clean()``-ed may not carry their own ID;
        callers needing the root-flow ID should access the root context.
        """
        return self.get_state(f"{self.express_prefix}EXECUTION_ID", "")

    # -- event bus --

    def get_or_create_event_bus(self):
        if self.event_bus:
            return self.event_bus
        if self.parent and self.parent.event_bus:
            self.event_bus = self.parent.event_bus
            return self.event_bus
        provider = _default_event_bus_provider
        if provider is not None:
            try:
                self.event_bus = provider()
            except Exception:
                logger.warning("Unable to get default event bus")
                self.event_bus = None
        else:
            self.event_bus = None
        return self.event_bus

    # -- child context --

    def child(self) -> ExecutionContext:
        return ExecutionContext(
            parent=self,
            express_prefix=self.express_prefix,
            express_input_name=self.express_input_name,
            express_parent_name=self.express_parent_name,
            express_node_name=self.express_node_name,
            express_global_name=self.express_global_name,
            express_environment_variable=self.express_environment_variable,
            event_bus=self.event_bus,
            evaluator=self._evaluator,
        )

    # -- serialization for distributed execution --

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._context)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs) -> ExecutionContext:
        ctx = cls(**kwargs)
        ctx._context = dict(data)
        return ctx
