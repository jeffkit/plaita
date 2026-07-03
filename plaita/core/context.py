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
from plaita.core.state import CheckpointState

if TYPE_CHECKING:
    from plaita.node.basic import Node

logger = logging.getLogger("plaita.core.context")

# 历史遗留：环境变量"敏感前缀黑名单"。**仅作为深度防御**——当 Flow 显式
# ``expose_env`` allowlist 命中某个看起来敏感的 key 时，这里仍会拦下，避免
# 用户无意中把 ``expose_env=["AWS_SECRET_ACCESS_KEY"]`` 写出来。
#
# 默认 ``$ENV`` 已经不再走"全 os.environ 减去黑名单"模型——见 ``expose_env``
# 字段说明。新代码请显式声明需要的环境变量。
_SENSITIVE_ENV_PREFIXES = (
    "AWS_SECRET", "AWS_SESSION", "DATABASE_", "DB_PASSWORD",
    "SECRET", "TOKEN", "API_KEY", "PRIVATE_KEY", "CREDENTIAL",
    "PASSWORD", "PASS_", "REDIS_PASSWORD",
)


def _safe_environment(allowlist: Optional[List[str]] = None) -> Dict[str, str]:
    """Return the ``$ENV`` dict for the given expose list.

    Security model (2026-07 refactor):

    - **Default (no allowlist)**: returns ``{}``. ``$ENV`` is empty unless the
      flow explicitly opts in. Previously this returned ``os.environ`` minus a
      prefix blacklist — that was a failed-security pattern: any secret whose
      name did not match the blacklist (e.g. ``STRIPE_KEY``, ``OPENAI_API_KEY``,
      ``PG_CONN``) was silently exposed to flow expressions and serialized
      into distributed checkpoints via ``to_dict()``.
    - **With allowlist**: returns only the listed keys that actually exist in
      ``os.environ``. Then runs the sensitive-prefix blacklist as a second
      defense layer and refuses to emit matches (logs a warning instead).
      ``KeyError``-style misses are silently dropped — typos in ``expose_env``
      should not crash the flow.
    """
    if not allowlist:
        return {}
    exposed: Dict[str, str] = {}
    for key in allowlist:
        if key in os.environ:
            exposed[key] = os.environ[key]
    leaked = [k for k in exposed
              if any(k.upper().startswith(p) for p in _SENSITIVE_ENV_PREFIXES)]
    for k in leaked:
        logger.warning(
            "expose_env lists a sensitive-looking key %r; dropping it. "
            "If this is intentional, rename the env var or override "
            "_safe_environment locally.", k,
        )
        exposed.pop(k, None)
    return exposed


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

# 历史上为避开 ``core → event`` 反向依赖, 这里曾用一个模块级可变全局
# ``_default_event_bus_provider`` + ``set_default_event_bus_provider`` 注入。
# 实际整个项目就一个 provider, 这套机制换来隐式全局 + 类型丢失 + 测试必须 mock
# 的代价。改为 ``core`` 直接 lazily import ``plaita.event.get_default_event_bus``
# 作为 fallback——core 仍不持有 EventBus 实例, 但承认 ``event`` 层是它的下游
# 协作者。Spring/Django 都允许这种 import, 收益 (类型 + 可测性) 远大于"分层纯洁"。
def _resolve_default_event_bus():
    """Lazily fetch the default EventBus from the event layer.

    Imported inside the function so the ``core`` package doesn't drag in
    ``plaita.event`` (and its optional redis/sqlalchemy deps) at import time.
    """
    try:
        from plaita.event import get_default_event_bus
        return get_default_event_bus()
    except Exception:
        logger.warning("Unable to get default event bus", exc_info=True)
        return None


def _resolve_expression_string(evaluator, state, prefix, parent, value):
    """Evaluate a single (string-or-not) value, with parent-context fallback.

    If the expression resolves to ``None`` and the value is a prefix string,
    recurse through ``parent.evaluate`` — the historical semantics for
    unresolved variables in child flows.
    """
    result = evaluator.evaluate(value, state, prefix)
    if (result is None and isinstance(value, str)
            and value.startswith(prefix) and parent is not None):
        return parent.evaluate(value)
    return result


class ExecutionContext:
    """Runtime state facade for a flow execution.

    Owns a ``CheckpointState`` (the typed, distributed-checkpoint model in
    ``plaita.core.state``) plus the in-memory-only pieces that never enter a
    checkpoint: the parent chain, event bus, cancel event, evaluator, and the
    ``expose_env`` allowlist. State read/write delegates to ``CheckpointState``
    so the magic-string key schema lives in one place.
    """

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
        expose_env: Optional[List[str]] = None,
    ) -> None:
        self.parent = parent
        self.event_bus = event_bus
        # $ENV allowlist (None/empty => default-empty $ENV). Flow layer pipes
        # its expose_env down; clean() does NOT reset this — it's config.
        self.expose_env = list(expose_env) if expose_env else []
        # cancel_event is thread-local & best-effort; cross-process cancel is
        # NOT supported (child gets a fresh unset event — see __getstate__).
        self.cancel_event = threading.Event()
        self.express_prefix = express_prefix
        self.express_input_name = express_input_name
        self.express_parent_name = express_parent_name
        self.express_node_name = express_node_name
        self.express_global_name = express_global_name
        self.express_environment_variable = express_environment_variable
        self._state = CheckpointState(**self._state_kwargs())
        # Eager execution id (available before clean/setup_flow; clean replaces
        # per run, from_dict overwrites from persist).
        self._state[f"{express_prefix}EXECUTION_ID"] = uuid.uuid4().hex
        self._evaluator = evaluator or ExpressionEvaluator()

    def _state_kwargs(self) -> dict:
        return dict(
            prefix=self.express_prefix,
            input_name=self.express_input_name,
            parent_name=self.express_parent_name,
            node_name=self.express_node_name,
            global_name=self.express_global_name,
            env_name=self.express_environment_variable,
        )

    # -- dict-like access (FlowExecution.context) --
    # ``context`` is the live CheckpointState (dict-like over prefixed keys);
    # CheckpointState.__eq__ vs dict keeps assertEqual(ctx, {...}) working.
    # The setter accepts a plain dict (storage format) and parses it back.

    @property
    def context(self) -> CheckpointState:
        return self._state

    @context.setter
    def context(self, value: Dict[str, Any]) -> None:
        self._state = CheckpointState.from_checkpoint_dict(value, **self._state_kwargs())

    def set_state(self, key: str, value: Any) -> None:
        self._state[key] = value

    def get_state(self, key: str, default: Any = None) -> Any:
        return self._state.get(key, default)

    def clean(self) -> None:
        """Reset state to a fresh run (new execution id + exposed env snapshot) and recreate cancel_event."""
        self._state = CheckpointState.fresh(
            **self._state_kwargs(),
            execution_id=uuid.uuid4().hex,
            env=_safe_environment(self.expose_env),
        )
        self.cancel_event = threading.Event()

    def __getstate__(self):
        # threading.Event isn't picklable; child gets a fresh unset event via
        # __setstate__. Parent cancel signal doesn't cross processes.
        state = self.__dict__.copy()
        state.pop("cancel_event", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if not getattr(self, "cancel_event", None):
            self.cancel_event = threading.Event()

    def setup_flow(self, flow, args: tuple, kwargs: dict) -> None:
        """Populate flow-level state ($INPUT/$PARENT/$GLOBAL/$FLOW_ID/$ENV) + sync flow.expose_env."""
        flow_allowlist = list(getattr(flow, "expose_env", None) or [])
        if flow_allowlist != self.expose_env:
            self.expose_env = flow_allowlist
        global_context = (flow.global_context.copy() if flow.global_context else {})
        global_context.update({"flow_id": flow.flow_id})
        self._state.setup_flow(
            input_value=_coerce_input_value(flow.input_type, args, kwargs),
            # $PARENT: plain-dict snapshot — _get_attr takes getattr() on
            # __dict__-bearing objects, so a live CheckpointState can't resolve $INPUT.
            parent_context=dict(self.parent.context) if self.parent else {},
            global_context=global_context,
            flow_id=flow.flow_id,
            env=_safe_environment(self.expose_env),
        )

    def evaluate(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self.evaluate(item) for item in value]
        if isinstance(value, dict):
            return {k: self.evaluate(v) for k, v in value.items()}
        return self._evaluate_string(value)

    def _evaluate_string(self, value: Any) -> Any:
        return _resolve_expression_string(
            self._evaluator, self._state, self.express_prefix, self.parent, value,
        )

    def get_global_variable(self, key: str, default: Any = None) -> Any:
        global_dict = self.get_state(f"{self.express_prefix}{self.express_global_name}", {})
        if key in global_dict:
            return global_dict[key]
        if self.parent:
            return self.parent.get_global_variable(key, default)
        return default

    def update_node_result(self, node, result: Any) -> None:
        self._state.update_node_result(node.id, result)

    @property
    def execution_id(self) -> str:
        return self.get_state(f"{self.express_prefix}EXECUTION_ID", "")

    # -- typed system-state accessors: CheckpointState owns field<->key mapping,
    # so to_dict/from_dict stay forward/backward-compatible across upgrades.

    @property
    def last_node_id(self) -> Optional[str]:
        return self.get_state(f"{self.express_prefix}LAST_NODE")

    @last_node_id.setter
    def last_node_id(self, value: Optional[str]) -> None:
        self.set_state(f"{self.express_prefix}LAST_NODE", value)

    @property
    def last_branch(self) -> Optional[str]:
        return self.get_state(f"{self.express_prefix}BRANCH")

    @last_branch.setter
    def last_branch(self, value: Optional[str]) -> None:
        self.set_state(f"{self.express_prefix}BRANCH", value)

    @property
    def flow_id(self) -> Optional[str]:
        return self.get_state(f"{self.express_prefix}FLOW_ID")

    @flow_id.setter
    def flow_id(self, value: Optional[str]) -> None:
        self.set_state(f"{self.express_prefix}FLOW_ID", value)

    def get_or_create_event_bus(self):
        if self.event_bus:
            return self.event_bus
        if self.parent and self.parent.event_bus:
            self.event_bus = self.parent.event_bus
            return self.event_bus
        self.event_bus = _resolve_default_event_bus()
        return self.event_bus

    def child(self) -> ExecutionContext:
        return ExecutionContext(
            parent=self, event_bus=self.event_bus,
            evaluator=self._evaluator, expose_env=self.expose_env,
            **{k: getattr(self, k) for k in (
                "express_prefix", "express_input_name", "express_parent_name",
                "express_node_name", "express_global_name",
                "express_environment_variable",
            )},
        )

    def to_dict(self) -> Dict[str, Any]:
        # Drift guard: nodes can still set_state() arbitrary keys; warn on
        # system-shaped unknowns so a new magic key is caught at review/log time.
        from plaita.core.state import validate_checkpoint
        data = self._state.to_checkpoint_dict()
        for warning in validate_checkpoint(data, self.express_prefix):
            logger.warning(warning)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs) -> ExecutionContext:
        ctx = cls(**kwargs)
        ctx._state = CheckpointState.from_checkpoint_dict(data, **ctx._state_kwargs())
        return ctx
