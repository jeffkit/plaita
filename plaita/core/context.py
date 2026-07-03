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
        expose_env: Optional[List[str]] = None,
    ) -> None:
        self._context: Dict[str, Any] = {}
        self.parent = parent
        self.event_bus = event_bus
        # $ENV 的 allowlist。``None`` / 空列表都意味着默认空 $ENV（不再泄漏
        # os.environ）。Flow 层会从 Flow.expose_env 把这份名单传下来。
        self.expose_env = list(expose_env) if expose_env else []
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

        ``$ENV`` 内容来自 ``self.expose_env`` allowlist（默认空）。``clean()``
        不会清空 ``expose_env`` 本身——它属于 context 配置，不是状态。
        """
        self._context = {}
        self.cancel_event = threading.Event()
        self.set_state(f"{self.express_prefix}EXECUTION_ID", uuid.uuid4().hex)
        self.set_state(
            f"{self.express_prefix}{self.express_environment_variable}",
            _safe_environment(self.expose_env),
        )

    def __getstate__(self):
        # threading.Event 不可 pickle。进程模式下子进程会经 ``__setstate__``
        # 重建一个**全新未触发**的 Event——这意味着父进程的 cancel 信号无法
        # 通过 pickle 传给子进程。如果调用方依赖 cancel 跨进程传播 (例如
        # Parallel mode=process 的分支内节点需要响应父进程超时取消), 当前实现
        # 不支持, 应改用 mode=thread 或显式 IPC。
        state = self.__dict__.copy()
        state.pop("cancel_event", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if not getattr(self, "cancel_event", None):
            self.cancel_event = threading.Event()

    # -- flow setup --

    def setup_flow(self, flow, args: tuple, kwargs: dict) -> None:
        """Initialize context with flow-level variables.

        Also propagates ``flow.expose_env`` to ``self.expose_env`` and re-runs
        ``_safe_environment`` so that ``$ENV`` reflects the flow-level allowlist
        rather than whatever ``clean()`` wrote (which used the previous, possibly
        empty, allowlist). This keeps ``$ENV`` consistent across re-runs.
        """
        flow_allowlist = list(getattr(flow, "expose_env", None) or [])
        if flow_allowlist != self.expose_env:
            self.expose_env = flow_allowlist
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
        # 用最终的 allowlist 重新刷新 $ENV，避免 child/parent allowlist 差异导致
        # 旧值残留。
        self.set_state(
            f"{self.express_prefix}{self.express_environment_variable}",
            _safe_environment(self.expose_env),
        )

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

    # -- typed system-state accessors (distributed checkpoint keys) --
    #
    # Previously all callers built magic strings like
    #   ``f"{pfx}LAST_NODE"`` / ``f"{pfx}BRANCH"`` / ``f"{pfx}FLOW_ID"``
    # inline, which scattered the key schema across runner.py, executor.py,
    # assignment.py and event/core.py.  These properties centralise the
    # key construction; the underlying storage format in ``_context`` is
    # intentionally unchanged so that distributed checkpoints (to_dict /
    # from_dict) remain forward- and backward-compatible.

    @property
    def last_node_id(self) -> Optional[str]:
        """ID of the most recently executed node (distributed checkpoint key)."""
        return self.get_state(f"{self.express_prefix}LAST_NODE")

    @last_node_id.setter
    def last_node_id(self, value: Optional[str]) -> None:
        self.set_state(f"{self.express_prefix}LAST_NODE", value)

    @property
    def last_branch(self) -> Optional[str]:
        """Branch taken by the most recently executed branching node."""
        return self.get_state(f"{self.express_prefix}BRANCH")

    @last_branch.setter
    def last_branch(self, value: Optional[str]) -> None:
        self.set_state(f"{self.express_prefix}BRANCH", value)

    @property
    def flow_id(self) -> Optional[str]:
        """Flow ID of the currently executing flow."""
        return self.get_state(f"{self.express_prefix}FLOW_ID")

    @flow_id.setter
    def flow_id(self, value: Optional[str]) -> None:
        self.set_state(f"{self.express_prefix}FLOW_ID", value)

    # -- event bus --

    def get_or_create_event_bus(self):
        if self.event_bus:
            return self.event_bus
        if self.parent and self.parent.event_bus:
            self.event_bus = self.parent.event_bus
            return self.event_bus
        provider = _resolve_default_event_bus
        if provider is not None:
            self.event_bus = provider()
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
            expose_env=self.expose_env,
        )

    # -- serialization for distributed execution --

    def to_dict(self) -> Dict[str, Any]:
        # 序列化前扫一遍 schema 漂移: 任何"看起来是 system key 但不在 CheckpointSchema"
        # 的字段都 warn。这是 ``ExecutionState(BaseModel)`` 完整建模路径上的中间步——
        # 在 schema 真做完之前, 至少让新加 magic key 的人在 review 时被发现。
        from plaita.core.state import validate_checkpoint
        for warning in validate_checkpoint(self._context, self.express_prefix):
            logger.warning(warning)
        return dict(self._context)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs) -> ExecutionContext:
        ctx = cls(**kwargs)
        ctx._context = dict(data)
        return ctx
