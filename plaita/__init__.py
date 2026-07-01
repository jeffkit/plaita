__version__ = "0.3.16"

import logging as _logging

_logger = _logging.getLogger(__name__)

_EXTRAS_GUIDE = {
    "redis": ("redis", "pip install plaita[redis]"),
    "server": ("fastapi", "pip install plaita[server]"),
    "code": ("execjs", "pip install plaita[code]"),
    "http": ("requests", "pip install plaita[http]"),
}


def _emit_extras_guidance():
    """Log guidance once when optional extras are missing, at DEBUG level."""
    missing = []
    for extra, (probe_module, install_cmd) in _EXTRAS_GUIDE.items():
        try:
            __import__(probe_module)
        except ImportError:
            missing.append((extra, install_cmd))
    if missing:
        names = ", ".join(name for name, _ in missing)
        _logger.debug(
            "Optional plaita extras not installed: %s. "
            "Install what you need, e.g.: pip install plaita[all]",
            names,
        )


_emit_extras_guidance()


def _check_extra_available(extra_name: str) -> bool:
    """Check if a specific extra dependency group is available.

    Returns True if the probe module for the extra can be imported.
    Raises ImportError with an actionable message if not.
    """
    if extra_name not in _EXTRAS_GUIDE:
        return True
    probe_module, install_cmd = _EXTRAS_GUIDE[extra_name]
    try:
        __import__(probe_module)
        return True
    except ImportError:
        raise ImportError(
            f"The '{extra_name}' extra is required for this feature but is not installed. "
            f"Install it with: {install_cmd}\n"
            f"Or install all extras: pip install plaita[all]"
        )


# 依赖反转: 注册一个 lazy 的默认 event bus provider, 让 plaita.core.context
# 在需要时自动取默认总线, 而 core 层不必 import plaita.event (避免 core→event
# 反向依赖)。provider 仅在第一次被调用时才 import plaita.event。
def _default_event_bus_provider():
    from plaita.event import get_default_event_bus
    return get_default_event_bus()


def _register_default_event_bus_provider() -> None:
    try:
        from plaita.core.context import set_default_event_bus_provider
        set_default_event_bus_provider(_default_event_bus_provider)
    except Exception:
        _logger.debug("Unable to register default event bus provider", exc_info=True)


_register_default_event_bus_provider()


# Upgrade guide: provide clear error messages when accessing optional features
# without the corresponding extras installed. Consumed by __getattr__ below.
_FEATURE_EXTRAS_MAP = {
    "FlowWorker": "server",
    "ManagementAPI": "server",
    "RedisStorage": "redis",
    "RedisEventBus": "redis",
    "CodeNode": "code",
    "HTTP": "http",
}

# Canonical home for each lazily re-exported public name. Kept explicit so
# that `from plaita import Flow` works out of the box without eagerly pulling
# the core layer into memory at package import time.
_LAZY_EXPORTS = {
    # plaita.core.flow
    "Flow": "plaita.core.flow",
    "parse": "plaita.core.flow",
    "parse_and_run": "plaita.core.flow",
    # plaita.core.executor
    "FlowExecution": "plaita.core.executor",
    "ExecutionMode": "plaita.core.executor",
    # plaita.core.errors
    "FlowExecutionException": "plaita.core.errors",
    "FlowErrorType": "plaita.core.errors",
    "FlowResultError": "plaita.core.errors",
    "NodeException": "plaita.core.errors",
    "ErrorStrategy": "plaita.core.errors",
    "ErrorHandler": "plaita.core.errors",
    "RecoverableErrorHandler": "plaita.core.errors",
    # plaita.core.callback
    "FlowCallback": "plaita.core.callback",
    "FlowEvent": "plaita.core.callback",
    "CallbackManager": "plaita.core.callback",
    "BaseCallbackManager": "plaita.core.callback",
    "LoggerCallback": "plaita.core.callback",
    # plaita.node
    "Node": "plaita.node",
    "node_register": "plaita.node",
    "parse_node": "plaita.node",
    "nodes": "plaita.node",
    "NodeRegistry": "plaita.node",
    "get_default_registry": "plaita.node",
}

# For optional-feature names: once the required extra is confirmed available,
# the name lives in this canonical module. ``HTTP`` (not ``HTTPNode``) is the
# real class name exported by ``plaita.node.http``.
_EXTRA_EXPORTS = {
    "FlowWorker": "plaita.server.flow_worker",
    "ManagementAPI": "plaita.server.control",
    "RedisStorage": "plaita.storage.redis",
    "RedisEventBus": "plaita.event.redis",
    "CodeNode": "plaita.node.code",
    "HTTP": "plaita.node.http",
}


def __getattr__(name: str):
    """Lazy top-level re-exports + actionable extras guidance.

    - ``from plaita import Flow`` / ``parse`` / ``Node`` / ``node_register`` ...
      resolve to the canonical ``plaita.core.*`` / ``plaita.node`` symbols without
      importing the core layer at package import time.
    - ``from plaita import types`` returns the ``plaita.core.types`` module so the
      deprecated ``plaita.types`` shim is NOT triggered.
    - Optional features (``FlowWorker``, ``HTTP``, ``RedisEventBus`` ...) raise
      a clear ImportError naming the missing extra instead of a bare ImportError.
    """
    import importlib

    if name in _LAZY_EXPORTS:
        module = importlib.import_module(_LAZY_EXPORTS[name])
        value = getattr(module, name)
        globals()[name] = value  # cache so subsequent access bypasses __getattr__
        return value

    if name == "types":
        module = importlib.import_module("plaita.core.types")
        globals()["types"] = module
        return module

    if name in _FEATURE_EXTRAS_MAP:
        # Raises an actionable ImportError if the extra is missing.
        _check_extra_available(_FEATURE_EXTRAS_MAP[name])
        module = importlib.import_module(_EXTRA_EXPORTS[name])
        value = getattr(module, name)
        globals()[name] = value
        return value

    raise AttributeError(f"module 'plaita' has no attribute {name!r}")


def __dir__():
    """Make lazy re-exports discoverable by IDEs / ``dir(plaita)``."""
    return sorted(
        set(globals())
        | set(_LAZY_EXPORTS)
        | set(_FEATURE_EXTRAS_MAP)
        | {"types"}
    )


if __name__ == "__main__":
    print(__version__)
