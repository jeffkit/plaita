__version__ = "0.5.0"

import logging as _logging

_logger = _logging.getLogger(__name__)

_EXTRAS_GUIDE = {
    "redis": ("redis", "pip install plaita[redis]"),
    "server": ("fastapi", "pip install plaita[server]"),
    "code": ("execjs", "pip install plaita[code]"),
    # http extra 实际含 requests + aiohttp 两个依赖，只探测 requests 会让
    # "缺 extra 可操作报错"的承诺在只装 requests 时落空（运行时才炸）。
    "http": (("requests", "aiohttp"), "pip install plaita[http]"),
}


def _emit_extras_guidance():
    """Log guidance once when optional extras are missing, at DEBUG level."""
    missing = []
    for extra, (probe_modules, install_cmd) in _EXTRAS_GUIDE.items():
        if isinstance(probe_modules, str):
            probe_modules = (probe_modules,)
        try:
            for probe_module in probe_modules:
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
    probe_modules, install_cmd = _EXTRAS_GUIDE[extra_name]
    if isinstance(probe_modules, str):
        probe_modules = (probe_modules,)
    try:
        for probe_module in probe_modules:
            __import__(probe_module)
        return True
    except ImportError:
        raise ImportError(
            f"The '{extra_name}' extra is required for this feature but is not installed. "
            f"Install it with: {install_cmd}\n"
            f"Or install all extras: pip install plaita[all]"
        )


# 历史遗留: ``_default_event_bus_provider`` 全局可变 singleton 已被删除,
# ``core.context`` 现在直接 lazily import ``plaita.event.get_default_event_bus``
# 作为 fallback (见 ``plaita/core/context.py::_resolve_default_event_bus``)。
# 顶层包不再做任何 provider 注册——core 与 event 是直接协作者, 不必表演"分层"。



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
    # plaita.client（README/console 文档的公开入口写法 plaita.PlaitaClient）
    "PlaitaClient": "plaita.client",
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
    "FlowErrorException": "plaita.core.errors",
    "FlowStartMissingError": "plaita.core.errors",
    "FlowTimeoutError": "plaita.core.errors",
    "NodeNotFoundError": "plaita.core.errors",
    "NodeExecutionError": "plaita.core.errors",
    "NodeTimeoutError": "plaita.core.errors",
    "ErrorResultException": "plaita.core.errors",
    "ResumeError": "plaita.core.errors",
    "ResumeType": "plaita.core.errors",
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
    """Make lazy re-exports discoverable by IDEs / ``dir(plaita)``.

    故意**不含** ``_FEATURE_EXTRAS_MAP`` 里的名字（``CodeNode``/``HTTP`` 等）：
    pydoc 的 ``help(plaita)`` 会对 ``__dir__`` 结果逐个 ``getattr``，缺 extra 时
    这些名字抛出的 ImportError 会打断整个 help 输出（只剩一行报错）。它们不在
    ``__all__`` 里，``from plaita import *`` 也不会因缺 extra 失败。
    """
    return sorted(
        set(globals())
        | set(_LAZY_EXPORTS)
        | {"types"}
    )


# 显式 __all__: 0.5.0 前顶层无 __all__, ``from plaita import *`` 得到空集。
# 只含开箱即用的名字——feature-gated（CodeNode/HTTP/Redis* 等）不进 __all__,
# 否则缺 extra 的进程 star-import 直接崩; 这些名字的获取方式见 docs 的 extras 表。
__all__ = sorted(set(_LAZY_EXPORTS) | {"types", "__version__"})


if __name__ == "__main__":  # pragma: no cover
    print(__version__)
