__version__ = "0.6.1"

import importlib.util as _importlib_util
import logging as _logging

_logger = _logging.getLogger(__name__)

_EXTRAS_GUIDE = {
    "redis": ("redis", "pip install plaita[redis]"),
    # server extra 的五个依赖全探（pyproject [project.optional-dependencies].server）。
    # #99：此前只探 fastapi——而 plaita/server/ 模块级真正 import 的是
    # cachetools/redis；宿主恰好装了 fastapi（例如宿主本身就是 FastAPI 应用）
    # 时探针误判「extra 已就绪」，`from plaita import FlowWorker` 抛裸
    # ModuleNotFoundError，没有任何 pip install 指引。
    "server": (
        ("fastapi", "uvicorn", "redis", "cachetools", "sqlalchemy"),
        "pip install plaita[server]",
    ),
    "code": ("execjs", "pip install plaita[code]"),
    # http extra 实际含 requests + aiohttp 两个依赖，只探测 requests 会让
    # "缺 extra 可操作报错"的承诺在只装 requests 时落空（运行时才炸）。
    "http": (("requests", "aiohttp"), "pip install plaita[http]"),
    # 观测适配（plaita.obs.LangfuseCallback，SDK v3 = OTel 内核一代）
    "langfuse": ("langfuse", "pip install plaita[langfuse]"),
}


def _probe_available(probe_module: str) -> bool:
    """探测可选 SDK 是否**可导入** —— 用 ``find_spec`` **定位**，不真正导入（#97）。

    ``__import__(name)`` 会把整个包**执行一遍**（fastapi / langfuse / aiohttp 都是重包）
    ⇒ 导入 ``plaita`` 的代价被可选依赖绑架（实测 523 ms 且 6 个 SDK 全被真正导入）。
    探测只需要回答"找不找得到"，``find_spec`` 足够且**不产生导入副作用**。

    注意 ``find_spec`` 在**父包缺失**时抛 ``ModuleNotFoundError``（``ImportError`` 子类），
    在某些 loader 下还会抛 ``ValueError``（``__spec__ is None``）⇒ 两种都当"不可用"。
    """
    try:
        return _importlib_util.find_spec(probe_module) is not None
    except (ImportError, ValueError):
        return False


def _emit_extras_guidance():
    """Log guidance once when optional extras are missing, at DEBUG level."""
    missing = []
    for extra, (probe_modules, install_cmd) in _EXTRAS_GUIDE.items():
        if isinstance(probe_modules, str):
            probe_modules = (probe_modules,)
        # #97：导入期**只定位、不导入**（见 `_probe_available`）。
        if not all(_probe_available(m) for m in probe_modules):
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
    "RedisExecutionStorage": "redis",
    "RedisFlowStorage": "redis",
    "RedisEventBus": "redis",
    "CodeNode": "code",
    "HTTP": "http",
    "LangfuseCallback": "langfuse",
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
    "ResumeGuardError": "plaita.core.errors",
    "StateSnapshotError": "plaita.core.errors",
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
# real class name exported by ``plaita.node.http``. Each key must therefore be a
# symbol that actually exists there——``getattr`` 取不到即抛 AttributeError
# （守卫见 tests/unit/test_module_coverage.py::TestLazyExportTables）。
_EXTRA_EXPORTS = {
    "FlowWorker": "plaita.server.flow_worker",
    "RedisExecutionStorage": "plaita.storage.redis",
    "RedisFlowStorage": "plaita.storage.redis",
    "RedisEventBus": "plaita.event.redis",
    "CodeNode": "plaita.node.code",
    "HTTP": "plaita.node.http",
    "LangfuseCallback": "plaita.obs",
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


# 可选事件循环加速（2026-10 wave3）：PLAITA_LOOP=uvloop 时安装 uvloop policy。
# unix-only；uvloop 未装时仅 debug 留痕、行为不变。进程级 opt-in——本模块
# import 即生效，须在首个事件循环创建前（run_async_from_sync /
# async_gen_to_sync 均经当前 policy 建循环）。
def _maybe_install_uvloop() -> None:
    import os

    if os.environ.get("PLAITA_LOOP", "").strip().lower() != "uvloop":
        return
    try:
        import uvloop
    except Exception as exc:  # noqa: BLE001 - 可选加速缺失：仅留痕，行为不变
        # 局部 import：本模块是包入口（顶层有意只 import stdlib），不在 import 期
        # 拉起 plaita.core 的依赖图；掩码器只在真要记这条 DEBUG 时才加载。
        from plaita.core.url_redaction import _scrub_exception_text

        # #151：exc_info 的 traceback 由 Formatter 在 logger 之后用原始异常重渲染，
        # 构造面清洗盖不住 ⇒ 在构造面渲染（清洗）并摘掉 exc_info。
        _logger.debug("PLAITA_LOOP=uvloop but uvloop is not installed: %s", _scrub_exception_text(exc))
        return
    uvloop.install()


_maybe_install_uvloop()
