"""plaita.node.code — CodeNode: user-supplied code execution.

Security model
--------------
CodeNode is **not** in the default NodeRegistry.  Callers must explicitly opt in
via ``register_code_node()`` before flows containing ``type: code`` can be
parsed.

Python backend selection is controlled by ``CodeNode.sandbox_backend``:

``"restricted"`` (default)
    Uses RestrictedPython to compile the user script to restricted bytecode.
    Dangerous builtins (``open``, ``exec``, ``eval``, ``__import__`` for
    non-allowlisted modules) are stripped.  Only modules listed in
    ``SANDBOX_SAFE_MODULES`` may be imported.  Recommended for most
    multi-tenant deployments.

``"subprocess"``
    Spawns a fresh Python interpreter for every invocation.  The child
    inherits the host's file system and network access, but is bounded by
    a configurable **wall-clock** timeout (there is no separate CPU-time
    limit: sleeping and CPU work both consume the same budget), and an
    **optional** address-space limit on Linux.  Safer than ``"restricted"``
    against code
    that bypasses RestrictedPython's AST guards, but does **not** block
    network or file I/O.  Input/output is serialised as JSON; only
    JSON-serialisable types are supported.  Environment variables:
    ``PLAITA_SANDBOX_TIMEOUT`` (seconds, default 10),
    ``PLAITA_SANDBOX_MEMORY_MB`` (MB, default ``0`` = unlimited).

``"docker"``
    Runs the script inside a one-shot Docker container with
    ``--network none``, ``--read-only``, configurable memory and CPU caps.
    Provides the strongest isolation; requires Docker (or Podman with the
    ``docker`` CLI shim) to be installed and the daemon to be running.
    Input/output is serialised as JSON.  Environment variables:
    ``PLAITA_SANDBOX_DOCKER_IMAGE`` (default ``python:3.12-slim``),
    ``PLAITA_SANDBOX_DOCKER_TIMEOUT`` (seconds, default 30),
    ``PLAITA_SANDBOX_DOCKER_MEMORY_MB`` (MB, default 128),
    ``PLAITA_SANDBOX_DOCKER_CPUS`` (default ``0.5``).

``"unsafe"``
    Raw ``exec()`` — identical to the historical behaviour.  All Python
    builtins and any importable module are available.  Only use this when you
    fully trust the flow authors.

JavaScript (``language: "js"``)
-------------------------------
``sandbox_backend`` selects the JS isolation level exactly like it does for
Python, but the implementations differ (``restricted`` has **no** JS
counterpart — RestrictedPython is Python-only):

``"docker"`` (default)
    Runs the script inside a one-shot container of
    ``PLAITA_SANDBOX_DOCKER_NODE_IMAGE`` (default ``node:20-alpine``) with the
    same hardening flags as the Python backend.

``"subprocess"``
    Spawns ``node -e <wrapped script>`` (``PLAITA_SANDBOX_NODE_BIN``, default
    ``node``) with the environment allowlist, a wall-clock timeout
    (``PLAITA_SANDBOX_TIMEOUT``) and process-group kill on timeout/cancel.

``"unsafe"``
    Historical PyExecJS path: an external engine is driven through the
    ``execjs`` package.  **No timeout, no cancellation, no isolation.** Only
    for fully trusted authors.

Historically ``language: "js"`` ignored ``sandbox_backend`` entirely and always
took the PyExecJS path — a per-node one-line bypass of the whole tier system
(2026-10 安全评审).  JS is now gated twice: operators must opt in via
``register_code_node(allowed_languages=(...))`` (default ``("python",)``), and
the declared backend must have a JS implementation.

All backends are transparently switchable via the ``sandbox_backend`` flow
JSON field; no other code changes are required when upgrading the isolation
level.

Tenant credential boundary (plaita#256, A42-06)
-----------------------------------------------
``"subprocess"`` runs user code in a fresh interpreter **without** a file-system
boundary: parent and child run as the same OS user, and all tenants share one
Fernet key. Code in the child can therefore read the default tenant's
``.plaita-credentials.json`` / ``.plaita-credentials.key`` (or call
``set_current_tenant("default")``) no matter which credential paths the parent
puts in its env. The subprocess backend is consequently **not** a credential
isolation boundary: non-default tenants are refused **before spawning**
(:class:`TenantContextPropagationError`, message marker
``PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE``), so their user code never
runs. Use the ``"docker"`` backend for tenant code (host credentials are not
mounted). Default-tenant executions keep the historical semantics. The
``"unsafe"`` / ``"restricted"`` backends run in-process with the caller's
context and are not credential boundaries either.
"""

from __future__ import annotations

import ast
import base64
import importlib
import json
import logging
import math
import operator as _op
import os
import signal
import subprocess
import sys
import textwrap
import time
import uuid
from pathlib import Path
from typing import Any, ClassVar, FrozenSet, Optional

from pydantic import model_validator

from plaita import subprocess_env as _env
from plaita.core.url_redaction import _scrub_exception_text
from plaita.credentials import subprocess_credentials_env
from plaita.tenant_context import (
    DEFAULT_TENANT_ID as _DEFAULT_TENANT_ID,
    current_tenant,
)

from .basic import Node

logger = logging.getLogger(__name__)

try:
    import execjs
except ImportError:
    execjs = None

try:
    from RestrictedPython import compile_restricted, safe_builtins
    from RestrictedPython.Guards import guarded_iter_unpack_sequence
    _RESTRICTED_AVAILABLE = True
except ImportError:
    _RESTRICTED_AVAILABLE = False


# 默认 Python 沙箱后端。0.5.0 起默认 ``"docker"`` (容器级隔离: 无网络 / 只读 FS /
# 资源上限)——0.4.x 的 ``"restricted"`` (RestrictedPython, AST 级) 已被证明存在
# 绕过向量, 不适合作为"对用户透明"的默认值。``register_code_node(default_backend=...)``
# 在启动期把它改成实际生效的后端, 并在生效后端为 docker 但 daemon 不可用时**拒绝注册**。
_DEFAULT_SANDBOX_BACKEND = "docker"


def _docker_available() -> bool:
    """探测 docker CLI 与 daemon 是否可用。``register_code_node`` 在默认后端为
    docker 时调用, 不可用则拒绝注册 (避免运行期才崩)。"""
    import shutil
    if not shutil.which("docker"):
        return False
    try:
        proc = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=5,
        )
        return proc.returncode == 0
    except Exception as exc:
        logger.debug("docker daemon probe failed: %s", _scrub_exception_text(exc))
        return False

JS_FUNC_NAME = "run"
PYTHON_FUNC_NAME = "run"

LANGUAGE_JS = "js"
LANGUAGE_PYTHON = "python"

# ---------------------------------------------------------------------------
# Sandbox configuration (env-var overrides)
# ---------------------------------------------------------------------------

def _positive_timeout_seconds(name: str, value: Any) -> float:
    """计时参数 fail-closed 校验：必须为有限正数（0/负值/非有限具名拒绝）。

    引擎语义 ``timeout=0`` 是零预算（立即超时）；历史实现
    ``deadline = (...) if timeout else None`` 把 0 当成「无限」——部署脚本
    把未设置的变量展开成 0 时，沙箱墙钟护栏会整体消失（r39 A1）。与
    plaita-nodes #205 的 ``_positive_limit`` 同风格：带字段名的 ValueError，
    让错误在启动/调用期暴露，而不是静默改道。
    """
    try:
        seconds = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be a positive number of seconds (got {value!r}); "
            "0 and negative values are rejected"
        ) from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError(
            f"{name} must be a positive number of seconds (got {value!r}); "
            "0 and negative values are rejected"
        )
    return seconds


def _positive_int_env_seconds(name: str, default: str) -> int:
    """读取正数秒级 env；0/负数/非整数在 import 期具名失败。"""
    raw = os.environ.get(name, default)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be a positive integer number of seconds (got {raw!r})"
        ) from exc
    if value <= 0:
        raise ValueError(
            f"{name} must be a positive integer number of seconds (got {raw!r}); "
            "0 is a zero budget, not unlimited"
        )
    return value


def _sandbox_memory_mb_env() -> int:
    """``PLAITA_SANDBOX_MEMORY_MB``：0=不限；负值/非数字 fail-fast（B2R31-03）。

    历史上负值被接受并在 runner 里被 ``>0`` 判据静默跳过（等同不限），
    运维以为设了上限。现在 import 期拒绝——安全/资源类非法值必须 loud。
    """
    from plaita.runtime_config import ConfigError, parse_int

    raw = os.environ.get("PLAITA_SANDBOX_MEMORY_MB", "0")
    if not str(raw).strip():
        logger.warning("PLAITA_SANDBOX_MEMORY_MB 为空，按 0（不限）处理")
        return 0
    try:
        return parse_int(raw, "PLAITA_SANDBOX_MEMORY_MB", minimum=0)
    except ConfigError as exc:
        raise ConfigError(
            [
                f"{exc}；0=不限（文档语义），负值/非数字拒绝启动"
            ]
        ) from exc


def _sandbox_docker_cpus_env() -> str:
    """``PLAITA_SANDBOX_DOCKER_CPUS``：启动期校验有限正数（B2R31-03）。

    历史上非法值到 ``docker run --cpus=abc`` 才在**执行期**失败；现在 import
    期 fail-fast，错误含变量名与期望格式。
    """
    from plaita.runtime_config import ConfigError, parse_float

    raw = os.environ.get("PLAITA_SANDBOX_DOCKER_CPUS", "0.5")
    if not str(raw).strip():
        logger.warning("PLAITA_SANDBOX_DOCKER_CPUS 为空，按 0.5 处理")
        return "0.5"
    try:
        value = parse_float(raw, "PLAITA_SANDBOX_DOCKER_CPUS", minimum=1e-9)
    except ConfigError as exc:
        raise ConfigError([f"{exc}；期望有限正数（如 0.5）"]) from exc
    return str(value)


# subprocess backend
SANDBOX_SUBPROCESS_TIMEOUT: int = _positive_int_env_seconds("PLAITA_SANDBOX_TIMEOUT", "10")
# RLIMIT_AS 上限（MB）。默认 0 = 不限制（与 macOS 上的历史实际行为一致）。
# 2026-10-06 事故：默认 256 在 Linux 上真实生效，node/pnpm 类命令启动即 V8 OOM
# （V8 启动期预留远超 256MB 的地址空间，RLIMIT_AS 卡的是虚拟地址而非 RSS），
# 输出只剩截断崩溃栈；macOS 不强制 RLIMIT_AS，故本地部署从未暴露。设 >0 才
# 启用限制——小内存纯 Python 脚本场景可显式收紧，node 系命令请保持 0。
SANDBOX_SUBPROCESS_MEMORY_MB: int = _sandbox_memory_mb_env()

# subprocess 后端的子进程环境变量白名单（2026-09 安全评审 P1）：历史上子进程
# 继承宿主全量 os.environ。实现已抽到公共层 ``plaita.subprocess_env``（gate /
# capture / agent 直跑等 plaita-nodes 节点共用），这里 re-export 保持既有导入点
# 零改动；需要额外变量时改 ``SUBPROCESS_ENV_EXTRA``（同一 dict 对象）或调用点传
# ``extra=``。
SUBPROCESS_ENV_ALLOWLIST = _env.SUBPROCESS_ENV_ALLOWLIST
SUBPROCESS_ENV_EXTRA = _env.SUBPROCESS_ENV_EXTRA

# docker backend
SANDBOX_DOCKER_IMAGE: str = os.environ.get("PLAITA_SANDBOX_DOCKER_IMAGE", "python:3.12-slim")
SANDBOX_DOCKER_TIMEOUT: int = _positive_int_env_seconds("PLAITA_SANDBOX_DOCKER_TIMEOUT", "30")
SANDBOX_DOCKER_MEMORY_MB: int = int(os.environ.get("PLAITA_SANDBOX_DOCKER_MEMORY_MB", "128"))
SANDBOX_DOCKER_CPUS: str = _sandbox_docker_cpus_env()
# 容器运行用户（如 "65534:65534"）。默认空 = 镜像默认用户（root）。
# python:3.12-slim 的 /tmp tmpfs 为 1777，nobody 可写；设了用户后沙箱内
# 代码写非 /tmp 路径会失败——这是预期约束而非 bug。
SANDBOX_DOCKER_USER: str = os.environ.get("PLAITA_SANDBOX_DOCKER_USER", "")

# JS (node) runner: 宿主侧 node 可执行文件 + docker 侧 node 镜像。
SANDBOX_NODE_BIN: str = os.environ.get("PLAITA_SANDBOX_NODE_BIN", "node")
SANDBOX_DOCKER_NODE_IMAGE: str = os.environ.get(
    "PLAITA_SANDBOX_DOCKER_NODE_IMAGE", "node:20-alpine")

# Modules that restricted sandboxed code is allowed to import.
#
# 安全边界声明（2026-09 安全评审）：restricted 后端**只防误用，不防恶意作者**。
# functools/operator 曾在白名单里——`operator.attrgetter("__class__")` 走 C 层
# 属性访问，绕过 Python 层的 _getattr_ 拦截，可经 __subclasses__/__globals__
# 完整逃逸（确定性 PoC），已移除。即便如此也不应把 restricted 当半信任沙箱：
# 半信任作者的代码请用 docker 后端（容器级隔离）。
SANDBOX_SAFE_MODULES: FrozenSet[str] = frozenset([
    "math", "json", "re", "datetime", "random", "string",
    "itertools", "collections",
    "decimal", "fractions", "statistics", "textwrap",
    "base64", "hashlib", "hmac", "struct",
    "enum", "dataclasses", "typing",
])

# 运营者通过 ``register_code_node(allowed_backends=(...))`` 设置的后端白名单。
# None = 不限制（历史行为）。设置后，流程 JSON 里的 ``sandbox_backend`` 不在
# 白名单内即**解析期硬失败**——没有这个约束，流程作者可以把运营者选定的
# 默认后端逐节点覆盖成 "unsafe"（宿主任意代码执行）。
_ALLOWED_SANDBOX_BACKENDS: Optional[FrozenSet[str]] = None

# 语言白名单（2026-10 安全评审 P1，plaita#29）。默认只放行 python：
# ``js`` 的历史实现（PyExecJS，见 run_js）**完全绕开沙箱档位**——无隔离/无超时/
# 无取消，白名单只作用于 Python。运营者经
# ``register_code_node(allowed_languages=(...))`` 显式放行 ``"js"`` 后才可提交 js
# 节点；放行后 js 同样按 ``sandbox_backend`` 走档位表（无 js 实现的后端拒绝）。
# 自定义语言（``register_runner`` 注册，运营者自己的进程内代码）不受此白名单约束。
DEFAULT_SANDBOX_ALLOWED_LANGUAGES: tuple = (LANGUAGE_PYTHON,)
_ALLOWED_LANGUAGES: FrozenSet[str] = frozenset(DEFAULT_SANDBOX_ALLOWED_LANGUAGES)

# ---------------------------------------------------------------------------
# Runner script template shared by subprocess and docker backends
# ---------------------------------------------------------------------------

_RUNNER_TEMPLATE = textwrap.dedent("""\
    import sys as _sys, json as _json, os as _os

    def _plaita_emit(_payload, _out=_sys.stdout, _dumps=_json.dumps):
        _out.write(_dumps(_payload))
        _out.flush()

    # 模块级用户代码（下面的占位符展开处）在固定尾巴的 try 之外：顶层异常
    # 若无人接管，进程带着 traceback 非零退出、stdout 没有信封，父进程只能按
    # 「无信封非零退出」的基础设施失败分类（可重试 ⇒ 确定性顶层错误烧满
    # retryTimes，顶层副作用重复执行，plaita#297）。excepthook 把顶层未捕获
    # 异常写成与 run() 函数体完全同形的 ok=false 信封——内层类型名随
    # ``type`` 字段穿透，重试分类两档一致。用户代码正常走完后立即还原默认
    # hook：runner 自身尾巴（stdin 非 JSON 等）的失败维持基础设施级缺省。
    def _plaita_module_excepthook(_exc_type, _exc_value, _exc_tb,
                                  _emit=_plaita_emit, _exit=_os._exit):
        try:
            _emit({"ok": False, "error": str(_exc_value),
                   "type": _exc_type.__name__})
        finally:
            # 跳过 atexit/其他线程的残余写：信封已经是本进程最后的输出，
            # 父进程按非零退出 + 信封解码（不再落入「无输出」分支）。
            _exit(1)

    _sys.excepthook = _plaita_module_excepthook

    # ---- resource limits (Linux only) ------------------------------------
    try:
        import resource as _resource
        _mem_bytes = __MEM_BYTES__
        if _mem_bytes > 0:
            _resource.setrlimit(_resource.RLIMIT_AS, (_mem_bytes, _mem_bytes))
    except Exception:
        pass

    # ---- user code -------------------------------------------------------
    __USER_CODE__
    _sys.excepthook = _sys.__excepthook__

    # ---- run -------------------------------------------------------------
    _input = _json.loads(_sys.stdin.read())
    try:
        _result = run(_input)
        _sys.stdout.write(_json.dumps({"ok": True, "result": _result}))
    except Exception as _e:
        _sys.stdout.write(_json.dumps({"ok": False, "error": str(_e), "type": type(_e).__name__}))
    _sys.stdout.flush()
""")

# Sentinel placeholders used by _build_runner_script.  Must be valid Python
# identifiers so they survive AST-based validation; they are replaced with
# literal values *before* the runner script is executed.
_PLACEHOLDER_MEM = "__MEM_BYTES__"
_PLACEHOLDER_CODE = "__USER_CODE__"


class SandboxEnvelopeError(RuntimeError):
    """沙箱 runner 信封 ``ok=false`` 的具名包装（node-F2 × plaita-go#43）。

    subprocess/docker 后端的 runner 把用户代码异常写成 JSON 信封
    ``{"ok": false, "type": <类型名>, "error": <消息>}``。旧实现把它压成裸
    ``RuntimeError``，内层类型只剩文本前缀，重试分类无法穿透（B3 r70 实测
    ``is_retryable_error(RuntimeError("ValueError: boom")) == True``——确定性
    失败被烧满 retryTimes，每次重试都是一次真实的子进程 spawn + 用户代码
    执行）。本类把信封保留的类型名挂在 ``origin_type`` 供
    ``core.runner.is_retryable_error`` 按确定性集分类。

    ``RuntimeError`` 子类且文本逐字不变（``f"{type}: {msg}"``，与 Go
    ``decodePythonRunnerOutput`` 同文），既有按 ``RuntimeError``/文案捕获的
    行为不受影响。只用于 **python** 信封：JS 信封的类型名体系不同
    （``ReferenceError``/``EvalError`` 等，无 ``ValueError``），维持裸
    ``RuntimeError`` 历史行为，不在 plaita-go#43 裁决集内。
    """

    def __init__(self, exc_type: str, exc_msg: str):
        super().__init__(f"{exc_type}: {exc_msg}")
        #: 信封保留的用户代码异常类型名（``"ValueError"``/``"TypeError"``/…
        #: 缺型时为 ``"Error"``）。重试分类按它对照
        #: ``core.errors.DETERMINISTIC_ERROR_TYPES``。
        self.origin_type = exc_type


def _build_runner_script(user_code: str, mem_bytes: int = 0) -> str:
    """Embed *user_code* inside the runner template.

    The user code is inserted at column-0 (module level).  Uses
    ``str.replace`` rather than ``str.format`` or ``string.Template`` so that
    arbitrary Python code (including dict literals containing ``{`` / ``}``)
    is safe.

    The runner carries no tenant/credential bootstrap: non-default tenants are
    refused by :func:`run_python_subprocess` before spawning (A42-06), and the
    docker backend mounts no host credentials.
    """
    script = _RUNNER_TEMPLATE.replace(_PLACEHOLDER_CODE, user_code)
    script = script.replace(_PLACEHOLDER_MEM, str(mem_bytes))
    return script


def _extract_trailing_envelope(raw: str) -> Optional[dict]:
    """从 stdout 末尾取出 runner 信封；找不到返回 ``None``。

    用户代码的 ``print``/模块级输出写在信封**之前**（runner 保证信封是最后
    一次写）：顶层 ``print('hello')`` 后抛异常时，raw 是 ``"hello\\n{...}"``，
    ``json.loads(raw)`` 会失败并把诊断降级成「not valid JSON」（plaita#297）。
    这里从最后一个 ``{"ok"`` 候选位置用 ``raw_decode`` 解析、且要求信封之后
    只剩空白——只接受真正的尾巴信封，避免把用户输出的任意 JSON 当信封。
    """
    decoder = json.JSONDecoder()
    idx = raw.rfind('{"ok"')
    while idx != -1:
        try:
            parsed, end = decoder.raw_decode(raw, idx)
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(parsed, dict) and "ok" in parsed and not raw[end:].strip():
                return parsed
        idx = raw.rfind('{"ok"', 0, idx)
    return None


def _decode_runner_output(raw: str, stderr: str, *, origin_typed: bool = False) -> Any:
    """Parse the JSON envelope written by the runner script.

    ``origin_typed=True``（python 后端专用）时 ``ok=false`` 分支抛
    ``SandboxEnvelopeError``（携带信封类型名供重试分类穿透）；JS 后端维持
    裸 ``RuntimeError`` 历史行为。基础设施级失败（无输出/坏 JSON）两语言
    都是无 ``origin_type`` 的裸 ``RuntimeError``，保持历史可重试缺省（与 Go
    ``decodePythonRunnerOutput`` 的注释口径一致）。

    用户代码写脏 stdout（信封前的 print）时按末尾信封解码（#297）；没有
    合法尾巴信封的脏输出仍是基础设施级失败，不按文本嗅探分类。
    """
    if not raw.strip():
        raise RuntimeError(
            f"Sandbox produced no output. Stderr: {stderr[:500] or '(empty)'}"
        )
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as exc:
        envelope = _extract_trailing_envelope(raw)
        if envelope is None:
            raise RuntimeError(
                f"Sandbox output is not valid JSON: {raw[:200]!r}. "
                f"Stderr: {stderr[:500] or '(empty)'}"
            ) from exc

    if not envelope.get("ok"):
        exc_type = envelope.get("type", "Error")
        exc_msg = envelope.get("error", "(unknown)")
        if origin_typed:
            raise SandboxEnvelopeError(exc_type, exc_msg)
        raise RuntimeError(f"{exc_type}: {exc_msg}")
    return envelope["result"]


# ---------------------------------------------------------------------------
# JS runner template shared by the subprocess and docker backends
# ---------------------------------------------------------------------------

# 用户代码（须定义 ``function run(input)``）嵌在模板顶部（模块级作用域），随后是
# 读 stdin、调用 run、写 JSON 信封的固定尾巴——同一个 eval 作用域内函数声明提升，
# 故尾巴能直接引用用户定义的 run。
_JS_RUNNER_TEMPLATE = textwrap.dedent("""\
    // ---- user code -------------------------------------------------------
    __USER_CODE__
    // ---- run -------------------------------------------------------------
    try {
      var _input = JSON.parse(require("fs").readFileSync(0, "utf8"));
      var _result = run(_input);
      process.stdout.write(JSON.stringify({
        ok: true,
        result: _result === undefined ? null : _result,
      }));
    } catch (_e) {
      process.stdout.write(JSON.stringify({
        ok: false,
        type: (_e && _e.name) || "Error",
        error: String(_e && _e.message ? _e.message : _e),
      }));
    }
""")

# 脚本经环境变量传进 node（而非 argv）：base64 后不进进程表，宿主 ``ps`` /
# 容器 ``docker inspect`` 都看不到用户代码。docker 与 subprocess 后端共用。
_JS_SCRIPT_ENV = "_PLAITA_SCRIPT"
_JS_BOOTSTRAP = (
    f"eval(Buffer.from(process.env.{_JS_SCRIPT_ENV},'base64').toString('utf8'))"
)


def _build_js_runner_script(user_code: str) -> str:
    """Embed *user_code* at column-0 of the JS runner template."""
    return _JS_RUNNER_TEMPLATE.replace(_PLACEHOLDER_CODE, user_code)


# ---------------------------------------------------------------------------
# JS runner
# ---------------------------------------------------------------------------

def _require_execjs():
    if execjs is None:
        raise ImportError(
            "PyExecJS is required for JavaScript code execution. "
            "Install it with: pip install plaita[code]"
        )


def run_js(code, input_value):
    """Execute JS *code* through PyExecJS — the ``"unsafe"`` backend only.

    .. warning::
        **No** isolation, **no** timeout, **no** cancellation: the external JS
        engine runs until it returns, with CommonJS ``require`` available and
        the host's full environment.  Flow authors cannot reach this path
        without an operator opt-in (language whitelist + backend whitelist);
        prefer :func:`run_js_docker` or :func:`run_js_subprocess`.
    """
    _require_execjs()
    context = execjs.compile(code)
    return context.call(JS_FUNC_NAME, input_value)


# ---------------------------------------------------------------------------
# Python runner — unsafe (historical behaviour, raw exec)
# ---------------------------------------------------------------------------

def run_python(code, *args, **kwargs):
    """Execute *code* with raw ``exec`` — no sandbox.

    .. warning::
        Provides **no isolation**. Use only when you fully trust the code author.
    """
    modules = import_modules(code)
    validate_run_function(code, kwargs)
    return execute_code(code, modules, *args, **kwargs)


def import_modules(code):
    tree = ast.parse(code)
    imports = [node for node in ast.walk(tree) if isinstance(node, ast.Import)]
    modules = {}
    for imp in imports:
        for name in imp.names:
            modules[name.name] = importlib.import_module(name.name)
    return modules


def validate_run_function(code, kwargs):
    tree = ast.parse(code)
    func_def = next(
        (node for node in ast.walk(tree)
         if isinstance(node, ast.FunctionDef) and node.name == PYTHON_FUNC_NAME),
        None,
    )
    if not func_def:
        raise ValueError(f"No {PYTHON_FUNC_NAME} function found")

    func_args_name = [arg.arg for arg in func_def.args.args]
    invalid_args = set(kwargs.keys()) - set(func_args_name)
    if invalid_args:
        raise ValueError(f'Invalid arguments: {", ".join(invalid_args)}')


def execute_code(code, modules, *args, **kwargs):
    exec(code, modules)  # noqa: S102
    return modules[PYTHON_FUNC_NAME](*args, **kwargs)


# ---------------------------------------------------------------------------
# Python runner — restricted sandbox (RestrictedPython)
# ---------------------------------------------------------------------------

def _make_safe_import(allowlist: Optional[FrozenSet[str]] = None):
    allowed = allowlist if allowlist is not None else SANDBOX_SAFE_MODULES

    def _safe_import(name, *args, **kwargs):
        mod_root = name.split(".")[0]
        if mod_root in allowed:
            return __import__(name, *args, **kwargs)
        raise ImportError(
            f"Module '{name}' is not allowed in the restricted sandbox. "
            f"Allowed root modules: {sorted(allowed)}. "
            "Set sandbox_backend='subprocess' or 'docker' for broader access, "
            "or 'unsafe' to disable the sandbox entirely."
        )
    return _safe_import


def _inplace_var(op, x, y):
    ops = {
        "+=": _op.add, "-=": _op.sub, "*=": _op.mul, "/=": _op.truediv,
        "//=": _op.floordiv, "%=": _op.mod, "**=": _op.pow,
        "&=": _op.and_, "|=": _op.or_, "^=": _op.xor,
        "<<=": _op.lshift, ">>=": _op.rshift,
    }
    fn = ops.get(op)
    if fn is None:
        raise TypeError(f"Unsupported in-place operator: {op}")
    return fn(x, y)


def run_python_restricted(code, input_value, *, extra_modules: Optional[FrozenSet[str]] = None):
    """Execute *code* inside a RestrictedPython sandbox.

    Only modules in :data:`SANDBOX_SAFE_MODULES` may be imported.
    Dangerous builtins (``open``, ``exec``, ``eval``, etc.) are stripped.
    The user code must define a ``run(input)`` function.
    """
    if not _RESTRICTED_AVAILABLE:
        raise ImportError(
            "RestrictedPython is required for sandbox_backend='restricted'. "
            "Install it with: pip install RestrictedPython"
        )

    allowlist = SANDBOX_SAFE_MODULES
    if extra_modules:
        allowlist = allowlist | frozenset(extra_modules)

    compiled = compile_restricted(code, "<plaita-sandbox>", "exec")
    namespace: dict = {
        "__builtins__": {**safe_builtins, "__import__": _make_safe_import(allowlist)},
        "__name__": "sandbox",
        "_getiter_": iter,
        "_getattr_": getattr,
        "_write_": lambda x: x,
        "_inplacevar_": _inplace_var,
        "_unpack_sequence_": guarded_iter_unpack_sequence,
    }
    exec(compiled, namespace)  # noqa: S102
    run_fn = namespace.get(PYTHON_FUNC_NAME)
    if run_fn is None:
        raise ValueError(
            f"No '{PYTHON_FUNC_NAME}' function found in restricted code. "
            "The script must define a function named 'run'."
        )
    return run_fn(input_value)


# ---------------------------------------------------------------------------
# Process-tree management (2026-09-30)
# ---------------------------------------------------------------------------

def _kill_process_tree(proc: subprocess.Popen) -> None:
    """对子进程的整个进程组发 SIGKILL。

    子进程以 ``start_new_session=True`` 启动（自成进程组），沙箱内代码再
    fork 出来的孙进程同组——killpg 一网打尽。历史上只杀直接子进程，沙箱
    超时后用户的子进程（如沙箱内再起的 CLI）全部成为孤儿。组已消失或
    平台限制时退化为杀直接子进程。

    安全护栏（2026-10-02 事故根因修复，缺一不可）：

    - ``target_pgid <= 1`` 拒杀——pgid 0/1 意味着 killpg(0/1)≡kill(0/-1)：
      前者屠杀调用方自身所在进程组，后者屠杀全机所有可杀进程。触发源是
      mock 对象的 ``pid`` 经 ``__index__`` 协议被解析成 0/1（getpgid 不抛
      TypeError、护栏不拦、kernel 直接整组 SIGKILL——2026-10-01~02 CI
      大规模「假停滞/137/日志失联」事故的最终根因）。
    - ``== os.getpgid(0)``（自身组）拒杀——子进程未按约定自成组时（spawn
      路径回归/mock 失真），killpg 会连测试/运行进程组一起带走。
    - pid 回收竞争：getpgid 与 killpg 之间目标 pid 可能易主他组——Linux 上
      用 /proc/<pid>/stat 的 pgrp 复核（两次读取一致才 killpg）；其他平台
      退化为 proc.kill()。
    """
    my_pgid = os.getpgid(0)
    target_pgid = None
    try:
        target_pgid = os.getpgid(proc.pid)
    except (TypeError, OSError):
        # TypeError：pid 非 int（测试 mock）；OSError：进程已消失
        pass
    # 不安全目标：pgid 0/1（killpg(0)≡kill(0)、killpg(1)≡kill(-1) 都会
    # 屠杀整组/全机）、等于自身组——一律退化为只杀直接子进程
    if target_pgid is not None and target_pgid > 1 and target_pgid != my_pgid:
        if sys.platform == "linux":
            # Linux：用 /proc/<pid>/stat 的 pgrp 复核（对抗 getpgid→killpg 之间
            # 的 pid 回收竞争——回收后 killpg 会误杀新主人的整组）。复核不过
            # （pid 已消失/易主）退化为 proc.kill()。
            try:
                stat_text = Path(f"/proc/{proc.pid}/stat").read_text()
                # comm 可含空格/括号：取右括号之后的字段——[0]=state [1]=ppid
                # [2]=pgrp（与 getpgid 对应的字段）
                pgrp = int(stat_text.rsplit(")", 1)[1].split()[2])
                if pgrp == target_pgid:
                    os.killpg(target_pgid, signal.SIGKILL)
                    return
            except (OSError, ValueError, IndexError):
                pass
        else:
            # macOS/Windows：无 /proc 复核（回收窗口极窄且历史无害），直接杀
            os.killpg(target_pgid, signal.SIGKILL)
            return
    try:
        proc.kill()
    except Exception:
        pass


def _popen_wait_cancellable(
    proc: subprocess.Popen,
    timeout: Optional[float],
    cancel_event,
    what: str,
    input_bytes: bytes,
) -> tuple:
    """可取消地等待 Popen：超时或 cancel_event 置位都 killpg 整组再抛。

    communicate 以 0.25s 步进重试（stdin 输入只在首调用传，超时续传是
    未定义行为）；两个结束通道都先把进程树杀干净、收回输出，再抛
    RuntimeError——调用方拿到异常的时刻进程已经不存在，不留孤儿。

    ``timeout`` 必须是有限正数（秒）：0/负值在历史上分别意味着「无限」和
    「误导性报错」，现在一律具名 ValueError（r39 A1）。校验失败时进程可能
    已经 spawn，先杀整组再抛，避免调用方拿到异常却留下裸进程。
    """
    try:
        timeout = _positive_timeout_seconds(f"{what} timeout", timeout)
    except ValueError:
        _kill_process_tree(proc)
        raise
    deadline = time.monotonic() + timeout
    first = True
    while True:
        try:
            if first:
                first = False
                return proc.communicate(input=input_bytes, timeout=0.25)
            return proc.communicate(timeout=0.25)
        except subprocess.TimeoutExpired:
            pass
        if cancel_event is not None and cancel_event.is_set():
            _kill_process_tree(proc)
            try:
                proc.communicate(timeout=1)
            except Exception:
                # 组外残留进程握住管道时会再超时——此时输出已不可得，直接放弃
                pass
            raise RuntimeError(f"{what} cancelled via cancel_event; process tree killed")
        if deadline is not None and time.monotonic() > deadline:
            _kill_process_tree(proc)
            try:
                proc.communicate(timeout=1)
            except Exception:
                pass
            raise RuntimeError(
                f"{what} timed out after {timeout}s (process tree killed). "
                "Increase the timeout env var or optimise the code."
            )


# ---------------------------------------------------------------------------
# Python runner — subprocess sandbox
# ---------------------------------------------------------------------------

class TenantContextPropagationError(RuntimeError):
    """非 default 租户无法在 subprocess 沙箱中安全执行（plaita#256 / A42-06）。

    子进程与 worker 同用户、文件系统不受限，且所有租户共用同一把解密密钥：
    无论父进程是否把凭据路径放进 env，子进程里的用户代码都能自行读取并解密
    default 租户文件，或 ``set_current_tenant("default")`` 改写路由。因此在
    spawn 之前就拒绝非 default 租户（用户代码零执行机会）；需要租户级 code
    节点时使用 ``docker`` 后端（容器内不挂载宿主凭据）。
    """


_SUBPROCESS_TENANT_ISOLATION_MARKER = (
    "PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE"
)


def _refuse_non_default_tenant_in_subprocess(language: str) -> None:
    """subprocess 后端 fail-closed：非 default 租户不进子进程（A42-06）。

    拒绝发生在 ``Popen`` 之前，用户代码完全没有执行机会。威胁模型是
    「租户作者半可信」：子进程继承宿主文件系统与租户共享的解密密钥，
    env 里传不传凭据路径都无法阻止其读取 default 凭据文件。
    """
    tenant_id = current_tenant()
    if tenant_id == _DEFAULT_TENANT_ID:
        return
    # fail-closed 闸门必须可观测：拒绝原因与租户/语言进 WARNING（不打印路径）。
    logger.warning(
        "subprocess sandbox refused non-default tenant=%s language=%s "
        "(%s); user code not executed",
        tenant_id, language, _SUBPROCESS_TENANT_ISOLATION_MARKER,
    )
    raise TenantContextPropagationError(
        f"{_SUBPROCESS_TENANT_ISOLATION_MARKER}: subprocess sandbox cannot "
        f"isolate credentials for tenant {tenant_id!r} (language={language}); "
        "the child would share the worker's file system and the tenant-wide "
        "credentials key, so its user code could read the default tenant's "
        "credentials. Refusing to run user code; use "
        "sandbox_backend='docker' for non-default tenants."
    )


def run_python_subprocess(code, input_value, cancel_event=None):
    """Execute *code* in a fresh Python subprocess.

    The child process is bounded by:

    * wall-clock timeout (``PLAITA_SANDBOX_TIMEOUT`` env var, default 10 s)
    * **optional** address-space limit on Linux (``PLAITA_SANDBOX_MEMORY_MB``
      env var; default ``0`` = unlimited — a non-zero value applies
      ``RLIMIT_AS`` and will kill node/pnpm-based commands at startup)
    * optional cooperative cancellation (``cancel_event`` — the code node's
      execution cancel flag; when set mid-run the whole process tree is
      killed and a RuntimeError raised)

    The child runs in its own process group (``start_new_session``); on
    timeout or cancel the **entire tree** is SIGKILLed — grandchildren spawned
    by the user code do not survive (2026-09-30 孤儿修复).

    File system and network access are **not** restricted — use the
    ``"docker"`` backend for full network isolation.

    Tenant boundary (plaita#256, A42-06): the child shares the worker's OS
    user and file system, and every tenant's credentials file is encrypted
    with the same key, so this backend is **not** a credential isolation
    boundary. Non-default tenants are refused before spawning
    (:class:`TenantContextPropagationError`, marker
    ``PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE``); their user code never
    runs. Use ``"docker"`` for tenant code. Default-tenant executions keep the
    historical semantics (including the credential routing env consumed by
    ``get_credential``).

    Input and output are serialised as JSON.  Only JSON-serialisable types
    are supported; complex Python objects (Pydantic models, custom classes)
    will fail at serialisation time.

    Raises
    ------
    RuntimeError
        If the subprocess times out, is cancelled, exits with a non-zero
        code, or the user code raises an exception.
    TenantContextPropagationError
        If the caller runs under a non-default tenant: this backend cannot
        isolate that tenant's credentials from the host file system.
    """
    _refuse_non_default_tenant_in_subprocess("python")
    mem_bytes = SANDBOX_SUBPROCESS_MEMORY_MB * 1024 * 1024
    runner = _build_runner_script(code, mem_bytes=mem_bytes)
    # 环境变量白名单重建（2026-09 安全评审 P1）：历史实现未传 env=，子进程
    # 拿到宿主全量 os.environ——生产环境里等于把 API key/云凭证交给沙箱内
    # 代码。白名单外可用 SUBPROCESS_ENV_EXTRA 按需补充。
    # 非 default 已在上方拒绝；这里的凭据路由仅服务 default 租户（其用户代码
    # 本就有权读取 default 凭据），不传 PLAITA_TENANT_ID——子进程没有租户
    # 引导，必须保持隐式 default 上下文。
    extra = subprocess_credentials_env()
    child_env = _env.build_subprocess_env(extra=extra)
    # 模块级 SUBPROCESS_ENV_EXTRA 可能残留租户变量；default 执行的身份
    # 由本进程决定，不能让子进程环境里的旧值把它改成别的租户。
    child_env.pop("PLAITA_TENANT_ID", None)
    child_env.pop("PLAITA_TENANT_SOURCE", None)
    proc = subprocess.Popen(
        [sys.executable, "-c", runner],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=child_env,
        start_new_session=True,
    )
    out, err = _popen_wait_cancellable(
        proc, SANDBOX_SUBPROCESS_TIMEOUT, cancel_event,
        "Subprocess sandbox", json.dumps(input_value).encode())

    stdout = out.decode(errors="replace") if out else ""
    stderr = err.decode(errors="replace") if err else ""

    if proc.returncode != 0 and not stdout.strip():
        raise RuntimeError(
            f"Subprocess exited with code {proc.returncode}. "
            f"Stderr: {stderr[:500] or '(empty)'}"
        )

    return _decode_runner_output(stdout, stderr, origin_typed=True)


# ---------------------------------------------------------------------------
# Python runner — Docker sandbox
# ---------------------------------------------------------------------------

def _docker_hardening_flags() -> list:
    """两种语言共用的容器加固参数（网络 / FS / 资源 / 能力收敛）。"""
    return [
        "--rm",
        "--network", "none",
        "--read-only",
        "--tmpfs", "/tmp",
        "--memory", f"{SANDBOX_DOCKER_MEMORY_MB}m",
        "--cpus", SANDBOX_DOCKER_CPUS,
        # 加固（2026-09 安全评审 P2）：防 fork bomb / 能力收敛 / 防提权。
        # --user 经 PLAITA_SANDBOX_DOCKER_USER 按需启用（默认镜像用户 root；
        # /tmp tmpfs 为 1777，nobody 可写）。网络已由 --network none 隔离。
        "--pids-limit", "64",
        "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        *(["--user", SANDBOX_DOCKER_USER] if SANDBOX_DOCKER_USER else []),
    ]


def _run_docker_container(container_name, cmd, timeout, cancel_event,
                          input_value, what, *, origin_typed: bool = False) -> Any:
    """跑一次性容器并解出 runner 的 JSON 信封（python / js docker 后端共用）。

    ``origin_typed`` 透传给 :func:`_decode_runner_output`（仅 python 信封
    携带内层类型名；见 :class:`SandboxEnvelopeError`）。

    超时或取消：``_popen_wait_cancellable`` 先 killpg 掉 ``docker run`` 客户端进程组，
    这里再 best-effort ``docker rm -f`` —— ``--rm`` 只在容器正常退出时生效，客户端被
    强杀后容器会残活。
    """
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Docker is not installed or not in PATH. "
            "Install Docker and ensure the daemon is running, "
            "or use sandbox_backend='subprocess' or 'restricted'."
        ) from exc
    try:
        out, err = _popen_wait_cancellable(
            proc, timeout, cancel_event, what, json.dumps(input_value).encode())
    except (RuntimeError, ValueError):
        # 客户端进程组已杀；容器本体归 dockerd 管，--rm 只在容器正常退出时
        # 生效——强杀客户端后必须显式 rm，否则容器残活。
        try:
            subprocess.run(
                ["docker", "rm", "-f", container_name],
                capture_output=True, timeout=15,
            )
        except Exception as exc:
            logger.debug("docker rm -f %s failed: %s", container_name, _scrub_exception_text(exc))
        raise

    stdout = out.decode(errors="replace") if out else ""
    stderr = err.decode(errors="replace") if err else ""

    if proc.returncode != 0 and not stdout.strip():
        stderr_text = stderr[:500] or "(empty)"
        # Detect common daemon-not-running scenarios so the error
        # message is actionable rather than a cryptic exit code.
        _daemon_down_signals = [
            "cannot connect to the docker daemon",
            "is the docker daemon running",
            "connection refused",
            "command not found",
        ]
        if any(sig in stderr_text.lower() for sig in _daemon_down_signals):
            raise RuntimeError(
                "Docker is not installed or the daemon is not running/accessible. "
                "Install Docker and start the daemon, or use "
                "sandbox_backend='subprocess' or 'restricted'."
            )
        raise RuntimeError(
            f"Docker container exited with code {proc.returncode}. "
            f"Stderr: {stderr_text}"
        )

    return _decode_runner_output(stdout, stderr, origin_typed=origin_typed)


def run_python_docker(code, input_value, cancel_event=None):
    """Execute *code* inside a one-shot Docker container.

    Isolation guarantees:

    * ``--network none`` — no outbound network access
    * ``--read-only`` — container file system is read-only (``/tmp`` writable)
    * ``--memory`` / ``--cpus`` — resource caps
    * Container is destroyed immediately after execution (``--rm``)

    Requires Docker (or a compatible daemon) to be installed and running.
    Configure via environment variables:

    * ``PLAITA_SANDBOX_DOCKER_IMAGE`` (default ``python:3.12-slim``)
    * ``PLAITA_SANDBOX_DOCKER_TIMEOUT`` (seconds, default 30)
    * ``PLAITA_SANDBOX_DOCKER_MEMORY_MB`` (MB, default 128)
    * ``PLAITA_SANDBOX_DOCKER_CPUS`` (default ``"0.5"``)

    Input and output are serialised as JSON.

    Implementation note
    -------------------
    The runner script is base64-encoded and passed via the ``_PLAITA_SCRIPT``
    environment variable.  The container entry-point is a one-liner that decodes
    and ``exec``s it; JSON input arrives via stdin.  This avoids both the
    ``python -`` pipe-conflict and volume-mount issues (e.g. colima's sshfs only
    exposes the home directory, so ``/var/folders`` temp files are inaccessible
    inside the VM).

    Timeout/cancel (2026-09-30): the container gets a unique ``--name``; the
    wait loop kills the whole ``docker run`` client process group and runs a
    best-effort ``docker rm -f`` so the container does not outlive the call.
    """
    runner = _build_runner_script(code, mem_bytes=0)  # resource limits via Docker flags
    runner_b64 = base64.b64encode(runner.encode()).decode()
    container_name = f"plaita-sbx-{uuid.uuid4().hex[:12]}"

    cmd = [
        "docker", "run",
        *_docker_hardening_flags(),
        "--name", container_name,
        "-i",
        "-e", f"_PLAITA_SCRIPT={runner_b64}",
        SANDBOX_DOCKER_IMAGE,
        "python", "-c",
        "import sys,base64,os; exec(base64.b64decode(os.environ['_PLAITA_SCRIPT']).decode())",
    ]

    return _run_docker_container(
        container_name, cmd, SANDBOX_DOCKER_TIMEOUT, cancel_event,
        input_value, "Docker sandbox", origin_typed=True)


# ---------------------------------------------------------------------------
# JS runner — subprocess sandbox
# ---------------------------------------------------------------------------

def run_js_subprocess(code, input_value, cancel_event=None):
    """Execute JS *code* in a fresh ``node`` subprocess sandboxed like Python's.

    Bounds (identical shape to :func:`run_python_subprocess`):

    * wall-clock timeout (``PLAITA_SANDBOX_TIMEOUT``, default 10 s)
    * cooperative cancellation (``cancel_event``) — timeout and cancel both
      SIGKILL the whole process group (``start_new_session=True``), so a
      runaway JS loop cannot hang the worker step (2026-10 安全评审 P1：此前的
      PyExecJS 路径既无超时也无取消)
    * environment allowlist (``plaita.subprocess_env``) instead of the host's
      full ``os.environ``

    The script itself is passed base64-encoded via ``_PLAITA_SCRIPT`` rather
    than argv, so user code does not show up in the host process table.

    File system and network access are **not** restricted — use the ``"docker"``
    backend for that.

    Tenant boundary (A42-06): like the Python backend, the node child shares
    the worker's OS user and file system, so non-default tenants are refused
    before spawning (:class:`TenantContextPropagationError`); their user code
    never runs. Use ``"docker"`` for tenant code.
    """
    _refuse_non_default_tenant_in_subprocess(LANGUAGE_JS)
    script_b64 = base64.b64encode(_build_js_runner_script(code).encode()).decode()
    child_env = _env.build_subprocess_env(extra={_JS_SCRIPT_ENV: script_b64})
    try:
        proc = subprocess.Popen(
            [SANDBOX_NODE_BIN, "-e", _JS_BOOTSTRAP],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=child_env,
            start_new_session=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Node.js executable {SANDBOX_NODE_BIN!r} not found "
            "(PLAITA_SANDBOX_NODE_BIN). language='js' with "
            "sandbox_backend='subprocess' needs Node.js on PATH; use "
            "sandbox_backend='docker' or 'unsafe' instead."
        ) from exc
    out, err = _popen_wait_cancellable(
        proc, SANDBOX_SUBPROCESS_TIMEOUT, cancel_event,
        "JS subprocess sandbox", json.dumps(input_value).encode())

    stdout = out.decode(errors="replace") if out else ""
    stderr = err.decode(errors="replace") if err else ""

    if proc.returncode != 0 and not stdout.strip():
        raise RuntimeError(
            f"JS subprocess exited with code {proc.returncode}. "
            f"Stderr: {stderr[:500] or '(empty)'}"
        )

    return _decode_runner_output(stdout, stderr)


# ---------------------------------------------------------------------------
# JS runner — Docker sandbox
# ---------------------------------------------------------------------------

def run_js_docker(code, input_value, cancel_event=None):
    """Execute JS *code* inside a one-shot container of a Node image.

    Isolation is the same as :func:`run_python_docker` (``--network none``,
    ``--read-only``, memory/CPU/pids caps, ``--cap-drop ALL``,
    ``no-new-privileges``); only the image and entrypoint differ
    (``PLAITA_SANDBOX_DOCKER_NODE_IMAGE``, default ``node:20-alpine``).
    The container is removed on exit, and timeouts/cancels kill the ``docker
    run`` client process group plus a best-effort ``docker rm -f``.
    """
    script_b64 = base64.b64encode(_build_js_runner_script(code).encode()).decode()
    container_name = f"plaita-js-sbx-{uuid.uuid4().hex[:12]}"

    cmd = [
        "docker", "run",
        *_docker_hardening_flags(),
        "--name", container_name,
        "-i",
        "-e", f"{_JS_SCRIPT_ENV}={script_b64}",
        SANDBOX_DOCKER_NODE_IMAGE,
        "node", "-e", _JS_BOOTSTRAP,
    ]

    return _run_docker_container(
        container_name, cmd, SANDBOX_DOCKER_TIMEOUT, cancel_event,
        input_value, "JS docker sandbox")


# ---------------------------------------------------------------------------
# Runner registry
# ---------------------------------------------------------------------------

# 自定义语言 runner（``register_runner``）——运营者在宿主进程内注册的代码。
# 内置语言（python / js）不走这里：它们按 ``_BACKENDS`` 的语言×档位表执行，
# 否则流程作者声明的 ``sandbox_backend`` 会被绕过（2026-10 安全评审 P1）。
Runners = {LANGUAGE_JS: run_js, LANGUAGE_PYTHON: run_python}

_PYTHON_BACKENDS = {
    "restricted": run_python_restricted,
    "subprocess": run_python_subprocess,
    "docker": run_python_docker,
    "unsafe": run_python,
}

# JS 无 ``restricted`` 档：RestrictedPython 是 Python 专用 AST 沙箱，没有 JS 对应
# 实现——静默降级（例如 js 声明 restricted 却跑宿主 execjs）正是本单要堵的洞，
# 故该档位在解析期直接拒绝。
_JS_BACKENDS = {
    "subprocess": run_js_subprocess,
    "docker": run_js_docker,
    "unsafe": run_js,
}

_BACKENDS = {
    LANGUAGE_PYTHON: _PYTHON_BACKENDS,
    LANGUAGE_JS: _JS_BACKENDS,
}

_BUILTIN_LANGUAGES = frozenset(_BACKENDS)


def register_runner(language, runner):
    """Register a custom language runner.

    自定义语言（如 ``"ruby"``）不受 ``allowed_languages`` 约束——runner 是运营者
    自己写进进程的代码，不是流程作者提供的。内置语言 ``"python"`` / ``"js"`` 不能
    这样替换：它们必须走 ``sandbox_backend`` 档位表，注册会抛 ``ValueError``。
    """
    if language in _BUILTIN_LANGUAGES:
        raise ValueError(
            f"{language!r} is a built-in language and cannot be replaced by "
            "register_runner; choose the isolation level per node via "
            "sandbox_backend instead."
        )
    Runners[language] = runner


def _check_language_allowed(language: str) -> None:
    """语言白名单硬校验（``register_code_node(allowed_languages=...)``）。"""
    if language not in _ALLOWED_LANGUAGES:
        raise ValueError(
            f"language={language!r} is not allowed by the operator. "
            f"Allowed languages: {sorted(_ALLOWED_LANGUAGES)}. "
            "JavaScript execution must be enabled explicitly via "
            "register_code_node(allowed_languages=(...)) (deployments can set it "
            "from PLAITA_SANDBOX_ALLOWED_LANGUAGES); it cannot be enabled per flow."
        )


# ---------------------------------------------------------------------------
# CodeNode
# ---------------------------------------------------------------------------

class CodeNode(Node):
    """Execute user-supplied code.

    Fields
    ------
    language : str
        ``"python"`` (default) or ``"js"``.  ``"js"`` must be enabled by the
        operator via ``register_code_node(allowed_languages=...)``.
    code : str
        Source code.  Python: must define a ``run(input)`` function.
        JS: must define a ``run`` function.
    input : Any
        Passed as the single argument to ``run``.  Supports flow expressions.
    sandbox_backend : Optional[str]
        Isolation level, applied to both languages。``None`` 时取模块级默认
        (``_DEFAULT_SANDBOX_BACKEND``, 0.5.0 起 ``"docker"``; 由
        ``register_code_node(default_backend=...)`` 在启动期设定)。

        * ``"docker"`` (默认) — Docker 容器, 网络+FS 隔离。
        * ``"restricted"`` — RestrictedPython in-process 沙箱 (AST 级, 有绕过向量;
          **仅 python**, js 声明它会在解析期报错)。
        * ``"subprocess"`` — fresh process (python exec / node), 资源+时间受限。
        * ``"unsafe"`` — raw ``exec`` / PyExecJS, 无沙箱、无超时。

    .. warning::
        ``CodeNode`` executes arbitrary user-supplied code.  It is **not**
        in the default NodeRegistry; call ``register_code_node()`` to opt in.
    """

    node_type: ClassVar[str] = "code"
    dry_run_safe: ClassVar[bool] = False
    # 任意代码执行须在宿主沙箱档位内跑（sandbox_backend 白名单体系，plaita#22/#29）。
    execution_requirements: ClassVar[tuple] = ("sandbox",)
    node_name: ClassVar[str] = "代码"

    language: Optional[str] = None
    code: Optional[str] = None
    input: Optional[Any] = None
    sandbox_backend: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def validate_code_node(cls, data):
        # 未显式指定后端时取模块级默认 (0.5.0 默认 docker)。
        if not data.get("sandbox_backend"):
            data["sandbox_backend"] = _DEFAULT_SANDBOX_BACKEND
        # 运营者白名单硬校验（2026-09 安全评审 P0）：流程 JSON 逐节点把
        # sandbox_backend 覆盖成 unsafe/subprocess 必须在解析期拦下，而不是
        # 等到执行期才生效——运营者的 register_code_node 选择是安全边界。
        allowed = _ALLOWED_SANDBOX_BACKENDS
        if allowed is not None and data["sandbox_backend"] not in allowed:
            raise ValueError(
                f"sandbox_backend={data['sandbox_backend']!r} is not allowed by the "
                f"operator. Allowed backends: {sorted(allowed)}. "
                "This restriction is set via register_code_node(allowed_backends=...) "
                "and cannot be overridden per flow."
            )
        if data.get("language") is None:
            data["language"] = LANGUAGE_PYTHON
        language = data["language"]
        # 字面量内置语言：语言白名单 + 「该语言有没有这个档位」都在解析期判定。
        # 动态 language 表达式（``"$INPUT.language"``）解析期拿不到值，留到
        # execute 期同一套校验兜底。未知字面量语言不在此拦（可能是
        # register_runner 注册的自定义语言，只有 execute 期知道）。
        if (isinstance(language, str) and not language.startswith("$")
                and language in _BACKENDS):
            _check_language_allowed(language)
            if data["sandbox_backend"] not in _BACKENDS[language]:
                raise ValueError(
                    f"sandbox_backend={data['sandbox_backend']!r} has no "
                    f"{language!r} implementation. Supported backends for "
                    f"language {language!r}: {sorted(_BACKENDS[language])}."
                )
        if language == LANGUAGE_PYTHON:
            if not data.get("code"):
                raise ValueError("Python code is required when language is python")
            try:
                tree = ast.parse(data["code"])
                func_def = next(
                    (node for node in ast.walk(tree)
                     if isinstance(node, ast.FunctionDef)
                     and node.name == PYTHON_FUNC_NAME),
                    None,
                )
                if not func_def:
                    raise ValueError(f"No {PYTHON_FUNC_NAME} function found")
            except ValueError as e:
                raise ValueError(f"Python code validation failed: {e}") from e
        return data

    def execute(self, execution) -> Any:
        language = execution.evaluate(self.language)
        code = execution.evaluate(self.code)
        input_value = execution.evaluate(self.input)

        backends = _BACKENDS.get(language)
        if backends is not None:
            # 动态 language 表达式的兜底：白名单与档位在此才可判定。
            _check_language_allowed(language)
            backend_fn = backends.get(self.sandbox_backend)
            if backend_fn is None:
                raise ValueError(
                    f"Unknown sandbox_backend={self.sandbox_backend!r} for "
                    f"language={language!r}. Supported: {sorted(backends)}"
                )
            if self.sandbox_backend in ("subprocess", "docker"):
                # 协作式取消：cancel_event 置位时子进程树整组击杀（2026-09-30）
                return backend_fn(
                    code, input_value,
                    cancel_event=getattr(execution, "cancel_event", None),
                )
            return backend_fn(code, input_value)

        if language in Runners:
            return Runners[language](code, input_value)

        raise ValueError(f"Unsupported language: {language}")
