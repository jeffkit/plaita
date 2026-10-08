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
    configurable CPU-time and wall-clock timeouts, and an **optional**
    address-space limit on Linux.  Safer than ``"restricted"`` against code
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
"""

from __future__ import annotations

import ast
import base64
import importlib
import json
import logging
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
    except Exception:
        logger.debug("docker daemon probe failed", exc_info=True)
        return False

JS_FUNC_NAME = "run"
PYTHON_FUNC_NAME = "run"

LANGUAGE_JS = "js"
LANGUAGE_PYTHON = "python"

# ---------------------------------------------------------------------------
# Sandbox configuration (env-var overrides)
# ---------------------------------------------------------------------------

# subprocess backend
SANDBOX_SUBPROCESS_TIMEOUT: int = int(os.environ.get("PLAITA_SANDBOX_TIMEOUT", "10"))
# RLIMIT_AS 上限（MB）。默认 0 = 不限制（与 macOS 上的历史实际行为一致）。
# 2026-10-06 事故：默认 256 在 Linux 上真实生效，node/pnpm 类命令启动即 V8 OOM
# （V8 启动期预留远超 256MB 的地址空间，RLIMIT_AS 卡的是虚拟地址而非 RSS），
# 输出只剩截断崩溃栈；macOS 不强制 RLIMIT_AS，故本地部署从未暴露。设 >0 才
# 启用限制——小内存纯 Python 脚本场景可显式收紧，node 系命令请保持 0。
SANDBOX_SUBPROCESS_MEMORY_MB: int = int(os.environ.get("PLAITA_SANDBOX_MEMORY_MB", "0"))

# subprocess 后端的子进程环境变量白名单（2026-09 安全评审 P1）：历史上子进程
# 继承宿主全量 os.environ。实现已抽到公共层 ``plaita.subprocess_env``（gate /
# capture / agent 直跑等 plaita-nodes 节点共用），这里 re-export 保持既有导入点
# 零改动；需要额外变量时改 ``SUBPROCESS_ENV_EXTRA``（同一 dict 对象）或调用点传
# ``extra=``。
SUBPROCESS_ENV_ALLOWLIST = _env.SUBPROCESS_ENV_ALLOWLIST
SUBPROCESS_ENV_EXTRA = _env.SUBPROCESS_ENV_EXTRA

# docker backend
SANDBOX_DOCKER_IMAGE: str = os.environ.get("PLAITA_SANDBOX_DOCKER_IMAGE", "python:3.12-slim")
SANDBOX_DOCKER_TIMEOUT: int = int(os.environ.get("PLAITA_SANDBOX_DOCKER_TIMEOUT", "30"))
SANDBOX_DOCKER_MEMORY_MB: int = int(os.environ.get("PLAITA_SANDBOX_DOCKER_MEMORY_MB", "128"))
SANDBOX_DOCKER_CPUS: str = os.environ.get("PLAITA_SANDBOX_DOCKER_CPUS", "0.5")
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
    import sys as _sys, json as _json

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


def _build_runner_script(user_code: str, mem_bytes: int = 0) -> str:
    """Embed *user_code* inside the runner template.

    The user code is inserted at column-0 (module level).  Uses
    ``str.replace`` rather than ``str.format`` or ``string.Template`` so that
    arbitrary Python code (including dict literals containing ``{`` / ``}``)
    is safe.
    """
    script = _RUNNER_TEMPLATE.replace(_PLACEHOLDER_CODE, user_code)
    script = script.replace(_PLACEHOLDER_MEM, str(mem_bytes))
    return script


def _decode_runner_output(raw: str, stderr: str) -> Any:
    """Parse the JSON envelope written by the runner script."""
    if not raw.strip():
        raise RuntimeError(
            f"Sandbox produced no output. Stderr: {stderr[:500] or '(empty)'}"
        )
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Sandbox output is not valid JSON: {raw[:200]!r}. "
            f"Stderr: {stderr[:500] or '(empty)'}"
        ) from exc

    if not envelope.get("ok"):
        exc_type = envelope.get("type", "Error")
        exc_msg = envelope.get("error", "(unknown)")
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
    """
    deadline = (time.monotonic() + timeout) if timeout else None
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

    Input and output are serialised as JSON.  Only JSON-serialisable types
    are supported; complex Python objects (Pydantic models, custom classes)
    will fail at serialisation time.

    Raises
    ------
    RuntimeError
        If the subprocess times out, is cancelled, exits with a non-zero
        code, or the user code raises an exception.
    """
    mem_bytes = SANDBOX_SUBPROCESS_MEMORY_MB * 1024 * 1024
    runner = _build_runner_script(code, mem_bytes=mem_bytes)
    # 环境变量白名单重建（2026-09 安全评审 P1）：历史实现未传 env=，子进程
    # 拿到宿主全量 os.environ——生产环境里等于把 API key/云凭证交给沙箱内
    # 代码。白名单外可用 SUBPROCESS_ENV_EXTRA 按需补充。
    child_env = _env.build_subprocess_env()
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

    return _decode_runner_output(stdout, stderr)


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
                          input_value, what) -> Any:
    """跑一次性容器并解出 runner 的 JSON 信封（python / js docker 后端共用）。

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
    except RuntimeError:
        # 客户端进程组已杀；容器本体归 dockerd 管，--rm 只在容器正常退出时
        # 生效——强杀客户端后必须显式 rm，否则容器残活。
        try:
            subprocess.run(
                ["docker", "rm", "-f", container_name],
                capture_output=True, timeout=15,
            )
        except Exception:
            logger.debug("docker rm -f %s failed", container_name, exc_info=True)
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

    return _decode_runner_output(stdout, stderr)


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
        input_value, "Docker sandbox")


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
    """
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
