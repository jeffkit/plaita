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
    configurable CPU-time and wall-clock timeouts, and a memory soft limit
    on Linux.  Safer than ``"restricted"`` against code that bypasses
    RestrictedPython's AST guards, but does **not** block network or file I/O.
    Input/output is serialised as JSON; only JSON-serialisable types are
    supported.  Environment variables: ``PLAITA_SANDBOX_TIMEOUT`` (seconds,
    default 10), ``PLAITA_SANDBOX_MEMORY_MB`` (MB, default 256).

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
import subprocess
import sys
import textwrap
from typing import Any, ClassVar, FrozenSet, Optional

from pydantic import model_validator

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
SANDBOX_SUBPROCESS_MEMORY_MB: int = int(os.environ.get("PLAITA_SANDBOX_MEMORY_MB", "256"))

# subprocess 后端的子进程环境变量白名单（2026-09 安全评审 P1）：
# 历史上子进程继承宿主全量 os.environ。需要额外变量时往 SUBPROCESS_ENV_EXTRA
# 里加（模块级，启动脚本里设置），或直接改这个白名单。
SUBPROCESS_ENV_ALLOWLIST: FrozenSet[str] = frozenset({
    "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE", "PYTHONIOENCODING",
})
SUBPROCESS_ENV_EXTRA: dict = {}

# docker backend
SANDBOX_DOCKER_IMAGE: str = os.environ.get("PLAITA_SANDBOX_DOCKER_IMAGE", "python:3.12-slim")
SANDBOX_DOCKER_TIMEOUT: int = int(os.environ.get("PLAITA_SANDBOX_DOCKER_TIMEOUT", "30"))
SANDBOX_DOCKER_MEMORY_MB: int = int(os.environ.get("PLAITA_SANDBOX_DOCKER_MEMORY_MB", "128"))
SANDBOX_DOCKER_CPUS: str = os.environ.get("PLAITA_SANDBOX_DOCKER_CPUS", "0.5")
# 容器运行用户（如 "65534:65534"）。默认空 = 镜像默认用户（root）。
# python:3.12-slim 的 /tmp tmpfs 为 1777，nobody 可写；设了用户后沙箱内
# 代码写非 /tmp 路径会失败——这是预期约束而非 bug。
SANDBOX_DOCKER_USER: str = os.environ.get("PLAITA_SANDBOX_DOCKER_USER", "")

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
# JS runner
# ---------------------------------------------------------------------------

def _require_execjs():
    if execjs is None:
        raise ImportError(
            "PyExecJS is required for JavaScript code execution. "
            "Install it with: pip install plaita[code]"
        )


def run_js(code, input_value):
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
# Python runner — subprocess sandbox
# ---------------------------------------------------------------------------

def run_python_subprocess(code, input_value):
    """Execute *code* in a fresh Python subprocess.

    The child process is bounded by:

    * wall-clock timeout (``PLAITA_SANDBOX_TIMEOUT`` env var, default 10 s)
    * soft memory limit on Linux (``PLAITA_SANDBOX_MEMORY_MB``, default 256 MB)

    File system and network access are **not** restricted — use the
    ``"docker"`` backend for full network isolation.

    Input and output are serialised as JSON.  Only JSON-serialisable types
    are supported; complex Python objects (Pydantic models, custom classes)
    will fail at serialisation time.

    Raises
    ------
    RuntimeError
        If the subprocess times out, exits with a non-zero code, or the
        user code raises an exception.
    """
    mem_bytes = SANDBOX_SUBPROCESS_MEMORY_MB * 1024 * 1024
    runner = _build_runner_script(code, mem_bytes=mem_bytes)
    # 环境变量白名单重建（2026-09 安全评审 P1）：历史实现未传 env=，子进程
    # 拿到宿主全量 os.environ——生产环境里等于把 API key/云凭证交给沙箱内
    # 代码。白名单外可用 SUBPROCESS_ENV_EXTRA 按需补充。
    child_env = {
        key: os.environ[key]
        for key in sorted(SUBPROCESS_ENV_ALLOWLIST)
        if key in os.environ
    }
    child_env.update(SUBPROCESS_ENV_EXTRA)
    try:
        proc = subprocess.run(
            [sys.executable, "-c", runner],
            input=json.dumps(input_value).encode(),
            capture_output=True,
            timeout=SANDBOX_SUBPROCESS_TIMEOUT,
            env=child_env,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Subprocess sandbox timed out after {SANDBOX_SUBPROCESS_TIMEOUT}s. "
            "Increase PLAITA_SANDBOX_TIMEOUT or optimise the code."
        ) from exc

    stdout = proc.stdout.decode(errors="replace")
    stderr = proc.stderr.decode(errors="replace")

    if proc.returncode != 0 and not stdout.strip():
        raise RuntimeError(
            f"Subprocess exited with code {proc.returncode}. "
            f"Stderr: {stderr[:500] or '(empty)'}"
        )

    return _decode_runner_output(stdout, stderr)


# ---------------------------------------------------------------------------
# Python runner — Docker sandbox
# ---------------------------------------------------------------------------

def run_python_docker(code, input_value):
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
    """
    runner = _build_runner_script(code, mem_bytes=0)  # resource limits via Docker flags
    runner_b64 = base64.b64encode(runner.encode()).decode()

    cmd = [
        "docker", "run",
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
        "-i",
        "-e", f"_PLAITA_SCRIPT={runner_b64}",
        SANDBOX_DOCKER_IMAGE,
        "python", "-c",
        "import sys,base64,os; exec(base64.b64decode(os.environ['_PLAITA_SCRIPT']).decode())",
    ]

    try:
        proc = subprocess.run(
            cmd,
            input=json.dumps(input_value).encode(),
            capture_output=True,
            timeout=SANDBOX_DOCKER_TIMEOUT,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Docker is not installed or not in PATH. "
            "Install Docker and ensure the daemon is running, "
            "or use sandbox_backend='subprocess' or 'restricted'."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Docker sandbox timed out after {SANDBOX_DOCKER_TIMEOUT}s."
        ) from exc

    stdout = proc.stdout.decode(errors="replace")
    stderr = proc.stderr.decode(errors="replace")

    if proc.returncode != 0 and not stdout.strip():
        stderr_text = stderr[:500] or "(empty)"
        # Detect common daemon-not-running scenarios so the error
        # message is actionable rather than a cryptic exit code.
        _daemon_down_signals = [
            "cannot connect to the docker daemon",
            "is the docker daemon running",
            "connection refused",
        ]
        if any(sig in stderr_text.lower() for sig in _daemon_down_signals):
            raise RuntimeError(
                "Docker daemon is not running or not accessible. "
                "Start Docker and try again, or use "
                "sandbox_backend='subprocess' or 'restricted'."
            )
        raise RuntimeError(
            f"Docker container exited with code {proc.returncode}. "
            f"Stderr: {stderr_text}"
        )

    return _decode_runner_output(stdout, stderr)


# ---------------------------------------------------------------------------
# Runner registry
# ---------------------------------------------------------------------------

Runners = {LANGUAGE_JS: run_js, LANGUAGE_PYTHON: run_python}

_PYTHON_BACKENDS = {
    "restricted": run_python_restricted,
    "subprocess": run_python_subprocess,
    "docker": run_python_docker,
    "unsafe": run_python,
}


def register_runner(language, runner):
    """Register a custom language runner (or replace an existing one)."""
    Runners[language] = runner


# ---------------------------------------------------------------------------
# CodeNode
# ---------------------------------------------------------------------------

class CodeNode(Node):
    """Execute user-supplied code.

    Fields
    ------
    language : str
        ``"python"`` (default) or ``"js"``.
    code : str
        Source code.  Python: must define a ``run(input)`` function.
        JS: must define a ``run`` function.
    input : Any
        Passed as the single argument to ``run``.  Supports flow expressions.
    sandbox_backend : Optional[str]
        Python-only isolation level。``None`` 时取模块级默认
        (``_DEFAULT_SANDBOX_BACKEND``, 0.5.0 起 ``"docker"``; 由
        ``register_code_node(default_backend=...)`` 在启动期设定)。

        * ``"docker"`` (默认) — Docker 容器, 网络+FS 隔离。
        * ``"restricted"`` — RestrictedPython in-process 沙箱 (AST 级, 有绕过向量)。
        * ``"subprocess"`` — fresh Python process, 资源受限。
        * ``"unsafe"`` — raw ``exec``, 无沙箱。

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
            data["language"] = "python"
        if data["language"] == "python":
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

        if language == LANGUAGE_PYTHON:
            backend_fn = _PYTHON_BACKENDS.get(self.sandbox_backend)
            if backend_fn is None:
                raise ValueError(
                    f"Unknown sandbox_backend={self.sandbox_backend!r}. "
                    f"Supported: {list(_PYTHON_BACKENDS)}"
                )
            return backend_fn(code, input_value)

        if language in Runners:
            return Runners[language](code, input_value)

        raise ValueError(f"Unsupported language: {language}")
