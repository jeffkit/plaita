"""plaita#29 回归：``language: "js"`` 必须走沙箱档位体系，不能再绕开。

历史实现::

    Runners = {LANGUAGE_JS: run_js, LANGUAGE_PYTHON: run_python}
    if language in Runners:                 # js 直接命中 → execjs 裸跑
        return Runners[language](code, input_value)

``run_js`` = PyExecJS（subprocess 拉起外部 JS 引擎，CommonJS ``require`` 可用），
无 timeout、无 cancel_event；而 ``_PYTHON_BACKENDS`` / ``sandbox_backend`` /
``allowed_backends`` 全部只作用于 Python。运营者配 ``allowed_backends=("docker",)``，
流程作者写一行 ``language: "js"`` 即绕过整套边界：宿主任意代码执行 + 读 worker 全量
env + 死循环 JS 挂死 worker 步骤（Python 路径的 10s/30s 超时对它不生效）。

本文件覆盖：
1. 语言白名单默认 fail-closed（js 须运营者显式放行）+ 语言×档位校验；
2. js 的 subprocess / docker 后端命令构造（镜像 / 加固参数 / env / base64 脚本）；
3. 超时、取消、进程组击杀、容器清理；
4. 生产入口（worker）接线：默认拒 js，env 放行后可用。
"""
from __future__ import annotations

import ast
import base64
import json
import shutil
import subprocess
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from plaita.node import code as code_mod

JS_CODE = "function run(input) { return input + 1; }"
ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# helpers / fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sandbox_globals(monkeypatch):
    """归一化并还原 ``code`` 模块级沙箱全局 + 默认注册表（防跨用例污染）。"""
    from plaita.node import get_default_registry

    registry = get_default_registry()
    saved = (
        code_mod._DEFAULT_SANDBOX_BACKEND,
        code_mod._ALLOWED_SANDBOX_BACKENDS,
        code_mod._ALLOWED_LANGUAGES,
        registry.get("code"),
    )
    for env in ("PLAITA_SANDBOX_ALLOWED_LANGUAGES", "PLAITA_SANDBOX_ALLOWED_BACKENDS",
                "PLAITA_CODE_BACKEND"):
        monkeypatch.delenv(env, raising=False)
    code_mod._ALLOWED_SANDBOX_BACKENDS = None
    code_mod._ALLOWED_LANGUAGES = frozenset(code_mod.DEFAULT_SANDBOX_ALLOWED_LANGUAGES)

    yield code_mod

    (code_mod._DEFAULT_SANDBOX_BACKEND, code_mod._ALLOWED_SANDBOX_BACKENDS,
     code_mod._ALLOWED_LANGUAGES, saved_node) = saved
    if saved_node is None:
        registry.unregister("code")
    else:
        registry.register(saved_node)


@pytest.fixture()
def js_enabled(sandbox_globals):
    """模拟运维放行 js（``register_code_node(allowed_languages=...)``）。"""
    sandbox_globals._ALLOWED_LANGUAGES = frozenset({"python", "js"})
    sandbox_globals._DEFAULT_SANDBOX_BACKEND = "subprocess"
    return sandbox_globals


def _js_node_dict(**overrides):
    data = {
        "type": "code",
        "id": "c",
        "language": "js",
        "code": JS_CODE,
        "sandbox_backend": "docker",
    }
    data.update(overrides)
    return data


def _registry():
    from plaita.node import NodeRegistry

    return NodeRegistry()


def _ok_proc(result=None):
    """Popen 替身：communicate 立即返回 JSON 信封。"""
    proc = MagicMock()
    proc.returncode = 0
    proc.communicate.return_value = (
        json.dumps({"ok": True, "result": result}).encode(), b"")
    return proc


def _script_from_subprocess(mock_popen) -> str:
    """从 mock 的 Popen 调用里取回 node 要 eval 的 base64 脚本。"""
    script_b64 = mock_popen.call_args.kwargs["env"][code_mod._JS_SCRIPT_ENV]
    return base64.b64decode(script_b64).decode()


def _script_from_docker_cmd(cmd) -> str:
    env_arg = next(a for a in cmd if a.startswith(f"{code_mod._JS_SCRIPT_ENV}="))
    return base64.b64decode(env_arg.split("=", 1)[1]).decode()


# ---------------------------------------------------------------------------
# 1. 语言白名单（默认 fail-closed）+ 语言×档位
# ---------------------------------------------------------------------------

def test_default_language_whitelist_is_python_only(sandbox_globals):
    """库默认只放行 python：js 必须由运营者显式放行（fail-closed）。"""
    assert code_mod.DEFAULT_SANDBOX_ALLOWED_LANGUAGES == ("python",)
    assert sandbox_globals._ALLOWED_LANGUAGES == frozenset({"python"})
    # js 的档位表没有 restricted（RestrictedPython 是 Python 专用 AST 沙箱）
    assert set(code_mod._JS_BACKENDS) == {"subprocess", "docker", "unsafe"}
    assert "restricted" not in code_mod._JS_BACKENDS


def test_js_node_rejected_at_parse_by_default(sandbox_globals):
    """默认语言白名单下提交 js 节点 → 解析期拒绝。

    验收臂 1：运营者配 ``allowed_backends=("docker",)`` 后 ``language: js`` 不得通过。
    """
    from plaita.node import register_code_node

    reg = _registry()
    with patch.object(code_mod, "_docker_available", return_value=True):
        register_code_node(registry=reg, default_backend="docker",
                           allowed_backends=("docker",))

    with pytest.raises(ValueError) as ctx:
        reg.parse_node(_js_node_dict())
    message = str(ctx.value)
    assert "language='js' is not allowed by the operator" in message
    assert "register_code_node(allowed_languages=" in message

    # 同配置下 python 节点不受影响
    node = reg.parse_node({"type": "code", "id": "p", "code": "def run(i): return i"})
    assert node.sandbox_backend == "docker"


def test_js_node_accepted_when_operator_enables_it(sandbox_globals):
    """运营者放行 js 后，字面量 js 节点按声明档位解析通过。"""
    from plaita.node import register_code_node

    reg = _registry()
    register_code_node(registry=reg, default_backend="subprocess",
                       allowed_backends=("subprocess", "docker"),
                       allowed_languages=("python", "js"))
    node = reg.parse_node(_js_node_dict(sandbox_backend="subprocess"))
    assert node.language == "js"
    assert node.sandbox_backend == "subprocess"


def test_js_rejected_on_backend_without_js_implementation(sandbox_globals):
    """js + restricted：没有 js 实现 → 解析期拒绝，不静默降级到宿主 execjs。"""
    from plaita.node import register_code_node

    reg = _registry()
    register_code_node(registry=reg, default_backend="subprocess",
                       allowed_languages=("python", "js"))
    with pytest.raises(ValueError) as ctx:
        reg.parse_node(_js_node_dict(sandbox_backend="restricted"))
    assert "has no 'js' implementation" in str(ctx.value)
    assert "['docker', 'subprocess', 'unsafe']" in str(ctx.value)


def test_js_dynamic_language_expression_checked_at_execute(sandbox_globals):
    """``language: "$INPUT.language"``（动态）在 execute 期兜同一套校验。"""
    node = code_mod.CodeNode(id="c", language="$INPUT.language", code="$INPUT.code",
                             input="$INPUT.input", sandbox_backend="subprocess")
    execution = MagicMock()
    execution.evaluate.side_effect = lambda value: {
        "$INPUT.language": "js", "$INPUT.code": JS_CODE, "$INPUT.input": 1,
    }[value]
    with pytest.raises(ValueError) as ctx:
        node.execute(execution)
    assert "language='js' is not allowed by the operator" in str(ctx.value)


def test_custom_language_needs_no_allowlisting(js_enabled):
    """自定义语言（register_runner）是运营者自己的代码，不受语言白名单约束。"""
    from plaita.node.code import register_runner

    register_runner("ruby", lambda code, value: f"ruby:{value}")
    try:
        node = code_mod.CodeNode(id="c", language="ruby", code="x = 1",
                                 input=7, sandbox_backend="unsafe")
        execution = MagicMock()
        execution.evaluate.side_effect = lambda value: value
        assert node.execute(execution) == "ruby:7"
    finally:
        code_mod.Runners.pop("ruby", None)


def test_register_runner_cannot_replace_builtin_language(js_enabled):
    """内置语言不能经 register_runner 绕过档位表。"""
    from plaita.node.code import register_runner

    for language in ("python", "js"):
        with pytest.raises(ValueError) as ctx:
            register_runner(language, lambda code, value: value)
        assert "built-in language" in str(ctx.value)
    assert code_mod.Runners["js"] is code_mod.run_js
    assert code_mod.Runners["python"] is code_mod.run_python


def test_unknown_language_still_unsupported(js_enabled):
    """未注册的语言照旧 execute 期报错。"""
    node = code_mod.CodeNode(id="c", language="python", code="def run(i): return i",
                             sandbox_backend="unsafe")
    node.language = "cobol"
    execution = MagicMock()
    execution.evaluate.side_effect = lambda value: value
    with pytest.raises(ValueError) as ctx:
        node.execute(execution)
    assert "Unsupported language: cobol" in str(ctx.value)


# ---------------------------------------------------------------------------
# 2. JS subprocess 后端
# ---------------------------------------------------------------------------

def test_js_subprocess_argv_env_and_timeout(js_enabled):
    """node 子进程：脚本走 env（不进进程表）+ env 白名单 + 新会话隔离。"""
    proc = _ok_proc(result=8)
    with patch("subprocess.Popen", return_value=proc) as mock_popen, \
            patch.dict("os.environ", {"PLAITA_SECRET_TOKEN": "leak-me"}, clear=False):
        assert code_mod.run_js_subprocess(JS_CODE, 7) == 8

    argv = mock_popen.call_args.args[0]
    assert argv == [code_mod.SANDBOX_NODE_BIN, "-e", code_mod._JS_BOOTSTRAP]

    kwargs = mock_popen.call_args.kwargs
    assert kwargs["start_new_session"] is True
    assert "PLAITA_SECRET_TOKEN" not in kwargs["env"]
    script = _script_from_subprocess(mock_popen)
    assert JS_CODE in script
    # 包装脚本的契约：读 stdin 的 JSON、两条信封分支（ok/失败带 type+error）
    assert "readFileSync(0" in script
    assert "ok: true" in script
    assert "ok: false" in script
    assert "type:" in script and "error:" in script
    # 用户代码不进 argv（ps / docker inspect 看不到）
    assert all(JS_CODE not in str(arg) for arg in argv)
    # 输入经 stdin 传 JSON
    assert proc.communicate.call_args.kwargs["input"] == b"7"


def test_js_subprocess_timeout_kills_process_tree(js_enabled, monkeypatch):
    """墙钟超时 → 整组击杀 + RuntimeError（js 死循环不再挂死 worker 步骤）。"""
    proc = MagicMock()
    proc.communicate.side_effect = subprocess.TimeoutExpired(cmd="node", timeout=1)
    monkeypatch.setattr(code_mod, "SANDBOX_SUBPROCESS_TIMEOUT", 0.01)
    with patch("subprocess.Popen", return_value=proc), \
            patch.object(code_mod, "_kill_process_tree") as kill:
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_subprocess("while (true) {}", None)
    assert "JS subprocess sandbox timed out" in str(ctx.value)
    assert "process tree killed" in str(ctx.value)
    assert kill.called


def test_js_subprocess_cancel_event_kills_process_tree(js_enabled):
    """cancel_event 置位 → 整组击杀 + RuntimeError（协作式取消）。"""
    cancel_event = threading.Event()
    cancel_event.set()
    proc = MagicMock()
    proc.communicate.side_effect = subprocess.TimeoutExpired(cmd="node", timeout=1)
    with patch("subprocess.Popen", return_value=proc), \
            patch.object(code_mod, "_kill_process_tree") as kill:
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_subprocess("while (true) {}", None, cancel_event=cancel_event)
    assert "cancelled via cancel_event" in str(ctx.value)
    assert kill.called


def test_js_subprocess_missing_node_binary_is_actionable(js_enabled):
    """宿主无 node → 可行动的 RuntimeError（而不是 FileNotFoundError 裸抛）。"""
    with patch("subprocess.Popen", side_effect=FileNotFoundError("no node")):
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_subprocess(JS_CODE, 1)
    message = str(ctx.value)
    assert "PLAITA_SANDBOX_NODE_BIN" in message
    assert "Node.js" in message


def test_js_subprocess_nonzero_exit_without_envelope(js_enabled):
    """非零退出且 stdout 为空 → RuntimeError 带上 stderr。"""
    proc = MagicMock()
    proc.returncode = 1
    proc.communicate.return_value = (b"", b"SyntaxError: boom")
    with patch("subprocess.Popen", return_value=proc):
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_subprocess("function run( {", 1)
    assert "JS subprocess exited with code 1" in str(ctx.value)
    assert "SyntaxError: boom" in str(ctx.value)


def test_js_subprocess_error_envelope_is_raised(js_enabled):
    """runner 信封 ok=false → RuntimeError（类型 + 消息）。"""
    proc = MagicMock()
    proc.returncode = 0
    proc.communicate.return_value = (
        json.dumps({"ok": False, "type": "ReferenceError",
                    "error": "run is not defined"}).encode(), b"")
    with patch("subprocess.Popen", return_value=proc):
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_subprocess("var x = 1;", 1)
    assert "ReferenceError: run is not defined" in str(ctx.value)


# ---------------------------------------------------------------------------
# 3. JS docker 后端
# ---------------------------------------------------------------------------

def test_js_docker_command_and_script(js_enabled, monkeypatch):
    """js docker 档：node 镜像 + 加固参数 + 脚本经 env base64。"""
    monkeypatch.setattr(code_mod, "SANDBOX_DOCKER_NODE_IMAGE", "node:20-alpine")
    proc = _ok_proc(result=3)
    with patch("subprocess.Popen", return_value=proc) as mock_popen:
        assert code_mod.run_js_docker(JS_CODE, 2) == 3

    cmd = mock_popen.call_args.args[0]
    assert cmd[:3] == ["docker", "run", "--rm"]
    assert cmd[cmd.index("--network") + 1] == "none"
    assert "--read-only" in cmd
    assert cmd[cmd.index("--cap-drop") + 1] == "ALL"
    assert "--security-opt" in cmd
    assert "--pids-limit" in cmd
    assert "--memory" in cmd and "--cpus" in cmd
    assert "node:20-alpine" in cmd
    assert cmd[-3:] == ["node", "-e", code_mod._JS_BOOTSTRAP]
    assert cmd[cmd.index("--name") + 1].startswith("plaita-js-sbx-")
    assert JS_CODE in _script_from_docker_cmd(cmd)
    # 用户代码不进 argv
    assert all(JS_CODE not in str(arg) for arg in cmd)
    assert mock_popen.call_args.kwargs["start_new_session"] is True


def test_python_and_js_docker_share_hardening(js_enabled):
    """两种语言的 docker 后端共用同一套加固参数（防加固漂移）。"""
    flags = code_mod._docker_hardening_flags()
    for flag in ("--rm", "--network", "--read-only", "--tmpfs", "--memory",
                 "--cpus", "--pids-limit", "--cap-drop", "--security-opt"):
        assert flag in flags
    assert flags[flags.index("--network") + 1] == "none"
    assert flags[flags.index("--cap-drop") + 1] == "ALL"
    assert flags[flags.index("--tmpfs") + 1] == "/tmp"
    assert flags[flags.index("--memory") + 1] == f"{code_mod.SANDBOX_DOCKER_MEMORY_MB}m"
    assert flags[flags.index("--cpus") + 1] == code_mod.SANDBOX_DOCKER_CPUS
    assert flags[flags.index("--pids-limit") + 1] == "64"
    assert flags[flags.index("--security-opt") + 1] == "no-new-privileges"

    with patch("subprocess.Popen", return_value=_ok_proc(result=1)) as py_popen:
        code_mod.run_python_docker("def run(i):\n    return i", 1)
    with patch("subprocess.Popen", return_value=_ok_proc(result=1)) as js_popen:
        code_mod.run_js_docker("function run(i) { return i; }", 1)
    py_cmd = py_popen.call_args.args[0]
    js_cmd = js_popen.call_args.args[0]
    shared = ["docker", "run", *flags]
    assert py_cmd[:len(shared)] == shared
    assert js_cmd[:len(shared)] == shared
    assert "python" in py_cmd
    assert "node" in js_cmd


def test_docker_hardening_user_flag_follows_env(js_enabled, monkeypatch):
    """``PLAITA_SANDBOX_DOCKER_USER`` 非空时追加 ``--user``（默认不追加）。"""
    assert "--user" not in code_mod._docker_hardening_flags()
    monkeypatch.setattr(code_mod, "SANDBOX_DOCKER_USER", "65534:65534")
    flags = code_mod._docker_hardening_flags()
    assert flags[flags.index("--user") + 1] == "65534:65534"


def test_js_docker_timeout_removes_container(js_enabled, monkeypatch):
    """超时：killpg 客户端 + ``docker rm -f`` 清理残活容器 + RuntimeError。"""
    proc = MagicMock()
    proc.communicate.side_effect = subprocess.TimeoutExpired(cmd="docker", timeout=30)
    monkeypatch.setattr(code_mod, "SANDBOX_DOCKER_TIMEOUT", 0.01)
    with patch("subprocess.Popen", return_value=proc), \
            patch("subprocess.run") as mock_run:
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_docker("while (true) {}", None)
    assert "JS docker sandbox timed out" in str(ctx.value)
    assert any("rm" in str(call) for call in mock_run.call_args_list)


def test_js_docker_daemon_down_is_actionable(js_enabled):
    """daemon 不可用 → 可行动的报错信息（共用容器层）。"""
    proc = MagicMock()
    proc.returncode = 1
    proc.communicate.return_value = (
        b"", b"Cannot connect to the Docker daemon is the docker daemon running")
    with patch("subprocess.Popen", return_value=proc):
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_docker(JS_CODE, 1)
    assert "Docker is not installed or the daemon" in str(ctx.value)


def test_js_docker_nonzero_exit_without_envelope(js_enabled):
    """非零退出且无信封 → 通用 docker 报错。"""
    proc = MagicMock()
    proc.returncode = 2
    proc.communicate.return_value = (b"", b"some other error")
    with patch("subprocess.Popen", return_value=proc):
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_docker(JS_CODE, 1)
    assert "Docker container exited with code 2" in str(ctx.value)


def test_js_docker_cancel_removes_container(js_enabled):
    """取消：同样清理容器（不留孤儿）。"""
    cancel_event = threading.Event()
    cancel_event.set()
    proc = MagicMock()
    proc.communicate.side_effect = subprocess.TimeoutExpired(cmd="docker", timeout=30)
    with patch("subprocess.Popen", return_value=proc), \
            patch("subprocess.run") as mock_run:
        with pytest.raises(RuntimeError) as ctx:
            code_mod.run_js_docker(JS_CODE, None, cancel_event=cancel_event)
    assert "cancelled via cancel_event" in str(ctx.value)
    assert any("rm" in str(call) for call in mock_run.call_args_list)


# ---------------------------------------------------------------------------
# 4. CodeNode.execute → 后端路由 / 生产入口接线
# ---------------------------------------------------------------------------

def test_execute_js_routes_through_declared_backend(js_enabled):
    """js 节点按 sandbox_backend 路由（不再固定走 execjs）。"""
    node = code_mod.CodeNode(id="c", language="js", code=JS_CODE, input=5,
                             sandbox_backend="docker")
    execution = MagicMock()
    execution.evaluate.side_effect = lambda value: value
    execution.cancel_event = None
    docker_fn = MagicMock(return_value=6)
    with patch.dict(code_mod._JS_BACKENDS, {"docker": docker_fn}):
        assert node.execute(execution) == 6
    docker_fn.assert_called_once_with(JS_CODE, 5, cancel_event=None)


def test_execute_js_subprocess_passes_cancel_event(js_enabled):
    """js subprocess 档把 execution.cancel_event 传给 runner（此前 js 无取消）。"""
    cancel_event = threading.Event()
    node = code_mod.CodeNode(id="c", language="js", code=JS_CODE, input=5,
                             sandbox_backend="subprocess")
    execution = MagicMock()
    execution.evaluate.side_effect = lambda value: value
    execution.cancel_event = cancel_event
    sub_fn = MagicMock(return_value=6)
    with patch.dict(code_mod._JS_BACKENDS, {"subprocess": sub_fn}):
        assert node.execute(execution) == 6
    sub_fn.assert_called_once_with(JS_CODE, 5, cancel_event=cancel_event)


def test_execute_js_unsafe_backend_keeps_execjs_path(js_enabled):
    """显式声明 unsafe 时保留历史 PyExecJS 路径（仅此一档）。"""
    node = code_mod.CodeNode(id="c", language="js", code=JS_CODE, input=5,
                             sandbox_backend="unsafe")
    execution = MagicMock()
    execution.evaluate.side_effect = lambda value: value
    execjs_fn = MagicMock(return_value=6)
    with patch.dict(code_mod._JS_BACKENDS, {"unsafe": execjs_fn}):
        assert node.execute(execution) == 6
    execjs_fn.assert_called_once_with(JS_CODE, 5)


def test_worker_entry_rejects_js_by_default(sandbox_globals):
    """worker 入口：未配置语言白名单时 js 节点解析期被拒。"""
    pytest.importorskip("cachetools")
    pytest.importorskip("redis")

    from plaita.node import get_default_registry
    from plaita.server import flow_worker as fw

    with patch.object(code_mod, "_docker_available", return_value=True):
        fw._register_code_node_for_worker()
    assert code_mod._ALLOWED_LANGUAGES == frozenset({"python"})

    with pytest.raises(ValueError) as ctx:
        get_default_registry().parse_node({
            "type": "code", "id": "j", "language": "js", "code": JS_CODE,
        })
    assert "is not allowed by the operator" in str(ctx.value)


def test_worker_entry_honours_language_env(sandbox_globals, monkeypatch):
    """worker 入口：PLAITA_SANDBOX_ALLOWED_LANGUAGES 放行 js 后可用。"""
    pytest.importorskip("cachetools")
    pytest.importorskip("redis")

    from plaita.node import get_default_registry
    from plaita.server import flow_worker as fw

    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_LANGUAGES", "python, js")
    monkeypatch.setenv("PLAITA_CODE_BACKEND", "subprocess")
    fw._register_code_node_for_worker()
    assert code_mod._ALLOWED_LANGUAGES == frozenset({"python", "js"})

    node = get_default_registry().parse_node({
        "type": "code", "id": "j", "language": "js", "code": JS_CODE,
    })
    assert node.language == "js"
    assert node.sandbox_backend == "subprocess"


def test_resolve_sandbox_allowed_languages_defaults_and_env(sandbox_globals, monkeypatch):
    """语言解析器：默认只 python；env 解析；非法值报错；python 始终保留。"""
    from plaita.node import resolve_sandbox_allowed_languages

    assert resolve_sandbox_allowed_languages("flow-worker") == ("python",)

    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_LANGUAGES", "js")
    assert resolve_sandbox_allowed_languages("flow-worker") == ("js", "python")

    # 空白分隔 + 两侧空白都要能解析（运维手写 env 的常见形态）
    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_LANGUAGES", " python js ")
    assert resolve_sandbox_allowed_languages("flow-worker") == ("js", "python")

    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_LANGUAGES", "python bogus")
    with pytest.raises(ValueError, match="unknown language"):
        resolve_sandbox_allowed_languages("flow-worker")


def test_resolve_sandbox_allowed_languages_warns_on_js(sandbox_globals, monkeypatch, caplog):
    """显式放行 js 不静默：WARNING 指出后果（供运维排查）。"""
    import logging

    from plaita.node import resolve_sandbox_allowed_languages

    monkeypatch.setenv("PLAITA_SANDBOX_ALLOWED_LANGUAGES", "python,js")
    with caplog.at_level(logging.WARNING, logger="plaita.node"):
        allowed = resolve_sandbox_allowed_languages("flow-worker")
    assert allowed == ("js", "python")
    assert any(r.levelno == logging.WARNING and "js" in r.getMessage()
               for r in caplog.records)


def test_register_code_node_rejects_unknown_language(sandbox_globals):
    """register_code_node(allowed_languages=...) 校验取值。"""
    from plaita.node import register_code_node

    with pytest.raises(ValueError, match="unknown language"):
        register_code_node(registry=_registry(), default_backend="subprocess",
                           allowed_languages=("python", "ruby"))


def test_register_code_node_rejects_unknown_backend(sandbox_globals):
    """后端白名单校验与语言白名单同源（plaita#22 契约不回退）。"""
    from plaita.node import register_code_node

    with pytest.raises(ValueError, match="unknown sandbox backend"):
        register_code_node(registry=_registry(), default_backend="subprocess",
                           allowed_backends=("docker", "bogus"))


# --- 接线守卫（镜像 plaita#22 的 allowed_backends 守卫） ---------------------

ENTRY_FILES = (
    ROOT / "plaita" / "server" / "flow_worker.py",
    ROOT / "plaita-console" / "backend" / "main.py",
    ROOT / "plaita-console" / "backend" / "api" / "cluster.py",
)


def test_production_entries_pass_allowed_languages():
    """部署入口注册 CodeNode 时必须传 `allowed_languages=`。

    漏传不会让 js 变得可执行（模块默认仍只放行 python），但会让
    `PLAITA_SANDBOX_ALLOWED_LANGUAGES` 静默失效——运营者以为放行了 js，流程
    却在解析期被拒。
    """
    seen = set()
    missing = []
    for path in ENTRY_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            func = node.func if isinstance(node, ast.Call) else None
            name = getattr(func, "id", None) or getattr(func, "attr", None)
            if name != "register_code_node":
                continue
            seen.add(path)
            if "allowed_languages" not in {kw.arg for kw in node.keywords}:
                missing.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert not missing, f"生产入口 register_code_node 必须传 allowed_languages=...：{missing}"
    assert seen == set(ENTRY_FILES), f"入口未接线：{ENTRY_FILES}"


# ---------------------------------------------------------------------------
# 5. 真跑（有 node / node 镜像的环境才执行）
# ---------------------------------------------------------------------------

_NODE_AVAILABLE = shutil.which(code_mod.SANDBOX_NODE_BIN) is not None


@pytest.mark.skipif(not _NODE_AVAILABLE, reason="no node runtime on PATH")
def test_js_subprocess_end_to_end(js_enabled):
    """真 node 子进程：JSON 输入 → run() → JSON 输出。"""
    assert code_mod.run_js_subprocess(JS_CODE, 41) == 42
    assert code_mod.run_js_subprocess(
        "function run(input) { return {doubled: input * 2}; }", 3) == {"doubled": 6}


@pytest.mark.skipif(not _NODE_AVAILABLE, reason="no node runtime on PATH")
def test_js_subprocess_error_end_to_end(js_enabled):
    """真 node：用户异常经信封回传为 RuntimeError。"""
    with pytest.raises(RuntimeError) as ctx:
        code_mod.run_js_subprocess(
            "function run(input) { throw new Error('boom'); }", 1)
    assert "boom" in str(ctx.value)


@pytest.mark.skipif(not _NODE_AVAILABLE, reason="no node runtime on PATH")
def test_js_subprocess_timeout_end_to_end(js_enabled, monkeypatch):
    """真 node 死循环：墙钟超时生效（历史 execjs 路径会永久挂住）。"""
    monkeypatch.setattr(code_mod, "SANDBOX_SUBPROCESS_TIMEOUT", 1)
    with pytest.raises(RuntimeError) as ctx:
        code_mod.run_js_subprocess("function run(input) { while (true) {} }", 1)
    assert "timed out" in str(ctx.value)


def _node_image_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        proc = subprocess.run(
            ["docker", "images", code_mod.SANDBOX_DOCKER_NODE_IMAGE,
             "--format", "{{.Repository}}"],
            capture_output=True, timeout=10,
        )
    except Exception:
        return False
    return proc.returncode == 0 and b"node" in proc.stdout


@pytest.mark.skipif(not _node_image_available(),
                    reason="node docker image not available locally")
def test_js_docker_end_to_end(js_enabled):
    """真容器：js 在 docker（node 镜像）内执行。"""
    assert code_mod.run_js_docker("function run(input) { return input + 1; }", 41) == 42
