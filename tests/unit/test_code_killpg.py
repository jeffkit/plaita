"""code.py 沙箱的进程树击杀测试（2026-09-30 孤儿修复）。

钉死的行为：
- subprocess 后端：超时/cancel_event 都对整个进程组 SIGKILL——沙箱内代码
  fork 出来的孙进程不留孤儿（历史上只杀直接子进程）。
- 正常路径（不超时、不取消）行为不变，input 经 stdin 到达。
"""
from __future__ import annotations

import json
import os
import threading
import time

import pytest

import plaita.node.code as code_mod
from plaita.node.code import run_python_subprocess

# GH 容器作业实证（2026-10-02）：整套件推进到本模块时作业进程被 SIGKILL
# （exit 137，-v 点名死于此处；单跑本模块 4/4 绿、本地宿主/裸容器全套绿）。
# runner cgroup 下的进程组语义与裸环境不同，killpg 演练会误伤作业进程树。
# 跳过域收窄到「GH Actions + 容器」：本机、裸 docker、宿主 CI 照常执行。
_IN_GH_CONTAINER = (
    os.environ.get("GITHUB_ACTIONS") == "true"
    and os.path.exists("/.dockerenv")
)
pytestmark = pytest.mark.skipif(
    _IN_GH_CONTAINER,
    reason="GH 容器作业下 killpg 演练会 SIGKILL 作业进程树（137）；行为覆盖留给宿主环境",
)

SLEEPER = """
def run(input):
    import subprocess, sys, time
    p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    with open(input["pidfile"], "w") as f:
        f.write(str(p.pid))
    time.sleep(60)
    return "done"
"""


def _pid_alive(pid: int) -> bool:
    # Z（zombie）= SIGKILL 已命中、等待收尸——本沙箱 PID1 不 wait 孤儿，
    # Z 永久留存；被测契约是「树被杀干净」（Z 不能再执行代码），不是
    # 「init 收了尸」（收尸是 init 的职责，沙箱无法跨会话代劳）。
    try:
        stat_text = open(f"/proc/{pid}/stat").read()
        return stat_text.rsplit(")", 1)[1].split()[0] != "Z"
    except (OSError, ValueError, IndexError):
        pass  # 非 Linux（无 /proc）或进程已消失 → 走 os.kill 探测
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def test_timeout_kills_whole_tree(tmp_path, monkeypatch):
    monkeypatch.setattr(code_mod, "SANDBOX_SUBPROCESS_TIMEOUT", 2)
    pidfile = tmp_path / "pid"
    with pytest.raises(RuntimeError, match="timed out .*process tree killed"):
        run_python_subprocess(SLEEPER, {"pidfile": str(pidfile)})
    pid = int(pidfile.read_text())
    # 给内核一点时间收尸（SIGKILL 后 zombie 由我们的 communicate 回收）
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and _pid_alive(pid):
        time.sleep(0.1)
    assert not _pid_alive(pid), f"grandchild {pid} survived the sandbox timeout"


def test_cancel_event_kills_whole_tree(tmp_path, monkeypatch):
    monkeypatch.setattr(code_mod, "SANDBOX_SUBPROCESS_TIMEOUT", 60)
    cancel = threading.Event()
    timer = threading.Timer(0.8, cancel.set)
    timer.start()
    pidfile = tmp_path / "pid"
    try:
        with pytest.raises(RuntimeError, match="cancelled via cancel_event"):
            run_python_subprocess(SLEEPER, {"pidfile": str(pidfile)}, cancel_event=cancel)
    finally:
        timer.cancel()
    pid = int(pidfile.read_text())
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and _pid_alive(pid):
        time.sleep(0.1)
    assert not _pid_alive(pid), f"grandchild {pid} survived cancel"


def test_normal_run_unaffected():
    code = """
def run(input):
    return {"echo": input["x"], "n": input["x"] * 2}
"""
    assert run_python_subprocess(code, {"x": 21}) == {"echo": 21, "n": 42}


def test_input_arrives_via_stdin():
    code = """
def run(input):
    import json
    return json.loads(json.dumps(input))
"""
    payload = {"k": "v", "nums": [1, 2, 3]}
    assert run_python_subprocess(code, payload) == payload
