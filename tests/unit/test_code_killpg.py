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
