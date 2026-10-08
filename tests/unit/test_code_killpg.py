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


def _wait_gone(pid: int, seconds: float = 5.0) -> bool:
    """等被杀的孙进程不再运行。本沙箱（无 init 的 PID namespace）里 SIGKILL
    后的孙进程无人收尸，永远停留在 Z (zombie)——os.kill(pid, 0) 对 zombie
    也成功，恰好误报「幸存」。Z 状态即判定已死：killpg 的被测行为是「信号
    送达且不再执行」，收尸归属不是断言对象。"""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            with open(f"/proc/{pid}/stat", "rb") as f:
                stat = f.read().decode(errors="replace")
            # comm 可含空格/括号：状态字段取右括号之后的第一个
            state = stat.rsplit(")", 1)[1].split()[0]
            if state == "Z":
                return True
        except (OSError, IndexError):
            return True
        time.sleep(0.05)
    return False


def test_timeout_kills_whole_tree(tmp_path, monkeypatch):
    monkeypatch.setattr(code_mod, "SANDBOX_SUBPROCESS_TIMEOUT", 2)
    pidfile = tmp_path / "pid"
    with pytest.raises(RuntimeError, match="timed out .*process tree killed"):
        run_python_subprocess(SLEEPER, {"pidfile": str(pidfile)})
    pid = int(pidfile.read_text())
    assert _wait_gone(pid), f"grandchild {pid} survived the sandbox timeout"


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
    assert _wait_gone(pid), f"grandchild {pid} survived cancel"


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
