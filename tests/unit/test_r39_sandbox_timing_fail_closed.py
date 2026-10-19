"""r39 面① A1/A9 回归：沙箱计时参数 fail-closed。

- ``timeout=0`` 历史被 ``deadline = (...) if timeout else None`` 当成
  「无限」，与引擎语义「0=零预算立即超时」相反——部署脚本把未设置的变量
  展开成 0 时，沙箱墙钟护栏整体消失。
- 负值历史进入 deadline 检查后报误导性的 ``timed out after -1s``。
- 文档曾声称 CPU-time 预算，实现只有墙钟（已改文档对齐实现）。

回归形态：env 在 import 期具名拒绝；直接调用等待原语也拒绝并杀进程树。
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from plaita.node.code import _popen_wait_cancellable

REPO_ROOT = Path(__file__).resolve().parents[2]


def _import_code_module(**env_overrides):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-c", "import plaita.node.code"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )


@pytest.mark.parametrize(
    "var",
    ["PLAITA_SANDBOX_TIMEOUT", "PLAITA_SANDBOX_DOCKER_TIMEOUT"],
)
@pytest.mark.parametrize("raw", ["0", "-1"])
def test_non_positive_timeout_rejected_at_import(var, raw):
    """0/负值不再在 import 期被接受（0 静默=无限，负值报误导文案）。"""
    proc = _import_code_module(**{var: raw})
    assert proc.returncode != 0, (
        f"{var}={raw} 在 import 期被接受——0 会静默变成无限，负值会报 "
        "'timed out after -1s'"
    )
    assert f"{var} must be a positive integer" in proc.stderr
    assert "0 is a zero budget" in proc.stderr


def _spawn_sleeper(seconds=1.0):
    return subprocess.Popen(
        [sys.executable, "-c", f"import time; time.sleep({seconds})"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )


@pytest.mark.parametrize("timeout", [0, -1, "-1", float("inf")])
def test_wait_rejects_invalid_timeout_and_kills_child(timeout):
    """直接调用等待原语：非法 timeout 立即具名失败，且不留裸进程。"""
    proc = _spawn_sleeper()
    started = time.monotonic()
    try:
        with pytest.raises(ValueError, match="timeout must be a positive"):
            _popen_wait_cancellable(proc, timeout, None, "unit sandbox", b"")
    finally:
        elapsed = time.monotonic() - started
    assert elapsed < 1.0, (
        f"非法 timeout={timeout!r} 未立即失败，耗时 {elapsed:.2f}s"
    )
    assert proc.wait(timeout=2) is not None, "非法 timeout 后子进程未被回收"
    assert proc.returncode != 0, "非法 timeout 后子进程仍存活"


def test_positive_timeout_still_completes():
    """正数路径不受 fail-closed 校验影响。"""
    proc = _spawn_sleeper(0.05)
    out, err = _popen_wait_cancellable(proc, 5, None, "unit sandbox", b"")
    assert proc.returncode == 0
    assert out == b"" and err == b""
