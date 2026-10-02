"""_kill_process_tree 安全护栏回归——2026-10-02 CI 集体击杀事故。

事故链（详见 .github/workflows/ci.yml 注释与本文件）：
``os.getpgid(mock.pid)`` 经 MagicMock 的 ``__index__`` 协议解析成 0/1，
``killpg(1)`` ≡ ``kill(-1)`` 屠杀全机同 uid 进程——GH 托管 runner agent
被回杀（作业假停滞/137/日志失联）、crypto 上 ubuntu 生产服务集体死亡。

护栏契约：mock/已消失 pid（pgid 解析失败）、pgid<=1、自身组——一律退化为
proc.kill()；只有真实子进程组（/proc pgrp 复核一致）才走 killpg。
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from unittest.mock import MagicMock, patch

import pytest

import plaita.node.code as code_mod
from plaita.node.code import _kill_process_tree


@pytest.fixture
def kill_spy():
    """监视 os.killpg / proc.kill 的调用。"""
    with patch.object(code_mod.os, "killpg") as mock_killpg:
        yield mock_killpg


def _mock_proc():
    proc = MagicMock()
    proc.pid = 424242
    return proc


def test_mock_pid_does_not_killpg_anything(kill_spy):
    """MagicMock pid：getpgid 经 __index__ 解析可能返回 0/1——两个都是
    灾难性 killpg 目标（0=自身组、1≡kill(-1) 全机），护栏必须全退化。"""
    proc = _mock_proc()
    # 让 os.getpgid 返回 mock 的 __index__ 结果（复现事故路径）
    with patch.object(code_mod.os, "getpgid", return_value=proc.pid.__index__()):
        _kill_process_tree(proc)
    killpg_calls = kill_spy.call_args_list
    for call in killpg_calls:
        args, _ = call
        assert args[0] not in (0, 1), f"killpg 落在危险组 {args[0]}"
    assert proc.kill.called


def test_mock_pid_index_one_falls_back_to_proc_kill(kill_spy):
    """__index__ 返回 1 的最坏情形：不得 killpg(1)≡kill(-1)。"""
    proc = _mock_proc()
    with patch.object(code_mod.os, "getpgid", return_value=1):
        _kill_process_tree(proc)
    killpg_calls = kill_spy.call_args_list
    assert not any(call.args[0] == 1 for call in killpg_calls)
    assert proc.kill.called


def test_real_child_group_is_killed():
    """真实子进程（start_new_session 自成组）超时 → 整组确实被杀（护栏
    不得把合法击杀也屏蔽）。"""
    proc = subprocess.Popen(
        ["python3", "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        time.sleep(0.3)  # 让子进程完成 setsid
        child_pgid = os.getpgid(proc.pid)
        assert child_pgid > 1
        with patch.object(code_mod.os, "killpg", wraps=code_mod.os.killpg) as spy:
            _kill_process_tree(proc)
        assert any(
            call.args[0] == child_pgid for call in spy.call_args_list
        ), "合法子进程组未被 killpg"
    finally:
        try:
            proc.kill()
        except OSError:
            pass


def test_recycled_pid_falls_back_to_proc_kill(kill_spy):
    """getpgid 与 /proc pgrp 不一致（pid 已回收易主）→ 退化 proc.kill，
    不得 killpg 他组（同机其他服务误杀防线）。"""
    proc = _mock_proc()
    # pid 为真实整数（getpgid 成功），但 /proc/<pid>/stat 不存在或 pgrp 不符
    with patch.object(code_mod.os, "getpgid", return_value=999999), \
            patch("plaita.node.code.Path") as mock_path:
        mock_path.return_value.read_text.side_effect = OSError("gone")
        _kill_process_tree(proc)
    assert not kill_spy.called, "pgid 复核失败仍走了 killpg"
    assert proc.kill.called
