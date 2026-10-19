"""A2R19-01/02：``brain=claude`` 的失败终态与子进程生命周期。

台账 r19 §2 两格（均 P2）：

- A2R19-01：provider 401/500/流中 500、``result.is_error=true`` 曾被包成
  ``RunFinished success``（stderr 进 DEVNULL、returncode 未检查）；
- A2R19-02：客户端 ``aclose`` 只断流不杀 CLI，探针 ``still_alive_after_aclose=True``。

本文件用可控 fake CLI（``PLAITA_CONSOLE_CLAUDE_CLI`` 指向脚本）复现四格错误
终态 + 断开/超时清理；负控为修复前实现（401 格报 success、aclose 后 pid 存活）。
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import claude_brain  # noqa: E402

BODY = {
    "threadId": "t-a2r19",
    "runId": "r-a2r19",
    "messages": [{"role": "user", "content": "hi"}],
}


def _fake_cli(tmp_path: Path, body: str) -> Path:
    """写一个最小 POSIX shell CLI（claude 的参数全部忽略）。"""
    path = tmp_path / "claude"
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(0o755)
    return path


def _parse(frame: bytes) -> dict:
    text = frame.decode()
    assert text.startswith("data: ")
    assert text.endswith("\n\n")
    return json.loads(text[len("data: "):].strip())


async def _collect(thread_id: str = "t-a2r19") -> list[dict]:
    body = dict(BODY, threadId=thread_id)
    return [
        _parse(frame)
        async for frame in claude_brain.claude_agui_stream(body, "hi")
    ]


def _run(coro):
    return asyncio.run(coro)


def _assert_no_success(events: list[dict]) -> None:
    assert all(ev["type"] != "RunFinished" for ev in events), events


def _wait_pid_file(path: Path, timeout: float = 5.0) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            text = path.read_text().strip()
            if text:
                return int(text)
        time.sleep(0.02)
    raise AssertionError(f"fake CLI 未在 {timeout}s 内写出 pid 文件 {path}")


def _assert_pid_gone(pid: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    # 清理失败时不要留孤儿：探针自己 SIGKILL，然后失败断言。
    try:
        os.kill(pid, 9)
    except ProcessLookupError:
        pass
    raise AssertionError(f"claude 子进程 {pid} 在 {timeout}s 后仍存活")


class TestRunErrorTerminal:
    """A2R19-01：非零退出 / is_error 一律 RunError，绝不 success。"""

    def test_provider_401_is_run_error(self, tmp_path, monkeypatch):
        cli = _fake_cli(
            tmp_path,
            'echo "Error: 401 Unauthorized invalid api key" >&2\n'
            "exit 1\n",
        )
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))

        events = _run(_collect("t-401"))

        assert events[0]["type"] == "RunStarted"
        assert events[-1]["type"] == "RunError"
        assert events[-1]["code"] == "claude_provider_error"
        assert "401" in events[-1]["message"]
        _assert_no_success(events)

    def test_provider_500_is_run_error(self, tmp_path, monkeypatch):
        cli = _fake_cli(
            tmp_path,
            'echo "500 Internal Server Error from provider" >&2\n'
            "exit 1\n",
        )
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))

        events = _run(_collect("t-500"))

        assert events[-1]["type"] == "RunError"
        assert events[-1]["code"] == "claude_provider_error"
        assert "500" in events[-1]["message"]
        _assert_no_success(events)

    def test_partial_text_then_500_keeps_text_but_errors(self, tmp_path, monkeypatch):
        cli = _fake_cli(
            tmp_path,
            "printf '%s\\n' "
            "'{\"type\":\"assistant\",\"message\":{\"content\":"
            "[{\"type\":\"text\",\"text\":\"半截回答\"}]}}'\n"
            'echo "stream aborted: 500" >&2\n'
            "exit 1\n",
        )
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))

        events = _run(_collect("t-partial"))

        deltas = [ev.get("delta") for ev in events if ev["type"] == "TextMessageContent"]
        assert deltas == ["半截回答"]
        assert events[-1]["type"] == "RunError"
        assert events[-1]["code"] == "claude_provider_error"
        assert "500" in events[-1]["message"]
        _assert_no_success(events)

    def test_result_is_error_true_is_run_error(self, tmp_path, monkeypatch):
        cli = _fake_cli(
            tmp_path,
            "printf '%s\\n' "
            "'{\"type\":\"result\",\"subtype\":\"error\",\"is_error\":true,"
            "\"result\":\"invalid api key\"}'\n"
            "exit 0\n",
        )
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))

        events = _run(_collect("t-is-error"))

        assert events[-1]["type"] == "RunError"
        assert events[-1]["code"] == "claude_result_error"
        assert "invalid api key" in events[-1]["message"]
        _assert_no_success(events)
        # is_error 的错误串不得被当作 assistant 正文补发
        assert not [ev for ev in events if ev["type"] == "TextMessageContent"]

    def test_error_message_redacts_provider_token(self, tmp_path, monkeypatch):
        cli = _fake_cli(
            tmp_path,
            'echo "401 ANTHROPIC_AUTH_TOKEN=sk-ant-SECRET123 rejected" >&2\n'
            "exit 1\n",
        )
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))

        events = _run(_collect("t-redact"))

        message = events[-1]["message"]
        assert "401" in message
        assert "SECRET123" not in message
        assert "***" in message

    def test_success_path_still_finishes_success(self, tmp_path, monkeypatch):
        cli = _fake_cli(
            tmp_path,
            "printf '%s\\n' "
            "'{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"s-ok\"}'\n"
            "printf '%s\\n' "
            "'{\"type\":\"assistant\",\"message\":{\"content\":"
            "[{\"type\":\"text\",\"text\":\"done\"}]}}'\n"
            "printf '%s\\n' "
            "'{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,"
            "\"result\":\"done\"}'\n"
            "exit 0\n",
        )
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))

        events = _run(_collect("t-ok"))

        assert events[-1]["type"] == "RunFinished"
        assert events[-1]["outcome"] == {"type": "success"}
        assert [ev.get("delta") for ev in events if ev["type"] == "TextMessageContent"] == ["done"]


class TestChildLifecycle:
    """A2R19-02：aclose / 超时后子进程必须消失（killpg + 有界回收）。"""

    def test_aclose_kills_child_process(self, tmp_path, monkeypatch):
        pid_file = tmp_path / "child.pid"
        cli = _fake_cli(
            tmp_path,
            "printf '%s\\n' "
            "'{\"type\":\"assistant\",\"message\":{\"content\":"
            "[{\"type\":\"text\",\"text\":\"开始\"}]}}'\n"
            f'echo $$ > "{pid_file}"\n'
            "exec sleep 60\n",
        )
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))

        async def scenario():
            gen = claude_brain.claude_agui_stream(dict(BODY, threadId="t-aclose"), "hi")
            assert (await gen.__anext__())  # RunStarted
            # 读到 TextMessageEnd（生成器挂在 yield 上，等价 SSE 客户端已断开）
            while True:
                ev = _parse(await gen.__anext__())
                if ev["type"] == "TextMessageEnd":
                    break
            pid = _wait_pid_file(pid_file)
            os.kill(pid, 0)  # 负控前置：此刻子进程确实存活（失败即 ProcessLookupError）
            await gen.aclose()
            return pid

        pid = _run(scenario())
        _assert_pid_gone(pid)

    def test_timeout_errors_and_kills_child(self, tmp_path, monkeypatch):
        pid_file = tmp_path / "child.pid"
        cli = _fake_cli(
            tmp_path,
            f'echo $$ > "{pid_file}"\n'
            "exec sleep 60\n",
        )
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_TIMEOUT", "1")

        events = _run(_collect("t-timeout"))

        assert events[-1]["type"] == "RunError"
        assert events[-1]["code"] == "claude_timeout"
        _assert_no_success(events)
        _assert_pid_gone(_wait_pid_file(pid_file))

    def test_invalid_timeout_env_falls_back_with_warning(self, monkeypatch, caplog):
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_TIMEOUT", "not-a-number")
        with caplog.at_level("WARNING", logger=claude_brain.__name__):
            assert claude_brain._resolve_timeout_seconds() == (
                claude_brain._DEFAULT_TIMEOUT_SECONDS
            )
        assert any("不是有效秒数" in rec.message for rec in caplog.records)
