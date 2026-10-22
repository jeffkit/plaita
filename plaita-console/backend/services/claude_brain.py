"""Claude Code 大脑：把 `claude -p --output-format stream-json` 子进程翻译为 AG-UI 事件流。

BrainRunner 抽象的第二实现（第一个是 recursive /agui 反代）：
- 输入：经 `_inject_context` 处理后的 RunAgentInput（system 头 + 画布状态已拼进
  最后一条 user 消息，作为 claude 的单轮 prompt）
- 输出：AG-UI 事件（RunStarted / TextMessage* / RunFinished / RunError），SSE 字节流
- 会话：(tenant_id, threadId) → claude session_id 映射（内存），续轮走
  `--resume`；键带租户段（r70 b/P2）——threadId 客户端可控，裸键曾使知道
  他租户 threadId 即可 `--resume` 其会话
- 凭证：经环境变量（ANTHROPIC_BASE_URL / ANTHROPIC_AUTH_TOKEN 等），后端不落盘；
  便捷方法 `resolve_claude_env()` 可从 flowcast providers.json 读取 anthropic
  兼容 provider（如 GLM）注入

失败语义（台账 A2R19-01/02）：
- 子进程 stderr 有界回收；`returncode != 0`、`result.is_error=true`、流读取
  异常、超时一律以 AG-UI ``RunError`` 收尾，**绝不**把 provider 401/500 或
  半截回答包装成 ``RunFinished success``。
- 子进程自成进程组（``start_new_session``）；客户端断开（``aclose``）/
  取消 / 超时走 ``finally`` 终止整组（SIGTERM → 有界等待 → SIGKILL），不留
  仍在调用 provider 的孤儿进程。
"""
import asyncio
import json
import logging
import os
import re
import shutil
import signal
import threading
import uuid
from typing import AsyncIterator, Dict, Optional, Tuple

from plaita.core.url_redaction import _scrub_url_text

logger = logging.getLogger(__name__)

# 会话映射键 = (tenant_id, thread_id)：threadId 客户端可控，租户段隔离
# --resume（r70 b/P2——裸 threadId 键曾让知道 ID 即可续他租户会话）。
_SESSIONS: Dict[Tuple[str, str], str] = {}
_SESSIONS_LOCK = threading.Lock()

_MAX_TURNS = "30"

#: 整个 CLI 调用的 deadline（秒）；可经环境变量覆盖，便于部署侧对齐反向代理
#: 空闲超时。<=0 / 非数字按默认并打 WARNING（不静默改道）。
_DEFAULT_TIMEOUT_SECONDS = 300.0
_TIMEOUT_ENV = "PLAITA_CONSOLE_CLAUDE_TIMEOUT"
#: stderr 只保留末段（错误原因通常在末尾），防止半截输出撑爆内存。
_STDERR_TAIL_BYTES = 4096
#: 终止子进程组的两段有界等待：SIGTERM 宽限 + SIGKILL 回收确认。
_TERMINATE_GRACE_SECONDS = 2.0
#: 错误消息截断长度（进 SSE 给前端展示）。
_ERROR_MESSAGE_LIMIT = 500

#: 裸 provider token（sk-…）不在 URL 凭据清洗器的判据内，此处单独兜底。
_BARE_PROVIDER_TOKEN_RE = re.compile(r"\bsk-[A-Za-z0-9_\-]{8,}\b")


def claude_available() -> bool:
    """本机存在可执行的 claude CLI 即视为可用。"""
    return shutil.which(os.getenv("PLAITA_CONSOLE_CLAUDE_CLI", "claude")) is not None


def resolve_claude_env(provider: str = "glm-52") -> Dict[str, str]:
    """从 flowcast providers.json 读取 anthropic 兼容 provider 的凭证。

    返回需要叠加到子进程 env 的变量（ANTHROPIC_BASE_URL / ANTHROPIC_AUTH_TOKEN）。
    providers.json 不存在或不含该 provider 时返回空 dict（沿用调用方环境）。
    """
    path = os.path.expanduser("~/.flowcast/providers.json")
    try:
        with open(path) as f:
            provider_cfg = json.load(f).get(provider) or {}
    except (OSError, ValueError):
        return {}
    key = provider_cfg.get("apiKey")
    if not key:
        return {}
    env = {"ANTHROPIC_AUTH_TOKEN": key}
    if provider_cfg.get("apiBase"):
        env["ANTHROPIC_BASE_URL"] = provider_cfg["apiBase"]
    return env


def _resolve_timeout_seconds() -> float:
    """读取 CLI deadline；非法配置按默认值并留 WARNING（降级必须可观测）。"""
    raw = (os.getenv(_TIMEOUT_ENV) or "").strip()
    if not raw:
        return _DEFAULT_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "%s=%r 不是有效秒数，claude CLI deadline 按默认 %.0fs",
            _TIMEOUT_ENV, raw, _DEFAULT_TIMEOUT_SECONDS,
        )
        return _DEFAULT_TIMEOUT_SECONDS
    if value <= 0:
        logger.warning(
            "%s=%r <= 0，claude CLI deadline 按默认 %.0fs",
            _TIMEOUT_ENV, raw, _DEFAULT_TIMEOUT_SECONDS,
        )
        return _DEFAULT_TIMEOUT_SECONDS
    return value


def _scrub_error_text(text: str) -> str:
    """错误文本进 SSE 前脱敏 + 截断。

    URL/凭据键值经核心清洗器（与 ``plaita.core`` 同一判据）；裸 ``sk-…``
    token 由本地正则兜底——provider 的错误体常把 key 原样回显。
    """
    scrubbed = _BARE_PROVIDER_TOKEN_RE.sub("***", text or "")
    scrubbed = _scrub_url_text(scrubbed)
    return scrubbed.strip()[:_ERROR_MESSAGE_LIMIT]


async def _read_stderr_tail(stream: asyncio.StreamReader) -> bytes:
    """持续排空 stderr，只保留末 ``_STDERR_TAIL_BYTES``（防止管道写满阻塞子进程）。"""
    tail = bytearray()
    try:
        while True:
            chunk = await stream.read(4096)
            if not chunk:
                break
            tail.extend(chunk)
            if len(tail) > _STDERR_TAIL_BYTES:
                del tail[: len(tail) - _STDERR_TAIL_BYTES]
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — 回收失败不影响终态判断
        logger.warning("读取 claude CLI stderr 失败（已保留 %d 字节）: %s", len(tail), exc)
    return bytes(tail)


async def _stderr_text(task: Optional["asyncio.Task[bytes]"]) -> str:
    """有界等待 stderr 回收任务（进程已退出时通常立即完成）。"""
    if task is None:
        return ""
    try:
        raw = await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
    except asyncio.TimeoutError:
        task.cancel()
        return ""
    except asyncio.CancelledError:
        task.cancel()
        return ""
    except Exception as exc:  # noqa: BLE001
        logger.warning("claude CLI stderr 回收异常: %s", exc)
        return ""
    return _scrub_error_text(raw.decode(errors="replace"))


def _signal_process_group(proc: asyncio.subprocess.Process, sig: int) -> None:
    """向子进程组发信号（子进程 ``start_new_session`` ⇒ pid 即 pgid）。"""
    if proc.returncode is not None:
        return
    killpg = getattr(os, "killpg", None)
    try:
        if killpg is not None:
            killpg(proc.pid, sig)
        else:  # 非 POSIX 平台没有进程组语义，退化为单进程信号
            proc.send_signal(sig)
    except ProcessLookupError:
        return
    except Exception as exc:  # noqa: BLE001 — 清理路径不能把异常抛给客户端
        logger.warning(
            "向 claude 子进程组发信号失败（pid=%s, sig=%s）: %s", proc.pid, sig, exc
        )


async def _terminate_process_group(proc: asyncio.subprocess.Process) -> None:
    """有界终止整棵子进程树：SIGTERM → 宽限 → SIGKILL → 回收确认。

    对齐 ``recursive_stream_turn`` 的终止语义；清理失败只 WARNING，不影响
    已发出的 SSE 终态。
    """
    if proc.returncode is not None:
        return
    _signal_process_group(proc, signal.SIGTERM)
    try:
        await asyncio.wait_for(proc.wait(), _TERMINATE_GRACE_SECONDS)
        return
    except asyncio.TimeoutError:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("等待 claude 子进程退出失败（pid=%s）: %s", proc.pid, exc)
    _signal_process_group(proc, signal.SIGKILL)
    try:
        await asyncio.wait_for(proc.wait(), _TERMINATE_GRACE_SECONDS)
    except asyncio.TimeoutError:
        logger.warning(
            "claude 子进程组 %.0fs 内未回收（pid=%s）", _TERMINATE_GRACE_SECONDS, proc.pid
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("回收 claude 子进程失败（pid=%s）: %s", proc.pid, exc)


async def claude_agui_stream(
    body: dict, context_note: str, tenant_id: str = ""
) -> AsyncIterator[bytes]:
    """驱动 claude CLI 并产出 AG-UI SSE 帧。

    body: 已反序列化的 RunAgentInput（_inject_context 已执行——最后一条 user
    消息包含 system 头与画布状态）。
    context_note: 兜底上下文文本（context 项拼接），goal 无 user 消息时使用。
    tenant_id: 调用方租户——会话映射按 (tenant_id, threadId) 隔离（r70 b/P2）。
    """
    cli = os.getenv("PLAITA_CONSOLE_CLAUDE_CLI", "claude")
    thread_id = str(body.get("threadId") or uuid.uuid4().hex)
    run_id = str(body.get("runId") or uuid.uuid4().hex)
    session_key = (str(tenant_id or ""), thread_id)

    with _SESSIONS_LOCK:
        session_id = _SESSIONS.get(session_key)

    user_msgs = [m for m in body.get("messages") or [] if m.get("role") == "user"]
    prompt = user_msgs[-1].get("content") if user_msgs else ""
    if not prompt or not str(prompt).strip():
        prompt = context_note or "继续。"
    prompt = str(prompt)

    argv = [cli, "-p", prompt, "--output-format", "stream-json", "--verbose", "--max-turns", _MAX_TURNS]
    if session_id:
        argv += ["--resume", session_id]

    env = dict(os.environ)
    env.update(resolve_claude_env())

    def sse(event: dict) -> bytes:
        return f"data: {json.dumps(event, ensure_ascii=False)}\n\n".encode()

    def run_error(code: str, message: str) -> bytes:
        return sse({"type": "RunError", "code": code, "message": _scrub_error_text(message)})

    yield sse(
        {
            "type": "RunStarted",
            "threadId": thread_id,
            "runId": run_id,
        }
    )

    loop = asyncio.get_running_loop()
    timeout_seconds = _resolve_timeout_seconds()
    deadline = loop.time() + timeout_seconds
    proc: Optional[asyncio.subprocess.Process] = None
    stderr_task: Optional["asyncio.Task[bytes]"] = None

    try:
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                start_new_session=True,
            )
        except OSError as exc:
            yield run_error("claude_spawn_failed", f"claude CLI 启动失败: {exc}")
            return

        assert proc.stdout is not None and proc.stderr is not None
        stderr_task = asyncio.create_task(_read_stderr_tail(proc.stderr))

        emitted_text = False
        new_session: Optional[str] = None
        result_error: Optional[str] = None
        timed_out = False

        try:
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    timed_out = True
                    break
                try:
                    line = await asyncio.wait_for(proc.stdout.readline(), timeout=remaining)
                except asyncio.TimeoutError:
                    timed_out = True
                    break
                if not line:
                    break
                line = line.decode(errors="replace").strip()
                if not line.startswith("{"):
                    continue
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                ev_type = ev.get("type")

                if ev_type == "system" and ev.get("subtype") == "init":
                    new_session = ev.get("session_id")
                    with _SESSIONS_LOCK:
                        _SESSIONS[session_key] = new_session or thread_id

                elif ev_type == "assistant":
                    content_blocks = (ev.get("message") or {}).get("content") or []
                    for block in content_blocks:
                        if block.get("type") == "text" and str(block.get("text") or "").strip():
                            emitted_text = True
                            message_id = uuid.uuid4().hex
                            yield sse(
                                {
                                    "type": "TextMessageStart",
                                    "messageId": message_id,
                                    "role": "assistant",
                                }
                            )
                            yield sse(
                                {
                                    "type": "TextMessageContent",
                                    "messageId": message_id,
                                    "delta": block["text"],
                                }
                            )
                            yield sse({"type": "TextMessageEnd", "messageId": message_id})

                elif ev_type == "result":
                    # is_error=true 是 provider 的显式失败终态：绝不把错误串当正文
                    # 补成 assistant 消息，也不在 returncode=0 时误报 success。
                    if ev.get("is_error"):
                        result_error = str(
                            ev.get("result") or ev.get("error") or "claude CLI 报告 is_error"
                        )
                        break
                    # 最终结果兜底（前面没有输出任何文本时补一条，保证前端有可渲染内容）
                    final = ev.get("result")
                    if final and not emitted_text:
                        message_id = uuid.uuid4().hex
                        yield sse(
                            {
                                "type": "TextMessageStart",
                                "messageId": message_id,
                                "role": "assistant",
                            }
                        )
                        yield sse(
                            {
                                "type": "TextMessageContent",
                                "messageId": message_id,
                                "delta": str(final),
                            }
                        )
                        yield sse({"type": "TextMessageEnd", "messageId": message_id})
        except Exception as exc:  # noqa: BLE001 — stdout 读失败按 provider 失败收口
            yield run_error("claude_stream_failed", f"读取 claude CLI 输出失败: {exc}")
            return

        if result_error is not None:
            detail = await _stderr_text(stderr_task)
            message = f"claude CLI 返回错误: {result_error}"
            if detail:
                message += f"（stderr: {detail}）"
            yield run_error("claude_result_error", message)
            return

        if timed_out:
            yield run_error(
                "claude_timeout",
                f"claude CLI 超过 {timeout_seconds:.0f}s 未结束（{_TIMEOUT_ENV}）",
            )
            return

        # 流已 EOF：在剩余 deadline 内等退出码（正常路径通常立即返回）。
        try:
            returncode = await asyncio.wait_for(
                proc.wait(), timeout=max(0.0, deadline - loop.time())
            )
        except asyncio.TimeoutError:
            yield run_error(
                "claude_timeout",
                f"claude CLI 超过 {timeout_seconds:.0f}s 未结束（{_TIMEOUT_ENV}）",
            )
            return

        if returncode != 0:
            stderr_text = await _stderr_text(stderr_task)
            message = f"claude CLI 退出码 {returncode}"
            if stderr_text:
                message += f": {stderr_text}"
            yield run_error("claude_provider_error", message)
            return

        yield sse(
            {
                "type": "RunFinished",
                "threadId": thread_id,
                "runId": run_id,
                "outcome": {"type": "success"},
            }
        )
    finally:
        # 客户端断开（aclose）/ 取消 / 任意退出路径：不留仍在调用 provider 的子进程。
        if stderr_task is not None and not stderr_task.done():
            stderr_task.cancel()
            try:
                await stderr_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                logger.warning("claude CLI stderr 回收任务收尾异常: %s", exc)
        if proc is not None:
            await _terminate_process_group(proc)
