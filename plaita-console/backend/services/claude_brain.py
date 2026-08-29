"""Claude Code 大脑：把 `claude -p --output-format stream-json` 子进程翻译为 AG-UI 事件流。

BrainRunner 抽象的第二实现（第一个是 recursive /agui 反代）：
- 输入：经 `_inject_context` 处理后的 RunAgentInput（system 头 + 画布状态已拼进
  最后一条 user 消息，作为 claude 的单轮 prompt）
- 输出：AG-UI 事件（RunStarted / TextMessage* / RunFinished），SSE 字节流
- 会话：threadId → claude session_id 映射（内存），续轮走 `--resume`
- 凭证：经环境变量（ANTHROPIC_BASE_URL / ANTHROPIC_AUTH_TOKEN 等），后端不落盘；
  便捷方法 `resolve_claude_env()` 可从 flowcast providers.json 读取 anthropic
  兼容 provider（如 GLM）注入
"""
import asyncio
import json
import logging
import os
import shutil
import threading
import uuid
from typing import AsyncIterator, Dict, Optional

logger = logging.getLogger(__name__)

_SESSIONS: Dict[str, str] = {}
_SESSIONS_LOCK = threading.Lock()

_MAX_TURNS = "30"


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


async def claude_agui_stream(body: dict, context_note: str) -> AsyncIterator[bytes]:
    """驱动 claude CLI 并产出 AG-UI SSE 帧。

    body: 已反序列化的 RunAgentInput（_inject_context 已执行——最后一条 user
    消息包含 system 头与画布状态）。
    context_note: 兜底上下文文本（context 项拼接），goal 无 user 消息时使用。
    """
    cli = os.getenv("PLAITA_CONSOLE_CLAUDE_CLI", "claude")
    thread_id = str(body.get("threadId") or uuid.uuid4().hex)
    run_id = str(body.get("runId") or uuid.uuid4().hex)

    with _SESSIONS_LOCK:
        session_id = _SESSIONS.get(thread_id)

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

    yield sse(
        {
            "type": "RunStarted",
            "threadId": thread_id,
            "runId": run_id,
        }
    )

    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=env,
        )
    except OSError as exc:
        yield sse(
            {
                "type": "RunFinished",
                "threadId": thread_id,
                "runId": run_id,
                "outcome": {"type": "error", "message": f"claude CLI 启动失败: {exc}"},
            }
        )
        return

    assert proc.stdout is not None
    emitted_text = False
    new_session: Optional[str] = None

    async for line in proc.stdout:
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
                _SESSIONS[thread_id] = new_session or thread_id

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

    await proc.wait()

    yield sse(
        {
            "type": "RunFinished",
            "threadId": thread_id,
            "runId": run_id,
            "outcome": {"type": "success"},
        }
    )
