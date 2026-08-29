"""Copilot Agent 端点：把编排页的 AG-UI 请求反代到 recursive `POST /agui`。

设计（docs/copilot-agent-plan.md M1）：
- recursive（`recursive http` 子命令）原生实现 AG-UI 协议（RunAgentInput / SSE /
  ToolCall / thread 会话）。console 后端只做：鉴权头注入、flow 上下文注入、流式透传。
- 上下文注入通道：前端把当前 flow JSON / 选中节点 / 子图栈放进 `RunAgentInput.context`
  （name = "plaita_flow_context"），本模块在透传前把它格式化拼接进最后一条 user 消息
  的 content 头部——recursive 的 goal 解析取最后一条 user 消息，这样信息必定到达模型。
- 配置：`PLAITA_CONSOLE_RECURSIVE_AGUI_URL`（如 http://127.0.0.1:8787/agui）；
  `PLAITA_CONSOLE_RECURSIVE_AGUI_API_KEY`（对应 recursive 侧 RECURSIVE_API_KEYS）。
  URL 未配置时返回 503 与可读提示。
"""
import json
import logging

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

try:
    from ..config import get_settings
    from .services import ai_flow, claude_brain, flow_store
except ImportError:  # 直接以 backend 为工作目录启动（python run.py）
    from config import get_settings
    from services import ai_flow, claude_brain, flow_store

logger = logging.getLogger(__name__)

router = APIRouter()

# Copilot system prompt：M1 通过追加到 user 消息头部送达（recursive /agui 的
# goal 解析取最后一条 user 消息；system role 是否透传给模型不可控）。
_COPILOT_PROMPT_HEADER = """\
你是 Plaita 流程编排 Copilot，帮助用户在可视化画布上创建和修改工作流。

## 工作方式
- 用户消息末尾附有「当前画布状态」（完整 flow JSON），你的一切修改都基于它。
- 需要输出修改时，把**完整的**新 flow JSON 放在 ```plaita-flow 代码块中，
  前端会自动应用并刷新画布。只输出有变化后完整结果，不要输出片段或 diff。
- flow JSON 结构：{"nodes":[{type,id,name,next,else_next,branches,...}],...}；
  线性连接用 next，if 分支用 next（真）+ else_next（假），switch/case 分支在
  branches[].next；子流程放 childFlow（完整子 Flow，含自身 start/end）。

## 节点类型与字段（类型特定字段平铺在节点对象上）
{nodes_digest}

## 回复要求
- 除 plaita-flow 代码块外，用简洁中文说明你做了什么、为什么。
- 用户需求含糊时先提问，不要臆测节点参数。
"""


def _nodes_digest() -> str:
    """节点类型摘要（复用现有 ai_flow 资产）。"""
    try:
        return ai_flow._available_nodes_digest()
    except Exception as exc:  # noqa: BLE001
        logger.warning("节点摘要生成失败: %s", exc)
        return ""


def _context_note(body: dict) -> str:
    """把 RunAgentInput.context 项拼为可读文本（全部带 value 的项）。"""
    parts = []
    for item in body.get("context") or []:
        if not isinstance(item, dict):
            continue
        value = str(item.get("value") or "")
        if not value.strip():
            continue
        title = item.get("name") or item.get("description") or "上下文"
        parts.append(f"### {title}\n{value}")
    return "\n\n".join(parts)


def _inject_context(body: dict) -> dict:
    """把 RunAgentInput.context 全部项（画布状态等）与 system 头拼接进最后一条 user 消息。

    不依赖 context item 的 name 字段——CopilotKit useCopilotReadable 产生的
    Context 形如 {description, value}，无 name；凡有 value 的项都拼接。
    """
    context_note = _context_note(body)
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return body

    system_header = _COPILOT_PROMPT_HEADER.replace(
        "{nodes_digest}", _nodes_digest()
    )
    tail = f"\n\n【当前画布状态】\n{context_note}" if context_note else ""

    for msg in reversed(messages):
        if isinstance(msg, dict) and msg.get("role") == "user":
            content = msg.get("content") or ""
            msg["content"] = f"{system_header}\n\n---\n\n{content}{tail}"
            return body
    # 没有 user 消息（不应发生）：追加一条
    messages.append({"role": "user", "content": f"{system_header}{tail}"})
    return body


@router.post("/copilot", tags=["admin", "copilot"])
async def copilot_run(request: Request):
    """AG-UI 协议端点：反代 recursive `POST /agui`，SSE 透传。"""
    settings = get_settings()
    base = (settings.recursive_agui_url or "").strip()
    use_claude = not base and claude_brain.claude_available()
    if not base and not use_claude:
        return JSONResponse(
            status_code=503,
            content={
                "detail": (
                    "Copilot 未配置大脑。可选：① 设置 PLAITA_CONSOLE_RECURSIVE_AGUI_URL"
                    "（recursive http 服务的 /agui 端点）；② 安装 claude CLI 后自动使用"
                    " Claude Code 大脑。"
                )
            },
        )

    try:
        body = _inject_context(await request.json())
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"detail": f"非法请求体: {exc}"})

    # 会话持久化：thread_id ↔ flow_id/version 关联入库（审计与回看）
    flow_id = request.headers.get("x-flow-id", "")
    if flow_id and isinstance(body.get("threadId"), str) and body["threadId"]:
        try:
            flow_store.get_flow_store().upsert_copilot_thread(
                thread_id=body["threadId"], flow_id=flow_id, bump_message=True
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("copilot thread upsert 失败: %s", exc)

    if not base:
        # claude 大脑：prompt 即处理后的最后一条 user 消息（含 system 头与画布状态）
        context_note = _context_note(body)
        messages = body.get("messages") or []
        goal = next(
            (m.get("content") for m in reversed(messages) if m.get("role") == "user"),
            context_note or "继续。",
        )
        return StreamingResponse(
            claude_brain.claude_agui_stream(body, str(goal)),
            media_type="text/event-stream",
        )

    url = base if base.endswith("/agui") else f"{base.rstrip('/')}/agui"
    headers = {"content-type": "application/json"}
    if settings.recursive_agui_api_key:
        headers["x-api-key"] = settings.recursive_agui_api_key

    async def stream():
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream(
                    "POST", url, content=json.dumps(body, ensure_ascii=False).encode(), headers=headers
                ) as resp:
                    if resp.status_code != 200:
                        detail = (await resp.aread()).decode(errors="replace")[:500]
                        payload = json.dumps(
                            {"detail": f"recursive /agui 错误 {resp.status_code}: {detail}"}
                        )
                        yield f"data: {payload}\n\n"
                        return
                    async for chunk in resp.aiter_bytes():
                        yield chunk
        except httpx.HTTPError as exc:
            payload = json.dumps({"detail": f"recursive /agui 不可达: {exc}"})
            yield f"data: {payload}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.get("/copilot/threads", tags=["admin", "copilot"])
async def list_copilot_threads(flow_id: str):
    """列出某流程的 Copilot 会话（最近更新优先）。"""
    try:
        return {"threads": flow_store.get_flow_store().list_copilot_threads(flow_id)}
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=500, content={"detail": str(exc)})
