"""Mock recursive /agui：返回标准 AG-UI SSE 事件流，用于 M1 端到端验证。

行为：echo 校验（上下文/规则是否注入到 goal）+ 返回固定的「添加 http 节点」flow IR，
IR 放在 ```plaita-flow 代码块中，供前端自动应用验证。
"""
import json
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()


@app.post("/agui")
async def agui(request: Request):
    body = await request.json()
    user_msgs = [m for m in body.get("messages", []) if m.get("role") == "user"]
    goal = user_msgs[-1].get("content", "") if user_msgs else ""
    got_context = "当前画布状态" in goal
    got_rules = "Plaita 流程编排 Copilot" in goal
    got_digest = "可用节点类型" in goal

    ir = {
        "nodes": [
            {"type": "start", "id": "start", "name": "start", "next": "h1"},
            {
                "type": "http",
                "id": "h1",
                "name": "AI添加的节点",
                "url": "https://example.com",
                "method": "GET",
                "next": "end",
            },
            {"type": "end", "id": "end", "name": "end", "output": "$INPUT.result"},
        ],
        "inputType": {"dataType": "object"},
    }
    reply = (
        f"收到。注入校验：画布状态={got_context}，规则={got_rules}，节点摘要={got_digest}。\n\n"
        f"我已在画布末尾添加一个 http 节点（AI添加的节点）。\n\n"
        f"```plaita-flow\n{json.dumps(ir, ensure_ascii=False)}\n```\n\n完成。"
    )

    async def sse():
        run_id = uuid.uuid4().hex
        thread_id = body.get("threadId") or "mock-thread"
        yield f'data: {json.dumps({"type": "RUN_STARTED", "threadId": thread_id, "runId": run_id})}\n\n'
        mid = uuid.uuid4().hex
        yield f'data: {json.dumps({"type": "TEXT_MESSAGE_START", "messageId": mid, "role": "assistant"})}\n\n'
        for i in range(0, len(reply), 48):
            yield f'data: {json.dumps({"type": "TEXT_MESSAGE_CONTENT", "messageId": mid, "delta": reply[i : i + 48]})}\n\n'
        yield f'data: {json.dumps({"type": "TEXT_MESSAGE_END", "messageId": mid})}\n\n'
        yield f'data: {json.dumps({"type": "RUN_FINISHED", "threadId": thread_id, "runId": run_id})}\n\n'

    return StreamingResponse(sse(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8901)
