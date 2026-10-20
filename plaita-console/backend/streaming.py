"""SSE 流式端点的 OpenAPI 响应类（A2R24-09）。

FastAPI 对无 ``response_model`` 的路由默认把 200 声明成 ``application/json``；
SSE 端点实际返回 ``text/event-stream``，按 OpenAPI 生成的客户端会按 JSON
解析。给路由挂 ``response_class=EventStreamResponse`` 后，schema 里的 200
媒体类型就是 ``text/event-stream``（schema 为 string），与实际一致。

这里只是**声明类**：端点仍返回 ``EventSourceResponse`` / ``StreamingResponse``，
不需要实例化本类。
"""
from starlette.responses import Response


class EventStreamResponse(Response):
    """OpenAPI 声明用的 SSE 响应类（``text/event-stream``）。"""

    media_type = "text/event-stream"
