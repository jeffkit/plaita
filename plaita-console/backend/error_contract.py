"""console 错误响应统一契约（A2R24-01）。

历史形态：186 个手工 ``HTTPException`` 的 ``detail`` 有 string / validation
list / 三种 dict；未捕获异常回 Starlette 默认 ``text/plain``。前端/codegen
无法用统一解析器，且 ``str(e)`` 会把 DB/Redis/文件路径带回客户端。

本模块提供两件事：

1. **统一 detail 形状**：``{"code": <机器码>, "message": <人读文案>, ...}``。
   字符串 detail、FastAPI 自动 422 的 list、三种历史 dict 都在
   ``normalize_detail`` 收敛为同一对象；历史 dict 的业务键（``conflict`` /
   ``error`` / ``reason`` 等）保留，``code`` 取其中的机器码。
2. **异常文本脱敏**：``scrubbed_detail`` 只把固定的可读文案放进响应，
   异常原文只进 WARNING 日志。禁止 ``detail=f"...{e}"`` 直接出网。

挂载：``install_error_handlers(app)`` 注册 HTTPException /
RequestValidationError / 未捕获 Exception 三个处理器；``main.create_app``
调用一次。响应体同时带顶层 ``request_id``（与 ``X-Request-ID`` 同值），
便于把客户端报错与审计/日志对上。
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from urllib.parse import urlsplit, urlunsplit

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)

#: 错误契约版本（响应体不暴露；契约变更时随 ADR/台账记录）。
ERROR_CONTRACT_VERSION = 1

#: HTTP 状态 → 默认机器码。手工 raise 未给 code 时使用。
STATUS_CODES: Dict[int, str] = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "conflict",
    410: "gone",
    413: "payload_too_large",
    422: "validation_error",
    429: "rate_limited",
    500: "internal_error",
    502: "bad_gateway",
    503: "service_unavailable",
    504: "gateway_timeout",
}

#: 默认人读文案（detail 为 None/无 message 时兜底）。
STATUS_MESSAGES: Dict[int, str] = {
    400: "请求无效",
    401: "未认证",
    403: "无权限",
    404: "资源不存在",
    405: "方法不允许",
    409: "资源冲突",
    410: "资源已不存在",
    413: "请求体过大",
    422: "请求参数校验失败",
    429: "请求过于频繁",
    500: "内部服务器错误",
    502: "上游服务错误",
    503: "服务暂不可用",
    504: "上游服务超时",
}

#: 历史 dict 里可能承载机器码的键（按优先级）。
_CODE_KEYS = ("code", "error", "conflict", "reason")


def status_code_of(status_code: int) -> str:
    """状态码对应的默认机器码（未知状态回落 ``http_<code>``）。"""
    return STATUS_CODES.get(status_code, f"http_{status_code}")


def status_message_of(status_code: int) -> str:
    """状态码对应的默认人读文案。"""
    return STATUS_MESSAGES.get(status_code, f"请求失败（HTTP {status_code}）")


def error_detail(code: str, message: str, **extra: Any) -> Dict[str, Any]:
    """构造统一错误 detail：``{"code", "message", ...extra}``。

    ``extra`` 里的历史业务键（``conflict`` / ``error`` / ``reason`` /
    ``latest_updated_at`` 等）原样保留；``None`` 值不落 JSON，避免客户端
    把 ``null`` 当业务值。
    """
    detail: Dict[str, Any] = {"code": str(code), "message": str(message)}
    for key, value in extra.items():
        if key in ("code", "message") or value is None:
            continue
        detail[key] = value
    return detail


def redact_url_userinfo(url: str) -> str:
    """URL 只保留 scheme://user@host:port（口令段 ``***``）。

    连接失败错误里保留「连的是哪个端点」的诊断价值，但不把 Redis/HTTP
    口令带进响应体。无法解析时 fail-closed 成 ``<url>``（绝不回原文）。
    """
    try:
        parts = urlsplit(str(url))
        if parts.username is None and parts.password is None:
            return str(url)
        host = parts.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        if parts.port is not None:
            host = f"{host}:{parts.port}"
        user = parts.username or ""
        netloc = f"{user}:***@{host}" if user else f"***@{host}"
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:  # noqa: BLE001 — 渲染失败不得回原文
        return "<url>"


def scrubbed_detail(
    log: logging.Logger,
    code: str,
    message: str,
    exc: BaseException,
    *,
    exc_info: bool = False,
    **extra: Any,
) -> Dict[str, Any]:
    """异常文本脱敏：响应只带固定文案，原文进日志（A2R24-01）。

    ``message`` 必须是**不含异常原文**的可读文案。需要堆栈的调用方传
    ``exc_info=True``（默认只记一行，避免解析类错误刷爆日志）。
    """
    log.warning("错误已脱敏为固定文案 code=%s: %s", code, exc, exc_info=exc_info)
    return error_detail(code, message, **extra)


def normalize_detail(detail: Any, status_code: int) -> Dict[str, Any]:
    """把任意历史 detail 收敛为统一对象（幂等）。

    - 统一对象：原样返回（缺 ``message`` 时补默认文案）。
    - 历史 dict：``code`` ← ``code``/``error``/``conflict``/``reason``
      （按序取第一个），``message`` ← ``message``，其余键保留。
    - list/tuple：FastAPI 自动 422 的 validation 列表 →
      ``{"code": "validation_error", "message": ..., "errors": [...]}``。
    - 其他（string/None/数字）：``{"code": <状态默认>, "message": <原文>}``。
    """
    if isinstance(detail, dict):
        raw = dict(detail)
        code = next(
            (raw[key] for key in _CODE_KEYS if raw.get(key) not in (None, "")),
            None,
        )
        message = raw.pop("message", None)
        raw.pop("code", None)
        if code is None:
            code = status_code_of(status_code)
        if message is None:
            message = status_message_of(status_code)
        return error_detail(str(code), str(message), **raw)
    if isinstance(detail, (list, tuple)):
        return error_detail(
            "validation_error",
            status_message_of(422),
            errors=list(detail),
        )
    if detail is None:
        return error_detail(
            status_code_of(status_code), status_message_of(status_code)
        )
    return error_detail(status_code_of(status_code), str(detail))


def _request_id(request: Request) -> Optional[str]:
    return getattr(getattr(request, "state", None), "request_id", None)


def _json_error(
    request: Request,
    status_code: int,
    detail: Any,
    headers: Optional[Dict[str, str]] = None,
) -> JSONResponse:
    body: Dict[str, Any] = {"detail": normalize_detail(detail, status_code)}
    request_id = _request_id(request)
    if request_id:
        body["request_id"] = request_id
        headers = dict(headers or {})
        headers.setdefault("X-Request-ID", request_id)
    return JSONResponse(
        jsonable_encoder(body), status_code=status_code, headers=headers
    )


async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    """HTTPException → 统一 JSON（保留 Retry-After 等 headers）。"""
    return _json_error(request, exc.status_code, exc.detail, exc.headers)


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """自动 422 → 与手工 422 同一对象形状（``errors`` 承载字段级明细）。"""
    return _json_error(
        request,
        422,
        {
            "code": "validation_error",
            "message": status_message_of(422),
            "errors": exc.errors(),
        },
    )


async def unhandled_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """未捕获异常 → JSON 500（不再回 ``text/plain``）；原文只进日志。"""
    request_id = _request_id(request)
    logger.error(
        "未捕获异常 path=%s method=%s request_id=%s",
        request.url.path,
        request.method,
        request_id,
        exc_info=exc,
    )
    return _json_error(
        request,
        500,
        error_detail("internal_error", status_message_of(500)),
    )


def install_error_handlers(app: FastAPI) -> None:
    """把统一错误处理器挂到 app（幂等：重复调用覆盖同一 key）。"""
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
