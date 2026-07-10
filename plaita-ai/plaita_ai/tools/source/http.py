"""HttpToolSource — 把 HTTP API 声明为可注册工具。"""

from __future__ import annotations

import logging
import string
from typing import Any, Callable, ClassVar, Dict, Optional

from pydantic import Field

from plaita_ai.tools.source.base import (
    BaseToolSource,
    ToolContext,
    extract_json_path,
)

logger = logging.getLogger(__name__)

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore[assignment]


class HttpToolSource(BaseToolSource):
    """配置/代码双轨的 HTTP 工具。

    URL 支持 ``{param}`` 占位符；其余未用于 path 的参数进入 query（GET/HEAD/DELETE）
    或 JSON body（POST/PUT/PATCH）。
    """

    type: ClassVar[str] = "http"

    url: str
    method: str = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    timeout: float = 10.0
    response_path: Optional[str] = None  # 如 "$.data"
    content_type: str = "application/json"
    addressing: Optional[str] = None  # 命名寻址服务（register_addressing）

    def to_callable(self) -> Callable[..., Any]:
        if requests is None:
            raise ImportError(
                "HttpToolSource 需要 requests。安装: pip install requests "
                "或 pip install plaita[http]"
            )

        source = self

        def invoke(*, context: Optional[ToolContext] = None, auth_context: Any = None, **kwargs: Any) -> Any:
            from plaita_ai.tools.addressing import apply_addressing

            if context is None and auth_context is not None:
                context = ToolContext(auth=auth_context)

            url, remaining = source._format_url(kwargs)
            headers = dict(source.headers)
            if context is not None:
                source._apply_context_headers(headers, context)

            method = source.method.upper()
            with apply_addressing(url, source.addressing) as resolved_url:
                req_kwargs: Dict[str, Any] = {
                    "method": method,
                    "url": resolved_url,
                    "headers": headers,
                    "timeout": source.timeout,
                }
                if method in ("GET", "HEAD", "DELETE"):
                    if remaining:
                        req_kwargs["params"] = remaining
                else:
                    if remaining:
                        if source.content_type == "application/json":
                            headers.setdefault("Content-Type", source.content_type)
                            req_kwargs["json"] = remaining
                        else:
                            headers.setdefault("Content-Type", source.content_type)
                            req_kwargs["data"] = remaining

                logger.debug("HttpToolSource %s %s", method, resolved_url)
                resp = requests.request(**req_kwargs)

            if resp.status_code >= 400:
                raise ValueError(
                    f"HTTP {resp.status_code} from {resolved_url}: {resp.text[:500]}"
                )

            ctype = resp.headers.get("Content-Type") or ""
            if "application/json" in ctype:
                try:
                    data: Any = resp.json()
                except Exception:
                    data = resp.text
            else:
                try:
                    data = resp.json()
                except Exception:
                    data = resp.text

            if source.response_path:
                data = extract_json_path(data, source.response_path)
            return data

        invoke.__name__ = self.name.replace("-", "_")
        invoke.__doc__ = self.description or f"HTTP {self.method} {self.url}"
        return invoke

    def _format_url(self, kwargs: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        remaining = dict(kwargs)
        names = [
            fname for _, fname, _, _ in string.Formatter().parse(self.url) if fname
        ]
        missing = [n for n in names if n not in remaining]
        if missing:
            raise ValueError(
                f"HttpToolSource {self.name!r}: URL 缺少路径参数 {missing}"
            )
        url = self.url.format(**{n: remaining[n] for n in names})
        for n in names:
            remaining.pop(n, None)
        return url, remaining

    @staticmethod
    def _apply_context_headers(headers: Dict[str, str], context: ToolContext) -> None:
        if context.trace_id and "X-Trace-Id" not in headers:
            headers["X-Trace-Id"] = context.trace_id
        if context.request_id and "X-Request-Id" not in headers:
            headers["X-Request-Id"] = context.request_id
        if context.auth is not None and "Authorization" not in headers:
            if isinstance(context.auth, str):
                headers["Authorization"] = (
                    context.auth
                    if context.auth.lower().startswith("bearer ")
                    else f"Bearer {context.auth}"
                )
