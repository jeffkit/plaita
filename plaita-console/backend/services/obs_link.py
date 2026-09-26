"""观测深链：执行实例 → Langfuse trace 页面 URL。

trace id 由 LangfuseCallback 按 ``create_trace_id(seed=语义id)`` 确定性派生
（sha256 前 16 字节），语义 id = console 的 execution_id——因此 URL 可以
纯计算得出，无需查询 Langfuse。

启用条件（与 local_executor._build_langfuse_callback 的开关口径一致）：
- ``PLAITA_CONSOLE_LANGFUSE`` 不为 ``false``，且
- 配了 ``LANGFUSE_PUBLIC_KEY``（auto 口径），且
- 配了 ``LANGFUSE_PROJECT_ID``（URL 需要项目段；不配则放弃深链）。
"""
from __future__ import annotations

import hashlib
import os
from typing import Optional

DEFAULT_LANGFUSE_HOST = "https://cloud.langfuse.com"


def langfuse_enabled() -> bool:
    mode = os.getenv("PLAITA_CONSOLE_LANGFUSE", "auto").strip().lower()
    if mode == "false":
        return False
    if mode == "true":
        return True
    return bool(os.getenv("LANGFUSE_PUBLIC_KEY", ""))


def langfuse_trace_url(execution_id: str) -> Optional[str]:
    """执行 ID → Langfuse trace 页 URL；未启用/信息不全时返回 None。"""
    if not langfuse_enabled():
        return None
    project_id = os.getenv("LANGFUSE_PROJECT_ID", "").strip()
    if not project_id:
        return None
    host = (os.getenv("LANGFUSE_HOST", "") or DEFAULT_LANGFUSE_HOST).rstrip("/")
    trace_id = hashlib.sha256(execution_id.encode()).hexdigest()[:32]
    return f"{host}/project/{project_id}/traces/{trace_id}"
