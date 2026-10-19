"""读面响应序列化层脱敏（plaita#263 / #265 共用）。

全局拓扑/基础设施配置没有租户内语义，但读面受众可能是普通租户 viewer
（``/api/services``，plaita#265）。连接串里的口令不能因为「读面只读」就
原样出网；本模块给响应模型提供**序列化层**兜底：即便未来新增端点漏挂
平台闸，只要响应模型继承 :class:`RedactedResponseModel`，URL userinfo
口令与凭据键值就不会以明文返回。

判据与 ``plaita.core.url_redaction`` 同源（凭据键名、``key=value`` 运行），
URL 只脱敏 userinfo/凭据 query，保留 scheme/host/port/path/db，供运维
诊断。任何异常退化为 ``***``（fail-closed），绝不上抛、绝不回明文。
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import BaseModel, model_serializer

try:
    from plaita.core.url_redaction import (
        REDACTED_VALUE,
        _is_credential_key,
        _mask_credential_runs,
    )
except ImportError:  # 最小安装（无 plaita 包）时 fail-closed 兜底，绝不回明文
    REDACTED_VALUE = "***"

    def _is_credential_key(key: str) -> bool:  # type: ignore[misc]
        raise RuntimeError("plaita.core.url_redaction 不可用")

    def _mask_credential_runs(text: str) -> str:  # type: ignore[misc]
        raise RuntimeError("plaita.core.url_redaction 不可用")


#: 任意 URL token（scheme://…）——只用于脱敏，不用于策略判断。
_URL_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]{0,31}://[^ \t\r\n\f\v\"'<>]+")
#: 响应结构递归深度上限：越界整棵子树换占位（fail-closed）。
_REDACT_MAX_DEPTH = 64


def _mask_url_token(token: str) -> str:
    """URL 口令段脱敏（保留 scheme/host/port/path/db，供平台运维诊断）。

    ``redis://:s3cret@host:6379/0`` → ``redis://:***@host:6379/0``；
    ``postgresql://user:pw@host/db`` → ``postgresql://user:***@host/db``。
    query 里键名像凭据的参数值一并换占位。解析失败 ⇔ 无法证明无口令 ⇒
    整段换 ``***``（绝不退回原文）。
    """
    try:
        parts = urlsplit(token)
    except ValueError:
        return REDACTED_VALUE
    netloc = parts.netloc
    if "@" in netloc:
        userinfo, _, hostport = netloc.rpartition("@")
        if ":" in userinfo:
            user, _, _password = userinfo.partition(":")
            netloc = f"{user}:***@{hostport}"
        else:
            # 只有 userinfo 一段（无法区分用户名/口令）⇒ 整段当凭据抹掉。
            netloc = f"***@{hostport}"
        parts = parts._replace(netloc=netloc)
    if parts.query:
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        redacted = [
            (name, REDACTED_VALUE if _is_credential_key(name) else value)
            for name, value in pairs
        ]
        if redacted != pairs:
            parts = parts._replace(
                query=urlencode(redacted, doseq=True, safe="*")
            )
    return urlunsplit(parts)


def _redact_string(text: str) -> str:
    """字符串里的 URL 口令段 + ``password=…`` 型凭据运行。"""
    try:
        masked = _URL_TOKEN_RE.sub(
            lambda match: _mask_url_token(match.group(0)), text
        )
        return _mask_credential_runs(masked)
    except Exception:  # noqa: BLE001 — 脱敏失败不得回原文
        return REDACTED_VALUE


def redact_read_secrets(value: Any, _depth: int = 0) -> Any:
    """读面响应的**序列化层**脱敏（plaita#263 / #265）。

    - dict：凭据键（``PASSWORD`` / ``SECRET`` / ``TOKEN`` / ``KEY`` 等，与
      ``plaita.core.url_redaction`` 同一判据）的整棵值换 ``***``；其余递归；
    - list/tuple：逐项递归（类型保持）；
    - str：URL 口令段与 ``key=value`` 凭据运行；
    - 其余标量原样。
    任何异常退化为 ``***``（fail-closed），绝不上抛、绝不回明文。
    """
    try:
        if _depth > _REDACT_MAX_DEPTH:
            return REDACTED_VALUE
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if isinstance(key, str) and _is_credential_key(key):
                    result[key] = REDACTED_VALUE
                else:
                    result[key] = redact_read_secrets(item, _depth + 1)
            return result
        if isinstance(value, list):
            return [redact_read_secrets(item, _depth + 1) for item in value]
        if isinstance(value, tuple):
            return tuple(redact_read_secrets(item, _depth + 1) for item in value)
        if isinstance(value, str):
            return _redact_string(value)
        return value
    except Exception:  # noqa: BLE001 — fail-closed
        return REDACTED_VALUE


class RedactedResponseModel(BaseModel):
    """响应模型的序列化层脱敏基类（plaita#263 / #265 纵深防御）。

    ``model_serializer(mode="wrap")`` 在 FastAPI 的 ``model_dump`` 路径上
    生效，对已是 ``***`` 的输入幂等。
    """

    @model_serializer(mode="wrap")
    def _serialize_redacted(self, handler):
        return redact_read_secrets(handler(self))
