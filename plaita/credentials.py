"""凭据解析——节点运行时按名取用外部服务的机密信息。

存储形态：加密 JSON 文件（由编排台 console 在凭据保存时导出），
``PLAITA_CREDENTIALS_FILE`` 指定路径（默认 ``.plaita-credentials.json``），
``PLAITA_CREDENTIALS_KEY`` 为 Fernet 密钥（未设时尝试同级 ``.plaita-credentials.key``
文件）。加密/解密依赖 ``cryptography``（``pip install plaita[credentials]``）。

多租户：解析时读租户上下文（``plaita.tenant_context.current_tenant``，与
分布式存储/日志同源）——非 default 租户读旁文件
``<stem>.<tenant_id><suffix>``（console 凭据页按租户导出的同一命名），
default/空租户沿用历史文件（零行为变化）。租户隔离由文件路径保证：
非 default 租户只看得见自己旁文件里的凭据，default 文件对它不可见；
报错信息只列**当前租户文件内**的凭据名，不泄露其他租户的存在。

节点内用法::

    from plaita.credentials import get_credential
    cred = get_credential("feishu-bot")   # -> {"url": "https://...", ...}
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


class CredentialError(RuntimeError):
    """凭据缺失/未配置/解密失败。message 面向用户，给出可操作的修复指引。"""


DEFAULT_FILE = ".plaita-credentials.json"
DEFAULT_KEY_FILE = ".plaita-credentials.key"
DEFAULT_TENANT_ID = "default"


def tenant_credentials_file(tenant_id: Optional[str], base: Optional[Path] = None) -> Path:
    """租户 → 凭据文件路径。default/空 = 基础文件（历史兼容），
    其余租户 = 旁文件 ``<stem>.<tenant_id><suffix>``（与 console 导出同规则）。"""
    if base is None:
        base = Path(os.environ.get("PLAITA_CREDENTIALS_FILE", DEFAULT_FILE))
    tid = tenant_id or DEFAULT_TENANT_ID
    if tid == DEFAULT_TENANT_ID:
        return base
    return base.with_name(f"{base.stem}.{tid}{base.suffix}")


def credentials_file() -> Path:
    """当前租户上下文对应的凭据文件（节点运行时经这里路由）。"""
    from plaita.tenant_context import current_tenant

    return tenant_credentials_file(current_tenant())


def _load_key() -> bytes:
    key = os.environ.get("PLAITA_CREDENTIALS_KEY")
    if key:
        return key.encode()
    key_file = Path(os.environ.get("PLAITA_CREDENTIALS_KEY_FILE", DEFAULT_KEY_FILE))
    if key_file.is_file():
        return key_file.read_bytes().strip()
    raise CredentialError(
        "未配置凭据解密密钥：请设置 PLAITA_CREDENTIALS_KEY（Fernet key），"
        "或提供 PLAITA_CREDENTIALS_KEY_FILE 指向密钥文件"
    )


def _fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError as e:
        raise CredentialError(
            "凭据功能需要 cryptography：pip install plaita[credentials]"
        ) from e
    return Fernet(_load_key())


def get_credential(name: str) -> Dict[str, Any]:
    """按名解析凭据，返回其数据 dict（如 {"url": ...} / {"username":..., "password":...}）。

    找不到或解密失败抛 :class:`CredentialError`，message 可直接展示给编排用户。
    """
    data = _read_store()
    entry = data.get(name)
    if entry is None:
        # 只列当前租户文件内的名字——跨租户文件互不可见，报错不得泄露
        # 其他租户配置了哪些凭据。
        known = ", ".join(sorted(data)) or "（无）"
        raise CredentialError(f"凭据 {name!r} 不存在（当前租户可用: {known}）。请在编排台「凭据」页创建")
    token = entry.get("data") if isinstance(entry, dict) else entry
    if token is None:
        raise CredentialError(f"凭据 {name!r} 内容为空")
    try:
        plain = _fernet().decrypt(token.encode()).decode()
        return json.loads(plain)
    except CredentialError:
        raise
    except Exception as e:  # noqa: BLE001 — 解密失败统一给出密钥指引
        raise CredentialError(
            f"凭据 {name!r} 解密失败（{e}）：请核对 PLAITA_CREDENTIALS_KEY 与写入时一致"
        ) from e


def credential_type(name: str) -> Optional[str]:
    """仅取凭据类型标签（不触碰密钥内容），不存在返回 None。"""
    entry = _read_store().get(name)
    if isinstance(entry, dict):
        return entry.get("type")
    return None


def _read_store() -> Dict[str, Dict[str, Any]]:
    path = credentials_file()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}
