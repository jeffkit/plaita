"""凭据解析——节点运行时按名取用外部服务的机密信息。

存储形态：加密 JSON 文件（由编排台 console 在凭据保存时导出），
``PLAITA_CREDENTIALS_FILE`` 指定路径（默认 ``.plaita-credentials.json``），
``PLAITA_CREDENTIALS_KEY`` 为 Fernet 密钥（未设时尝试同级 ``.plaita-credentials.key``
文件）。加密/解密依赖 ``cryptography``（``pip install plaita[credentials]``）。

文件按租户分区：``PLAITA_CREDENTIALS_FILE`` 只承载 default 租户（历史路径），
其余租户是同目录旁文件 ``<stem>.<tenant_id><suffix>``（命名与 console
``services/credentials_svc.credentials_file`` 一致）。当前租户取
:func:`plaita.tenant_context.current_tenant`——FlowWorker 每处理一条任务
消息前注入、处理完 reset，节点侧凭据解析因此天然按租户隔离：非 default
租户既读不到 default 文件的密文，报错也不会列出跨租户的凭据名。

节点内用法::

    from plaita.credentials import get_credential
    cred = get_credential("feishu-bot")   # -> {"url": "https://...", ...}

租户上下文要穿过节点执行线程才生效：``plaita.core.runner`` /
``plaita.core.parallel_executor`` / ``plaita.core.async_utils`` 把节点投到
同步节点池、超时裸线程、分支线程池、进程池或惰性模式的驱动线程时，都携带
调用方的环境态快照（``plaita.env_context``，白名单——只传租户等注册项，
不整份复制 context）。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from plaita.tenant_context import DEFAULT_TENANT_ID, current_tenant


class CredentialError(RuntimeError):
    """凭据缺失/未配置/解密失败。message 面向用户，给出可操作的修复指引。"""


DEFAULT_FILE = ".plaita-credentials.json"
DEFAULT_KEY_FILE = ".plaita-credentials.key"


def credentials_file(tenant_id: Optional[str] = None) -> Path:
    """当前（或指定）租户的凭据文件路径。

    ``tenant_id=None`` 取 :func:`current_tenant`；default/空租户用
    ``PLAITA_CREDENTIALS_FILE``，其余租户用同目录旁文件——两处命名规则
    必须与 console 导出侧一致，否则 worker 读到空文件。
    """
    base = Path(os.environ.get("PLAITA_CREDENTIALS_FILE", DEFAULT_FILE))
    tid = current_tenant() if tenant_id is None else tenant_id
    if not tid or tid == DEFAULT_TENANT_ID:
        return base
    return base.with_name(f"{base.stem}.{tid}{base.suffix}")


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
        known = ", ".join(sorted(data)) or "（无）"
        raise CredentialError(f"凭据 {name!r} 不存在（可用: {known}）。请在编排台「凭据」页创建")
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
