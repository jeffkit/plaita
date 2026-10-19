"""凭据解析——节点运行时按名取用外部服务的机密信息。

存储形态：加密 JSON 文件（由编排台 console 在凭据保存时**原子替换**导出），
``PLAITA_CREDENTIALS_FILE`` 指定路径（默认 ``.plaita-credentials.json``），
``PLAITA_CREDENTIALS_KEY`` 为 Fernet 密钥（未设时尝试同级 ``.plaita-credentials.key``
文件）。加密/解密依赖 ``cryptography``（``pip install plaita[credentials]``）。

文件缺失视为「尚未配置凭据」（空 store）；文件存在但为空/损坏则抛
:class:`CredentialStoreCorruptError`（fail-loud），不会静默当成空 store。
导出写失败不会破坏旧文件——旧内容仍可正常解析。

多租户：解析时读租户上下文（``plaita.tenant_context.current_tenant``，与
分布式存储/日志同源）——非 default 租户读旁文件
``<stem>.<tenant_id><suffix>``（console 凭据页按租户导出的同一命名），
default/空租户沿用历史文件（零行为变化）。租户隔离由文件路径保证：
非 default 租户只看得见自己旁文件里的凭据，default 文件对它不可见；
报错信息只列**当前租户文件内**的凭据名，不泄露其他租户的存在。

节点内用法::

    from plaita.credentials import get_credential
    cred = get_credential("feishu-bot")   # -> {"url": "https://...", ...}

跨进程用法（plaita#256）：ContextVar 不跨进程，子进程里的 ``get_credential``
不会自动继承父进程的租户。需要把基础凭据文件与密钥路径经 env 显式传入
（:func:`subprocess_credentials_env`），子进程入口再 ``set_current_tenant``，
由 ``credentials_file()`` 按租户展开旁文件。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


class CredentialError(RuntimeError):
    """凭据缺失/未配置/解密失败。message 面向用户，给出可操作的修复指引。"""


class CredentialStoreCorruptError(CredentialError):
    """凭据文件存在但为空/损坏/不可读——与「文件不存在（尚未配置）」区分。

    文件缺失时返回空 store（合法的「还没有凭据」）；文件存在却读不出完整
    JSON 时不能静默当成空 store，否则所有凭据会表现为「不存在」，把排查
    方向引向重新创建凭据，掩盖真正的导出写失败/文件截断。
    """


DEFAULT_FILE = ".plaita-credentials.json"
DEFAULT_KEY_FILE = ".plaita-credentials.key"
DEFAULT_TENANT_ID = "default"


def credentials_base_file() -> Path:
    """未按租户展开的基础凭据文件路径（``PLAITA_CREDENTIALS_FILE`` 或默认）。"""
    return Path(os.environ.get("PLAITA_CREDENTIALS_FILE", DEFAULT_FILE))


def tenant_credentials_file(tenant_id: Optional[str], base: Optional[Path] = None) -> Path:
    """租户 → 凭据文件路径。default/空 = 基础文件（历史兼容），
    其余租户 = 旁文件 ``<stem>.<tenant_id><suffix>``（与 console 导出同规则）。"""
    if base is None:
        base = credentials_base_file()
    tid = tenant_id or DEFAULT_TENANT_ID
    if tid == DEFAULT_TENANT_ID:
        return base
    return base.with_name(f"{base.stem}.{tid}{base.suffix}")


def credentials_file() -> Path:
    """当前租户上下文对应的凭据文件（节点运行时经这里路由）。"""
    from plaita.tenant_context import current_tenant

    return tenant_credentials_file(current_tenant())


def credentials_key_file() -> Path:
    """凭据解密密钥文件路径（``PLAITA_CREDENTIALS_KEY_FILE`` 或 cwd 默认文件）。

    只返回**路径**；``PLAITA_CREDENTIALS_KEY`` 的内联密钥不在此暴露——跨进程
    调用方应显式传路径，不要把密钥材料复制进子进程环境（plaita#256）。
    """
    return Path(os.environ.get("PLAITA_CREDENTIALS_KEY_FILE", DEFAULT_KEY_FILE))


def subprocess_credentials_env() -> Dict[str, str]:
    """子进程凭据路由的显式 env（plaita#256）。

    返回可直接并入 ``plaita.subprocess_env.build_subprocess_env(extra=...)`` 的
    ``PLAITA_CREDENTIALS_FILE`` / ``PLAITA_CREDENTIALS_KEY_FILE``：
    子进程的 ``get_credential()`` 与父进程看到同一租户文件，且密钥路径被
    显式固定，不再静默落回 cwd 的 default 文件。

    ``PLAITA_CREDENTIALS_FILE`` 传的是**基础文件**，不是已展开的旁文件——
    子进程入口会显式恢复租户上下文，``credentials_file()`` 再按同一规则展开
    一次。传旁文件会被二次追加 ``.<tenant>`` 后缀（plaita#256 首版实测）。
    密钥只传**路径**，不传内联 ``PLAITA_CREDENTIALS_KEY``——后者只存在于
    宿主环境，复制给沙箱代码等于把可解密所有租户文件的密钥交出去。
    """
    return {
        "PLAITA_CREDENTIALS_FILE": str(credentials_base_file()),
        "PLAITA_CREDENTIALS_KEY_FILE": str(credentials_key_file()),
    }


def _load_key() -> bytes:
    key = os.environ.get("PLAITA_CREDENTIALS_KEY")
    if key:
        return key.encode()
    key_file = credentials_key_file()
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


def _corrupt(path: Path, reason: str) -> CredentialStoreCorruptError:
    return CredentialStoreCorruptError(
        f"凭据文件 {path} {reason}。导出文件由编排台在保存凭据时重建："
        "请核对磁盘空间与文件权限后，在编排台「凭据」页重新保存任意一条凭据"
    )


def _read_store() -> Dict[str, Dict[str, Any]]:
    path = credentials_file()
    if not path.is_file():
        return {}
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        # is_file() 与读取之间的外部删除：仍按「尚未配置」处理。
        return {}
    except OSError as e:
        raise _corrupt(path, f"无法读取（{e}）") from e
    if not raw.strip():
        raise _corrupt(path, "为空（导出写入可能被中断/截断）")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise _corrupt(
            path, f"不是合法 JSON（{e.msg}，line {e.lineno} column {e.colno}）"
        ) from e
    if not isinstance(data, dict):
        raise _corrupt(path, f"顶层必须是对象，实际为 {type(data).__name__}")
    return data
