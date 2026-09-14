"""凭据管理服务：加密落库 + 导出引擎可读的凭据文件。

密钥解析顺序（console 与引擎侧 plaita.credentials 一致）：
1. 环境变量 ``PLAITA_CREDENTIALS_KEY``（Fernet key）
2. 密钥文件 ``PLAITA_CREDENTIALS_KEY_FILE``（默认与 DB 同目录 ``.plaita-credentials.key``，
   首次使用自动生成 440 权限）

安全边界（如实说明）：密钥文件与 DB 同机存放，属静态加密（at-rest）而非
密钥管理服务；多机部署请把 KEY 放到密钥管理设施并统一注入。
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select

try:
    from ..models.flow import Base, Credential, DEFAULT_TENANT_ID
    from .flow_store import get_flow_store
except ImportError:  # 平铺布局（cwd=backend，services 为顶层包）
    from models.flow import Base, Credential, DEFAULT_TENANT_ID  # type: ignore
    from services.flow_store import get_flow_store  # type: ignore

logger = logging.getLogger(__name__)


class CredentialsDisabledError(RuntimeError):
    """密钥初始化失败等导致凭据功能不可用。"""


def _db_dir() -> Path:
    """从 PLAITA_CONSOLE_DB_URL 推断 DB 所在目录（默认 cwd）。"""
    db_url = os.environ.get("PLAITA_CONSOLE_DB_URL", "sqlite:///./plaita_console.db")
    if db_url.startswith("sqlite:///"):
        raw = db_url[len("sqlite:///"):]
        return Path(raw).resolve().parent
    return Path.cwd()


def credentials_key_file() -> Path:
    return Path(os.environ.get("PLAITA_CREDENTIALS_KEY_FILE", _db_dir() / ".plaita-credentials.key"))


def credentials_file(tenant_id: str = DEFAULT_TENANT_ID) -> Path:
    """凭据导出文件路径：default 租户沿用历史文件（兼容既有引擎读取），
    其余租户写旁文件 ``<stem>.<tenant_id><suffix>``，实现租户隔离。"""
    base = Path(os.environ.get("PLAITA_CREDENTIALS_FILE", Path.cwd() / ".plaita-credentials.json"))
    if tenant_id == DEFAULT_TENANT_ID:
        return base
    return base.with_name(f"{base.stem}.{tenant_id}{base.suffix}")


def _fernet() -> Fernet:
    key = os.environ.get("PLAITA_CREDENTIALS_KEY")
    if not key:
        key_file = credentials_key_file()
        if key_file.is_file():
            key = key_file.read_bytes().strip().decode()
        else:
            key = Fernet.generate_key().decode()
            key_file.parent.mkdir(parents=True, exist_ok=True)
            key_file.write_bytes(key.encode())
            try:
                os.chmod(key_file, 0o600)
            except OSError:
                pass
            logger.info("已生成凭据密钥文件: %s", key_file)
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception as e:  # noqa: BLE001
        raise CredentialsDisabledError(f"PLAITA_CREDENTIALS_KEY 不是合法 Fernet key: {e}") from e


def _encrypt(data: Dict[str, Any]) -> str:
    return _fernet().encrypt(json.dumps(data, ensure_ascii=False).encode()).decode()


def _decrypt(token: str) -> Dict[str, Any]:
    try:
        return json.loads(_fernet().decrypt(token.encode()).decode())
    except InvalidToken as e:
        raise CredentialsDisabledError(
            "凭据解密失败：PLAITA_CREDENTIALS_KEY/密钥文件与写入时不一致"
        ) from e


def list_credentials(tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """列出凭据元信息（不含密文）。tenant_id=None 为平台全量视角。"""
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Credential).order_by(Credential.name)
        if tenant_id is not None:
            query = query.where(Credential.tenant_id == tenant_id)
        rows = session.scalars(query).all()
        return [
            {
                "name": r.name,
                "tenant_id": r.tenant_id,
                "type": r.type,
                "desc": r.desc,
                "updated_at": r.updated_at.isoformat(),
            }
            for r in rows
        ]


def get_credential_record(name: str, tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Credential).where(Credential.name == name)
        if tenant_id is not None:
            query = query.where(Credential.tenant_id == tenant_id)
        row = session.scalars(query).first()
        if row is None:
            return None
        return {"name": row.name, "tenant_id": row.tenant_id, "type": row.type, "desc": row.desc, "data": _decrypt(row.data_json)}


def save_credential(name: str, type_: str, data: Dict[str, Any], desc: str = "",
                    tenant_id: str = DEFAULT_TENANT_ID) -> None:
    if not name:
        raise ValueError("凭据名称不能为空")
    if not isinstance(data, dict) or not data:
        raise ValueError("凭据数据必须是非空对象")
    store = get_flow_store()
    encrypted = _encrypt(data)
    with store._session_local() as session:
        row = session.scalars(
            select(Credential).where(
                Credential.tenant_id == tenant_id, Credential.name == name
            )
        ).first()
        if row is None:
            session.add(
                Credential(
                    tenant_id=tenant_id, name=name, type=type_, data_json=encrypted, desc=desc
                )
            )
        else:
            row.type = type_
            row.data_json = encrypted
            if desc:
                row.desc = desc
        session.commit()
    _export_store(tenant_id)


def delete_credential(name: str, tenant_id: Optional[str] = None) -> bool:
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Credential).where(Credential.name == name)
        if tenant_id is not None:
            query = query.where(Credential.tenant_id == tenant_id)
        row = session.scalars(query).first()
        if row is None:
            return False
        session.delete(row)
        session.commit()
        affected = row.tenant_id
    _export_store(affected)
    return True


def _export_store(tenant_id: Optional[str] = None) -> None:
    """把凭据（密文）按租户导出为引擎可读文件，供节点运行时解密。

    tenant_id=None 导出全部租户（各写各的文件）；指定租户只重导该租户。
    default 租户沿用历史文件路径，保持既有引擎读取兼容。
    """
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Credential)
        if tenant_id is not None:
            query = query.where(Credential.tenant_id == tenant_id)
        rows = session.scalars(query).all()
    by_tenant: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        by_tenant.setdefault(r.tenant_id or DEFAULT_TENANT_ID, {})[r.name] = {
            "type": r.type,
            "data": r.data_json,
        }
    if tenant_id is not None:
        # 指定租户：即使已清空也要重写文件（删除最后一条凭据时清空残留）
        targets = {tenant_id: by_tenant.get(tenant_id, {})}
    else:
        # 全量：覆盖存在的租户 + default（历史文件路径始终刷新）
        targets = dict(by_tenant)
        targets.setdefault(DEFAULT_TENANT_ID, {})
    for tid, payload in targets.items():
        path = credentials_file(tid)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        logger.info("已导出租户 %s 凭据文件 %s（%d 条）", tid, path, len(payload))
