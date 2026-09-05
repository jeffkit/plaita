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
    from ..models.flow import Base, Credential
    from .flow_store import get_flow_store
except ImportError:  # 平铺布局（cwd=backend，services 为顶层包）
    from models.flow import Base, Credential  # type: ignore
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


def credentials_file() -> Path:
    return Path(os.environ.get("PLAITA_CREDENTIALS_FILE", Path.cwd() / ".plaita-credentials.json"))


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


def list_credentials() -> List[Dict[str, Any]]:
    """列出凭据元信息（不含密文）。"""
    store = get_flow_store()
    with store._session_local() as session:
        rows = session.scalars(select(Credential).order_by(Credential.name)).all()
        return [
            {"name": r.name, "type": r.type, "desc": r.desc, "updated_at": r.updated_at.isoformat()}
            for r in rows
        ]


def get_credential_record(name: str) -> Optional[Dict[str, Any]]:
    store = get_flow_store()
    with store._session_local() as session:
        row = session.scalars(select(Credential).where(Credential.name == name)).first()
        if row is None:
            return None
        return {"name": row.name, "type": row.type, "desc": row.desc, "data": _decrypt(row.data_json)}


def save_credential(name: str, type_: str, data: Dict[str, Any], desc: str = "") -> None:
    if not name:
        raise ValueError("凭据名称不能为空")
    if not isinstance(data, dict) or not data:
        raise ValueError("凭据数据必须是非空对象")
    store = get_flow_store()
    encrypted = _encrypt(data)
    with store._session_local() as session:
        row = session.scalars(select(Credential).where(Credential.name == name)).first()
        if row is None:
            session.add(Credential(name=name, type=type_, data_json=encrypted, desc=desc))
        else:
            row.type = type_
            row.data_json = encrypted
            if desc:
                row.desc = desc
        session.commit()
    _export_store()


def delete_credential(name: str) -> bool:
    store = get_flow_store()
    with store._session_local() as session:
        row = session.scalars(select(Credential).where(Credential.name == name)).first()
        if row is None:
            return False
        session.delete(row)
        session.commit()
    _export_store()
    return True


def _export_store() -> None:
    """把全部凭据（密文）导出为引擎可读文件，供节点运行时解密。"""
    store = get_flow_store()
    with store._session_local() as session:
        rows = session.scalars(select(Credential)).all()
        payload = {
            r.name: {"type": r.type, "data": r.data_json}
            for r in rows
        }
    path = credentials_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    logger.info("已导出凭据文件 %s（%d 条）", path, len(payload))
