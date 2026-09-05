"""部署/晋升服务。

- 每次 publish 写一条部署记录（环境、操作人、定义 SHA-256 指纹）
- 晋升 = 在环境 A 的 console 导出版本「晋升包」（定义 + 指纹），
  到环境 B 的 console 导入为草稿再发布；导入时校验指纹，防篡改/错包。

环境由 console 实例的 ``PLAITA_CONSOLE_ENV`` 标识（dev/test/prod）。
"""
from __future__ import annotations

import hashlib
import logging
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select

try:
    from ..models.flow import Deployment
except ImportError:
    from models.flow import Deployment  # type: ignore

logger = logging.getLogger(__name__)


def current_environment() -> str:
    return os.environ.get("PLAITA_CONSOLE_ENV", "dev")


def definition_hash(definition: str) -> str:
    return hashlib.sha256(definition.encode()).hexdigest()


def record(flow_id: str, version: str, environment: str, actor: str = "",
           definition: str = "") -> None:
    from datetime import datetime

    try:
        from .flow_store import get_flow_store
    except ImportError:
        from flow_store import get_flow_store  # type: ignore
    store = get_flow_store()
    with store._session_local() as session:
        session.add(
            Deployment(
                flow_id=flow_id,
                version=version,
                environment=environment,
                actor=actor,
                definition_hash=definition_hash(definition) if definition else "",
                created_at=datetime.utcnow(),
            )
        )
        session.commit()


def list_deployments(flow_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    try:
        from .flow_store import get_flow_store
    except ImportError:
        from flow_store import get_flow_store  # type: ignore
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Deployment).order_by(Deployment.created_at.desc()).limit(limit)
        if flow_id:
            query = query.where(Deployment.flow_id == flow_id)
        return [
            {
                "flow_id": r.flow_id,
                "version": r.version,
                "environment": r.environment,
                "actor": r.actor,
                "definition_hash": r.definition_hash,
                "created_at": r.created_at.isoformat(),
            }
            for r in session.scalars(query).all()
        ]


def build_promotion_package(store, flow_id: str, version: str) -> Dict[str, Any]:
    """导出晋升包：定义 + 元信息 + 指纹（跨环境 console 间晋升的载体）。"""
    record_out = store.get_version(flow_id, version)
    if record_out is None:
        raise LookupError(f"版本不存在: {flow_id}@{version}")
    return {
        "kind": "plaita-promotion",
        "flow_id": flow_id,
        "version": version,
        "definition": json.loads(record_out.definition),
        "layout": record_out.layout,
        "definition_hash": definition_hash(record_out.definition),
        "exported_from": current_environment(),
        "exported_at": datetime.utcnow().isoformat(),
    }


def import_promotion_package(store, package: Dict[str, Any],
                             new_version: Optional[str] = None,
                             publish: bool = False) -> Dict[str, Any]:
    """导入晋升包为草稿（可选直接发布）。指纹校验失败拒绝导入。

    返回 {flow_id, version, status}。发布需调用方（API 层）再走 publish 以
    复用引擎同步 + 部署记录路径。
    """
    if package.get("kind") != "plaita-promotion":
        raise ValueError("不是合法的晋升包（kind 不符）")
    flow_id = package["flow_id"]
    version = new_version or package["version"]
    definition = json.dumps(package["definition"], ensure_ascii=False)
    expected = package.get("definition_hash")
    if expected and definition_hash(definition) != expected:
        raise ValueError("定义指纹校验失败：晋升包已被修改或损坏")

    store.ensure_flow(flow_id)
    store.save_flow_definition(
        flow_id=flow_id,
        version=version,
        definition=definition,
        layout=package.get("layout") or "",
        status="published" if publish else "draft",
        created_by="promotion",
    )
    return {"flow_id": flow_id, "version": version,
            "status": "published" if publish else "draft"}
