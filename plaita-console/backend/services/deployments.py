"""部署/晋升服务。

- 每次 publish 写一条部署记录（环境、操作人、定义 SHA-256 指纹）
- 晋升 = 在环境 A 的 console 导出版本「晋升包」（定义 + 指纹），
  到环境 B 的 console 导入为草稿再发布；导入时校验指纹，防篡改/错包。

环境由 console 实例的 ``PLAITA_CONSOLE_ENV`` 标识（dev/test/prod）。

A2R25-01：指纹必须对**解析后的语义**计算。旧实现导出时对原始 definition
字符串哈希、导入时对 ``json.dumps(ensure_ascii=False)`` 重排后的字符串哈希，
任何缩进/键序/ASCII 转义差异都会让正常包被误报为「已修改或损坏」（UI 的
``JSON.stringify(def, null, 2)`` 是主路径）。现在：

- ``definition_hash_v2`` = 正典 JSON（``sort_keys`` + 紧凑分隔符）的 SHA-256；
- ``definition_hash`` 保留为「``ensure_ascii=False`` 紧凑 dump」的 SHA-256，
  让新导出的包仍可被旧版本 console 的导入逻辑识别（旧逻辑正是这样重算）。

A2R25-03：导入复用保存路径的 semver / flow_id / Flow schema 校验，且不再
接受 ``publish=true`` 直接落 published——发布只由 API 层在校验后调用
``publish_version``，避免绕过正常闸门。
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
    from . import validation as validation_svc
except ImportError:
    from models.flow import Deployment  # type: ignore
    from services import validation as validation_svc  # type: ignore

logger = logging.getLogger(__name__)

PROMOTION_HASH_VERSION = 2


class PromotionValidationError(ValueError):
    """晋升包字段/定义不合法（API 映射 422）。"""


class PromotionConflictError(ValueError):
    """目标版本已被占用（API 映射 409），携带结构化冲突信息。"""

    def __init__(self, message: str, *, conflict: str, **details: Any) -> None:
        super().__init__(message)
        self.conflict = conflict
        self.details = details


def current_environment() -> str:
    return os.environ.get("PLAITA_CONSOLE_ENV", "dev")


def definition_hash(definition: str) -> str:
    return hashlib.sha256(definition.encode()).hexdigest()


def canonical_definition(definition: Any) -> str:
    """定义 → 与缩进/键序/ASCII 转义无关的正典 JSON 串。"""
    obj = json.loads(definition) if isinstance(definition, str) else definition
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_definition_hash(definition: Any) -> str:
    return definition_hash(canonical_definition(definition))


def _legacy_definition_dumps(definition: Any) -> List[str]:
    """旧导入器可能重算出的两种 dump 形状（默认 separators）。"""
    obj = json.loads(definition) if isinstance(definition, str) else definition
    return [
        json.dumps(obj, ensure_ascii=False),
        json.dumps(obj, ensure_ascii=True),
    ]


def verify_package_hash(package: Dict[str, Any]) -> None:
    """校验包内定义指纹；兼容只有 ``definition_hash`` 的旧包。

    先于 Flow schema 校验执行：被篡改的包应得到「指纹校验失败」这一具名
    结论，而不是被 schema 校验抢先报成另一个错误。
    """
    definition = package.get("definition")
    expected_v2 = package.get("definition_hash_v2")
    expected = package.get("definition_hash")
    if expected_v2:
        try:
            actual = canonical_definition_hash(definition)
        except (TypeError, ValueError) as e:
            raise PromotionValidationError(f"晋升包 definition 无法解析: {e}") from e
        if actual != expected_v2:
            raise ValueError("定义指纹校验失败：晋升包已被修改或损坏")
        return
    if expected:
        try:
            legacy_hashes = {
                definition_hash(serialized)
                for serialized in _legacy_definition_dumps(definition)
            }
        except (TypeError, ValueError) as e:
            raise PromotionValidationError(f"晋升包 definition 无法解析: {e}") from e
        if expected not in legacy_hashes:
            raise ValueError("定义指纹校验失败：晋升包已被修改或损坏")


def record(flow_id: str, version: str, environment: str, actor: str = "",
           definition: str = "", tenant_id: str = "") -> None:
    from datetime import datetime

    try:
        from .flow_store import get_flow_store
    except ImportError:
        from flow_store import get_flow_store  # type: ignore
    store = get_flow_store()
    with store._session_local() as session:
        session.add(
            Deployment(
                tenant_id=tenant_id,
                flow_id=flow_id,
                version=version,
                environment=environment,
                actor=actor,
                definition_hash=definition_hash(definition) if definition else "",
                created_at=datetime.utcnow(),
            )
        )
        session.commit()


def list_deployments(flow_id: Optional[str] = None, limit: int = 200,
                     tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
    try:
        from .flow_store import get_flow_store
    except ImportError:
        from flow_store import get_flow_store  # type: ignore
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Deployment).order_by(Deployment.created_at.desc()).limit(limit)
        if tenant_id is not None:
            query = query.where(Deployment.tenant_id == tenant_id)
        if flow_id:
            query = query.where(Deployment.flow_id == flow_id)
        return [
            {
                "flow_id": r.flow_id,
                "tenant_id": r.tenant_id,
                "version": r.version,
                "environment": r.environment,
                "actor": r.actor,
                "definition_hash": r.definition_hash,
                "created_at": r.created_at.isoformat(),
            }
            for r in session.scalars(query).all()
        ]


def build_promotion_package(store, flow_id: str, version: str,
                            tenant_id: Optional[str] = None) -> Dict[str, Any]:
    """导出晋升包：定义 + 元信息 + 指纹（跨环境 console 间晋升的载体）。"""
    record_out = store.get_version(flow_id, version, tenant_id=tenant_id)
    if record_out is None:
        raise LookupError(f"版本不存在: {flow_id}@{version}")
    definition_obj = json.loads(record_out.definition)
    return {
        "kind": "plaita-promotion",
        "flow_id": flow_id,
        "tenant_id": tenant_id or "",
        "version": version,
        "definition": definition_obj,
        # 旧字段：保持旧导入器可识别的紧凑 dump 哈希（不随存储缩进变化）。
        "definition_hash": definition_hash(
            json.dumps(definition_obj, ensure_ascii=False)
        ),
        # 新字段：语义正典哈希（sort_keys + 紧凑分隔符），优先于旧字段校验。
        "definition_hash_v2": canonical_definition_hash(definition_obj),
        "definition_hash_version": PROMOTION_HASH_VERSION,
        "layout": record_out.layout,
        # plaita#300：目标 runtime pool 随包晋升（旧包缺此键 → 导入侧默认 python）
        "runtime": getattr(record_out, "runtime", None) or "python",
        "exported_from": current_environment(),
        "exported_at": datetime.utcnow().isoformat(),
    }


def import_promotion_package(store, package: Dict[str, Any],
                             new_version: Optional[str] = None,
                             tenant_id: str = "",
                             force: bool = False) -> Dict[str, Any]:
    """导入晋升包为草稿。指纹/字段/定义校验失败拒绝导入。

    - 目标版本已发布：409 ``published_immutable``（不静默改已发布内容）；
    - 目标版本已有草稿且未传 ``force=True``：409 ``draft_exists``；
    - 覆盖草稿时返回旧定义哈希，供 API 层写审计（A2R25-02）。

    发布不由本函数完成：API 层在导入成功后另行校验并调用
    ``publish_version``，避免 ``publish=true`` 绕过 Flow schema 闸（A2R25-03）。
    导入归属当前租户上下文（tenant_id）。
    """
    if not isinstance(package, dict):
        raise PromotionValidationError("晋升包必须是 JSON 对象")
    if package.get("kind") != "plaita-promotion":
        raise ValueError("不是合法的晋升包（kind 不符）")
    flow_id = package.get("flow_id")
    version = new_version if new_version is not None else package.get("version")
    definition_obj = package.get("definition")
    if not isinstance(flow_id, str) or not flow_id.strip():
        raise PromotionValidationError("晋升包缺少 flow_id")
    try:
        validation_svc.validate_flow_id(flow_id)
    except ValueError as e:
        raise PromotionValidationError(str(e)) from e
    if not isinstance(version, str) or not version:
        raise PromotionValidationError("晋升包缺少 version")
    if not validation_svc.is_semver(version):
        raise PromotionValidationError(f"非法 semver 版本号: {version}")
    if not isinstance(definition_obj, dict):
        raise PromotionValidationError("晋升包 definition 必须是 JSON 对象")
    layout = package.get("layout")
    if layout is None:
        layout = ""
    if not isinstance(layout, str):
        raise PromotionValidationError("晋升包 layout 必须是 JSON 字符串")
    # plaita#300：runtime 随包晋升；缺键（旧包）= 默认池 python，非法值拒绝。
    runtime = package.get("runtime")
    if runtime is not None:
        try:
            from .runtime_pools import normalize_runtime
        except ImportError:
            from runtime_pools import normalize_runtime  # type: ignore
        try:
            runtime = normalize_runtime(runtime)
        except ValueError as e:
            raise PromotionValidationError(f"晋升包 runtime 非法: {e}") from e

    # 先校验指纹（被篡改的包得到具名 400），再做 Flow schema 校验。
    verify_package_hash(package)
    try:
        validation_svc.validate_definition(definition_obj)
    except ValueError as e:
        raise PromotionValidationError(str(e)) from e

    definition = json.dumps(definition_obj, ensure_ascii=False)
    existing = store.get_version(flow_id, version, tenant_id=tenant_id)
    previous_status: Optional[str] = None
    previous_hash: Optional[str] = None
    if existing is not None:
        previous_status = existing.status
        previous_hash = canonical_definition_hash(existing.definition or "{}")
        if existing.status == "published":
            raise PromotionConflictError(
                f"版本 {flow_id}@{version} 已发布，不可覆盖；"
                "灾备恢复请使用新的版本号",
                conflict="published_immutable",
                previous_definition_hash=previous_hash,
            )
        if not force:
            raise PromotionConflictError(
                f"版本 {flow_id}@{version} 已存在草稿，导入会覆盖当前内容；"
                "请显式 force=true 确认覆盖",
                conflict="draft_exists",
                latest_updated_at=(
                    existing.updated_at.isoformat() if existing.updated_at else None
                ),
                previous_definition_hash=previous_hash,
            )

    store.ensure_flow(flow_id, tenant_id=tenant_id)
    store.save_flow_definition(
        flow_id=flow_id,
        version=version,
        definition=definition,
        layout=layout,
        status="draft",
        created_by="promotion",
        tenant_id=tenant_id,
        force=force,
        runtime=runtime,
    )
    return {
        "flow_id": flow_id,
        "version": version,
        "status": "draft",
        "overwritten": existing is not None,
        "previous_status": previous_status,
        "previous_definition_hash": previous_hash,
        "definition_hash": canonical_definition_hash(definition_obj),
    }
