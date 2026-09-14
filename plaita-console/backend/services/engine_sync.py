"""
console 流程库 → 引擎运行时存储 的同步。

console 的流程定义存 SQLite（flow_store），而 FlowWorker 从引擎的
Redis 流程存储（plaita.storage.redis.RedisFlowStorage）解析定义。不做这个同步，
「发布 → 启动」链路在 worker 侧永远找不到流程——界面全通、执行全断。

多租户：键空间按租户隔离（与引擎侧 tenant_context 同一映射规则）——
``{ns}:flow:{flow_id}:{version}``，ns 对 default/空租户为历史前缀 ``plaita``，
其余租户为 ``plaita:{tenant_id}``。

同步时机：发布（publish）写引擎存储；删除流程/版本时清理。
同步失败只记日志不阻断主流程（console 是权威库，可重新发布修复）。
"""
from __future__ import annotations

import json
import logging
from typing import Dict, Optional, Union

from redis import Redis

try:
    from plaita.storage.redis import RedisFlowStorage
    from plaita.server.tenant_context import tenant_namespace
except ImportError:  # 平铺布局（cwd=backend）运行时
    import sys
    from pathlib import Path

    _plaita_root = str(Path(__file__).resolve().parents[3])
    if _plaita_root not in sys.path:
        sys.path.insert(0, _plaita_root)
    from plaita.storage.redis import RedisFlowStorage
    from plaita.server.tenant_context import tenant_namespace

logger = logging.getLogger(__name__)


def _storage(redis: Redis, tenant_id: Optional[str] = None) -> RedisFlowStorage:
    return RedisFlowStorage(client=redis, namespace=tenant_namespace(tenant_id))


def sync_flow_to_engine(redis: Redis, flow_id: str, version: str,
                        definition: Union[str, Dict],
                        tenant_id: Optional[str] = None) -> bool:
    """把某个已发布版本的定义写入引擎 Redis 流程存储（归属租户的 namespace）。"""
    try:
        defn = json.loads(definition) if isinstance(definition, str) else dict(definition)
    except (TypeError, json.JSONDecodeError) as e:
        logger.error("同步流程 %s@%s 失败：definition 不是合法 JSON: %s", flow_id, version, e)
        return False

    defn["flow_id"] = flow_id
    defn["version"] = version
    try:
        ok = _storage(redis, tenant_id).save_flow(defn)
        if not ok:
            logger.error("同步流程 %s@%s 到引擎存储失败（租户 %s）", flow_id, version, tenant_id)
        else:
            logger.info("已同步流程 %s@%s 到引擎存储（租户 %s）", flow_id, version, tenant_id)
        return ok
    except Exception as e:
        logger.error("同步流程 %s@%s 到引擎存储异常: %s", flow_id, version, e, exc_info=True)
        return False


def remove_flow_from_engine(redis: Redis, flow_id: str,
                            tenant_id: Optional[str] = None) -> None:
    """删除流程时清理引擎存储（全部版本 + 注册集合）。尽力而为。"""
    try:
        _storage(redis, tenant_id).delete_flow(flow_id)
        logger.info("已从引擎存储清理流程 %s（租户 %s）", flow_id, tenant_id)
    except Exception as e:
        logger.warning("清理引擎存储流程 %s 失败: %s", flow_id, e, exc_info=True)


def remove_flow_version_from_engine(redis: Redis, flow_id: str, version: str,
                                    tenant_id: Optional[str] = None) -> None:
    """删除单个版本时清理引擎存储对应键与版本注册。尽力而为。"""
    ns = tenant_namespace(tenant_id)
    try:
        redis.delete(f"{ns}:flow:{flow_id}:{version}")
        redis.srem(f"{ns}:flow_versions:{flow_id}", version)
        logger.info("已从引擎存储清理版本 %s@%s（租户 %s）", flow_id, version, tenant_id)
    except Exception as e:
        logger.warning("清理引擎存储版本 %s@%s 失败: %s", flow_id, version, e, exc_info=True)
