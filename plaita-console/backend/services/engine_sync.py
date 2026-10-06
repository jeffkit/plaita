"""
console 流程库 → 引擎运行时存储 的同步。

console 的流程定义存 SQLite/PG（flow_store），而 FlowWorker 从引擎的
Redis 流程存储（plaita.storage.redis.RedisFlowStorage）解析定义。不做这个同步，
「发布 → 启动」链路在 worker 侧永远找不到流程——界面全通、执行全断。

多租户：键空间按租户隔离（与引擎侧 tenant_context 同一映射规则）——
``{ns}:flow:{flow_id}:{version}``，ns 对 default/空租户为历史前缀 ``plaita``，
其余租户为 ``plaita:{tenant_id}``。

同步时机：
- 发布（publish）写引擎存储；删除流程/版本时清理（逐事件钩子）。
- 启动时 ``sync_all_published_to_engine`` 尽力回填全部已发布版本，兜底
  「历史版本从未重跑发布」的漂移（如 console 从 SQLite 迁到 PG 后 Redis 为空）。

**边界（务必知悉）**：本模块只把定义写进「console 进程所连的那个 Redis」。
console 与 FlowWorker **可能不在同一台机器、不共享同一 Redis**——此时本模块
（含启动钩子）只解决同机同 Redis 场景；跨机场景必须由运维在目标 Redis 所在
机器上跑 ``plaita-console/scripts/sync_published_to_engine.py``（用目标机器的
``PLAITA_CONSOLE_REDIS_URL``）来对齐。启动钩子不代表跨机同步已解决。

同步失败只记日志不阻断主流程（console 是权威库，可重新发布/重跑脚本修复）。
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


def sync_all_published_to_engine(redis: Redis, store, tenant_id: Optional[str] = None
                                 ) -> Dict[str, int]:
    """把 store 里全部 status=published 的版本回填/对齐进引擎 Redis 存储。

    幂等：``save_flow`` 是覆盖写，重复调用只会重写同样内容，不产生脏数据。
    用于启动时的漂移兜底（历史版本从未重跑发布）以及运维批量对齐。

    入参 ``store`` 为 console FlowStore（鸭子类型：只需 ``list_flows`` /
    ``list_versions``，避免与 flow_store 循环 import）。``tenant_id=None``
    表示跨全部租户（``list_flows(None)`` 返回平台全量）。

    **仅写本进程所连的 Redis**——跨机的 worker 需在目标机器另跑回填脚本
    （见模块 docstring 的「边界」段）。返回 ``{"written", "skipped", "errors"}``。

    本函数绝不抛异常：任何单条失败都计入 errors 并继续，供启动钩子安全调用。
    """
    stats = {"written": 0, "skipped": 0, "errors": 0}
    try:
        flows = store.list_flows(tenant_id)
    except Exception as e:  # 读库失败：整体放弃，交由调用方处理
        logger.warning("回填引擎存储：列举流程失败（租户 %s）: %s", tenant_id, e, exc_info=True)
        stats["errors"] += 1
        return stats

    for flow in flows:
        fid = getattr(flow, "flow_id", None)
        ftenant = getattr(flow, "tenant_id", tenant_id) or tenant_id
        if not fid:
            continue
        try:
            versions = store.list_versions(fid, tenant_id=ftenant)
        except Exception as e:
            logger.warning("回填引擎存储：列举 %s 版本失败: %s", fid, e)
            stats["errors"] += 1
            continue
        for v in versions:
            if getattr(v, "status", "") != "published":
                continue
            if not (getattr(v, "definition", "") or "").strip():
                stats["skipped"] += 1
                continue
            if sync_flow_to_engine(redis, fid, v.version, v.definition,
                                   tenant_id=ftenant):
                stats["written"] += 1
            else:
                stats["errors"] += 1
    logger.info("引擎存储回填完成：%s", stats)
    return stats
