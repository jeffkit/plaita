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
import os
import time
from typing import Any, Dict, Iterator, Optional, Tuple, Union

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from plaita.core.url_redaction import _redact_env_value
from plaita.tenant_context import guard_tenant_write

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

#: 每批 pipeline 的版本数（A2R20-06）：3 次往返/版本 → ~1 次/批。
DEFAULT_SYNC_BATCH_SIZE = 200
#: 启动回填墙钟预算（秒）：超预算不阻断启动，状态经 /health 暴露为 degraded。
DEFAULT_SYNC_DEADLINE_SECONDS = 5.0
#: 单次回填的版本数上限（与 deadline 双保险；触界即 truncated）。
DEFAULT_SYNC_MAX_VERSIONS = 50_000

_BATCH_ENV = "PLAITA_CONSOLE_ENGINE_SYNC_BATCH"
_DEADLINE_ENV = "PLAITA_CONSOLE_ENGINE_SYNC_DEADLINE"
_MAX_VERSIONS_ENV = "PLAITA_CONSOLE_ENGINE_SYNC_MAX_VERSIONS"


def _storage(redis: Redis, tenant_id: Optional[str] = None) -> RedisFlowStorage:
    return RedisFlowStorage(client=redis, namespace=tenant_namespace(tenant_id))


def _unencodable_reason(flow_id: str, version: str, defn: Dict) -> Optional[str]:
    """save 面编码闸（卡 A-1）：与发布闸同一判据，不可编码返回具名原因。

    复用 ``engine_outbox.ensure_definition_encodable``（对「投递后工件」跑
    同款 hash 计算，UnicodeEncodeError 收口为具名异常）；懒导入避免
    services 内环（engine_outbox 顶层 import 本模块）。返回 ``None`` =
    可编码；定义不可解析等非本闸类别由调用方既有分支处理。
    """
    try:
        from .engine_outbox import (
            DefinitionUnencodableError,
            ensure_definition_encodable,
        )
    except ImportError:  # 平铺布局（cwd=backend）运行时
        from engine_outbox import (  # type: ignore
            DefinitionUnencodableError,
            ensure_definition_encodable,
        )
    try:
        ensure_definition_encodable(flow_id, version, defn)
    except DefinitionUnencodableError as e:
        return str(e)
    return None


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
    # 卡 A-1（A129-01）save 面同闸：孤立代理转义定义经 save_flow 默认
    # ensure_ascii=True dumps 不炸、落 Redis 成功，Python worker 读回
    # canonical hash 即炸（毒丸）——与发布闸同一判据，拒写、零键落 Redis。
    reason = _unencodable_reason(flow_id, version, defn)
    if reason is not None:
        logger.error("同步流程 %s@%s 拒写引擎存储（租户 %s）：%s",
                     flow_id, version, tenant_id, reason)
        return False
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


def _env_positive(kind, name: str, default):
    """环境变量 → 正数；非法值 WARNING + 默认（降级必须可观测）。"""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = kind(raw)
    except (TypeError, ValueError):
        value = None
    if value is None or value <= 0:
        logger.warning(
            "%s=%s 非法（需正数），使用默认 %r", name, _redact_env_value(raw), default
        )
        return default
    return value


def _resolve_sync_budget(
    batch_size: Optional[int],
    deadline_seconds: Optional[float],
    max_versions: Optional[int],
) -> Tuple[int, float, int]:
    """显式参数 > env > 默认。"""
    batch = (
        int(batch_size)
        if batch_size is not None
        else _env_positive(int, _BATCH_ENV, DEFAULT_SYNC_BATCH_SIZE)
    )
    deadline = (
        float(deadline_seconds)
        if deadline_seconds is not None
        else _env_positive(float, _DEADLINE_ENV, DEFAULT_SYNC_DEADLINE_SECONDS)
    )
    max_versions = (
        int(max_versions)
        if max_versions is not None
        else _env_positive(int, _MAX_VERSIONS_ENV, DEFAULT_SYNC_MAX_VERSIONS)
    )
    return max(1, batch), max(0.001, deadline), max(1, max_versions)


def _iter_published_entries(store, tenant_id: Optional[str], stats: Dict[str, Any]
                            ) -> Iterator[Tuple[Optional[str], str, str, str]]:
    """逐个产出 ``(tenant_id, flow_id, version, definition)``。

    优先用 FlowStore 的**分页批量**查询（``iter_published_versions``）：把
    「2 次 SQL/flow」收敛到「1 次 SQL/页」，使启动回填不再随 flow 数线性增长。
    鸭子类型 store（测试替身/旧实现）没有该方法时退回逐 flow 列举。
    """
    bulk = getattr(store, "iter_published_versions", None)
    if callable(bulk):
        yield from bulk(tenant_id)
        return

    flows = store.list_flows(tenant_id)
    for flow in flows:
        fid = getattr(flow, "flow_id", None)
        ftenant = getattr(flow, "tenant_id", tenant_id) or tenant_id
        if not fid:
            continue
        try:
            versions = store.list_versions(fid, tenant_id=ftenant)
        except Exception as e:  # noqa: BLE001 — 单 flow 读失败不拖垮其余 flow
            logger.warning("回填引擎存储：列举 %s 版本失败: %s", fid, e)
            stats["errors"] += 1
            continue
        for v in versions:
            if getattr(v, "status", "") != "published":
                continue
            yield ftenant, fid, v.version, getattr(v, "definition", "") or ""


def _flush_batch(redis: Redis, batch: list, stats: Dict[str, Any]) -> Optional[str]:
    """一个 pipeline 写一批：SET + SADD(flow_list) + SADD(flow_versions)。

    返回 ``None`` 表示继续；返回具名原因表示**停止**（连接级失败/租户闸拒绝）。
    普通数据错误（如某条命令 WRONGTYPE）计错后继续后续批次。
    """
    if not batch:
        return None
    try:
        # 与 RedisFlowStorage.save_flow 同源的写入门禁（#239 phase 2）：
        # 批量化不得绕过「租户来源 default 时 fail-closed」。
        guard_tenant_write("redis.save_flow")
    except Exception as e:  # noqa: BLE001 — 具名拒绝已在 guard 内，计数即可
        stats["errors"] += len(batch)
        logger.warning("回填引擎存储：租户写入门禁拒绝（%d 版本）: %s", len(batch), e)
        return "tenant_guard"

    pipe = redis.pipeline(transaction=False)
    for key, fid, version, payload, flow_list_key, versions_key in batch:
        pipe.set(key, payload)
        pipe.sadd(flow_list_key, fid)
        pipe.sadd(versions_key, version)
    try:
        pipe.execute()
    except (RedisConnectionError, RedisTimeoutError, OSError) as e:
        stats["errors"] += len(batch)
        logger.warning(
            "回填引擎存储：批次写入失败（%d 版本，连接/超时）: %s", len(batch), e
        )
        return "redis_unavailable"
    except Exception as e:  # noqa: BLE001 — 数据级错误：计错但继续后续批次
        stats["errors"] += len(batch)
        logger.warning("回填引擎存储：批次写入失败（%d 版本）: %s", len(batch), e)
        return None
    stats["written"] += len(batch)
    stats["batches"] += 1
    return None


def sync_all_published_to_engine(
    redis: Redis,
    store,
    tenant_id: Optional[str] = None,
    *,
    batch_size: Optional[int] = None,
    deadline_seconds: Optional[float] = None,
    max_versions: Optional[int] = None,
) -> Dict[str, Any]:
    """把 store 里全部 status=published 的版本回填/对齐进引擎 Redis 存储。

    幂等：写入是覆盖写，重复调用只会重写同样内容，不产生脏数据。
    用于启动时的漂移兜底（历史版本从未重跑发布）以及运维批量对齐。

    入参 ``store`` 为 console FlowStore（鸭子类型：只需 ``list_flows`` /
    ``list_versions``，避免与 flow_store 循环 import）。``tenant_id=None``
    表示跨全部租户（``list_flows(None)`` 返回平台全量）。

    **A2R20-06 预算**（启动路径不得无界）：
    - 每 ``batch_size``（默认 200）个版本一个 pipeline：3 次往返/版本 →
      ~1 次/批；
    - 墙钟 ``deadline_seconds``（默认 5s）与版本数 ``max_versions``（默认
      50k）任一触界即停，已读到的部分批次仍写出，剩余标记 ``truncated``；
    - 触界/失败都留 WARNING，返回值供 ``/health`` 暴露 ``ready/degraded``。

    **仅写本进程所连的 Redis**——跨机的 worker 需在目标机器另跑回填脚本
    （见模块 docstring 的「边界」段）。返回写入/跳过/错误/预算统计。

    本函数绝不抛异常：任何失败都计入 errors，供启动钩子安全调用。
    """
    batch_size, deadline_seconds, max_versions = _resolve_sync_budget(
        batch_size, deadline_seconds, max_versions
    )
    stats: Dict[str, Any] = {
        "written": 0,
        "skipped": 0,
        "errors": 0,
        "versions": 0,
        "batches": 0,
        "truncated": False,
        "truncated_reason": None,
        "batch_size": batch_size,
        "deadline_seconds": deadline_seconds,
        "max_versions": max_versions,
        "elapsed_ms": 0,
    }
    started = time.monotonic()
    deadline_at = started + deadline_seconds
    batch: list = []
    stop_reason: Optional[str] = None

    try:
        entries = _iter_published_entries(store, tenant_id, stats)
        for ftenant, fid, version, definition in entries:
            if not definition.strip():
                stats["skipped"] += 1
                continue
            if stats["versions"] >= max_versions:
                stop_reason = "max_versions"
                break
            if time.monotonic() >= deadline_at:
                stop_reason = "deadline"
                break

            try:
                defn = json.loads(definition) if isinstance(definition, str) else dict(definition)
            except (TypeError, ValueError) as e:
                logger.warning("回填引擎存储：%s@%s 定义不是合法 JSON: %s", fid, version, e)
                stats["errors"] += 1
                continue
            defn["flow_id"] = fid
            defn["version"] = version
            try:
                payload = json.dumps(defn)
            except (TypeError, ValueError) as e:
                logger.warning("回填引擎存储：%s@%s 定义不可序列化: %s", fid, version, e)
                stats["errors"] += 1
                continue
            # 卡 A-1：回填与 sync_flow_to_engine 同闸——存量/DB 级写入的
            # 孤立代理定义不再在每次启动时被重新写成毒丸。
            reason = _unencodable_reason(fid, version, defn)
            if reason is not None:
                logger.warning("回填引擎存储：%s@%s 拒写：%s", fid, version, reason)
                stats["errors"] += 1
                continue

            ns = tenant_namespace(ftenant)
            batch.append((
                f"{ns}:flow:{fid}:{version}",
                fid,
                version,
                payload,
                f"{ns}:flow_list",
                f"{ns}:flow_versions:{fid}",
            ))
            stats["versions"] += 1
            if len(batch) >= batch_size:
                reason = _flush_batch(redis, batch, stats)
                batch = []
                if reason is not None:
                    stop_reason = reason
                    break
    except Exception as e:  # noqa: BLE001 — 读库/分页查询失败：不阻断启动
        stats["errors"] += 1
        stop_reason = "store_error"
        logger.warning("回填引擎存储：列举已发布版本失败（租户 %s）: %s",
                       tenant_id, e, exc_info=True)

    # 预算/读库触界时，已读到的部分批次仍写出（不丢已完成的工作）；
    # 写入本身失败（连接/租户闸）时不重试。
    if batch and stop_reason not in ("redis_unavailable", "tenant_guard"):
        reason = _flush_batch(redis, batch, stats)
        if reason is not None:
            stop_reason = reason

    stats["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    if stop_reason is not None:
        stats["truncated"] = True
        stats["truncated_reason"] = stop_reason
        logger.warning(
            "引擎存储回填未完成（%s）：written=%s skipped=%s errors=%s "
            "versions=%s batches=%s elapsed_ms=%s（启动继续，health=degraded）",
            stop_reason, stats["written"], stats["skipped"], stats["errors"],
            stats["versions"], stats["batches"], stats["elapsed_ms"],
        )
    else:
        logger.info(
            "引擎存储回填完成：written=%s skipped=%s errors=%s versions=%s "
            "batches=%s elapsed_ms=%s",
            stats["written"], stats["skipped"], stats["errors"],
            stats["versions"], stats["batches"], stats["elapsed_ms"],
        )
    return stats
