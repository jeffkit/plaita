"""派发台账（#42 全局调度层 / W23-a 期 1）。

## 为什么需要这一层

期 0 交付了「派发侧能**算**出该给谁」（`capacity.decide_route`：盘多 → 在跑少
→ id 字典序）与影子计划；期 1 把它接进**真实派发**。真实派发一旦发生，就必须
回答事后可查的两个问题：**谁派给了谁**、**为何派给它**——否则「按容量路由」
只是一句无法审计的宣称（出了问题只能翻 console 日志）。

本模块提供那段可审计的事实：

1. **台账条目 schema**（`build_ledger_entry`）：一条 JSON 记录，含
   `decided_at` / `decided_by`（派发方）/ `issue_id` / `queue` /
   `target_instance_id`（派给谁）/ `decision`（targeted|broadcast）/
   `reason`（为何，**稳定前缀**）/ 候选与拒绝明细 / `message_id`；
2. **落盘**（`record_dispatch`）：写进 `plaita:dispatch:ledger`（Redis Stream，
   与任务队列同款「一字段 JSON」信封）——**只增不改**，可 XRANGE 回放；
3. **入队 + 记台账的唯一入口**（`enqueue_with_ledger`）：把路由事实
   （`target_instance_id`）**同时**写进消息体与台账条目。两者由同一份输入
   派生 ⇒ 台账**不可能**与真实消息不一致（台账不说谎）；
4. **对账**（`ledger_queue_parity`）：`XLEN(台账) == XLEN(队列)` —— 期 1 的
   可断言判据（台账条数 == 入队条数），也是运维随时可跑的自检。

## 红线 / 边界

- **不新增基础设施**：复用既有 Redis（任务队列同一个客户端），只加一个
  Stream 键；台账写失败**不阻塞**已成功的派发（记 ERROR + 在返回值里透出
  `ledger_recorded=False`，绝不因记账故障把已入队的单吞掉）。
- **影子/干跑不写台账**：只有**真的入队**才有一条台账（否则「台账条数 ==
  入队条数」会被影子计划污染）。
- 读侧（`read_ledger` / `ledger_length`）对 Redis 故障返回空/`None`，绝不抛出。
"""
from __future__ import annotations

import json
import logging
import os
import socket
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from plaita.core.url_redaction import _scrub_exception_text

logger = logging.getLogger("plaita.server.dispatch_ledger")

#: 台账 Stream 键（env 可覆盖，便于金丝雀/测试隔离）。
LEDGER_KEY_ENV = "PLAITA_DISPATCH_LEDGER_KEY"
DEFAULT_LEDGER_KEY = "plaita:dispatch:ledger"

#: 台账条目 schema 版本（语义版本：字段含义变化才 bump）。
LEDGER_SCHEMA_VERSION = 1

#: 派发方标识（「谁派给谁」的前半截）：默认主机名，env 可覆盖。
DISPATCHER_ID_ENV = "PLAITA_DISPATCHER_ID"

#: 信封字段：与 ``task_queue`` 同款（一字段 JSON + 版本），便于同一套工具读。
LEDGER_PAYLOAD_FIELD = "payload"
LEDGER_SCHEMA_FIELD = "schema_version"

#: 决策类型（稳定字符串）。
DECISION_TARGETED = "targeted"    # 有容量路由目标：显式派发给该 worker
DECISION_BROADCAST = "broadcast"  # 无可用目标：放行（广播抢单，存量行为）


def ledger_key_from_env() -> str:
    """台账键（``PLAITA_DISPATCH_LEDGER_KEY``，空白回默认）。"""
    return (os.environ.get(LEDGER_KEY_ENV) or "").strip() or DEFAULT_LEDGER_KEY


def dispatcher_id_from_env() -> str:
    """派发方标识（``PLAITA_DISPATCHER_ID``，缺省主机名）。"""
    raw = (os.environ.get(DISPATCHER_ID_ENV) or "").strip()
    if raw:
        return raw
    try:
        return socket.gethostname() or "unknown-dispatcher"
    except Exception as exc:  # noqa: BLE001 — 取不到主机名不影响派发
        logger.debug("取主机名失败，回退 unknown-dispatcher: %s",
                     _scrub_exception_text(exc))
        return "unknown-dispatcher"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_rows(raw: Any) -> List[Dict[str, Any]]:
    """规范化候选/拒绝明细（``[{instance_id, reason}]``，非法输入 → 空列表）。"""
    if not isinstance(raw, (list, tuple)):
        return []
    rows: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, str):
            rows.append({"instance_id": item, "reason": None})
        elif isinstance(item, dict):
            rows.append({
                "instance_id": str(item.get("instance_id") or ""),
                "reason": item.get("reason"),
                "host": item.get("host"),
            })
    return rows


def build_ledger_entry(
    *,
    issue_id: str,
    queue: str,
    target_instance_id: Optional[str] = None,
    reason: str = "",
    decision: Optional[str] = None,
    allowed: Any = None,
    rejected: Any = None,
    stale: Any = None,
    candidate_count: Optional[int] = None,
    tenant_id: Optional[str] = None,
    decided_at: Optional[str] = None,
    decided_by: Optional[str] = None,
    message_id: Optional[str] = None,
    enqueued: bool = False,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """构造一条台账条目（纯函数，可直接断言）。

    ``decision`` 缺省按 ``target_instance_id`` 推导：有目标 = ``targeted``，
    无目标 = ``broadcast``（放行）。**不**在缺目标时臆造目标——「没派给谁」是
    真实且必须留痕的事实（红线：容量缺失/全被拒 ⇒ 放行）。
    """
    target = target_instance_id if isinstance(target_instance_id, str) and target_instance_id.strip() else None
    if decision is None:
        decision = DECISION_TARGETED if target else DECISION_BROADCAST
    allowed_rows = _as_rows(allowed)
    rejected_rows = _as_rows(rejected)
    entry: Dict[str, Any] = {
        "ledger_schema_version": LEDGER_SCHEMA_VERSION,
        "decided_at": decided_at or _now_iso(),
        "decided_by": decided_by if decided_by is not None else dispatcher_id_from_env(),
        "issue_id": str(issue_id),
        "queue": str(queue),
        "decision": str(decision),
        "reason": str(reason or ""),
        "target_instance_id": target,
        "enqueued": bool(enqueued),
        "message_id": message_id,
        "candidate_count": (
            len(allowed_rows) + len(rejected_rows)
            if candidate_count is None else int(candidate_count)
        ),
        "allowed": allowed_rows,
        "rejected": rejected_rows,
        "stale": [str(x) for x in stale] if isinstance(stale, (list, tuple)) else [],
        "tenant_id": tenant_id,
    }
    if extra:
        for key, value in extra.items():
            entry.setdefault(key, value)
    return entry


def record_dispatch(
    redis_client: Any,
    entry: Dict[str, Any],
    *,
    ledger_key: Optional[str] = None,
) -> Optional[str]:
    """把台账条目写进 Stream；返回 msg id，失败返回 ``None``（**绝不抛**）。

    写失败只记 ERROR：调用方（``enqueue_with_ledger``）据此在返回值里透出
    ``ledger_recorded=False``——记账故障不得回滚一次已成功的派发。
    """
    if redis_client is None:
        logger.error("无 Redis 客户端，派发台账无法落盘（本条决策只有日志）")
        return None
    key = ledger_key or ledger_key_from_env()
    try:
        msg_id = redis_client.xadd(
            key,
            {
                LEDGER_PAYLOAD_FIELD: json.dumps(entry, ensure_ascii=False, sort_keys=True),
                LEDGER_SCHEMA_FIELD: str(LEDGER_SCHEMA_VERSION),
            },
        )
    except Exception as exc:  # noqa: BLE001 — 记账失败不阻塞派发
        logger.error("派发台账写入失败（本次派发已入队但无台账）: %s", _scrub_exception_text(exc))
        return None
    if isinstance(msg_id, bytes):
        msg_id = msg_id.decode()
    return str(msg_id)


def read_ledger(
    redis_client: Any,
    *,
    count: Optional[int] = None,
    ledger_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """读台账条目（旧 → 新）。Redis 故障/坏条目 → 跳过（绝不抛）。

    「坏条目」= payload 缺失 / 不可解析 / 不是 JSON 对象。它们**不**让整个
    台账读不出来：跳过并在返回值里不留痕（``ledger_rows_with_errors`` 提供
    带错误计数的版本）。
    """
    rows, _ = ledger_rows_with_errors(redis_client, count=count, ledger_key=ledger_key)
    return rows


def ledger_rows_with_errors(
    redis_client: Any,
    *,
    count: Optional[int] = None,
    ledger_key: Optional[str] = None,
) -> Any:
    """``(可解析条目, 坏条目数)``——坏条目不静默吞掉（可观测）。"""
    if redis_client is None:
        return [], 0
    key = ledger_key or ledger_key_from_env()
    try:
        if count is None:
            raw_entries = redis_client.xrange(key)
        else:
            # XREVRANGE 取最近 N 条后再正序，保证「最近 N 条」语义
            raw_entries = list(reversed(redis_client.xrevrange(key, count=int(count))))
    except Exception as exc:  # noqa: BLE001 — 读侧故障按「读不到」处理
        logger.warning("派发台账读取失败（按空台账处理）: %s", _scrub_exception_text(exc))
        return [], 0
    rows: List[Dict[str, Any]] = []
    bad = 0
    for msg_id, fields in raw_entries or []:
        try:
            data = _fields_to_entry(fields)
        except Exception as exc:  # noqa: BLE001 — 坏条目不阻塞整本台账
            logger.debug("台账条目解析失败（计坏条目并跳过）: %s",
                         _scrub_exception_text(exc))
            bad += 1
            continue
        if data is None:
            bad += 1
            continue
        data.setdefault("ledger_id", _decode(msg_id))
        rows.append(data)
    return rows, bad


def _fields_to_entry(fields: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(fields, dict):
        return None
    raw = None
    for key in (LEDGER_PAYLOAD_FIELD, LEDGER_PAYLOAD_FIELD.encode()):
        if key in fields:
            raw = fields[key]
            break
    if raw is None:
        return None
    raw = _decode(raw)
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _decode(value: Any) -> Any:
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def ledger_length(redis_client: Any, *, ledger_key: Optional[str] = None) -> Optional[int]:
    """台账条数（``XLEN``）；Redis 故障返回 ``None``（**不**伪装成 0）。"""
    if redis_client is None:
        return None
    try:
        return int(redis_client.xlen(ledger_key or ledger_key_from_env()))
    except Exception as exc:  # noqa: BLE001 — 读侧故障如实返回 None
        logger.warning("派发台账 XLEN 读取失败: %s", _scrub_exception_text(exc))
        return None


def queue_length(redis_client: Any, queue_name: str) -> Optional[int]:
    """队列条数（``XLEN``）；Redis 故障返回 ``None``。"""
    if redis_client is None:
        return None
    try:
        return int(redis_client.xlen(queue_name))
    except Exception as exc:  # noqa: BLE001
        logger.warning("队列 %s XLEN 读取失败: %s", queue_name, _scrub_exception_text(exc))
        return None


def ledger_queue_parity(
    redis_client: Any,
    queue_name: str,
    *,
    ledger_key: Optional[str] = None,
) -> Dict[str, Any]:
    """期 1 判据的自检：``台账条数 == 入队条数``。

    任一读失败 → ``parity=False`` + ``error``（**不**把「读不到」当成「相等」：
    那会让判据在 Redis 故障时静默变绿）。
    """
    ledger_n = ledger_length(redis_client, ledger_key=ledger_key)
    queue_n = queue_length(redis_client, queue_name)
    if ledger_n is None or queue_n is None:
        return {
            "ledger": ledger_n,
            "queue": queue_n,
            "queue_name": queue_name,
            "expected": None,
            "parity": False,
            "error": "台账或队列条数读取失败（无法证明相等）",
        }
    return {
        "ledger": ledger_n,
        "queue": queue_n,
        "queue_name": queue_name,
        "expected": queue_n,
        "parity": ledger_n == queue_n,
        "error": None,
    }


def enqueue_with_ledger(
    redis_client: Any,
    *,
    stream_key: str,
    task: Dict[str, Any],
    entry: Dict[str, Any],
    ledger_key: Optional[str] = None,
) -> Dict[str, Any]:
    """**入队 + 记台账的唯一入口**（期 1 判据只有一份实现）。

    ``entry['target_instance_id']`` 是路由事实的**唯一来源**：本函数把它写进
    ``task[TARGET_INSTANCE_FIELD]``（``None`` ⇒ 从消息体移除该键 = 广播放行）。
    台账与消息体因此**不可能**分叉——「台账写着派给 A、消息却没有 A」这类
    假台账在结构上被排除。

    返回 ``{"enqueued", "message_id", "ledger_id", "ledger_recorded", "error"}``；
    **绝不抛**（消费方／HTTP 层据此返回可判别结果）。
    """
    from .capacity import TARGET_INSTANCE_FIELD, normalize_target_instance_id
    from .task_queue import enqueue_task

    out: Dict[str, Any] = {
        "enqueued": False,
        "message_id": None,
        "ledger_id": None,
        "ledger_recorded": False,
        "error": None,
    }
    if redis_client is None:
        out["error"] = "no-redis-client"
        logger.error("无 Redis 客户端：派发 %s 未入队", entry.get("issue_id"))
        return out

    body = dict(task) if isinstance(task, dict) else {}
    target = normalize_target_instance_id(entry.get("target_instance_id"))
    if target is None:
        body.pop(TARGET_INSTANCE_FIELD, None)
    else:
        body[TARGET_INSTANCE_FIELD] = target

    recorded = dict(entry)
    recorded["target_instance_id"] = target
    recorded["queue"] = stream_key
    recorded["enqueued"] = True

    try:
        message_id = enqueue_task(redis_client, stream_key, body)
    except Exception as exc:  # noqa: BLE001 — 入队失败=没派发（调用方决定重试）
        logger.error("任务入队失败（不记台账，避免假台账）: %s", _scrub_exception_text(exc))
        out["error"] = f"enqueue-failed: {type(exc).__name__}: {exc}"
        return out
    out["enqueued"] = True
    out["message_id"] = message_id
    recorded["message_id"] = message_id

    ledger_id = record_dispatch(redis_client, recorded, ledger_key=ledger_key)
    out["ledger_id"] = ledger_id
    out["ledger_recorded"] = ledger_id is not None
    if ledger_id is None:
        # 如实透出：本次派发**没有**台账（台账条数将 < 入队条数）。不抛、不回滚
        # ——记账故障不得让一次已成功的派发凭空消失。
        out["error"] = "ledger-write-failed"
    return out
