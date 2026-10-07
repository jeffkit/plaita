"""本地单机模式的**启动对账**（无损升级在无队列模式下的对应机制）。

本地模式没有 Redis 任务队列：执行由**进程内线程**推进，状态存 SQLite。因此
console 进程一旦重启，重启前处于 ``running`` 的执行会失去唯一的执行线程——
它既不会被队列重投（没有队列），也不会被租约/看门狗回收（没有租约），
于是**永远停在 running**（僵尸执行）。集群模式的等价物是「消息留在 pending
等 XCLAIM 重投」，本地模式必须自己补上。

对账策略（env ``PLAITA_CONSOLE_RECONCILE_ORPHANS``）：

- ``suspend``（默认）：置为 ``suspended`` 并写入原因。执行在**每个步界都落过
  checkpoint**，所以它是可恢复的——由人决定何时恢复（不自动续跑：本地模式
  没有 at-least-once 的语义保障，自动续跑等于无声重放那一步的副作用）。
- ``fail``：置为 ``failed`` 并结束时间——适合「宁可显式失败也不要留可恢复态」
  的运维口径。
- ``off``：不对账（仅排查期使用）。

只处理 ``running``：``suspended`` 本来就等人恢复，其余是终态。对账是**幂等**的，
且用条件更新（``running → 目标态``）避免与并发状态推进打架。
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_MODE = "suspend"
VALID_MODES = ("suspend", "fail", "off")

# 对账写回的错误体：前端执行详情页会直接展示 message，并据此提示可恢复
REASON = "interrupted_by_restart"


def _mode_from_env() -> str:
    raw = (os.environ.get("PLAITA_CONSOLE_RECONCILE_ORPHANS") or "").strip().lower()
    if not raw:
        return DEFAULT_MODE
    if raw not in VALID_MODES:
        logger.warning(
            "PLAITA_CONSOLE_RECONCILE_ORPHANS=%r 非法（可选 %s），回退 %s",
            raw,
            "/".join(VALID_MODES),
            DEFAULT_MODE,
        )
        return DEFAULT_MODE
    return raw


def _error_payload(mode: str) -> str:
    if mode == "suspend":
        message = (
            "console 进程重启，本地模式执行失去执行线程（本地模式无队列重投），"
            "已置为挂起——检查点完整，可从断点恢复"
        )
        hint = "确认可接受副作用重放后，用「恢复」从步界检查点继续；或直接重跑该流程"
    else:
        message = "console 进程重启，本地模式执行失去执行线程（本地模式无队列重投），已判定为失败"
        hint = "修正原因后重跑该流程"
    return json.dumps(
        {
            "message": message,
            "reason": REASON,
            "mode": mode,
            "reconciled_at": datetime.utcnow().isoformat(),
            "hint": hint,
        },
        ensure_ascii=False,
    )


def reconcile_orphan_local_executions(
    mode: Optional[str] = None,
    store: Any = None,
) -> Dict[str, Any]:
    """对账本地模式里失去执行线程的 ``running`` 执行。返回统计摘要。

    设计为「尽力而为、不阻断启动」：单条失败只记日志，继续处理其余记录。
    """
    try:
        from . import flow_store as fs
    except ImportError:  # 平铺布局（cwd=backend）运行时
        import flow_store as fs  # type: ignore

    resolved_mode = (mode or _mode_from_env()).lower()
    if resolved_mode not in VALID_MODES:
        logger.warning("对账模式 %r 非法，回退 %s", resolved_mode, DEFAULT_MODE)
        resolved_mode = DEFAULT_MODE

    summary: Dict[str, Any] = {
        "mode": resolved_mode,
        "scanned": 0,
        "reconciled": 0,
        "skipped": 0,
        "failed": 0,
    }
    if resolved_mode == "off":
        return summary

    store = store or fs.get_flow_store()
    target_status = "suspended" if resolved_mode == "suspend" else "failed"
    try:
        rows = fs.list_local_executions()
    except Exception as exc:  # noqa: BLE001 — 对账失败不得阻断启动
        logger.warning("本地执行对账：读取执行列表失败，跳过本轮对账: %s", exc)
        return summary

    payload = _error_payload(resolved_mode)
    for row in rows:
        if row.get("status") != "running":
            continue
        summary["scanned"] += 1
        execution_id = row.get("execution_id")
        if not execution_id:
            continue
        try:
            # 条件推进：只有仍是 running 才改（并发完成/取消的被执行让过）
            moved = fs.update_local_execution_status_if(execution_id, "running", target_status)
            if not moved:
                summary["skipped"] += 1
                continue
            fields: Dict[str, Any] = {"error_json": payload}
            if target_status == "failed":
                fields["end_time"] = datetime.utcnow()
            fs.update_local_execution(execution_id, **fields)
            summary["reconciled"] += 1
        except Exception as exc:  # noqa: BLE001 — 单条失败不影响其余记录
            summary["failed"] += 1
            logger.warning("本地执行对账失败 execution_id=%s: %s", execution_id, exc)

    if summary["reconciled"] > 0:
        logger.warning(
            "本地执行对账：%d 条僵尸 running 执行已置为 %s（console 重启导致执行线程丢失；"
            "本地模式无队列重投）。如需关闭对账：PLAITA_CONSOLE_RECONCILE_ORPHANS=off",
            summary["reconciled"],
            target_status,
        )
    return summary
