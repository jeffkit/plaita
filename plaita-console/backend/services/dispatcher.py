"""console 派发决策面（#42 全局调度层：W10-a 期 0 竖切 + W23-a 期 1）。

## 定位

issue-keeper#16 要把「派发决策上移 console」——谁先跑、跑几个、发给哪台机器。
plaita 侧缺的**不是**队列或抢单（``task_queue`` 抢单 + ``execution_lease``
fencing 已具），而是**派发侧对 worker 容量的可见性**。本模块是那块可见性的
console 侧出口：

- 输入：注册表里 worker 上报的容量快照（``plaita.server.capacity``）；
- 输出①（期 0）：确定性派单计划（目标 worker + 可派/被拒名单 + 拒绝原因）；
- 输出②（期 1）：``dispatch_one`` 把同一结论用于**一次真实派发**——目标写进
  消息体顶层 ``target_instance_id``，并落一条派发台账（``plaita:dispatch:ledger``，
  「谁派给谁 / 为何」），由 worker 侧按既有 ``TaskNotForThisWorker`` 交接让出。

## 何时才真的发单（三道闸，默认全关）

① ``shadow``（``PLAITA_CONSOLE_DISPATCH_SHADOW``，默认真）；② ``dry_run``
（``dispatch_one`` 默认真）；③ 调用方必须显式给出真实队列与 issue。默认姿态下
本模块**不 import 副作用路径、不写任何 Redis 键**——影子期唯一产物是 HTTP
响应/日志（验收 5）。

## 与 intake flow 的关系

真正的 `issue-intake` flow（拉 issue 快照 → 过滤 → 排序 → 并发额度 →
容量路由）落在 issue-keeper 仓（plaita 仓不放流程定义）。本模块把其中
**与 console/plaita 有关的那一段**（容量路由 + 拒绝原因 + 确定性排序 +
显式派发 + 台账）做成可单测、可复用的函数，flow 侧只需调它并按 assignment
节点表达策略。

## 并行验证纪律

影子 → 金丝雀（按仓）→ 全量。影子期**生产队列零新增**（验收 5）；金丝雀期
队列物理隔离（``XLEN`` 互相独立，期 1 判据③）。
"""
from __future__ import annotations

import logging
import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))


def _load_engine_module(name: str):
    """取 ``plaita.server.<name>``：先常规导入，失败则按**文件路径**兜底。

    为什么需要兜底：console 有两种运行姿态——(a) ``python run.py`` 从 checkout
    起，``sys.path`` 里是本仓 ``plaita/``；(b) 裸 ``import main``（部分 console
    单测的姿态）时 ``plaita`` 可能被 site-packages 的 editable 安装抢先解析到
    **另一个 checkout**，那里没有本单新增的 ``capacity.py`` / ``dispatch_ledger.py``。
    派发决策面是「能不能派、派给谁、有没有台账」，不该因为 console 用哪种姿态
    启动就整个打不开——故按路径直载。
    两处都不可用 → 让 ImportError 冒出来（Fail loud：静默降级成「无容量」会让
    派发门悄悄失效，比启动失败更糟）。
    """
    dotted = f"plaita.server.{name}"
    try:
        module = importlib.import_module(dotted)  # noqa: PLC0415
        return module
    except ImportError:
        pass
    here = Path(__file__).resolve()
    for root in here.parents:
        candidate = root / "plaita" / "server" / f"{name}.py"
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location(dotted, str(candidate))
            if spec is not None and spec.loader is not None:
                module = importlib.util.module_from_spec(spec)
                # **先注册再执行**：dataclass / 标准库按 ``__module__`` 反查
                # ``sys.modules``，不注册会在 @dataclass 处炸 AttributeError
                # （实测）。
                sys.modules.setdefault(dotted, module)
                try:
                    spec.loader.exec_module(module)
                except Exception:
                    sys.modules.pop(dotted, None)
                    raise
                return module
    raise ImportError(
        f"找不到 plaita/server/{name}.py（console 需要 plaita 仓的调度基元模块）"
    )


#: 加载顺序有意义：``dispatch_ledger`` 内部按相对名引用 ``.capacity``，
#: 先把容量模块装进 ``sys.modules`` 再做别的。
_capacity = _load_engine_module("capacity")
_ledger = _load_engine_module("dispatch_ledger")

DEFAULT_WORKER_SERVICE_TYPE = _capacity.DEFAULT_WORKER_SERVICE_TYPE
DispatchCandidate = _capacity.DispatchCandidate
TARGET_INSTANCE_FIELD = _capacity.TARGET_INSTANCE_FIELD
capacity_age_seconds = _capacity.capacity_age_seconds
decide_route = _capacity.decide_route
dispatch_queue_from_env = _capacity.dispatch_queue_from_env
filter_dispatchable = _capacity.filter_dispatchable
load_worker_capacity = _capacity.load_worker_capacity
select_dispatch_target = _capacity.select_dispatch_target
shadow_mode_from_env = _capacity.shadow_mode_from_env

DEFAULT_LEDGER_KEY = _ledger.DEFAULT_LEDGER_KEY
build_ledger_entry = _ledger.build_ledger_entry
enqueue_with_ledger = _ledger.enqueue_with_ledger
ledger_key_from_env = _ledger.ledger_key_from_env
ledger_length = _ledger.ledger_length
ledger_queue_parity = _ledger.ledger_queue_parity
read_ledger = _ledger.read_ledger


def worker_capacity_rows(redis_client: Any) -> List[Dict[str, Any]]:
    """容量快照列表（按剩余盘降序、instance_id 升序——看板口径与派发一致）。

    Redis 不可用/注册表为空 → 空列表（caller 据此显示「无 worker 上报」，
    而不是 500：容量面读不到不该让整页打不开）。
    """
    candidates = load_worker_capacity(redis_client)
    rows: List[Dict[str, Any]] = []
    for cand in sorted(candidates, key=lambda c: c.sort_key()):
        block = cand.capacity or {}
        rows.append({
            "instance_id": cand.instance_id,
            "host": cand.host,
            "status": cand.status,
            "active_tasks": cand.active_tasks,
            "capacity": block or None,
            "capacity_stale": cand.capacity_stale,
            "capacity_age_seconds": capacity_age_seconds(block),
            "dispatchable": cand.dispatchable,
            "reject_reason": cand.reject_reason(),
        })
    return rows


def build_dispatch_plan(
    redis_client: Any,
    issue_ids: Optional[List[str]] = None,
    *,
    queue_name: Optional[str] = None,
    service_type: str = DEFAULT_WORKER_SERVICE_TYPE,
    shadow: Optional[bool] = None,
) -> Dict[str, Any]:
    """确定性派单计划（**只算不发**）。

    ``shadow`` 缺省读 env（默认 ``True``）。即便显式关闭影子，本函数也只
    返回计划——真正的入队是**另一个显式步骤**（``dispatch_one``），避免
    「看计划」与「发单」被同一个调用顺带做掉。
    """
    candidates = load_worker_capacity(redis_client, service_type=service_type)
    route = decide_route(candidates)
    shadow_on = shadow_mode_from_env() if shadow is None else bool(shadow)
    queue = queue_name or dispatch_queue_from_env()
    return {
        "shadow": shadow_on,
        "shadow_mode": shadow_on,
        "queue": queue,
        "issue_ids": list(issue_ids or []),
        "target": route.target_instance_id,
        "reason": route.reason,
        "allowed": [c.instance_id for c in route.allowed],
        "rejected": [
            {"instance_id": r.instance_id, "host": r.host, "reason": r.reason}
            for r in route.rejected
        ],
        "stale": [c.instance_id for c in candidates if c.capacity_stale],
        "candidate_count": len(candidates),
        "enqueued": 0,
    }


def dispatch_target_for(
    redis_client: Any,
    *,
    service_type: str = DEFAULT_WORKER_SERVICE_TYPE,
) -> Optional[DispatchCandidate]:
    """单点查询：本轮该由哪台 worker 接单（``None`` = 本轮不派）。"""
    return select_dispatch_target(load_worker_capacity(redis_client, service_type=service_type))


# --------------------------------------------------------------- 期 1：真实派发


def dispatch_one(
    redis_client: Any,
    issue_id: str,
    *,
    flow_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    queue_name: Optional[str] = None,
    tenant_id: Optional[str] = None,
    actor: Optional[str] = None,
    dry_run: bool = True,
    shadow: Optional[bool] = None,
    service_type: str = DEFAULT_WORKER_SERVICE_TYPE,
    ledger_key: Optional[str] = None,
) -> Dict[str, Any]:
    """单次可控派发（#42 期 1）：**容量路由 → 显式目标 → 入队 + 落台账**。

    三道闸，缺一不发单（默认姿态 = 什么都不发生）：

    1. ``shadow``（缺省读 ``PLAITA_CONSOLE_DISPATCH_SHADOW``，**默认 True**）
       —— 影子期只算不发（回归验收 5：生产队列零新增、零台账）；
    2. ``dry_run``（**默认 True**）—— 调用方必须显式传 ``dry_run=False``；
    3. 可用目标由 ``capacity.decide_route`` 给出（盘多 → 在跑少 → id 字典序）。

    路由结果写进消息体顶层 ``target_instance_id``；worker 侧非目标即按既有
    ``TaskNotForThisWorker`` 交接让出（不新增协议）。**无可用目标时照派**，
    只是不带该字段 = 广播抢单（红线：容量缺失/非法/全被拒一律放行，不因上报
    故障停摆）。

    ``queue_name`` 缺省跟随 ``PLAITA_CONSOLE_TASK_QUEUE``；金丝雀期由调用方
    显式传金丝雀队列（如 ``plaita:flow:queue:canary``）——生产队列 XLEN 与
    金丝雀队列 XLEN 因此互相独立（期 1 判据③）。

    ``tenant_id`` / ``dry_run`` / ``shadow`` 是服务端权威输入（#262）：HTTP
    层从认证上下文与 env 解析后传入，不接受请求体直接覆盖。``actor`` 是
    认证主体的用户名/凭证标识，原样写进台账（「谁派的」）。
    """
    candidates = load_worker_capacity(redis_client, service_type=service_type)
    route = decide_route(candidates)
    shadow_on = shadow_mode_from_env() if shadow is None else bool(shadow)
    queue = queue_name or dispatch_queue_from_env()
    exec_id = execution_id or issue_id

    result: Dict[str, Any] = {
        "shadow": shadow_on,
        "dry_run": bool(dry_run),
        "issue_id": str(issue_id),
        "execution_id": exec_id,
        "queue": queue,
        "target": route.target_instance_id,
        "reason": route.reason,
        "decision": "targeted" if route.target_instance_id else "broadcast",
        "allowed": [c.instance_id for c in route.allowed],
        "rejected": [
            {"instance_id": r.instance_id, "host": r.host, "reason": r.reason}
            for r in route.rejected
        ],
        "stale": [c.instance_id for c in candidates if c.capacity_stale],
        "candidate_count": len(candidates),
        "enqueued": False,
        "message_id": None,
        "ledger_id": None,
        "ledger_recorded": False,
        "ledger_key": ledger_key or ledger_key_from_env(),
        "error": None,
    }

    if shadow_on or dry_run:
        # 只算不发：**不**入队、**不**写台账（连键都不建）——影子/干跑期唯一
        # 产物是这份返回值/日志。
        result["decision"] = "shadow" if shadow_on else "dry_run"
        return result

    body: Dict[str, Any] = {
        "type": "start",
        "flow_id": flow_id or issue_id,
        "params": dict(params or {}),
        "execution_id": exec_id,
        "timestamp": datetime.now().isoformat(),
    }
    if tenant_id:
        body["tenant_id"] = tenant_id

    entry = build_ledger_entry(
        issue_id=issue_id,
        queue=queue,
        target_instance_id=route.target_instance_id,
        reason=route.reason,
        allowed=result["allowed"],
        rejected=route.rejected,
        stale=result["stale"],
        candidate_count=len(candidates),
        tenant_id=tenant_id,
        actor=actor,
        extra={"execution_id": exec_id},
    )
    out = enqueue_with_ledger(
        redis_client, stream_key=queue, task=body, entry=entry, ledger_key=ledger_key,
    )
    result.update(out)
    result["ledger_key"] = ledger_key or ledger_key_from_env()
    return result


def dispatch_many(
    redis_client: Any,
    issue_ids: List[str],
    **kwargs: Any,
) -> Dict[str, Any]:
    """批量派发（金丝雀一轮）：逐条走 ``dispatch_one``，聚合实际入队/台账计数。

    逐条而非一次入队，是为了**每条一个决策**——容量路由的目标可能随在跑数
    变化（``active_tasks`` 是快照的），且台账「一条 == 一次入队」的判据要求
    决策与入队一一对应。
    """
    results = [dispatch_one(redis_client, issue_id, **kwargs) for issue_id in (issue_ids or [])]
    return {
        "queue": results[0]["queue"] if results else (
            kwargs.get("queue_name") or dispatch_queue_from_env()
        ),
        "requested": len(issue_ids or []),
        "enqueued_count": sum(1 for r in results if r.get("enqueued")),
        "ledger_count": sum(1 for r in results if r.get("ledger_recorded")),
        "targets": sorted({r["target"] for r in results if r.get("target")}),
        "dispatches": results,
    }


def ledger_rows(redis_client: Any, *, count: Optional[int] = None) -> List[Dict[str, Any]]:
    """派发台账条目（旧 → 新）；Redis 故障/坏条目 → 跳过（不抛）。"""
    return read_ledger(redis_client, count=count)


def dispatch_parity(redis_client: Any, queue_name: Optional[str] = None) -> Dict[str, Any]:
    """期 1 判据的自检：``台账条数 == 入队条数``（可选按队列名对照）。"""
    return ledger_queue_parity(redis_client, queue_name or dispatch_queue_from_env())
