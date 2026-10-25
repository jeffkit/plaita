"""Error normalization policies for flow execution.

Three intentionally-different policies live here so their contracts are visible
in one place rather than scattered across ``FlowExecution`` methods:

- :func:`finish_normal` — **normal (eager) mode**. ``FlowExecutionException``
  subclasses propagate unchanged but fire ``on_flow_end`` with their own
  ``code``/``message`` here (C1-4: no raise site ever fired it); any other
  exception is wrapped into ``FlowErrorException`` / ``-500`` and
  ``on_flow_end`` is fired here.
- :func:`raise_distributed_error` — **distributed (eager) mode**. *Every*
  exception, including ``FlowExecutionException`` subclasses, is normalized to
  ``FlowErrorException`` / ``-500``. Distributed callers expect a single flat
  error contract; the subclass detail is internal-only.
- :func:`emit_flow_end_on_close` — **lazy generator ``finally``**. Non-
  ``FlowExecutionException`` exceptions get a ``-500`` ``on_flow_end``;
  ``FlowExecutionException`` gets one carrying its own ``code``/``message``;
  a clean exit emits the historical ``result=None`` ``on_flow_end``.

plaita#330 追加第四条策略：**normal/generator 模式根出口的挂起载体呈现**
（:func:`surface_root_suspension` / :func:`surface_root_suspension_agen`）——
``FlowSuspendedCarrier`` 穿出根（``parent is None``）时身份转换为
``SuspendNotExecutableError``（独立异常类，终态不重试）。子执行（parent 非空）
不做转换：carrier 原样上抛，分布式父执行靠它把挂起送到步边界（NS-M3 传播半边
守恒）。``PLAITA_SUSPEND_GUARD=warn`` 时呈现回滚为裸 carrier + WARNING（旋钮
只管呈现与注册不变量，关不掉运行期闸）。

Do **not** collapse these into one helper — the normal-vs-distributed
difference is a documented external contract (see ``FlowExecution.run_distributed``
docstring).
"""
from __future__ import annotations

import logging

from plaita.core.errors import (
    FlowErrorException,
    FlowExecutionException,
    FlowSuspendedCarrier,
    SuspendNotExecutableError,
    suspend_guard_mode,
)
from plaita.core.url_redaction import _scrub_url_text

logger = logging.getLogger("plaita.core.executor")


def surface_root_suspension(carrier: FlowSuspendedCarrier) -> SuspendNotExecutableError:
    """把穿出 normal/generator 根出口的挂起载体呈现为具名终态失败（plaita#330）。

    结构化错误体素材全部取自载体现场：挂起节点（node_id/node_type/source_line）、
    挂起节点所在 flow（flow_id）、声明面（cause：有声明=environment，空声明=
    过渡期 undeclared）、帧路径（frames 根→叶）与作用域闸判定（fix：分布式可救
    → run_distributed；迭代/并行帧 → restructure——分布式同样会具名拒绝）。
    """
    # 函数内导入：strategies 依赖本模块的上游（errors），反向顶层导入成环。
    from plaita.core.strategies import _scope_gate_blocker

    node = carrier.node
    declared = (
        getattr(node, "execution_tier", None) is not None
        or tuple(getattr(node, "execution_requirements", ()) or ())
    )
    cause = "environment" if declared else "undeclared"

    blocker = _scope_gate_blocker(carrier)
    if blocker is not None:
        fix = {
            "action": "restructure",
            "call": None,
            "message": (
                f"挂起节点位于 {getattr(blocker.node, 'node_type', '?')!r} 帧 "
                f"{getattr(blocker.node, 'id', '?')!r} 内：v1 仅支持 child/reference "
                "调用链内挂起，分布式运行时同样具名拒绝；请把挂起节点移出迭代/并行帧"
            ),
        }
    else:
        fix = {
            "action": "run_distributed",
            "call": None,
            "message": (
                "本地 normal/generator 模式不具备挂起/恢复能力：事件/审批/延时等待"
                "需要分布式运行时。请到 console 将流程发布到分布式池验证，或使用 "
                "FlowExecution.run_distributed(...) 逐步驱动"
            ),
        }

    loc = f"，source line {node.source_line}" if getattr(node, "source_line", None) is not None else ""
    message = (
        "这不是源码缺陷，不要删除/替换该节点：\n"
        f"挂起节点 {getattr(node, 'id', '?')!r} ({getattr(node, 'node_type', '?')}{loc}) "
        f"在 flow {getattr(carrier.flow, 'flow_id', '?')!r} 返回了 pending 载荷，"
        "但当前执行模式（normal/generator）不具备挂起能力，流程已具名终止、"
        "后续节点未执行。"
    )
    chain = [carrier] + list(carrier.frames)  # 内层挂起点 → 最外父帧
    frame_path = [
        f"{getattr(f.node, 'id', '?')}({getattr(f.node, 'node_type', '?')})"
        for f in reversed(chain)
    ]
    return SuspendNotExecutableError(
        message,
        node=node,
        source_line=getattr(node, "source_line", None),
        flow_id=getattr(carrier.flow, "flow_id", None),
        cause=cause,
        fix=fix,
        frame_path=frame_path,
    )


def present_root_suspension(carrier: FlowSuspendedCarrier):
    """根出口呈现入口（含旋钮）：严格档返回 ``SuspendNotExecutableError``；
    ``warn`` 档留 NS-M3 原样 carrier、只落一条带结构化错误体的 WARNING。"""
    presented = surface_root_suspension(carrier)
    if suspend_guard_mode() == "warn":
        logger.warning(
            "[suspend-guard=warn] suspending node at normal/generator root "
            "presented as failure (kept raw carrier): %s",
            presented.to_dict(),
        )
        return carrier
    return presented


async def surface_root_suspension_agen(agen):
    """lazy（generator）根出口 twin：迭代中把载体转换为具名终态（不 yield 挂起步，
    与 Go ``ExecuteGenerator`` 的 Step{Error} 决策分类对齐，A2 评审 §2 采 Throw）。"""
    try:
        async for item in agen:
            yield item
    except FlowSuspendedCarrier as carrier:
        exc = present_root_suspension(carrier)
        if exc is carrier:
            raise
        raise exc from carrier


async def finish_normal(coro, flow, callback_manager, *, root: bool = False):
    """Await ``coro`` for normal mode; fire ``on_flow_end``; normalize errors.

    ``FlowExecutionException`` subclasses propagate untouched after firing
    ``on_flow_end`` with the exception's own ``code``/``message`` (C1-4,
    2026-10 评审修复包 C1：历史上直接透传不回调——注释声称 raise site 已发，
    但全仓 ``on_flow_end`` 只在本模块出现，raise site 从未发过，eager 路径
    FEE 时生命周期回调整个缺失). Anything else becomes a
    ``FlowErrorException`` / ``-500`` and triggers ``on_flow_end`` here.

    ``root``（plaita#330）：仅模式根（``parent is None``）为真——根出口把
    ``FlowSuspendedCarrier`` 身份转换为 ``SuspendNotExecutableError`` 后按同一条
    ``on_flow_end`` 链路呈现；非根（子执行）保持 carrier 原样透传（NS-M3 传播
    半边），回调节携带的仍是 carrier 自身 code/message（零行为变化）。
    """
    try:
        result = await coro
    except FlowSuspendedCarrier as carrier:
        exc = present_root_suspension(carrier) if root else carrier
        error = {"code": getattr(exc, "code", -500) or -500, "message": str(exc)}
        callback_manager.on_flow_end(flow, None, error, exception=exc)
        if exc is carrier:
            raise
        raise exc from carrier
    except FlowExecutionException as e:
        error = {"code": getattr(e, "code", -500) or -500, "message": str(e)}
        callback_manager.on_flow_end(flow, None, error, exception=e)
        raise
    except Exception as e:
        error = {"code": -500, "message": str(e)}
        callback_manager.on_flow_end(flow, None, error, exception=e)
        # raise 已携带完整 traceback，这里不再重复打栈
        logger.error("flow error: %s", _scrub_url_text(e))
        raise FlowErrorException(str(e)) from e
    callback_manager.on_flow_end(flow, result=result)
    return result


def raise_distributed_error(e, flow, callback_manager):
    """Distributed-mode normalization: *all* exceptions → ``FlowErrorException`` / ``-500``.

    Unlike :func:`finish_normal`, ``FlowExecutionException`` subclasses are also
    flattened — the distributed external contract is a single error shape, the
    subclass detail is internal-only.
    """
    error = {"code": -500, "message": str(e)}
    callback_manager.on_flow_end(flow, None, error, exception=e)
    logger.error("flow error: %s", _scrub_url_text(e))
    raise FlowErrorException(str(e)) from e


def emit_flow_end_on_close(flow, exception, callback_manager):
    """Lazy generator ``finally``: emit the deferred ``on_flow_end``.

    If the generator exited with a non-``FlowExecutionException`` exception,
    normalize it to ``-500`` and fire ``on_flow_end``. A
    ``FlowExecutionException`` fires ``on_flow_end`` with its own
    ``code``/``message`` in ``error`` (C1-4，2026-10 评审修复包 C1：历史上
    lazy 分支把 FEE 抹成 ``result=None, error=None``，宿主从回调无法区分
    失败与正常结束). A clean exit keeps the historical ``result=None``
    callback (the end step itself carries the flow result).
    """
    if exception is not None and not isinstance(exception, FlowExecutionException):
        error = {"code": -500, "message": str(exception)}
        callback_manager.on_flow_end(flow, None, error, exception=exception)
    elif exception is not None:
        error = {"code": getattr(exception, "code", -500) or -500, "message": str(exception)}
        callback_manager.on_flow_end(flow, None, error, exception=exception)
    else:
        callback_manager.on_flow_end(flow, result=None)
