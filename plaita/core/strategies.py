"""plaita.core.strategies — Execution strategies and mode/option definitions.

0.5.0 从 ``plaita.core.executor`` 拆出。本模块只装"执行策略"层:
- ``ExecutionMode`` / ``ExecutionStrategy`` Protocol / ``RunOptions`` / ``_StateView``;
- 三个具体策略 ``NormalStrategy`` / ``GeneratorStrategy`` / ``DistributedStrategy``;
- 策略共用的模块级 helper: ``_advance_one`` / ``_create_lazy_output`` /
  ``_create_end_output`` / ``_subscribe_event``;
- 嵌套挂起（plaita#311 NS-M3）的边界转换与沿路径恢复 helper:
  ``_park_nested_suspension``（DistributedStrategy 方法）/ ``_handle_nested_resume``
  / ``_advance_frame_to_end`` / ``_scope_gate_blocker`` / ``_carrier_frame_dicts`` /
  ``_load_suspend_frames`` / ``flow_contains_suspending_node``。

Driver (``FlowExecution``) 留在 ``plaita.core.executor``, 通过本模块组合策略。
``executor`` 重新导出本模块的公开符号, 保持 ``from plaita.core.executor import
ExecutionMode / NormalStrategy ...`` 的历史导入路径不断。
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, TYPE_CHECKING

from plaita.core.callback import BaseCallbackManager, CallbackManager
from plaita.core.capabilities import SUSPEND_ALLOWED_FRAME_NODE_TYPES
from plaita.core.context import ExecutionContext
from plaita.core.deadline import execution_deadline_ms
from plaita.core.errors import (
    ErrorStrategy,
    ExecutionDeadlineError,
    FlowExecutionException,
    FlowStartMissingError,
    FlowSuspendRejectedError,
    FlowSuspendedCarrier,
    FlowTimeoutError,
    ResumeError,
    ResumeGuardError,
    ResumeType,
)
from plaita.core.runner import NodeRunner
from plaita.tenant_context import DEFAULT_TENANT_ID, current_tenant
from plaita.core.url_redaction import _scrub_url_text

if TYPE_CHECKING:
    from plaita.event.core import EventBus

logger = logging.getLogger("plaita.core.strategies")


@dataclass
class RunOptions:
    """Per-run execution knobs, owned by the Driver (``FlowExecution``).

    Bundling ``mode`` / ``timeout`` into a dataclass makes "these are call-time
    options, not component lifecycle state" explicit. Strategies still receive
    ``timeout_ms`` as a loose param (their protocol is fixed by SC-010 tests);
    this object is Driver-internal plus the ``execution.options`` facade for
    node plugins that read ``execution.mode``.

    0.5.0 起 ``mode`` 类型为 ``Optional[ExecutionMode]``——历史上是裸字符串
    (``"normal"``/``"generator"``/``"distributed"``), 全库散落 ``mode == "generator"``
    字符串比较, 拼写错误静默成 False。现在内部一律走 enum, 公共入口 (``Flow.run``
    / ``FlowExecution.__init__``) 仍接受字符串, 经 ``_coerce_mode`` 统一一次。
    """

    mode: Optional[ExecutionMode] = None
    timeout: Optional[int] = None


def _coerce_mode(m) -> Optional[ExecutionMode]:
    """把字符串 / enum / None 统一成 ``Optional[ExecutionMode]``。

    公共入口接受 ``mode="generator"`` 这种历史字符串写法, 在边界处 coerce 一次,
    内部比较全用 enum, 杜绝 ``mode == "generater"`` 这类拼写错误静默成 False。
    """
    if m is None or isinstance(m, ExecutionMode):
        return m
    if isinstance(m, str):
        return ExecutionMode.from_string(m)
    return m


class _StateView:
    """Typed, ``None``-normalized read view over ``ExecutionContext`` state.

    ``execution.state.flow_id`` returns ``None`` for an unset key, vs the raw
    ``CheckpointState`` field which holds the ``_UNSET`` sentinel. Replaces the
    historical ``execution.flow_id`` / ``execution.last_node_id`` /
    ``execution.last_branch`` facade pass-throughs so the Driver no longer
    re-exports typed state under bare attribute names.
    """

    __slots__ = ("_ctx",)

    def __init__(self, ctx: ExecutionContext) -> None:
        self._ctx = ctx

    @property
    def flow_id(self) -> Optional[str]:
        return self._ctx.get_state(f"{self._ctx.express_prefix}FLOW_ID")

    @property
    def last_node_id(self) -> Optional[str]:
        return self._ctx.get_state(f"{self._ctx.express_prefix}LAST_NODE")

    @property
    def last_branch(self) -> Optional[str]:
        return self._ctx.get_state(f"{self._ctx.express_prefix}BRANCH")


class ExecutionMode(Enum):
    NORMAL = "normal"
    GENERATOR = "generator"
    DISTRIBUTED = "distributed"

    @classmethod
    def from_string(cls, mode: str) -> "ExecutionMode":
        try:
            return cls[mode.upper()]
        except KeyError:
            valid = ", ".join(m.value for m in cls)
            raise ValueError(
                f"Unknown execution mode: {mode!r}. Valid modes: {valid}"
            ) from None


class ExecutionStrategy(Protocol):
    """Protocol for mode-specific flow execution."""

    async def execute(
        self,
        flow,
        context: ExecutionContext,
        runner: NodeRunner,
        callback_manager: CallbackManager,
        params: Optional[Dict] = None,
        timeout_ms: Optional[int] = None,
        **options: Any,
    ) -> Any: ...


class NormalStrategy:
    """Execute all nodes to completion, return final result."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        # plaita#267：显式 flow/execution/调用期 timeout 缺省时补执行级总预算
        # （默认 1800s，PLAITA_EXECUTION_DEADLINE_SECONDS 可调）。每个节点拿到的
        # remaining 会随墙钟递减，单次执行不会无限占用共享 worker。
        timeout_ms = execution_deadline_ms(timeout_ms)
        next_node = flow.start_node
        if next_node is None:
            raise FlowStartMissingError()
        result = None
        reached_end = False
        start_time = time.time()

        while next_node:
            remaining = None
            if timeout_ms is not None:
                elapsed_ms = int((time.time() - start_time) * 1000)
                remaining = max(0, timeout_ms - elapsed_ms)

            result, branch, next_node, reached_end = await _advance_one(
                flow, runner, callback_manager, next_node, max_timeout_ms=remaining,
            )
            if reached_end:
                break

            logger.debug("next_node: %s with branch: %s", next_node, branch)

            if timeout_ms is not None and (time.time() - start_time) > timeout_ms / 1000:
                raise ExecutionDeadlineError(
                    f"Flow execution Timeout: execution deadline of {timeout_ms}ms "
                    f"exceeded before running node {next_node.id}"
                )

        if not reached_end:
            logger.debug("not reached_end: %s", next_node)
            pfx = context.express_prefix
            result = context.get_state(f"{pfx}{context.express_node_name}", {})

        return result


class GeneratorStrategy:
    """Async generator yielding per-node output for debug/stepping."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        # plaita#267：同 NormalStrategy——缺省补执行级总预算。
        timeout_ms = execution_deadline_ms(timeout_ms)
        next_node = flow.start_node
        if next_node is None:
            raise FlowStartMissingError()
        reached_end = False
        start_time = time.time()

        while next_node:
            current = next_node
            remaining = None
            if timeout_ms is not None:
                remaining = max(0, timeout_ms - int((time.time() - start_time) * 1000))
            result, branch, next_node, reached_end = await _advance_one(
                flow, runner, callback_manager, current, max_timeout_ms=remaining,
            )
            # End 节点这一步就是流程终点, is_end 必须为 True——历史上这里
            # 恒为 False, 消费方按 ``step["is_end"]`` 判完成永远不触发
            # (0.5.0 回归: docs/guide/execution-modes.md 的完成判断失效)。
            yield _create_lazy_output(
                current, result, branch, context.to_dict(),
                is_end=reached_end, execution_id=context.execution_id,
            )
            if reached_end:
                break

            logger.debug("next_node: %s with branch: %s", next_node, branch)

            if timeout_ms is not None and (time.time() - start_time) > timeout_ms / 1000:
                raise ExecutionDeadlineError(
                    f"Flow execution Timeout: execution deadline of {timeout_ms}ms "
                    f"exceeded before running node {next_node.id}"
                )

        if not reached_end:
            logger.debug("not reached_end: %s", next_node)
            pfx = context.express_prefix
            result = context.get_state(f"{pfx}{context.express_node_name}", {})
            yield _create_end_output(None, result, context.to_dict(), execution_id=context.execution_id)


class DistributedStrategy:
    """Execute one node per call with context persistence for suspend/resume."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        # plaita#267：distributed 每一步同样受执行级总预算约束（每步独立计，
        # 挂起/等待事件的时间不占用 worker，也不是本预算的用途）。缺省
        # 1800s，显式 timeout 优先。
        timeout_ms = execution_deadline_ms(timeout_ms)
        saved_context = options.get("saved_context")
        resume_type = ResumeType.coerce(options.get("resume_type", "continue"))
        resume_data = options.get("resume_data")
        pfx = context.express_prefix

        if saved_context:
            context.context = saved_context
            # 恢复路径也要认预铸的 execution_id（plaita#123 配套）：上面
            # ``context.context = saved_context`` 是**整体替换** checkpoint
            # （``CheckpointState.from_checkpoint_dict``），若 checkpoint 缺
            # ``$EXECUTION_ID``（resume 场景常见），引擎 execution_id 会退化成
            # 空串——``result.execution_id`` 回空、worker 侧重试计数键
            # （``noderetry:{execution_id}:{node}``）的 execution_id 段也随之为空，
            # 运维无法从键反查执行。此前 ``execution_id`` 只在 fresh-start 分支
            # 经 ``clean()`` 生效，恢复分支把它整个丢弃。
            # 语义：调用方显式给出时才覆盖（不回写 checkpoint，仅钉住本步的
            # 执行身份）；不给出则保持 checkpoint 原值，零回归。
            seeded_execution_id = options.get("execution_id")
            if seeded_execution_id:
                context.set_state(
                    f"{context.express_prefix}EXECUTION_ID", seeded_execution_id
                )
        else:
            context.clean(execution_id=options.get("execution_id"))
            context.setup_flow(flow, (), params or {})
            callback_manager.on_flow_start(flow)

        # G1：RETRY 与 CONTINUE 同一条步进路径——error 态 execution 的
        # checkpoint 里失败节点无条目（runner 成功后才写 last_node_id/结果），
        # 从 last_node_id 后继步进即恰好重跑失败节点、已完成节点不重放。
        # _handle_resume 是 EventNode 挂起专用路径，retry 不进。
        if saved_context and resume_type not in (ResumeType.CONTINUE, ResumeType.RETRY):
            return await self._handle_resume(flow, context, runner, callback_manager, resume_type, resume_data,
                                             max_timeout_ms=timeout_ms)

        if not saved_context and resume_type is not ResumeType.CONTINUE:
            # checkpoint 丢失/未传时，resume_type='cancel' 等会被静默丢弃、
            # 直接开跑全新流程——掩盖故障（R6 fuzz B2）。显式给出非 continue
            # 的 resume 意图却没有可恢复状态，应当报错（retry 同理：它只能
            # 唤醒已存在的 error 执行，不能凭空开新跑）。
            raise ResumeGuardError(
                f"resume_type={resume_type.value!r} requires a saved_context, "
                "but none was provided; the execution cannot be resumed. To start "
                "a fresh run, omit resume_type (or pass resume_type='continue')."
            )

        if saved_context and resume_type in (ResumeType.CONTINUE, ResumeType.RETRY):
            # 防御: 挂起中的 EventNode 不允许用默认 ``continue`` 绕过——历史上
            # 这条路会跳过 pending 校验直接推进到 End, 流程"正常完成", 事件
            # 永不消费且订阅泄漏, 无任何报错。只有事件已被 resume 消费
            # (状态不再是 pending) 后才允许 continue 推进后续节点。
            last_node_id = context.last_node_id
            if last_node_id:
                suspended_node = flow.find_node_by_id(last_node_id)
                # ``is True`` 而非真值判断: is_suspending 是 bool 属性, 需要滤掉
                # mock/异构节点对象上的 truthy 属性代理。
                if suspended_node is not None and getattr(suspended_node, "is_suspending", False) is True:
                    pfx_resume = context.express_prefix
                    node_results = context.get_state(f"{pfx_resume}{context.express_node_name}", {})
                    prev_state = node_results.get(last_node_id) if isinstance(node_results, dict) else None
                    status = prev_state.get("status", "") if isinstance(prev_state, dict) else ""
                    if status == "pending":
                        raise ResumeGuardError(
                            f"Execution is suspended at EventNode {last_node_id!r} (status=pending); "
                            "resume_type='continue'/'retry' would silently skip it. "
                            "Use resume_type='event'/'cancel'/'timeout' to resolve the event first.",
                            node=suspended_node,
                        )
            # plaita#311 NS-M3：嵌套挂起的 checkpoint 带帧路径（$SUSPEND_FRAMES，
            # 最内帧的 pending 挂起点不在根 $LAST_NODE 上）——continue/retry
            # 同样不得绕过（#33 守卫的嵌套半边；帧存在即执行仍处于挂起态）。
            frame_dicts = _load_suspend_frames(context)
            if frame_dicts:
                raise ResumeGuardError(
                    "Execution is suspended inside a nested child/reference call chain "
                    f"({_describe_frame_path(frame_dicts)}); resume_type='continue'/'retry' "
                    "would silently skip the pending suspension. "
                    "Use resume_type='event'/'cancel'/'timeout' to resolve the innermost "
                    "suspended node first.",
                )

        try:
            current_node, result, branch = await self._determine_current_node(
                flow, context, runner, callback_manager, max_timeout_ms=timeout_ms)
        except FlowSuspendedCarrier as carrier:
            # plaita#311 NS-M3：start 节点即子流程且内层挂起（或 start 节点本身
            # 是挂起型节点——_start_new_flow 已压入根帧）——转换为合法挂起。
            return await self._park_nested_suspension(carrier, flow, context, callback_manager)

        if not current_node:
            result = context.get_state(f"{pfx}{context.express_node_name}", {})
            return _create_end_output(None, result, context.to_dict(), execution_id=context.execution_id)

        # plaita#71：第 6 参（``max_timeout_ms``）不能丢——历史实现从第 2 步起
        # 不再把流程级/调用期预算下传，节点超时预算归零（只有首步的
        # ``_start_new_flow`` 收到），流程级超时在 distributed 逐步路径上形同虚设。
        return await self._execute_current_node(
            flow, context, runner, callback_manager, current_node, max_timeout_ms=timeout_ms)

    async def _determine_current_node(self, flow, context, runner, callback_manager, max_timeout_ms=None):
        last_node_id = context.last_node_id
        if last_node_id:
            return self._get_next_from_last(flow, context, last_node_id)
        return await self._start_new_flow(flow, context, runner, callback_manager, max_timeout_ms)

    def _get_next_from_last(self, flow, context, last_node_id):
        pfx = context.express_prefix
        current_node = flow.find_node_by_id(last_node_id)
        node_results = context.get_state(f"{pfx}{context.express_node_name}", {})
        result = node_results.get(last_node_id)
        branch = context.last_branch

        # 统一走 flow.next_node: 分支节点按 branch 选 branch.next, 普通节点走 next,
        # 避免在此重复实现一套与 flow._get_branch_target 易漂移的图遍历逻辑。
        next_node = flow.next_node(current_node, branch)
        if next_node is None and not flow.is_end_node(current_node):
            # 与 Normal/Generator 的 _advance_one 对齐: 后继解析不出不允许
            # 静默"完成"——分支未命中走 _handle_unmatched_branch, 普通节点
            # 缺 next 走 _handle_missing_next（历史上 distributed 会合成一个
            # is_end=True 的假 End 步）。
            if getattr(current_node, "branching", False):
                _handle_unmatched_branch(current_node, branch)
            else:
                _handle_missing_next(current_node)
        return next_node, result, branch

    async def _start_new_flow(self, flow, context, runner, callback_manager, max_timeout_ms=None):
        start_node = flow.start_node
        if not start_node:
            return None, None, None

        result, branch = await runner.run_node(
            flow, start_node, callback_manager=callback_manager, max_timeout_ms=max_timeout_ms)

        # plaita#311 NS-M3：start 节点本身是挂起型节点时同样不得把 pending 载荷
        # 当数据越过——与 _execute_current_node 的挂起分支同构，经 carrier 走
        # _park_nested_suspension 统一收口（根帧在此压入，转换点在 execute 的
        # except）。常规 flow 的入口被构造期约束为 type=start（Flow.start_node），
        # 本检查是自定义 registry / 程序化构造下的纵深防御。
        if getattr(start_node, "is_suspending", False) is True:
            carrier = FlowSuspendedCarrier(
                (
                    f"Node {start_node.id!r} ({getattr(start_node, 'node_type', '?')}) suspended "
                    f"in flow {getattr(flow, 'flow_id', '?')!r} at the start node: the pending "
                    "payload is refused as data (plaita#311 fail-closed)."
                ),
                node=start_node, flow=flow, context=context, node_result=result,
            )
            carrier.push_frame(flow, context, start_node)
            raise carrier

        # flow.next_node already handles both branching and non-branching nodes;
        # no need to replicate the "if start_node.next" guard here.
        current_node = flow.next_node(start_node, branch)
        if current_node is None and not flow.is_end_node(start_node):
            # 与 Normal/Generator 的 _advance_one 对齐: Start 后解析不出后继
            # 也不允许静默合成 is_end=True 的假 End 步（首步姊妹路径）。
            if getattr(start_node, "branching", False):
                _handle_unmatched_branch(start_node, branch)
            else:
                _handle_missing_next(start_node)
        return current_node, result, branch

    async def _execute_current_node(self, flow, context, runner, callback_manager, current_node, max_timeout_ms=None):
        try:
            result, branch = await runner.run_node(
                flow, current_node, callback_manager=callback_manager, max_timeout_ms=max_timeout_ms)
        except FlowSuspendedCarrier as carrier:
            # plaita#311 NS-M3 传播转换点（rs execute_child → advance_from 拦截
            # 的对应面）：子执行内部的挂起沿调用链穿出到本分布式步边界——把
            # 本帧（根）压入调用栈后转换为**本执行的合法挂起**。挂起不是本层
            # 节点的失败：不进重试/errorHandler（runner 逃逸分支已保证）。
            carrier.push_frame(flow, context, current_node)
            return await self._park_nested_suspension(carrier, flow, context, callback_manager)

        if flow.is_end_node(current_node):
            return _create_end_output(current_node, result, context.to_dict(), execution_id=context.execution_id)

        if current_node.is_suspending:
            # 订阅失败时禁止挂起：否则 execution 停在 suspended，而 pending
            # 校验又会挡住 resume，流程永久僵尸化。
            subscribed = await _subscribe_event(current_node, flow, result, context)
            if not subscribed:
                raise FlowExecutionException(
                    message=(
                        f"Event subscription failed for node {current_node.id}; "
                        "refusing to suspend without an active subscription"
                    ),
                    node=current_node,
                )
            callback_manager.on_node_suspend(flow, current_node)
            callback_manager.on_flow_suspend(flow)
            return _create_lazy_output(
                current_node, result, branch, context.to_dict(), is_suspend=True, execution_id=context.execution_id,
            )

        return _create_lazy_output(current_node, result, branch, context.to_dict(), execution_id=context.execution_id)

    # -- 嵌套挂起：边界转换与沿路径恢复（plaita#311 NS-M3）--------------------
    async def _park_nested_suspension(self, carrier, flow, context, callback_manager):
        """委托模块级实现（SC-003 类体预算：实现放模块层，见文件头说明）。"""
        return await _park_nested_suspension(carrier, flow, context, callback_manager)

    async def _handle_resume(self, flow, context, runner, callback_manager, resume_type, resume_data,
                             max_timeout_ms=None):
        # 统一在此 coerce, 覆盖 execute (已 coerce, 幂等) 与 _handle_resume_operation
        # (历史直传字符串) 两条入口, 避免裸字符串漏到下面的 enum 比较。
        resume_type = ResumeType.coerce(resume_type)
        pfx = context.express_prefix

        # plaita#311 NS-M3：嵌套挂起的 checkpoint 带帧路径——决策步与续跑沿帧
        # 路径进行（最内帧先恢复 → 帧回退逐层返回）。无帧路径保持既有根流程
        # 行为逐字不变（向后兼容钉）。
        frame_dicts = _load_suspend_frames(context)
        if frame_dicts:
            return await self._handle_nested_resume(
                flow, context, runner, callback_manager, resume_type, resume_data,
                frame_dicts, max_timeout_ms=max_timeout_ms)

        last_node_id = context.last_node_id
        if not last_node_id:
            raise ResumeGuardError("No suspended node found for resume")

        current_node = flow.find_node_by_id(last_node_id)
        # 用 is_suspending 标志判定, 而非 isinstance(EventNode): core 层不反向依赖
        # node 插件层; 保留 "is not an EventNode" 文案以兼容历史测试断言。
        if not current_node.is_suspending:
            raise ResumeGuardError(f"Node {current_node.id} is not an EventNode", node=current_node)
        node_results = context.get_state(f"{pfx}{context.express_node_name}", {})
        prev_state = node_results.get(last_node_id, {})

        # 准入三闸（pending 状态 / resume_type 白名单 / #221 订阅绑定）与嵌套
        # 路径共用同一 helper——语义逐字一致（plaita#33/#221）。
        resume_data = _gate_resume_carrier(current_node, prev_state, resume_type, resume_data)

        exec_ctx = runner.node_execution or context
        callback_manager.on_flow_resume(flow)
        callback_manager.on_node_resume(flow, current_node)

        try:
            # 多态分发下沉到节点自身 (Node.resume), core 不再 import EventNode
            result = current_node.resume(exec_ctx, resume_type, resume_data)
            callback_manager.on_node_end(flow, current_node, result)
        except Exception as e:
            error = {"code": -500, "message": str(e)}
            callback_manager.on_node_end(flow, current_node, None, error, exception=e)
            logger.error("Error during resume: %s", _scrub_url_text(e))
            resume_err = ResumeError(str(e), node=current_node)
            resume_err.source_line = getattr(current_node, "source_line", None)
            raise resume_err from e

        context.update_node_result(current_node, result)
        await self._unregister_suspended_subscription(current_node, context, prev_state)
        return _create_lazy_output(current_node, result, None, context.to_dict(), is_suspend=False, execution_id=context.execution_id)
    async def _handle_nested_resume(self, flow, context, runner, callback_manager,
                                    resume_type, resume_data, frame_dicts,
                                    max_timeout_ms=None):
        """委托模块级实现（SC-003 类体预算：实现放模块层，见文件头说明）。"""
        return await _handle_nested_resume(
            self, flow, context, runner, callback_manager, resume_type, resume_data,
            frame_dicts, max_timeout_ms=max_timeout_ms)

    async def _unregister_suspended_subscription(self, node, context, prev_state):
        """resume 完成后注销挂起期注册的事件订阅。

        历史上订阅永不注销：分布式部署下每个后续同类型事件都会匹配到死订阅、
        经 event_filter 再 enqueue 一次注定失败的 resume（ResumeError 噪音 +
        队列垃圾）。hasattr 探测保持对简化 bus 实现的兼容。
        """
        if not isinstance(prev_state, dict):
            return
        subscription_id = prev_state.get("subscription_id")
        if not subscription_id:
            return
        event_bus = context.get_or_create_event_bus()
        unregister = getattr(event_bus, "unregister_subscription", None) if event_bus else None
        if unregister is None:
            return
        try:
            outcome = unregister(subscription_id)
            if asyncio.iscoroutine(outcome):
                await outcome
            logger.debug("unregistered subscription %s for resumed node %s", subscription_id, node.id)
        except Exception as e:  # noqa: BLE001 - 注销失败不影响 resume 结果
            logger.warning("failed to unregister subscription %s after resume: %s", subscription_id, _scrub_url_text(e))


# ---------------------------------------------------------------------------
# Strategy-shared helpers (module-level)
# ---------------------------------------------------------------------------


#: 必须绑定挂起期订阅的 resume 类型（plaita#221）。
#:
#: ``event`` / ``timeout`` 都是 **event_filter 代投**的订阅决议：消息体里带着
#: 当初注册的 ``subscription_id``（event_filter.py:233 / :342），因此可以且必须
#: 与挂起 checkpoint 里 ``_subscribe_event`` 写下的 ``node_state["subscription_id"]``
#: 比对。``cancel`` 是**执行级**意图（console 取消消息 ``data=None``，见
#: ``api/executions.py`` 的 cancel 入队；取消当前挂起节点本来就是它的语义），
#: 不按订阅绑定——把它纳入比对会让所有 cancel 都变成「陈旧消息」。
_SUBSCRIPTION_BOUND_RESUME_TYPES = (ResumeType.EVENT, ResumeType.TIMEOUT)

#: 手工 resume（console 审批 / 本地档恢复）携带订阅绑定的**私有载体键**。
#:
#: ``event_filter`` 代投的 event/timeout 载荷把 ``subscription_id`` 直接放在
#: ``data`` 里——那是生产消息形状，节点也会把它写进 ``event_data``。console 的
#: 审批载荷是**用户数据**（如 ``{"approved": True}``），若把 ``subscription_id``
#: 混进去会污染 ``$NODE.<id>.event_data`` 与流程输出，故手工路径用本键承载绑定；
#: 准入校验通过后由 ``_handle_resume`` 在分发前剥掉，不进节点载荷。
RESUME_BINDING_KEY = "__plaita_subscription_id"


def _resume_binding_violation(prev_state, resume_type, resume_data) -> Optional[str]:
    """resume 消息是否**未绑定**当前挂起的订阅（plaita#221）；无违规返回 None。

    判据只有一条：resume 消息里的订阅 id 必须等于挂起 checkpoint 里
    ``prev_state["subscription_id"]``。订阅 id 取自两处：

    * ``resume_data["subscription_id"]``——event_filter 代投的 event/timeout
      载荷（生产消息形状）；
    * ``resume_data[RESUME_BINDING_KEY]``——console 手工 resume 的私有载体键
      （不污染用户载荷）。

    event 类型再加一道 ``event_type`` 比对（消息里的 ``event_type`` 与挂起节点
    解析后的 ``event_type``）。

    兼容面（刻意保守，不误杀）：

    * 非 ``event``/``timeout`` 类型直接放行（见 ``_SUBSCRIPTION_BOUND_RESUME_TYPES``）；
    * ``prev_state`` 没有 ``subscription_id``（历史 checkpoint、简化 event bus
      替身）⇒ 没有可比对的绑定信息，放行——修复前行为不变；
    * ``event_type`` 任一缺失则不比对（event_filter 的两种载荷都会带，
      缺失更可能来自历史消息而非伪造）。

    返回的是**可读原因**（进 WARNING 与 ``ResumeGuardError``），不是布尔——
    排障时要能一眼看出是「缺字段」还是「id 对不上」。
    """
    if resume_type not in _SUBSCRIPTION_BOUND_RESUME_TYPES:
        return None
    if not isinstance(prev_state, dict):
        return None
    expected_sub = prev_state.get("subscription_id")
    if not expected_sub:
        return None
    if not isinstance(resume_data, dict):
        return (
            f"resume 消息未携带订阅信息（期望 subscription_id={expected_sub!r}）"
        )
    got_sub = resume_data.get("subscription_id") or resume_data.get(RESUME_BINDING_KEY)
    if not got_sub:
        return f"resume 消息缺少 subscription_id（期望 {expected_sub!r}）"
    if str(got_sub) != str(expected_sub):
        return (
            f"subscription_id 不匹配（消息 {got_sub!r} ≠ 挂起订阅 {expected_sub!r}）"
        )
    expected_type = prev_state.get("event_type")
    got_type = resume_data.get("event_type")
    if expected_type and got_type and str(got_type) != str(expected_type):
        return (
            f"event_type 不匹配（消息 {got_type!r} ≠ 挂起 event_type {expected_type!r}）"
        )
    return None


# ---------------------------------------------------------------------------
# 嵌套挂起（plaita#311 NS-M3）：边界转换与沿路径恢复的实现体
# （模块级——SC-003 类体预算；DistributedStrategy 以同名薄方法委托）
# ---------------------------------------------------------------------------

async def _park_nested_suspension(carrier, flow, context, callback_manager):
    """把穿出到分布式步边界的内层挂起载体转换成本执行的合法挂起。

    rs ``suspend_flow``（订阅注册 + 挂起态落 $NODE + SuspendedExecution）与
    传播转换点闸（验证报告修正清单 #1）的合并对应面。次序对齐 rs 的
    fail-closed 承诺：**作用域闸先于订阅注册**——被拒的挂起绝不留下孤儿
    订阅（plaita 的注册只发生在本边界，结构上优于 rs 的注册后注销）。

    调用契约：carrier 的调用栈最后一帧已是根帧（调用方压入）；本方法：

    1. 作用域闸：调用栈每一帧的子节点必须是 child/reference——迭代帧
       （loop/map/filter/find/reduce/while）与 parallel 分支帧具名拒绝；
    2. 订阅注册：**最内帧**挂起节点的订阅（correlation_id=根执行 id，
       恢复消息据此路由回载帧路径的根 checkpoint）；
    3. 逐帧 checkpoint：帧路径写进根上下文 ``$SUSPEND_FRAMES``（最内帧在
       前；reference 帧的 $PARENT 构造期快照随帧上下文显式另存——验证
       报告存疑 #14 的逐帧快照要求）；
    4. 可观测：最内帧 ``on_node_suspend`` + 逐帧 ``on_flow_suspend``
       （最内→根；穿越型子节点的 NodeEnd(error) 已由 runner 补发——rs
       Q2 的两种 NodeEnd 形状口径）。
    """
    # -- 1. 作用域闸（fail-closed；child/reference 放行）--
    blocked = _scope_gate_blocker(carrier)
    if blocked is not None:
        raise FlowSuspendRejectedError(
            f"Suspension carrier crossed a {getattr(blocked.node, 'node_type', '?')!r} "
            f"frame boundary at node {blocked.node.id!r}: suspension v1 supports only "
            "child/reference call chains (loop/map/filter/find/reduce/while "
            "iterations and parallel branches need iteration-state snapshotting, "
            "a future milestone; see plaita-rs docs/PROGRAM.md 程式一)",
            node=blocked.node,
        )

    # -- 2. 最内帧订阅注册（correlation_id=根执行 execution_id）--
    pending_state = dict(carrier.node_result) if isinstance(carrier.node_result, dict) else carrier.node_result
    subscribed = await _subscribe_event(
        carrier.node, carrier.flow, pending_state, carrier.context,
        correlation_id=context.execution_id,
    )
    if not subscribed:
        raise FlowExecutionException(
            message=(
                f"Event subscription failed for node {carrier.node.id} in nested "
                f"flow {getattr(carrier.flow, 'flow_id', '?')!r}; refusing to suspend "
                "without an active subscription"
            ),
            node=carrier.node,
        )

    # -- 3. 逐帧 checkpoint（$SUSPEND_FRAMES 写进根上下文状态）--
    context.set_state(
        f"{context.express_prefix}{SUSPEND_FRAMES_KEY}",
        _carrier_frame_dicts(carrier, context),
    )

    # -- 4. 可观测（最内帧先于外层帧，验证报告 Q2 顺序细则）--
    callback_manager.on_node_suspend(carrier.flow, carrier.node)
    for frame in [carrier] + list(carrier.frames):
        callback_manager.on_flow_suspend(frame.flow)

    outermost = carrier.frames[-1].node if carrier.frames else carrier.node
    return _create_lazy_output(
        outermost, pending_state, None, context.to_dict(),
        is_suspend=True, execution_id=context.execution_id,
    )


async def _handle_nested_resume(strategy, flow, context, runner, callback_manager,
                                resume_type, resume_data, frame_dicts,
                                max_timeout_ms=None):
    """嵌套挂起的沿帧路径恢复（plaita#311 NS-M3）。

    与 rs ``SuspendedExecution::resume`` 续跑循环 / go ``ResumeSuspension``
    语义逐点对齐：**决策步**（最内帧挂起节点，``node.resume`` 多态分发不变，
    订阅注销按最内帧执行）→ 最内流程续跑 → 每层 flow 完成时作为「调用返回」
    把值回填父帧（``$NODE[child]``=返回值 + ``$LAST_NODE``=child + 续跑
    ``child.next``）→ 根帧回填后本步收尾——根流程的后续步进由后续 continue
    步驱动（plaita 分布式逐步契约；worker 对非挂起非终态结果自动续步）。
    续跑途中再次挂起则合并调用栈重新入挂起态（新现场更深在前 + 会话剩余栈
    在后，rs ``inner.call_stack.extend(stack)`` 对应面）。

    帧的 flow 定义不从 checkpoint 反序列化，而是沿根 flow 的 child/reference
    节点（``node.child_flow``）逐层下行解析——flow_id 不匹配即具名拒绝
    （定义漂移防线，与 worker 的 flow_hash 指纹同族语义）。
    """
    if resume_type not in (ResumeType.CANCEL, ResumeType.TIMEOUT, ResumeType.EVENT):
        raise ResumeError(
            f"Unsupported resume type for EventNode: {resume_type.value}, "
            "only support cancel, timeout, event",
        )

    # -- 帧解析与 flow 链下行（最外→最内；帧 schema 见 _carrier_frame_dicts）--
    if not isinstance(frame_dicts, list) or len(frame_dicts) < 2:
        raise ResumeError(
            "Malformed nested suspension frames: expected at least an innermost "
            f"frame and the root frame, got {frame_dicts!r}"
        )
    if frame_dicts[-1].get("context") is not None:
        raise ResumeError(
            "Malformed nested suspension frames: the outermost (root) frame must "
            "not carry a context snapshot (the root state is the checkpoint body)"
        )

    resolved = []  # [(node, frame_flow, frame_ctx)] 最外→最内
    frame_flow = flow      # 根帧 flow = 本次恢复调用的根 flow
    frame_ctx = context    # 根帧 ctx = 主上下文（checkpoint 本体已在 execute 恢复）
    for depth, entry in enumerate(reversed(frame_dicts)):
        saved_ctx = entry.get("context")
        if saved_ctx is None and entry is not frame_dicts[-1]:
            raise ResumeError(
                f"Malformed nested suspension frame at innermost depth "
                f"{len(frame_dicts) - 1 - depth}: intermediate frames must carry "
                "a context snapshot (only the root frame may omit it)"
            )
        if depth > 0:
            # 更内一帧：ctx 从帧快照重建；flow 已由上一层迭代下行。
            # parent=根 ctx——event bus / cancel_requested / 租户身份经父子
            # 链继承（ExecutionContext.child() 同款七项继承）。
            frame_ctx = context.child()
            frame_ctx.context = saved_ctx
        node_id = entry.get("node_id")
        flow_id = getattr(frame_flow, "flow_id", None)
        try:
            node = frame_flow.find_node_by_id(node_id)
        except Exception as e:
            raise ResumeError(
                f"Nested suspension frame node {node_id!r} not found in flow "
                f"{flow_id!r}: the frame path drifted from the flow definition "
                "(was the flow modified after suspension?)"
            ) from e
        entry_flow_id = entry.get("flow_id")
        if entry_flow_id and flow_id != entry_flow_id:
            raise ResumeError(
                f"Nested suspension frame flow mismatch at node {node_id!r}: "
                f"checkpoint recorded flow {entry_flow_id!r}, resume flow is "
                f"{flow_id!r}"
            )
        resolved.append((node, frame_flow, frame_ctx))
        if entry is not frame_dicts[0]:
            child_flow = getattr(node, "child_flow", None)
            if child_flow is None:
                raise ResumeError(
                    f"Nested suspension frame node {node_id!r} in flow {flow_id!r} "
                    "has no child_flow: cannot descend the suspension frame path"
                )
            frame_flow = child_flow

    # -- 决策步：最内帧挂起节点（准入三闸与根路径逐字同语义）--
    node, inner_flow, inner_ctx = resolved[-1]
    if getattr(node, "is_suspending", False) is not True:
        # 保留 "is not an EventNode" 文案以兼容历史测试断言（根路径同款）。
        raise ResumeGuardError(f"Node {node.id} is not an EventNode", node=node)
    pfx_inner = inner_ctx.express_prefix
    node_results = inner_ctx.get_state(f"{pfx_inner}{inner_ctx.express_node_name}", {})
    prev_state = node_results.get(node.id) if isinstance(node_results, dict) else None
    resume_data = _gate_resume_carrier(node, prev_state, resume_type, resume_data)

    # node.resume 直接收**帧上下文**：EventNode.on_event 读 $NODE 帧状态，
    # 传根 facade 会读错帧（嵌套语义面）。
    callback_manager.on_flow_resume(flow)
    callback_manager.on_node_resume(inner_flow, node)
    try:
        result = node.resume(inner_ctx, resume_type, resume_data)
        callback_manager.on_node_end(inner_flow, node, result)
    except Exception as e:
        error = {"code": -500, "message": str(e)}
        callback_manager.on_node_end(inner_flow, node, None, error, exception=e)
        logger.error("Error during resume: %s", _scrub_url_text(e))
        resume_err = ResumeError(str(e), node=node)
        resume_err.source_line = getattr(node, "source_line", None)
        raise resume_err from e

    inner_ctx.update_node_result(node, result)
    await strategy._unregister_suspended_subscription(node, inner_ctx, prev_state)

    # -- 续跑循环：栈 = 待返回父帧（index 0 = 最内父帧，末项 = 根帧）--
    stack = [
        (resolved[k][1], resolved[k][2], resolved[k][0])
        for k in range(len(resolved) - 2, -1, -1)
    ]
    current_flow, current_ctx = inner_flow, inner_ctx
    next_node = inner_flow.next_node(node, None)
    if next_node is None and not inner_flow.is_end_node(node):
        _handle_missing_next(node)

    while True:
        try:
            value = await _advance_frame_to_end(
                current_flow, current_ctx, callback_manager, next_node,
                max_timeout_ms=max_timeout_ms,
                node_execution=runner.node_execution)
        except FlowSuspendedCarrier as carrier:
            # 再挂起：新现场（更深）的调用栈在前，本会话剩余栈在后拼接，
            # 然后走同一边界转换（作用域闸 → 订阅注册 → 逐帧 checkpoint，
            # 旧帧路径被 $SUSPEND_FRAMES 覆写）。旧订阅已在上方注销——
            # 「旧注销先于新注册」次序成立（程式 ③.6）。
            for frame_flow, frame_ctx, frame_node in stack:
                carrier.push_frame(frame_flow, frame_ctx, frame_node)
            return await _park_nested_suspension(carrier, flow, context, callback_manager)

        # 本帧完成：每完成帧发 FlowEnd(result=返回值)，最内→根（恢复方向
        # 无 FlowStart，验证报告 Q2 口径；根帧的 FlowEnd 仍由既有机制负责）。
        callback_manager.on_flow_end(current_flow, result=value)

        if not stack:
            raise ResumeError(
                "nested suspension resume: call stack exhausted before the root "
                "frame return step"
            )
        frame_flow, frame_ctx, child_node = stack.pop(0)
        # 返回步 output 校验（rs validate_child_return_output 对应面）：
        # 校验失败 = ResumeError（终态失败，不静默回填）。
        validate = getattr(child_node, "_validate_output", None)
        if validate is not None:
            try:
                validate(value)
            except Exception as e:
                raise ResumeError(
                    f"call frame node {child_node.id!r}: {e}", node=child_node,
                ) from e
        # 回填细则（程式 ③.2）：$NODE[child]=v、$LAST_NODE=child、不写 $ERROR。
        frame_ctx.update_node_result(child_node, value)
        frame_ctx.last_node_id = child_node.id
        callback_manager.on_node_end(frame_flow, child_node, value)
        if not stack:
            # 根帧回填完成 → 本步收尾：帧路径已消费（置空 $SUSPEND_FRAMES），
            # 根流程从 child.next 的推进交给后续 continue 步（plaita 分布式
            # 逐步契约；输出 id=根帧子节点、is_suspend=False——worker据此
            # 置 running 并自动续步）。
            context.set_state(f"{context.express_prefix}{SUSPEND_FRAMES_KEY}", None)
            return _create_lazy_output(
                child_node, value, None, context.to_dict(),
                is_suspend=False, execution_id=context.execution_id,
            )
        current_flow, current_ctx = frame_flow, frame_ctx
        next_node = frame_flow.next_node(child_node, None)
        if next_node is None and not frame_flow.is_end_node(child_node):
            _handle_missing_next(child_node)


def _gate_resume_carrier(node, prev_state, resume_type, resume_data):
    """决策步准入三闸（plaita#311 NS-M3 抽取）：根路径与嵌套路径共用。

    语义与抽取前逐字一致（plaita#33 / #221）：

    1. ``status=pending``——非 pending 的挂起节点不可再决议；
    2. resume_type 白名单（cancel/timeout/event）；
    3. #221 订阅绑定——event/timeout 载体必须绑定挂起期登记的那条订阅
       （``_resume_binding_violation``），陈旧载体具名拒绝、执行保持挂起。

    通过后剥掉私有绑定键（不进节点载荷）并返回清洗后的 resume_data。
    """
    if not isinstance(prev_state, dict):
        prev_state = {}
    if prev_state.get("status", "") != "pending":
        raise ResumeGuardError(
            f"EventNode {node.id} is not in pending status: {prev_state.get('status', '')}",
            node=node,
        )
    if resume_type not in (ResumeType.CANCEL, ResumeType.TIMEOUT, ResumeType.EVENT):
        raise ResumeError(
            f"Unsupported resume type for EventNode: {resume_type.value}, only support cancel, timeout, event",
            node=node,
        )
    violation = _resume_binding_violation(prev_state, resume_type, resume_data)
    if violation:
        logger.warning(
            "丢弃陈旧 resume（%s）：%s；执行保持挂起，等待当前订阅的决议",
            resume_type.value, violation,
        )
        raise ResumeGuardError(
            f"Stale {resume_type.value} resume for EventNode {node.id}: "
            f"{violation}; execution stays suspended",
            node=node,
        )
    if isinstance(resume_data, dict) and RESUME_BINDING_KEY in resume_data:
        return {
            key: value for key, value in resume_data.items()
            if key != RESUME_BINDING_KEY
        }
    return resume_data


# ---------------------------------------------------------------------------
# 嵌套挂起（plaita#311 NS-M3）共享 helper
# ---------------------------------------------------------------------------

#: 嵌套挂起帧路径的 checkpoint 键。已登记进 ``CheckpointSchema.SYSTEM_KEY_NAMES``
#: （state.py），``validate_checkpoint`` 不再告警。值为帧列表：最内帧在前、
#: 末项=根帧（schema 见 :func:`_carrier_frame_dicts`）；``None``/缺失 = 无帧
#: （根流程直挂或旧 checkpoint——向后兼容路径）。
SUSPEND_FRAMES_KEY = "SUSPEND_FRAMES"

#: 作用域闸放行的帧节点类型（程式一 v1：仅 child/reference 调用链可承载
#: 挂起穿透）。单源在 plaita.core.capabilities（#331 入口静态闭包的
#: ``fix=restructure`` 判定共用），此处保留历史别名（仓内既有引用面不动）。
_SUSPEND_ALLOWED_FRAME_NODE_TYPES = SUSPEND_ALLOWED_FRAME_NODE_TYPES


def _load_suspend_frames(context):
    """从（已恢复的根）上下文读嵌套挂起帧路径；无帧/畸形一律归一为 ``[]``。"""
    frames = context.get_state(f"{context.express_prefix}{SUSPEND_FRAMES_KEY}", None)
    return frames if isinstance(frames, list) and frames else []


def _describe_frame_path(frame_dicts):
    """帧路径的可读投影（守卫消息用）：``flow:node -> flow:node -> …``（最内在前）。"""
    return " -> ".join(
        f"{d.get('flow_id', '?')!r}:{d.get('node_id', '?')!r}" for d in frame_dicts
    )


def _carrier_frame_dicts(carrier, root_context):
    """carrier 现场 + 调用栈 → checkpoint 帧列表（最内帧在前，rs call_stack 同序）。

    schema（每帧）：``{"node_id", "flow_id", "context"}``——

    - 现场帧（首项）：node_id = 挂起节点 id；
    - 父帧：node_id = 该帧 flow 中接收更内层返回值的子节点 id（rs CallFrame
      同构：值回填目标）；
    - 根帧（末项）：``context=None``——根状态即主 checkpoint 本体，不重复存。

    每帧 context 经 ``to_dict()`` 显式快照：reference 帧的 ``$PARENT`` 是构造期
    快照、父状态不在子帧的活引用里（验证报告存疑 #14），逐帧显式另存是嵌套
    checkpoint 的正确性前提。
    """
    dicts = [{
        "node_id": carrier.node.id,
        "flow_id": getattr(carrier.flow, "flow_id", None),
        "context": carrier.context.to_dict(),
    }]
    for frame in carrier.frames:
        dicts.append({
            "node_id": frame.node.id,
            "flow_id": getattr(frame.flow, "flow_id", None),
            # 根帧的上下文即主 checkpoint 本体（identity 判定），不重复存。
            "context": None if frame.context is root_context else frame.context.to_dict(),
        })
    return dicts


def _scope_gate_blocker(carrier):
    """传播转换点作用域闸（验证报告修正清单 #1）：返回第一个阻断帧，无则 None。

    child/reference 调用链放行；迭代帧（loop/map/filter/find/reduce/while）、
    parallel 分支帧及其余复合节点（fail-closed 缺省）具名阻断。plaita 的 parent
    链是真实执行链（无 rs/go reference ambient 断链问题），但为三仓规格一致仍
    采用转换点检查：帧节点对象由传播层压栈时捕获，永远是真实帧、无眼镜面。
    """
    for frame in carrier.frames:
        if getattr(frame.node, "node_type", None) not in _SUSPEND_ALLOWED_FRAME_NODE_TYPES:
            return frame
    return None


def flow_contains_suspending_node(flow) -> bool:
    """静态扫描：flow 图（含嵌套 child/reference）里是否存在挂起型节点。

    parallel 分支派发前的确定性闸（plaita#311 NS-M3，rs
    ``reject_suspending_branches`` 对应面）：后台分支的动态拒绝是异步的、无法
    同步上抛——v1 语义 = 分支图内不允许挂起型节点。getattr 鸭子判定，core 不
    反向依赖 node 插件层。
    """
    for node in getattr(flow, "nodes", None) or []:
        if getattr(node, "is_suspending", False) is True:
            return True
        child = getattr(node, "child_flow", None)
        if child is not None and flow_contains_suspending_node(child):
            return True
    return False


class _FrameExecutionView:
    """恢复会话帧的节点执行视图（NS-M4 case 09 缺陷的修正面）。

    恢复续跑的帧推进需要**双身份**：state/evaluate 必须落在**帧 ctx**（内层
    `$NODE` 可见），`get_child_execution`/`run_compatible` 必须走**宿主
    facade**（child/reference 的子执行机制长在 facade 上）。二者分属不同
    对象——`NodeRunner(context, node_execution=facade)` 的正典接线（executor
    的同帧执行）会让节点经 facade 读到**根** `$NODE`（case 09 实测：内层 end
    的 `$NODE.evB` 求值 None）。本视图按成员归属显式分派，未列成员
    `__getattr__` 回落 facade（子执行 bus/取消/租户身份等继承面）。
    """

    def __init__(self, frame_ctx, facade):
        self._frame_ctx = frame_ctx
        self._facade = facade

    # -- 帧 ctx 侧（状态与求值语义）--
    @property
    def express_prefix(self):
        return self._frame_ctx.express_prefix

    @property
    def execution_id(self):
        return self._frame_ctx.execution_id

    @property
    def context(self):
        return self._frame_ctx.context

    def evaluate(self, value):
        return self._frame_ctx.evaluate(value)

    def get_state(self, key, default=None):
        return self._frame_ctx.get_state(key, default)

    def set_state(self, key, value):
        self._frame_ctx.set_state(key, value)

    def set_scratch(self, key, value):
        self._frame_ctx.set_scratch(key, value)

    def update_node_result(self, node, result):
        self._frame_ctx.update_node_result(node, result)

    def get_node_state(self, node_id, default=None):
        # EventNode._get_node_state 的 hasattr 探测面（frame ctx 若实现则用之）。
        getter = getattr(self._frame_ctx, "get_node_state", None)
        return getter(node_id, default) if getter else default

    def get_global_variable(self, key, default=None):
        # 全局变量随帧继承（child() 七项继承），帧 ctx 为准。
        getter = getattr(self._frame_ctx, "get_global_variable", None)
        return getter(key, default) if getter else self._facade.get_global_variable(key, default)

    # -- facade 侧（子执行机制与宿主身份）--
    def get_child_execution(self):
        return self._facade.get_child_execution()

    def run_compatible(self, flow, lazy, *args, **params):
        return self._facade.run_compatible(flow, lazy, *args, **params)

    def arun_compatible(self, flow, lazy, *args, **params):
        return self._facade.arun_compatible(flow, lazy, *args, **params)

    def get_or_create_event_bus(self):
        return self._facade.get_or_create_event_bus()

    def __getattr__(self, name):
        # 未列成员回落 facade（cancel/set_global_variable 等宿主继承面）。
        return getattr(self._facade, name)


async def _advance_frame_to_end(flow, context, callback_manager, start_node, max_timeout_ms=None, node_execution=None):
    """恢复会话的单帧推进体（rs ``advance_and_finalize`` / go ``runNodes`` 的
    恢复侧对应面）：从给定节点推进到 End 并返回其输出。

    途中挂起经 :func:`_advance_one` 的 carrier 机制上抛（调用方捕获后走再挂起
    栈拼接）；节点错误照常走既有 retry/errorHandler 链——恢复会话不绕过首跑
    的本帧策略链（验证报告 Q8.1 的 fail-open 反面）。

    预算（程式一④）：每帧新鲜发放——``execution_deadline_ms``（flow 自有
    timeout 由上游合并传入，缺省执行级 1800s），节点之间检查墙钟，与
    ``NormalStrategy`` 同形；分布式逐步本就 per-step 新鲜发放，本 helper 把同
    一口径落到嵌套恢复的每一帧。
    """
    max_timeout_ms = execution_deadline_ms(max_timeout_ms)
    # 节点执行视图（NS-M4 case 09 修正面）：evaluate/state 走帧 ctx（内层
    # $NODE 可见），get_child_execution/run_compatible 走宿主 facade——单传
    # facade 会让内层 end 的 `$NODE.evB` 读到根状态（None）；单传帧 ctx 会让
    # child 的 get_child_execution 炸 AttributeError。两个都是实测。
    runner = NodeRunner(
        context,
        node_execution=(
            node_execution if node_execution is None
            else _FrameExecutionView(context, node_execution)
        ),
    )
    node = start_node
    start_time = time.time()
    while node is not None:
        result, branch, next_node, is_end = await _advance_one(
            flow, runner, callback_manager, node, max_timeout_ms=max_timeout_ms)
        if is_end:
            return result
        if next_node is None:
            # _handle_missing_next 的 errorHandler continue 逃生口：与
            # NormalStrategy 同口径——以 $NODE 状态表收尾。
            break
        if max_timeout_ms is not None and (time.time() - start_time) > max_timeout_ms / 1000:
            raise ExecutionDeadlineError(
                f"Flow execution Timeout: execution deadline of {max_timeout_ms}ms "
                f"exceeded before running node {next_node.id}"
            )
        node = next_node
    pfx = context.express_prefix
    return context.get_state(f"{pfx}{context.express_node_name}", {})


def _create_lazy_output(node, result, branch, context, is_suspend=False, execution_id=None, is_end=False):
    return {
        "id": node.id,
        "type": node.node_type,
        "name": node.name,
        "result": result,
        "branch": branch or "",
        "context": context,
        "is_end": is_end,
        "is_suspend": is_suspend,
        "execution_id": execution_id,
    }


async def _advance_one(flow, runner, callback_manager, node, max_timeout_ms=None):
    """Run one node via *runner*, update LAST_NODE/BRANCH/$NODE state, then resolve
    the next node. Returns ``(result, branch, next_node, is_end)``.

    Shared by ``NormalStrategy`` and ``GeneratorStrategy`` so the
    run → detect-end → resolve-next sequence lives in exactly one place.
    Distributed mode cannot share it wholesale because it suspends on
    ``EventNode`` instead of advancing, so it keeps its own single-step path.

    plaita#311 NS-M3（嵌套挂起）两面：

    - **fail-closed**（修正清单 #7：与子执行 fail-open 是同一个洞）：挂起型节点
      的 pending 载荷不得当普通数据继续推进——``is_suspending`` 为真即抛
      :class:`FlowSuspendedCarrier`（先于 end 判断与后继解析；挂起节点不可能
      是 End）。根流程 normal/generator 模式由此从「静默假完成」变为具名失败；
      分布式根的子执行里，载体沿调用链上抛到分布式步边界转换成合法挂起。
      ``is True`` 的 bool 代理防御与本模块 continue 守卫（历史 288 行）同款。
    - **传播压帧**（rs ``advance_from`` 的 push_frame 对应面）：carrier 从内层
      ``runner.run_node`` 穿出时，把本帧（flow/上下文/子节点）压入其调用栈后
      原样上抛——恢复续跑循环按同序消费（最深帧先返回）。
    """
    try:
        result, branch = await runner.run_node(
            flow, node, max_timeout_ms=max_timeout_ms, callback_manager=callback_manager,
        )
    except FlowSuspendedCarrier as carrier:
        carrier.push_frame(flow, runner.context, node)
        raise
    # plaita#311 fail-closed：挂起检查先于 end 判断（挂起节点不可能是 End，
    # rs advance_from 同序）。
    if getattr(node, "is_suspending", False) is True:
        raise FlowSuspendedCarrier(
            (
                f"Node {node.id!r} ({getattr(node, 'node_type', '?')}) suspended in flow "
                f"{getattr(flow, 'flow_id', '?')!r}: the pending payload is refused as "
                "data (plaita#311 fail-closed). Suspend/resume requires distributed "
                "mode; inside normal/generator executions a suspending node is a "
                "terminal named failure."
            ),
            node=node, flow=flow, context=runner.context, node_result=result,
        )
    is_end = flow.is_end_node(node)
    next_node = None if is_end else flow.next_node(node, branch)
    if next_node is None and not is_end:
        # 非 End 节点解析不出后继：分支节点是"分支未命中"，普通节点是"缺 next"。
        # 两条路都不允许静默"成功"（历史上流程会带着 $NODE 状态表"成功"结束）。
        if getattr(node, "branching", False):
            _handle_unmatched_branch(node, branch)
        else:
            _handle_missing_next(node)
    return result, branch, next_node, is_end


def _handle_missing_next(node):
    """普通（非分支）节点缺 ``next`` 且不是 End：拒绝静默"成功"。

    与 :func:`_handle_unmatched_branch` 同源的姊妹路径：分支未命中早有防御，
    但普通节点末尾漏写 next（或漏接 End）历史上同样会把整个 ``$NODE`` 状态表
    当流程结果"成功"返回。现在默认抛错；节点显式配置
    ``errorHandler.strategy=continue/continue-with`` 时保留旧的"继续"逃生口
    （降级为 warning，流程以 $NODE 状态表收尾）。
    """
    handler = getattr(node, "error_handler", None)
    strategy = handler.strategy if handler is not None else None
    if strategy in (ErrorStrategy.CONTINUE, ErrorStrategy.CONTINUE_WITH):
        logger.warning(
            "non-branching node %s has no 'next' and is not an End node; "
            "ending the flow with the $NODE state as its result "
            "(errorHandler.strategy=%s legacy behavior)",
            node.id, strategy.value,
        )
        return
    raise FlowExecutionException(
        message=(
            f"Node '{node.id}' ({getattr(node, 'node_type', '?')}) has no 'next' "
            f"and is not an End node; the flow cannot continue and refuses to end "
            f"with an ambiguous intermediate result. Connect this node to a "
            f"successor or an End node, or set its "
            f"errorHandler.strategy='continue' to opt into the legacy skip behavior."
        ),
        node=node,
    )


def _handle_unmatched_branch(node, branch):
    """分支节点未命中任何分支且无 default：拒绝静默"成功"。

    历史行为是返回 None → 调度层把整个 ``$NODE`` 状态表当流程结果返回，
    流程带着中间态"成功"结束——这是编排框架最危险的静默错误结果。
    现在默认抛错；节点显式配置 ``errorHandler.strategy=continue/continue-with``
    时保留旧的"继续"逃生口（降级为 warning）。
    """
    handler = getattr(node, "error_handler", None)
    strategy = handler.strategy if handler is not None else None
    if strategy in (ErrorStrategy.CONTINUE, ErrorStrategy.CONTINUE_WITH):
        # 逃生口语义（与 0.5.x 历史行为一致）：分支节点的"下游"由分支定义，
        # 未命中即无下游——流程在此收尾，$NODE 状态表作为流程结果返回。
        logger.warning(
            "branching node %s matched no branch and has no default (branch=%r); "
            "ending the flow with the $NODE state as its result "
            "(errorHandler.strategy=%s legacy behavior)",
            node.id, branch, strategy.value,
        )
        return
    raise FlowExecutionException(
        message=(
            f"Branching node '{node.id}' ({getattr(node, 'node_type', '?')}) matched "
            f"no branch and has no default branch; refusing to end the flow with an "
            f"ambiguous intermediate result. Add a default branch, or set this node's "
            f"errorHandler.strategy='continue' to opt into the legacy skip behavior."
        ),
        node=node,
    )


def _create_end_output(node, result, context, execution_id=None):
    return {
        "id": node.id if node else None,
        "type": "end",
        "name": node.name if node else "结束",
        "result": result,
        "branch": "",
        "context": context,
        "is_end": True,
        "is_suspend": False,
        "execution_id": execution_id,
    }


async def _subscribe_event(node, flow, node_state, context, correlation_id=None):
    """Subscribe an EventNode to its event bus.

    This is an async function: when ``event_bus.register_subscription`` is a
    coroutine function it is awaited directly inside the caller's event loop,
    rather than being bridged through a worker thread + fresh loop.

    ``correlation_id``（plaita#311 NS-M3）：显式覆盖订阅的关联 id。嵌套挂起在
    分布式步边界注册**最内帧**节点的订阅，但恢复消息必须路由到**根分布式
    执行**（checkpoint 载帧路径的那个 execution）——缺省仍用
    ``context.execution_id``（根流程直挂的既有行为不变）。
    """
    event_bus = context.get_or_create_event_bus()
    if not event_bus:
        error_state = node.on_error(context, "Unable to get event bus")
        context.update_node_result(node, error_state)
        return False

    try:
        resolved_event_type = node_state.get("event_type", node.event_type)
        subscription_params = {
            "event_type": resolved_event_type,
            "filter_condition": node.event_filter,
            "correlation_id": correlation_id or context.execution_id,
            "flow_id": flow.flow_id,
            "node_id": node.id,
        }
        # 波次④（订阅自动超时）：仅当节点配置了 subscription_timeout 才携带
        # timeout 形参。存量节点（None）保持与历史完全一致的参数集——不依赖
        # 各 bus 实现是否接受该形参（register_subscription 的 ABC/redis/memory/
        # sqlalchemy 均已就绪，简化测试替身未必），超时语义由 event_filter 侧
        # SubscriptionTimeoutChecker 消费，此处只负责透传。
        node_subscription_timeout = getattr(node, "subscription_timeout", None)
        if node_subscription_timeout is not None:
            subscription_params["timeout"] = node_subscription_timeout

        # P3（2026-10-02 评审遗留 #6）：非 default 租户把当前租户写进订阅——
        # 超时恢复路径（event_filter._on_subscription_timeout）没有事件载体，
        # 租户必须在挂起时就随订阅持久化，否则恢复时按 default 命名空间加载
        # 执行状态必然 miss，超时 resume 整体失效。default/空租户不带该键，
        # 与历史参数集逐字节一致（存量简化 bus 替身零破坏，同 timeout 先例）；
        # 挂起时租户 ContextVar 已由 worker 消息分发（_dispatch_task）设置。
        # 租户上下文自顶层 plaita.tenant_context 引入——core 禁止 import
        # server（import 分层检查）。
        suspended_tenant = current_tenant()
        if suspended_tenant and suspended_tenant != DEFAULT_TENANT_ID:
            subscription_params["tenant_id"] = suspended_tenant

        if asyncio.iscoroutinefunction(event_bus.register_subscription):
            subscription_id = await event_bus.register_subscription(**subscription_params)
        else:
            subscription_id = event_bus.register_subscription(**subscription_params)

        node_state["subscription_id"] = subscription_id
        # plaita#320：把**解析后**的事件类型与订阅超时随 checkpoint 落进
        # node_state——worker 挂起落盘构造 pending（kind=event）时只能读
        # context（拿不到节点对象），动态 event_type（$expr）解析结果与
        # subscription_timeout 不落这里，pending 就得空手编造或缺 deadline。
        # 均为加键（存量读者全部 .get 取用），checkpoint 形状向后兼容。
        node_state["event_type"] = resolved_event_type
        node_state["subscription_timeout"] = node_subscription_timeout
        context.update_node_result(node, node_state)
        return True
    except Exception as e:
        error_state = node.on_error(context, f"Subscribe failed: {e}")
        context.update_node_result(node, error_state)
        return False
