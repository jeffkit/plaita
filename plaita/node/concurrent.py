import asyncio
import logging
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import Field, model_validator

from plaita.core.parallel_executor import (
    PROCESS,
    THREAD,
    BackGroundProcessPool,  # re-export for backward compat (历史调用方从本模块导入)
    BackGroundThreadPool,
    ParallelExecutor,
    in_plaita_pool_thread,
    make_executor,
)
from plaita.node import Node
from plaita.node.decide import Branch
from plaita.core.strategies import ExecutionMode

# re-export
__all__ = [
    "Parallel",
    "ParallelBranch",
    "ARTIFICIAL",
    "COROUTINE",
    "THREAD",
    "PROCESS",
    "BackGroundThreadPool",
    "BackGroundProcessPool",
]

logger = logging.getLogger(__name__)

ARTIFICIAL = "artificial"
COROUTINE = "coroutine"

# 后台分支可观测性状态: execution_id -> {"futures": [...], "errors": [...]}。
# 故意不挂在 ``execution`` 实例上——process 模式下 ``execution`` 会被 pickle 到
# 子进程, 而 ``concurrent.futures.Future`` / 锁不可 pickle, 挂在 execution 上会让
# 后续 join 分支提交时 pickle 失败。模块级 dict 按 execution_id 索引, 子进程不
# 触碰, callback 在主进程跑, 安全。
import threading as _threading

# RLock: get_background_errors 在持锁状态下会再调 _get_bg_state, 需可重入。
_BG_LOCK = _threading.RLock()
_BG_STATE: Dict[str, Dict[str, list]] = {}


def _get_bg_state(execution) -> Dict[str, list]:
    eid = getattr(execution, "execution_id", None) or str(id(execution))
    with _BG_LOCK:
        st = _BG_STATE.get(eid)
        if st is None:
            st = {"futures": [], "errors": []}
            _BG_STATE[eid] = st
        return st


class ParallelBranch(Branch):
    """ParallelBranch represents a branch of a parallel node"""

    flow: Optional[Any] = None
    input: Any = None

    @model_validator(mode="before")
    @classmethod
    def setup_flow(cls, values: Dict) -> Dict:
        from plaita.core.flow import Flow

        flow = values.get("flow")
        if isinstance(flow, str):
            values["flow"] = Flow.model_validate_json(flow)
        elif isinstance(flow, dict):
            values["flow"] = Flow.model_validate(flow)
        elif isinstance(flow, Flow):
            values["flow"] = flow
        return values


class Parallel(Node):
    """
    Parallel represents a parallel node
    """

    node_type: ClassVar[str] = "parallel"
    node_name: ClassVar[str] = "并行"

    branches: List[ParallelBranch] = Field(default_factory=list)
    is_conditional: bool = Field(default=False)
    mode: str = Field(default=THREAD)
    join_branches: List[str] = Field(default_factory=list)

    # setup_branches 消费的 camelCase 遗留键（is_conditional/join_branches 已是声明字段）
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"joinBranches", "isConditional"})

    @model_validator(mode="before")
    @classmethod
    def setup_branches(cls, values: Dict) -> Dict:
        branches = values.get("branches", [])
        values["branches"] = []
        for branch in branches:
            if isinstance(branch, ParallelBranch):
                values["branches"].append(branch)
            elif isinstance(branch, dict):
                values["branches"].append(ParallelBranch(**branch))
            else:
                raise ValueError(f"Unknown branch type: {type(branch)}")
        # joinBranches， isConditional 这两个字段如果存在，则需要进行处理
        join_branches = values.get("joinBranches") or values.get("join_branches")
        if join_branches:
            values["join_branches"] = join_branches
        is_conditional = values.get("isConditional") or values.get("is_conditional")
        if is_conditional:
            values["is_conditional"] = True
        return values

    def validate(self):
        # Add validation logic here
        pass

    def exec_branch(self, pb: ParallelBranch, execution):
        """执行并行节点的分支flow

        异常不在此处吞咽——交由调用方（``_process_future_result`` / coroutine 路径）
        显式决定如何记录。历史上这里 ``return None`` 让崩溃分支与"返回 None"无法
        区分，导致下游节点拿到静默错误结果继续执行。

        当分支未指定 ``input``（`pb.input is None`，典型场景为 @flow DSL 编译生成
        的 PARALLEL 节点）时，自动继承父流程的 ``$INPUT``，使分支子流程能自然地用
        ``INPUT.x`` 访问父流程的输入字段。显式设置了 ``input`` 的分支不受影响。
        """
        branch_execution = execution.get_child_execution()
        lazy = execution.mode == ExecutionMode.GENERATOR
        if pb.input is None:
            # @flow DSL 生成的 PARALLEL 分支没有显式 input 字段；继承父流程 INPUT
            # 使子流程能用 INPUT.x 访问父流程的输入（与 @childflow + CHILD 的语义一致）。
            input_key = f"{execution.express_prefix}{execution.express_input_name}"
            input_value = execution.evaluate(input_key)
        else:
            input_value = execution.evaluate(pb.input)
        rs = branch_execution.run_compatible(pb.flow, lazy, input_value)
        logger.debug("branch %s executed: %s", pb.name, rs)
        return rs

    def _build_executor(self, mode: str, execution) -> Optional[ParallelExecutor]:
        """按 ``mode`` 构造执行器。进程模式在父进程 cancel_event 已触发时
        直接放弃启动子进程 (进程模式 cancel 不跨进程传播, 启动了也响应不了)。"""
        if mode == PROCESS:
            cancel_event = getattr(execution, "cancel_event", None)
            if cancel_event is not None and cancel_event.is_set():
                logger.warning(
                    "parallel %s: cancel_event already set, skip process branches", self.id,
                )
                return None
        return make_executor(mode)

    def pool_execute(self, pool_type: Optional[str] = None, execution=None) -> Dict[str, Any]:
        """使用 ``ParallelExecutor`` 执行并行节点。

        历史上每个 Parallel 节点 ``with ThreadPoolExecutor()`` 自起+销毁一个池——
        节点递归嵌套时会指数级开 worker、且默认无 ``max_workers``。现在通过
        ``plaita.core.parallel_executor.make_executor`` 拿一个复用模块级单例池的
        执行器 (thread/process), 与 ``Map.concurrent`` 走同一套 ``ParallelExecutor``
        协议。cancel/timeout 跨进程限制见 ``ParallelExecutor.supports_cancel_propagation``。
        """
        branches_to_execute = self.match_condition_branches(execution)
        join_branches, background_branches = self._split_branches(branches_to_execute)

        mode = pool_type or self.mode
        executor = self._build_executor(mode, execution)
        if executor is None:
            return {}

        if mode == THREAD and in_plaita_pool_thread() and (join_branches or background_branches):
            # 本节点已跑在共享线程池的 worker 上: 再向同一池提交并阻塞 join,
            # 分支数 >= max_workers 时所有 worker 互相等待, 永久死锁。降级为
            # 串行内联执行 (正确性优先, 仅嵌套场景放弃并发)。
            return self._execute_nested_inline(join_branches, background_branches, execution)

        self._execute_background_branches(background_branches, executor, execution)
        return self._execute_join_branches(join_branches, executor, execution)

    def _execute_nested_inline(self, join_branches, background_branches, execution) -> Dict[str, Any]:
        """池 worker 线程上的串行降级路径。

        background 分支保持"失败留痕不抛"的容错语义; join 分支保持
        ``_process_future_result`` 的错误哨兵结构, 但全部在当前线程顺序执行。
        """
        logger.debug(
            "parallel %s: on a plaita pool worker thread; executing %d join + %d background "
            "branches inline (nested shared-pool submission would starve)",
            self.id, len(join_branches), len(background_branches),
        )
        results: Dict[str, Any] = {}
        for branch in background_branches:
            try:
                self.exec_branch(branch, execution)
            except Exception as exc:  # noqa: BLE001 - 与 background done_callback 同语义
                logger.warning("parallel background branch %r raised: %s", branch.name, exc, exc_info=True)
                state = _get_bg_state(execution)
                with _BG_LOCK:
                    state["errors"].append({"branch": branch.name, "error": str(exc)})
        for branch in join_branches:
            try:
                results[branch.name] = self.exec_branch(branch, execution)
            except Exception as exc:  # noqa: BLE001
                logger.warning("parallel branch %s raised: %s", branch.name, exc, exc_info=True)
                results[branch.name] = {
                    "__parallel_error__": str(exc),
                    "__branch__": branch.name,
                }
        return results

    def _split_branches(self, branches):
        join_branches = [b for b in branches if b.name in self.join_branches]
        background_branches = [b for b in branches if b.name not in self.join_branches]
        return join_branches, background_branches

    def _execute_background_branches(self, background_branches, executor, execution):
        """fire-and-forget: submit 后持有 future 引用 + ``done_callback`` 记录失败。

        不等结果 (保持 fire-and-forget 语义), 但失败不再沉默——callback 把异常
        记进模块级 ``_BG_STATE[execution_id]["errors"]`` (一个
        ``[{branch, error}, ...]`` 列表) 并 ``logger.warning``。future 引用存
        在同处供调试期可选等待 (``wait_background_branches``)。

        状态存模块级而非 ``execution`` 实例: process 模式下 ``execution`` 被
        pickle 到子进程, Future/锁不可 pickle, 挂在 execution 上会让后续 join
        分支提交 pickle 失败。

        历史上这里是真 fire-and-forget: 不持 future、无回调、无超时, 分支崩溃
        全静默, 运维侧无任何信号。0.5.0 起失败至少留痕。
        """
        if not background_branches:
            return
        state = _get_bg_state(execution)
        errors = state["errors"]
        for branch in background_branches:
            fut = executor.submit(self.exec_branch, branch, execution)
            state["futures"].append(fut)
            fut.add_done_callback(
                self._make_background_done_callback(branch, errors)
            )

    @staticmethod
    def _make_background_done_callback(branch, errors):
        """构造 future done_callback: 失败时把异常追加到共享 errors 列表。"""
        def _on_done(fut):
            try:
                exc = fut.exception()
            except Exception as e:  # cancelled / executor shutdown
                logger.debug("background future.exception() unavailable", exc_info=True)
                exc = e
            if exc is None:
                return
            logger.warning(
                "parallel background branch %r raised: %s", branch.name, exc,
                exc_info=True,
            )
            with _BG_LOCK:
                errors.append({"branch": branch.name, "error": str(exc)})
        return _on_done

    def wait_background_branches(self, execution, timeout=None):
        """调试用: 等待本节点提交的后台分支 future 完成 (不取结果, 仅 join)。

        返回 ``{"done": n, "not_done": m}``。生产路径无需调用——后台分支本就是
        fire-and-forget; 此接口供测试与故障排查时确认后台分支是否已落地。
        """
        import concurrent.futures as _cf
        futures = _get_bg_state(execution)["futures"]
        if not futures:
            return {"done": 0, "not_done": 0}
        done, not_done = _cf.wait(list(futures), timeout=timeout)
        return {"done": len(done), "not_done": len(not_done)}

    def get_background_errors(self, execution) -> list:
        """读取本执行实例的后台分支失败记录 (``[{branch, error}, ...]`` 的拷贝)。

        失败分支在 ``done_callback`` 里记入; 主流程不抛、不阻塞——仅留痕供
        运维/测试侧观测。返回拷贝避免外部误改内部状态。
        """
        with _BG_LOCK:
            return list(_get_bg_state(execution)["errors"])

    def _execute_join_branches(self, join_branches, executor, execution):
        # submit 全部分支后用 ``wait`` 阻塞到所有 future 完成。等所有 future（含
        # 抛错的）完成后再统一收集结果, 避免某分支抛错时 results 缺字段让下游拿
        # 到 KeyError。
        results: Dict[str, Any] = {}
        if not join_branches:
            return results
        lock = executor.lock
        future_to_branch = {
            executor.submit(self.exec_branch, branch, execution): branch for branch in join_branches
        }
        done, _ = executor.wait(future_to_branch)
        for future in done:
            branch = future_to_branch[future]
            self._process_future_result(future, branch, results, lock)
        return results

    def _process_future_result(self, future, branch, results, lock):
        """收集 future 结果。分支抛错时记一个显式错误对象，不静默丢成 None。

        返回的 ``results`` 字典里：成功分支值为子流程产出；失败分支值为
        ``{"__parallel_error__": str(exc), "__branch__": branch.name}`` 这种哨兵，
        下游节点拿到时一眼可识别（不像 None 那样跟合法返回混淆）。
        """
        try:
            result = future.result()
            with lock:
                results[branch.name] = result
        except Exception as exc:
            logger.warning("parallel branch %s raised: %s", branch.name, exc, exc_info=True)
            with lock:
                results[branch.name] = {
                    "__parallel_error__": str(exc),
                    "__branch__": branch.name,
                }

    def thread_execute(self, execution):
        """使用线程池来执行并行节点"""
        return self.pool_execute(THREAD, execution)

    def process_execute(self, execution):
        """使用进程池来执行并行节点。

        **cancel 信号限制**: ``ExecutionContext.__getstate__`` 会把不可 pickle
        的 ``threading.Event`` (cancel_event) 弹掉, 子进程拿到的是全新未触发
        的 Event。父进程的超时取消信号**不会跨进程传播**——如果分支内节点
        需要响应取消 (例如父 flow 已超时), 应改用 ``mode=thread``。该限制在
        ``ParallelExecutor.supports_cancel_propagation`` 上显式声明。

        唯一能做的进程边界检查: 进入 process_execute 时若父进程 cancel_event
        已被 set, 直接放弃启动子进程, 避免无谓开销 (在 ``_build_executor`` 里)。
        """
        return self.pool_execute(PROCESS, execution)

    def coroutine_execute(self, execution):
        """使用协程来执行并行节点。

        历史实现用 ``loop.run_until_complete`` 自驱事件循环——在已有事件循环
        （FastAPI / Starlette / 任何 async 框架）中调用必抛 RuntimeError。

        当前节点的 ``execute`` 是 sync 接口、由 ``run_compatible`` 同步驱动：
        试图在 sync 路径里再嵌套一个事件循环本质上是同步-异步桥接套娃。
        在不引入完整 async strategy 改造的前提下，直接拒绝该模式，避免静默崩。
        需要真并发的用户请用 ``mode=thread`` 或 ``mode=process``。
        """
        raise ValueError(
            "parallel mode='coroutine' is no longer supported in sync execution path: "
            "it nested asyncio.run_until_complete inside the sync bridge and reliably "
            "crashed under any running event loop (FastAPI / async frameworks). "
            "Use mode='thread' or mode='process' for concurrent branch execution."
        )

    def execute(self, execution):
        """执行并行节点, 根据mode来选择执行方式"""
        if self.mode == COROUTINE:
            results = self.coroutine_execute(execution)
        elif self.mode == PROCESS:
            results = self.process_execute(execution)
        elif self.mode == THREAD:
            results = self.thread_execute(execution)
        else:
            raise ValueError(f"Unknown mode: {self.mode}")

        return results

    async def exec_branch_async(self, pb: "ParallelBranch", execution):
        """异步执行单个并行分支。"""
        branch_execution = execution.get_child_execution()
        input_value = execution.evaluate(pb.input)
        rs = await branch_execution.arun_compatible(pb.flow, False, input_value)
        logger.debug("async branch %s executed: %s", pb.name, rs)
        return rs

    async def arun(self, execution):
        """异步执行并行节点。

        - coroutine 模式：用 ``asyncio.gather`` 真并发执行所有 join 分支。
        - thread / process 模式：保持语义兼容，每个分支在线程中执行，
          通过 ``asyncio.to_thread`` 避免阻塞事件循环。
        """
        branches_to_execute = self.match_condition_branches(execution)
        join_branches, background_branches = self._split_branches(branches_to_execute)

        if self.mode == COROUTINE:
            # background branches: fire-and-forget as asyncio tasks
            async def _bg(branch):
                try:
                    await self.exec_branch_async(branch, execution)
                except Exception as exc:
                    logger.warning("async background branch %r raised: %s", branch.name, exc, exc_info=True)

            for branch in background_branches:
                asyncio.ensure_future(_bg(branch))

            # join branches: truly concurrent via gather
            async def _join(branch):
                try:
                    result = await self.exec_branch_async(branch, execution)
                    return branch.name, result
                except Exception as exc:
                    logger.warning("async parallel branch %s raised: %s", branch.name, exc, exc_info=True)
                    return branch.name, {
                        "__parallel_error__": str(exc),
                        "__branch__": branch.name,
                    }

            pairs = await asyncio.gather(*(_join(b) for b in join_branches))
            return {name: result for name, result in pairs}
        else:
            # thread / process: delegate to sync pool_execute in a thread
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.pool_execute, self.mode, execution)

    def match_condition_branches(self, execution):
        """根据条件匹配需要执行的分支"""
        if not self.is_conditional:
            return self.branches
        return [
            b
            for b in self.branches
            if b.condition is None or b.condition.match(execution.context, execution.express_prefix)
        ]
