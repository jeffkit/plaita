import atexit
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Lock as ProcessLock
from threading import Lock
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import Field, model_validator

from plaita.node import Node
from plaita.node.decide import Branch

logger = logging.getLogger(__name__)

ARTIFICIAL = "artificial"
COROUTINE = "coroutine"
PROCESS = "process"
THREAD = "thread"

# 后台分支是 fire-and-forget (submit 后不持 future 引用, 不取结果)。模块级
# 池子的两个问题历史遗留: 无 max_workers (默认会按 ``min(32, os.cpu_count()+4)``
# 算 thread 池, 进程池按 cpu 数), 无 shutdown 钩子。pytest/Jupyter/Web 服务
# 进程隐式持有这两个 pool, 解释器退出时若不显式 shutdown, 待办 future 可能丢。
_DEFAULT_BG_THREAD_WORKERS = int(os.environ.get("PLAITA_BG_THREAD_WORKERS", "8"))
_DEFAULT_BG_PROCESS_WORKERS = int(os.environ.get("PLAITA_BG_PROCESS_WORKERS", str(os.cpu_count() or 4)))

BackGroundThreadPool = ThreadPoolExecutor(
    max_workers=_DEFAULT_BG_THREAD_WORKERS,
    thread_name_prefix="plaita-bg-thread",
)
BackGroundProcessPool = ProcessPoolExecutor(
    max_workers=_DEFAULT_BG_PROCESS_WORKERS,
)


def _shutdown_background_pools() -> None:
    """进程退出时显式 shutdown 后台池, 避免待办任务悬挂。"""
    # cancel_futures=True: 解释器已经在退出, 没机会跑排队中的任务, 与其卡住
    # 等, 不如直接丢。已在跑的会被 wait。
    for pool, name in (
        (BackGroundThreadPool, "BackGroundThreadPool"),
        (BackGroundProcessPool, "BackGroundProcessPool"),
    ):
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:  # pragma: no cover - 退出路径上的 best-effort
            logger.debug("%s shutdown raised during atexit", name, exc_info=True)


atexit.register(_shutdown_background_pools)


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
        """
        branch_execution = execution.get_child_execution()
        lazy = execution.mode == "generator"
        input_value = execution.evaluate(pb.input)
        rs = branch_execution.run_compatible(pb.flow, lazy, input_value)
        logger.debug("branch %s executed: %s", pb.name, rs)
        return rs

    def pool_execute(self, pool_type=THREAD, execution=None):
        """使用线程池或进程池来执行并行节点"""
        branches_to_execute = self.match_condition_branches(execution)
        join_branches, background_branches = self._split_branches(branches_to_execute)

        pool = self._create_pool(pool_type)
        lock = self._create_lock(pool_type)

        self._execute_background_branches(background_branches, pool_type, execution)

        results = self._execute_join_branches(join_branches, pool, lock, execution)

        return results

    def _split_branches(self, branches):
        join_branches = [b for b in branches if b.name in self.join_branches]
        background_branches = [b for b in branches if b.name not in self.join_branches]
        return join_branches, background_branches

    def _create_pool(self, pool_type):
        if pool_type == THREAD:
            return ThreadPoolExecutor()
        elif pool_type == PROCESS:
            return ProcessPoolExecutor()
        else:
            raise ValueError(f"Unknown pool type: {pool_type}")

    def _create_lock(self, pool_type):
        return Lock() if pool_type == THREAD else ProcessLock()

    def _execute_background_branches(self, background_branches, pool_type, execution):
        if background_branches:
            bg_pool = BackGroundThreadPool if pool_type == THREAD else BackGroundProcessPool
            for branch in background_branches:
                bg_pool.submit(self.exec_branch, branch, execution)

    def _execute_join_branches(self, join_branches, pool, lock, execution):
        results = {}
        with pool as executor:
            future_to_branch = {
                executor.submit(self.exec_branch, branch, execution): branch for branch in join_branches
            }
            for future in as_completed(future_to_branch):
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
        """使用进程池来执行并行节点"""
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

    def match_condition_branches(self, execution):
        """根据条件匹配需要执行的分支"""
        if not self.is_conditional:
            return self.branches
        return [
            b
            for b in self.branches
            if b.condition is None or b.condition.match(execution.context, execution.express_prefix)
        ]
