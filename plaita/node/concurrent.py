import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Lock as ProcessLock
from threading import Lock
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import Field, model_validator

from plaita.node import Node
from plaita.node.decide import Branch

ARTIFICIAL = "artificial"
COROUTINE = "coroutine"
PROCESS = "process"
THREAD = "thread"

BackGroundThreadPool = ThreadPoolExecutor()
BackGroundProcessPool = ProcessPoolExecutor()


class ParallelBranch(Branch):
    """ParallelBranch represents a branch of a parallel node"""

    flow: Optional[Any] = None
    input: Any = None

    @model_validator(mode="before")
    @classmethod
    def setup_flow(cls, values: Dict) -> Dict:
        from plaita.flow import Flow

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
        """执行并行节点的分支flow"""
        try:
            branch_execution = execution.get_child_execution()
            lazy = execution.mode == "generator"
            input_value = execution.evaluate(pb.input)
            rs = branch_execution.run_compatible(pb.flow, lazy, input_value)
            print(f"branch {pb.name} executed: {rs}")
            return rs
        except Exception as e:
            print(f"branch {pb.name} generated an exception: {e}")
            return None

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
        try:
            result = future.result()
            with lock:
                results[branch.name] = result
        except (Exception, ValueError) as exc:
            print(f"{branch.name} generated an exception: {exc}")

    def thread_execute(self, execution):
        """使用线程池来执行并行节点"""
        return self.pool_execute(THREAD, execution)

    def process_execute(self, execution):
        """使用进程池来执行并行节点"""
        return self.pool_execute(PROCESS, execution)

    def coroutine_execute(self, execution):
        """使用协程来执行并行节点"""
        branches_to_execute = self.match_condition_branches(execution)
        join_branches = [b for b in branches_to_execute if b.name in self.join_branches]

        async def execute_branch(pb: ParallelBranch):
            branch_execution = execution.get_child_execution()
            input_value = execution.evaluate(pb.input)
            lazy = execution.mode == "generator"
            return branch_execution.run_compatible(pb.flow, lazy, input_value)

        async def gather_results():
            tasks = [execute_branch(branch) for branch in join_branches]
            return await asyncio.gather(*tasks)

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        results = loop.run_until_complete(gather_results())
        return {branch.name: result for branch, result in zip(join_branches, results)}

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
