"""plaita#72：``$PARENT`` 的惰性快照必须是**快照**，不能是 live 视图。

历史实现把父执行的 live ``CheckpointState`` 包进 ``_LazyRootSnapshot``，真正的
物化推迟到子侧首读。该等价性只在「父在子执行存续期内被阻塞」时成立（``InlineFlow``
同步子流程）；``Parallel`` 的 fire-and-forget 后台分支不阻塞父——父在分支提交后
立刻继续推进并写 ``$NODE``，分支稍后的首读会把父的"未来态"读进"快照"。

本文件钉住：构造即冻结（值 + 顶层键集）、后台分支读到的是分支创建时的父状态。
"""

import threading
import time
import unittest
from typing import ClassVar
from unittest.mock import patch

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.state import CheckpointState, _LazyRootSnapshot
from plaita.node import End, Node, Start
from plaita.node.concurrent import Parallel, ParallelBranch
from plaita.node.mock import Mock


def _new_state():
    return CheckpointState(
        prefix="$", input_name="INPUT", parent_name="PARENT",
        node_name="NODE", global_name="GLOBAL", env_name="ENV",
    )


class TestLazyRootSnapshotIsASnapshot(unittest.TestCase):
    def test_snapshot_does_not_see_parent_advance_after_creation(self):
        state = _new_state()
        state.update_node_result("before", "at-branch-creation")

        snapshot = _LazyRootSnapshot(state)
        # 父在快照创建之后继续推进（后台分支场景：父不阻塞）
        state.update_node_result("later", 42)

        self.assertNotIn("later", snapshot["$NODE"],
                         "$PARENT 快照不得包含创建之后写入的节点结果")
        self.assertEqual(snapshot["$NODE"], {"before": "at-branch-creation"})
        # live 父状态当然看得到（证明"父确实推进了"）
        self.assertIn("later", state["$NODE"])

    def test_snapshot_root_key_set_is_frozen_at_construction(self):
        state = _new_state()
        snapshot = _LazyRootSnapshot(state)
        state["$LATE_KEY"] = "after"

        self.assertNotIn("$LATE_KEY", snapshot)
        self.assertNotIn("$LATE_KEY", list(iter(snapshot)))
        self.assertEqual(len(snapshot), 0)

    def test_root_execution_parent_namespace_is_empty_mapping(self):
        """无父执行时 $PARENT 是空 dict（不是 None）——根运行的既有口径。"""
        execution = FlowExecution()
        execution.execute(_build_flows(), {})
        self.assertEqual(execution.context["$PARENT"], {})

    def test_snapshot_first_read_still_isolates_after_parent_advance(self):
        """边界说明：构造时只浅拷顶层，根键的**深物化仍按首读**进行。

        因此「首读之后」的父侧原地写不再影响快照（C1-2 既有保证）；「首读之前」
        父对某个根键值**原地**嵌套写则会透出——**且这是受支持路径可达的，不是
        "不可达窗口"**：节点或表达式用 ``$F.set``/``$F.pop`` 这类 mutate 函数
        原地改活 ``$GLOBAL``（或其它根键值）时，早先创建的（如后台分支）快照
        随后就会读到新值（独立验收已构造出该反例）。引擎自身的**记账**写入
        （``update_node_result`` / ``setup_flow``）换对象而非原地改，只保证那条
        路径不透出。

        也就是说 ``$PARENT`` 承诺的是「顶层键集 + 各根键在创建时的值对象」冻结，
        **不是**嵌套容器的深隔离；要后者需在分支创建时全量深物化，代价是
        while/for 热路径的每轮全量 deepcopy 回归（动机见
        ``plaita/core/state.py::_LazyRootSnapshot``），本单明确不采用。
        """
        state = _new_state()
        state.update_node_result("n", {"deep": [1, 2]})
        snapshot = _LazyRootSnapshot(state)

        self.assertEqual(snapshot["$NODE"]["n"]["deep"], [1, 2])  # 首读物化
        state["$NODE"]["n"]["deep"].append(3)  # 首读之后的原地写

        self.assertEqual(snapshot["$NODE"]["n"]["deep"], [1, 2])


class _SlowNode(Node):
    """睡一小会儿的节点，确保父流程在分支首读之前完成 ``later``。"""

    node_type: ClassVar[str] = "slow_issue72"
    seconds: float = 0.3

    def run(self, execution):
        time.sleep(self.seconds)
        return "slept"


def _build_flows():
    sub = Flow(
        flow_id="bg", version="1.0", runtime="python",
        nodes=[
            Start(id="st", next="slow"),
            _SlowNode(id="slow", next="e"),
            End(id="e", **{"resultType": "success", "output": "$PARENT.$NODE.later"}),
        ],
    )
    main = Flow(
        flow_id="main72", version="1.0", runtime="python",
        nodes=[
            Start(id="s", next="p"),
            Parallel(id="p", branches=[ParallelBranch(name="bg", flow=sub)], next="later"),
            Mock(id="later", value=42, next="e"),
            End(id="e", **{"resultType": "success", "output": "$NODE.later"}),
        ],
    )
    return main


class TestBackgroundBranchParentSnapshot(unittest.TestCase):
    def _run_with_delayed_worker_child_creation(self, flow, execution):
        """跑一遍 flow；若子执行是在**非主线程**里创建的，先延迟 0.6s。

        模拟「分支线程比父流程慢」的调度：父流程会先把 ``later`` 写完。修复后
        后台分支的子执行在父线程、submit 之前创建（延迟分支不触发），快照钉在
        分支创建时；若实现退回到「分支线程里才创建子执行」，延迟让父的未来态
        必然先落地，快照就会读进 ``later``。
        """
        main_thread = threading.current_thread()
        original_get_child = FlowExecution.get_child_execution

        def delayed_get_child(self):
            if threading.current_thread() is not main_thread:
                time.sleep(0.6)
            return original_get_child(self)

        with patch.object(FlowExecution, "get_child_execution", delayed_get_child):
            return execution.execute(flow, {})

    def test_background_branch_reads_parent_at_branch_creation(self):
        """issue 实测三：后台分支求值 $PARENT.$NODE.later 必须为 None（节点尚不存在）。"""
        flow = _build_flows()
        execution = FlowExecution()

        branch_results = {}
        original = Parallel.exec_branch

        def spy(par_self, pb, *args, **kwargs):
            result = original(par_self, pb, *args, **kwargs)
            branch_results[pb.name] = result
            return result

        with patch.object(Parallel, "exec_branch", spy):
            parent_result = self._run_with_delayed_worker_child_creation(flow, execution)

        par = next(n for n in flow.nodes if n.node_type == "parallel")
        stats = par.wait_background_branches(execution, timeout=5)
        deadline = time.time() + 2
        while "bg" not in branch_results and time.time() < deadline:
            time.sleep(0.01)

        self.assertEqual(parent_result, 42)
        self.assertEqual(stats["done"], 1, "后台分支应已执行完成")
        self.assertEqual(branch_results.get("bg"), None,
                         "分支读到的必须是分支创建时的父状态（later 尚不存在）")
        self.assertEqual(par.get_background_errors(execution), [])
        self.assertIn("later", execution.context["$NODE"],
                      "对照：父自己的 live $NODE 必须包含 later")

    def test_join_branch_runs_with_its_own_child_execution(self):
        """join 分支（不传 branch_execution）必须自建子执行并正常求值。

        钉住 ``exec_branch`` 的 ``branch_execution is None`` 分支：把它写反
        （或让就地创建退化成 None）会让 join 分支在 ``None.run_compatible`` 上
        炸成 ``__parallel_error__`` 哨兵，而不是分支结果。
        """
        sub = Flow(
            flow_id="join-body", version="1.0", runtime="python",
            nodes=[
                Start(id="st", next="e"),
                End(id="e", **{"resultType": "success", "output": "JOINED"}),
            ],
        )
        flow = Flow(
            flow_id="main-join", version="1.0", runtime="python",
            nodes=[
                Start(id="s", next="p"),
                Parallel(
                    id="p",
                    branches=[ParallelBranch(name="j", flow=sub)],
                    join_branches=["j"],
                    next="e",
                ),
                End(id="e", **{"resultType": "success", "output": "$NODE.p.j"}),
            ],
        )
        execution = FlowExecution()
        result = execution.execute(flow, {})
        self.assertEqual(result, "JOINED")


if __name__ == "__main__":
    unittest.main()
