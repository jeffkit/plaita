"""test_clean_c2_parallel_input — 清理批次 C2-2 回归。

修复点：Parallel 异步分支（coroutine 模式 → ``exec_branch_async``）此前对
无显式 ``input`` 的分支直接 ``evaluate(None)`` 得 None——sync 路径
（``exec_branch``）有「继承父 $INPUT」兜底，同一 flow sync/async 行为分叉。
现在两条路径共用 ``_resolve_branch_input``。

修复前实证分叉场景：分支子流程的输出是插值字符串（``"title={% $INPUT.title %}"``）
——不以表达式前缀开头的字符串享受不到子流程的父级 fallback，sync 得
``"title=hello"`` 而 async 得 ``"title=None"``。

覆盖：
1. e2e：DSL 风格 PARALLEL（分支无 input）sync(thread)/async(coroutine) 同结果；
2. 显式 input 的分支不受影响；
3. ``_resolve_branch_input`` 单元断言：None input → 求值 ``$INPUT`` 键；
   显式 input → 求值表达式本身（sync/async 两入口都走它）。
"""

from __future__ import annotations

import asyncio
import json
import unittest
from unittest import mock

from plaita.core.flow import Flow
from plaita.io import Property, types
from plaita.node import End, Start
from plaita.node.concurrent import Parallel, ParallelBranch

INPUT = {"title": "hello", "n": 1}


def _sub_flow(output) -> Flow:
    """最小分支子流程：把收到的 INPUT 以指定 output 形状回显。"""
    return Flow(
        flow_id=f"sub-{abs(hash(str(output))) % 10000}",
        version="1",
        runtime="python",
        output_type=Property(data_type=types.OBJECT, is_required=True),
        nodes=[
            Start(id="start", next="end"),
            End(id="end", resultType="success", output=output),
        ],
    )


def _build_main_flow(mode: str) -> Flow:
    """主流程：一个无 input 分支 + 一个显式 input 分支的 PARALLEL 节点。"""
    return Flow(
        flow_id="main-c2-parallel",
        version="1",
        runtime="python",
        output_type=Property(data_type=types.OBJECT, is_required=True),
        nodes=[
            Start(id="start", next="parallel"),
            Parallel(
                id="parallel",
                name="parallel",
                branches=[
                    # 无 input（@flow DSL 编译生成的 PARALLEL 分支形状）
                    {"name": "b_noinput", "flow": _sub_flow("title={% $INPUT.title %}")},
                    # 显式 input：不受修复影响
                    {"name": "b_explicit", "flow": _sub_flow("$INPUT"), "input": {"k": "v"}},
                ],
                mode=mode,
                join_branches=["b_noinput", "b_explicit"],
                next="end",
            ),
            End(id="end", resultType="success", output="$NODE.parallel"),
        ],
    )


class TestParallelBranchInputParity(unittest.TestCase):
    """e2e：无 input 分支 sync/async 同结果（插值场景是修复前的分叉点）。"""

    def test_sync_thread_and_async_coroutine_same_result(self):
        sync_result = _build_main_flow("thread").run(dict(INPUT))
        async_result = asyncio.run(_build_main_flow("coroutine").arun(dict(INPUT)))
        self.assertEqual(sync_result, async_result)
        # 语义断言：无 input 分支拿到父 INPUT（修复前 async 是 "title=None"）
        self.assertEqual(sync_result["b_noinput"], "title=hello")
        # 显式 input 分支不受影响
        self.assertEqual(sync_result["b_explicit"], {"k": "v"})

    def test_async_branch_no_longer_gets_none(self):
        async_result = asyncio.run(_build_main_flow("coroutine").arun(dict(INPUT)))
        self.assertEqual(async_result["b_noinput"], "title=hello")

    def test_dict_output_shape_parity(self):
        """object 形状的 output（字典递归求值）同样 sync/async 同结果。"""
        sub = _sub_flow({"got": "$INPUT"})
        flow = Flow(
            flow_id="main-c2-parallel-obj",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel", name="parallel",
                    branches=[{"name": "b_noinput", "flow": sub}],
                    mode="coroutine",
                    join_branches=["b_noinput"], next="end",
                ),
                End(id="end", resultType="success", output="$NODE.parallel"),
            ],
        )
        sync_flow = Flow(
            flow_id="main-c2-parallel-obj",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.OBJECT, is_required=True),
            nodes=[
                Start(id="start", next="parallel"),
                Parallel(
                    id="parallel", name="parallel",
                    branches=[{"name": "b_noinput", "flow": sub}],
                    mode="thread",
                    join_branches=["b_noinput"], next="end",
                ),
                End(id="end", resultType="success", output="$NODE.parallel"),
            ],
        )
        sync_result = sync_flow.run(dict(INPUT))
        async_result = asyncio.run(flow.arun(dict(INPUT)))
        self.assertEqual(sync_result, async_result)
        self.assertEqual(sync_result["b_noinput"], {"got": INPUT})


class TestResolveBranchInput(unittest.TestCase):
    """单元断言：sync/async 两入口共用 _resolve_branch_input。"""

    def _make_execution(self):
        execution = mock.MagicMock()
        execution.express_prefix = "$"
        execution.express_input_name = "INPUT"
        return execution

    def test_none_input_evaluates_parent_input_key(self):
        p = Parallel(id="p", name="p", mode="coroutine")
        pb = ParallelBranch.model_validate({"name": "b", "flow": {"id": "x", "version": "1", "runtime": "python", "nodes": []}})
        self.assertIsNone(pb.input)
        execution = self._make_execution()
        execution.evaluate.return_value = {"inherited": True}
        value = p._resolve_branch_input(pb, execution)
        execution.evaluate.assert_called_once_with("$INPUT")
        self.assertEqual(value, {"inherited": True})

    def test_explicit_input_evaluated_as_expression(self):
        p = Parallel(id="p", name="p", mode="coroutine")
        pb = ParallelBranch.model_validate({"name": "b", "input": {"k": "v"},
                                            "flow": {"id": "x", "version": "1", "runtime": "python", "nodes": []}})
        execution = self._make_execution()
        execution.evaluate.return_value = {"k": "v"}
        value = p._resolve_branch_input(pb, execution)
        execution.evaluate.assert_called_once_with({"k": "v"})
        self.assertEqual(value, {"k": "v"})

    def test_exec_branch_and_exec_branch_async_share_resolver(self):
        """两个入口都经 _resolve_branch_input（async 不再绕过兜底）。"""
        p = Parallel(id="p", name="p", mode="coroutine")
        pb = ParallelBranch.model_validate({"name": "b", "flow": {"id": "x", "version": "1", "runtime": "python", "nodes": []}})
        execution = self._make_execution()
        execution.evaluate.return_value = 42
        child = mock.MagicMock()
        child.arun_compatible = mock.AsyncMock(return_value="ok")
        execution.get_child_execution.return_value = child
        execution.mode = "normal"

        with mock.patch.object(Parallel, "_resolve_branch_input",
                               wraps=p._resolve_branch_input) as spy:
            p.exec_branch(pb, execution)
            spy.assert_called_once_with(pb, execution)

            async def go():
                await p.exec_branch_async(pb, execution)
            asyncio.run(go())
            self.assertEqual(spy.call_count, 2)
            spy.assert_called_with(pb, execution)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
