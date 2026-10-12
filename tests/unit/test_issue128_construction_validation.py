"""plaita#128 回归：两条构造路径的**构建期节点校验必须一致**。

发现的经过值得记一笔：本 issue 最初的归因（"validate 循环误入 `if changed:` 块内"）
**是错的**——回查引进该循环的提交 `91a1806` 与本 issue 的基线 `0bc9e4b`，循环**始终在
块外**，是提报时读错了代码。

真正的成因：`resolve_nodes()` 是**唯一**执行 `node.validate()` 的地方，而
**直接构造 `Flow(nodes=[...])` 从不调用它**（只有 `model_validate` /
`model_validate_json` 在 `plaita/core/flow.py` 显式调一次）。于是"传 `Node` 实例"
这条主路径（builder / `@flow` 前端 / 大量既有测试）坏配置**连 warning 都没有**，
比传 dict 那条路更哑——与 `resolve_nodes` 注释里声称要修的"一条路响一条路哑"同构。

本文件钉住三件事：
1. 传 `Node` 实例构造 `Flow(...)` 时，坏配置**必须**产生校验 warning；
2. 传 dict 走 `model_validate` 时，**同样**产生 warning，且**只产生一次**（不重复告警）；
3. 合法配置两条路都**不得**产生校验 warning。
"""

import json
import logging

import pytest

from plaita.core.flow import Flow
from plaita.node import End, Start
from plaita.node.assignment import Assignment

_MARK = "failed construction-time validation"


def _bad_nodes():
    """一个违反 Assignment 必填校验的节点（无 output / 无 upstream_output）。"""
    return [
        Start(id="s", next="bad"),
        Assignment(id="bad", outputType={"dataType": "string"}, next="e"),
        End(id="e", **{"resultType": "success", "output": "$NODE.bad"}),
    ]


def _good_nodes():
    return [
        Start(id="s", next="a"),
        Assignment(id="a", output="x", outputType={"dataType": "string"}, next="e"),
        End(id="e", **{"resultType": "success", "output": "$NODE.a"}),
    ]


def _bad_flow_dict():
    return {
        "flow_id": "t128",
        "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "bad"},
            {"type": "assignment", "id": "bad",
             "outputType": {"dataType": "string"}, "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.bad"},
        ],
    }


def _capture(caplog, fn):
    with caplog.at_level(logging.WARNING, logger="plaita"):
        fn()
    return [r.getMessage() for r in caplog.records if _MARK in r.getMessage()]


class TestNodeInstancePathIsValidated:
    """核心：#128 之前这条路径完全不校验。"""

    def test_node_instance_construction_warns(self, caplog):
        msgs = _capture(caplog, lambda: Flow(
            flow_id="t128i", version="1.0", runtime="python", nodes=_bad_nodes(),
        ))
        assert msgs, "传 Node 实例构造 Flow(...) 必须跑构建期校验并告警"
        assert "bad" in msgs[0], f"告警应点名问题节点：{msgs[0]}"

    def test_node_instance_good_config_does_not_warn(self, caplog):
        msgs = _capture(caplog, lambda: Flow(
            flow_id="t128g", version="1.0", runtime="python", nodes=_good_nodes(),
        ))
        assert not msgs, f"合法配置不应告警：{msgs}"


class TestBothPathsAgree:
    """两条路径的校验能力必须一致（这就是本 single 的"一条路响一条路哑"）。"""

    def test_dict_path_warns(self, caplog):
        msgs = _capture(caplog, lambda: Flow.model_validate(_bad_flow_dict()))
        assert msgs, "dict 路径（model_validate）必须告警"

    def test_dict_path_warns_exactly_once(self, caplog):
        """#128 的补跑不得让 dict 路径重复告警（否则降级日志会翻倍）。"""
        msgs = _capture(caplog, lambda: Flow.model_validate(_bad_flow_dict()))
        assert len(msgs) == 1, f"dict 路径应恰好告警一次，实际 {len(msgs)}：{msgs}"

    def test_both_paths_produce_the_same_signal(self, caplog):
        inst = _capture(caplog, lambda: Flow(
            flow_id="t128i", version="1.0", runtime="python", nodes=_bad_nodes(),
        ))
        dicts = _capture(caplog, lambda: Flow.model_validate(_bad_flow_dict()))
        assert bool(inst) == bool(dicts), (
            f"两条构造路径的校验信号必须一致：instances={inst}, dict={dicts}"
        )


class TestValidationNeverBlocksConstruction:
    """校验是 warning 降级——坏配置不得让构造抛错（既有策略，勿改）。"""

    def test_construction_does_not_raise_on_bad_config(self):
        flow = Flow(flow_id="t128nb", version="1.0", runtime="python", nodes=_bad_nodes())
        assert flow is not None

    def test_validator_swallows_internal_errors(self, caplog):
        """`validate()` 自身抛非预期异常时也不得让构造失败。"""

        class _Exploding(Start):
            def validate(self):  # type: ignore[override]
                raise RuntimeError("boom")

        flow = Flow(
            flow_id="t128x", version="1.0", runtime="python",
            nodes=[_Exploding(id="s", next="e"),
                   End(id="e", **{"resultType": "success"})],
        )
        assert flow is not None

    def test_validate_nodes_is_public_and_reusable(self, caplog):
        """抽出 `validate_nodes()` 后应可对任意节点集合复用。"""
        flow = Flow(flow_id="t128r", version="1.0", runtime="python", nodes=_good_nodes())
        msgs = _capture(caplog, lambda: flow.validate_nodes([
            Assignment(id="bad2", outputType={"dataType": "string"}),
        ]))
        assert msgs and "bad2" in msgs[0]
