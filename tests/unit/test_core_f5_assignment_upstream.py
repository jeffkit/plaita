"""core-F5（P2）：assignment 多 upstream 无命中静默产出空列表 ``[]``。

审计文档 ``local/plaita-audit-20261017.md`` P2 执行正确性：
``Assignment.execute`` 在 ``len(upstream_output) > 1`` 时按
``state.last_node_id`` 过滤；无命中时 ``value`` 停留在过滤结果 ``[]``，随后
``execution.evaluate([])``。空列表若通过 ``output_type``（如 array/any），
就会被当作合法结果下发——下游无法区分「合法空结果」与「上游根本没有命中」。

修复：多 upstream 无命中 → 具名 :class:`AssignmentError`（``ValueError``
子类），消息带节点 id、当前上游、声明列表；**不调用** ``evaluate``。
单 upstream、命中路径、数据/映射/索引/``$F.*`` 表达式求值面全部不变。

负控：还原「无命中保持 []」→ 本文件 no-match 用例红
（``DID NOT RAISE AssignmentError``）。
"""
from unittest.mock import MagicMock

import pytest

from plaita.core import types
from plaita.io import Property
from plaita.node.assignment import Assignment, AssignmentError

NODE_ID = "asg-join"


def _prop(dtype=types.ARRAY):
    return Property(data_type=dtype)


def _multi_node(output_type=None):
    return Assignment(
        id=NODE_ID,
        output_type=output_type or _prop(),
        upstream_output=[
            {"upstream": "u1", "value": "$NODE.u1"},
            {"upstream": "u2", "value": "$NODE.u2"},
        ],
    )


class TestMultiUpstreamNoMatch:
    def test_no_match_raises_named_error(self):
        """双 upstream + last_node_id 不在声明列表 → AssignmentError，且不
        进入求值（旧实现的 `evaluate([])` 正是静默 [] 的来源）。"""
        node = _multi_node(_prop(types.ARRAY))
        execution = MagicMock()
        execution.state.last_node_id = "u3"
        # 旧行为：过滤结果 [] 被原样交给 evaluate，array 类型校验通过后下发。
        execution.evaluate.return_value = []

        with pytest.raises(AssignmentError) as ei:
            node.execute(execution)

        msg = str(ei.value)
        assert NODE_ID in msg
        assert "u3" in msg
        assert "u1" in msg and "u2" in msg
        assert "[]" in msg
        execution.evaluate.assert_not_called()

    def test_error_is_value_error_subclass(self):
        """具名但不破坏既有 ``except ValueError`` 处理面。"""
        assert issubclass(AssignmentError, ValueError)

    def test_missing_last_node_id_also_rejected(self):
        """last_node_id 为空（配置/编排异常）同样不得静默 []。"""
        node = _multi_node()
        execution = MagicMock()
        execution.state.last_node_id = None

        with pytest.raises(AssignmentError):
            node.execute(execution)

        execution.evaluate.assert_not_called()


class TestLegalSurfacesUnchanged:
    def test_single_upstream_unchanged(self):
        node = Assignment(
            id="asg-single",
            output_type=_prop(types.STRING),
            upstream_output=[{"upstream": "u", "value": "$NODE.u"}],
        )
        execution = MagicMock()
        execution.evaluate.return_value = "X"

        assert node.execute(execution) == "X"
        execution.evaluate.assert_called_once_with("$NODE.u")

    def test_multi_upstream_match_selects_last_node(self):
        node = _multi_node(_prop(types.STRING))
        execution = MagicMock()
        execution.state.last_node_id = "u2"
        execution.evaluate.return_value = "B"

        assert node.execute(execution) == "B"
        execution.evaluate.assert_called_once_with("$NODE.u2")

    @pytest.mark.parametrize(
        "raw,evaluated",
        [
            ({"a": [1, 2]}, {"a": [1, 2]}),          # 数据/映射
            ("$F.index($NODE.u2, 'x')", 7),          # 索引 / $F.* 表达式
            ("$NODE.u2", [1, 2, 3]),                 # 数据列表
        ],
        ids=["mapping", "function-index", "data-list"],
    )
    def test_match_value_surface_unchanged(self, raw, evaluated):
        """命中路径的 value 必须原样交给 evaluate，返回值原样下发——数据/
        映射/索引/``$F.*`` 均不受本次修复影响。"""
        node = Assignment(
            id=NODE_ID,
            output_type=_prop(types.ANY),
            upstream_output=[
                {"upstream": "u1", "value": "unused"},
                {"upstream": "u2", "value": raw},
            ],
        )
        execution = MagicMock()
        execution.state.last_node_id = "u2"
        execution.evaluate.return_value = evaluated

        assert node.execute(execution) == evaluated
        execution.evaluate.assert_called_once_with(raw)
