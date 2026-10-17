"""plaita#37 —— 引擎侧 token/成本字段 + 终态聚合落盘。

红先证（修复前在 origin/main(87b8ad0) 工作树上实跑，见 /tmp/w9-a-out/RED-PROOF.md）：
- ``ExecutionState`` 无 ``usage``/``cost`` 字段（``"usage" in
  ExecutionState.model_fields`` 为 False）；worker 跑完一条**节点确实返回了
  ``usage``** 的 flow 后，落盘状态里零 usage 字段；
- usage 唯一去处是 ``plaita/obs.py``（→ Langfuse ``usage_details``），
  Langfuse 开关默认关（``auto`` 无 key 即不观测），未启用部署里 token 消耗
  在系统任何地方不可见；
- 每节点结果 ``context["$NODE"][id]["usage"]`` **确实存在**（真源在，只是
  引擎不聚合、不落盘）——这是本切片成立的依据。

本文件守住「引擎自己落 usage」这条竖切：字段可选（老状态零回归）、终态聚合、
**未采集（None）与采集到 0 严格可分**、best-effort（聚合失败不得影响执行）、
resume 不覆写，以及「精度错（ValueError）」与「值不自洽（TokenCountMismatch）」
两类错误可分。
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("cachetools")

from plaita.event.memory import InMemoryEventBus  # noqa: E402
from plaita.server import flow_worker as flow_worker_mod  # noqa: E402
from plaita.server.flow_worker import FlowWorker  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402
from plaita.storage.usage import (  # noqa: E402
    MAX_PRECISION,
    TOKENS_PER_COUNT,
    TokenCountMismatch,
    aggregate_node_usage,
    format_count,
    format_tokens_per_count,
    guard_token_counter,
    merge_usage,
    node_results_from_context,
    normalize_usage,
    ratio_is_consistent,
    validate_count,
)


def _usage_node(node_id: str, *, total: int, next_id: str, model: str = "m1",
                source: str = "observed"):
    return {
        "id": node_id,
        "type": "assignment",
        "output": {
            "model": model,
            "usage": {
                "prompt_tokens": total - 3,
                "completion_tokens": 3,
                "total_tokens": total,
                **({"source": source} if source != "observed" else {}),
            },
            "text": "x",
        },
        "next": next_id,
    }


def _flow(nodes):
    return {
        "flow_id": "usage-flow",
        "name": "usage 流程",
        "version": "1.0.0",
        "nodes": [{"id": "start", "type": "start", "next": nodes[0]["id"]},
                  *nodes,
                  {"id": "end", "type": "end", "output": "success"}],
    }


def _worker(flow):
    execution_storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(flow)
    worker = FlowWorker(
        execution_storage=execution_storage,
        flow_storage=flow_storage,
        event_bus=InMemoryEventBus(),
    )
    return worker, execution_storage


class TestStateSchema:
    def test_usage_defaults_to_none(self):
        assert ExecutionState(execution_id="e", context={}).usage is None

    def test_old_state_dict_without_usage_loads(self):
        dumped = ExecutionState(execution_id="e", context={}).model_dump()
        dumped.pop("usage", None)
        assert ExecutionState(**dumped).usage is None

    def test_usage_round_trips_and_is_json_safe(self):
        usage = {"total_tokens": 18, "llm_calls": 1, "models": ["m1"],
                 "source": "observed", "precision": 2}
        state = ExecutionState(execution_id="e", context={}, usage=usage)
        dumped = json.loads(state.model_dump_json())
        assert dumped["usage"] == usage
        assert ExecutionState(**dumped).usage == usage


class TestNormalizeUsage:
    def test_openai_shape(self):
        assert normalize_usage(
            {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        ) == {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}

    def test_anthropic_shape(self):
        norm = normalize_usage({"input_tokens": 7, "output_tokens": 2})
        assert norm["prompt_tokens"] == 7
        assert norm["completion_tokens"] == 2
        assert norm["total_tokens"] == 9     # total 缺失由 input+output 兜底

    def test_langfuse_shape_is_passthrough(self):
        assert normalize_usage({"input": 1, "output": 2, "total": 3}) == {
            "prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3,
        }

    def test_zero_tokens_is_collected_not_none(self):
        """采集到 0 ≠ 未采集（本单的核心展示口径）。"""
        norm = normalize_usage({"total_tokens": 0})
        assert norm is not None and norm["total_tokens"] == 0

    def test_garbage_is_none(self):
        assert normalize_usage(None) is None
        assert normalize_usage("18") is None
        assert normalize_usage({}) is None

    def test_estimated_source_is_preserved(self):
        norm = normalize_usage({"total_tokens": 4, "source": "estimated"})
        assert norm["source"] == "estimated"


class TestAggregateNodeUsage:
    def test_sums_nodes_models_and_calls(self):
        node_results = {
            "a": {"model": "m1", "usage": {"prompt_tokens": 10, "completion_tokens": 5}},
            "b": {"model": "m2", "usage": {"input_tokens": 4, "output_tokens": 1}},
            "plain": {"value": 1},          # 无 usage 的节点不计数
        }
        usage = aggregate_node_usage(node_results)
        assert usage["prompt_tokens"] == 14
        assert usage["completion_tokens"] == 6
        assert usage["total_tokens"] == 20
        assert usage["llm_calls"] == 2
        assert usage["models"] == ["m1", "m2"]
        assert usage["source"] == "observed"
        # count/tokens_per_count 是 patch_count 的回填语义，聚合阶段不编造
        assert "count" not in usage

    def test_none_when_no_node_reports_usage(self):
        assert aggregate_node_usage({"a": {"value": 1}}) is None
        assert aggregate_node_usage({}) is None
        assert aggregate_node_usage(None) is None

    def test_mixed_source_when_some_estimated(self):
        usage = aggregate_node_usage({
            "a": {"usage": {"total_tokens": 10}},
            "b": {"usage": {"total_tokens": 10, "source": "estimated"}},
        })
        assert usage["source"] == "mixed"

    def test_all_estimated(self):
        usage = aggregate_node_usage({
            "a": {"usage": {"total_tokens": 10, "source": "estimated"}},
        })
        assert usage["source"] == "estimated"

    def test_merge_usage_sums_two_runs(self):
        first = aggregate_node_usage({"a": {"model": "m1", "usage": {"total_tokens": 10}}})
        second = aggregate_node_usage({"b": {"model": "m2", "usage": {"total_tokens": 5}}})
        merged = merge_usage(first, second)
        assert merged["total_tokens"] == 15
        assert merged["llm_calls"] == 2
        assert merged["models"] == ["m1", "m2"]

    def test_node_results_from_context_reads_dollar_node(self):
        assert node_results_from_context({"$NODE": {"a": {"usage": {}}}}) == {"a": {"usage": {}}}
        assert node_results_from_context({"EXPRESS_PREFIX": "$", "$NODE": {"a": 1}}) == {"a": 1}
        assert node_results_from_context(None) == {}
        assert node_results_from_context({"$NODE": "broken"}) == {}


class TestCountGuard:
    """tokens/count 一致性：**精度错（ValueError）** 与 **值不自洽
    （TokenCountMismatch）** 两类错误必须可分——处置动作不同（调用方 bug vs
    数据被改过），混一类会让告警路由错。"""

    def test_token_count_mismatch_is_not_a_value_error(self):
        assert not issubclass(TokenCountMismatch, ValueError), (
            "刻意不继承 ValueError：调用方的 `except ValueError` 不得吞掉"
            "「历史成本被改过」这类需要单独路由的告警"
        )

    def test_consistent_ratio_passes(self):
        assert ratio_is_consistent(total_tokens=40, count=10,
                                   tokens_per_count=TOKENS_PER_COUNT)
        assert validate_count(10, tokens_per_count=4.0, total_tokens=40) == 10

    def test_mismatch_raises_named_error(self):
        with pytest.raises(TokenCountMismatch):
            validate_count(10, tokens_per_count=4.0, total_tokens=100)

    def test_precision_error_is_plain_value_error(self):
        with pytest.raises(ValueError) as ei:
            validate_count(10, tokens_per_count=1 / 3)
        assert not isinstance(ei.value, TokenCountMismatch)
        assert str(MAX_PRECISION) in str(ei.value)

    def test_negative_or_non_finite_tokens_per_count_rejected(self):
        with pytest.raises(ValueError):
            validate_count(10, tokens_per_count=-1.0)
        with pytest.raises(ValueError):
            validate_count(10, tokens_per_count=float("nan"))
        with pytest.raises(ValueError):
            validate_count(10, tokens_per_count=float("inf"))

    def test_count_must_be_non_negative_int(self):
        with pytest.raises(ValueError):
            guard_token_counter(-1)
        with pytest.raises(ValueError):
            guard_token_counter(True)
        with pytest.raises(ValueError):
            guard_token_counter(1.5)
        assert guard_token_counter(0) == 0

    def test_zero_count_skips_ratio_check(self):
        """count=0 时比例无定义，不得判不自洽（未采集/空 run）。"""
        assert validate_count(0, tokens_per_count=4.0, total_tokens=0) == 0


class TestDisplayFormat:
    def test_format_count_distinguishes_uncollected_from_zero(self):
        assert format_count(None) == "未采集"
        assert format_count(0) == "0"
        assert format_count(1234567) == "1,234,567"

    def test_format_count_rejects_negative(self):
        with pytest.raises(ValueError):
            format_count(-1)

    def test_format_tokens_per_count_precision(self):
        assert format_tokens_per_count(None) == "未采集"
        assert format_tokens_per_count(4.0) == "4"
        assert format_tokens_per_count(3.142857) == "3.142857"
        with pytest.raises(ValueError):
            format_tokens_per_count(-0.5)


class TestWorkerPersistsUsage:
    def test_terminal_state_carries_aggregated_usage(self):
        flow = _flow([
            _usage_node("a", total=18, next_id="b", model="m1"),
            _usage_node("b", total=7, next_id="end", model="m2"),
        ])
        worker, storage = _worker(flow)
        result = worker.start_flow(flow_id="usage-flow", params={}, version="1.0.0")
        state = storage.load_execution_state(result["execution_id"])
        assert state.status == "completed"
        usage = state.usage
        assert usage is not None, "终态落盘必须带引擎侧 usage（Langfuse 不是唯一出口）"
        assert usage["total_tokens"] == 25
        assert usage["prompt_tokens"] == 19
        assert usage["completion_tokens"] == 6
        assert usage["llm_calls"] == 2
        assert usage["models"] == ["m1", "m2"]
        assert usage["source"] == "observed"

    def test_uncollected_is_none_not_zero(self):
        flow = _flow([{"id": "assign", "type": "assignment", "output": {"v": 1},
                       "next": "end"}])
        worker, storage = _worker(flow)
        result = worker.start_flow(flow_id="usage-flow", params={}, version="1.0.0")
        state = storage.load_execution_state(result["execution_id"])
        assert state.usage is None, "没有任何节点报 usage ⇒ 未采集（None），绝不是 0"

    def test_collected_zero_is_not_none(self):
        flow = _flow([{
            "id": "zero", "type": "assignment",
            "output": {"model": "m1", "usage": {"total_tokens": 0}, "text": ""},
            "next": "end",
        }])
        worker, storage = _worker(flow)
        result = worker.start_flow(flow_id="usage-flow", params={}, version="1.0.0")
        state = storage.load_execution_state(result["execution_id"])
        assert state.usage is not None
        assert state.usage["total_tokens"] == 0
        assert state.usage["llm_calls"] == 1

    def test_usage_not_written_before_terminal(self):
        """非终态不得落 usage：半截数字被当成最终成本比没有数字更坏。"""
        worker, storage = _worker(_flow([
            _usage_node("a", total=18, next_id="end"),
        ]))
        state = ExecutionState(execution_id="e1", context={
            "$NODE": {"a": {"model": "m1", "usage": {"total_tokens": 18}}}
        }, status="running")
        worker._collect_usage("e1", state)
        assert state.usage is None

    def test_existing_usage_is_never_overwritten(self):
        """resume 不覆写（与 node_timings/flow_hash 同款零回归套路）。"""
        worker, storage = _worker(_flow([
            _usage_node("a", total=18, next_id="end"),
        ]))
        marker = {"total_tokens": 999, "source": "observed"}
        state = ExecutionState(execution_id="e1", context={
            "$NODE": {"a": {"model": "m1", "usage": {"total_tokens": 18}}}
        }, status="completed", usage=marker)
        worker._collect_usage("e1", state)
        assert state.usage == marker

    def test_aggregation_failure_never_breaks_execution(self, monkeypatch, caplog):
        """负控：落盘口径的 best-effort——聚合抛错必须响亮点名，但执行照常终态。"""
        def _boom(_node_results):
            raise RuntimeError("aggregate exploded")

        monkeypatch.setattr(flow_worker_mod, "aggregate_node_usage", _boom)
        worker, storage = _worker(_flow([
            _usage_node("a", total=18, next_id="end"),
        ]))
        with caplog.at_level("WARNING"):
            result = worker.start_flow(flow_id="usage-flow", params={}, version="1.0.0")
        state = storage.load_execution_state(result["execution_id"])
        assert state.status == "completed", "聚合失败绝不得影响执行结果"
        assert state.usage is None
        assert any("usage" in r.message.lower() or "usage" in r.getMessage().lower()
                   for r in caplog.records), "失败必须响亮（禁止静默）"

    def test_worker_without_execution_keeps_state_untouched(self):
        worker, _ = _worker(_flow([_usage_node("a", total=18, next_id="end")]))
        state = ExecutionState(execution_id="ghost", context={})
        worker._collect_usage("ghost", state)
        assert state.usage is None
