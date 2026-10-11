"""回归：structural check 不得因 model_dump 丢失 node_type 而对所有 flow 误报。

背景（2026-10-11）：Node 的 ``node_type`` 是类级 ClassVar，``model_dump()`` 不输出。
旧实现直接 dump 后交 validate_flow_ir，``type=start`` 永远找不到，worker 日志里
对每个 flow 打 ``缺少 type=start 入口节点`` 的假阳性 WARNING。
"""

import json
import logging

from plaita.core.errors import FlowStartMissingError
from plaita.core.flow import Flow


def _structural_warnings(caplog):
    return [r for r in caplog.records if "structural check" in r.getMessage()]


def test_valid_flow_with_start_emits_no_structural_warning(caplog):
    flow = {
        "nodes": [
            {"id": "start", "type": "start", "next": "done"},
            {"id": "done", "type": "end"},
        ]
    }
    with caplog.at_level(logging.WARNING):
        Flow.from_string(json.dumps(flow))
    assert _structural_warnings(caplog) == []


def test_flow_without_start_still_warns(caplog):
    # 无 start 的流程解析期仍会在后续 start_node 访问时抛 FlowStartMissingError，
    # 这里只断言 structural check 的告警仍然发出（告警未被误伤）。
    flow = {"nodes": [{"id": "only", "type": "end"}]}
    with caplog.at_level(logging.WARNING):
        try:
            Flow.from_string(json.dumps(flow))
        except FlowStartMissingError:
            pass
    assert len(_structural_warnings(caplog)) == 1
