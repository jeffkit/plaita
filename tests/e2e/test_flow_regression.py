"""
T097b: JSON flow definition regression test.

Collects representative JSON flow definitions from tests/fixture/ and
tests/integration/server/ and verifies parse + execute round-trip produces
correct results after refactoring.
"""
import json
from pathlib import Path

import pytest

from plaita.core.flow import Flow, parse


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixture"
SERVER_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "integration" / "server"


def _collect_json_flows():
    """Collect JSON flow files that can be parsed without server node types."""
    flows = []
    for d in (FIXTURE_DIR, SERVER_FIXTURE_DIR):
        if d.exists():
            for f in sorted(d.glob("*.json")):
                flows.append(f)
    return flows


class TestFlowDefinitionRegression:
    """Verify JSON flow definitions parse correctly after refactoring."""

    @pytest.fixture(params=_collect_json_flows(), ids=lambda p: p.name)
    def flow_file(self, request):
        return request.param

    def test_parse_json_flow(self, flow_file):
        """Every JSON flow file should parse without error."""
        content = flow_file.read_text(encoding="utf-8")
        data = json.loads(content)

        node_types = {n.get("type") for n in data.get("nodes", [])}
        server_types = {"delay", "redis_queue", "kafka_queue", "http_callback", "approval"}
        if node_types & server_types:
            pytest.skip(f"Flow uses server node types: {node_types & server_types}")

        flow = parse(content)
        assert flow is not None
        assert isinstance(flow, Flow)

    def test_parse_preserves_flow_id(self, flow_file):
        """Parsed flow should have the correct flow_id."""
        content = flow_file.read_text(encoding="utf-8")
        data = json.loads(content)

        node_types = {n.get("type") for n in data.get("nodes", [])}
        server_types = {"delay", "redis_queue", "kafka_queue", "http_callback", "approval"}
        if node_types & server_types:
            pytest.skip(f"Flow uses server node types: {node_types & server_types}")

        flow = parse(content)
        expected_id = data.get("flow_id") or data.get("flowId") or data.get("id")
        assert flow.flow_id == expected_id

    def test_parse_preserves_node_count(self, flow_file):
        """Parsed flow should have the same number of nodes."""
        content = flow_file.read_text(encoding="utf-8")
        data = json.loads(content)

        node_types = {n.get("type") for n in data.get("nodes", [])}
        server_types = {"delay", "redis_queue", "kafka_queue", "http_callback", "approval"}
        if node_types & server_types:
            pytest.skip(f"Flow uses server node types: {node_types & server_types}")

        flow = parse(content)
        assert len(flow.nodes) == len(data.get("nodes", []))

    def test_assignment_flow_execute_roundtrip(self):
        """The assigment.json fixture should parse and execute correctly."""
        fixture = FIXTURE_DIR / "assigment.json"
        if not fixture.exists():
            pytest.skip("assigment.json not found")

        flow = Flow.from_string(fixture.read_text())
        result = flow.run({"bb": "test_value"})
        assert result is not None
        assert result["dd"] == "test_value"

    def test_assignment_flow_branch_path(self):
        """assigment.json: input bb=123 should take the branch path."""
        fixture = FIXTURE_DIR / "assigment.json"
        if not fixture.exists():
            pytest.skip("assigment.json not found")

        flow = Flow.from_string(fixture.read_text())
        result = flow.run({"bb": "123"})
        assert result is not None
        assert result["dd"] == "456"

    def test_from_string_roundtrip(self):
        """Flow.from_string → flow.run should work for inline JSON."""
        inline_json = json.dumps({
            "flow_id": "regression-inline",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "inline-ok", "resultType": "success"},
            ],
        })
        flow = Flow.from_string(inline_json)
        result = flow.run()
        assert result == "inline-ok"

    def test_model_validate_roundtrip(self):
        """Flow.model_validate → flow.run should work for dict input."""
        flow = Flow.model_validate({
            "flow_id": "regression-dict",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "dict-ok", "resultType": "success"},
            ],
        })
        result = flow.run()
        assert result == "dict-ok"

    def test_parse_function_roundtrip(self):
        """parse() → flow.run should work."""
        flow = parse(json.dumps({
            "flow_id": "regression-parse",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "parse-ok", "resultType": "success"},
            ],
        }))
        result = flow.run()
        assert result == "parse-ok"
