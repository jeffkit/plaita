"""Tests for plaita.core.flow — Flow model, unified identifier, canonical parse."""

import json
import warnings
import unittest

from plaita.core.flow import Flow, parse, parse_and_run


class TestUnifiedFlowIdentifier(unittest.TestCase):
    """T080: flow_id is canonical, id property emits DeprecationWarning,
    model_validator accepts id/flowId/flow_id as input."""

    def test_flow_id_is_canonical_field(self):
        flow = Flow(flow_id="my-flow", nodes=[])
        self.assertEqual(flow.flow_id, "my-flow")

    def test_id_property_returns_flow_id_with_deprecation_warning(self):
        flow = Flow(flow_id="my-flow", nodes=[])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            value = flow.id
            self.assertEqual(value, "my-flow")
            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            self.assertGreaterEqual(len(deprecation_warnings), 1)
            self.assertIn("flow_id", str(deprecation_warnings[0].message))

    def test_model_validator_normalizes_id_to_flow_id(self):
        data = {"id": "from-id", "nodes": []}
        flow = Flow.model_validate(data)
        self.assertEqual(flow.flow_id, "from-id")

    def test_model_validator_normalizes_flowId_to_flow_id(self):
        data = {"flowId": "from-flowId", "nodes": []}
        flow = Flow.model_validate(data)
        self.assertEqual(flow.flow_id, "from-flowId")

    def test_model_validator_flow_id_direct(self):
        data = {"flow_id": "direct", "nodes": []}
        flow = Flow.model_validate(data)
        self.assertEqual(flow.flow_id, "direct")

    def test_flow_id_priority_over_id(self):
        data = {"flow_id": "canonical", "id": "legacy", "nodes": []}
        flow = Flow.model_validate(data)
        self.assertEqual(flow.flow_id, "canonical")

    def test_json_with_id_field_parses(self):
        json_str = json.dumps({"id": "json-id", "nodes": []})
        flow = Flow.from_string(json_str)
        self.assertEqual(flow.flow_id, "json-id")

    def test_json_with_flowId_field_parses(self):
        json_str = json.dumps({"flowId": "json-flowId", "nodes": []})
        flow = Flow.from_string(json_str)
        self.assertEqual(flow.flow_id, "json-flowId")

    def test_id_property_not_a_separate_field(self):
        model_fields = Flow.model_fields
        self.assertNotIn("id", model_fields, "id should be a property, not a Pydantic field")


class TestCanonicalParse(unittest.TestCase):
    """T083: Flow.model_validate, Flow.from_string, parse_and_run all work
    consistently using the model_validator as canonical entry point."""

    def test_model_validate_dict(self):
        data = {"flow_id": "mv-test", "nodes": []}
        flow = Flow.model_validate(data)
        self.assertEqual(flow.flow_id, "mv-test")
        self.assertIsInstance(flow, Flow)

    def test_from_string(self):
        json_str = json.dumps({"flow_id": "fs-test", "nodes": []})
        flow = Flow.from_string(json_str)
        self.assertEqual(flow.flow_id, "fs-test")
        self.assertIsInstance(flow, Flow)

    def test_parse_with_dict(self):
        data = {"flow_id": "parse-dict", "nodes": []}
        flow = parse(data)
        self.assertIsNotNone(flow)
        self.assertEqual(flow.flow_id, "parse-dict")

    def test_parse_with_json_string(self):
        json_str = json.dumps({"flow_id": "parse-str", "nodes": []})
        flow = parse(json_str)
        self.assertIsNotNone(flow)
        self.assertEqual(flow.flow_id, "parse-str")

    def test_parse_with_id_key(self):
        data = {"id": "legacy-id", "nodes": []}
        flow = parse(data)
        self.assertIsNotNone(flow)
        self.assertEqual(flow.flow_id, "legacy-id")

    def test_parse_with_flowId_key(self):
        data = {"flowId": "camel-id", "nodes": []}
        flow = parse(data)
        self.assertIsNotNone(flow)
        self.assertEqual(flow.flow_id, "camel-id")

    def test_parse_returns_none_for_empty(self):
        self.assertIsNone(parse(None))
        self.assertIsNone(parse(""))

    def test_parse_invalid_json_raises(self):
        with self.assertRaises(RuntimeError):
            parse("not-valid-json")

    def test_model_validate_and_parse_give_same_flow_id(self):
        data = {"flowId": "consistency-test", "nodes": []}
        flow_mv = Flow.model_validate(data)
        flow_parse = parse(data)
        self.assertEqual(flow_mv.flow_id, flow_parse.flow_id)


if __name__ == "__main__":
    unittest.main()
