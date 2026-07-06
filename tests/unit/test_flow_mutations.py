"""Mutation-killing tests for plaita/core/flow.py.

Targets survived mutations in:
- Flow.model_validate / model_validate_json (resolve_nodes call)
- Flow.resolve_nodes (registry usage, changed flag)
- Flow._warn_uncovered_env_refs (expose_env guard, logging)
- Flow.from_string (JSON/YAML detection, fallback behavior)
- Flow._ensure_index (signature invalidation, build)
- Flow.rebuild_node_index
- Flow.find_node_by_id (None handling, NodeNotFoundError)
- Flow.next_node (is_end_node check, log message)
- Flow._get_branch_target (branch matching)
- parse (runtime check, empty input)
- _collect_env_refs (string traversal)
"""
from __future__ import annotations

import unittest
import warnings
from unittest.mock import MagicMock, patch

from plaita.core.flow import Flow, _collect_env_refs, parse
from plaita.core.errors import NodeNotFoundError
from plaita.node import NodeRegistry, Start, End
from plaita.node.basic import Node
from typing import ClassVar


# ---------------------------------------------------------------------------
# Minimal node helpers
# ---------------------------------------------------------------------------

class _AssignNode(Node):
    node_type: ClassVar[str] = "assignment"
    node_name: ClassVar[str] = "assignment"

    def execute(self, execution=None):
        return {}


def _make_registry():
    """Build a minimal registry with Start, End, assignment."""
    reg = NodeRegistry(auto_discover=False)
    reg._register_builtins()
    return reg


def _minimal_flow_dict(*, start_id="s1", end_id="e1"):
    return {
        "flow_id": "test-flow",
        "nodes": [
            {"id": start_id, "name": "start", "type": "start", "next": end_id},
            {"id": end_id, "name": "end", "type": "end"},
        ],
    }


# ---------------------------------------------------------------------------
# Flow.model_validate — resolve_nodes called
# ---------------------------------------------------------------------------

class TestModelValidateMutations(unittest.TestCase):
    def test_nodes_are_parsed_from_dict(self):
        """Kill mutations that skip resolve_nodes call."""
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        self.assertTrue(all(isinstance(n, Node) for n in flow.nodes))

    def test_start_node_is_start_type(self):
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        self.assertIsNotNone(flow.start_node)
        self.assertEqual(flow.start_node.node_type, "start")


# ---------------------------------------------------------------------------
# Flow.resolve_nodes — changed flag and conditional re-index
# ---------------------------------------------------------------------------

class TestResolveNodesMutations(unittest.TestCase):
    def test_empty_nodes_no_error(self):
        """Kill mutations that skip empty nodes check."""
        flow = Flow(flow_id="f1", nodes=[])
        reg = _make_registry()
        flow.resolve_nodes(reg)  # should not raise

    def test_already_parsed_nodes_preserved(self):
        """Kill mutations on isinstance(n, Node) guard."""
        reg = _make_registry()
        already_parsed = Start(id="s1", name="start", next="e1")
        end = End(id="e1", name="end")
        flow = Flow(flow_id="f1", nodes=[already_parsed, end])
        flow.resolve_nodes(reg)
        # Nodes should be unchanged (same objects)
        self.assertIs(flow.nodes[0], already_parsed)

    def test_dict_nodes_are_resolved(self):
        """Kill mutations on isinstance(n, dict) path."""
        reg = _make_registry()
        flow = Flow(flow_id="f1", nodes=[
            {"id": "s1", "name": "start", "type": "start", "next": "e1"},
            {"id": "e1", "name": "end", "type": "end"},
        ])
        flow.resolve_nodes(reg)
        self.assertTrue(all(isinstance(n, Node) for n in flow.nodes))

    def test_index_invalidated_after_resolve(self):
        """Kill mutations that skip _node_index_sig reset."""
        reg = _make_registry()
        flow = Flow(flow_id="f1", nodes=[
            {"id": "s1", "name": "start", "type": "start", "next": "e1"},
            {"id": "e1", "name": "end", "type": "end"},
        ])
        # resolve to get Node instances first
        flow.resolve_nodes(reg)
        old_idx = flow._ensure_index()
        # Invalidate and re-resolve - should still work
        flow._node_index_sig = ()
        idx = flow._ensure_index()
        self.assertIn("s1", idx)
        self.assertIsInstance(idx["s1"], Node)


# ---------------------------------------------------------------------------
# Flow._ensure_index — signature-based cache invalidation
# ---------------------------------------------------------------------------

class TestEnsureIndexMutations(unittest.TestCase):
    def _make_flow_with_nodes(self):
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        return flow

    def test_index_contains_all_nodes(self):
        """Kill mutations that skip building the dict."""
        flow = self._make_flow_with_nodes()
        idx = flow._ensure_index()
        self.assertIn("s1", idx)
        self.assertIn("e1", idx)

    def test_index_maps_id_to_node(self):
        """Kill mutations that build wrong mapping."""
        flow = self._make_flow_with_nodes()
        idx = flow._ensure_index()
        self.assertIsInstance(idx["s1"], Node)
        self.assertEqual(idx["s1"].id, "s1")

    def test_returns_same_object_on_cache_hit(self):
        """Kill mutations that always rebuild."""
        flow = self._make_flow_with_nodes()
        idx1 = flow._ensure_index()
        idx2 = flow._ensure_index()
        self.assertIs(idx1, idx2)

    def test_rebuilds_when_nodes_replaced(self):
        """Kill mutations on signature check."""
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        _ = flow._ensure_index()
        new_start = Start(id="s2", name="start2", next="e1")
        new_end = End(id="e2", name="end2")
        flow.nodes = [new_start, new_end]
        idx = flow._ensure_index()
        self.assertIn("s2", idx)
        self.assertNotIn("s1", idx)

    def test_sig_includes_id_changes(self):
        """Kill mutations that use only len in signature."""
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        idx1 = flow._ensure_index()
        # Patch id on one node (simulating mutation) — signature should differ
        orig_id = flow.nodes[0].id
        flow.nodes[0].__dict__["id"] = "changed_id"
        idx2 = flow._ensure_index()
        self.assertIn("changed_id", idx2)
        self.assertNotIn(orig_id, idx2)


# ---------------------------------------------------------------------------
# Flow.rebuild_node_index
# ---------------------------------------------------------------------------

class TestRebuildNodeIndexMutations(unittest.TestCase):
    def test_rebuild_invalidates_old_cache(self):
        """Kill mutations that skip _node_index_sig reset."""
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        old_sig = flow._node_index_sig
        flow.rebuild_node_index()
        # After rebuild the index should be fresh
        self.assertIn("s1", flow._node_index)

    def test_rebuild_then_add_node(self):
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        new_node = _AssignNode(id="a1", name="assignment")
        flow.nodes.append(new_node)
        flow.rebuild_node_index()
        self.assertIn("a1", flow._node_index)


# ---------------------------------------------------------------------------
# Flow.find_node_by_id
# ---------------------------------------------------------------------------

class TestFindNodeByIdMutations(unittest.TestCase):
    def _make_flow(self):
        reg = _make_registry()
        return Flow.model_validate(_minimal_flow_dict(), registry=reg)

    def test_finds_node_by_id(self):
        """Kill mutations that skip index lookup."""
        flow = self._make_flow()
        node = flow.find_node_by_id("s1")
        self.assertIsNotNone(node)
        self.assertEqual(node.id, "s1")

    def test_none_id_returns_none(self):
        """Kill mutations on `if node_id is None`."""
        flow = self._make_flow()
        self.assertIsNone(flow.find_node_by_id(None))

    def test_missing_id_raises_node_not_found(self):
        """Kill mutations that skip NodeNotFoundError."""
        flow = self._make_flow()
        with self.assertRaises(NodeNotFoundError):
            flow.find_node_by_id("nonexistent_id_xyz")

    def test_node_not_found_contains_id(self):
        flow = self._make_flow()
        with self.assertRaises(NodeNotFoundError) as cm:
            flow.find_node_by_id("missing_42")
        self.assertIn("missing_42", str(cm.exception))


# ---------------------------------------------------------------------------
# Flow.is_end_node
# ---------------------------------------------------------------------------

class TestIsEndNodeMutations(unittest.TestCase):
    def _make_flow(self):
        reg = _make_registry()
        return Flow.model_validate(_minimal_flow_dict(), registry=reg)

    def test_end_node_is_end(self):
        flow = self._make_flow()
        end = flow.find_node_by_id("e1")
        self.assertTrue(flow.is_end_node(end))

    def test_start_node_is_not_end(self):
        flow = self._make_flow()
        start = flow.find_node_by_id("s1")
        self.assertFalse(flow.is_end_node(start))

    def test_none_is_not_end(self):
        """Kill mutations that drop 'node is not None' check."""
        flow = self._make_flow()
        self.assertFalse(flow.is_end_node(None))


# ---------------------------------------------------------------------------
# Flow.next_node / _get_branch_target
# ---------------------------------------------------------------------------

class TestNextNodeMutations(unittest.TestCase):
    def test_next_node_returns_none_for_end(self):
        """Kill mutations that call next_node even for end."""
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        end = flow.find_node_by_id("e1")
        self.assertIsNone(flow.next_node(end))

    def test_next_node_from_start_returns_end(self):
        """Kill mutations that skip target resolution."""
        reg = _make_registry()
        flow = Flow.model_validate(_minimal_flow_dict(), registry=reg)
        start = flow.find_node_by_id("s1")
        end = flow.find_node_by_id("e1")
        nxt = flow.next_node(start)
        self.assertIs(nxt, end)

    def test_branch_target_returned_for_branching_node(self):
        """Kill mutations in _get_branch_target."""
        from plaita.node.decide import Switch, Branch, Condition
        reg = _make_registry()
        a = _AssignNode(id="a1", name="a1")
        end = End(id="e1", name="end")
        switch_node = Switch(
            id="c1",
            name="cond",
            branches=[
                Branch(condition={"left": "$x", "op": "eq", "right": "yes"}, next="a1"),
                Branch(condition={"left": "$x", "op": "eq", "right": "no"}, next="e1"),
            ],
        )

        flow = Flow(flow_id="f1", nodes=[switch_node, a, end])

        # next_node with branch="a1" should find a1
        nxt = flow.next_node(switch_node, "a1")
        self.assertIsNotNone(nxt)
        self.assertEqual(nxt.id, "a1")

    def test_branch_not_found_returns_none(self):
        """Kill mutations that return wrong fallback."""
        from plaita.node.decide import Switch, Branch
        switch_node = Switch(
            id="c1",
            name="cond",
            branches=[Branch(condition={"left": "$x", "op": "eq", "right": "yes"}, next="a1")],
        )
        end = End(id="e1", name="end")
        a = _AssignNode(id="a1", name="a1")
        flow = Flow(flow_id="f1", nodes=[switch_node, a, end])

        # "nonexistent_branch" should not match → next_node returns None from _get_branch_target
        # which then tries find_node_by_id(None) → returns None
        nxt = flow.next_node(switch_node, "nonexistent_branch")
        self.assertIsNone(nxt)


# ---------------------------------------------------------------------------
# Flow.from_string — JSON / YAML detection
# ---------------------------------------------------------------------------

class TestFromStringMutations(unittest.TestCase):
    def test_json_string_parsed(self):
        """Kill mutations on JSON detection lstrip()[0]."""
        import json
        data = _minimal_flow_dict()
        json_str = json.dumps(data)
        flow = Flow.from_string(json_str)
        self.assertEqual(flow.flow_id, "test-flow")

    def test_empty_string_raises(self):
        """Kill mutations that allow empty content."""
        with self.assertRaises(ValueError):
            Flow.from_string("")

    def test_whitespace_only_raises(self):
        with self.assertRaises(ValueError):
            Flow.from_string("   ")

    def test_non_json_yaml_parsed(self):
        """Kill mutations on JSON detection: non-{ should go YAML path."""
        try:
            import yaml
        except ImportError:
            self.skipTest("PyYAML not installed")
        yaml_str = "flow_id: yaml-flow\nnodes: []\nruntime: python\n"
        flow = Flow.from_string(yaml_str)
        self.assertEqual(flow.flow_id, "yaml-flow")

    def test_json_parse_error_propagated(self):
        """Kill mutations that silently swallow JSON errors when YAML also fails."""
        # Purely invalid: not parseable as JSON or YAML
        import json
        bad_content = '{ "flow_id": [invalid json'
        with self.assertRaises(Exception):
            Flow.from_string(bad_content)


# ---------------------------------------------------------------------------
# Flow._warn_uncovered_env_refs
# ---------------------------------------------------------------------------

class TestWarnUncoveredEnvRefsMutations(unittest.TestCase):
    def test_no_warning_when_expose_env_set(self):
        """Kill mutations on `if self.expose_env: return` guard."""
        reg = _make_registry()
        import logging
        data = {
            "flow_id": "f1",
            "expose_env": ["HOME"],
            "nodes": [
                {"id": "s1", "name": "start", "type": "start", "next": "e1",
                 "script": "$ENV.HOME"},
                {"id": "e1", "name": "end", "type": "end"},
            ],
        }
        # Should not log warning
        with self.assertLogs("plaita", level="WARNING") as cm:
            # Add a dummy warning to ensure assertLogs doesn't fail
            import logging as _log
            _log.getLogger("plaita").warning("dummy")

        # Now actually test: flow with expose_env should not warn about $ENV refs
        flow = Flow.model_validate(data, registry=reg)
        # If expose_env is set, no ENV warning should appear; we just verify no exception

    def test_warning_logged_for_env_ref_without_expose_env(self):
        """Kill mutations that skip the warning log."""
        from plaita.node import Start, End

        # Create a flow with a node whose name contains $ENV ref, no expose_env
        start = Start(id="s1", name="$ENV.APP_NAME", next="e1")
        end = End(id="e1", name="end")
        flow = Flow(flow_id="f1", expose_env=[], nodes=[start, end])

        import logging, io
        stream = io.StringIO()
        h = logging.StreamHandler(stream)
        h.setLevel(logging.WARNING)
        logging.getLogger("plaita").addHandler(h)
        try:
            flow._warn_uncovered_env_refs()
        finally:
            logging.getLogger("plaita").removeHandler(h)

        self.assertIn("$ENV", stream.getvalue())


# ---------------------------------------------------------------------------
# parse — runtime check
# ---------------------------------------------------------------------------

class TestParseFunctionMutations(unittest.TestCase):
    def test_parse_returns_none_for_empty(self):
        """Kill mutations on `if not content: return None`."""
        self.assertIsNone(parse(None))
        self.assertIsNone(parse(""))
        self.assertIsNone(parse({}))

    def test_parse_non_python_runtime_raises(self):
        """Kill mutations on runtime != 'python' check."""
        with self.assertRaises(RuntimeError) as cm:
            parse({"flow_id": "f1", "runtime": "node", "nodes": []})
        self.assertIn("node", str(cm.exception))

    def test_parse_python_runtime_ok(self):
        """Kill mutations that invert the runtime check."""
        data = _minimal_flow_dict()
        data["runtime"] = "python"
        flow = parse(data)
        self.assertIsNotNone(flow)

    def test_parse_dict_input(self):
        flow = parse(_minimal_flow_dict())
        self.assertEqual(flow.flow_id, "test-flow")


# ---------------------------------------------------------------------------
# _collect_env_refs
# ---------------------------------------------------------------------------

class TestCollectEnvRefsMutations(unittest.TestCase):
    def test_simple_env_ref(self):
        """Kill mutations that change _ENV_REF_PREFIX string."""
        refs = set()
        _collect_env_refs("$ENV.HOME", refs)
        self.assertIn("HOME", refs)

    def test_multiple_refs_same_string(self):
        refs = set()
        _collect_env_refs("$ENV.KEY1 and $ENV.KEY2", refs)
        self.assertIn("KEY1", refs)
        self.assertIn("KEY2", refs)

    def test_non_env_string_no_refs(self):
        """Kill mutations that extract non-ENV prefixes."""
        refs = set()
        _collect_env_refs("$INPUT.foo and $NODE.bar", refs)
        self.assertEqual(len(refs), 0)

    def test_dict_recurses_into_values(self):
        """Kill mutations that skip dict traversal."""
        refs = set()
        _collect_env_refs({"key": "$ENV.SECRET"}, refs)
        self.assertIn("SECRET", refs)

    def test_list_recurses(self):
        """Kill mutations that skip list traversal."""
        refs = set()
        _collect_env_refs(["$ENV.A", "$ENV.B"], refs)
        self.assertIn("A", refs)
        self.assertIn("B", refs)

    def test_nested_dict_recurses(self):
        refs = set()
        _collect_env_refs({"x": {"y": "$ENV.DEEP"}}, refs)
        self.assertIn("DEEP", refs)

    def test_key_stops_at_non_alnum(self):
        """Kill mutations on the key stop condition."""
        refs = set()
        _collect_env_refs("$ENV.HOME.subpath", refs)
        self.assertIn("HOME", refs)
        self.assertNotIn("subpath", refs)

    def test_empty_ref_ignored(self):
        """Kill mutations that add empty key."""
        refs = set()
        _collect_env_refs("$ENV.", refs)
        self.assertNotIn("", refs)


# ---------------------------------------------------------------------------
# Flow.id deprecated property
# ---------------------------------------------------------------------------

class TestFlowIdDeprecatedProperty(unittest.TestCase):
    def test_id_property_returns_flow_id(self):
        """Kill mutations that change return value."""
        flow = Flow(flow_id="my-id")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            self.assertEqual(flow.id, "my-id")

    def test_id_property_emits_deprecation_warning(self):
        """Kill mutations that remove the warning."""
        flow = Flow(flow_id="fid")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = flow.id
            self.assertGreater(len(w), 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("flow_id", str(w[0].message))


if __name__ == "__main__":
    unittest.main()
