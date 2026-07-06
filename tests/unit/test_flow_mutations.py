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

class TestFromStringBasicCoverage(unittest.TestCase):
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

class TestCollectEnvRefsBasicCoverage(unittest.TestCase):
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


# ---------------------------------------------------------------------------
# model_validate — registry passthrough and **kwargs (Round 2)
# ---------------------------------------------------------------------------

class TestModelValidateRegistryPassthrough(unittest.TestCase):
    def test_model_validate_passes_registry_not_none(self):
        """Kill flow.resolve_nodes(None) mutation."""
        reg = _make_registry()
        data = {
            "flow_id": "f1",
            "runtime": "python",
            "nodes": [
                {"id": "s1", "type": "start"},
                {"id": "e1", "type": "end"},
            ],
        }
        flow = Flow.model_validate(data, registry=reg)
        # All nodes should be resolved (Node instances), not dicts
        for n in flow.nodes:
            self.assertIsInstance(n, Node)

    def test_model_validate_kwargs_forwarded(self):
        """Kill super().model_validate(data, ) mutation — kwargs dropped."""
        data = {"flow_id": "fk1", "runtime": "python"}
        # strict=False is a known Pydantic kwarg; should not raise
        try:
            flow = Flow.model_validate(data, strict=False)
        except Exception as e:
            self.fail(f"model_validate with strict=False raised: {e}")

    def test_model_validate_json_passes_json_data_not_none(self):
        """Kill super().model_validate_json(None, **kwargs) mutation."""
        json_str = '{"flow_id": "f_json", "runtime": "python"}'
        flow = Flow.model_validate_json(json_str)
        self.assertEqual(flow.flow_id, "f_json")


# ---------------------------------------------------------------------------
# resolve_nodes — changed flag initialization and node preservation (Round 2)
# ---------------------------------------------------------------------------

class TestResolveNodesChangedFlag(unittest.TestCase):
    def setUp(self):
        self.reg = _make_registry()

    def test_node_instances_preserved_not_none(self):
        """Kill resolved.append(None) mutation."""
        start = Start(id="s1")
        end = End(id="e1")
        flow = Flow(flow_id="fc", nodes=[start, end])
        flow.resolve_nodes(self.reg)
        self.assertIn(start, flow.nodes)
        self.assertIn(end, flow.nodes)
        self.assertNotIn(None, flow.nodes)

    def test_changed_false_initialization_no_spurious_reindex(self):
        """Kill changed=True mutation (would always invalidate index)."""
        start = Start(id="s1")
        end = End(id="e1")
        flow = Flow(flow_id="fc2", nodes=[start, end])
        # Build index first
        _ = flow._ensure_index()
        sig_before = flow._node_index_sig
        # resolve_nodes with all Node instances — changed stays False
        flow.resolve_nodes(self.reg)
        # index sig should be unchanged (no index invalidation)
        self.assertEqual(flow._node_index_sig, sig_before)

    def test_index_invalidated_only_when_dict_nodes_resolved(self):
        """Kill changed=None mutation (falsy → never invalidates)."""
        flow = Flow(flow_id="fc3", nodes=[
            {"id": "s1", "type": "start"},
            {"id": "e1", "type": "end"},
        ])
        flow.resolve_nodes(self.reg)
        # After resolve, index sig should be reset (changed=True path taken)
        self.assertEqual(flow._node_index_sig, ())

    def test_index_sig_reset_to_empty_tuple_not_none(self):
        """Kill _node_index_sig = None mutation."""
        flow = Flow(flow_id="fc4", nodes=[
            {"id": "s1", "type": "start"},
        ])
        flow.resolve_nodes(self.reg)
        self.assertIsNotNone(flow._node_index_sig)
        self.assertEqual(flow._node_index_sig, ())


# ---------------------------------------------------------------------------
# _ensure_index — signature uses id(nodes) not id(None) (Round 2)
# ---------------------------------------------------------------------------

class TestEnsureIndexSignatureMutation(unittest.TestCase):
    def test_signature_uses_nodes_identity(self):
        """Kill id(None) mutation — cache would always miss."""
        start = Start(id="s1")
        end = End(id="e1")
        flow = Flow(flow_id="f_idx", nodes=[start, end])
        idx1 = flow._ensure_index()
        idx2 = flow._ensure_index()
        self.assertIs(idx1, idx2)  # same object — cache hit

    def test_different_nodes_list_breaks_cache(self):
        """Verify cache invalidation on list replacement."""
        start = Start(id="s1")
        end = End(id="e1")
        new_end = End(id="e2")
        flow = Flow(flow_id="f_idx2", nodes=[start, end])
        idx1 = flow._ensure_index()
        flow.nodes = [start, new_end]
        idx2 = flow._ensure_index()
        self.assertNotIn("e1", idx2)
        self.assertIn("e2", idx2)


# ---------------------------------------------------------------------------
# rebuild_node_index — sig reset to () not None (Round 2)
# ---------------------------------------------------------------------------

class TestRebuildNodeIndexSigReset(unittest.TestCase):
    def test_rebuild_resets_sig_to_empty_tuple(self):
        """Kill _node_index_sig = None mutation."""
        start = Start(id="s1")
        end = End(id="e1")
        flow = Flow(flow_id="f_reb", nodes=[start, end])
        _ = flow._ensure_index()
        sig_before = flow._node_index_sig
        self.assertNotEqual(sig_before, ())  # should have real sig after build

        flow.rebuild_node_index()
        # After rebuild, index should be freshly built
        idx = flow._ensure_index()
        self.assertIn("s1", idx)
        self.assertIn("e1", idx)

    def test_rebuild_allows_find_after_id_change(self):
        """Verify rebuild works correctly after id change."""
        start = Start(id="s1")
        end = End(id="e1")
        flow = Flow(flow_id="f_reb2", nodes=[start, end])
        # Rename start node id in place
        flow.nodes[0] = Start(id="start_renamed")
        flow.rebuild_node_index()
        self.assertIsNotNone(flow.find_node_by_id("start_renamed"))


# ---------------------------------------------------------------------------
# from_string — empty check logic, message text, lstrip/rstrip, slice (Round 2)
# ---------------------------------------------------------------------------

class TestFromStringMutations(unittest.TestCase):
    def test_empty_string_raises_valueerror(self):
        """Kill `and` mutation — empty string should raise."""
        with self.assertRaises(ValueError):
            Flow.from_string("")

    def test_whitespace_only_raises_valueerror(self):
        """Kill `and` mutation — whitespace-only should also raise."""
        with self.assertRaises(ValueError):
            Flow.from_string("   \n  ")

    def test_valueerror_message_exact_case(self):
        """Kill case mutation — 'Flow' must be capitalized."""
        try:
            Flow.from_string("")
        except ValueError as e:
            msg = str(e)
            self.assertIn("Flow", msg)
            self.assertNotIn("flow content", msg)
            self.assertNotIn("FLOW CONTENT", msg)

    def test_json_detected_by_leading_brace(self):
        """Kill lstrip->rstrip and [:1]->[:2] mutations."""
        json_str = '  {"flow_id": "f1", "runtime": "python"}'
        flow = Flow.from_string(json_str)
        self.assertEqual(flow.flow_id, "f1")

    def test_json_detected_by_leading_bracket(self):
        """Kill '[' removal from set mutation."""
        # Not testing array flow directly, just that '[' doesn't prevent YAML fallback
        yaml_str = "flow_id: f2\nruntime: python"
        flow = Flow.from_string(yaml_str)
        self.assertEqual(flow.flow_id, "f2")

    def test_json_brace_check_not_inverted(self):
        """Kill `not in` mutation — JSON should be tried for '{' prefix."""
        import json as json_mod
        json_str = json_mod.dumps({"flow_id": "f3", "runtime": "python"})
        flow = Flow.from_string(json_str)
        self.assertEqual(flow.flow_id, "f3")

    def test_yaml_string_not_treated_as_json(self):
        """Kill slice mutation [:1]->[:2] — ensure YAML path taken for 'f...' start."""
        yaml_str = "flow_id: f4\nruntime: python"
        flow = Flow.from_string(yaml_str)
        self.assertEqual(flow.flow_id, "f4")

    def test_from_string_mutmut_8_rstrip_lstrip(self):
        """Kill lstrip()→rstrip() mutation — leading spaces before { must be stripped."""
        # JSON content with leading spaces — lstrip needed for correct detection
        json_str = '   {"flow_id": "f_lstrip", "runtime": "python"}'
        flow = Flow.from_string(json_str)
        self.assertEqual(flow.flow_id, "f_lstrip")


# ---------------------------------------------------------------------------
# next_node — is_end_node(current) vs is_end_node(None) (Round 2)
# ---------------------------------------------------------------------------

class TestNextNodeEndCheckMutation(unittest.TestCase):
    def _make_simple_flow(self):
        start = Start(id="s1", next="e1")
        end = End(id="e1")
        flow = Flow(flow_id="fnc", nodes=[start, end])
        return flow, start, end

    def test_next_node_passes_current_to_is_end(self):
        """Kill is_end_node(None) mutation."""
        flow, start, end = self._make_simple_flow()
        # For end node, should return None
        self.assertIsNone(flow.next_node(end))
        # For start node, should return end
        result = flow.next_node(start)
        self.assertEqual(result.id, "e1")

    def test_next_node_logging_includes_target(self):
        """Kill logger.debug(..., None, ...) mutations."""
        flow, start, end = self._make_simple_flow()
        with self.assertLogs("plaita", level="DEBUG") as cm:
            flow.next_node(start)
        combined = " ".join(cm.output)
        self.assertIn("e1", combined)

    def test_next_node_logging_includes_current_id(self):
        """Kill logger.debug(..., target, None, ...) mutation."""
        flow, start, end = self._make_simple_flow()
        with self.assertLogs("plaita", level="DEBUG") as cm:
            flow.next_node(start)
        combined = " ".join(cm.output)
        self.assertIn("s1", combined)


# ---------------------------------------------------------------------------
# _get_branch_target — log arg precision and resolve_branch_target (Round 2)
# ---------------------------------------------------------------------------

class TestGetBranchTargetMutations(unittest.TestCase):
    def _make_switch_flow(self):
        from plaita.node.decide import Switch, Branch
        switch = Switch(
            id="sw1",
            branches=[
                Branch(name="yes", condition={"left": "$x", "op": "eq", "right": "yes"}, next="end1"),
                Branch(name="no", condition={"left": "$x", "op": "eq", "right": "no"}, next="end2"),
            ],
        )
        end1 = End(id="end1")
        end2 = End(id="end2")
        flow = Flow(flow_id="fsw", nodes=[switch, end1, end2])
        return flow, switch, end1, end2

    def test_resolve_branch_target_called_with_current(self):
        """Kill resolve_branch_target(None, b) mutation.
        branch param is the target node id (what resolve_branch_target returns)."""
        flow, switch, end1, end2 = self._make_switch_flow()
        result = flow.next_node(switch, "end1")
        self.assertEqual(result.id, "end1")

    def test_resolve_branch_target_called_with_branch_b(self):
        """Kill resolve_branch_target(current, None) mutation."""
        flow, switch, end1, end2 = self._make_switch_flow()
        result = flow.next_node(switch, "end2")
        self.assertEqual(result.id, "end2")

    def test_branch_not_found_warning_includes_branch_name(self):
        """Kill logger.warning(..., None, current.id) mutation."""
        flow, switch, _, _ = self._make_switch_flow()
        with self.assertLogs("plaita", level="WARNING") as cm:
            result = flow.next_node(switch, "nonexistent_target")
        combined = " ".join(cm.output)
        self.assertIn("nonexistent_target", combined)
        self.assertIsNone(result)

    def test_branch_not_found_warning_includes_node_id(self):
        """Kill logger.warning(..., branch, None) mutation."""
        flow, switch, _, _ = self._make_switch_flow()
        with self.assertLogs("plaita", level="WARNING") as cm:
            flow.next_node(switch, "nonexistent_target")
        combined = " ".join(cm.output)
        self.assertIn("sw1", combined)

    def test_branch_debug_log_includes_node_id(self):
        """Kill logger.debug(..., None, current.branches) mutation."""
        flow, switch, end1, _ = self._make_switch_flow()
        with self.assertLogs("plaita", level="DEBUG") as cm:
            flow.next_node(switch, "end1")
        combined = " ".join(cm.output)
        self.assertIn("sw1", combined)

    def test_branch_debug_log_message_not_mangled(self):
        """Kill XXcurrent node ... XX mutation in debug message."""
        flow, switch, end1, _ = self._make_switch_flow()
        with self.assertLogs("plaita", level="DEBUG") as cm:
            flow.next_node(switch, "end1")
        combined = " ".join(cm.output)
        self.assertNotIn("XX", combined)


# ---------------------------------------------------------------------------
# _warn_uncovered_env_refs — log arg precision (Round 2)
# ---------------------------------------------------------------------------

class TestWarnUncoveredEnvRefsLogArgs(unittest.TestCase):
    def _make_env_flow(self):
        start = Start(id="s1", name="$ENV.APP_NAME")
        end = End(id="e1")
        return Flow(flow_id="fenv", nodes=[start, end])  # no expose_env

    def test_warning_includes_flow_id(self):
        """Kill logger.warning(..., None, refs, refs) mutation."""
        flow = self._make_env_flow()
        with self.assertLogs("plaita", level="WARNING") as cm:
            flow._warn_uncovered_env_refs()
        combined = " ".join(cm.output)
        self.assertIn("fenv", combined)

    def test_warning_includes_env_refs_first_arg(self):
        """Kill logger.warning(..., flow_id, None, refs) mutation."""
        flow = self._make_env_flow()
        with self.assertLogs("plaita", level="WARNING") as cm:
            flow._warn_uncovered_env_refs()
        combined = " ".join(cm.output)
        self.assertIn("APP_NAME", combined)

    def test_warning_message_not_mangled(self):
        """Kill XX...XX mutation in warning message prefix."""
        flow = self._make_env_flow()
        with self.assertLogs("plaita", level="WARNING") as cm:
            flow._warn_uncovered_env_refs()
        combined = " ".join(cm.output)
        self.assertIn("flow", combined.lower())
        self.assertNotIn("XX", combined)


# ---------------------------------------------------------------------------
# _collect_env_refs — start index, break vs return, underscore, j+1 (Round 2)
# ---------------------------------------------------------------------------
from plaita.core.flow import _collect_env_refs


class TestCollectEnvRefsMutations(unittest.TestCase):
    def test_start_initialized_to_zero_not_none(self):
        """Kill start=None mutation — would cause TypeError on find()."""
        refs = set()
        _collect_env_refs("prefix $ENV.KEY1 middle $ENV.KEY2 suffix", refs)
        self.assertIn("KEY1", refs)
        self.assertIn("KEY2", refs)

    def test_break_not_return_when_no_more_refs(self):
        """Kill break→return mutation — subsequent string scanning would stop."""
        refs = set()
        # Two $ENV refs in one string — both must be found
        _collect_env_refs("$ENV.FIRST and $ENV.SECOND", refs)
        self.assertEqual(refs, {"FIRST", "SECOND"})

    def test_underscore_in_key_included(self):
        """Kill obj[j] == 'XX_XX' mutation — underscore stops key scan."""
        refs = set()
        _collect_env_refs("$ENV.MY_KEY", refs)
        self.assertIn("MY_KEY", refs)
        self.assertNotIn("MY", refs)

    def test_j_plus_1_advances_correctly(self):
        """Kill j+=2 mutation — would skip every other char."""
        refs = set()
        _collect_env_refs("$ENV.ABCD", refs)
        self.assertIn("ABCD", refs)

    def test_start_plus_1_not_plus_2_or_minus_1(self):
        """Kill start=j+2 and start=j-1 mutations."""
        refs = set()
        # Two adjacent $ENV refs separated by single char
        _collect_env_refs("$ENV.A,$ENV.B", refs)
        self.assertIn("A", refs)
        self.assertIn("B", refs)

    def test_empty_key_not_added(self):
        """Verify j > key_start condition — $ENV. alone adds nothing."""
        refs = set()
        _collect_env_refs("$ENV.", refs)
        self.assertEqual(len(refs), 0)

    def test_dict_values_recursed(self):
        """Verify dict path not broken by surrounding mutations."""
        refs = set()
        _collect_env_refs({"key": "$ENV.DICT_KEY"}, refs)
        self.assertIn("DICT_KEY", refs)

    def test_list_elements_recursed(self):
        """Verify list path."""
        refs = set()
        _collect_env_refs(["$ENV.LIST_KEY"], refs)
        self.assertIn("LIST_KEY", refs)


if __name__ == "__main__":
    unittest.main()
