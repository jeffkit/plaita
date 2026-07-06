"""Mutation-killing tests for plaita/core/state.py.

Targets survived mutations in:
- CheckpointSchema.system_keys / all_known_keys
- validate_checkpoint
- CheckpointState._field_to_key (all key names)
- CheckpointState.__contains__ (EXPRESS_PREFIX path)
- CheckpointState.from_checkpoint_dict (default parameters)
- CheckpointState.fresh
"""
from __future__ import annotations

import unittest

from plaita.core.state import (
    CheckpointSchema,
    CheckpointState,
    validate_checkpoint,
)


# ---------------------------------------------------------------------------
# CheckpointSchema — exact key content
# ---------------------------------------------------------------------------

class TestCheckpointSchemaExactKeys(unittest.TestCase):
    def test_system_keys_exact_content(self):
        """Kill mutations on SYSTEM_KEY_NAMES entries."""
        keys = CheckpointSchema.system_keys("$")
        self.assertIn("$LAST_NODE", keys)
        self.assertIn("$BRANCH", keys)
        self.assertIn("$FLOW_ID", keys)
        self.assertIn("$EXECUTION_ID", keys)
        self.assertEqual(len(keys), 4)

    def test_system_keys_custom_prefix(self):
        """Kill prefix mutations."""
        keys = CheckpointSchema.system_keys("#")
        self.assertIn("#LAST_NODE", keys)
        self.assertNotIn("$LAST_NODE", keys)

    def test_bare_keys_exact(self):
        """Kill mutations that change BARE_KEY_NAMES."""
        bare = CheckpointSchema.bare_keys()
        self.assertIn("EXPRESS_PREFIX", bare)
        self.assertEqual(len(bare), 1)

    def test_all_known_keys_includes_expression_names(self):
        """Kill mutations on EXPRESSION_NAMES."""
        keys = CheckpointSchema.all_known_keys("$")
        for name in ("$INPUT", "$NODE", "$GLOBAL", "$PARENT", "$ENV"):
            self.assertIn(name, keys)

    def test_all_known_keys_includes_bare(self):
        keys = CheckpointSchema.all_known_keys()
        self.assertIn("EXPRESS_PREFIX", keys)

    def test_all_known_keys_count(self):
        """Kill mutations that drop entries: 4 system + 1 bare + 5 expression = 10."""
        keys = CheckpointSchema.all_known_keys()
        self.assertGreaterEqual(len(keys), 10)


# ---------------------------------------------------------------------------
# validate_checkpoint
# ---------------------------------------------------------------------------

class TestValidateCheckpointMutations(unittest.TestCase):
    def test_clean_with_standard_prefix(self):
        d = {"$LAST_NODE": "n1", "my_local": "x"}
        self.assertEqual(validate_checkpoint(d, "$"), [])

    def test_warns_for_typo_key(self):
        d = {"$TYPO_KEY": "x"}
        ws = validate_checkpoint(d)
        self.assertEqual(len(ws), 1)
        self.assertIn("$TYPO_KEY", ws[0])

    def test_ignores_registered_bare_key(self):
        d = {"EXPRESS_PREFIX": "$"}
        self.assertEqual(validate_checkpoint(d), [])

    def test_empty_dict_clean(self):
        self.assertEqual(validate_checkpoint({}), [])

    def test_warning_message_contains_schema_hint(self):
        """Kill mutations that alter warning message."""
        d = {"$UNKNOWN": "v"}
        ws = validate_checkpoint(d)
        self.assertTrue(len(ws) > 0)
        msg = ws[0]
        self.assertIn("$UNKNOWN", msg)
        self.assertIn("CheckpointSchema", msg)


# ---------------------------------------------------------------------------
# CheckpointState._field_to_key — exact field-to-key mapping
# ---------------------------------------------------------------------------

class TestFieldToKeyMutations(unittest.TestCase):
    def _mapping(self, prefix="$"):
        s = CheckpointState(prefix=prefix)
        return s._field_to_key()

    def test_last_node_id_key(self):
        """Kill XXlast_node_idXX mutation."""
        m = self._mapping()
        self.assertIn("last_node_id", m)
        self.assertEqual(m["last_node_id"], "$LAST_NODE")

    def test_last_branch_key(self):
        """Kill XXlast_branchXX mutation."""
        m = self._mapping()
        self.assertIn("last_branch", m)
        self.assertEqual(m["last_branch"], "$BRANCH")

    def test_flow_id_key(self):
        m = self._mapping()
        self.assertIn("flow_id", m)
        self.assertEqual(m["flow_id"], "$FLOW_ID")

    def test_execution_id_key(self):
        m = self._mapping()
        self.assertIn("execution_id", m)
        self.assertEqual(m["execution_id"], "$EXECUTION_ID")

    def test_input_value_key(self):
        """Kill mutations that change input_name default."""
        m = self._mapping()
        self.assertIn("input_value", m)
        self.assertEqual(m["input_value"], "$INPUT")

    def test_node_results_key(self):
        m = self._mapping()
        self.assertIn("node_results", m)
        self.assertEqual(m["node_results"], "$NODE")

    def test_global_context_key(self):
        m = self._mapping()
        self.assertIn("global_context", m)
        self.assertEqual(m["global_context"], "$GLOBAL")

    def test_parent_context_key(self):
        m = self._mapping()
        self.assertIn("parent_context", m)
        self.assertEqual(m["parent_context"], "$PARENT")

    def test_env_key(self):
        m = self._mapping()
        self.assertIn("env", m)
        self.assertEqual(m["env"], "$ENV")

    def test_all_9_fields_mapped(self):
        m = self._mapping()
        self.assertEqual(len(m), 9)

    def test_custom_prefix_changes_all_system_keys(self):
        m = self._mapping("#")
        self.assertEqual(m["last_node_id"], "#LAST_NODE")
        self.assertEqual(m["last_branch"], "#BRANCH")
        self.assertEqual(m["flow_id"], "#FLOW_ID")
        self.assertEqual(m["execution_id"], "#EXECUTION_ID")

    def test_custom_input_name(self):
        """Kill mutations on input_name field reference."""
        s = CheckpointState(prefix="$", input_name="IN")
        m = s._field_to_key()
        self.assertEqual(m["input_value"], "$IN")

    def test_custom_node_name(self):
        s = CheckpointState(prefix="$", node_name="NODES")
        m = s._field_to_key()
        self.assertEqual(m["node_results"], "$NODES")

    def test_custom_global_name(self):
        s = CheckpointState(prefix="$", global_name="GLOB")
        m = s._field_to_key()
        self.assertEqual(m["global_context"], "$GLOB")

    def test_custom_parent_name(self):
        s = CheckpointState(prefix="$", parent_name="PAR")
        m = s._field_to_key()
        self.assertEqual(m["parent_context"], "$PAR")

    def test_custom_env_name(self):
        s = CheckpointState(prefix="$", env_name="ENVIRON")
        m = s._field_to_key()
        self.assertEqual(m["env"], "$ENVIRON")


# ---------------------------------------------------------------------------
# CheckpointState.__contains__ — EXPRESS_PREFIX special case
# ---------------------------------------------------------------------------

class TestCheckpointContainsMutations(unittest.TestCase):
    def test_express_prefix_absent_not_in(self):
        """Kill == → != mutation: when not set, should be False."""
        s = CheckpointState()
        self.assertNotIn("EXPRESS_PREFIX", s)

    def test_express_prefix_set_is_in(self):
        """After setting EXPRESS_PREFIX, should be in."""
        s = CheckpointState()
        s["EXPRESS_PREFIX"] = "$"
        self.assertIn("EXPRESS_PREFIX", s)

    def test_non_string_not_in(self):
        """Kill mutations on isinstance check."""
        s = CheckpointState()
        self.assertNotIn(123, s)
        self.assertNotIn(None, s)

    def test_schema_key_absent_not_in(self):
        """Key is present only after being set."""
        s = CheckpointState()
        self.assertNotIn("$LAST_NODE", s)

    def test_schema_key_set_is_in(self):
        s = CheckpointState()
        s["$LAST_NODE"] = "n1"
        self.assertIn("$LAST_NODE", s)

    def test_extra_key_is_in(self):
        """Extra keys should also be found."""
        s = CheckpointState()
        s["my_extra"] = "value"
        self.assertIn("my_extra", s)

    def test_branch_not_set_not_in(self):
        """Kill mutations that always return True for EXPRESS_PREFIX."""
        s = CheckpointState()
        # $BRANCH was never set, even though EXPRESS_PREFIX was:
        s["EXPRESS_PREFIX"] = "$"
        self.assertNotIn("$BRANCH", s)


# ---------------------------------------------------------------------------
# CheckpointState.from_checkpoint_dict — default parameter values
# ---------------------------------------------------------------------------

class TestFromCheckpointDictDefaults(unittest.TestCase):
    def test_default_prefix_is_dollar(self):
        """Kill prefix="$" → "XX$XX" mutation."""
        s = CheckpointState.from_checkpoint_dict({"$INPUT": 1})
        # If prefix was wrong the key routing would fail
        self.assertEqual(s["$INPUT"], 1)

    def test_default_input_name_is_INPUT(self):
        """Kill input_name="INPUT" → "XXINPUTXX" mutation."""
        s = CheckpointState.from_checkpoint_dict({"$INPUT": {"x": 2}})
        self.assertEqual(s.input_value, {"x": 2})

    def test_default_node_name_is_NODE(self):
        """Kill node_name="NODE" mutation."""
        s = CheckpointState.from_checkpoint_dict({"$NODE": {"n1": "r"}})
        self.assertEqual(s.node_results, {"n1": "r"})

    def test_default_global_name_is_GLOBAL(self):
        s = CheckpointState.from_checkpoint_dict({"$GLOBAL": {"gk": "gv"}})
        self.assertEqual(s.global_context, {"gk": "gv"})

    def test_default_parent_name_is_PARENT(self):
        s = CheckpointState.from_checkpoint_dict({"$PARENT": {"pk": "pv"}})
        self.assertEqual(s.parent_context, {"pk": "pv"})

    def test_default_env_name_is_ENV(self):
        s = CheckpointState.from_checkpoint_dict({"$ENV": {"HOME": "/h"}})
        self.assertEqual(s.env, {"HOME": "/h"})

    def test_express_prefix_in_dict_overrides_default(self):
        """Kill mutations that ignore EXPRESS_PREFIX key in data."""
        s = CheckpointState.from_checkpoint_dict({"EXPRESS_PREFIX": "#", "#LAST_NODE": "n2"})
        self.assertEqual(s.last_node_id, "n2")
        self.assertEqual(s.prefix, "#")

    def test_explicit_prefix_used_when_no_express_prefix(self):
        """Kill mutations on fallback prefix."""
        s = CheckpointState.from_checkpoint_dict({"#INPUT": "val"}, prefix="#")
        self.assertEqual(s.input_value, "val")

    def test_empty_data_creates_empty_state(self):
        s = CheckpointState.from_checkpoint_dict({})
        self.assertEqual(s.to_checkpoint_dict(), {})

    def test_none_data_creates_empty_state(self):
        s = CheckpointState.from_checkpoint_dict(None)
        self.assertEqual(s.to_checkpoint_dict(), {})

    def test_custom_input_name_parameter(self):
        s = CheckpointState.from_checkpoint_dict({"$IN": "v"}, input_name="IN")
        self.assertEqual(s.input_value, "v")

    def test_custom_node_name_parameter(self):
        s = CheckpointState.from_checkpoint_dict({"$NODES": {"n1": "r"}}, node_name="NODES")
        self.assertEqual(s.node_results, {"n1": "r"})


# ---------------------------------------------------------------------------
# CheckpointState.fresh — initial state
# ---------------------------------------------------------------------------

class TestCheckpointStateFresh(unittest.TestCase):
    def test_fresh_has_execution_id(self):
        """Kill mutmut fresh mutations."""
        s = CheckpointState.fresh(
            execution_id="exec-99",
            env={"K": "V"},
        )
        self.assertIn("$EXECUTION_ID", s)
        self.assertEqual(s["$EXECUTION_ID"], "exec-99")

    def test_fresh_has_env(self):
        """Kill mutations that drop env assignment."""
        s = CheckpointState.fresh(
            execution_id="e1",
            env={"HOME": "/home/user"},
        )
        self.assertIn("$ENV", s)
        self.assertEqual(s["$ENV"], {"HOME": "/home/user"})

    def test_fresh_no_other_system_keys(self):
        """Kill mutations that add extra keys."""
        s = CheckpointState.fresh(execution_id="e2", env={})
        checkpoint = s.to_checkpoint_dict()
        self.assertNotIn("$LAST_NODE", checkpoint)
        self.assertNotIn("$BRANCH", checkpoint)
        self.assertNotIn("$FLOW_ID", checkpoint)

    def test_fresh_custom_prefix(self):
        """Kill mutations that use wrong prefix in key formatting."""
        s = CheckpointState.fresh(
            prefix="#",
            execution_id="e3",
            env={},
        )
        self.assertIn("#EXECUTION_ID", s)
        self.assertIn("#ENV", s)

    def test_fresh_custom_env_name(self):
        """Kill mutations on env_name parameter."""
        s = CheckpointState.fresh(
            execution_id="e4",
            env_name="ENVIRON",
            env={"V": "1"},
        )
        self.assertIn("$ENVIRON", s)

    def test_fresh_exactly_two_keys(self):
        """Fresh state has exactly 2 keys: EXECUTION_ID + ENV."""
        s = CheckpointState.fresh(execution_id="e5", env={})
        checkpoint = s.to_checkpoint_dict()
        self.assertEqual(len(checkpoint), 2)


# ---------------------------------------------------------------------------
# CheckpointState.setup_flow — key assignments
# ---------------------------------------------------------------------------

class TestSetupFlowMutations(unittest.TestCase):
    def test_setup_flow_sets_input(self):
        s = CheckpointState()
        s.setup_flow(
            input_value={"x": 1},
            parent_context={},
            global_context={},
            flow_id="f1",
            env={},
        )
        self.assertEqual(s["$INPUT"], {"x": 1})

    def test_setup_flow_sets_flow_id(self):
        s = CheckpointState()
        s.setup_flow(
            input_value={},
            parent_context={},
            global_context={},
            flow_id="my-flow",
            env={},
        )
        self.assertEqual(s["$FLOW_ID"], "my-flow")

    def test_setup_flow_sets_express_prefix(self):
        s = CheckpointState()
        s.setup_flow(
            input_value={},
            parent_context={},
            global_context={},
            flow_id="f1",
            env={},
        )
        self.assertIn("EXPRESS_PREFIX", s)
        self.assertEqual(s["EXPRESS_PREFIX"], "$")

    def test_setup_flow_sets_global(self):
        s = CheckpointState()
        s.setup_flow(
            input_value={},
            parent_context={},
            global_context={"gk": "gv"},
            flow_id="f1",
            env={},
        )
        self.assertEqual(s["$GLOBAL"], {"gk": "gv"})

    def test_setup_flow_sets_parent(self):
        s = CheckpointState()
        s.setup_flow(
            input_value={},
            parent_context={"pk": "pv"},
            global_context={},
            flow_id="f1",
            env={},
        )
        self.assertEqual(s["$PARENT"], {"pk": "pv"})

    def test_setup_flow_sets_env(self):
        s = CheckpointState()
        s.setup_flow(
            input_value={},
            parent_context={},
            global_context={},
            flow_id="f1",
            env={"HOME": "/h"},
        )
        self.assertEqual(s["$ENV"], {"HOME": "/h"})


# ---------------------------------------------------------------------------
# CheckpointState.update_node_result
# ---------------------------------------------------------------------------

class TestUpdateNodeResultMutations(unittest.TestCase):
    def test_creates_node_map_if_absent(self):
        s = CheckpointState()
        s.update_node_result("n1", {"val": 1})
        self.assertIn("$NODE", s)
        self.assertEqual(s["$NODE"], {"n1": {"val": 1}})

    def test_appends_to_existing_map(self):
        s = CheckpointState()
        s["$NODE"] = {"n1": "first"}
        s.update_node_result("n2", "second")
        node_map = s["$NODE"]
        self.assertEqual(node_map["n1"], "first")
        self.assertEqual(node_map["n2"], "second")

    def test_overwrites_existing_key(self):
        s = CheckpointState()
        s.update_node_result("n1", "v1")
        s.update_node_result("n1", "v2")
        self.assertEqual(s["$NODE"]["n1"], "v2")

    def test_uses_prefix_from_state(self):
        """Kill mutations that use wrong prefix."""
        s = CheckpointState(prefix="#", node_name="NODE")
        s.update_node_result("n1", "r1")
        self.assertIn("#NODE", s)

    def test_uses_node_name_from_state(self):
        """Kill mutations on self.node_name."""
        s = CheckpointState(node_name="RESULTS")
        s.update_node_result("n1", "r1")
        self.assertIn("$RESULTS", s)
        self.assertEqual(s["$RESULTS"]["n1"], "r1")


if __name__ == "__main__":
    unittest.main()
