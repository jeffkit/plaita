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


# ---------------------------------------------------------------------------
# 补强轮：精确行为断言（与上面同类测试互补，class 名不冲突）
# ---------------------------------------------------------------------------
from plaita.core.state import _key


class TestKeyHelper(unittest.TestCase):
    def test_key_concat_prefix_and_name(self):
        self.assertEqual(_key("$", "LAST_NODE"), "$LAST_NODE")
        self.assertEqual(_key("#", "INPUT"), "#INPUT")
        self.assertEqual(_key("", "X"), "X")


class TestCheckpointSchemaExtra(unittest.TestCase):
    def test_system_keys_custom_prefix(self):
        self.assertEqual(
            CheckpointSchema.system_keys("#"),
            ["#LAST_NODE", "#BRANCH", "#FLOW_ID", "#EXECUTION_ID"],
        )

    def test_system_keys_count_matches_names(self):
        self.assertEqual(
            len(CheckpointSchema.system_keys("$")),
            len(CheckpointSchema.SYSTEM_KEY_NAMES),
        )

    def test_bare_keys_is_list(self):
        self.assertEqual(CheckpointSchema.bare_keys(), ["EXPRESS_PREFIX"])

    def test_all_known_keys_combines_all_categories(self):
        keys = set(CheckpointSchema.all_known_keys("$"))
        for k in ["$LAST_NODE", "$BRANCH", "$FLOW_ID", "$EXECUTION_ID",
                  "$INPUT", "$NODE", "$GLOBAL", "$PARENT", "$ENV",
                  "EXPRESS_PREFIX"]:
            self.assertIn(k, keys)


class TestValidateCheckpointExtra(unittest.TestCase):
    def test_multiple_unknown_uppercase_keys_warn_each(self):
        warnings = validate_checkpoint({"$FOO": 1, "$BAR": 2})
        self.assertEqual(len(warnings), 2)
        msgs = " ".join(warnings)
        self.assertIn("$FOO", msgs)
        self.assertIn("$BAR", msgs)

    def test_warning_message_mentions_schema_registration(self):
        warnings = validate_checkpoint({"$TYPO": 1})
        self.assertEqual(len(warnings), 1)
        self.assertIn("$TYPO", warnings[0])
        self.assertIn("CheckpointSchema", warnings[0])

    def test_custom_prefix_unknown_key_warns(self):
        warnings = validate_checkpoint({"#UNKNOWN": 1}, prefix="#")
        self.assertEqual(len(warnings), 1)
        self.assertIn("#UNKNOWN", warnings[0])

    def test_custom_prefix_known_system_key_no_warning(self):
        self.assertEqual(validate_checkpoint({"#LAST_NODE": "x"}, prefix="#"), [])

    def test_lowercase_node_local_keys_not_warned(self):
        self.assertEqual(validate_checkpoint({"lower_key": 1, "mixedKey": 2}), [])

    def test_bare_express_prefix_not_warned(self):
        self.assertEqual(validate_checkpoint({"EXPRESS_PREFIX": "$"}), [])

    def test_validate_prefix_but_not_uppercase_no_warning(self):
        # "$lower" 以 $ 开头但后缀非全大写——and 条件 False，不告警；mutant(or) 会告警
        self.assertEqual(validate_checkpoint({"$lower": 1}), [])

    def test_validate_uppercase_but_no_prefix_no_warning(self):
        # "UPPER" 全大写但无 $ 前缀——and 条件 False，不告警；mutant(or) 会告警
        self.assertEqual(validate_checkpoint({"UPPER": 1}), [])


class TestGetitemKeyError(unittest.TestCase):
    def test_absent_schema_key_raises_keyerror(self):
        s = CheckpointState()
        with self.assertRaises(KeyError):
            _ = s["$LAST_NODE"]

    def test_absent_express_prefix_raises_keyerror(self):
        s = CheckpointState()
        with self.assertRaises(KeyError):
            _ = s["EXPRESS_PREFIX"]

    def test_absent_extra_raises_keyerror(self):
        s = CheckpointState()
        with self.assertRaises(KeyError):
            _ = s["no_such_extra"]

    def test_present_schema_key_returns_value(self):
        s = CheckpointState()
        s["$LAST_NODE"] = "n1"
        self.assertEqual(s["$LAST_NODE"], "n1")

    def test_express_prefix_returns_prefix_when_present(self):
        s = CheckpointState()
        s["EXPRESS_PREFIX"] = "#"
        self.assertEqual(s["EXPRESS_PREFIX"], "#")

    def test_extra_key_returns_value(self):
        s = CheckpointState()
        s["my_extra"] = 42
        self.assertEqual(s["my_extra"], 42)


class TestGetitemKeyErrorCarriesKey(unittest.TestCase):
    """raise KeyError(key) vs KeyError(None)——断言异常 args[0] 是被查的键。"""

    def test_absent_schema_key_error_carries_key(self):
        s = CheckpointState()
        with self.assertRaises(KeyError) as cm:
            _ = s["$LAST_NODE"]
        self.assertEqual(cm.exception.args[0], "$LAST_NODE")

    def test_absent_express_prefix_error_carries_key(self):
        s = CheckpointState()
        with self.assertRaises(KeyError) as cm:
            _ = s["EXPRESS_PREFIX"]
        self.assertEqual(cm.exception.args[0], "EXPRESS_PREFIX")

    def test_absent_extra_error_carries_key(self):
        s = CheckpointState()
        with self.assertRaises(KeyError) as cm:
            _ = s["nope"]
        self.assertEqual(cm.exception.args[0], "nope")


class TestSetitemRouting(unittest.TestCase):
    def test_setitem_express_prefix_updates_prefix_field(self):
        s = CheckpointState()
        s["EXPRESS_PREFIX"] = "#"
        self.assertEqual(s.prefix, "#")
        self.assertIn("EXPRESS_PREFIX", s)

    def test_setitem_schema_key_routes_to_typed_field(self):
        s = CheckpointState()
        s["$FLOW_ID"] = "f1"
        self.assertEqual(s.flow_id, "f1")
        self.assertIn("$FLOW_ID", s)

    def test_setitem_extra_creates_extras_dict(self):
        s = CheckpointState()
        s["my_extra"] = "v"
        self.assertEqual(s["my_extra"], "v")
        self.assertIn("my_extra", s)


class TestContainsExtra(unittest.TestCase):
    def test_non_str_key_returns_false(self):
        s = CheckpointState()
        self.assertFalse(99 in s)
        self.assertFalse(None in s)

    def test_express_prefix_membership_tracks_present(self):
        s = CheckpointState()
        self.assertNotIn("EXPRESS_PREFIX", s)
        s["EXPRESS_PREFIX"] = "$"
        self.assertIn("EXPRESS_PREFIX", s)

    def test_schema_key_membership_tracks_present(self):
        s = CheckpointState()
        self.assertNotIn("$LAST_NODE", s)
        s["$LAST_NODE"] = "n"
        self.assertIn("$LAST_NODE", s)

    def test_extra_membership(self):
        s = CheckpointState()
        self.assertNotIn("extra1", s)
        s["extra1"] = 1
        self.assertIn("extra1", s)


class TestIterLenKeys(unittest.TestCase):
    def test_len_counts_present_plus_extras(self):
        s = CheckpointState()
        s["$LAST_NODE"] = "n"
        s["$INPUT"] = 1
        s["extra1"] = "x"
        self.assertEqual(len(s), 3)

    def test_iter_yields_all_keys_no_duplicates(self):
        s = CheckpointState()
        s["$LAST_NODE"] = "n"
        s["extra1"] = "x"
        keys = list(s)
        self.assertIn("$LAST_NODE", keys)
        self.assertIn("extra1", keys)
        self.assertEqual(len(keys), len(set(keys)))

    def test_keys_items_values_consistent(self):
        s = CheckpointState()
        s["$LAST_NODE"] = "n"
        s["extra1"] = "x"
        self.assertEqual(set(s.keys()), {"$LAST_NODE", "extra1"})
        self.assertEqual(dict(s.items()), {"$LAST_NODE": "n", "extra1": "x"})
        self.assertEqual(sorted(s.values()), ["n", "x"])


class TestGetDefault(unittest.TestCase):
    def test_get_returns_default_for_absent(self):
        s = CheckpointState()
        self.assertIsNone(s.get("$LAST_NODE"))
        self.assertEqual(s.get("$LAST_NODE", "fb"), "fb")

    def test_get_returns_value_for_present(self):
        s = CheckpointState()
        s["$LAST_NODE"] = "n"
        self.assertEqual(s.get("$LAST_NODE", "fb"), "n")


class TestEqAndHash(unittest.TestCase):
    def test_eq_against_other_checkpointstate(self):
        a = CheckpointState.from_checkpoint_dict({"$INPUT": 1})
        b = CheckpointState.from_checkpoint_dict({"$INPUT": 1})
        c = CheckpointState.from_checkpoint_dict({"$INPUT": 2})
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)

    def test_eq_against_non_dict_non_state_returns_false(self):
        s = CheckpointState.from_checkpoint_dict({"$INPUT": 1})
        self.assertFalse(s == 42)
        self.assertFalse(s == "string")
        self.assertFalse(s == [1, 2])

    def test_not_hashable(self):
        s = CheckpointState()
        with self.assertRaises(TypeError):
            hash(s)


class TestFreshExtra(unittest.TestCase):
    def test_fresh_carries_only_execution_id_and_env(self):
        s = CheckpointState.fresh(execution_id="ex1", env={"E": 1})
        self.assertEqual(s["$EXECUTION_ID"], "ex1")
        self.assertEqual(s["$ENV"], {"E": 1})
        self.assertNotIn("$LAST_NODE", s)
        self.assertNotIn("$INPUT", s)
        self.assertNotIn("$FLOW_ID", s)
        self.assertEqual(len(s), 2)

    def test_fresh_default_prefix_and_names(self):
        s = CheckpointState.fresh(execution_id="ex", env={})
        self.assertEqual(s.prefix, "$")
        self.assertEqual(s.input_name, "INPUT")
        self.assertEqual(s.parent_name, "PARENT")
        self.assertEqual(s.node_name, "NODE")
        self.assertEqual(s.global_name, "GLOBAL")
        self.assertEqual(s.env_name, "ENV")

    def test_fresh_custom_prefix_and_names(self):
        s = CheckpointState.fresh(
            prefix="#", env_name="ENVV", execution_id="ex2", env={"X": 9},
        )
        self.assertIn("#EXECUTION_ID", s)
        self.assertIn("#ENVV", s)
        self.assertNotIn("#ENV", s)
        self.assertEqual(s["#ENVV"], {"X": 9})

    def test_fresh_custom_names_propagated(self):
        s = CheckpointState.fresh(
            execution_id="ex", env={}, input_name="INP", parent_name="PAR",
            node_name="NOD", global_name="GLO", env_name="ENVV",
        )
        self.assertEqual(s.input_name, "INP")
        self.assertEqual(s.parent_name, "PAR")
        self.assertEqual(s.node_name, "NOD")
        self.assertEqual(s.global_name, "GLO")
        self.assertEqual(s.env_name, "ENVV")
        s2 = CheckpointState.fresh(execution_id="ex", env={}, prefix="#")
        self.assertEqual(s2.prefix, "#")


class TestSetupFlowExtra(unittest.TestCase):
    def test_setup_flow_populates_all_flow_keys(self):
        s = CheckpointState.fresh(execution_id="ex", env={})
        s.setup_flow(
            input_value={"x": 1}, parent_context={"p": 2}, global_context={"g": 3},
            flow_id="f1", env={"HOME": "/h"},
        )
        self.assertEqual(s["$INPUT"], {"x": 1})
        self.assertEqual(s["$PARENT"], {"p": 2})
        self.assertEqual(s["$GLOBAL"], {"g": 3})
        self.assertEqual(s["$FLOW_ID"], "f1")
        self.assertEqual(s["$ENV"], {"HOME": "/h"})
        self.assertEqual(s["EXPRESS_PREFIX"], "$")

    def test_setup_flow_custom_prefix_uses_prefix(self):
        s = CheckpointState.fresh(prefix="#", execution_id="ex", env={})
        s.setup_flow(
            input_value=1, parent_context={}, global_context={},
            flow_id="f", env={},
        )
        self.assertEqual(s["#INPUT"], 1)
        self.assertEqual(s["#FLOW_ID"], "f")
        self.assertEqual(s["EXPRESS_PREFIX"], "#")


class TestUpdateNodeResultExtra(unittest.TestCase):
    def test_update_node_result_creates_map_when_absent(self):
        s = CheckpointState.fresh(execution_id="ex", env={})
        self.assertNotIn("$NODE", s)
        s.update_node_result("n1", {"v": 1})
        self.assertEqual(s["$NODE"], {"n1": {"v": 1}})

    def test_update_node_result_appends_to_existing(self):
        s = CheckpointState.fresh(execution_id="ex", env={})
        s.update_node_result("n1", 1)
        s.update_node_result("n2", 2)
        self.assertEqual(s["$NODE"], {"n1": 1, "n2": 2})

    def test_update_node_result_custom_node_name(self):
        s = CheckpointState.fresh(execution_id="ex", env={}, node_name="NODER")
        s.update_node_result("n1", "r")
        self.assertEqual(s["$NODER"], {"n1": "r"})


class TestFromCheckpointDictPrefixResolution(unittest.TestCase):
    def test_express_prefix_in_data_overrides_fallback(self):
        d = {"EXPRESS_PREFIX": "#", "#INPUT": 1}
        s = CheckpointState.from_checkpoint_dict(d, prefix="$")
        self.assertEqual(s.to_checkpoint_dict(), d)
        self.assertEqual(s.prefix, "#")

    def test_non_dict_data_falls_back_to_prefix(self):
        s = CheckpointState.from_checkpoint_dict(None, prefix="#")  # type: ignore[arg-type]
        self.assertEqual(s.prefix, "#")
        self.assertEqual(s.to_checkpoint_dict(), {})


class TestFieldToKeyTypedAccess(unittest.TestCase):
    """_field_to_key 把每个 schema 键路由到 typed field；直接断言 typed field
    才能杀灭各 field 行的字符串常量变异（round-trip 整体比较会被 extras 路由绕过）。"""

    def test_each_schema_key_routes_to_typed_field(self):
        s = CheckpointState()
        s["$LAST_NODE"] = "n"
        s["$BRANCH"] = "b"
        s["$FLOW_ID"] = "f"
        s["$EXECUTION_ID"] = "e"
        s["$INPUT"] = {"x": 1}
        s["$NODE"] = {"n1": 1}
        s["$GLOBAL"] = {"g": 1}
        s["$PARENT"] = {"p": 1}
        s["$ENV"] = {"env": 1}
        self.assertEqual(s.last_node_id, "n")
        self.assertEqual(s.last_branch, "b")
        self.assertEqual(s.flow_id, "f")
        self.assertEqual(s.execution_id, "e")
        self.assertEqual(s.input_value, {"x": 1})
        self.assertEqual(s.node_results, {"n1": 1})
        self.assertEqual(s.global_context, {"g": 1})
        self.assertEqual(s.parent_context, {"p": 1})
        self.assertEqual(s.env, {"env": 1})

    def test_custom_names_route_to_typed_fields(self):
        s = CheckpointState(input_name="INP", parent_name="PAR", node_name="NOD",
                            global_name="GLO", env_name="ENVV")
        s["$INP"] = 1
        s["$PAR"] = 2
        s["$NOD"] = 3
        s["$GLO"] = 4
        s["$ENVV"] = 5
        self.assertEqual(s.input_value, 1)
        self.assertEqual(s.parent_context, 2)
        self.assertEqual(s.node_results, 3)
        self.assertEqual(s.global_context, 4)
        self.assertEqual(s.env, 5)


class TestSchemaDefaults(unittest.TestCase):
    def test_system_keys_default_prefix(self):
        self.assertEqual(
            CheckpointSchema.system_keys(),
            ["$LAST_NODE", "$BRANCH", "$FLOW_ID", "$EXECUTION_ID"],
        )

    def test_all_known_keys_default_prefix(self):
        keys = CheckpointSchema.all_known_keys()
        self.assertIn("$LAST_NODE", keys)
        self.assertIn("EXPRESS_PREFIX", keys)
        self.assertIn("$INPUT", keys)


class TestFromCheckpointDictNamesExtra(unittest.TestCase):
    """from_checkpoint_dict 把各 *_name 参数透传给模型——逐个断言才能杀灭
    `parent_name=parent_name,` / `global_name=...` / `env_name=...` 行被删的变异。"""

    def test_default_prefix_and_names(self):
        s = CheckpointState.from_checkpoint_dict({})
        self.assertEqual(s.prefix, "$")
        self.assertEqual(s.input_name, "INPUT")
        self.assertEqual(s.parent_name, "PARENT")
        self.assertEqual(s.node_name, "NODE")
        self.assertEqual(s.global_name, "GLOBAL")
        self.assertEqual(s.env_name, "ENV")

    def test_custom_names_propagated_to_model(self):
        s = CheckpointState.from_checkpoint_dict(
            {}, input_name="INP", parent_name="PAR", node_name="NOD",
            global_name="GLO", env_name="ENVV",
        )
        self.assertEqual(s.input_name, "INP")
        self.assertEqual(s.parent_name, "PAR")
        self.assertEqual(s.node_name, "NOD")
        self.assertEqual(s.global_name, "GLO")
        self.assertEqual(s.env_name, "ENVV")

    def test_custom_names_route_keys(self):
        s = CheckpointState.from_checkpoint_dict({"$INP": 7}, input_name="INP")
        self.assertEqual(s.input_value, 7)
        self.assertEqual(s.to_checkpoint_dict(), {"$INP": 7})


if __name__ == "__main__":
    unittest.main()
