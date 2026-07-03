"""Tests for the distributed-checkpoint schema (plaita.core.state).

Goal: stop the silent drift where every caller invents its own
``f"{pfx}SOME_KEY"`` magic string. Any new system key must be registered in
``CheckpointSchema.SYSTEM_KEY_NAMES`` — otherwise ``validate_checkpoint``
will warn when the context is serialised.

Also covers ``CheckpointState`` — the typed BaseModel that backs
``ExecutionContext`` — including the byte-lossless round-trip property test
that pins the distributed-checkpoint binary-compat contract.
"""

import unittest

from plaita.core.state import (
    CheckpointSchema,
    CheckpointState,
    validate_checkpoint,
)


class CheckpointSchemaTest(unittest.TestCase):
    def test_system_keys_are_prefixed(self):
        keys = CheckpointSchema.system_keys("$")
        self.assertIn("$LAST_NODE", keys)
        self.assertIn("$BRANCH", keys)
        self.assertIn("$FLOW_ID", keys)
        self.assertIn("$EXECUTION_ID", keys)

    def test_bare_keys_include_prefix_marker(self):
        self.assertIn("EXPRESS_PREFIX", CheckpointSchema.bare_keys())

    def test_validate_clean_checkpoint(self):
        data = {
            "$LAST_NODE": "n1",
            "$BRANCH": None,
            "$FLOW_ID": "f1",
            "$EXECUTION_ID": "abc",
            "$INPUT": {"x": 1},
            "$NODE": {},
            "$GLOBAL": {},
            "$PARENT": {},
            "$ENV": {},
            "EXPRESS_PREFIX": "$",
            "my_node_local_key": "anything",  # node-local, not a warning
        }
        self.assertEqual(validate_checkpoint(data), [])

    def test_validate_warns_on_unknown_uppercase_key(self):
        data = {"$UNKNOWN_SYSTEM_KEY": "x"}
        warnings = validate_checkpoint(data)
        self.assertEqual(len(warnings), 1)
        self.assertIn("$UNKNOWN_SYSTEM_KEY", warnings[0])

    def test_validate_ignores_lowercase_node_local_keys(self):
        data = {"some_node_local": "x", "my_state": 42}
        self.assertEqual(validate_checkpoint(data), [])

    def test_to_dict_emits_drift_warning(self, ):
        # integration with ExecutionContext: serialising a context that has
        # an unknown system-shaped key should warn via the logger.
        import logging
        from plaita.core.context import ExecutionContext

        ctx = ExecutionContext()
        ctx.set_state("$TYPO_LAST_NODE", "should-be-warned")
        with self.assertLogs("plaita.core.context", level="WARNING") as cm:
            ctx.to_dict()
        self.assertTrue(
            any("$TYPO_LAST_NODE" in m for m in cm.output),
            f"expected drift warning in log, got: {cm.output}",
        )


class CheckpointStateRoundTripTest(unittest.TestCase):
    """``CheckpointState`` must round-trip old-style prefixed dicts losslessly.

    This is the binary-compat contract: a checkpoint written by the legacy
    ``dict(self._context)`` code must load via ``from_checkpoint_dict`` and
    re-emit via ``to_checkpoint_dict`` to the **exact** same dict (same key
    set, same values). Any drift here breaks Redis/SQL-persisted checkpoints
    on upgrade.
    """

    def test_empty_dict_round_trips(self):
        s = CheckpointState.from_checkpoint_dict({})
        self.assertEqual(s.to_checkpoint_dict(), {})

    def test_full_schema_dict_round_trips(self):
        d = {
            "$LAST_NODE": "n1",
            "$BRANCH": "b1",
            "$FLOW_ID": "f1",
            "$EXECUTION_ID": "abc",
            "$INPUT": {"x": 1},
            "$NODE": {"n1": {"v": 1}},
            "$GLOBAL": {"flow_id": "f1"},
            "$PARENT": {"p": 2},
            "$ENV": {"HOME": "/h"},
            "EXPRESS_PREFIX": "$",
        }
        s = CheckpointState.from_checkpoint_dict(d)
        self.assertEqual(s.to_checkpoint_dict(), d)

    def test_partial_dict_preserves_absent_keys(self):
        # A schema field that was never written must NOT appear in output —
        # otherwise old checkpoints gain spurious ``$LAST_NODE: None`` keys.
        d = {"$INPUT": {"a": 1}, "my_local": 42}
        s = CheckpointState.from_checkpoint_dict(d)
        out = s.to_checkpoint_dict()
        self.assertEqual(out, d)
        self.assertNotIn("$LAST_NODE", out)
        self.assertNotIn("EXPRESS_PREFIX", out)

    def test_node_local_extras_round_trip(self):
        d = {"$INPUT": 1, "my_node_local": {"any": "thing"}, "another": [1, 2]}
        s = CheckpointState.from_checkpoint_dict(d)
        self.assertEqual(s.to_checkpoint_dict(), d)

    def test_none_value_is_distinct_from_absent(self):
        # ``$INPUT: null`` is a real value, not "missing".
        d = {"$INPUT": None}
        s = CheckpointState.from_checkpoint_dict(d)
        out = s.to_checkpoint_dict()
        self.assertEqual(out, d)
        self.assertIn("$INPUT", out)
        self.assertIsNone(out["$INPUT"])

    def test_custom_prefix_round_trips(self):
        d = {
            "#LAST_NODE": "n1",
            "#INPUT": {"x": 1},
            "EXPRESS_PREFIX": "#",
        }
        s = CheckpointState.from_checkpoint_dict(d)
        self.assertEqual(s.to_checkpoint_dict(), d)

    def test_dict_like_access_over_prefixed_keys(self):
        s = CheckpointState.from_checkpoint_dict({"$LAST_NODE": "n1", "$INPUT": {"x": 1}})
        self.assertEqual(s["$LAST_NODE"], "n1")
        self.assertEqual(s["$INPUT"], {"x": 1})
        self.assertIn("$LAST_NODE", s)
        self.assertNotIn("$BRANCH", s)
        self.assertEqual(s.get("$BRANCH", "fallback"), "fallback")
        self.assertEqual(set(s.keys()), {"$LAST_NODE", "$INPUT"})

    def test_setitem_routes_schema_keys_to_fields(self):
        s = CheckpointState()
        s["$LAST_NODE"] = "n2"
        s["$INPUT"] = {"y": 2}
        s["my_local"] = 99
        self.assertEqual(s.last_node_id, "n2")
        self.assertEqual(s.input_value, {"y": 2})
        self.assertEqual(s["my_local"], 99)
        self.assertEqual(s.to_checkpoint_dict(), {"$LAST_NODE": "n2", "$INPUT": {"y": 2}, "my_local": 99})

    def test_eq_against_plain_dict(self):
        # ExecutionContext clients (and tests) compare ``execution.context`` to
        # a plain dict — CheckpointState.__eq__ must honour that.
        s = CheckpointState.from_checkpoint_dict({"$INPUT": {"x": 1}})
        self.assertEqual(s, {"$INPUT": {"x": 1}})
        self.assertNotEqual(s, {"$INPUT": {"x": 2}})

    def test_property_round_trip_random(self):
        # Property-style: many random old-style dicts round-trip exactly.
        import random

        rng = random.Random(20260703)
        # EXPRESS_PREFIX is always a prefix string in real checkpoints; the
        # other schema keys carry arbitrary JSON-able values.
        prefix_pool = ["$", "#", "@"]
        value_pool = [
            "s", 123, None, {"k": "v"}, [1, 2, "x"], "", True, False,
            {"nested": {"deep": [1, {"a": 2}]}},
        ]
        value_schema_keys = [
            "$LAST_NODE", "$BRANCH", "$FLOW_ID", "$EXECUTION_ID",
            "$INPUT", "$NODE", "$GLOBAL", "$PARENT", "$ENV",
        ]
        for _ in range(200):
            d: dict = {}
            if rng.random() < 0.5:
                d["EXPRESS_PREFIX"] = rng.choice(prefix_pool)
            for k in value_schema_keys:
                if rng.random() < 0.5:
                    d[k] = rng.choice(value_pool)
            for _ in range(rng.randint(0, 3)):
                d[f"local_{rng.randint(0, 5)}"] = rng.choice(value_pool)
            s = CheckpointState.from_checkpoint_dict(d)
            self.assertEqual(s.to_checkpoint_dict(), d, f"round-trip failed for {d}")


if __name__ == "__main__":
    unittest.main()
