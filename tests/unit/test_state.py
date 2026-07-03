"""Tests for the distributed-checkpoint schema (plaita.core.state).

Goal: stop the silent drift where every caller invents its own
``f"{pfx}SOME_KEY"`` magic string. Any new system key must be registered in
``CheckpointSchema.SYSTEM_KEY_NAMES`` — otherwise ``validate_checkpoint``
will warn when the context is serialised.
"""

import unittest

from plaita.core.state import CheckpointSchema, validate_checkpoint


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


if __name__ == "__main__":
    unittest.main()
