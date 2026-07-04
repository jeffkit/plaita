"""Unit tests for plaita/storage/base.py concrete helpers.

Covers:
- serialize_state() / deserialize_state() success and error paths
"""

from __future__ import annotations

import unittest

from plaita.storage.memory import MemoryExecutionStorage


class TestStorageBaseMethods(unittest.TestCase):
    def setUp(self):
        self.storage = MemoryExecutionStorage()

    def test_serialize_state_basic(self):
        """Lines 111-112: serialize_state returns JSON string."""
        result = self.storage.serialize_state({"key": "value", "num": 42})
        import json
        self.assertEqual(json.loads(result), {"key": "value", "num": 42})

    def test_serialize_state_error_raises(self):
        """Lines 113-115: serialize_state re-raises on non-serializable value."""
        class NotSerializable:
            pass

        with self.assertRaises(Exception):
            self.storage.serialize_state({"bad": NotSerializable()})

    def test_deserialize_state_basic(self):
        """Lines 127-128: deserialize_state parses JSON string."""
        result = self.storage.deserialize_state('{"x": 1, "y": "hello"}')
        self.assertEqual(result, {"x": 1, "y": "hello"})

    def test_deserialize_state_error_raises(self):
        """Lines 129-131: deserialize_state re-raises on invalid JSON."""
        with self.assertRaises(Exception):
            self.storage.deserialize_state("not valid json {{{")


if __name__ == "__main__":
    unittest.main()
