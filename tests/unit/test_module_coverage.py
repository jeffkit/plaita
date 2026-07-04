"""Tests to improve coverage of low-coverage modules:

- plaita/__init__.py  (57%  → lines 41-48, 148-161, 166)
- plaita/core/__init__.py (50% → lines 78-82)
- plaita/io.py        (81%  → lines 59, 62-64, 72, 77, 81, 113-120, 163, 176, 202-206, 209, 280-287, 305)
- plaita/node/child.py (81% → lines 28, 40-44, 71-75)
"""
from __future__ import annotations

import json
import unittest
from unittest.mock import patch


# ---------------------------------------------------------------------------
# plaita/__init__.py
# ---------------------------------------------------------------------------

class TestPlaitaInit(unittest.TestCase):
    def test_check_extra_available_unknown_name_returns_true(self):
        """Line 41: unknown extra name → always available."""
        from plaita import _check_extra_available
        self.assertTrue(_check_extra_available("totally_unknown_extra"))

    def test_check_extra_available_module_present(self):
        """Line 44-46: known extra whose probe module is installed → True."""
        # 'redis' extra probes 'redis'; may or may not be installed.
        # Use 'http' extra which probes 'requests' (always available).
        from plaita import _check_extra_available
        result = _check_extra_available("http")
        self.assertTrue(result)

    def test_check_extra_available_missing_raises(self):
        """Lines 47-52: missing extra raises ImportError with guidance."""
        from plaita import _check_extra_available, _EXTRAS_GUIDE
        with patch("builtins.__import__", side_effect=ImportError("no module")):
            # Patch __import__ at the right location
            pass
        # Use a fake extra with a module that definitely doesn't exist
        import plaita as _p
        original = _p._EXTRAS_GUIDE.copy()
        _p._EXTRAS_GUIDE["_fake_test"] = ("_nonexistent_xyz_module_abc", "pip install fake")
        try:
            with self.assertRaises(ImportError) as ctx:
                _check_extra_available("_fake_test")
            self.assertIn("_fake_test", str(ctx.exception))
            self.assertIn("pip install fake", str(ctx.exception))
        finally:
            del _p._EXTRAS_GUIDE["_fake_test"]

    def test_getattr_types_module(self):
        """Lines 148-151: `from plaita import types` returns the module."""
        import plaita
        # Remove cached entry if present
        plaita.__dict__.pop("types", None)
        types_mod = plaita.types
        from plaita.core import types as expected
        # Same module
        self.assertIs(types_mod.STRING, expected.STRING)

    def test_getattr_lazy_export(self):
        """Lines 142-146: lazy export resolves correctly."""
        import plaita
        plaita.__dict__.pop("Flow", None)  # remove cache
        from plaita import Flow
        from plaita.core.flow import Flow as ExpectedFlow
        self.assertIs(Flow, ExpectedFlow)

    def test_getattr_feature_extras_available(self):
        """Lines 153-159: optional feature extra available → returns class."""
        import plaita
        # HTTP requires 'requests' which is typically available.
        # Clear cache first.
        plaita.__dict__.pop("HTTP", None)
        try:
            http_cls = plaita.HTTP
            self.assertIsNotNone(http_cls)
        except ImportError:
            self.skipTest("requests not installed")

    def test_getattr_unknown_raises_attribute_error(self):
        """Line 161: completely unknown name raises AttributeError."""
        import plaita
        with self.assertRaises(AttributeError):
            _ = plaita._totally_unknown_xyz

    def test_dir_includes_lazy_exports(self):
        """Line 166: __dir__ includes lazy export names."""
        import plaita
        names = dir(plaita)
        self.assertIn("Flow", names)
        self.assertIn("Node", names)
        self.assertIn("types", names)


# ---------------------------------------------------------------------------
# plaita/core/__init__.py
# ---------------------------------------------------------------------------

class TestPlaitaCoreInit(unittest.TestCase):
    def test_lazy_getattr_flow(self):
        """Lines 78-81: lazy __getattr__ resolves Flow."""
        import plaita.core as core
        # trigger lazy getattr
        flow_cls = core.Flow
        from plaita.core.flow import Flow
        self.assertIs(flow_cls, Flow)

    def test_lazy_getattr_execution_context(self):
        """Lines 78-81: lazy __getattr__ resolves ExecutionContext."""
        import plaita.core as core
        ec = core.ExecutionContext
        from plaita.core.context import ExecutionContext
        self.assertIs(ec, ExecutionContext)

    def test_lazy_getattr_expression_registry(self):
        """Lines 78-81: lazy __getattr__ resolves ExpressionRegistry."""
        import plaita.core as core
        er = core.ExpressionRegistry
        from plaita.core.expression import ExpressionRegistry
        self.assertIs(er, ExpressionRegistry)

    def test_lazy_getattr_unknown_raises(self):
        """Line 82: unknown name raises AttributeError."""
        import plaita.core as core
        with self.assertRaises(AttributeError):
            _ = core._totally_unknown_xyz_abc


# ---------------------------------------------------------------------------
# plaita/io.py
# ---------------------------------------------------------------------------

class TestPropertyFromJson(unittest.TestCase):
    def test_from_json_returns_none_for_empty(self):
        """Line 77: from_json with falsy content returns None."""
        from plaita.io import Property
        self.assertIsNone(Property.from_json(None))
        self.assertIsNone(Property.from_json({}))

    def test_from_json_with_json_string(self):
        """Line 81: from_json parses JSON string input."""
        from plaita.io import Property
        p = Property.from_json('{"dataType": "string"}')
        self.assertEqual(p.data_type, "string")

    def test_from_json_already_property_instance(self):
        """Line 78-79: from_json returns Property instance unchanged."""
        from plaita.io import Property
        original = Property(data_type="string")
        result = Property.from_json(original)
        self.assertIs(result, original)


class TestHandleObjectType(unittest.TestCase):
    def test_handle_object_required_list(self):
        """Lines 59, 62-64: required list marks children as is_required."""
        from plaita.io import Property
        content = {
            "dataType": "object",
            "children": {
                "name": {"dataType": "string"},
                "age": {"dataType": "integer"},
            },
            "required": ["name"],
        }
        p = Property.from_json(content)
        self.assertTrue(p.children["name"].is_required)
        self.assertFalse(p.children["age"].is_required)

    def test_handle_object_child_name_set_from_key(self):
        """Lines 57-60: child with no 'name' gets name set from key."""
        from plaita.io import Property
        content = {
            "dataType": "object",
            "children": {
                "city": {"dataType": "string"},
            },
        }
        p = Property.from_json(content)
        self.assertEqual(p.children["city"].name, "city")


class TestHandleArrayType(unittest.TestCase):
    def test_handle_array_with_children_list(self):
        """Line 72: handle_array_type with children list."""
        from plaita.io import Property
        content = {
            "dataType": "array",
            "children": [
                {"dataType": "string"},
                {"dataType": "integer"},
            ],
        }
        p = Property.from_json(content)
        self.assertEqual(len(p.children), 2)

    def test_handle_array_with_item_type(self):
        """Lines 69-70: handle_array_type with item_type."""
        from plaita.io import Property
        content = {
            "dataType": "array",
            "item_type": {"dataType": "string"},
        }
        p = Property.from_json(content)
        self.assertIsNotNone(p.item_type)
        self.assertEqual(p.item_type.data_type, "string")


class TestPropertyStr(unittest.TestCase):
    def test_str_array_with_item_type(self):
        """Lines 113-115: __str__ for array with item_type."""
        from plaita.io import Property
        p = Property.from_json({
            "dataType": "array",
            "item_type": {"dataType": "string"},
        })
        s = str(p)
        self.assertIn("string", s)

    def test_str_array_with_children(self):
        """Lines 116-117: __str__ for array with children list."""
        from plaita.io import Property
        p = Property.from_json({
            "dataType": "array",
            "children": [{"dataType": "string"}, {"dataType": "integer"}],
        })
        s = str(p)
        self.assertIn("string", s)

    def test_str_object_type(self):
        """Lines 118-119: __str__ for object type."""
        from plaita.io import Property
        p = Property.from_json({
            "dataType": "object",
            "children": {"x": {"dataType": "string"}},
        })
        s = str(p)
        self.assertIn("x", s)


class TestRegisteredFunctionsProxy(unittest.TestCase):
    def test_contains(self):
        """Line 163: __contains__ works."""
        from plaita.io import REGISTERED_FUNCTIONS
        self.assertIn("add", REGISTERED_FUNCTIONS)
        self.assertNotIn("_nonexistent_xyz", REGISTERED_FUNCTIONS)

    def test_repr(self):
        """Line 176: __repr__ returns string."""
        from plaita.io import REGISTERED_FUNCTIONS
        r = repr(REGISTERED_FUNCTIONS)
        self.assertIn("RegisteredFunctionsProxy", r)


class TestGetAttr(unittest.TestCase):
    def test_get_attr_array_index_dict(self):
        """Lines 202-204: get_attr with array index on dict."""
        from plaita.io import get_attr
        obj = {"items": [10, 20, 30]}
        result = get_attr(obj, "items[1]")
        self.assertEqual(result, 20)

    def test_get_attr_array_index_object(self):
        """Lines 205-206: get_attr with array index on object attribute."""
        from plaita.io import get_attr

        class Obj:
            items = [10, 20, 30]

        result = get_attr(Obj(), "items[2]")
        self.assertEqual(result, 30)

    def test_get_attr_object_with_dict(self):
        """Lines 208-209: get_attr on object with __dict__ (non-array)."""
        from plaita.io import get_attr

        class Obj:
            def __init__(self):
                self.name = "alice"

        result = get_attr(Obj(), "name")
        self.assertEqual(result, "alice")


class TestMatchTypes(unittest.TestCase):
    def _prop(self, data_type):
        from plaita.io import Property
        return Property(data_type=data_type)

    def test_match_string_valid(self):
        """Line 280: match string type (type constant is "string")."""
        from plaita.io import match
        self.assertTrue(match(self._prop("string"), "hello"))
        self.assertFalse(match(self._prop("string"), ""))  # empty string falsy
        self.assertFalse(match(self._prop("string"), 42))

    def test_match_integer(self):
        """Line 281-282: match integer type (constant is "integer")."""
        from plaita.io import match
        self.assertTrue(match(self._prop("integer"), 5))
        self.assertFalse(match(self._prop("integer"), 5.0))  # float is not int
        self.assertFalse(match(self._prop("integer"), "5"))

    def test_match_float(self):
        """Lines 283-284: match float type (constant is "float")."""
        from plaita.io import match
        self.assertTrue(match(self._prop("float"), 3.14))
        self.assertTrue(match(self._prop("float"), 3))  # int is float-compatible

    def test_match_bool(self):
        """Lines 285-286: match boolean type (constant is "boolean")."""
        from plaita.io import match
        self.assertTrue(match(self._prop("boolean"), True))
        self.assertTrue(match(self._prop("boolean"), False))
        self.assertFalse(match(self._prop("boolean"), 1))  # int is not bool

    def test_match_number(self):
        """Lines 287: match number type (constant is "number")."""
        from plaita.io import match
        from decimal import Decimal
        self.assertTrue(match(self._prop("number"), 42))
        self.assertTrue(match(self._prop("number"), 3.14))
        self.assertTrue(match(self._prop("number"), Decimal("1.5")))
        self.assertFalse(match(self._prop("number"), "42"))

    def test_match_unknown_type_returns_false(self):
        """Line 287 (else path): unknown type → False."""
        from plaita.io import match
        self.assertFalse(match(self._prop("unknown_type_xyz"), "hello"))


class TestMatchArray(unittest.TestCase):
    def test_match_array_children_length_mismatch(self):
        """Line 305: array with children, length mismatch → False."""
        from plaita.io import Property, match
        p = Property.from_json({
            "dataType": "array",
            "children": [{"dataType": "string"}, {"dataType": "integer"}],
        })
        # Wrong length
        self.assertFalse(match(p, ["hello"]))  # 1 item vs 2 expected
        # Correct length
        self.assertTrue(match(p, ["hello", 42]))


# ---------------------------------------------------------------------------
# plaita/node/child.py
# ---------------------------------------------------------------------------

class TestFlowNodeBase(unittest.TestCase):
    def test_flow_node_execute_returns_none(self):
        """Line 28: FlowNode.execute base class is `pass` (returns None).

        We call execute() directly since NodeRunner prefers arun() when
        available, which means execute() would otherwise never run.
        """
        from plaita.node.child import FlowNode
        from plaita.core.flow import Flow
        from unittest.mock import MagicMock

        child = Flow.model_validate({
            "flow_id": "c1",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": 42},
            ],
        })
        # Use model_construct to bypass validator so child_flow is set
        fn = FlowNode.model_construct(child_flow=child, input=None)
        # execute is a pass statement — returns None
        result = fn.execute(MagicMock())
        self.assertIsNone(result)


class TestInlineFlowExecuteDirect(unittest.TestCase):
    """Test InlineFlow.execute (sync) directly, since NodeRunner may prefer arun."""

    def _make_inline_node(self):
        from plaita.node.child import InlineFlow
        from plaita.core.flow import Flow

        child = Flow.model_validate({
            "flow_id": "cf", "inputType": {"dataType": "any"},
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "$INPUT"},
            ],
        })
        return InlineFlow.model_construct(child_flow=child, input="$INPUT.val")

    def test_inline_flow_execute_direct(self):
        """Lines 40-44: InlineFlow.execute called directly."""
        from plaita.core.executor import FlowExecution
        from plaita.core.strategies import ExecutionMode
        from unittest.mock import MagicMock, patch

        node = self._make_inline_node()

        # Build a mock execution context
        execution = MagicMock()
        execution.mode = ExecutionMode.NORMAL
        execution.evaluate.return_value = "hello_value"
        child_execution = MagicMock()
        child_execution.run_compatible.return_value = "hello_value"
        execution.get_child_execution.return_value = child_execution

        result = node.execute(execution)
        self.assertEqual(result, "hello_value")
        child_execution.run_compatible.assert_called_once()


class TestReferenceFlowExecuteDirect(unittest.TestCase):
    """Test ReferenceFlow.execute (sync) directly."""

    def _make_ref_node(self):
        from plaita.node.child import ReferenceFlow
        from plaita.core.flow import Flow

        child = Flow.model_validate({
            "flow_id": "cf", "inputType": {"dataType": "any"},
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "$INPUT"},
            ],
        })
        # Build ReferenceFlow via model_construct to bypass validator
        return ReferenceFlow.model_construct(
            child_flow=child, input="$INPUT.val",
            node_type="reference",
        )

    def test_reference_flow_execute_direct(self):
        """Lines 71-75: ReferenceFlow.execute called directly."""
        from plaita.core.strategies import ExecutionMode
        from unittest.mock import MagicMock

        node = self._make_ref_node()
        execution = MagicMock()
        execution.mode = ExecutionMode.NORMAL
        execution.evaluate.return_value = "ref_value"
        child_execution = MagicMock()
        child_execution.run_compatible.return_value = "ref_value"
        execution.get_child_execution.return_value = child_execution

        result = node.execute(execution)
        self.assertEqual(result, "ref_value")
        child_execution.run_compatible.assert_called_once()


if __name__ == "__main__":
    unittest.main()
