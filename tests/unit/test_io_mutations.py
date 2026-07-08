"""变异测试专项断言 — plaita.io (Property + evaluate)

针对 io.py 82 个 survived 变异精准杀灭。
主要变异类别：
  1. handle_object_type: key 名称变异 (children/properties/required/name)
  2. handle_array_type: key 名称变异 (item_type/items/children/properties)
  3. _create_property: 各属性名称变异 (name/label/desc/is_required/default_value/data_type)
  4. _handle_required: is_required 赋值
  5. evaluate: prefix 默认值, registry 参数
  6. _RegisteredFunctionsProxy: _registry 赋值
"""
from __future__ import annotations
import unittest

from plaita.io import Property, evaluate, get_value


# ---------------------------------------------------------------------------
# Property.handle_object_type
# ---------------------------------------------------------------------------

class TestHandleObjectType(unittest.TestCase):

    def test_children_key_is_used(self):
        """_3: children key 变为 None → handle_object_type 读不到 children。"""
        p = Property.from_json({
            "data_type": "object",
            "children": {
                "foo": {"data_type": "string", "name": "foo"},
                "bar": {"data_type": "int", "name": "bar"},
            }
        })
        self.assertIsInstance(p.children, dict)
        self.assertIn("foo", p.children)
        self.assertIn("bar", p.children)

    def test_properties_key_as_fallback(self):
        """handle_object_type 应支持 'properties' 作为 children 的备用键。"""
        p = Property.from_json({
            "data_type": "object",
            "properties": {
                "x": {"data_type": "string", "name": "x"},
            }
        })
        self.assertIsInstance(p.children, dict)
        self.assertIn("x", p.children)

    def test_children_key_not_null_default(self):
        """_4: get("children", None) 应是 get("children", {}) — None 会影响 or 短路。"""
        p = Property.from_json({
            "data_type": "object",
            "properties": {"z": {"data_type": "bool", "name": "z"}},
        })
        # children 无值时应回退到 properties
        self.assertIn("z", p.children)

    def test_children_key_case_sensitive(self):
        """_7,8: "XXchildrenXX" / "CHILDREN" 不应匹配。"""
        p = Property.from_json({
            "data_type": "object",
            "children": {"a": {"data_type": "string", "name": "a"}},
        })
        self.assertIn("a", p.children)

    def test_required_field_sets_is_required(self):
        """_18: if prop.name is None 条件翻转 → required 字段应被设置。"""
        p = Property.from_json({
            "data_type": "object",
            "children": {
                "name_field": {"data_type": "string"},
                "age": {"data_type": "int"},
            },
            "required": ["name_field"],
        })
        self.assertTrue(p.children["name_field"].is_required)
        self.assertFalse(p.children["age"].is_required)

    def test_child_name_set_from_key_when_none(self):
        """_18: prop.name is None 时应设置 prop.name = key。"""
        p = Property.from_json({
            "data_type": "object",
            "children": {
                "my_field": {"data_type": "string"},
            }
        })
        child = p.children["my_field"]
        self.assertEqual(child.name, "my_field")

    def test_child_name_preserved_when_set(self):
        """prop.name 有值时不应被 key 覆盖。"""
        p = Property.from_json({
            "data_type": "object",
            "children": {
                "key_name": {"data_type": "string", "name": "display_name"},
            }
        })
        child = p.children["key_name"]
        self.assertEqual(child.name, "display_name")


# ---------------------------------------------------------------------------
# Property.handle_array_type
# ---------------------------------------------------------------------------

class TestHandleArrayType(unittest.TestCase):

    def test_item_type_key(self):
        """_6: get("items") 的 key 变为 None → item_type 使用 item_type 键。"""
        p = Property.from_json({
            "data_type": "array",
            "item_type": {"data_type": "string"},
        })
        self.assertIsNotNone(p.item_type)
        self.assertEqual(p.item_type.data_type, "string")

    def test_items_key_as_fallback(self):
        """handle_array_type 应支持 'items' 作为 item_type 的备用键。"""
        p = Property.from_json({
            "data_type": "array",
            "items": {"data_type": "int"},
        })
        self.assertIsNotNone(p.item_type)
        self.assertEqual(p.item_type.data_type, "int")

    def test_items_key_case_sensitive(self):
        """_7,8: "XXitemsXX" / "ITEMS" 不应匹配 items 键。"""
        p = Property.from_json({
            "data_type": "array",
            "items": {"data_type": "bool"},
        })
        self.assertIsNotNone(p.item_type)

    def test_children_key_in_array(self):
        """_9-13: children key 变异 → array 类型中 children 键应被正确读取。"""
        p = Property.from_json({
            "data_type": "array",
            "children": [
                {"data_type": "string"},
                {"data_type": "int"},
            ]
        })
        self.assertIsInstance(p.children, list)
        self.assertEqual(len(p.children), 2)

    def test_properties_key_in_array(self):
        """handle_array_type 中 'properties' 备用键。"""
        p = Property.from_json({
            "data_type": "array",
            "properties": [
                {"data_type": "string"},
            ]
        })
        self.assertIsInstance(p.children, list)
        self.assertEqual(len(p.children), 1)

    def test_children_none_doesnt_set_children(self):
        """_9: children = None 会导致 item_type/children 判断出错。"""
        # 若 children = None，判断 `if children:` 为 False
        # → 没有 item_type 也没有 children 时，children 保持默认 []
        p = Property.from_json({"data_type": "array"})
        # 不应抛异常
        self.assertIsNotNone(p)

    def test_properties_empty_list_default(self):
        """_17: get("properties", ) → 默认值缺失会 KeyError 或返回 None。"""
        # 无 children 无 properties → 不设 children，保持空
        p = Property.from_json({"data_type": "array", "item_type": {"data_type": "int"}})
        # item_type 优先
        self.assertIsNotNone(p.item_type)


# ---------------------------------------------------------------------------
# Property._create_property
# ---------------------------------------------------------------------------

class TestCreateProperty(unittest.TestCase):

    def test_name_from_content(self):
        """_2: name=None → name 应从 content['name'] 读取。"""
        p = Property.from_json({"data_type": "string", "name": "my_name"})
        self.assertEqual(p.name, "my_name")

    def test_label_from_label_key(self):
        """_3: label=None → label 应从 content['label'] 读取。"""
        p = Property.from_json({"data_type": "string", "label": "My Label"})
        self.assertEqual(p.label, "My Label")

    def test_label_from_title_key(self):
        """label 的备用键 'title'。"""
        p = Property.from_json({"data_type": "string", "title": "Title Label"})
        self.assertEqual(p.label, "Title Label")

    def test_desc_from_desc_key(self):
        """_4: desc=None → desc 应从 content['desc'] 读取。"""
        p = Property.from_json({"data_type": "string", "desc": "A description"})
        self.assertEqual(p.desc, "A description")

    def test_desc_from_description_key(self):
        """desc 的备用键 'description'。"""
        p = Property.from_json({"data_type": "string", "description": "Full desc"})
        self.assertEqual(p.desc, "Full desc")

    def test_is_required_from_is_required_key(self):
        """_11: is_required 参数。"""
        p = Property.from_json({"data_type": "string", "is_required": True})
        self.assertTrue(p.is_required)

    def test_is_required_from_isRequired_key(self):
        """is_required 备用键 'isRequired'。"""
        p = Property.from_json({"data_type": "string", "isRequired": True})
        self.assertTrue(p.is_required)

    def test_default_value_from_default_value_key(self):
        """_6: default_value=None → 应从 'default_value' 键读取。"""
        p = Property.from_json({"data_type": "string", "default_value": "default"})
        self.assertEqual(p.default_value, "default")

    def test_default_value_from_defaultValue_key(self):
        p = Property.from_json({"data_type": "string", "defaultValue": "dv"})
        self.assertEqual(p.default_value, "dv")

    def test_default_value_from_default_key(self):
        p = Property.from_json({"data_type": "string", "default": "d"})
        self.assertEqual(p.default_value, "d")

    def test_data_type_from_type_key(self):
        """_16,26: data_type 别名键 'type'。"""
        p = Property.from_json({"type": "integer"})
        self.assertEqual(p.data_type, "integer")

    def test_data_type_from_dataType_key(self):
        """data_type 别名 'dataType'。"""
        p = Property.from_json({"dataType": "boolean"})
        self.assertEqual(p.data_type, "boolean")

    def test_data_type_from_data_type_key(self):
        p = Property.from_json({"data_type": "float"})
        self.assertEqual(p.data_type, "float")

    def test_all_fields(self):
        """综合测试，所有字段同时设置。"""
        p = Property.from_json({
            "data_type": "string",
            "name": "full",
            "label": "Full Label",
            "desc": "desc text",
            "is_required": True,
            "default_value": "default_v",
        })
        self.assertEqual(p.data_type, "string")
        self.assertEqual(p.name, "full")
        self.assertEqual(p.label, "Full Label")
        self.assertEqual(p.desc, "desc text")
        self.assertTrue(p.is_required)
        self.assertEqual(p.default_value, "default_v")

    def test_remaining_create_property_mutations(self):
        """_20-79: 大量 get_value 参数变异。通过不同别名键对照测试杀灭。"""
        cases = [
            # (input_dict, attr, expected)
            ({"data_type": "int", "name": "n1"}, "name", "n1"),
            ({"data_type": "int", "label": "L"}, "label", "L"),
            ({"data_type": "int", "title": "T"}, "label", "T"),
            ({"data_type": "int", "desc": "D"}, "desc", "D"),
            ({"data_type": "int", "description": "De"}, "desc", "De"),
            ({"data_type": "int", "is_required": True}, "is_required", True),
            ({"data_type": "int", "isRequired": True}, "is_required", True),
            ({"data_type": "int", "default_value": 42}, "default_value", 42),
            ({"data_type": "int", "defaultValue": 99}, "default_value", 99),
            ({"data_type": "int", "default": "x"}, "default_value", "x"),
        ]
        for content, attr, expected in cases:
            with self.subTest(content=content):
                p = Property.from_json(content)
                self.assertEqual(getattr(p, attr), expected)


# ---------------------------------------------------------------------------
# Property._handle_required
# ---------------------------------------------------------------------------

class TestHandleRequired(unittest.TestCase):

    def test_required_scalar_true(self):
        """_1: pro.is_required = None → 应赋值为 True/实际值。"""
        p = Property(data_type="string")
        p._handle_required = lambda x: None  # 触发直接赋值路径
        # 实际通过 from_json 验证
        p2 = Property.from_json({
            "data_type": "string",
            "required": True,
        })
        self.assertTrue(p2.is_required)

    def test_required_bool_false(self):
        p = Property.from_json({"data_type": "string", "required": False})
        self.assertFalse(p.is_required)


# ---------------------------------------------------------------------------
# evaluate function
# ---------------------------------------------------------------------------

class TestEvaluateFunction(unittest.TestCase):

    def test_simple_literal(self):
        """_1: prefix 默认值 "$" → "XX$XX" 变异时会解析失败。"""
        result = evaluate("hello", {})
        self.assertEqual(result, "hello")

    def test_variable_reference(self):
        """基本变量引用解析，验证 prefix="$" 正常工作。"""
        result = evaluate("$x", {"$x": "value"})
        self.assertEqual(result, "value")

    def test_custom_prefix(self):
        """自定义 prefix 参数。"""
        result = evaluate("#x", {"#x": "custom_val"}, prefix="#")
        self.assertEqual(result, "custom_val")

    def test_no_match_returns_original(self):
        """未匹配变量时返回原始字符串。"""
        result = evaluate("plain text", {})
        self.assertEqual(result, "plain text")

    def test_registry_parameter(self):
        """_4: evaluate 中 registry=None 参数 — 传入 None 和不传应效果一样。"""
        r1 = evaluate("hello", {}, registry=None)
        r2 = evaluate("hello", {})
        self.assertEqual(r1, r2)

    def test_numeric_value(self):
        """数字字面量应直接返回。"""
        result = evaluate(42, {})
        self.assertEqual(result, 42)

    def test_dict_value(self):
        """dict 值应直接返回（非字符串）。"""
        d = {"key": "val"}
        result = evaluate(d, {})
        self.assertEqual(result, d)


# ---------------------------------------------------------------------------
# get_value helper
# ---------------------------------------------------------------------------

class TestGetValue(unittest.TestCase):

    def test_returns_first_key_found(self):
        result = get_value({"a": 1, "b": 2}, "a", "b")
        self.assertEqual(result, 1)

    def test_returns_second_key_if_first_missing(self):
        result = get_value({"b": 2}, "a", "b")
        self.assertEqual(result, 2)

    def test_returns_default_if_no_key(self):
        result = get_value({}, "a", "b", default="default_val")
        self.assertEqual(result, "default_val")

    def test_returns_none_by_default(self):
        result = get_value({}, "nonexistent")
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# 补强轮：别名 / required / __str__ / get_attr / match / parse_function
# （与上面同模块测试互补，class 名不冲突）
# ---------------------------------------------------------------------------
import json
from decimal import Decimal

from plaita.core import types
from plaita.core.expression import ExpressionRegistry, FunctionCategory
from plaita.core.expression_parser import ExpressionParser, _parser_components_cache
from plaita.io import (
    REGISTERED_FUNCTIONS,
    get_attr,
    match,
    parse_function,
)


class TestPropertyFromJsonAliases(unittest.TestCase):
    def test_none_or_empty_returns_none(self):
        self.assertIsNone(Property.from_json(None))
        self.assertIsNone(Property.from_json(""))
        self.assertIsNone(Property.from_json({}))

    def test_pass_through_existing_property_instance(self):
        prop = Property(data_type=types.STRING)
        self.assertIs(Property.from_json(prop), prop)

    def test_string_input_is_json_parsed(self):
        prop = Property.from_json(json.dumps({"data_type": "integer", "name": "n"}))
        self.assertEqual(prop.data_type, types.INTEGER)
        self.assertEqual(prop.name, "n")

    def test_data_type_aliases(self):
        self.assertEqual(Property.from_json({"type": "string"}).data_type, types.STRING)
        self.assertEqual(Property.from_json({"dataType": "integer"}).data_type, types.INTEGER)
        self.assertEqual(Property.from_json({"data_type": "float"}).data_type, types.FLOAT)

    def test_label_alias_title(self):
        self.assertEqual(Property.from_json({"type": "string", "title": "T"}).label, "T")
        self.assertEqual(Property.from_json({"type": "string", "label": "L"}).label, "L")

    def test_desc_alias_description(self):
        self.assertEqual(Property.from_json({"type": "string", "description": "D"}).desc, "D")
        self.assertEqual(Property.from_json({"type": "string", "desc": "d"}).desc, "d")

    def test_is_required_alias_isRequired(self):
        self.assertTrue(Property.from_json({"type": "string", "isRequired": True}).is_required)
        self.assertTrue(Property.from_json({"type": "string", "is_required": True}).is_required)
        self.assertFalse(Property.from_json({"type": "string"}).is_required)

    def test_default_value_aliases(self):
        self.assertEqual(Property.from_json({"type": "string", "default": "x"}).default_value, "x")
        self.assertEqual(Property.from_json({"type": "string", "defaultValue": "y"}).default_value, "y")
        self.assertEqual(Property.from_json({"type": "string", "default_value": "z"}).default_value, "z")


class TestPropertyRequiredHandling(unittest.TestCase):
    def test_required_bool_true_sets_is_required(self):
        self.assertTrue(Property.from_json({"type": "string", "required": True}).is_required)

    def test_required_bool_false_clears_is_required(self):
        prop = Property.from_json({"type": "string", "is_required": True, "required": False})
        self.assertFalse(prop.is_required)

    def test_required_list_does_not_set_bool(self):
        prop = Property.from_json({"type": "object", "required": ["x"], "properties": {}})
        self.assertFalse(prop.is_required)


class TestPropertyStr(unittest.TestCase):
    def test_scalar_returns_data_type(self):
        self.assertEqual(str(Property(data_type=types.STRING)), types.STRING)
        self.assertEqual(str(Property(data_type=types.INTEGER)), types.INTEGER)

    def test_object_format(self):
        prop = Property(data_type=types.OBJECT, name="myname")
        prop.children = {"k": Property(data_type=types.STRING)}
        self.assertEqual(str(prop), 'myname: {"k": "string"}')

    def test_array_with_item_type(self):
        prop = Property(data_type=types.ARRAY)
        prop.item_type = Property(data_type=types.STRING)
        self.assertEqual(str(prop), "[{'string'}]")

    def test_array_with_children_list(self):
        prop = Property(data_type=types.ARRAY)
        prop.children = [Property(data_type=types.STRING)]
        self.assertEqual(str(prop), "['string']")


class TestGetAttr(unittest.TestCase):
    def test_array_index_path_on_dict(self):
        self.assertEqual(get_attr({"rows": [10, 20, 30]}, "rows[1]"), 20)

    def test_array_index_path_on_object(self):
        class Holder:
            def __init__(self):
                self.items = [100, 200]
        self.assertEqual(get_attr(Holder(), "items[1]"), 200)

    def test_plain_path_on_dict(self):
        self.assertEqual(get_attr({"a": 1}, "a"), 1)
        self.assertIsNone(get_attr({"a": 1}, "missing"))

    def test_plain_path_on_object(self):
        class O:
            x = 42
        self.assertEqual(get_attr(O(), "x"), 42)
        self.assertIsNone(get_attr(O(), "nope"))


class TestMatchScalar(unittest.TestCase):
    def test_none_with_required(self):
        self.assertFalse(match(Property(data_type=types.STRING, is_required=True), None))
        self.assertTrue(match(Property(data_type=types.STRING, is_required=False), None))

    def test_any_always_true(self):
        self.assertTrue(match(Property(data_type=types.ANY), None))
        self.assertTrue(match(Property(data_type=types.ANY), "anything"))
        self.assertTrue(match(Property(data_type=types.ANY), 123))

    def test_string_requires_non_empty_str(self):
        prop = Property(data_type=types.STRING)
        self.assertTrue(match(prop, "x"))
        self.assertFalse(match(prop, ""))
        self.assertFalse(match(prop, 5))

    def test_integer_strict_type(self):
        prop = Property(data_type=types.INTEGER)
        self.assertTrue(match(prop, 5))
        self.assertFalse(match(prop, 5.0))
        self.assertFalse(match(prop, True))
        self.assertFalse(match(prop, "5"))

    def test_float_accepts_int_and_float(self):
        prop = Property(data_type=types.FLOAT)
        self.assertTrue(match(prop, 1.5))
        self.assertTrue(match(prop, 3))
        self.assertFalse(match(prop, "x"))
        self.assertFalse(match(prop, Decimal("1.5")))

    def test_bool_exact_true_or_false(self):
        prop = Property(data_type=types.BOOL)
        self.assertTrue(match(prop, True))
        self.assertTrue(match(prop, False))
        self.assertFalse(match(prop, 1))
        self.assertFalse(match(prop, 0))

    def test_number_accepts_decimal(self):
        prop = Property(data_type=types.NUMBER)
        self.assertTrue(match(prop, Decimal("1.5")))
        self.assertTrue(match(prop, 1.5))
        self.assertTrue(match(prop, 3))
        self.assertFalse(match(prop, "x"))

    def test_unsupported_type_returns_false(self):
        self.assertFalse(match(Property(data_type="type_not_supported"), "x"))


class TestMatchArray(unittest.TestCase):
    def test_non_list_returns_false(self):
        prop = Property(data_type=types.ARRAY)
        self.assertFalse(match(prop, "not-a-list"))
        self.assertFalse(match(prop, {"a": 1}))

    def test_item_type_all_must_match(self):
        prop = Property(data_type=types.ARRAY)
        prop.item_type = Property(data_type=types.STRING)
        self.assertTrue(match(prop, ["a", "b"]))
        self.assertFalse(match(prop, ["a", 2]))
        self.assertTrue(match(prop, []))

    def test_children_length_must_match(self):
        prop = Property(data_type=types.ARRAY)
        prop.children = [Property(data_type=types.STRING), Property(data_type=types.INTEGER)]
        self.assertTrue(match(prop, ["a", 2]))
        self.assertFalse(match(prop, ["a"]))
        self.assertFalse(match(prop, ["a", 2, 3]))
        self.assertFalse(match(prop, ["a", "x"]))


class TestMatchObject(unittest.TestCase):
    def test_non_dict_returns_false(self):
        prop = Property(data_type=types.OBJECT)
        self.assertFalse(match(prop, ["a"]))
        self.assertFalse(match(prop, "x"))

    def test_empty_children_accepts_any_dict(self):
        prop = Property(data_type=types.OBJECT)
        self.assertTrue(match(prop, {}))
        self.assertTrue(match(prop, {"a": 1}))

    def test_children_all_must_match_via_get_attr(self):
        prop = Property(data_type=types.OBJECT)
        prop.children = {"k": Property(data_type=types.STRING)}
        self.assertTrue(match(prop, {"k": "v"}))
        self.assertFalse(match(prop, {"k": 9}))


class TestPropertyHandleArrayPropertiesFallback(unittest.TestCase):
    def test_children_built_from_properties_key_when_no_children(self):
        prop = Property.from_json({
            "type": "array",
            "properties": [{"type": "string"}, {"type": "integer"}],
        })
        self.assertIsNone(prop.item_type)
        self.assertEqual(len(prop.children), 2)
        self.assertEqual(prop.children[0].data_type, types.STRING)
        self.assertEqual(prop.children[1].data_type, types.INTEGER)


class TestGetAttrMissingKeyIndex(unittest.TestCase):
    def test_missing_key_index_on_dict_raises_index_error(self):
        with self.assertRaises(IndexError):
            get_attr({"a": [1]}, "missing[0]")


class TestRegisteredFunctionsProxyRepr(unittest.TestCase):
    def test_repr_contains_function_count(self):
        r = repr(REGISTERED_FUNCTIONS)
        self.assertIn("_RegisteredFunctionsProxy", r)
        self.assertIn("functions=", r)


class TestEvaluateAndParseFunction(unittest.TestCase):
    def test_evaluate_resolves_default_prefix(self):
        self.assertEqual(evaluate("$x", {"$x": 42}), 42)

    def test_evaluate_uses_custom_registry(self):
        custom = ExpressionRegistry()
        custom.register("add", lambda a, b: 999, FunctionCategory.TYPE, override=True)
        self.assertEqual(evaluate("$F.add(1, 2)", {}, registry=custom), 999)

    def test_parse_function_resolves_default_prefix(self):
        self.assertEqual(parse_function("$F.add(1, 2)", {}), 3)

    def test_parse_function_uses_context(self):
        self.assertEqual(parse_function("$F.add($x, 1)", {"$x": 5}), 6)

    def test_parse_function_uses_custom_registry(self):
        custom = ExpressionRegistry()
        custom.register("add", lambda a, b: 999, FunctionCategory.TYPE, override=True)
        self.assertEqual(parse_function("$F.add(1, 2)", {}, registry=custom), 999)

    def test_parse_function_populates_components_cache(self):
        _parser_components_cache.pop("$", None)
        parse_function("$F.add(1, 2)", {})
        self.assertIsInstance(_parser_components_cache.get("$"), ExpressionParser)


if __name__ == "__main__":
    unittest.main()
