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


if __name__ == "__main__":
    unittest.main()
