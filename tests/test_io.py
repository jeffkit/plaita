from datetime import date, datetime
from unittest import TestCase

from plaita.core import types
from plaita.io import REGISTERED_FUNCTIONS, Property


class IOTestCase(TestCase):

    def test_create_property_from_dict(self):
        schema_data = {
            "name": "",
            "label": "",
            "data_type": "object",
            "required": False,
            "default": "",
            "ref": "",
            "properties": {
                "comboPname": {
                    "label": "项目名称",
                    "name": "comboPname",
                    "data_type": "string",
                    "default": "",
                    "required": True,
                    "ref": "",
                    "desc": "",
                },
                "pstatus": {
                    "label": "开发状态",
                    "name": "pstatus",
                    "data_type": "array",
                    "default": "",
                    "required": False,
                    "ref": "",
                    "desc": "",
                    "item_type": {"data_type": "string"},
                },
                "managementStatus": {
                    "label": "管理状态",
                    "name": "managementStatus",
                    "data_type": "array",
                    "default": "",
                    "required": False,
                    "ref": "",
                    "desc": "",
                    "item_type": {"data_type": "string"},
                },
                "contentRating": {
                    "label": "项目评级",
                    "name": "contentRating",
                    "data_type": "array",
                    "default": "",
                    "required": False,
                    "ref": "",
                    "desc": "",
                    "item_type": {"data_type": "string"},
                },
                "studio": {
                    "label": "工作室名称",
                    "name": "studio",
                    "data_type": "array",
                    "default": "",
                    "required": False,
                    "ref": "",
                    "desc": "",
                    "item_type": {"data_type": "string"},
                },
                "topicType": {
                    "label": "题材类型",
                    "name": "topicType",
                    "data_type": "array",
                    "default": "",
                    "required": False,
                    "ref": "",
                    "desc": "",
                    "item_type": {"data_type": "string"},
                },
                "pageIndex": {
                    "label": "pageIndex",
                    "name": "pageIndex",
                    "data_type": "integer",
                    "default": 0,
                    "required": False,
                    "ref": "",
                },
                "pageSize": {
                    "label": "pageSize",
                    "name": "pageSize",
                    "data_type": "integer",
                    "default": 1000,
                    "required": False,
                    "ref": "",
                },
                "category": {
                    "label": "category",
                    "name": "category",
                    "data_type": "string",
                    "default": "2",
                    "required": False,
                    "ref": "",
                },
                "belong": {
                    "label": "belong",
                    "name": "belong",
                    "data_type": "string",
                    "default": "1",
                    "required": False,
                    "ref": "",
                },
                "producer": {
                    "label": "制片人",
                    "name": "producer",
                    "data_type": "array",
                    "default": "",
                    "required": False,
                    "ref": "",
                    "desc": "",
                    "item_type": {"data_type": "string"},
                },
                "topicTrack": {
                    "label": "赛道",
                    "name": "topicTrack",
                    "data_type": "array",
                    "default": "",
                    "required": False,
                    "ref": "",
                    "desc": "",
                    "item_type": {"data_type": "string"},
                },
                "cooperationMode": {
                    "label": "合作模式",
                    "name": "cooperationMode",
                    "data_type": "array",
                    "default": "",
                    "required": False,
                    "ref": "",
                    "desc": "",
                    "item_type": {"data_type": "string"},
                },
                "uptime": {
                    "label": "计划开机年份",
                    "name": "uptime",
                    "data_type": "string",
                    "default": "",
                    "required": False,
                    "desc": "",
                },
                "reviewedDeliverDate": {
                    "label": "按计划过审待播时间过滤",
                    "name": "reviewedDeliverDate",
                    "data_type": "string",
                    "default": "",
                    "required": False,
                    "desc": "",
                },
                "manageOnlineTime": {
                    "label": "按上线时间过滤",
                    "name": "manageOnlineTime",
                    "data_type": "string",
                    "default": "",
                    "required": False,
                    "desc": "",
                },
                "isIp": {
                    "label": "按IP类型过滤",
                    "name": "isIp",
                    "data_type": "string",
                    "default": "",
                    "required": False,
                    "desc": "",
                },
            },
        }
        p = Property.from_json(schema_data)
        self.assertEqual(types.OBJECT, p.data_type)
        self.assertEqual(17, len(p.children))
        self.assertEqual(types.STRING, p.children["comboPname"].data_type)
        self.assertEqual(types.ARRAY, p.children["pstatus"].data_type)
        self.assertEqual(types.STRING, p.children["pstatus"].item_type.data_type)


class TestRegisteredFunctions(TestCase):
    # 数学运算测试
    def test_math_operations(self):
        self.assertEqual(REGISTERED_FUNCTIONS["add"](5, 3), 8)
        self.assertEqual(REGISTERED_FUNCTIONS["sub"](5, 3), 2)
        self.assertEqual(REGISTERED_FUNCTIONS["mul"](5, 3), 15)
        self.assertEqual(REGISTERED_FUNCTIONS["div"](6, 3), 2)
        self.assertEqual(REGISTERED_FUNCTIONS["mod"](5, 3), 2)
        self.assertEqual(REGISTERED_FUNCTIONS["pow"](2, 3), 8)
        self.assertEqual(REGISTERED_FUNCTIONS["abs"](-5), 5)
        self.assertEqual(REGISTERED_FUNCTIONS["ceil"](2.3), 3)
        self.assertEqual(REGISTERED_FUNCTIONS["floor"](2.7), 2)
        self.assertEqual(REGISTERED_FUNCTIONS["round"](2.3456, 0), 2)
        self.assertEqual(REGISTERED_FUNCTIONS["round"](2.3456, 2), 2.35)
        self.assertEqual(REGISTERED_FUNCTIONS["round"](2.3456, 5), 2.3456)
        self.assertEqual(REGISTERED_FUNCTIONS["trunc"](2.3456), 2)
        self.assertEqual(REGISTERED_FUNCTIONS["sqrt"](4), 2)

    # 字符串操作测试
    def test_string_operations(self):
        self.assertEqual(REGISTERED_FUNCTIONS["lower"]("HELLO"), "hello")
        self.assertEqual(REGISTERED_FUNCTIONS["upper"]("hello"), "HELLO")
        self.assertEqual(REGISTERED_FUNCTIONS["capitalize"]("hello"), "Hello")
        self.assertEqual(REGISTERED_FUNCTIONS["title"]("hello world"), "Hello World")
        self.assertEqual(REGISTERED_FUNCTIONS["strip"]("  hello  "), "hello")
        self.assertEqual(REGISTERED_FUNCTIONS["lstrip"]("  hello  "), "hello  ")
        self.assertEqual(REGISTERED_FUNCTIONS["rstrip"]("  hello  "), "  hello")
        self.assertEqual(REGISTERED_FUNCTIONS["replace"]("hello world", "world", "Python"), "hello Python")
        self.assertEqual(REGISTERED_FUNCTIONS["split"]("a,b,c", ","), ["a", "b", "c"])
        self.assertEqual(REGISTERED_FUNCTIONS["concat"]("hello", " ", "world"), "hello world")
        self.assertTrue(REGISTERED_FUNCTIONS["startswith"]("hello", "he"))
        self.assertTrue(REGISTERED_FUNCTIONS["endswith"]("hello", "lo"))

    # 逻辑运算测试
    def test_logical_operations(self):
        self.assertTrue(REGISTERED_FUNCTIONS["and"](True, True))
        self.assertFalse(REGISTERED_FUNCTIONS["and"](True, False))
        self.assertTrue(REGISTERED_FUNCTIONS["or"](False, True))
        self.assertFalse(REGISTERED_FUNCTIONS["or"](False, False))
        self.assertTrue(REGISTERED_FUNCTIONS["or"](False, False, True))
        self.assertTrue(REGISTERED_FUNCTIONS["not"](False))
        self.assertFalse(REGISTERED_FUNCTIONS["not"](True))

    # 数组操作测试
    def test_array_operations(self):
        lst = [1, 2, 3]
        self.assertEqual(REGISTERED_FUNCTIONS["len"](lst), 3)
        self.assertEqual(REGISTERED_FUNCTIONS["length"](lst), 3)
        self.assertEqual(REGISTERED_FUNCTIONS["index"](lst, 2), 1)
        self.assertEqual(REGISTERED_FUNCTIONS["slice"](lst, 1, 3), [2, 3])
        self.assertEqual(REGISTERED_FUNCTIONS["append"](lst, 4), [1, 2, 3, 4])
        self.assertEqual(REGISTERED_FUNCTIONS["extend"](lst, [4, 5]), [1, 2, 3, 4, 5])
        self.assertEqual(REGISTERED_FUNCTIONS["insert"](lst, 1, 2), [1, 2, 2, 3])
        self.assertEqual(REGISTERED_FUNCTIONS["remove"](lst, 3), [1, 2])
        self.assertEqual(REGISTERED_FUNCTIONS["reverse"](lst), [3, 2, 1])
        self.assertEqual(REGISTERED_FUNCTIONS["sort"](lst), [1, 2, 3])
        self.assertEqual(REGISTERED_FUNCTIONS["sort"](lst, None, True), [3, 2, 1])
        lst = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 20}]
        self.assertEqual(
            REGISTERED_FUNCTIONS["sort"](lst, "age"), [{"name": "Bob", "age": 20}, {"name": "Alice", "age": 25}]
        )
        self.assertEqual(
            REGISTERED_FUNCTIONS["sort"](lst, "age", True), [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 20}]
        )
        lst = [1, 2, 3]
        self.assertEqual(REGISTERED_FUNCTIONS["getListItem"](lst, 1), 2)
        REGISTERED_FUNCTIONS["setListItem"](lst, 1, 4)
        self.assertEqual(lst, [1, 4, 3])
        self.assertEqual(REGISTERED_FUNCTIONS["addListItem"](lst, 5), [1, 4, 3, 5])
        self.assertEqual(lst, [1, 4, 3])
        REGISTERED_FUNCTIONS["delListItem"](lst, 1)
        self.assertEqual(lst, [1, 3])
        self.assertEqual(REGISTERED_FUNCTIONS["pop"](lst, 1), 3)
        self.assertEqual(lst, [1])

    # 字典操作测试
    def test_dict_operations(self):
        dct = {"a": 1, "b": 2}
        self.assertEqual(list(REGISTERED_FUNCTIONS["keys"](dct)), ["a", "b"])
        self.assertEqual(list(REGISTERED_FUNCTIONS["values"](dct)), [1, 2])
        self.assertEqual(list(REGISTERED_FUNCTIONS["items"](dct)), [("a", 1), ("b", 2)])
        self.assertEqual(REGISTERED_FUNCTIONS["get"](dct, "a"), 1)
        self.assertEqual(REGISTERED_FUNCTIONS["get"](dct, "c", "default"), "default")
        REGISTERED_FUNCTIONS["set"](dct, "b", 3)
        self.assertEqual(dct, {"a": 1, "b": 3})
        REGISTERED_FUNCTIONS["delete"](dct, "b")
        self.assertEqual(dct, {"a": 1})
        REGISTERED_FUNCTIONS["clear"](dct)
        self.assertEqual(dct, {})
        dct = {"a": 1, "b": 2}
        self.assertEqual(REGISTERED_FUNCTIONS["getDictValue"](dct, "a"), 1)
        self.assertEqual(REGISTERED_FUNCTIONS["getDictValue"](dct, "c", "default"), "default")
        REGISTERED_FUNCTIONS["setDictValue"](dct, "b", 3)
        self.assertEqual(dct, {"a": 1, "b": 3})
        REGISTERED_FUNCTIONS["delDictValue"](dct, "b")
        self.assertEqual(dct, {"a": 1})
        self.assertEqual(list(REGISTERED_FUNCTIONS["getDictKeys"](dct)), ["a"])
        self.assertEqual(list(REGISTERED_FUNCTIONS["getDictValues"](dct)), [1])
        REGISTERED_FUNCTIONS["clearDict"](dct)
        self.assertEqual(dct, {})

    # 测试日期时间操作
    def test_date_time_operations(self):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.assertEqual(REGISTERED_FUNCTIONS["now"](), now)
        today = date.today().strftime("%Y-%m-%d")
        self.assertEqual(REGISTERED_FUNCTIONS["today"](), today)

        fmt_time = "%d/%m/%Y %H:%M:%S"
        fmt_date = "%d/%m/%Y"
        now = datetime.now().strftime(fmt_time)
        self.assertEqual(REGISTERED_FUNCTIONS["now"](fmt_time), now)
        today = date.today().strftime(fmt_date)
        self.assertEqual(REGISTERED_FUNCTIONS["today"](fmt_date), today)

    # 测试 JSON 操作
    def test_json_operations(self):
        obj = {"key": "value"}
        json_str = '{"key": "value"}'
        self.assertEqual(REGISTERED_FUNCTIONS["json_loads"](json_str), obj)
        self.assertEqual(REGISTERED_FUNCTIONS["json_dumps"](obj), json_str)
