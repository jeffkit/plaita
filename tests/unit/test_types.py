from unittest import TestCase

from plaita.core import types


# 测试types的validate方法
class TypesTestCase(TestCase):

    def test_get_native_type(self):
        self.assertEqual("str", types.get_native_type(types.STRING))
        self.assertEqual("int", types.get_native_type(types.INTEGER))
        self.assertEqual("float", types.get_native_type(types.FLOAT))
        self.assertEqual("Decimal", types.get_native_type(types.DECIMAL))
        self.assertEqual("float", types.get_native_type(types.NUMBER))
        self.assertEqual("list", types.get_native_type(types.ARRAY))
        self.assertEqual("dict", types.get_native_type(types.MAP))
        self.assertEqual("dict", types.get_native_type(types.OBJECT))
        self.assertEqual("str", types.get_native_type(types.TYPE))
        self.assertEqual("datetime", types.get_native_type(types.DATETIME))
        self.assertEqual("date", types.get_native_type(types.DATE))
        self.assertEqual("float", types.get_native_type(types.TIMESTAMP))
        with self.assertRaises(ValueError):
            types.get_native_type("unknown")

    def test_validate(self):
        types.valid(types.STRING, "hello")
        types.valid(types.BOOL, True)
        types.valid(types.INTEGER, 1)
        types.valid(types.FLOAT, 1.1)
        types.valid(types.ARRAY, [1, 2, 3])
        types.valid(types.MAP, {"name": "tom"})
        types.valid(types.OBJECT, {"name": "tom"})

        # 测试数据类型不匹配
        with self.assertRaises(types.ValidationError) as ex:
            types.valid(types.INTEGER, "1")
        self.assertEqual(f'必须是{types.INTEGER}类型的值，但是传入了"1"', str(ex.exception))

        # 测试字符串长度超长
        with self.assertRaises(types.ValidationError) as ex:
            types.valid(types.STRING, "123456", [{"name": "max_length", "length": 5}])
        self.assertEqual("长度不能超过5", str(ex.exception))

        # 测试整数最小值
        with self.assertRaises(types.ValidationError) as ex:
            types.valid(types.INTEGER, 1, [{"name": "min", "min_value": 2}])
        self.assertEqual("不能小于2", str(ex.exception))

        # 测试整数最大值
        with self.assertRaises(types.ValidationError) as ex:
            types.valid(types.INTEGER, 3, [{"name": "max", "max_value": 2}])
        self.assertEqual("不能大于2", str(ex.exception))

        # 测试自定义validator
        types.register_validator("min_length", lambda value, length: len(value) >= length, "长度不能小于{length}")
        with self.assertRaises(types.ValidationError) as ex:
            types.valid(types.STRING, "123", [{"name": "min_length", "length": 5}])
        self.assertEqual("长度不能小于5", str(ex.exception))
