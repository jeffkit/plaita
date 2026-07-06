"""变异测试专项断言 — plaita.core.errors

针对 errors.py 的 34 个 survived 变异精准杀灭。
主要变异类别：
  1. super().__init__(None) — 消息参数变为 None
  2. self.code/message/error_type/node/source_line = None
  3. 条件逻辑翻转（is not None → is None，>= → >，None → "NODE NOT FOUND"）
  4. 默认参数字符串变为 "XXXX"
"""
from __future__ import annotations
import unittest

from plaita.core.errors import (
    DEFAULT_NODE_ABORT_CODE,
    ErrorResultException,
    ErrorStrategy,
    FlowErrorType,
    FlowExecutionException,
    FlowResultError,
    FlowStartMissingError,
    FlowTimeoutError,
    NodeExecutionError,
    NodeNotFoundError,
    NodeTimeoutError,
    ResumeError,
    ResumeType,
)


# ---------------------------------------------------------------------------
# FlowResultError
# ---------------------------------------------------------------------------

class TestFlowResultError(unittest.TestCase):

    def test_message_format(self):
        """_1: super().__init__(None) — 消息被置为 None。"""
        err = FlowResultError(code=42, message="bad")
        msg = str(err)
        self.assertIn("42", msg)
        self.assertIn("bad", msg)
        self.assertIn("FlowResultError", msg)

    def test_code_attribute(self):
        """_2: self.code = None。"""
        err = FlowResultError(code=99, message="x")
        self.assertEqual(err.code, 99)

    def test_message_attribute(self):
        """_3: self.message = None。"""
        err = FlowResultError(code=1, message="hello")
        self.assertEqual(err.message, "hello")

    def test_code_none_allowed(self):
        """Baseline: code=None message=None 应可正常构造。"""
        err = FlowResultError()
        # 不报错即可
        self.assertIsNotNone(str(err))


# ---------------------------------------------------------------------------
# FlowExecutionException
# ---------------------------------------------------------------------------

class TestFlowExecutionException(unittest.TestCase):

    def test_default_message_is_empty_string(self):
        """_1: message 默认值 "" → "XXXX"。"""
        err = FlowExecutionException()
        self.assertEqual(err.message, "")

    def test_code_attribute(self):
        """_2: self.code = None。"""
        err = FlowExecutionException(code=123, message="m")
        self.assertEqual(err.code, 123)

    def test_message_attribute(self):
        """_3: self.message = None。"""
        err = FlowExecutionException(code=1, message="test_msg")
        self.assertEqual(err.message, "test_msg")

    def test_error_type_explicit(self):
        """_5: 条件翻转 (is not None → is None) 导致 explicit error_type 被忽略。"""
        err = FlowExecutionException(
            code=-500, message="m", error_type=FlowErrorType.NODE_ERROR
        )
        self.assertEqual(err.error_type, FlowErrorType.NODE_ERROR)

    def test_error_type_default_from_class(self):
        """_6: type(None).error_type 替换 type(self).error_type — 无 error_type 时应用类默认值。"""
        err = FlowExecutionException(code=-500, message="m")
        self.assertEqual(err.error_type, FlowErrorType.FLOW_ERROR)

    def test_node_attribute(self):
        """_7: self.node = None。"""
        node_obj = object()
        err = FlowExecutionException(node=node_obj)
        self.assertIs(err.node, node_obj)

    def test_source_line_attribute(self):
        """_8: self.source_line = None。"""
        err = FlowExecutionException(source_line=42)
        self.assertEqual(err.source_line, 42)

    def test_super_init_message(self):
        """_9: super().__init__(None) — str(err) 不应为 None。"""
        err = FlowExecutionException(code=1, message="visible")
        self.assertIn("visible", str(err))


# ---------------------------------------------------------------------------
# NodeNotFoundError
# ---------------------------------------------------------------------------

class TestNodeNotFoundError(unittest.TestCase):

    def test_message_with_node_id(self):
        """_1: message is not None 条件翻转 → node_id 时总生成默认消息。"""
        err = NodeNotFoundError(node_id="abc")
        self.assertIn("abc", str(err))

    def test_message_without_node_id(self):
        """_2: message 默认路径。"""
        err = NodeNotFoundError()
        self.assertIn("not found", str(err).lower())

    def test_message_without_node_id_exact(self):
        """_3: 无 node_id 时消息应为 'Node not found'（非 'NODE NOT FOUND'）。"""
        err = NodeNotFoundError()
        self.assertEqual(str(err), "Node not found")

    def test_explicit_message_preserved(self):
        """_4: 显式消息应保留。"""
        err = NodeNotFoundError(message="custom msg")
        self.assertEqual(str(err), "custom msg")

    def test_default_message_for_none_id(self):
        """_5: node_id=None 与 node_id="" 应都用默认消息。"""
        self.assertEqual(str(NodeNotFoundError(node_id=None)), "Node not found")
        self.assertEqual(str(NodeNotFoundError(node_id="")), "Node not found")

    def test_code_is_minus_500(self):
        """_6: super().__init__(None, ...) → code 应为 -500。"""
        err = NodeNotFoundError(node_id="x")
        self.assertEqual(err.code, -500)

    def test_error_type(self):
        """_7: error_type 应为 NODE_NOT_FOUND。"""
        err = NodeNotFoundError()
        self.assertEqual(err.error_type, FlowErrorType.NODE_NOT_FOUND)

    def test_node_attribute_preserved(self):
        """_8: node 参数应被保留。"""
        n = object()
        err = NodeNotFoundError(node=n)
        self.assertIs(err.node, n)

    def test_node_attribute_is_none_by_default(self):
        """_9: super().__init__(self.code, message, self.error_type, None) — node 默认 None。"""
        err = NodeNotFoundError()
        self.assertIsNone(err.node)

    def test_message_contains_node_id(self):
        """_10,11,12,13: 各种变异后消息内容。"""
        err = NodeNotFoundError(node_id="my_node")
        self.assertIn("my_node", str(err))
        self.assertIn("not found", str(err).lower())


# ---------------------------------------------------------------------------
# NodeExecutionError
# ---------------------------------------------------------------------------

class TestNodeExecutionError(unittest.TestCase):

    def test_code_is_default_abort_code(self):
        """_1: super().__init__(None, ...) — code 应为 DEFAULT_NODE_ABORT_CODE。"""
        err = NodeExecutionError(message="err")
        self.assertEqual(err.code, DEFAULT_NODE_ABORT_CODE)

    def test_message_preserved(self):
        """_2: message 应被保留。"""
        err = NodeExecutionError(message="the error")
        self.assertEqual(err.message, "the error")

    def test_error_type(self):
        """_3: error_type 应为 NODE_ERROR。"""
        err = NodeExecutionError(message="e")
        self.assertEqual(err.error_type, FlowErrorType.NODE_ERROR)

    def test_node_preserved(self):
        """_4: node 参数应被保留（_8 removes node param）。"""
        n = object()
        err = NodeExecutionError(message="e", node=n)
        self.assertIs(err.node, n)

    def test_custom_code(self):
        """_8: super().__init__(code, message, self.error_type, ) — node 丢失。
        用 node= 参数验证 node 正确存储。"""
        err = NodeExecutionError(message="e", code=-9999, node=object())
        self.assertEqual(err.code, -9999)
        self.assertIsNotNone(err.node)


# ---------------------------------------------------------------------------
# ErrorResultException
# ---------------------------------------------------------------------------

class TestErrorResultException(unittest.TestCase):

    def test_code_preserved(self):
        """_1: super().__init__(None, ...) — code 应为用户传入值。"""
        err = ErrorResultException(code=404, message="not found")
        self.assertEqual(err.code, 404)

    def test_message_preserved(self):
        """_2: message 参数。"""
        err = ErrorResultException(code=500, message="server error")
        self.assertEqual(err.message, "server error")

    def test_error_type(self):
        """_3: error_type 应为 ERROR_RESULT。"""
        err = ErrorResultException(code=1, message="m")
        self.assertEqual(err.error_type, FlowErrorType.ERROR_RESULT)

    def test_node_preserved(self):
        """_4: node 参数。"""
        n = object()
        err = ErrorResultException(code=1, message="m", node=n)
        self.assertIs(err.node, n)

    def test_node_none_by_default(self):
        """_8: super().__init__(code, message, self.error_type, ) — node 丢失。"""
        err = ErrorResultException(code=1, message="m")
        self.assertIsNone(err.node)

    def test_str_representation(self):
        """组合断言：str() 不应为 None 或空。"""
        err = ErrorResultException(code=200, message="ok")
        s = str(err)
        self.assertEqual(s, "ok")


# ---------------------------------------------------------------------------
# 其他错误类
# ---------------------------------------------------------------------------

class TestResumeType(unittest.TestCase):

    def test_coerce_from_string(self):
        self.assertEqual(ResumeType.coerce("continue"), ResumeType.CONTINUE)
        self.assertEqual(ResumeType.coerce("cancel"), ResumeType.CANCEL)
        self.assertEqual(ResumeType.coerce("timeout"), ResumeType.TIMEOUT)
        self.assertEqual(ResumeType.coerce("event"), ResumeType.EVENT)

    def test_coerce_from_enum(self):
        self.assertEqual(ResumeType.coerce(ResumeType.CONTINUE), ResumeType.CONTINUE)

    def test_coerce_invalid_raises(self):
        with self.assertRaises(ResumeError):
            ResumeType.coerce("invalid_type")

    def test_coerce_non_str_non_enum_raises(self):
        with self.assertRaises(ResumeError):
            ResumeType.coerce(123)


class TestFlowStartMissingError(unittest.TestCase):

    def test_default_message(self):
        err = FlowStartMissingError()
        self.assertIn("start", str(err).lower())
        self.assertEqual(err.error_type, FlowErrorType.NODE_NOT_FOUND)


class TestNodeTimeoutError(unittest.TestCase):

    def test_node_error_type(self):
        err = NodeTimeoutError("timeout!")
        self.assertEqual(err.error_type, FlowErrorType.NODE_ERROR)

    def test_flow_error_type(self):
        err = NodeTimeoutError("flow timeout", error_type=FlowErrorType.FLOW_ERROR)
        self.assertEqual(err.error_type, FlowErrorType.FLOW_ERROR)


class TestFlowTimeoutError(unittest.TestCase):

    def test_default_message(self):
        err = FlowTimeoutError()
        self.assertIn("timeout", str(err).lower())
        self.assertEqual(err.error_type, FlowErrorType.FLOW_ERROR)


if __name__ == "__main__":
    unittest.main()
