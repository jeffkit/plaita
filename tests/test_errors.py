from unittest import TestCase

from plaita.core.errors import FlowErrorType, FlowExecutionException
from plaita.flow import Flow
from plaita.node import Assignment, End, Start


class ErrorsTestCase(TestCase):
    """
    Test cases for error handling
    """

    def test_error_result(self):
        """
        返回结果为错误时，外层抛出FlowExecutionError，error_type为error_result
        """
        flow = Flow(
            flow_id="error-result",
            version="1",
            runtime="python",
            nodes=[
                Start(id="start", next="end"),
                End(id="end", **{"resultType": "error", "error": {"code": 100, "message": "error"}}),
            ],
        )
        try:
            flow.run()
        except FlowExecutionException as cm:
            self.assertEqual(cm.error_type, FlowErrorType.ERROR_RESULT)
        else:
            self.fail("FlowExecutionException not raised")

    def test_node_error(self):
        """
        节点执行异常时，外层抛出FlowExecutionError，error_type为node_error
        """
        flow = Flow(
            flow_id="node-error",
            version="1",
            runtime="python",
            nodes=[
                Start(id="start", next="assign"),
                # 增加赋值节点，引用一个不存在的属性
                Assignment(id="assign", next="end", output="$.not_exist"),
                End(id="end", **{"resultType": "success", "result": "success"}),
            ],
        )
        try:
            flow.run()
        except FlowExecutionException as cm:
            self.assertEqual(cm.error_type, FlowErrorType.NODE_ERROR)
        else:
            self.fail("FlowExecutionException not raised")

    def test_flow_error(self):
        """
        流程自身执行异常，如节点不存在等，外层抛出FlowExecutionError。
        节点缺失统一为 NODE_NOT_FOUND (不再抛裸 ValueError)。
        """
        flow = Flow(
            flow_id="flow-error",
            version="1",
            runtime="python",
            nodes=[
                Start(id="start", next="not_exist"),
                End(id="end", **{"resultType": "success", "result": "success"}),
            ],
        )
        try:
            flow.run()
        except FlowExecutionException as cm:
            self.assertEqual(cm.error_type, FlowErrorType.NODE_NOT_FOUND)
        else:
            self.fail("FlowExecutionException not raised")
