"""P3 清理 (0.5.0 起):
1. ``plaita.flow`` deprecated shim 已在 0.5.0 删除——本测试钉死"模块不再存在";
2. server 内部 flow_worker 应从 ``plaita.core`` 取 Flow/FlowExecution/ExecutionMode,
   而非已删除的 ``plaita.flow`` shim。
"""

import importlib
import inspect
import unittest


class TestShimRemoved(unittest.TestCase):
    def test_plaita_flow_shim_is_gone(self):
        """0.5.0 删了 ``plaita.flow`` shim, import 该模块必须 ImportError。"""
        with self.assertRaises(ImportError):
            importlib.import_module("plaita.flow")


class TestFlowWorkerUsesCorePath(unittest.TestCase):
    def test_flow_worker_imports_from_core_not_shim(self):
        from plaita.server import flow_worker

        src = inspect.getsource(flow_worker)
        # shim 已删, 任何残留 import 都会运行期炸, 这里再钉一道
        self.assertNotIn("from plaita.flow import", src)


if __name__ == "__main__":
    unittest.main()
