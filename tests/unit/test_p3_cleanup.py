"""P3 清理:
1. plaita.flow shim 的 __all__ 不应导出私有函数 _create_lazy_output/_create_end_output;
2. server 内部 flow_worker 应从 plaita.core 取 Flow/FlowExecution/ExecutionMode,
   而非 deprecated 的 plaita.flow shim。
"""

import ast
import unittest


class TestShimNoPrivateExports(unittest.TestCase):
    def test_flow_shim_all_excludes_private_helpers(self):
        import plaita.flow as shim

        self.assertNotIn("_create_lazy_output", shim.__all__)
        self.assertNotIn("_create_end_output", shim.__all__)


class TestFlowWorkerUsesCorePath(unittest.TestCase):
    def test_flow_worker_imports_from_core_not_shim(self):
        import inspect

        from plaita.server import flow_worker

        src = inspect.getsource(flow_worker)
        # 不应从 deprecated shim 导入 Flow/FlowExecution/ExecutionMode
        self.assertNotIn("from plaita.flow import", src)


if __name__ == "__main__":
    unittest.main()
