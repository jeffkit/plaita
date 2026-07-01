"""B7: plaita.core.executor 不应再在模块末尾兜底导入 NodeException。

该导入既晚又未被本模块使用, 属于死代码。
"""

import unittest


class TestExecutorNoDeadNodeExceptionImport(unittest.TestCase):
    def test_executor_does_not_reexport_node_exception(self):
        import plaita.core.executor as executor

        self.assertFalse(
            hasattr(executor, "NodeException"),
            "plaita.core.executor 不应再导出 NodeException (死导入)",
        )


if __name__ == "__main__":
    unittest.main()
