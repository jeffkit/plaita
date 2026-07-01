"""B1 复现: 子流程回调不应被重复触发两次。

`FlowExecution.get_child_execution()` 创建子执行器时, 应通过
`CallbackManager.child()` 继承父级 handler, 而不是把父级 handler
当作"新增 handler"再传一次 -- 否则 `inherit_handlers=True` 会让每个
handler 在子级出现两份, 子流程的每个生命周期事件都被双发。
"""

import unittest
from unittest.mock import MagicMock

from plaita.core.callback import CallbackManager, FlowCallback
from plaita.core.executor import FlowExecution


class _Flow:
    flow_id = "parent-flow"


class _Node:
    id = "n1"


class TestChildCallbackDedup(unittest.TestCase):
    def test_get_child_execution_does_not_duplicate_handlers(self):
        """get_child_execution 后子级每个 handler 只应出现一次。"""
        handler = MagicMock(spec=FlowCallback)
        parent = FlowExecution(callback_handlers=[handler])

        child = parent.get_child_execution()

        # 子级 handler 列表里, 该 handler 只应出现一次
        self.assertEqual(
            child.callback_manager.handlers.count(handler),
            1,
            f"子级 handler 被重复注册: {child.callback_manager.handlers}",
        )

    def test_child_event_fires_once(self):
        """子级分发一次事件, 父级 handler 应只被调用一次。"""
        handler = MagicMock(spec=FlowCallback)
        parent = FlowExecution(callback_handlers=[handler])
        child = parent.get_child_execution()

        flow, node = _Flow(), _Node()
        child.callback_manager.on_node_start(flow, node)

        handler.on_node_start.assert_called_once_with(flow, node)

    def test_child_manager_direct_does_not_duplicate(self):
        """CallbackManager.child() 自身语义: 不传 handler 时只继承父级各一份。"""
        handler = MagicMock(spec=FlowCallback)
        mgr = CallbackManager([handler])
        child = mgr.child()
        self.assertEqual(child.handlers.count(handler), 1)


if __name__ == "__main__":
    unittest.main()
