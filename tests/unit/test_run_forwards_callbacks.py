"""A3 复现: FlowExecution.run classmethod 应转发 callback_handlers。

``FlowExecution.run(...)`` 目前用 ``cls(event_bus=event_bus)`` 构造, 忽略
用户回调, 导致通过 classmethod 入口(如 FlowWorker 既往用法)无法挂回调。
应支持 ``callback_handlers`` 参数并转发给构造器。
"""

import unittest
from unittest.mock import MagicMock

from plaita.core import types
from plaita.core.callback import FlowCallback
from plaita.core.executor import FlowExecution
from plaita.io import Property
from plaita.node import End, Start


def _simple_flow():
    from plaita.core.flow import Flow
    return Flow(
        flow_id="a3-run",
        version="1.0",
        runtime="python",
        output_type=Property(data_type=types.STRING, name="r"),
        nodes=[
            Start(id="start", next="end"),
            End(id="end", **{"resultType": "success", "output": "ok"}),
        ],
    )


class TestRunClassmethodForwardsCallbacks(unittest.TestCase):
    def test_run_accepts_and_invokes_callback_handlers(self):
        handler = MagicMock(spec=FlowCallback)
        FlowExecution.run(_simple_flow(), params={}, callback_handlers=[handler])
        handler.on_flow_start.assert_called_once()
        handler.on_flow_end.assert_called_once()

    def test_run_callback_counts_single_dispatch(self):
        class Counter(FlowCallback):
            def __init__(self):
                self.starts = 0
                self.ends = 0

            def on_flow_start(self, flow, **k):
                self.starts += 1

            def on_flow_end(self, flow, result=None, error=None, exception=None, **k):
                self.ends += 1

        c = Counter()
        FlowExecution.run(_simple_flow(), params={}, callback_handlers=[c])
        self.assertEqual(c.starts, 1)
        self.assertEqual(c.ends, 1)


if __name__ == "__main__":
    unittest.main()
