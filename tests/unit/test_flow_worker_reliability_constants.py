"""钉死 FlowWorker 公开的可靠性相关常量，避免默默改掉语义却不更新文档。"""
from __future__ import annotations

import unittest


class TestFlowWorkerReliabilityConstants(unittest.TestCase):
    def test_persist_every_n_steps_is_documented_default(self):
        from plaita.server.flow_worker import FlowWorker

        self.assertEqual(FlowWorker.PERSIST_EVERY_N_STEPS, 5)
        self.assertGreater(FlowWorker.PERSIST_EVERY_N_STEPS, 1)

    def test_docstring_mentions_at_most_once_for_redis_worker(self):
        from plaita.server.flow_worker import RedisFlowWorker

        doc = RedisFlowWorker.__doc__ or ""
        self.assertIn("at-most-once", doc)
        self.assertIn("blpop", doc)


if __name__ == "__main__":
    unittest.main()
