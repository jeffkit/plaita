"""Track C 任务2核实+回归：审批/回调状态必须落 Redis 跨实例共享，而非进程内存。

历史缺陷：``ApprovalService.pending_approvals`` 与
``HttpCallbackService.registered_callbacks`` 均为进程内 dict——
1. 服务重启全丢：审批中/已注册回调永久失联；
2. 多实例部署时决策提交到另一实例查无此审批。

修复后：记录迁 Redis（``plaita:approval:pending:{id}`` /
``plaita:http_callback:registered:{path}``，JSON + 7 天 TTL 自清理，与仓内
事件/订阅键 TTL 惯例一致）；决策提交经每审批 SET NX 锁串行化
（execution_lease / event_filter 的既有原子原语风格，compare-and-del 释放），
同一审批人跨实例重复提交被原子拦住。无 redis_client（单测/内存模式）回退
进程内 dict，历史行为与形状（get_pending_approvals / get_approval_details /
get_registered_callbacks 的返回结构，examples/server_demo 在消费）不变。
"""
from __future__ import annotations

import asyncio
import unittest

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.approval_service import ApprovalService
from plaita.server.services.http_callback_service import HttpCallbackService

APPROVAL_TTL = 7 * 86400


def _approval_task(
    approval_id="a1", execution_id="e1", strategy="all", approvers=("u1", "u2")
):
    return {
        "approval_id": approval_id,
        "node_id": "approval_1",
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "approval_decision",
        "approval_config": {"title": "请审批"},
        "approver_config": {"approvers": list(approvers), "strategy": strategy},
    }


def _callback_task(path="/cb/1", execution_id="e1"):
    return {
        "node_id": "cb_1",
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "http_callback_triggered",
        "callback_config": {"path": path},
    }


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class ApprovalRedisStateTest(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()

    def make(self):
        return ApprovalService(self.bus, {}, redis_client=self.redis)

    def test_two_instances_share_pending_approvals(self):
        """实例 A 创建的审批，实例 B 可见可审（修复前：各自内存，红）。"""
        svc1, svc2 = self.make(), self.make()
        _run(svc1.handle_task(_approval_task()))
        pending = svc2.get_pending_approvals()
        self.assertIn("a1", pending, "实例 B 看不到实例 A 创建的审批——状态未跨实例共享")

    def test_decision_visible_and_duplicate_blocked_cross_instance(self):
        """跨实例决策串行一致：B 提交后 A 再见同审批人重复提交被拦。"""
        svc1, svc2 = self.make(), self.make()
        _run(svc1.handle_task(_approval_task()))

        r1 = _run(svc2.submit_approval_decision("a1", "u1", "approve"))
        self.assertEqual(r1["status"], "success")
        self.assertNotIn("final_decision", r1, "all 策略 2 人不应一票定案")

        # 同一审批人换实例重复提交 → 原子拦住
        r2 = _run(svc1.submit_approval_decision("a1", "u1", "approve"))
        self.assertEqual(r2["status"], "error")
        self.assertEqual(r2["message"], "已经审批过")

        # 决策对另一实例可见（当前_approvals=1）
        self.assertEqual(
            svc1.get_pending_approvals()["a1"]["current_approvals"], 1
        )

        # 第二人批准（任意实例）→ 定案，记录移除
        r3 = _run(svc1.submit_approval_decision("a1", "u2", "approve"))
        self.assertEqual(r3["status"], "success")
        self.assertEqual(r3["final_decision"], "approve")
        self.assertNotIn("a1", svc2.get_pending_approvals())

    def test_duplicate_submit_races_exactly_one_success(self):
        """同一审批人双实例并发提交：恰好一次成功、一次被原子拦住。"""
        svc1, svc2 = self.make(), self.make()
        _run(svc1.handle_task(_approval_task()))

        async def _race():
            return await asyncio.gather(
                svc1.submit_approval_decision("a1", "u1", "approve"),
                svc2.submit_approval_decision("a1", "u1", "approve"),
            )

        results = _run(_race())
        ok = [r for r in results if r["status"] == "success"]
        dup = [r for r in results if r["message"] == "已经审批过"]
        self.assertEqual(len(ok), 1, f"应恰好一次成功: {results}")
        self.assertEqual(len(dup), 1, f"重复提交应被拦一次: {results}")

    def test_approval_survives_restart_and_stop_keeps_redis_state(self):
        """重启/停机不丢：stop_service 不清 Redis（旧实现 clear() 即全丢）。"""
        svc1 = self.make()
        _run(svc1.handle_task(_approval_task()))
        svc1.stop_service()

        svc2 = self.make()
        details = svc2.get_approval_details("a1")
        self.assertNotIn("error", details, "stop_service 后记录丢失")
        r = _run(svc2.submit_approval_decision("a1", "u1", "approve"))
        self.assertEqual(r["status"], "success")

    def test_record_has_ttl(self):
        """记录键带 TTL 自清理（7 天惯例）。"""
        svc = self.make()
        _run(svc.handle_task(_approval_task()))
        ttl = self.redis.ttl("plaita:approval:pending:a1")
        self.assertGreater(ttl, 0, "审批记录键未设置 TTL")
        self.assertLessEqual(ttl, APPROVAL_TTL)

    def test_no_redis_falls_back_to_memory(self):
        """无 redis_client：回退进程内存（单测/内存模式历史行为不变）。"""
        svc1 = ApprovalService(self.bus)
        svc2 = ApprovalService(self.bus)
        _run(svc1.handle_task(_approval_task()))
        self.assertIn("a1", svc1.get_pending_approvals())
        self.assertNotIn("a1", svc2.get_pending_approvals())
        r = _run(svc1.submit_approval_decision("a1", "u1", "approve"))
        self.assertEqual(r["status"], "success")

    def test_get_pending_approvals_shape_unchanged(self):
        """对外形状保持（examples/server_demo 消费该返回结构）。"""
        svc = self.make()
        _run(svc.handle_task(_approval_task()))
        pending = svc.get_pending_approvals()
        self.assertEqual(
            set(pending["a1"].keys()),
            {
                "approval_id",
                "status",
                "created_time",
                "required_approvers",
                "current_approvals",
                "strategy",
            },
        )
        self.assertEqual(pending["a1"]["status"], "pending")
        self.assertEqual(pending["a1"]["current_approvals"], 0)

    def test_get_approval_details_shape_unchanged(self):
        """详情含 task_config/approvals 等全量字段（与内存版一致）。"""
        svc = self.make()
        _run(svc.handle_task(_approval_task()))
        details = svc.get_approval_details("a1")
        self.assertEqual(details["approval_id"], "a1")
        self.assertEqual(details["status"], "pending")
        self.assertIn("task_config", details)
        self.assertEqual(details["required_approvers"], ["u1", "u2"])
        self.assertEqual(svc.get_approval_details("missing"), {"error": "审批任务不存在"})


class HttpCallbackRedisStateTest(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()

    def make(self):
        return HttpCallbackService(self.bus, {}, redis_client=self.redis)

    def test_registration_shared_across_instances(self):
        """实例 A 注册的回调路径，实例 B 能处理请求（修复前：各自内存，红）。"""
        svc1, svc2 = self.make(), self.make()
        _run(svc1.handle_task(_callback_task()))
        resp = _run(svc2.handle_callback_request("/cb/1", {"ok": 1}))
        self.assertNotEqual(
            resp.get("status"), "error", "实例 B 查无实例 A 注册的回调——状态未跨实例共享"
        )
        self.assertEqual(resp, {"status": "success"})

    def test_callback_claimed_at_most_once(self):
        """回调处理是原子认领：两个实例并发到达，恰一实例触发，另一实例得到未注册。"""
        svc1, svc2, svc3 = self.make(), self.make(), self.make()
        _run(svc1.handle_task(_callback_task()))

        async def _race():
            return await asyncio.gather(
                svc2.handle_callback_request("/cb/1", {"ok": 1}),
                svc3.handle_callback_request("/cb/1", {"ok": 1}),
            )

        results = _run(_race())
        ok = [r for r in results if r.get("status") == "success"]
        miss = [r for r in results if r.get("message") == "回调路径未注册"]
        self.assertEqual(len(ok), 1, f"应恰一实例认领成功: {results}")
        self.assertEqual(len(miss), 1, f"另一实例应得到未注册: {results}")

    def test_registration_has_ttl_and_survives_stop(self):
        """注册键带 TTL；stop_service 不清 Redis 记录。"""
        svc1 = self.make()
        _run(svc1.handle_task(_callback_task()))
        ttl = self.redis.ttl("plaita:http_callback:registered:/cb/1")
        self.assertGreater(ttl, 0, "回调注册键未设置 TTL")
        svc1.stop_service()
        svc2 = self.make()
        self.assertIn("/cb/1", svc2.get_registered_callbacks())

    def test_no_redis_falls_back_to_memory(self):
        """无 redis_client：回退进程内存（历史行为不变）。"""
        svc1 = HttpCallbackService(self.bus)
        svc2 = HttpCallbackService(self.bus)
        _run(svc1.handle_task(_callback_task()))
        self.assertIn("/cb/1", svc1.get_registered_callbacks())
        self.assertEqual(svc2.get_registered_callbacks(), {})
        resp = _run(svc1.handle_callback_request("/cb/1", {"ok": 1}))
        self.assertEqual(resp, {"status": "success"})
        self.assertEqual(svc1.get_registered_callbacks(), {})

    def test_get_registered_callbacks_shape_unchanged(self):
        """形状保持：{path: {task_config, registered_time}}。"""
        svc = self.make()
        _run(svc.handle_task(_callback_task()))
        cbs = svc.get_registered_callbacks()
        self.assertEqual(set(cbs.keys()), {"/cb/1"})
        self.assertIn("task_config", cbs["/cb/1"])
        self.assertIn("registered_time", cbs["/cb/1"])


if __name__ == "__main__":
    unittest.main()
