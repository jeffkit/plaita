"""#113 回归：``http_callback`` 的 ``auth_config`` 必须在消费侧被真正消费。

历史缺陷：``HttpCallbackNode`` 生成并落库完整凭据（token / basic /
signature），``HttpCallbackService.handle_callback_request()`` 却没有任何
凭据参数——认领一条回调记录的唯一判据是 URL 路径。``require_auth`` 默认
``True``（schema 宣称「需要认证」），实际零校验：不带任何凭据的 POST 即被
判成功并消费掉注册记录（可唤醒任意挂起执行）。

本文件钉住的契约：

1. 校验发生在**认领之前**——校验失败时注册记录必须仍然存在
   （``_CLAIM_CALLBACK_LUA`` 在 #188 后是「占位而不删除」，但先占位仍会把
   合法回调挡在处理中窗口外；未授权请求绝不能消耗掉认领机会）；
2. 凭据比对走 ``hmac.compare_digest``（常数时间）；
3. 三种 ``auth_type`` 的正确/错误凭据各一条；signature 另含过期时间戳；
4. ``auth_config`` 缺失/为空 → 视为未声明认证（兼容历史注册记录）；
   声明了认证但类型未知 → fail closed；
5. Q5：认证已配置时，未认证视角下「凭据缺失/错误/过期」与「路径不存在」
   必须是**同一条**响应（两条不同的 message 合起来就是 path-existence
   oracle）。注册记录未声明 ``auth_config`` 时认证不生效——那是已文档化的
   **部署事实边界**，行为不变（不是凭据正确性的 oracle）。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.core.executor import FlowExecution
from plaita.event.memory import InMemoryEventBus
from plaita.server.nodes.http_callback_node import HttpCallbackNode
from plaita.server.services import http_callback_service as hcs
from plaita.server.services.http_callback_service import HttpCallbackService

PATH = "/callback/f3/cb1/e3"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _callback_task(path=PATH, auth_config=None):
    task = {
        "type": "http_callback",
        "node_id": "cb1",
        "execution_id": "e3",
        "flow_id": "f3",
        "event_type": "http_callback",
        "callback_config": {"path": path, "method": "POST"},
        "response_config": {},
    }
    if auth_config is not None:
        task["auth_config"] = auth_config
    return task


def _token_auth(token="SECRET"):
    return {"enabled": True, "type": "token", "token": token}


def _signature_headers(secret, body, timestamp=None, signature=None):
    timestamp = int(time.time()) if timestamp is None else timestamp
    if signature is None:
        payload = f"{timestamp}." + json.dumps(
            body, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return {
        "X-Plaita-Timestamp": str(timestamp),
        "X-Plaita-Signature": signature,
    }


class HttpCallbackTokenAuthTest(unittest.TestCase):
    """token 模式：未授权请求必须被拒且不消费注册记录（#113 原始洞）。"""

    def setUp(self):
        self.bus = InMemoryEventBus()

    def make(self):
        return HttpCallbackService(self.bus, {})

    def register(self, svc, path=PATH, auth_config=None):
        self.assertTrue(_run(svc.handle_task(_callback_task(path, auth_config))))

    def test_missing_credential_rejected_and_record_preserved(self):
        """不带 Authorization → error，且注册记录仍在（修复前：success + 记录被删）。

        Q5：响应不再是「认证失败：token 不匹配」的凭据细节，而是与「路径不存在」
        逐字节一致的统一失败响应（细节只进日志）。
        """
        svc = self.make()
        self.register(svc, auth_config=_token_auth())

        resp = _run(svc.handle_callback_request(PATH, {"evil": True}))

        self.assertEqual(resp.get("status"), "error", f"未授权回调被放行: {resp}")
        self.assertEqual(
            resp,
            {"status": "error", "message": hcs.CALLBACK_FAILURE_MESSAGE},
            "未认证响应泄漏了凭据失败细节（Q5）",
        )
        self.assertIn(PATH, svc.get_registered_callbacks(), "未授权请求消费掉了注册记录")

    def test_wrong_token_rejected_and_record_preserved(self):
        svc = self.make()
        self.register(svc, auth_config=_token_auth())

        resp = _run(
            svc.handle_callback_request(
                PATH, {"e": 1}, {"Authorization": "Bearer WRONG"}
            )
        )

        self.assertEqual(resp.get("status"), "error")
        self.assertEqual(
            resp,
            {"status": "error", "message": hcs.CALLBACK_FAILURE_MESSAGE},
        )
        self.assertIn(PATH, svc.get_registered_callbacks())

    def test_correct_token_accepted_and_consumed(self):
        svc = self.make()
        self.register(svc, auth_config=_token_auth())

        resp = _run(
            svc.handle_callback_request(
                PATH, {"ok": 1}, {"Authorization": "Bearer SECRET"}
            )
        )

        self.assertEqual(resp, {"status": "success"})
        self.assertNotIn(PATH, svc.get_registered_callbacks())

    def test_header_name_is_case_insensitive(self):
        """HTTP 头名大小写不敏感：``authorization`` 同样生效。"""
        svc = self.make()
        self.register(svc, auth_config=_token_auth("MiXeD"))

        resp = _run(
            svc.handle_callback_request(PATH, {}, {"authorization": "bearer MiXeD"})
        )

        self.assertEqual(resp, {"status": "success"})

    def test_compare_digest_is_used_for_credential_check(self):
        """凭据比对必须走 ``hmac.compare_digest``（常数时间），不得用 ``==``。"""
        svc = self.make()
        self.register(svc, auth_config=_token_auth())

        with patch(
            "plaita.server.services.http_callback_service.hmac.compare_digest",
            wraps=hmac.compare_digest,
        ) as spy:
            _run(
                svc.handle_callback_request(
                    PATH, {}, {"Authorization": "Bearer SECRET"}
                )
            )
            self.assertTrue(spy.called, "token 比对没有经过 hmac.compare_digest")

    def test_no_auth_config_is_not_enforced(self):
        """历史/手写注册记录没有 auth_config → 视为未声明认证，行为不变。"""
        svc = self.make()
        self.register(svc, auth_config=None)

        resp = _run(svc.handle_callback_request(PATH, {"ok": 1}))

        self.assertEqual(resp, {"status": "success"})

    def test_disabled_auth_is_not_enforced(self):
        svc = self.make()
        self.register(svc, auth_config={"enabled": False})

        resp = _run(svc.handle_callback_request(PATH, {"ok": 1}))

        self.assertEqual(resp, {"status": "success"})

    def test_unknown_auth_type_fails_closed(self):
        """声明了认证但类型未知 → 拒绝（不得静默放行）。"""
        svc = self.make()
        self.register(svc, auth_config={"enabled": True, "type": "magic"})

        resp = _run(svc.handle_callback_request(PATH, {"ok": 1}))

        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks())

    def test_enabled_without_type_fails_closed(self):
        svc = self.make()
        self.register(svc, auth_config={"enabled": True})

        resp = _run(svc.handle_callback_request(PATH, {"ok": 1}))

        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks())

    def test_node_generated_auth_config_is_the_enforced_one(self):
        """端到端：节点产出的 auth_config 原样落库，且正是消费侧校验的那份。"""
        execution = FlowExecution(event_bus=InMemoryEventBus())
        execution.clean()
        execution.set_state("$FLOW_ID", "f3")
        execution.set_state("$EXECUTION_ID", "e3")
        node = HttpCallbackNode(id="cb1", event_type="http_callback", auth_token="SECRET")
        auth_config = node._generate_auth_config(execution)
        self.assertEqual(auth_config, {"enabled": True, "type": "token", "token": "SECRET"})

        svc = self.make()
        self.register(svc, auth_config=auth_config)

        self.assertEqual(
            _run(svc.handle_callback_request(PATH, {})).get("status"), "error"
        )
        self.assertIn(PATH, svc.get_registered_callbacks())
        self.assertEqual(
            _run(
                svc.handle_callback_request(
                    PATH, {}, {"Authorization": "Bearer SECRET"}
                )
            ),
            {"status": "success"},
        )


class HttpCallbackUniformFailureTest(unittest.TestCase):
    """Q5：认证已配置时，未认证调用方不得区分「凭据错误」与「路径不存在」。

    旧契约（被本类取代）：未注册路径 → ``回调路径未注册``；凭据错误 →
    ``认证失败：<细节>``。两条 message 并存 = 一个 path-existence oracle：
    未认证者可据此枚举已注册的回调路径。现统一为
    ``hcs.CALLBACK_FAILURE_MESSAGE``，凭据细节只进服务日志。
    """

    def setUp(self):
        self.bus = InMemoryEventBus()

    def make(self):
        return HttpCallbackService(self.bus, {})

    def register(self, svc, path=PATH, auth_config=None):
        self.assertTrue(_run(svc.handle_task(_callback_task(path, auth_config))))

    def test_unauthenticated_viewpoint_is_one_byte_identical_response(self):
        """(a) 凭据缺失/错误/签名过期/路径不存在 → 逐字节一致的同一条响应。"""
        svc = self.make()
        self.register(svc, auth_config=_token_auth())
        sig_path = "/callback/f3/cb1/sig"
        self.register(
            svc,
            path=sig_path,
            auth_config={
                "enabled": True,
                "type": "signature",
                "secret": "S3CR3T",
                "algorithm": "HMAC-SHA256",
            },
        )

        missing = _run(svc.handle_callback_request(PATH, {"x": 1}))
        wrong = _run(
            svc.handle_callback_request(
                PATH, {"x": 1}, {"Authorization": "Bearer WRONG"}
            )
        )
        stale = int(time.time()) - hcs.CALLBACK_SIGNATURE_TOLERANCE_SECONDS - 10
        expired = _run(
            svc.handle_callback_request(
                sig_path, {"x": 1}, _signature_headers("S3CR3T", {"x": 1}, timestamp=stale)
            )
        )
        unknown = _run(
            svc.handle_callback_request("/callback/f3/cb1/never-registered", {"x": 1})
        )

        expected = {"status": "error", "message": hcs.CALLBACK_FAILURE_MESSAGE}
        for label, resp in (
            ("missing", missing),
            ("wrong", wrong),
            ("expired", expired),
            ("unknown", unknown),
        ):
            self.assertEqual(resp, expected, f"{label} 响应可区分路径是否存在: {resp}")
        blobs = {
            json.dumps(r, ensure_ascii=False, sort_keys=True)
            for r in (missing, wrong, expired, unknown)
        }
        self.assertEqual(len(blobs), 1, f"响应不是逐字节一致: {blobs}")
        # 被拒的请求不得消费任何注册记录
        self.assertIn(PATH, svc.get_registered_callbacks())
        self.assertIn(sig_path, svc.get_registered_callbacks())

    def test_unknown_path_is_uniform_even_without_any_auth_registered(self):
        """实例上没有任何认证声明时，未知路径仍回统一失败响应（不因无 auth 分叉）。"""
        svc = self.make()
        resp = _run(svc.handle_callback_request("/cb/never-registered", {"x": 1}))
        self.assertEqual(
            resp, {"status": "error", "message": hcs.CALLBACK_FAILURE_MESSAGE}
        )

    def test_fail_closed_unknown_auth_type_uses_uniform_response(self):
        """fail-closed 分支同样不泄漏细节（未知 type 不得自报 type）。"""
        svc = self.make()
        self.register(svc, auth_config={"enabled": True, "type": "magic"})
        resp = _run(
            svc.handle_callback_request(PATH, {"x": 1}, {"Authorization": "Bearer SECRET"})
        )
        self.assertEqual(
            resp, {"status": "error", "message": hcs.CALLBACK_FAILURE_MESSAGE}
        )
        self.assertIn(PATH, svc.get_registered_callbacks())

    def test_authenticated_request_still_succeeds_and_consumes_record(self):
        """(b) 鉴权通过后行为不变：恰一次消费并 resume。"""
        svc = self.make()
        self.register(svc, auth_config=_token_auth())

        resp = _run(
            svc.handle_callback_request(
                PATH, {"ok": 1}, {"Authorization": "Bearer SECRET"}
            )
        )

        self.assertEqual(resp, {"status": "success"})
        self.assertNotIn(PATH, svc.get_registered_callbacks(), "成功回调未消费记录")

    def test_auth_absent_record_still_behaves_as_before(self):
        """(c) 免认证注册记录（auth_config 缺失）仍可被任何调用方触发。

        这是已文档化的**部署事实边界**（该回调按契约本就不需要认证），
        不是「凭据是否正确」的 oracle。
        """
        svc = self.make()
        self.register(svc, path="/cb/noauth", auth_config=None)

        hit = _run(svc.handle_callback_request("/cb/noauth", {"ok": 1}))
        unknown = _run(svc.handle_callback_request("/cb/never-registered", {"ok": 1}))

        self.assertEqual(hit, {"status": "success"})
        self.assertEqual(
            unknown, {"status": "error", "message": hcs.CALLBACK_FAILURE_MESSAGE}
        )
        self.assertNotEqual(hit, unknown)


class HttpCallbackBasicAuthTest(unittest.TestCase):
    def setUp(self):
        self.bus = InMemoryEventBus()

    def make(self):
        return HttpCallbackService(self.bus, {})

    def register(self, svc, path=PATH):
        auth = {"enabled": True, "type": "basic", "username": "u", "password": "p"}
        self.assertTrue(_run(svc.handle_task(_callback_task(path, auth))))

    def test_correct_basic_credentials_accepted(self):
        svc = self.make()
        self.register(svc)
        header = base64.b64encode(b"u:p").decode()

        resp = _run(
            svc.handle_callback_request(
                PATH, {}, {"Authorization": f"Basic {header}"}
            )
        )

        self.assertEqual(resp, {"status": "success"})

    def test_wrong_password_rejected_and_record_preserved(self):
        svc = self.make()
        self.register(svc)
        header = base64.b64encode(b"u:WRONG").decode()

        resp = _run(
            svc.handle_callback_request(
                PATH, {}, {"Authorization": f"Basic {header}"}
            )
        )

        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks())

    def test_malformed_basic_header_rejected(self):
        svc = self.make()
        self.register(svc)

        resp = _run(
            svc.handle_callback_request(
                PATH, {}, {"Authorization": "Basic !!!not-base64!!!"}
            )
        )

        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks())


class HttpCallbackSignatureAuthTest(unittest.TestCase):
    def setUp(self):
        self.bus = InMemoryEventBus()

    def make(self):
        return HttpCallbackService(self.bus, {})

    def register(self, svc, path=PATH):
        auth = {
            "enabled": True,
            "type": "signature",
            "secret": "S3CR3T",
            "algorithm": "HMAC-SHA256",
        }
        self.assertTrue(_run(svc.handle_task(_callback_task(path, auth))))

    def test_correct_signature_accepted(self):
        svc = self.make()
        self.register(svc)
        body = {"status": "completed", "data": {"result": "ok"}}

        resp = _run(
            svc.handle_callback_request(PATH, body, _signature_headers("S3CR3T", body))
        )

        self.assertEqual(resp, {"status": "success"})

    def test_signature_over_other_body_rejected(self):
        """签名只覆盖发送的 body：换 body 不换签名 → 拒绝。"""
        svc = self.make()
        self.register(svc)
        signed = {"status": "completed"}

        resp = _run(
            svc.handle_callback_request(
                PATH, {"status": "evil"}, _signature_headers("S3CR3T", signed)
            )
        )

        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks())

    def test_wrong_secret_rejected(self):
        svc = self.make()
        self.register(svc)
        body = {"ok": 1}

        resp = _run(
            svc.handle_callback_request(PATH, body, _signature_headers("WRONG", body))
        )

        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks())

    def test_expired_timestamp_rejected_and_record_preserved(self):
        """时间窗外的签名视为重放/过期 → 拒绝且不消费注册记录。"""
        svc = self.make()
        self.register(svc)
        body = {"ok": 1}
        stale = int(time.time()) - hcs.CALLBACK_SIGNATURE_TOLERANCE_SECONDS - 10

        resp = _run(
            svc.handle_callback_request(
                PATH, body, _signature_headers("S3CR3T", body, timestamp=stale)
            )
        )

        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks())

    def test_missing_signature_headers_rejected(self):
        svc = self.make()
        self.register(svc)

        resp = _run(svc.handle_callback_request(PATH, {"ok": 1}))

        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks())


class HttpCallbackAuthRedisModeTest(unittest.TestCase):
    """Redis 模式：未授权请求不得触碰注册键；正确凭据仍恰一实例认领。"""

    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()

    def make(self):
        return HttpCallbackService(self.bus, {}, redis_client=self.redis)

    def test_rejected_request_keeps_redis_record_then_correct_token_claims(self):
        svc = self.make()
        self.assertTrue(_run(svc.handle_task(_callback_task(PATH, _token_auth()))))

        resp = _run(svc.handle_callback_request(PATH, {"evil": True}))
        self.assertEqual(resp.get("status"), "error")
        self.assertIn(PATH, svc.get_registered_callbacks(), "未授权请求删掉了 Redis 注册键")

        resp = _run(
            svc.handle_callback_request(
                PATH, {"ok": 1}, {"Authorization": "Bearer SECRET"}
            )
        )
        self.assertEqual(resp, {"status": "success"})
        self.assertNotIn(PATH, svc.get_registered_callbacks())

    def test_unknown_path_and_wrong_credential_are_uniform_in_redis_mode(self):
        """Q5 在 Redis 路径上同样成立：读记录与认领两条分支措辞一致。"""
        svc = self.make()
        self.assertTrue(_run(svc.handle_task(_callback_task(PATH, _token_auth()))))

        wrong = _run(
            svc.handle_callback_request(
                PATH, {"x": 1}, {"Authorization": "Bearer WRONG"}
            )
        )
        unknown = _run(
            svc.handle_callback_request("/cb/never-registered", {"x": 1})
        )

        self.assertEqual(wrong, unknown, "Redis 模式下响应可区分路径是否存在")
        self.assertEqual(
            unknown, {"status": "error", "message": hcs.CALLBACK_FAILURE_MESSAGE}
        )
        self.assertIn(PATH, svc.get_registered_callbacks(), "未授权请求删掉了 Redis 注册键")

    def test_correct_token_claimed_at_most_once_across_instances(self):
        svc1, svc2, svc3 = self.make(), self.make(), self.make()
        self.assertTrue(_run(svc1.handle_task(_callback_task(PATH, _token_auth()))))
        headers = {"Authorization": "Bearer SECRET"}

        async def _race():
            return await asyncio.gather(
                svc2.handle_callback_request(PATH, {"ok": 1}, headers),
                svc3.handle_callback_request(PATH, {"ok": 1}, headers),
            )

        results = _run(_race())
        ok = [r for r in results if r.get("status") == "success"]
        miss = [r for r in results if r.get("message") == "回调路径未注册"]
        self.assertEqual(len(ok), 1, f"应恰一实例认领成功: {results}")
        self.assertEqual(len(miss), 1, f"另一实例应得到未注册: {results}")
