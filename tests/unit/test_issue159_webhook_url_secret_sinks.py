"""plaita#159 —— webhook URL 的**密钥在 path/query 里**，日志不得出现它。

``#88`` 的 ``factory._mask_url`` 抹 userinfo 口令、**原样保留 path**；而 Slack
（``/services/T…/B…/X…``）、飞书（``/open-apis/bot/v2/hook/…``）、钉钉
（``?access_token=…``）、企业微信（``?key=…``）四家的凭据**就在 path/query**。于是：

* ``plaita/server/alerts.py`` 投递失败/队列满的 WARNING 回显 ``self.url``；
* ``plaita/server/flow_worker.py`` 启用死信告警的 INFO 回显 ``self._alert_webhook``；

这两行等价于把「往该 webhook 投任意内容」的能力写进日志。``#158`` 的守卫当时只认
``redis_url`` 这个名字，所以对 ``self.url`` 完全失明——本文件同时把守卫的**通用性**
钉住（见 :class:`TestWebhookUrlsCannotHideFromTheGuard`）。

边界（jeffkit 验收清单）：``WebhookAlerter.url`` / ``RedisFlowWorker._alert_webhook``
仍持**原始串**（POST 要用），本单只改「渲染进日志的那一份」；POST/传输路径一字未动
（见 :class:`TestPostPathStillSeesTheOriginalUrl`）。
"""
from __future__ import annotations

import json
import logging

import pytest

from plaita.server import url_redaction
from plaita.server.alerts import WebhookAlerter
from plaita.server.url_redaction import (
    UNPRINTABLE_URL,
    _mask_webhook_url,
    _mask_webhook_url_for_log,
)
from tests.unit._url_sink_support import (
    build_redis_flow_worker,
    credential_url_log_sites,
    find_needles,
    leak_needles,
    log_record_fields,
)

PATH_SECRET = "Zq4Vm8Lp"
QUERY_SECRET = "Rt6Yn3Kd"

SLACK_URL = f"https://hooks.slack.com/services/T000/B000/{PATH_SECRET}"
FEISHU_URL = f"https://open.feishu.cn/open-apis/bot/v2/hook/{PATH_SECRET}"
DINGTALK_URL = f"https://oapi.dingtalk.com/robot/send?access_token={QUERY_SECRET}"
WECOM_URL = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={QUERY_SECRET}"

NEEDLES = leak_needles(PATH_SECRET) + leak_needles(QUERY_SECRET)


# --------------------------------------------------------------------------- #
# 渲染器规格
# --------------------------------------------------------------------------- #
class TestWebhookRendererSpec:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            (SLACK_URL, "https://hooks.slack.com/***/***/***/***"),
            (FEISHU_URL, "https://open.feishu.cn/***/***/***/***/***"),
            (DINGTALK_URL, "https://oapi.dingtalk.com/***/***?***"),
            (WECOM_URL, "https://qyapi.weixin.qq.com/***/***/***?***"),
        ],
        ids=["slack", "feishu", "dingtalk", "wecom"],
    )
    def test_keeps_endpoint_and_masks_every_path_segment_and_the_query(self, url, expected):
        assert _mask_webhook_url(url) == expected

    def test_renderer_output_never_carries_the_secret(self):
        for url in (SLACK_URL, FEISHU_URL, DINGTALK_URL, WECOM_URL):
            assert find_needles(_mask_webhook_url(url), NEEDLES) == [], url

    def test_drops_userinfo_but_keeps_host(self):
        url = f"https://user:{PATH_SECRET}@hooks.slack.com/services/{PATH_SECRET}"
        assert _mask_webhook_url(url) == "https://hooks.slack.com/***/***"

    def test_path_segments_query_and_fragment_all_become_placeholders(self):
        assert _mask_webhook_url("https://host/a/b/c?k=v#frag") == "https://host/***/***/***?***#***"

    def test_ipv6_host_and_port_survive(self):
        assert (
            _mask_webhook_url(f"https://[2001:db8::1]:8443/services/{PATH_SECRET}")
            == "https://[2001:db8::1]:8443/***/***"
        )

    def test_bare_endpoint_without_credentials_is_unchanged(self):
        # 没有 path/query/userinfo ⇒ 本来就没有凭据，端点原文可以留下（诊断价值）。
        assert _mask_webhook_url("https://hooks.slack.com") == "https://hooks.slack.com"

    def test_non_http_scheme_degrades_to_safe_constant(self):
        assert _mask_webhook_url("unix:///tmp/redis.sock") == UNPRINTABLE_URL
        assert _mask_webhook_url(f"javascript:alert('{PATH_SECRET}')") == UNPRINTABLE_URL


# --------------------------------------------------------------------------- #
# 渲染器 totality：任何输入都不抛、都可 utf-8 编码、都不回流原文
# --------------------------------------------------------------------------- #
_TOTALITY_CASES = [
    pytest.param(f"https://h/{PATH_SECRET}\udcff", id="surrogate-in-path"),
    pytest.param(f"https://ho\udcffst/{PATH_SECRET}", id="surrogate-in-host"),
    pytest.param(b"https://hooks.slack.com/services/\xff\xfe" + PATH_SECRET.encode(), id="bytes-bad-utf8"),
    pytest.param(bytearray(b"https://hooks.slack.com/services/\xff" + PATH_SECRET.encode()), id="bytearray"),
    pytest.param(None, id="none"),
    pytest.param(123, id="int"),
    pytest.param(object(), id="opaque-object"),
    pytest.param("", id="empty"),
    pytest.param("http://[::1", id="invalid-ipv6"),
    pytest.param("http://host:notaport/x", id="non-numeric-port"),
    pytest.param(f"hooks.slack.com/services/{PATH_SECRET}", id="no-scheme"),
    pytest.param(PATH_SECRET, id="bare-token"),
    pytest.param(f"/{PATH_SECRET}", id="bare-path"),
    pytest.param("https://host:80x/" + PATH_SECRET, id="port-junk"),
]


class TestWebhookRendererIsTotal:
    @pytest.mark.parametrize("value", _TOTALITY_CASES)
    def test_never_raises_and_output_is_loggable(self, value):
        rendered = _mask_webhook_url(value)
        assert isinstance(rendered, str)
        rendered.encode("utf-8")  # 不抛 ⇒ 任何 utf-8 handler 都写得出去

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(f"https://hooks.slack.com/services/{PATH_SECRET}", id="slack"),
            pytest.param(f"https://user:{PATH_SECRET}@h/x", id="userinfo"),
            pytest.param(f"https://h/a?t={QUERY_SECRET}", id="query"),
            pytest.param(f"hooks.slack.com/services/{PATH_SECRET}", id="no-scheme"),
            pytest.param(PATH_SECRET, id="bare-token"),
        ],
    )
    def test_never_returns_a_credential_bearing_input_verbatim(self, value):
        assert _mask_webhook_url(value) != value

    @pytest.mark.parametrize("value", _TOTALITY_CASES)
    def test_totality_cases_never_leak_a_needle(self, value):
        assert find_needles(_mask_webhook_url(value), NEEDLES) == []

    def test_random_bytes_never_raise(self):
        import random

        rng = random.Random(20261015)
        for _ in range(200):
            raw = bytes(rng.randrange(1, 0x100) for _ in range(rng.randrange(0, 24)))
            rendered = _mask_webhook_url(raw)
            assert isinstance(rendered, str)
            rendered.encode("utf-8")
            as_text = _mask_webhook_url(raw.decode("utf-8", "surrogateescape"))
            assert isinstance(as_text, str)
            as_text.encode("utf-8")

    def test_renderer_is_idempotent_on_its_own_output(self):
        for url in (SLACK_URL, DINGTALK_URL, WECOM_URL, FEISHU_URL):
            once = _mask_webhook_url(url)
            assert _mask_webhook_url(once) == once


# --------------------------------------------------------------------------- #
# 渲染失败 ⇒ 安全常量（绝不炸调用方、绝不回流原文）
# --------------------------------------------------------------------------- #
class TestRendererFailureDegradesToSafeConstant:
    def test_wrapper_returns_constant_when_renderer_raises(self, monkeypatch):
        monkeypatch.setattr(
            url_redaction,
            "_mask_webhook_url",
            lambda value: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        rendered = _mask_webhook_url_for_log(SLACK_URL)
        assert rendered == UNPRINTABLE_URL
        assert rendered != SLACK_URL

    def test_dead_letter_enable_log_survives_renderer_failure(self, monkeypatch, caplog):
        monkeypatch.setattr(
            url_redaction,
            "_mask_webhook_url",
            lambda value: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        worker, _client = build_redis_flow_worker(SLACK_URL, alert_webhook=SLACK_URL, enable_registry=False)
        try:
            with caplog.at_level(logging.INFO, logger="plaita"):
                worker._start_alerting()  # 不得抛
        finally:
            worker._stop_alerting()
        fields = log_record_fields(caplog.records)
        assert UNPRINTABLE_URL in fields
        assert find_needles(fields, NEEDLES) == []

    def test_delivery_failure_log_survives_renderer_failure(self, monkeypatch, caplog):
        monkeypatch.setattr(
            url_redaction,
            "_mask_webhook_url",
            lambda value: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        def failing_post(url, body, headers, timeout):
            raise RuntimeError("transport down")

        alerter = WebhookAlerter(SLACK_URL, post=failing_post)
        with caplog.at_level(logging.WARNING, logger="plaita.server.alerts"):
            assert alerter.send({"event": "dead_letter"}) is True
            assert alerter.flush(timeout=5) is True
        fields = log_record_fields(caplog.records)
        assert UNPRINTABLE_URL in fields
        assert find_needles(fields, NEEDLES) == []


# --------------------------------------------------------------------------- #
# sink 5：死信告警 webhook 启用的 INFO
# --------------------------------------------------------------------------- #
class TestDeadLetterWebhookEnableLogHidesCredential:
    def _records(self, caplog):
        worker, _client = build_redis_flow_worker(
            "redis://127.0.0.1:6379/0", alert_webhook=SLACK_URL, enable_registry=False
        )
        try:
            with caplog.at_level(logging.INFO, logger="plaita"):
                worker._start_alerting()
        finally:
            worker._stop_alerting()
        return worker, caplog.records

    def test_info_record_never_carries_the_secret(self, caplog):
        _worker, records = self._records(caplog)
        assert records, "没有产生任何日志记录"
        fields = log_record_fields(records)
        assert find_needles(fields, NEEDLES) == [], fields
        assert SLACK_URL not in fields

    def test_info_record_still_shows_the_endpoint(self, caplog):
        _worker, records = self._records(caplog)
        fields = log_record_fields(records)
        assert "死信告警 webhook 已启用" in fields
        assert "https://hooks.slack.com/***/***/***/***" in fields

    def test_worker_still_holds_the_original_url_for_posting(self, caplog):
        # 注意：不能在 ``_records`` 的 finally 里已 ``_stop_alerting()`` 之后断言
        # ``_alerter``——停机把 alerter 置回 None 是既有语义。这里自己走一遍。
        worker, _client = build_redis_flow_worker(
            "redis://127.0.0.1:6379/0", alert_webhook=SLACK_URL, enable_registry=False
        )
        try:
            with caplog.at_level(logging.INFO, logger="plaita"):
                worker._start_alerting()
            assert worker._alert_webhook == SLACK_URL
            assert worker._alerter is not None
            assert worker._alerter.url == SLACK_URL
        finally:
            worker._stop_alerting()


# --------------------------------------------------------------------------- #
# sink 4 / 4b：投递失败 WARNING 与队列满 WARNING
# --------------------------------------------------------------------------- #
class TestWebhookDeliveryFailureLogHidesCredential:
    def _records(self, caplog, url: str = SLACK_URL):
        def failing_post(inner_url, body, headers, timeout):
            raise RuntimeError("transport down")

        alerter = WebhookAlerter(url, post=failing_post)
        with caplog.at_level(logging.WARNING, logger="plaita.server.alerts"):
            assert alerter.send({"event": "dead_letter"}) is True
            assert alerter.flush(timeout=5) is True
        return alerter, caplog.records

    def test_warning_record_never_carries_the_secret(self, caplog):
        _alerter, records = self._records(caplog)
        assert records, "没有产生任何 WARNING 记录"
        fields = log_record_fields(records)
        assert find_needles(fields, NEEDLES) == [], fields
        assert SLACK_URL not in fields

    def test_warning_record_still_shows_the_endpoint(self, caplog):
        _alerter, records = self._records(caplog)
        fields = log_record_fields(records)
        assert "https://hooks.slack.com/***/***/***" in fields

    @pytest.mark.parametrize("url", [DINGTALK_URL, WECOM_URL, FEISHU_URL], ids=["dingtalk", "wecom", "feishu"])
    def test_all_four_vendor_shapes_are_covered(self, caplog, url):
        _alerter, records = self._records(caplog, url)
        assert find_needles(log_record_fields(records), NEEDLES) == []


class TestWebhookQueueFullLogHidesCredential:
    def _records(self, caplog):
        alerter = WebhookAlerter(SLACK_URL, queue_size=1, post=lambda *a: None)
        # 不让消费者把队列排空——否则 put_nowait 永远成功，走不到 queue.Full 分支。
        alerter._ensure_thread = lambda: None  # type: ignore[method-assign]
        with caplog.at_level(logging.WARNING, logger="plaita.server.alerts"):
            assert alerter.send({"event": "one"}) is True
            assert alerter.send({"event": "two"}) is False
        return alerter, caplog.records

    def test_queue_full_warning_never_carries_the_secret(self, caplog):
        _alerter, records = self._records(caplog)
        assert records, "没有产生任何 WARNING 记录"
        fields = log_record_fields(records)
        assert "队列已满" in fields
        assert find_needles(fields, NEEDLES) == [], fields
        assert SLACK_URL not in fields


# --------------------------------------------------------------------------- #
# 边界：POST / 传输路径必须仍然拿到**原始串**
# --------------------------------------------------------------------------- #
class TestPostPathStillSeesTheOriginalUrl:
    def test_alerter_attribute_keeps_the_original_url(self):
        alerter = WebhookAlerter(SLACK_URL, post=lambda *a: None)
        assert alerter.url == SLACK_URL

    def test_post_receives_the_exact_original_url_and_body(self):
        seen = []

        def post(url, body, headers, timeout):
            seen.append((url, body, headers, timeout))
            raise RuntimeError("stop here")

        alerter = WebhookAlerter(SLACK_URL, post=post, timeout=2.5)
        assert alerter.send({"event": "dead_letter"}) is True
        assert alerter.flush(timeout=5) is True
        assert len(seen) == 1
        url, body, headers, timeout = seen[0]
        assert url == SLACK_URL, "POST 的 URL 被掩码污染了（掩码泄漏进行为）"
        assert json.loads(body) == {"event": "dead_letter"}
        assert headers == {"Content-Type": "application/json"}
        assert timeout == 2.5

    def test_worker_keeps_the_original_webhook_for_the_alerter(self):
        worker, _client = build_redis_flow_worker(
            "redis://127.0.0.1:6379/0", alert_webhook=SLACK_URL, enable_registry=False
        )
        try:
            worker._start_alerting()
            assert worker._alert_webhook == SLACK_URL
            assert worker._alerter.url == SLACK_URL
        finally:
            worker._stop_alerting()

    def test_env_sourced_webhook_is_also_unchanged(self, monkeypatch):
        monkeypatch.setenv("PLAITA_ALERT_WEBHOOK", SLACK_URL)
        worker, _client = build_redis_flow_worker("redis://127.0.0.1:6379/0", enable_registry=False)
        try:
            assert worker._alert_webhook == SLACK_URL
        finally:
            worker._stop_alerting()


# --------------------------------------------------------------------------- #
# 守卫的 URL 通用性（#159 之所以对 #158 的守卫隐身）
# --------------------------------------------------------------------------- #
class TestWebhookUrlsCannotHideFromTheGuard:
    def test_guard_catches_webhook_url_identifiers(self):
        for source in (
            'logger.warning("告警 webhook 发送失败 url=%s: %s", self.url, exc)\n',
            'logger.info("死信告警 webhook 已启用: %s", self._alert_webhook)\n',
            'logger.warning("url=%s", alert_webhook)\n',
            'logger.warning("url=%s", webhook_url)\n',
            'logger.warning("url=%s", self.alerter.url)\n',
        ):
            assert credential_url_log_sites(source), source

    def test_guard_catches_the_hidden_inside_call_chain_form(self):
        assert credential_url_log_sites('logger.warning("url=%s", self.url.strip())\n')

    def test_guard_accepts_the_webhook_masker(self):
        for source in (
            'logger.warning("url=%s", _mask_webhook_url_for_log(self.url))\n',
            'logger.info("hook: %s", _mask_webhook_url_for_log(self._alert_webhook))\n',
            'logger.warning("url=%s", _mask_webhook_url(self.url))\n',
        ):
            assert credential_url_log_sites(source) == [], source

    def test_passwords_helper_is_not_a_laundering_device(self):
        # ``_passwords`` 返回**原始口令**；把它当掩码器就等于放行原文。
        assert credential_url_log_sites('logger.warning("url=%s", _passwords(self.url))\n')
