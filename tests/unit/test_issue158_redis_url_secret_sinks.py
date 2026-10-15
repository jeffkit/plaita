"""plaita#158 —— 带凭据的 Redis URL 不得进入「日志」与「注册元数据」两个持久面。

``#88`` 只修了 URL **解析器**（``RedisUrlError`` 的消息经 ``_mask_url`` 渲染）。同一个
连接串还有别的出口，且都**不需要畸形输入**：

1. ``plaita/server/services/__main__.py`` 启动时 ``logger.info("连接 Redis: %s", redis_url)``
   → 口令进 INFO 日志（落盘 / 日志聚合 / ``docker logs``）；
2. ``plaita/server/flow_worker.py`` 把原始 ``redis_url`` 塞进注册 ``metadata``
   → ``registry.register`` 原样 ``SETEX`` 进 ``plaita:registry:flow_worker:<instance_id>``
   （口令**持久化**在 Redis 键里）；
3. ``plaita/server/node_timings.py`` 的两处 ``%r`` 回显节点对象
   → ``repr(RedisNode(target="redis://user:pw@host"))`` 把口令写进 WARNING。

断言纪律（r1 定验的四条已并入）：

* **记录级**：断言 ``record.msg`` / ``record.args`` / ``getMessage()`` / 格式化文本，
  不只断言 ``caplog.text``；
* **needle = 口令本体 + 每一个 ≥3 字符前缀/后缀**（掩码从中间切开时留下的尾巴
  在日志聚合里照样能拼回口令）；
* 掩码**任何输入都不许抛**，且失败要退化为安全常量、绝不退回原 URL（r1 定验 FAIL (a)：
  非 UTF-8 口令曾让 ``RedisFlowWorker.__init__`` 与 ``services.__main__.main()``
  抛 ``UnicodeEncodeError``）；
* ``--live`` 那类「自己手搓原文再读回判泄露」的自证伪装置不留在仓库里，改为直接跑
  **生产**路径（见 :class:`TestRegistryLiveReadbackUsesProductionHelper`）。
"""
from __future__ import annotations

import json
import logging
import random

import pytest

from plaita.core.url_redaction import UNPRINTABLE_URL, _mask_url_for_log
from tests.unit._url_sink_support import (
    PLAITA_DIR,
    PingFailingRedis,
    _MASKER_NAMES,
    build_redis_flow_worker,
    credential_url_log_sites,
    find_needles,
    leak_needles,
    log_record_fields,
    scan_plaita_for_credential_url_logs,
    stored_values,
)

#: 混合大小写 + 数字的口令：它的任何 ≥3 字符前缀/后缀都不可能偶然出现在这些日志的
#: 固定文案里（纯英文口令的 ``sup``/``ret`` 之类会与无关单词撞车）。
SECRET = "Q7xK2pW9"
URL = f"redis://user:{SECRET}@127.0.0.1:6379/0"
MASKED_URL = "redis://user:***@127.0.0.1:6379/0"

#: POSIX ``os.environ`` 对非 UTF-8 字节用 surrogateescape 解码得到的孤立代理字符
#: ——redis-py 的 ``from_url`` **接受**它，所以这是合法配置，不是幻想输入。
NON_UTF8_SECRET = "p\udcffw"
NON_UTF8_URL = f"redis://user:{NON_UTF8_SECRET}@127.0.0.1:6379/0"

NEEDLES = leak_needles(SECRET)


# --------------------------------------------------------------------------- #
# 出口 1：启动 INFO 日志
# --------------------------------------------------------------------------- #
class TestServiceStartupLogHidesPassword:
    """``python -m plaita.server.services`` 的 INFO 行不得含口令。"""

    def _run_main(self, monkeypatch, caplog, redis_url: str = URL):
        import redis as redis_pkg

        from plaita.server.services import __main__ as svc_main

        monkeypatch.setenv("PLAITA_REDIS_URL", redis_url)
        monkeypatch.setattr(
            redis_pkg.Redis, "from_url", lambda url=None, **kw: PingFailingRedis(url)
        )
        monkeypatch.setattr("sys.argv", ["services", "delay_service"])

        with caplog.at_level(logging.INFO, logger="plaita"):
            with pytest.raises(SystemExit):
                svc_main.main()
        return caplog.text

    def test_info_line_shows_masked_url(self, monkeypatch, caplog):
        text = self._run_main(monkeypatch, caplog)
        assert "连接 Redis" in text
        assert MASKED_URL in text

    def test_info_line_never_contains_the_password(self, monkeypatch, caplog):
        text = self._run_main(monkeypatch, caplog)
        assert SECRET not in text, text

    def test_info_line_never_contains_the_raw_url(self, monkeypatch, caplog):
        text = self._run_main(monkeypatch, caplog)
        assert URL not in text, text

    def test_no_log_record_field_carries_the_password_or_a_needle(self, monkeypatch, caplog):
        """r1 定验第 4 条：``record.msg`` / ``record.args`` 也要钉，不能只看渲染文本。"""
        self._run_main(monkeypatch, caplog)
        fields = log_record_fields(caplog.records)
        assert caplog.records, "启动器没有产生任何日志记录"
        assert fields, "记录字段为空"
        assert find_needles(fields, NEEDLES) == []
        assert URL not in fields


# --------------------------------------------------------------------------- #
# 出口 2：注册元数据（registry.register → SETEX 持久化）
# --------------------------------------------------------------------------- #
class TestRegistryMetadataHidesPassword:
    """注册键的值（``ServiceInfo.to_json()``）不得含口令。"""

    def test_registry_key_name_is_unchanged(self):
        worker, client = build_redis_flow_worker(URL)
        try:
            assert worker.register_service() is True
        finally:
            worker.unregister_service()
        assert client.setex_calls, "register_service() 没有写注册键"
        key = client.setex_calls[0][0]
        assert key.startswith("plaita:registry:flow_worker:")

    def test_registry_metadata_carries_no_password(self):
        worker, client = build_redis_flow_worker(URL)
        try:
            assert worker.register_service() is True
        finally:
            worker.unregister_service()
        stored = stored_values(client)
        assert stored, "register_service() 没有写注册键"
        assert find_needles(stored, NEEDLES) == []
        assert URL not in stored

    def test_registry_metadata_keeps_masked_url_and_endpoint(self):
        worker, client = build_redis_flow_worker(URL)
        try:
            assert worker.register_service() is True
        finally:
            worker.unregister_service()
        payload = json.loads(client.setex_calls[0][2])
        meta = payload["metadata"]
        # 键名/语义不变：仍然叫 redis_url，只是值是掩码形态。
        assert meta["redis_url"] == MASKED_URL
        # 端点三件套保住运维诊断价值。
        assert meta["redis_host"] == "127.0.0.1"
        assert meta["redis_port"] == 6379
        assert meta["redis_db"] == 0
        assert meta["queue_name"] == "plaita:flow:queue"

    def test_malformed_url_is_still_mask_only_and_never_raises(self):
        """畸形 URL：注册元数据降级为只带掩码 URL，且不抛。"""
        from plaita.server.flow_worker import _redis_registry_metadata

        meta = _redis_registry_metadata(f"redis:/user:{SECRET}@host")
        assert find_needles(json.dumps(meta), NEEDLES) == []
        assert SECRET not in json.dumps(meta)
        assert "redis_host" not in meta  # 解析失败：不编造端点
        assert meta["redis_url"].startswith("redis:/")


class TestRegistryLiveReadbackUsesProductionHelper:
    """r1 定验 FAIL (b)：``--live`` 校验**自证伪**——它自己手搓原文元数据写进去再读回来
    判「是否含口令」，于是在**已修好**的树上恒判 LEAK/exit 1。

    修法：读回路径必须消费**生产** helper（``_redis_registry_metadata``）构造的元数据。
    这里把「写 → 读回 → 比对」整条链路钉住，并附一条**负向对照**证明 needle 检查本身
    有能力对「手搓原文」判红（否则「没检出」与「检查失灵」不分）。
    """

    def test_readback_equals_production_helper_output_and_has_no_password(self):
        from plaita.server.flow_worker import _redis_registry_metadata

        worker, client = build_redis_flow_worker(URL)
        try:
            assert worker.register_service() is True
            key = client.setex_calls[0][0]
            readback = client.get(key)  # 真·读回，不是重算
        finally:
            worker.unregister_service()
        assert readback is not None
        meta = json.loads(readback)["metadata"]
        production = _redis_registry_metadata(URL)
        # 读回的端点三件套就是生产 helper 的输出 ⇒ 校验走的是生产路径。
        for field in ("redis_url", "redis_host", "redis_port", "redis_db"):
            assert meta[field] == production[field], field
        assert find_needles(readback, NEEDLES) == []

    def test_negative_control_handbuilt_raw_metadata_would_be_caught(self):
        """负向对照：手搓原文元数据**必须**被 needle 检查抓住。"""
        handbuilt = json.dumps({"metadata": {"redis_url": URL}})
        assert find_needles(handbuilt, NEEDLES) != []


# --------------------------------------------------------------------------- #
# r1 定验 FAIL (a)：非 UTF-8 口令 + 掩码失败都不得炸调用方
# --------------------------------------------------------------------------- #
class TestNonUtf8PasswordNeverBreaksCallers:
    def test_services_main_survives_surrogateescape_password(self, monkeypatch, caplog):
        """非 UTF-8 口令（surrogateescape 孤立代理字符）下：只走 ``SystemExit``。"""
        import redis as redis_pkg

        from plaita.server.services import __main__ as svc_main

        monkeypatch.setenv("PLAITA_REDIS_URL", NON_UTF8_URL)
        monkeypatch.setattr(
            redis_pkg.Redis, "from_url", lambda url=None, **kw: PingFailingRedis(url)
        )
        monkeypatch.setattr("sys.argv", ["services", "delay_service"])
        with caplog.at_level(logging.INFO, logger="plaita"):
            with pytest.raises(SystemExit):  # 而不是 UnicodeEncodeError
                svc_main.main()
        text = log_record_fields(caplog.records)
        assert NON_UTF8_SECRET not in text
        assert NON_UTF8_URL not in text

    def test_worker_construction_and_registration_survive_surrogateescape_password(self):
        """r1 定验里 ``RedisFlowWorker.__init__`` 曾直接抛 ``UnicodeEncodeError``。"""
        worker, client = build_redis_flow_worker(NON_UTF8_URL)
        try:
            assert worker.register_service() is True
        finally:
            worker.unregister_service()
        stored = stored_values(client)
        assert stored, "register_service() 没有写注册键"
        assert NON_UTF8_SECRET not in stored
        assert NON_UTF8_URL not in stored

    def test_renderer_failure_degrades_to_safe_constant_not_raw_url(self, monkeypatch):
        """掩码器被 patch 成抛异常：调用点必须拿到安全常量，绝不拿到原串。"""
        from plaita.core import url_redaction
import plaita.server.factory  # noqa: F401 — plaita#160/#161 重切：_mask_url 的注册点在本模块
                          # （factory 模块体 server→core 向下注册）；不 import 则日志/渲染路径 fail-closed。

        def boom(value):
            raise UnicodeEncodeError("utf-8", "x", 0, 1, "boom")

        monkeypatch.setattr(url_redaction, "_mask_url", boom)
        rendered = url_redaction._mask_url_for_log(URL)
        assert rendered == url_redaction.UNPRINTABLE_URL
        assert rendered != URL
        assert find_needles(rendered, NEEDLES) == []

    def test_registry_metadata_failure_degrades_to_safe_constant(self, monkeypatch):
        from plaita.server import flow_worker as flow_worker_module
        from plaita.core import url_redaction

        def boom(value):
            raise RuntimeError("masker exploded")

        monkeypatch.setattr(url_redaction, "_mask_url", boom)
        meta = flow_worker_module._redis_registry_metadata(URL)
        assert meta["redis_url"] == url_redaction.UNPRINTABLE_URL
        assert find_needles(json.dumps(meta), NEEDLES) == []

    def test_arbitrary_bytes_password_never_raises_and_never_leaks_the_raw_url(self):
        """随机字节 fuzz：``_mask_url_for_log`` 不抛、结果可 utf-8 编码、不含原始 URL。"""
        rng = random.Random(20261015)
        for _ in range(200):
            raw = bytes(rng.randrange(0x20, 0x100) for _ in range(rng.randrange(3, 12)))
            password = raw.decode("utf-8", "surrogateescape")
            candidate = f"redis://user:{password}@127.0.0.1:6379/0"
            rendered = _mask_url_for_log(candidate)
            assert isinstance(rendered, str)
            rendered.encode("utf-8")  # 不抛 ⇒ 任何 utf-8 handler 都写得出去
            assert candidate not in rendered

    def test_failure_never_breaks_the_startup_log_path(self, monkeypatch, caplog):
        """把 ``_mask_url`` patch 成抛异常后，服务启动器依旧只走 ``SystemExit``。"""
        import redis as redis_pkg

        from plaita.core import url_redaction
        from plaita.server.services import __main__ as svc_main

        monkeypatch.setattr(
            url_redaction,
            "_mask_url",
            lambda value: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        monkeypatch.setenv("PLAITA_REDIS_URL", URL)
        monkeypatch.setattr(
            redis_pkg.Redis, "from_url", lambda url=None, **kw: PingFailingRedis(url)
        )
        monkeypatch.setattr("sys.argv", ["services", "delay_service"])
        with caplog.at_level(logging.INFO, logger="plaita"):
            with pytest.raises(SystemExit):
                svc_main.main()
        fields = log_record_fields(caplog.records)
        assert find_needles(fields, NEEDLES) == []
        assert url_redaction.UNPRINTABLE_URL in fields


# --------------------------------------------------------------------------- #
# 同类面：观测告警不得回显节点对象的 repr（RedisNode.target 是带凭据 URL）
# --------------------------------------------------------------------------- #
class TestNodeTimingAlertsNeverEchoNodeRepr:
    def _redis_node(self):
        pytest.importorskip("redis")
        from plaita.node.redis import RedisNode

        return RedisNode(id="redis-node-1", target=URL, command="GET", arguments="k")

    def _fail_start(self, caplog, node):
        from plaita.server.node_timings import NodeTimingCallback

        def boom():
            raise RuntimeError("clock boom")

        callback = NodeTimingCallback(clock=boom)
        with caplog.at_level(logging.WARNING):
            callback.on_node_start(flow=None, node=node)
            callback.on_node_end(flow=None, node=node)
        return caplog.records

    def test_premise_node_repr_really_carries_the_target(self):
        node = self._redis_node()
        assert SECRET in repr(node), "前提失效：节点 repr 本应带上 target"

    def test_warning_records_never_carry_the_password(self, caplog):
        records = self._fail_start(caplog, self._redis_node())
        assert records, "没有产生 WARNING 记录"
        fields = log_record_fields(records)
        assert find_needles(fields, NEEDLES) == []
        assert URL not in fields

    def test_warning_still_locates_the_node(self, caplog):
        records = self._fail_start(caplog, self._redis_node())
        fields = log_record_fields(records)
        assert "RedisNode(id=redis-node-1)" in fields


# --------------------------------------------------------------------------- #
# 源码级守卫：logger.* 不得直接插值任何凭据 URL
# --------------------------------------------------------------------------- #
class TestNoLoggerCallInterpolatesRawCredentialUrl:
    """守卫：全 ``plaita/`` 里 ``logger.*`` 不得插值**未经掩码**的凭据 URL。

    与 ``#88`` 的消息断言同源纪律：**负向对照必须先证明守卫会红**，否则「没检出」
    可能只是守卫失灵。``#159`` 的两个 sink（``self.url`` / ``self._alert_webhook``）
    正是 r1 守卫的盲区——那时判据只有「名字里有 ``redis_url``」。
    """

    def test_guard_reddens_on_raw_url_log(self):
        assert credential_url_log_sites('logger.info("连接 Redis: %s", redis_url)\n') == [
            (1, "redis_url")
        ]

    def test_guard_reddens_on_attribute_and_subscript_forms(self):
        for source in (
            'logger.warning("cfg=%s", args.redis_url)\n',
            'logger.error("cfg=%s", cfg["redis_url"])\n',
            'logger.info(f"连接 {self.redis_url}")\n',
            'logger.info("连接 %s" % redis_url)\n',
        ):
            assert credential_url_log_sites(source), source

    def test_guard_reddens_when_the_url_is_hidden_inside_a_call_chain(self):
        """r1 假阴之二：``Call.func`` 从不被访问 ⇒ ``redis_url.strip()`` 漏判。"""
        assert credential_url_log_sites('logger.info("连接 %s", redis_url.strip())\n')
        assert credential_url_log_sites('logger.info("连接 %s", redis_url.replace("a", "b"))\n')
        assert credential_url_log_sites('logger.info("连接 %s", args.redis_url.strip())\n')

    def test_guard_reddens_on_non_redis_credential_urls(self):
        """``#159`` 的盲区：``self.url`` / ``self._alert_webhook`` / webhook 局部变量。"""
        for source in (
            'logger.warning("告警 webhook 发送失败 url=%s: %s", self.url, exc)\n',
            'logger.info("死信告警 webhook 已启用: %s", self._alert_webhook)\n',
            'logger.info("hook=%s", alert_webhook)\n',
            'logger.info("hook=%s", webhook_url)\n',
            'logger.info("u=%s", request.url)\n',
            'logger.info("u=%s", cfg["url"])\n',
        ):
            assert credential_url_log_sites(source), source

    def test_passwords_is_not_treated_as_a_masker(self):
        """r1 假阴之一：``_passwords`` 返回的是**原始口令**，不能进白名单。"""
        assert "_passwords" not in _MASKER_NAMES
        assert credential_url_log_sites('logger.info("%s", _passwords(redis_url))\n')

    def test_guard_accepts_masked_forms(self):
        for source in (
            'logger.info("连接 Redis: %s", _mask_url(redis_url))\n',
            'logger.info("连接 Redis: %s", _mask_url_for_log(redis_url))\n',
            'logger.warning("端点不可用: %s", _scrub(redis_url, redis_url))\n',
            'logger.warning("url=%s", _mask_webhook_url_for_log(self.url))\n',
            'logger.info("hook: %s", _mask_webhook_url_for_log(self._alert_webhook))\n',
            'logger.info("连接 Redis: %s", _mask_url_for_log(redis_url).strip())\n',
        ):
            assert credential_url_log_sites(source) == [], source

    def test_guard_ignores_unrelated_names(self):
        for source in (
            'logger.info("msg=%s", exc)\n',
            'logger.debug("q=%s", queue_name)\n',
            'logger.info("count=%d", total)\n',
        ):
            assert credential_url_log_sites(source) == [], source

    def test_probe_file_reintroducing_the_pattern_reddens_then_greens(self, tmp_path):
        """**探针文件 red→green**：把仓库里已修好的写法还原成旧写法，守卫必须判红；
        换回修复后的写法必须判绿。

        探针直接由**真实源文件**改一行生成，因此不会与源文件漂移。
        """
        alerts_source = (PLAITA_DIR / "server" / "alerts.py").read_text(encoding="utf-8")
        fixed_call = "_mask_webhook_url_for_log(self.url)"
        assert fixed_call in alerts_source, "前提失效：alerts.py 里没有掩码后的写法"

        probe = tmp_path / "probe_alerts.py"
        probe.write_text(alerts_source.replace(fixed_call, "self.url"), encoding="utf-8")
        red = credential_url_log_sites(probe.read_text(encoding="utf-8"), filename=str(probe))
        assert red, "守卫没有抓住被还原成旧写法的探针文件（假阴）"

        probe.write_text(alerts_source, encoding="utf-8")
        green = credential_url_log_sites(probe.read_text(encoding="utf-8"), filename=str(probe))
        assert green == [], green

    def test_probe_file_for_redis_sink_reddens_then_greens(self, tmp_path):
        main_source = (
            PLAITA_DIR / "server" / "services" / "__main__.py"
        ).read_text(encoding="utf-8")
        fixed_call = "_mask_url_for_log(redis_url)"
        assert fixed_call in main_source, "前提失效：services/__main__.py 里没有掩码后的写法"

        probe = tmp_path / "probe_main.py"
        probe.write_text(main_source.replace(fixed_call, "redis_url"), encoding="utf-8")
        assert credential_url_log_sites(
            probe.read_text(encoding="utf-8"), filename=str(probe)
        ), "守卫没有抓住被还原成旧写法的探针文件（假阴）"

    def test_no_credential_url_in_any_plaita_logger_call(self):
        offenders = scan_plaita_for_credential_url_logs()
        assert offenders == [], (
            "logger.* 直接插值了原始凭据 URL（口令/webhook 密钥会进日志）——"
            "请经 plaita/core/url_redaction.py 的 _mask_url_for_log / "
            "_mask_webhook_url_for_log 渲染后再记录：\n  " + "\n  ".join(offenders)
        )
