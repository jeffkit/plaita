"""plaita#158+#159 r2 —— 策略 URL 不得经**异常对象**流入日志 sink。

r1（``757f04e9``）把六个 ``logger.*`` 直接插值 URL 的记录面收口，但守卫的判据是
「``logger.*`` 实参里有没有 URL 名字」。``plaita/node/http.py`` 的策略消息把
**原始 URL 拼进异常**：

* ``_host_allowed`` / ``_validate_host_target`` 的 ``URLPolicyError`` 文案带
  ``?access_token=…``（``#159`` 的钉钉形状）；
* ``HTTP.execute`` 用 ``NodeException(code, str(err))`` 包它；
* ``core.runner._handle_node_error`` 折成 ``NodeExecutionError``，
  ``abort`` 之外的策略还把 ``e`` 交给 ``logger.warning(…, e)``；
  ``server/flow_worker`` 在默认 abort 路径也打 ``exc``（含 ``exc_info=True``）。

于是一条链上有五个输出面（``str`` / ``repr`` / ``traceback.format_exception`` /
捕获到的 WARNING·ERROR 记录的 ``msg``·``args``·``getMessage``·格式化后的
``exc_text`` / ``NodeExecutionError`` 链）。「logger 实参名」判据对**异常对象**
结构性失明——这正是 r2 独立定验发现的、r1 修不掉且守卫看不见的同类出口。

修法：在 ``node/http.py`` **构造消息时**就用 ``url_redaction`` 的渲染器掩码
（``_mask_policy_url``），明文根本不进异常对象。本文件钉三件事：

1. 渲染器保 ``scheme://host[:port]``、抹每个 path segment 与整条 query；
2. 动态红/绿：用真实 ``HTTP`` 节点 + 真实 ``_handle_node_error``，五个面 + 三条
   路径都不含密钥本体与任何 ≥3 字符前缀/后缀；
3. 守卫第二腿：``raise`` 消息里未掩码的 URL 名必须判红（探针由**真实源文件**
   改一行生成）。
"""
from __future__ import annotations

import asyncio
import logging
import re
import traceback
from contextlib import contextmanager

import pytest

from plaita.core.errors import ErrorHandler, ErrorStrategy, NodeException, NodeExecutionError
from plaita.core.runner import _handle_node_error
import plaita.node.http as http_mod
from plaita.node.http import (
    HTTP,
    URLPolicyError,
    _host_allowed,
    _validate_host_target,
)
from plaita.server.url_redaction import UNPRINTABLE_URL
from tests.unit import _url_sink_support as sink_support
from tests.unit._url_sink_support import (
    PLAITA_DIR,
    REPO_ROOT,
    find_needles,
    leak_needles,
    log_record_fields,
)

#: 钉钉 ``?access_token=`` 形状的密钥：混合大小写 + 数字，任何 ≥3 字符前缀/后缀
#: 都不会偶然出现在固定文案里。
SECRET = "Ks9Qm4Zt"
URL = f"http://10.1.2.3/robot/send?access_token={SECRET}"
NEEDLES = leak_needles(SECRET)

_FORMATTER = logging.Formatter("%(levelname)s %(name)s %(message)s")


def _policy_renderer():
    """本单新增的构造面渲染器；在 757f04e9 上不存在 ⇒ 相关断言必须判红。"""
    renderer = getattr(http_mod, "_mask_policy_url", None)
    assert renderer is not None, (
        "缺少 plaita/node/http.py::_mask_policy_url——策略 URL 没有在构造消息时掩码"
    )
    return renderer


def _exception_message_sites(source: str, filename: str = "<memory>"):
    """守卫第二腿；在 757f04e9 的 support 模块里不存在 ⇒ 相关断言必须判红。"""
    fn = getattr(sink_support, "exception_message_url_sites", None)
    assert fn is not None, (
        "守卫缺少第二腿：_url_sink_support.exception_message_url_sites"
        "（「raise 消息里未掩码的 URL 名」判据）"
    )
    return fn(source, filename)


def _scan_exception_messages():
    fn = getattr(sink_support, "scan_plaita_for_exception_url_messages", None)
    assert fn is not None, "守卫缺少第二腿的全树扫描 scan_plaita_for_exception_url_messages"
    return fn()


def _run_policy_node(**kwargs) -> NodeException:
    """真实 HTTP 节点走真实 ``execute``：策略拒绝 ⇒ ``NodeException``。"""
    node = HTTP(
        id="http_policy_sink",
        name="http_policy_sink",
        url=URL,
        method="GET",
        **kwargs,
    )
    with pytest.raises(NodeException) as excinfo:
        node.execute({})
    return excinfo.value


def _chain_text(exc: BaseException) -> str:
    """异常本体 + ``__cause__``/``__context__`` 链的 ``str``/``repr``/traceback。"""
    chunks = []
    seen = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chunks.append(str(current))
        chunks.append(repr(current))
        chunks.append("".join(
            traceback.format_exception(type(current), current, current.__traceback__)
        ))
        current = current.__cause__ or current.__context__
    return "\n".join(chunks)


def _records_text(records) -> str:
    """记录级字段 + Formatter 渲染（含 ``exc_info`` 的 traceback）。"""
    chunks = [log_record_fields(records)]
    for record in records:
        chunks.append(_FORMATTER.format(record))
    return "\n".join(chunks)


def _assert_clean(text: str, label: str) -> None:
    hits = find_needles(text, NEEDLES)
    assert hits == [], f"{label} 泄露了策略 URL 的密钥（needles={hits}）：\n{text}"


# --------------------------------------------------------------------------- #
# ① 渲染器：诊断价值保留，凭据全抹
# --------------------------------------------------------------------------- #
class TestPolicyUrlRenderer:
    def test_keeps_endpoint_and_masks_path_and_query(self):
        assert _policy_renderer()(URL) == "http://10.1.2.3/***/***?***"

    def test_never_returns_the_secret_or_any_needle(self):
        _assert_clean(_policy_renderer()(URL), "_mask_policy_url")

    def test_also_masks_userinfo_password(self):
        """HTTP 策略 URL 也可能带 ``#88`` 型 userinfo 口令——两个渲染器串联。"""
        rendered = _policy_renderer()(f"http://user:{SECRET}@10.1.2.3/robot/send")
        assert "10.1.2.3" in rendered
        _assert_clean(rendered, "_mask_policy_url(userinfo)")

    def test_is_total_and_fail_closed(self):
        """任意输入都不抛；判别不了端点时退化常量，绝不回原文。"""
        render = _policy_renderer()
        for value in (None, b"\xff\xfe", "", "not a url", f"redis://u:{SECRET}@h/0",
                      object(), f"http://10.1.2.3/{SECRET}"):
            rendered = render(value)
            assert isinstance(rendered, str)
            _assert_clean(rendered, f"_mask_policy_url({value!r})")
        assert render("not a url") == UNPRINTABLE_URL


# --------------------------------------------------------------------------- #
# ② 动态红/绿：五面 × 三路径
# --------------------------------------------------------------------------- #
class TestPolicyExceptionNeverCarriesTheSecret:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"deniedHosts": ["10.0.0.0/8"]},
            {"blockPrivateNetworks": True},
            {"allowedHosts": ["only.example.com"]},
        ],
        ids=["denied-hosts", "block-private", "allowed-hosts"],
    )
    def test_success_path_surfaces(self, kwargs):
        """路径 A：``HTTP.execute`` 直接抛的 ``NodeException``。"""
        exc = _run_policy_node(**kwargs)
        assert isinstance(exc, NodeException)
        _assert_clean(str(exc), "str(NodeException)")
        _assert_clean(repr(exc), "repr(NodeException)")
        _assert_clean("".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ), "traceback.format_exception(NodeException)")
        # 诊断价值仍在：端点保留
        assert "10.1.2.3" in str(exc)

    def test_policy_error_message_itself_is_masked(self):
        """构造面：``URLPolicyError``（不是包装后的 NodeException）就已是掩码形态。"""
        calls = [
            lambda: _host_allowed(URL, None, ["10.0.0.0/8"], False),
            lambda: _host_allowed(URL, None, None, True),
            lambda: _host_allowed(URL, ["only.example.com"], None, False),
            lambda: _host_allowed(f"ftp://10.1.2.3/x?access_token={SECRET}", None, None, False),
            lambda: _host_allowed(f"http:///x?access_token={SECRET}", None, None, False),
            lambda: _validate_host_target("10.1.2.3", ["10.1.2.3"], None, ["10.0.0.0/8"], False, URL),
            lambda: _validate_host_target("10.1.2.3", None, None, None, True, URL),
            lambda: _validate_host_target("10.1.2.3", ["10.1.2.3"], ["only.example.com"], None, False, URL),
        ]
        for call in calls:
            with pytest.raises(URLPolicyError) as excinfo:
                call()
            _assert_clean(str(excinfo.value), "str(URLPolicyError)")
            _assert_clean(repr(excinfo.value), "repr(URLPolicyError)")

    def test_continue_handler_surfaces(self, caplog):
        """路径 B：errorHandler.strategy=continue ⇒ runner 的 WARNING + DEBUG 记录。"""
        exc = _run_policy_node(deniedHosts=["10.0.0.0/8"])
        node = HTTP(id="http_policy_sink", name="http_policy_sink", url=URL, method="GET")
        with caplog.at_level(logging.DEBUG, logger="plaita.core.runner"):
            result = _handle_node_error(
                None, node, ErrorHandler(strategy=ErrorStrategy.CONTINUE), exc,
            )
        assert result is None  # CONTINUE 语义未变
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert warnings, "前提失效：CONTINUE 路径没有产生 WARNING 记录"
        _assert_clean(_records_text(caplog.records), "CONTINUE records")
        _assert_clean(caplog.text, "caplog.text(CONTINUE)")

    def test_default_abort_surfaces(self, caplog):
        """路径 C：默认 abort ⇒ ``NodeExecutionError``，flow_worker 打 ``exc``。"""
        exc = _run_policy_node(deniedHosts=["10.0.0.0/8"])
        node = HTTP(id="http_policy_sink", name="http_policy_sink", url=URL, method="GET")
        with pytest.raises(NodeExecutionError) as excinfo:
            _handle_node_error(None, node, None, exc)
        abort = excinfo.value
        assert abort.__cause__ is exc  # 因果链未变（`raise … from e`）
        _assert_clean(str(abort), "str(NodeExecutionError)")
        _assert_clean(repr(abort), "repr(NodeExecutionError)")
        _assert_clean(_chain_text(abort), "NodeExecutionError __cause__ chain")

        worker_logger = logging.getLogger("plaita.server.flow_worker")
        with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
            worker_logger.warning(
                "任务 %s 节点执行失败待重投 (delivery=%s): %s", "1-0", 1, abort,
            )
            worker_logger.error(
                "任务 %s 节点执行失败: %s", "1-0", abort, exc_info=True,
            )
        _assert_clean(_records_text(caplog.records), "abort log records")
        _assert_clean(caplog.text, "caplog.text(abort)")


# --------------------------------------------------------------------------- #
# ③ 守卫第二腿：异常消息里的 URL
# --------------------------------------------------------------------------- #
class TestGuardSeesUrlBearingExceptionMessages:
    def test_guard_reddens_on_raw_url_in_raise_message(self):
        assert _exception_message_sites('raise URLPolicyError(f"x: {url}")\n') == [(1, "url")]

    def test_guard_reddens_on_attribute_and_subscript_forms(self):
        for source in (
            'raise NodeException(1002, f"x: {self.redis_url}")\n',
            "raise NodeExecutionError(f\"x: {cfg['url']}\")\n",
            'raise RuntimeError("x: %s" % alert_webhook)\n',
            'raise ValueError("x: {}".format(request.url))\n',
        ):
            assert _exception_message_sites(source), source

    def test_guard_accepts_masked_forms(self):
        for source in (
            'raise URLPolicyError(f"x: {_mask_policy_url(url)}")\n',
            'raise URLPolicyError(f"x: {_mask_webhook_url_for_log(_mask_url_for_log(url))!r}")\n',
            'raise NodeException(code, _scrub(url, url))\n',
        ):
            assert _exception_message_sites(source) == [], source

    def test_guard_ignores_non_url_names(self):
        """节点 id 也叫 ``target``，但它不是 URL——判据不许误伤（r2 扫描的假阳类）。"""
        for source in (
            'raise ValueError(f"count={total}")\n',
            'raise FlowIRValidationError(f"节点指向 {target!r}")\n',
            'raise _CodeflowError("循环变量必须是纯名字", target)\n',
        ):
            assert _exception_message_sites(source) == [], source

    def test_probe_policy_raise_reddens_then_greens(self, tmp_path):
        """探针由**真实源文件**改一行生成——先证明守卫会红，再回到绿。"""
        http_source = (PLAITA_DIR / "node" / "http.py").read_text(encoding="utf-8")
        fixed_call = "_mask_policy_url(url)"
        assert fixed_call in http_source, "前提失效：http.py 里没有 _mask_policy_url(url)"

        probe = tmp_path / "probe_http.py"
        probe.write_text(http_source.replace(fixed_call, "url", 1), encoding="utf-8")
        red = _exception_message_sites(
            probe.read_text(encoding="utf-8"), filename=str(probe)
        )
        assert red, "守卫没有抓住把策略消息还原成原始 URL 的探针（假阴）"

        probe.write_text(http_source, encoding="utf-8")
        green = _exception_message_sites(
            probe.read_text(encoding="utf-8"), filename=str(probe)
        )
        assert green == [], green

    def test_probe_renderer_bypass_reddens_then_greens(self):
        """渲染器被绕过（等价于「消息仍走构造面、但渲染器退化为原文」）时五个面重红。

        由**真实模块**生成：把 ``_mask_policy_url`` 临时替换成恒等函数，再用真实
        策略消息断言——AST 守卫看不见「渲染器内部被改坏」，这条动态探针负责补位。
        """
        original = _policy_renderer()
        try:
            http_mod._mask_policy_url = lambda value: str(value)  # 绕过掩码
            with pytest.raises(URLPolicyError) as excinfo:
                _host_allowed(URL, None, ["10.0.0.0/8"], False)
            hits = find_needles(str(excinfo.value), NEEDLES)
            assert hits, "前提失效：渲染器被绕过时策略消息竟然还是干净的"
        finally:
            http_mod._mask_policy_url = original
        # 还原后回到绿
        with pytest.raises(URLPolicyError) as excinfo:
            _host_allowed(URL, None, ["10.0.0.0/8"], False)
        _assert_clean(str(excinfo.value), "restored renderer")

    def test_no_exception_message_carries_a_credential_url(self):
        offenders = _scan_exception_messages()
        assert offenders == [], (
            "raise 消息直接插值了原始凭据 URL——明文会随异常对象进 "
            "runner/worker 的日志 sink。请经 plaita/node/http.py 的 "
            "_mask_policy_url（或 url_redaction 的渲染器）渲染后再抛：\n  "
            + "\n  ".join(offenders)
        )


# --------------------------------------------------------------------------- #
# ④ 守卫覆盖面：定验实测的 MISS 族（别名 / getattr / get / full_url 族 /
#    inline getLogger / 异常 notes）
# --------------------------------------------------------------------------- #
def _log_sites(source: str, filename: str = "<memory>"):
    fn = getattr(sink_support, "credential_url_log_sites", None)
    assert fn is not None, "守卫缺少 credential_url_log_sites"
    return fn(source, filename)


class TestGuardCoversTheVerifiedMissFamily:
    def test_guard_sees_inline_getlogger(self):
        """定验真发现：``logging.getLogger(__name__).info(...)`` 在旧判据下恒漏。

        旧 ``_is_log_call`` 对「func 是 Call 的属性」只拿得到方法名 ``"info"``，
        ``rsplit('.',1)[0]`` 得到非空 ``"info"`` ⇒ 回退分支是死代码 ⇒ 恒返回 False。
        """
        assert _log_sites(
            'logging.getLogger(__name__).info("连接 Redis: %s", redis_url)\n'
        ) == [(1, "redis_url")]
        assert _log_sites(
            'logging.getLogger("plaita.x").warning("hook=%s", self._alert_webhook)\n'
        )

    def test_probe_inline_getlogger_from_real_source_reddens_then_greens(self, tmp_path):
        """探针由**真实源文件**（``server/log_handler.py`` 的 inline 调用）改一处生成。"""
        source_path = PLAITA_DIR / "server" / "log_handler.py"
        real_source = source_path.read_text(encoding="utf-8")
        fixed_line = 'logging.getLogger(__name__).warning("日志刷新失败: %s", e)'
        assert fixed_line in real_source, "前提失效：log_handler.py 里没有该 inline 调用"

        # 真实源本身干净（e 不是 URL 名）
        assert _log_sites(real_source, filename=str(source_path)) == []
        # 只把 e 换成 URL 名 ⇒ 守卫必须看见 inline 形态
        probe = tmp_path / "probe_log_handler.py"
        probe.write_text(
            real_source.replace(fixed_line, fixed_line.replace(", e)", ", redis_url)"), 1),
            encoding="utf-8",
        )
        assert _log_sites(probe.read_text(encoding="utf-8"), filename=str(probe)), (
            "守卫对 inline `logging.getLogger(...)` 形态失明"
        )

    def test_guard_sees_url_family_names_and_getattr_get(self):
        for source in (
            'logger.info("u=%s", full_url)\n',
            'logger.info("u=%s", callback_url)\n',
            'logger.info("u=%s", base_url)\n',
            "logger.info(\"u=%s\", cfg['full_url'])\n",
            'logger.info("u=%s", getattr(cfg, "url"))\n',
            'logger.info("u=%s", cfg.get("url"))\n',
        ):
            assert _log_sites(source), source

    def test_guard_sees_local_aliases(self):
        for source in (
            'u = redis_url\nlogger.info("u=%s", u)\n',
            'u = redis_url\nw = u\nlogger.info("w=%s", w)\n',
            'u = self.url\nlogger.warning("u=%s", u)\n',
        ):
            assert _log_sites(source), source

    def test_guard_does_not_taint_through_arbitrary_expressions(self):
        """别名只认**纯复制**：``x = f(redis_url)`` 不把 x 判成 URL（否则全树误红）。"""
        assert _log_sites('x = f(redis_url)\nlogger.info("x=%s", x)\n') == []

    def test_guard_sees_exception_notes(self):
        """``exc.add_note(url)`` 的 notes 由 traceback 打印，同样是输出面。"""
        assert _exception_message_sites("exc.add_note(redis_url)\n") == [(1, "redis_url")]

    def test_capability_boundaries_are_documented_not_silent(self):
        """判据能力边界（定验实测 MISS，刻意不判；docstring 有声明）。"""
        doc = sink_support.credential_url_log_sites.__doc__ or ""
        assert "能力边界" in doc
        for boundary in ("adapter.info", "stream.write", "print"):
            assert boundary in doc, boundary
        # 边界形态确实判不了（不是「碰巧干净」）
        assert _log_sites('adapter.info("u=%s", redis_url)\n') == []
        assert _log_sites('stream.write(str(redis_url))\n') == []


# --------------------------------------------------------------------------- #
# ⑤ 记录级 ``record.__dict__``：结构化日志/JSON shipper 的真实泄露面
# --------------------------------------------------------------------------- #
class TestLogRecordDictSurface:
    def test_extra_key_is_asserted_even_when_msg_and_args_are_clean(self):
        record = logging.LogRecord(
            "plaita.test", logging.INFO, __file__, 1, "endpoint configured", (), None
        )
        record.hook = URL  # 等价于 logger.info(..., extra={"hook": URL})
        # msg/args/getMessage 三面确实干净 —— 只看它们会 100% 漏判
        assert URL not in str(record.msg)
        assert URL not in repr(record.args)
        assert URL not in record.getMessage()
        # log_record_fields 覆盖 __dict__ ⇒ 判红
        assert find_needles(log_record_fields([record]), NEEDLES), (
            "log_record_fields 没有覆盖 record.__dict__（extra= 注入的密钥漏判）"
        )

    def test_standard_attrs_do_not_false_positive(self):
        record = logging.LogRecord(
            "plaita.test", logging.INFO, __file__, 1, "hello %s", ("world",), None
        )
        assert find_needles(log_record_fields([record]), NEEDLES) == []


# --------------------------------------------------------------------------- #
# ⑥ 第三方异常文本：requests/aiohttp 把请求 URL 写进自己的异常
# --------------------------------------------------------------------------- #
def _scrubber():
    fn = getattr(http_mod, "_scrub_url_text", None)
    assert fn is not None, "缺少 plaita/node/http.py::_scrub_url_text（第三方异常文本清洗）"
    return fn


def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class TestThirdPartyExceptionTextIsScrubbed:
    def test_scrubber_masks_absolute_and_relative_refs(self):
        scrub = _scrubber()
        samples = (
            f"Max retries exceeded with url: /robot/send?access_token={SECRET} (Caused by X)",
            f"500, message='boom', url='http://h/robot/send?access_token={SECRET}'",
            f"boom for url: https://hooks.slack.com/services/T00/B00/{SECRET}",
            f"bad url: http://user:{SECRET}@h/x",
        )
        for sample in samples:
            _assert_clean(scrub(sample), "scrubber")

    def test_scrubber_keeps_host_and_is_total(self):
        scrub = _scrubber()
        rendered = scrub(f"Max retries exceeded with url: /robot/send?access_token={SECRET}")
        assert "/***/***?***" in rendered
        for value in (None, b"\xff\xfe", "", object(), 123, "nothing to mask"):
            assert isinstance(scrub(value), str)

    def test_wrap_http_node_err_scrubs_the_message(self):
        node = HTTP(id="http_wrap", name="http_wrap", url=URL, method="GET")
        exc = node.wrap_http_node_err(
            1002,
            f"Max retries exceeded with url: /robot/send?access_token={SECRET}",
            None,
        )
        assert isinstance(exc, NodeException)
        _assert_clean(str(exc), "wrap_http_node_err")

    @pytest.mark.parametrize("surface_path", ["execute", "arun"])
    def test_connection_failure_end_to_end(self, surface_path):
        """真实网络失败（本地无监听端口）：requests/aiohttp 文本不得带 token。"""
        if surface_path == "execute":
            pytest.importorskip("requests")
        else:
            pytest.importorskip("aiohttp")
        port = _free_port()
        url = f"http://127.0.0.1:{port}/robot/send?access_token={SECRET}"
        node = HTTP(id="http_third", name="http_third", url=url, method="GET")
        with pytest.raises(NodeException) as excinfo:
            if surface_path == "execute":
                node.execute({})
            else:
                import asyncio

                asyncio.run(node.arun({}))
        exc = excinfo.value
        _assert_clean(str(exc), f"{surface_path} str(NodeException)")
        _assert_clean(repr(exc), f"{surface_path} repr(NodeException)")
        _assert_clean("".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ), f"{surface_path} traceback")
        assert str(port) in str(exc)  # 诊断价值：host:port 仍在

    def test_third_party_error_through_runner_sinks(self, caplog):
        pytest.importorskip("requests")
        port = _free_port()
        url = f"http://127.0.0.1:{port}/robot/send?access_token={SECRET}"
        node = HTTP(id="http_third", name="http_third", url=url, method="GET")
        with pytest.raises(NodeException) as excinfo:
            node.execute({})
        exc = excinfo.value

        with caplog.at_level(logging.DEBUG, logger="plaita.core.runner"):
            _handle_node_error(
                None, node, ErrorHandler(strategy=ErrorStrategy.CONTINUE), exc,
            )
        _assert_clean(_records_text(caplog.records), "third-party CONTINUE records")
        _assert_clean(caplog.text, "third-party CONTINUE caplog.text")

        with pytest.raises(NodeExecutionError) as excinfo2:
            _handle_node_error(None, node, None, exc)
        _assert_clean(_chain_text(excinfo2.value), "third-party NodeExecutionError chain")

    def test_callback_node_log_line_is_masked(self, caplog):
        """守卫把 ``full_url`` 族纳入判据后点名的唯一生产记录点（#159 r2 修）。"""
        from plaita.server.nodes.http_callback_node import HttpCallbackNode

        node = HttpCallbackNode(id="cb1", name="cb1")
        execution = type("Ctx", (), {
            "execution_id": "exec-secret-id",
            "state": type("S", (), {"flow_id": "flow-secret-id"})(),
        })()
        with caplog.at_level(logging.INFO, logger="plaita"):
            config = node.generate_service_config(execution)
        # 返回值（POST/注册用）仍是原文
        assert config["callback_config"]["full_url"].endswith("/exec-secret-id")
        # 记录面已掩码：端点保留，path segment 全是 ***
        rendered = _records_text(caplog.records)
        assert "http://localhost:8080" in rendered
        assert "/callback/" not in rendered, rendered
        assert find_needles(rendered, leak_needles("exec-secret-id")) == [], rendered


# --------------------------------------------------------------------------- #
# ⑦ r3：**方括号 authority** 的绝对 URL（r2 的匹配器在 `[` 处截断 ⇒ 原文穿过）
# --------------------------------------------------------------------------- #
# 家族：`aiohttp` 对畸形方括号 URL 抛 ``InvalidUrlClientError``，其 ``__str__`` 就是
# **裸 URL**，`_send_async` → `handle_http_node_err` → `wrap_http_node_err` 一路
# ``str(err)`` 转手。r2 的 ``_ABSOLUTE_URL_RE`` 把 ``[``/``]`` 排除在字符类外，
# 于是匹配截断/落空 ⇒ 请求 URL（含 ``access_token``）原文进 NodeException。
# IPv4 对照（无方括号）从来都被掩码，用来钉住「这不是笼统的清洗器失效」。
_BRACKET_FAMILY = [
    ("bracket-hostname", "http://[webhook.internal]:8443"),
    ("bracket-loopback-port", "http://[::1]:999999"),
    ("bracket-empty", "http://[]:8443"),
    ("bracket-v4-mapped", "http://[::FFFF:127.0.0.1]:999999"),
    ("ipv4-control", "http://127.0.0.1:999999"),
]


def _family_url(endpoint: str) -> str:
    return f"{endpoint}/robot/send?access_token={SECRET}"


def _family_params():
    return [
        pytest.param(name, endpoint, endpoint.split("://", 1)[1], id=name)
        for name, endpoint in _BRACKET_FAMILY
    ]


def _aiohttp_invalid_url_text(url: str) -> str:
    """前提探针：``aiohttp`` 对畸形方括号 URL 的异常文本**就是裸 URL**。

    这条前提是 r3 finding 的另一半——如果哪天 aiohttp 不再把 URL 放进 ``str(e)``，
    下面「诊断端点仍在」的断言会以「前提失效」的形式响，而不是悄悄变绿。
    """
    aiohttp = pytest.importorskip("aiohttp")

    async def probe():
        async with aiohttp.ClientSession() as session:
            async with session.get(url):
                pass  # pragma: no cover - 到不了这里

    with pytest.raises(Exception) as excinfo:
        asyncio.run(probe())
    return str(excinfo.value)


class TestBracketedAuthorityFamilyIsMasked:
    """r3：方括号 authority 全家族 —— 五个面干净 + 端点（host:port）仍可诊断。"""

    @pytest.mark.parametrize("variant,url,diagnostic", _family_params())
    def test_renderer_level_masks_and_keeps_endpoint(self, variant, url, diagnostic):
        scrub = _scrubber()
        full = _family_url(url)
        for text in (full, f"boom url='{full}'", f"Invalid URL: {full} (caused by X)"):
            rendered = scrub(text)
            _assert_clean(rendered, f"scrubber[{variant}]")
            assert diagnostic in rendered, (
                f"{variant}: 端点丢了（诊断价值要求 host:port 保留）：{rendered!r}"
            )

    @pytest.mark.parametrize("variant,url,diagnostic", _family_params())
    def test_aiohttp_surface_end_to_end(self, variant, url, diagnostic):
        """真实 ``HTTP.arun`` + 真实 aiohttp：整条链的五个面都不得带 needle。"""
        full = _family_url(url)
        # 前提：aiohttp 的 InvalidUrlClientError 文本是裸 URL（否则这条链根本不含 URL）
        probe_text = _aiohttp_invalid_url_text(full)
        assert full in probe_text, (
            f"前提失效：aiohttp 的异常文本不再带裸 URL（{probe_text!r}）——"
            "本用例的「掩码前后」语义需要重新锚定"
        )

        node = HTTP(id="http_bracket", name="http_bracket", url=full, method="GET")
        with pytest.raises(NodeException) as excinfo:
            asyncio.run(node.arun({}))
        exc = excinfo.value

        _assert_clean(str(exc), f"{variant} str(NodeException)")
        _assert_clean(repr(exc), f"{variant} repr(NodeException)")
        _assert_clean("".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ), f"{variant} traceback")
        assert diagnostic in str(exc), (
            f"{variant}: 掩码把端点也吃了（str={str(exc)!r}）"
        )

        with caplog_continue(node, exc) as records:
            _assert_clean(_records_text(records), f"{variant} CONTINUE records")

        with caplog_abort(node, exc) as (abort, records):
            _assert_clean(_chain_text(abort), f"{variant} abort chain")
            _assert_clean(_records_text(records), f"{variant} abort records")

    def test_userinfo_with_bracketed_authority_is_masked(self):
        scrub = _scrubber()
        raw = f"http://user:{SECRET}@[::1]:8080/robot/send?access_token={SECRET}"
        rendered = scrub(f"boom url='{raw}'")
        _assert_clean(rendered, "userinfo+bracket")
        assert "[::1]:8080" in rendered, rendered

    def test_endpoint_fallback_drops_userinfo_of_malformed_bracket(self):
        """``[bad]`` 不是合法 IPv6 ⇒ `_mask_policy_url` 退化；兜底救端点但丢 userinfo。"""
        scrub = _scrubber()
        raw = f"http://user:{SECRET}@[bad]:1/x?access_token={SECRET}"
        rendered = scrub(raw)
        _assert_clean(rendered, "malformed bracket + userinfo")
        assert rendered == "http://[bad]:1", rendered

    def test_non_http_bracket_scheme_still_degrades(self):
        """scheme 判据与 r2 逐字一致：``redis://`` 的方括号形态仍退化常量。"""
        assert _scrubber()(f"redis://[::1]:6379/0?token={SECRET}") == UNPRINTABLE_URL


@contextmanager
def caplog_continue(node, exc):
    """CONTINUE 路径的记录捕获（与 ② 的 ``_handle_node_error`` 用法同源）。"""
    logger = logging.getLogger("plaita.core.runner")
    handler = _CaptureHandler()
    old = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        _handle_node_error(
            None, node, ErrorHandler(strategy=ErrorStrategy.CONTINUE), exc,
        )
        yield handler.records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old)


@contextmanager
def caplog_abort(node, exc):
    """abort 路径：``NodeExecutionError`` + flow_worker 的 WARNING/ERROR 记录。"""
    with pytest.raises(NodeExecutionError) as excinfo:
        _handle_node_error(None, node, None, exc)
    abort = excinfo.value
    logger = logging.getLogger("plaita.server.flow_worker")
    handler = _CaptureHandler()
    old = logger.level
    logger.setLevel(logging.WARNING)
    logger.addHandler(handler)
    try:
        logger.warning(
            "任务 %s 节点执行失败待重投 (delivery=%s): %s", "1-0", 1, abort,
        )
        logger.error("任务 %s 节点执行失败: %s", "1-0", abort, exc_info=True)
        yield abort, handler.records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old)


class _CaptureHandler(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records: list = []

    def emit(self, record):
        self.records.append(record)


# --------------------------------------------------------------------------- #
# ⑧ r3：`_scrub_url_text` 必须 total（含孤立代理字符的敌对批次）
# --------------------------------------------------------------------------- #
_SURROGATE_HIGH = "\ud800"
_SURROGATE_LOW = "\udcff"
_URL_WITH_SECRET = f"http://h/robot/send?access_token={SECRET}"

#: hostile 批次：孤立代理字符（surrogateescape 型输入的代表）是核心，外加
#: ``None`` / 非 UTF-8 bytes / 容器 / 非字符串对象 —— totality 的常见反例面。
_HOSTILE_TEXTS = [
    ("none", None),
    ("empty", ""),
    ("plain", "nothing to mask"),
    ("bytes-non-utf8", b"\xff\xfe"),
    ("int", 123),
    ("object", object()),
    ("dict", {"url": _URL_WITH_SECRET}),
    ("list", [_URL_WITH_SECRET]),
    ("lone-high", _SURROGATE_HIGH),
    ("lone-low", _SURROGATE_LOW),
    ("lone-high-in-text", f"boom {_SURROGATE_HIGH} tail"),
    ("lone-low-in-text", f"boom {_SURROGATE_LOW} tail"),
    ("surrogate-then-url", f"{_SURROGATE_HIGH} {_URL_WITH_SECRET}"),
    ("url-then-surrogate", f"{_URL_WITH_SECRET} {_SURROGATE_HIGH}"),
    ("surrogate-in-url-path",
     f"http://h/ro{_SURROGATE_HIGH}bot/send?access_token={SECRET}"),
    ("surrogate-in-url-host",
     f"http://ho{_SURROGATE_HIGH}st/robot/send?access_token={SECRET}"),
    ("surrogate-in-bracketed-url",
     f"http://[web{_SURROGATE_HIGH}hook]:8443/x?access_token={SECRET}"),
    ("surrogate-then-needle", f"token={SECRET}{_SURROGATE_HIGH}"),
    ("lone-surrogate-then-needle", f"{_SURROGATE_HIGH}{SECRET}"),
    ("requests-text-with-surrogate",
     f"Max retries exceeded with url: /robot/send?access_token={SECRET}{_SURROGATE_HIGH}"),
    ("aiohttp-text-with-surrogate",
     f"500, message='boom', url='{_URL_WITH_SECRET}'{_SURROGATE_LOW}"),
]


class TestScrubberIsTotalOnHostileBatch:
    """r3：孤立代理字符原样穿过会让下游任何 utf-8 日志 handler 抛异常。"""

    def test_never_raises_and_output_is_str_and_encodable(self):
        scrub = _scrubber()
        for label, value in _HOSTILE_TEXTS:
            rendered = scrub(value)
            assert isinstance(rendered, str), label
            try:
                rendered.encode("utf-8")
            except UnicodeEncodeError as exc:  # pragma: no cover - 修复前在此响
                raise AssertionError(
                    f"{label}: 清洗结果写不进 utf-8 日志（{exc}）：{rendered!r}"
                ) from exc

    def test_is_idempotent_on_the_batch(self):
        scrub = _scrubber()
        for label, value in _HOSTILE_TEXTS:
            once = scrub(value)
            assert scrub(once) == once, f"{label}: 非幂等（{once!r}）"

    def test_never_returns_a_credential_bearing_input_verbatim(self):
        scrub = _scrubber()
        for label, value in _HOSTILE_TEXTS:
            rendered = scrub(value)
            _assert_clean(rendered, f"hostile[{label}]")
            if isinstance(value, str) and SECRET in value:
                assert rendered != value, f"{label}: 带凭据的输入被原样返回"

    def test_mask_policy_url_control_on_the_same_batch(self):
        """对照（定验口径）：``_mask_policy_url`` 在同批次上本就 total/可编码/幂等。"""
        render = _policy_renderer()
        for label, value in _HOSTILE_TEXTS:
            rendered = render(value)
            assert isinstance(rendered, str), label
            rendered.encode("utf-8")
            assert render(rendered) == rendered, f"{label}: 非幂等"
            _assert_clean(rendered, f"policy[{label}]")


# --------------------------------------------------------------------------- #
# ⑨ r4：定验反例的**分隔符矩阵** —— 14 字符 × {方括号, 非方括号} × 六个面
# --------------------------------------------------------------------------- #
# r3 声称「方括号 authority 不再原样穿过」，被 r4 定验直接证伪：
#
# * ``_ABSOLUTE_URL_RE`` 把 ``, ; ( ) " ' < > { }`` 排除在字符类外 ⇒ 匹配在分隔符
#   处截断，而 ``re.sub`` **只替换匹配片段**，尾巴（含 ``access_token=``）原样留下；
# * token 截断在 ``[`` 之后时 ``_mask_bracketed_absolute_url("http://[web")`` 返回
#   **输入本身** ⇒ 替换是恒等 ⇒ 整条带凭据的 URL 逐字穿过。
#
# 复现前提（必须写死，否则会得到**假绿**）：节点上**不设任何策略**
# （``deniedHosts`` / ``blockPrivateNetworks`` / ``allowedHosts`` 全空）。设了策略时
# **URL 解析**先抛（r5 更正：不是 ``_check_policy``——它根本没走到），
# code=1001 ``Failed to parse: Host '[web hook]:8443' contains invalid character ' '``
# ——URL 根本没到 aiohttp，泄露面被掩盖。
_R4_SEPARATORS = ("\n", " ", "\t", "\r", '"', "'", "<", ">", "(", ")", "{", "}", ",", ";")
_R4_ASCII_WS = frozenset(" \t\r\n\f\v")
_R4_BRACKET_PREFIX = "http://[web{sep}hook]:8443/robot/send?access_token="
_R4_PLAIN_PREFIX = "http://127.0.0.1:999999/robot/send?a=1{sep}2&access_token="


def _r4_bracketed(sep: str) -> str:
    return _R4_BRACKET_PREFIX.format(sep=sep) + SECRET


def _r4_plain(sep: str) -> str:
    return _R4_PLAIN_PREFIX.format(sep=sep) + SECRET


def _r4_params():
    return [
        pytest.param(shape, sep, factory, id=f"{shape}-{ord(sep):04X}")
        for shape, factory in (("bracketed", _r4_bracketed), ("nonbracketed", _r4_plain))
        for sep in _R4_SEPARATORS
    ]


def _r4_expected(shape: str, sep: str) -> str:
    """定验口径的**精确字符串**：分隔符留在 token 内 ⇔ 整段被掩码。

    ASCII 空白是刻意保留的 token 边界：URL 在那里本来就不再是一个 URL（aiohttp 也
    拒收），因此掩码覆盖不到尾巴——尾巴里的凭据交给「凭据键=值」那条规则收拾，
    非凭据部分（``hook]:8443/robot/send``）原样留下（诊断价值）。
    """
    if shape == "bracketed":
        if sep in _R4_ASCII_WS:
            return f"{UNPRINTABLE_URL}{sep}hook]:8443/robot/send?access_token=***"
        return f"http://[web{sep}hook]:8443"
    if sep in _R4_ASCII_WS:
        return f"http://127.0.0.1:999999/***/***?***{sep}2&access_token=***"
    return "http://127.0.0.1:999999/***/***?***"


def _six_surfaces(node, exc):
    """一条链上的六个输出面（r4 口径）：3 个异常本体面 + CONTINUE + abort 两路记录。"""
    surfaces = {
        "str(NodeException)": str(exc),
        "repr(NodeException)": repr(exc),
        "traceback.format_exception": "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ),
    }
    with caplog_continue(node, exc) as records:
        surfaces["CONTINUE records"] = _records_text(records)
    with caplog_abort(node, exc) as (abort, records):
        surfaces["abort chain"] = _chain_text(abort)
        surfaces["abort records"] = _records_text(records)
    return surfaces


class TestSeparatorMatrixAllSixSurfaces:
    """r4：14 个分隔符 × 2 种 authority 形状 —— 渲染器精确形态 + 六个面无机密。"""

    @pytest.mark.parametrize("shape,sep,factory", _r4_params())
    def test_renderer_level_exact_string(self, shape, sep, factory):
        url = factory(sep)
        rendered = _scrubber()(url)
        _assert_clean(rendered, f"scrubber[{shape}/{sep!r}]")
        assert rendered != url, f"{shape}/{sep!r}: 恒等替换（原文穿过）：{rendered!r}"
        assert rendered == _r4_expected(shape, sep), (
            f"{shape}/{sep!r}: 分隔符没有留在 token 内 / 掩码形态不符\n"
            f"  实际={rendered!r}\n  期望={_r4_expected(shape, sep)!r}"
        )

    @pytest.mark.parametrize("shape,sep,factory", _r4_params())
    def test_aiohttp_precondition_the_text_is_the_bare_url(self, shape, sep, factory):
        """前提探针：``InvalidUrlClientError`` 的文本**就是**这条裸 URL。

        前提失效（aiohttp 不再把 URL 写进异常文本）时以显式断言响，而不是悄悄变绿。
        """
        url = factory(sep)
        probe_text = _aiohttp_invalid_url_text(url)
        assert url in probe_text, (
            f"前提失效：aiohttp 文本不再带裸 URL（{probe_text!r}）"
        )

    @pytest.mark.parametrize("shape,sep,factory", _r4_params())
    def test_six_surfaces_end_to_end(self, shape, sep, factory):
        """真实 ``HTTP.arun``（**无策略约束**）+ 真实 ``_handle_node_error`` 两路。"""
        url = factory(sep)
        node = HTTP(id="http_sep", name="http_sep", url=url, method="GET")
        with pytest.raises(NodeException) as excinfo:
            asyncio.run(node.arun({}))
        surfaces = _six_surfaces(node, excinfo.value)
        assert sorted(surfaces) == [
            "CONTINUE records", "abort chain", "abort records",
            "repr(NodeException)", "str(NodeException)", "traceback.format_exception",
        ]
        for label, text in surfaces.items():
            _assert_clean(text, f"{shape}/{sep!r} {label}")


class TestPolicySetPathCannotShowTheLeak:
    """复现前提的反面：**设了策略**时 URL 解析就先炸，泄露面被掩盖。

    r5 更正口径：失败**不在** ``_check_policy``——它连走都没走到。实测
    code=1001 ``Failed to parse: Host '[web hook]:8443' contains invalid character ' '``。
    """

    def test_policy_rejection_happens_before_aiohttp(self):
        url = _r4_bracketed(" ")
        node = HTTP(
            id="http_policy_first", name="http_policy_first", url=url, method="GET",
            deniedHosts=["10.0.0.0/8"],
        )
        with pytest.raises(NodeException) as excinfo:
            node.execute({})
        text = str(excinfo.value)
        # 前提：这条路径的文案里根本没有请求 URL（连凭据都不在），所以「needle 没了」
        # 在这条路径上**不能**作为修复的证据——这正是 r3 假绿的机制。
        assert SECRET not in text
        assert "10.0.0.0/8" not in text  # 策略条目都没走到（URL 解析先失败）
        assert "Failed to parse" in text, text
        assert excinfo.value.code == 1001, excinfo.value.code
        assert "invalid character" in text, text


# --------------------------------------------------------------------------- #
# ⑩ r4：边界只认 ASCII 空白（Unicode 空格不再截断 token）
# --------------------------------------------------------------------------- #
_ASCII_WHITESPACE = [
    ("space", " "), ("tab", "\t"), ("cr", "\r"), ("lf", "\n"), ("ff", "\f"), ("vt", "\v"),
]
_UNICODE_WHITESPACE = [
    ("NBSP", "\u00a0"), ("NARROW-NBSP", "\u202f"), ("THIN", "\u2009"), ("HAIR", "\u200a"),
    ("IDEOGRAPHIC", "\u3000"), ("EM-SPACE", "\u2003"), ("EN-QUAD", "\u2000"),
    ("OGHAM", "\u1680"), ("NEL", "\u0085"), ("LS", "\u2028"), ("PS", "\u2029"),
]


class TestAsciiOnlyTokenBoundary:
    def test_boundary_class_is_ascii_only(self):
        """结构钉子：边界类里不许再出现 ``\\s``（``str`` 下它是 Unicode 感知的）。"""
        pattern = http_mod._ABSOLUTE_URL_RE.pattern
        assert "\\s" not in pattern, pattern
        assert "[^ \\t\\r\\n\\f\\v]" in pattern, pattern

    @pytest.mark.parametrize("name,space", _UNICODE_WHITESPACE, ids=lambda v: v if isinstance(v, str) else "")
    def test_unicode_spaces_do_not_end_the_token(self, name, space):
        url = f"http://h/x{space}?t={SECRET}"
        rendered = _scrubber()(url)
        _assert_clean(rendered, f"unicode-space[{name}]")
        assert rendered == "http://h/***?***", (
            f"{name}（U+{ord(space):04X}）截断了 token，尾巴原样留下：{rendered!r}"
        )

    @pytest.mark.parametrize("name,space", _UNICODE_WHITESPACE, ids=lambda v: v if isinstance(v, str) else "")
    def test_unicode_space_tail_is_not_verbatim(self, name, space):
        url = f"http://h/x{space}?t={SECRET}"
        assert _scrubber()(url) != url

    @pytest.mark.parametrize("name,space", _ASCII_WHITESPACE, ids=lambda v: v if isinstance(v, str) else "")
    def test_ascii_whitespace_is_the_boundary(self, name, space):
        """ASCII 空白**仍然**是边界（刻意）——凭据由「凭据键=值」规则兜住。"""
        url = f"http://h/x{space}?access_token={SECRET}"
        rendered = _scrubber()(url)
        _assert_clean(rendered, f"ascii-ws[{name}]")
        assert rendered == f"http://h/***{space}?access_token=***", rendered

    @pytest.mark.parametrize("name,space", _ASCII_WHITESPACE, ids=lambda v: v if isinstance(v, str) else "")
    def test_ascii_whitespace_tail_with_non_credential_key_is_out_of_criterion(self, name, space):
        """诚实边界（钉住）：空白截断后的尾巴若用**非凭据键**（``t=``），仍会留下。

        ASCII 空白是刻意保留的边界（URL 在那里本来就不再是一个 URL），尾巴里的
        ``t=K`` 既不是 URL 形状、键名也不像凭据 ⇒ 判据之外，**原样返回**。这是
        设计选择：把任意 ``key=value`` 都掩掉会连 ``code=1002`` 一起删。
        真实 webhook 形态用的是 ``access_token`` / ``key`` / ``token``（见上一条）。
        """
        url = f"http://h/x{space}?t={SECRET}"
        assert _scrubber()(url) == f"http://h/***{space}?t={SECRET}"


# --------------------------------------------------------------------------- #
# ⑪ r4：绝不恒等替换（恒等 ⇒ 常量），但已掩码形态必须是不动点
# --------------------------------------------------------------------------- #
def _render_guard():
    fn = getattr(http_mod, "_render_is_usable", None)
    assert fn is not None, (
        "缺少 plaita/node/http.py::_render_is_usable——「绝不恒等替换」总守卫"
    )
    return fn


class TestNoIdentityReplacement:
    def test_truncated_bracket_token_fails_closed(self):
        """r3 定验的直接成因：``http://[web`` 曾被原样返回（恒等替换）。"""
        assert _scrubber()("http://[web") == UNPRINTABLE_URL
        assert _scrubber()("boom url='http://[web") == f"boom url='{UNPRINTABLE_URL}"

    def test_payload_less_endpoint_fails_closed(self):
        """r3 反例 A（``[`` 未闭合）仍必须退化常量；**无凭据的裸端点**不再退化（r5）。

        FINDING 3：r4 把 ``http://h`` 也判成「渲染成自身却什么也没掩」⇒ 常量，
        无凭据 URL 连端点都丢了。端点不携带凭据（没有任何 path/query 可泄）时保留
        才是正确渲染——但**未闭合方括号**永远例外（凭据可能落在被截断的那段里）。
        """
        assert _scrubber()("http://[web") == UNPRINTABLE_URL
        assert _scrubber()("boom url='http://[web") == f"boom url='{UNPRINTABLE_URL}"
        assert _scrubber()("http://h") == "http://h"

    def test_guard_rejects_identity_and_plaintext_tail(self):
        usable = _render_guard()
        # 恒等 + **非**规范形态 ⇒ 不可用（r3 反例 A：`[` 未闭合，凭据可能被截在里面）
        assert usable("http://[web", "http://[web") is False
        # 恒等 + **规范**形态（无 payload 的裸端点）⇒ 可用（r5 FINDING 3）
        assert usable("http://h", "http://h") is True
        assert usable("http://host:8080", "http://host:8080") is True
        assert usable("http://h/a?t=K", "http://h/a?t=K") is False
        # 渲染结果里仍留着明文 payload 片段 ⇒ 不可用（B 类尾部泄漏）
        assert usable("http://h/a?t=K", "http://h/***?t=K") is False
        # 正常掩码 / 幂等不动点 ⇒ 可用
        assert usable("http://h/a?t=K", "http://h/***?***") is True
        assert usable("http://h/***/***?***", "http://h/***/***?***") is True
        assert usable("http://[web,hook]:8443", "http://[web,hook]:8443") is True
        assert usable("http://h", UNPRINTABLE_URL) is False
        assert usable("http://h", "") is False
        # r5 第 5 条判据：userinfo 是 #88 的口令形态，渲染结果不许再带 `@`
        assert usable("http://u:p@h", "http://u:p@h") is False
        assert usable("http://u:p@h", "http://h") is True

    def test_guard_rejects_residual_original_payload(self):
        """第 4 条判据单独钉住：原始 path/query 明文留在输出里 ⇒ 不可用。"""
        leaves = getattr(http_mod, "_leaves_plaintext_payload", None)
        assert leaves is not None, (
            "缺少 plaita/node/http.py::_leaves_plaintext_payload"
            "（原始 path/query 残留判据）"
        )
        assert leaves("http://h/a?t=K", "http://h/***?t=K") is True
        assert leaves("http://h/a?t=K", "http://h/***?***") is False
        assert leaves("http://[web,hook]:8443/x?t=K", "http://[web,hook]:8443") is False
        assert leaves("http://h/robot/robot", "http://h/***/***") is False

    @pytest.mark.parametrize("shape,sep,factory", _r4_params())
    def test_recognised_shape_is_never_returned_verbatim(self, shape, sep, factory):
        url = factory(sep)
        assert _scrubber()(url) != url

    def test_already_masked_forms_are_fixed_points(self):
        """恒等守卫不许把**已经掩码过**的串再降级成常量（幂等性）。"""
        for text in (
            "http://h/***/***?***",
            "http://[::1]:999999/***/***?***",
            "http://[web,hook]:8443",
            "http://[webhook.internal]:8443",
            UNPRINTABLE_URL,
            "Max retries exceeded with url: /***/***?***",
        ):
            once = _scrubber()(text)
            assert _scrubber()(once) == once, f"非幂等：{text!r} -> {once!r}"


# --------------------------------------------------------------------------- #
# ⑫ r4：裸的「凭据键=值」运行（无 scheme、无 url: 标签）也掩
# --------------------------------------------------------------------------- #
_CREDENTIAL_KEYS = (
    "access_token", "token", "secret", "password", "passwd", "pwd", "key", "sig",
    "auth", "signature", "x-api-key", "client_secret", "apiKey", "accessKey",
    "session", "cookie", "bearer", "credential", "private_key",
)
_NON_CREDENTIAL_RUNS = (
    "code=1002", "port=8080", "block_private_networks=true", "v=1", "a=1,2",
    "status=500", "delivery=1", "host=example.com", "monkey=1", "sortkey=2",
    "retries=3",
)


class TestBareCredentialRunIsMasked:
    """r3 的残口：``token=K…`` / 100k 前缀 + ``?access_token=K…`` 曾被原样返回。"""

    def test_bare_token_run_is_masked(self):
        assert _scrubber()(f"token={SECRET}") == "token=***"

    def test_huge_prefix_with_query_token_is_masked(self):
        rendered = _scrubber()("x" * 100000 + f"?access_token={SECRET}")
        _assert_clean(rendered, "100k+access_token")
        assert rendered.endswith("?access_token=***")
        assert rendered.startswith("x" * 100000)

    def test_huge_input_is_scrubbed_in_linear_time(self):
        """性能钉子：``_ABSOLUTE_URL_RE`` 的 scheme 体必须**有界**。

        不加界时 ``[A-Za-z0-9+.\\-]*`` 在「100k 字母数字串」上退化成 O(n²) 回溯
        （r4 实测 5318 ms；加界后 4.6 ms）。这不是洁癖：定验的敌对批次里就有
        「100k 串 + ``?access_token=``」，掩码是日志/停机路径上的同步调用。
        """
        import time

        for text in ("x" * 100000 + f"?access_token={SECRET}", "x" * 100000 + "://"):
            started = time.monotonic()
            _scrubber()(text)
            elapsed = time.monotonic() - started
            assert elapsed < 3.0, f"100k 输入耗时 {elapsed:.2f}s（scheme 界丢了？）"

    def test_prose_credential_kv_is_masked(self):
        rendered = _scrubber()(f"login failed: password={SECRET} for user bob")
        _assert_clean(rendered, "prose password=")
        assert rendered == "login failed: password=*** for user bob"

    @pytest.mark.parametrize("key", _CREDENTIAL_KEYS)
    def test_credential_keys_are_masked(self, key):
        assert _scrubber()(f"{key}={SECRET}") == f"{key}=***"

    @pytest.mark.parametrize("text", _NON_CREDENTIAL_RUNS)
    def test_non_credential_key_value_runs_pass_through(self, text):
        """诚实边界（钉住）：键名不像凭据的裸 ``key=value`` **刻意**原样返回。

        它们是诊断文本的主体（``code=1002`` / ``port=8080``）。掩掉等于把「为什么
        失败」也一起删了——这是设计选择，不是遗漏。
        """
        assert _scrubber()(text) == text


# --------------------------------------------------------------------------- #
# ⑬ r4：非 http(s) 方括号 URL 的常量退化族（r3 文档在此处说错，r4 更正）
# --------------------------------------------------------------------------- #
class TestNonHttpBracketSchemesDegradeToConstant:
    """r3 文档称「``redis://`` / ``ftp://`` 与 r2 逐字一致」——**假的**。

    r2 的字符类把 ``[``/``]`` 排除在外，这些串根本不匹配 ⇒ ``re.sub`` 一动不动 ⇒
    **原文逐字穿过**。r4 收进方括号后它们走到端点兜底，被 scheme 判据拦下，退化为
    常量。这里的断言同时钉住新行为与「不再逐字穿过」。
    """

    @pytest.mark.parametrize("url", [
        "redis://[::1]:6379/0",
        "redis://[::1]:6379/0?token=" + SECRET,
        "ftp://[::1]:21/x",
        "ws://[::1]:9/x",
        "wss://[::1]:9/x",
        "redis://user:pw@[::1]:6379/0",
        "amqp://[::1]:5672/vhost",
    ])
    def test_degrade_to_constant_and_never_verbatim(self, url):
        rendered = _scrubber()(url)
        assert rendered == UNPRINTABLE_URL, rendered
        assert rendered != url

    def test_http_bracket_endpoint_diagnostic_survives(self):
        assert _scrubber()("http://[::1]:1/x") == "http://[::1]:1/***"
        assert _scrubber()("http://[webhook.internal]:8443") == "http://[webhook.internal]:8443"


# --------------------------------------------------------------------------- #
# ⑭ r4：敌对批次（36 条，r5 更正：r4 写的是「33 条」）的六条 totality 性质
# --------------------------------------------------------------------------- #
_R4_HOSTILE_TEXTS = [
    ("none", None),
    ("empty", ""),
    ("bytes-non-utf8", b"\xff\xfe"),
    ("int", 123),
    ("object", object()),
    ("dict-with-url", {"url": _URL_WITH_SECRET}),
    ("list-with-url", [_URL_WITH_SECRET, _URL_WITH_SECRET]),
    ("lone-high-surrogate", "\ud800"),
    ("lone-low-surrogate", "\udcff"),
    ("surrogate-in-url-path", f"http://h/ro\ud800bot/send?access_token={SECRET}"),
    ("surrogate-in-bracket-url", f"http://[web\ud800hook]:8443/x?access_token={SECRET}"),
    ("bare-token-run", f"token={SECRET}"),
    ("bare-access-token-run", f"?access_token={SECRET}"),
    ("huge-prefix-token", "x" * 100000 + f"?access_token={SECRET}"),
    ("nbsp-boundary", f"http://h/x\u00a0?t={SECRET}"),
    ("ideographic-boundary", f"http://h/x\u3000?t={SECRET}"),
    ("bracket-space", f"http://[web hook]:8443/robot/send?access_token={SECRET}"),
    ("bracket-truncated", "http://[web"),
    ("bracket-unterminated-with-secret", f"http://[web/robot/send?access_token={SECRET}"),
    ("nonhttp-bracket", f"redis://[::1]:6379/0?token={SECRET}"),
    ("csv-query", f"https://oapi.dingtalk.com/robot/send?ids=1,2&access_token={SECRET}"),
    ("semicolon-path", f"http://[::1]:999999/robot;v=1/send?access_token={SECRET}"),
    ("paren-query", f"http://[::FFFF:127.0.0.1]:999999/robot/send?x=(1)&access_token={SECRET}"),
    ("requests-text", f"Max retries exceeded with url: /robot/send?access_token={SECRET}"),
    ("aiohttp-text", f"InvalidUrlClientError: http://[web,hook]:8443/robot/send?access_token={SECRET}"),
    ("json-ish", f'{{"hook": "https://hooks.slack.com/services/T00/B00/{SECRET}"}}'),
    ("already-masked", "http://h/***/***?***"),
    ("endpoint-only", "http://[webhook.internal]:8443"),
    ("constant", UNPRINTABLE_URL),
    ("prose", "nothing to mask here"),
    ("credential-prose", f"login failed: password={SECRET} for user bob"),
    ("quoted-url", f"url='http://h/x?access_token={SECRET}'"),
    ("multi-url", f"a http://h1/x?t=token={SECRET} b http://h2/y?access_token={SECRET} c"),
    ("control-chars", f"http://h/x\x01\x02?t={SECRET}"),
    ("percent-encoded", f"http://h/a%20b?access_token%3D{SECRET}"),
    ("versionish", f"https://h/v1/x?api_key={SECRET}&v=2"),
]


class TestR4HostileBatchSixProperties:
    """r4：36 条敌对输入 × 六条性质（不抛 / str / 可编码 / 幂等 / 无 needle / 非恒等）。

    条数**不再写死**：`test_batch_counts_match_their_docstrings` 把这里的数字与
    ``len(_R4_HOSTILE_TEXTS)`` 对表（r4 的 docstring 写「33 条」而实际 36 条）。
    """

    def test_batch_counts_match_their_docstrings(self):
        """文档里的条数必须等于列表长度——r5 更正 r4 的「33 条」（实际 36）。"""
        assert len(_R4_HOSTILE_TEXTS) == 36, len(_R4_HOSTILE_TEXTS)
        assert len(_HOSTILE_TEXTS) == 21, len(_HOSTILE_TEXTS)
        assert "36 条" in TestR4HostileBatchSixProperties.__doc__
        assert "21 " not in TestR4HostileBatchSixProperties.__doc__

    def test_never_raises_and_is_str_and_encodable(self):
        scrub = _scrubber()
        for label, value in _R4_HOSTILE_TEXTS:
            rendered = scrub(value)
            assert isinstance(rendered, str), label
            try:
                rendered.encode("utf-8")
            except UnicodeEncodeError as exc:  # pragma: no cover - 修复前在此响
                raise AssertionError(
                    f"{label}: 写不进 utf-8 日志（{exc}）：{rendered!r}"
                ) from exc

    def test_is_idempotent(self):
        scrub = _scrubber()
        for label, value in _R4_HOSTILE_TEXTS:
            once = scrub(value)
            assert scrub(once) == once, f"{label}: 非幂等（{once!r} → {scrub(once)!r}）"

    def test_never_carries_a_needle(self):
        scrub = _scrubber()
        for label, value in _R4_HOSTILE_TEXTS:
            _assert_clean(scrub(value), f"hostile[{label}]")

    def test_never_returns_a_credential_bearing_input_verbatim(self):
        scrub = _scrubber()
        for label, value in _R4_HOSTILE_TEXTS:
            if isinstance(value, str) and SECRET in value:
                assert scrub(value) != value, f"{label}: 带凭据的输入被原样返回"

    def test_recognised_shape_is_never_a_verbatim_pass_through(self):
        """认出形状 ⇒ 必须改；唯一例外是**已经是**规范掩码/端点形态的串（不动点）。"""
        for label, value in _R4_HOSTILE_TEXTS:
            if not isinstance(value, str):
                continue
            recognised = bool(
                http_mod._ABSOLUTE_URL_RE.search(value)
                or _credential_spans()(value)
            )
            if recognised and not http_mod._token_is_canonical(value):
                assert _scrubber()(value) != value, f"{label}: 认出了形状却一字未改"


# --------------------------------------------------------------------------- #
# ⑮ r4：前置探针 —— 无策略约束这条链**真的**会把裸 URL 交给 aiohttp
# --------------------------------------------------------------------------- #
class TestNoPolicyPreconditionIsReal:
    def test_default_node_has_no_policy_constraints(self):
        node = HTTP(id="http_defaults", name="http_defaults", url=_r4_plain(","), method="GET")
        assert node.allowed_hosts in (None, [])
        assert node.denied_hosts in (None, [])
        assert node.block_private_networks is False

    def test_raw_url_reaches_aiohttp_text(self):
        url = _r4_plain(",")
        assert url in _aiohttp_invalid_url_text(url)

    def test_with_policy_the_raw_url_never_reaches_aiohttp(self):
        """对照：设了策略 ⇒ **URL 解析**提前抛（code=1001 ``Failed to parse``）⇒ 泄露面被掩盖。

        r5 更正：失败点在 URL 解析，**不是** ``_check_policy``。
        """
        node = HTTP(
            id="http_policy_masks_leak", name="http_policy_masks_leak",
            url=_r4_plain(","), method="GET", blockPrivateNetworks=True,
        )
        with pytest.raises(NodeException) as excinfo:
            node.execute({})
        assert SECRET not in str(excinfo.value)


# --------------------------------------------------------------------------- #
# ⑯ r5（FINDING 1）：凭据键文法 —— 引号在**键**这一侧 + 驼峰/粘连键名
# --------------------------------------------------------------------------- #
def _credential_spans():
    """r5 的凭据运行扫描器；在 r4（``4d832348``）上不存在 ⇒ 相关断言必须判红。"""
    fn = getattr(http_mod, "_credential_run_spans", None)
    assert fn is not None, (
        "缺少 plaita/node/http.py::_credential_run_spans——凭据键=值的线性扫描器"
    )
    return fn


def _is_credential_key():
    fn = getattr(http_mod, "_is_credential_key", None)
    assert fn is not None, "缺少 plaita/node/http.py::_is_credential_key"
    return fn


#: r5 判据：**键名分量**（非字母数字边界 + 驼峰转折切分，全小写，允许复数）相等
#: 于词干集合之一。正例——每一条都在 r4 上实测 ``keymatch=None``（原文穿过）。
_R5_CREDENTIAL_KEYS = (
    # (a) 引号在**键**这一侧（r4 只允许引号出现在分隔符**之后**）
    '"access_token"', "'access_token'", '"token"', "'password'",
    '"Authorization"', '"apiKey"',
    # (b) 驼峰 / Pascal / 粘连
    "accessToken", "sessionId", "secretKey", "authToken", "refreshToken",
    "idToken", "apiSecret", "bearerToken", "xAccessToken", "APIKey",
    "apiKey", "Authorization", "credentials", "accesstoken", "authtoken",
    "secretkey", "sessionid", "xapikey",
    # 基线（r4 已覆盖，防回归）
    "access_token", "token", "x-api-key", "x_api_key", "X-Api-Key", "secret",
    "password", "passwd", "pwd", "sig", "signature", "key", "apikey",
    "accesskey", "private_key", "session_id", "client_secret", "auth",
    "x-auth-token", "cookie", "bearer", "credential", "clientsecret",
)
#: 负例——**必须**原样穿过（诊断文本的主体；``monkey`` / ``sortkey`` 是 r4 就钉住的
#: 声明的残口：字母串里恰好含 ``key``）。
_R5_NON_CREDENTIAL_KEYS = (
    "code", "port", "v", "monkey", "sortkey", "block_private_networks",
    "status", "delivery", "host", "retries", "a", "timeout", "url", "id",
    "tokenizer", "secretary", "passwordless", "keynote", "hockey", "author",
    "apiname", "sortkeys", "keys", "monkeys", "tokens", "authentic",
)
#: (label, 原文模板, **精确**渲染)；``{s}`` 处填 :data:`SECRET`。
_R5_CREDENTIAL_MATRIX = (
    ("json-double-quote", '{"access_token": "{s}"}', '{"access_token": "***"}'),
    ("json-tight", '{"token":"{s}"}', '{"token":"***"}'),
    ("json-single-quote", "{'access_token': '{s}'}", "{'access_token': '***'}"),
    ("json-authorization", '{"Authorization":"Bearer {s}"}', '{"Authorization":"***"}'),
    ("json-quoted-value-with-space", '{"password": "{s} tail"}', '{"password": "***"}'),
    ("quoted-key-eq", 'failed "password"={s}', 'failed "password"=***'),
    ("quoted-key-colon", '"token": {s}', '"token": ***'),
    ("camel-accessToken", "accessToken={s}", "accessToken=***"),
    ("camel-sessionId", "sessionId={s}", "sessionId=***"),
    ("camel-secretKey", "secretKey={s}", "secretKey=***"),
    ("camel-authToken", "authToken={s}", "authToken=***"),
    ("camel-refreshToken", "refreshToken={s}", "refreshToken=***"),
    ("camel-idToken", "idToken={s}", "idToken=***"),
    ("camel-apiSecret", "apiSecret={s}", "apiSecret=***"),
    ("camel-bearerToken", "bearerToken={s}", "bearerToken=***"),
    ("camel-xAccessToken", "xAccessToken={s}", "xAccessToken=***"),
    ("pascal-Authorization", "Authorization={s}", "Authorization=***"),
    ("plural-credentials", "credentials={s}", "credentials=***"),
    ("glued-accesstoken", "accesstoken={s}", "accesstoken=***"),
    ("header-bearer", "Authorization: Bearer {s}", "Authorization: ***"),
    ("url-plus-kv-mixed",
     'boom: {"url": "http://h/x", "access_token": "{s}"}',
     'boom: {"url": "http://h/*** "access_token": "***"}'),
)
#: 键名不像凭据的裸 ``key=value``：**逐字**原样返回（r4 钉住的设计选择，r5 保持）。
_R5_NON_CREDENTIAL_RUNS = (
    "code=1002", "port=8080", "v=1", "block_private_networks=true",
    "monkey=1", "sortkey=2", "status=500", "delivery=1", "host=example.com",
    "retries=3", "a=1,2", "timeout=30", "tokenizer=1", "secretary=1",
    "passwordless=true", "keynote=1", "hockey=2", "apiname=x",
)


class _EntryRsp:
    """非空 + ``send_request_fail`` ⇒ 走 ``HTTP_DO_REQUEST_ERROR`` 那条入口。"""

    raw_request = "REQ"
    raw_response = None

    def empty(self):
        return False

    def send_request_fail(self):
        return True


def _entry_exception(text: str, path: str):
    """**真实入口** ``HTTP.handle_http_node_err(RuntimeError(text), rsp)`` 的两个路径。"""
    node = HTTP(id="http_kv_entry", name="http_kv_entry", url="http://h/x", method="GET")
    rsp = None if path == "gen" else _EntryRsp()
    return node, node.handle_http_node_err(RuntimeError(text), rsp)


class TestCredentialKeyCriterionR5:
    """r5 的判据本体：**分量级**词干相等（含驼峰转折与复数），正反两个方向都钉住。"""

    @pytest.mark.parametrize("key", _R5_CREDENTIAL_KEYS)
    def test_credential_key_forms_are_recognised(self, key):
        assert _is_credential_key()(key) is True, key

    @pytest.mark.parametrize("key", _R5_NON_CREDENTIAL_KEYS)
    def test_non_credential_key_forms_stay_benign(self, key):
        """分量**相等**而不是子串包含：``monkey`` / ``sortkey`` / ``tokenizer`` 不命中。"""
        assert _is_credential_key()(key) is False, key

    @pytest.mark.parametrize("key", _R5_CREDENTIAL_KEYS)
    def test_credential_key_forms_are_masked(self, key):
        assert _scrubber()(f"{key}={SECRET}") == f"{key}=***"

    @pytest.mark.parametrize("text", _R5_NON_CREDENTIAL_RUNS)
    def test_non_credential_runs_pass_through(self, text):
        assert _scrubber()(text) == text


class TestCredentialKeyMatrixR5:
    """r5 键矩阵：精确渲染 + 六个面 × 两条真实入口路径。**

    入口路径两条都走 ``HTTP.handle_http_node_err``：``rsp=None``（``1001``）与
    ``rsp`` 非空且 ``send_request_fail()``（``1002``）——同一个 ``wrap_http_node_err``，
    两个 code。面是 :func:`_six_surfaces` 的六条（str / repr / traceback /
    CONTINUE 记录 / abort ``__cause__`` 链 / abort 记录）。
    """

    @pytest.mark.parametrize(
        "label,template,expected",
        _R5_CREDENTIAL_MATRIX,
        ids=[case[0] for case in _R5_CREDENTIAL_MATRIX],
    )
    def test_renderer_exact_string(self, label, template, expected):
        text = template.replace("{s}", SECRET)
        rendered = _scrubber()(text)
        _assert_clean(rendered, f"renderer[{label}]")
        assert rendered == expected.replace("{s}", SECRET), (
            f"{label}: 实际={rendered!r} 期望={expected.replace('{s}', SECRET)!r}"
        )

    @pytest.mark.parametrize(
        "label,template,expected",
        _R5_CREDENTIAL_MATRIX,
        ids=[case[0] for case in _R5_CREDENTIAL_MATRIX],
    )
    @pytest.mark.parametrize("path", ("gen", "do"))
    def test_six_surfaces_through_the_real_entry_point(self, label, template, expected, path):
        text = template.replace("{s}", SECRET)
        node, exc = _entry_exception(text, path)
        surfaces = _six_surfaces(node, exc)
        assert sorted(surfaces) == [
            "CONTINUE records", "abort chain", "abort records",
            "repr(NodeException)", "str(NodeException)", "traceback.format_exception",
        ]
        for name, rendered in surfaces.items():
            _assert_clean(rendered, f"{label}[{path}] {name}")

    @pytest.mark.parametrize("text", _R5_NON_CREDENTIAL_RUNS)
    @pytest.mark.parametrize("path", ("gen", "do"))
    def test_benign_diagnostics_survive_the_real_entry_point(self, text, path):
        """反方向：诊断文本必须**逐字**活下来（不是「没凭据就行」）。"""
        node, exc = _entry_exception(text, path)
        assert text in str(exc), f"{text!r} 被吃掉了：{str(exc)!r}"
        assert "***" not in str(exc), str(exc)

    def test_spans_level_evidence(self):
        """``keymatch`` 级的证据：每个正例都必须扫出跨度，每个反例必须一个都没有。"""
        spans = _credential_spans()
        for key in _R5_CREDENTIAL_KEYS:
            assert spans(f"{key}={SECRET}"), f"{key}: 没扫出跨度（keymatch=None）"
        for text in _R5_NON_CREDENTIAL_RUNS:
            assert spans(text) == [], f"{text}: 不该有跨度"


class TestCredentialRunScalingR5:
    """r5（FINDING 2）：凭据键那一趟必须**线性**，敌对形状不得灾难性回溯。

    r4 的 ``_CREDENTIAL_RUN_RE`` 有嵌套量词、紧接一个必须出现的 ``[=:]``：
    ``"token_"*n`` 这种「全是分量边界、就是没有分隔符」的串上回溯爆炸。
    实测（同一台机器，``4d832348``）：n=1000 → 79.8 ms、2000 → 311.8 ms、
    4000 → 1254.1 ms、8000 → 5251.4 ms、30000 → 73080.1 ms（二次）；
    r5 扫描器：0.35 / 0.87 / 1.00 / 1.99 / 7.89 ms（线性）。归因也实测过：
    24 KB 输入上 ``_CREDENTIAL_RUN_RE.search`` 单独 1343 ms，两趟 ``sub`` ≤0.5 ms。

    判据两条一起用（只比值会被小 n 的计时噪声骗，只比绝对值会被机器负载骗）：
    * **硬上限** 2.0 s/次（r4 在 n=8000 就已经 5.25 s ⇒ 直接判红）；
    * n → 8n 的增长比 ≤ 20×（线性 ≈8×；下限 0.20 s 吸收常数项与噪声）。
    """

    _SHAPES = (
        ("underscore-glued", lambda n: "token_" * n),
        ("dot-glued", lambda n: "token." * n),
        ("kv-repeated", lambda n: "access_token=" * n),
        ("quoted-colon-repeated", lambda n: '"token":' * n),
        # r6 自加的形状：两处窗口（键回看、引号值 find）去掉后必须仍然线性。
        ("eq-run", lambda n: "=" * n),
        ("colon-run", lambda n: ":" * n),
        ("quote-run", lambda n: '"' * n),
        ("unclosed-quote-megabytes", lambda n: 'token="' + "p" * n),
        ("unclosed-quote-multi", lambda n: ('token="' + "p" * 200 + "=") * (n // 210)),
        ("mixed-quote-eq", lambda n: '"=' * (n // 2)),
        ("suffix-key", lambda n: "access_token_" + "a" * n + "=" + SECRET),
        ("escaped-json", lambda n: ('{\\"token\\":\\"' + "p" * 200 + '\\"}') * (n // 215)),
    )
    _SIZES = (1000, 2000, 4000, 8000)

    @pytest.mark.parametrize("label,factory", _SHAPES, ids=[s[0] for s in _SHAPES])
    def test_adversarial_shapes_grow_at_most_linearly(self, label, factory):
        import time

        scrub = _scrubber()
        times = {}
        for n in self._SIZES:
            text = factory(n)
            started = time.monotonic()
            scrub(text)
            times[n] = time.monotonic() - started
            assert times[n] < 2.0, (
                f"{label} n={n}（{len(text)} 字符）耗时 {times[n]:.3f}s —— "
                "凭据键那一趟退化成回溯了？"
            )
        bound = max(20.0 * times[self._SIZES[0]], 0.20)
        assert times[self._SIZES[-1]] < bound, (
            f"{label}: n={self._SIZES[0]} → {times[self._SIZES[0]]:.4f}s，"
            f"n={self._SIZES[-1]} → {times[self._SIZES[-1]]:.4f}s（上限 {bound:.3f}s）"
        )

    def test_honest_paths_are_still_masked(self):
        """性能修法不许把掩码修掉：四条诚实路径仍逐字掩值。"""
        cases = (
            (f"token={SECRET}", "token=***"),
            (f'{{"access_token": "{SECRET}"}}', '{"access_token": "***"}'),
            (f"Authorization: Bearer {SECRET}", "Authorization: ***"),
            (f"x-api-key={SECRET}", "x-api-key=***"),
            (f"login failed: password={SECRET} for user bob",
             "login failed: password=*** for user bob"),
        )
        for text, expected in cases:
            rendered = _scrubber()(text)
            _assert_clean(rendered, f"honest[{text!r}]")
            assert rendered == expected, (text, rendered, expected)

    def test_glued_words_without_a_separator_are_left_alone(self):
        """``"token_"*n`` 里没有 ``=``/``:`` ⇒ 不是 key=value，逐字返回（r3~r5 同）。"""
        text = "token_" * 500
        assert _scrubber()(text) == text


# --------------------------------------------------------------------------- #
# ⑰ r5（FINDING 3）：无凭据的 URL 保留端点，不再退化成常量
# --------------------------------------------------------------------------- #
#: (label, 原文, **精确**渲染)。r4（``4d832348``）在这些输入上一律
#: ``<unprintable-url>``——渲染器的职责是掩**凭据**，不是擦掉 URL。
_FREE_URL_CASES = (
    ("bare-host", "http://example.com", "http://example.com"),
    ("host-port", "http://host:8080", "http://host:8080"),
    ("https-bare", "https://api.example.com", "https://api.example.com"),
    ("failed-to-connect", "Failed to connect to http://example.com",
     "Failed to connect to http://example.com"),
    ("failed-to-connect-with-path", "Failed to connect to http://example.com/path?q=1",
     "Failed to connect to http://example.com/***?***"),
    ("with-path", "http://example.com/path", "http://example.com/***"),
    ("with-query", "http://example.com/p?q=1", "http://example.com/***?***"),
    ("userinfo-dropped", "http://user:pw@example.com", "http://example.com"),
    ("slack-shaped", "https://hooks.slack.com/services/T0/B0", "https://hooks.slack.com/***/***/***"),
)
#: **仍然**退化为常量的族（逐条给出理由并钉住）。
_STILL_CONSTANT = (
    ("unterminated-bracket", "http://[web"),          # r3 反例 A：凭据可能在被截断的方括号里
    ("non-http-scheme", "redis://h"),                 # 冻结的 url_redaction._SAFE_SCHEMES
    ("empty-authority", "http:///path"),              # 没有 host ⇒ 无端点可救
)


class TestCredentialFreeUrlsKeepTheirEndpoint:
    @pytest.mark.parametrize(
        "label,text,expected", _FREE_URL_CASES, ids=[c[0] for c in _FREE_URL_CASES]
    )
    def test_endpoint_survives(self, label, text, expected):
        rendered = _scrubber()(text)
        _assert_clean(rendered, f"free-url[{label}]")
        assert rendered == expected, f"{label}: 实际={rendered!r} 期望={expected!r}"

    @pytest.mark.parametrize(
        "label,text", _STILL_CONSTANT, ids=[c[0] for c in _STILL_CONSTANT]
    )
    def test_still_degrade_to_the_constant(self, label, text):
        """必须**仍然**退化常量：逐条给理由（不是笼统地 fail-closed）。"""
        assert _scrubber()(text) == UNPRINTABLE_URL, label

    def test_endpoints_are_fixed_points(self):
        for label, text, _expected in _FREE_URL_CASES:
            once = _scrubber()(text)
            assert _scrubber()(once) == once, f"{label}: 非幂等（{once!r}）"

    def test_no_credential_bearing_shape_regressed(self):
        """反方向：掩码不许因为「保留端点」而漏。逐条对着 needle 断言。"""
        cases = (
            f"http://user:{SECRET}@example.com",
            f"http://example.com?access_token={SECRET}",
            f"http://example.com/robot/send?access_token={SECRET}",
            f"http://[web,hook]:8443/x?token={SECRET}",
            f"https://hooks.slack.com/services/T00/B00/{SECRET}",
            f"http://example.com {{'access_token': '{SECRET}'}}",
        )
        for text in cases:
            rendered = _scrubber()(text)
            _assert_clean(rendered, f"regression[{text!r}]")
            assert rendered != text, text

    def test_authority_is_kept_by_contract(self):
        """诚实边界（**r4 起就存在**，不是 r5 新开的暴露面）：

        ``#88`` 冻结的 ``_mask_url_for_log`` 契约就是「抹 userinfo 口令、**保留** host」，
        ``url_redaction`` 同样只保留 ``scheme://host[:port]``。因此把凭据**只**写进
        host 位置的形态在任何带 payload 的 URL 上都早已原样保留（``4d832348`` 实测：
        ``http://Ks9Qm4Zt/robot/send?access_token=X`` → ``http://Ks9Qm4Zt/***/***?***``）。

        **r5 原文在此说错了**：r5 曾写「r5 只把**无 payload** 的那一格从常量改回端点，
        暴露面**同源同形**、没有扩大」——错。同类同形是对的，但 **r5 确实扩大了输入
        集合**：``http://Ks9Qm4Zt``（无 payload，r4 是常量）在 r5 变成原文。
        r6 把这一格里 authority 疑似承载凭据的部分收回（见
        :class:`TestCredentialShapedAuthorityR6`），只留下**有 payload** 的一格
        （本测试）作为与 ``access_token%3D…`` 并列的已声明残口。
        """
        assert _scrubber()(f"http://{SECRET}/robot/send?access_token=X") == \
            f"http://{SECRET}/***/***?***"


# --------------------------------------------------------------------------- #
# ⑱ r5（FINDING 4）：文档里的测试引用与条数**不会腐**
# --------------------------------------------------------------------------- #
_TEST_REF_RE = re.compile(r"(Test[A-Za-z0-9_]+)::(test_[A-Za-z0-9_]+)")


def _http_module_docstring_refs():
    """``plaita/node/http.py`` 全文（含 docstring）里出现的 ``TestX::test_y`` 引用。"""
    source = (PLAITA_DIR / "node" / "http.py").read_text(encoding="utf-8")
    return sorted(set(_TEST_REF_RE.findall(source)))


class TestDocstringReferencesResolve:
    """FINDING 4：``_scrub_url_text`` 的 docstring 曾指向**不存在**的类
    （``TestCredentialRunIsMasked``，真名 ``TestBareCredentialRunIsMasked``）。

    与其改一次字，不如把「引用可解析」变成检查：``plaita/`` 里任何
    ``TestX::test_y`` 引用都必须能在 ``tests/unit/test_issue159_*.py`` 里找到。
    """

    def test_every_reference_resolves(self):
        corpus = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((REPO_ROOT / "tests" / "unit").glob("test_issue159_*.py"))
        )
        refs = _http_module_docstring_refs()
        assert refs, "没有扫到任何 TestX::test_y 引用（判据失效了？）"
        missing = [
            f"{cls}::{name}" for cls, name in refs
            if f"class {cls}" not in corpus or f"def {name}" not in corpus
        ]
        assert missing == [], f"docstring 指向不存在的测试：{missing}"


# --------------------------------------------------------------------------- #
# ⑲ r6（FINDING 4）：带引号的值必须掩到**真末尾**——>4 KB 的窗口截断回归
# --------------------------------------------------------------------------- #
_R6_SIZES = (8192, 65536, 262144)


def _r6_place(secret: str, size: int, where: str) -> str:
    """把密钥放在 ``size`` 长值体的 start / middle / end。

    ``middle`` 的起点**严格 > 4096**（r5 的 ``_QUOTED_VALUE_WINDOW``），
    这样三个位置都覆盖到窗口边界两侧。
    """
    if where == "start":
        return secret + "p" * (size - len(secret))
    if where == "end":
        return "p" * (size - len(secret)) + secret
    half = min(max(size // 2, 4104), size - len(secret))
    return "p" * half + secret + "p" * (size - half - len(secret))


def _r6_quoted_large_cases():
    cases = []
    for size in _R6_SIZES:
        for where in ("start", "middle", "end"):
            body = _r6_place(SECRET, size, where)
            cases.append((f"dq-closed-{size}-{where}",
                          f'access_token="{body}"', 'access_token="***"'))
            cases.append((f"dq-unclosed-{size}-{where}",
                          f'access_token="{body}', 'access_token="***'))
    body = _r6_place(SECRET, 8192, "end")
    cases.append(("sq-closed-8k-end", f"password='{body}'", "password='***'"))
    cases.append(("sq-unclosed-8k-end", f"password='{body}", "password='***"))
    cases.append(("xapikey-closed-8k-end", f'x-api-key="{body}"', 'x-api-key="***"'))
    cases.append(("xapikey-unclosed-8k-end", f'x-api-key="{body}', 'x-api-key="***'))
    cases.append(("json-key-closed-8k-end", f'{{"access_token": "{body}"}}',
                  '{"access_token": "***"}'))
    cases.append(("query-param-closed-8k-end", f"/robot/send?access_token={body}",
                  "/robot/send?access_token=***"))
    return tuple(cases)


_R6_QUOTED_LARGE = _r6_quoted_large_cases()


class TestLargeQuotedValueR6:
    """r6 FINDING 4：``_QUOTED_VALUE_WINDOW = 4096`` 把掩码截断在窗口末尾。

    r5（``646d9ad``）实测：``access_token="<8184×p>Ks9Qm4Zt"`` → 输出是
    ``access_token="***<4096 字符明文，含密钥本体>``；**闭合引号在窗口外时同样截断**
    （``find`` 在 ``[v0, v0+4096)`` 里找不到闭引号就 ``v1 = v0 + 4096``）。
    r4（``4d83234``）同一输入是 ``access_token="***"``——**r5 相对 r4 回归**。
    可达性：多 claim 的 IdP JWT（Azure AD 等）>4 KB 是常态。

    修法：``find`` 不带 end ⇒ 扫到真末尾；线性由 ``i = v1`` 的摊还给出
    （未闭合引号一次扫到 n 即结束），无需任何窗口。
    """

    @pytest.mark.parametrize(
        "label,text,expected", _R6_QUOTED_LARGE,
        ids=[case[0] for case in _R6_QUOTED_LARGE],
    )
    def test_renderer_masks_the_whole_value(self, label, text, expected):
        rendered = _scrubber()(text)
        _assert_clean(rendered, f"r6-large[{label}]")
        assert rendered == expected, (
            f"{label}: len(in)={len(text)} len(out)={len(rendered)}\n"
            f"  实际={rendered[:80]!r}…\n  期望={expected[:80]!r}…"
        )

    @pytest.mark.parametrize(
        "label,text,expected", _R6_QUOTED_LARGE,
        ids=[case[0] for case in _R6_QUOTED_LARGE],
    )
    @pytest.mark.parametrize("path", ("gen", "do"))
    def test_six_surfaces_through_the_real_entry_point(self, label, text, expected, path):
        node, exc = _entry_exception(text, path)
        surfaces = _six_surfaces(node, exc)
        for name, rendered in surfaces.items():
            _assert_clean(rendered, f"r6-large[{label}][{path}] {name}")

    @pytest.mark.parametrize("size", _R6_SIZES)
    def test_span_reaches_the_true_end_of_the_value(self, size):
        """跨度级证据：掩码区间必须真的到值末尾，而不是窗口末尾。"""
        spans = _credential_spans()
        for where in ("start", "middle", "end"):
            body = _r6_place(SECRET, size, where)
            unclosed = f'access_token="{body}'
            got = spans(unclosed)
            assert got, f"{size}/{where}: 没扫出跨度"
            assert got[-1][1] == len(unclosed), (
                f"{size}/{where}: 跨度止于 {got[-1][1]}，值的真末尾是 {len(unclosed)}"
            )
            closed = unclosed + '"'
            got = spans(closed)
            assert got and got[-1][1] == len(closed) - 1, (size, where, got, len(closed))

    def test_unclosed_quote_is_a_fixed_point(self):
        """幂等：掩完一次之后再洗一次必须逐字不变（占位不进二次跨度）。"""
        once = _scrubber()('access_token="' + _r6_place(SECRET, 8192, "end"))
        assert _scrubber()(once) == once


# --------------------------------------------------------------------------- #
# ⑳ r6（FINDING 5）：键 token 的后缀侧——64 字符回看窗口的截断回归
# --------------------------------------------------------------------------- #
_R6_SUFFIX_SIZES = (58, 59, 64, 200, 4096)


class TestKeyTokenSuffixWindowR6:
    """r6 FINDING 5：``_MAX_KEY_WINDOW = 64`` 在**前缀**侧安全，在**后缀**侧漏。

    ``access_token_`` + ``'a'*s`` + ``=SECRET``：键 token 的前 64 字符恰好是
    ``access_token_`` + 55 个 ``a``；``s ≥ 59`` 时回看窗口整段落在 ``a`` 串里 ⇒
    键名取成纯 ``a`` 串、判据落空、原文穿过。r4（``4d832348``）在 ``s = 200``
    仍掩（正则无窗口上限）——**又是 r5 相对 r4 的回归**。

    修法：回看到 :data:`_KEY_CHARS` 的起点，不设上限；线性由「各分隔符的键回看
    区间两两不交」给出（分隔符本身不在 ``_KEY_CHARS`` 里，回看不到前一个分隔符之前）。
    """

    @pytest.mark.parametrize("size", _R6_SUFFIX_SIZES)
    def test_suffix_side_key_token_is_masked(self, size):
        text = "access_token_" + "a" * size + "=" + SECRET
        rendered = _scrubber()(text)
        _assert_clean(rendered, f"r6-suffix[{size}]")
        assert rendered == "access_token_" + "a" * size + "=***", (
            f"s={size}: 键名后缀把回看窗口撑爆 ⇒ 判据落空\n  实际={rendered!r}"
        )

    @pytest.mark.parametrize("size", (59, 4096))
    @pytest.mark.parametrize("path", ("gen", "do"))
    def test_six_surfaces_through_the_real_entry_point(self, size, path):
        text = "access_token_" + "a" * size + "=" + SECRET
        node, exc = _entry_exception(text, path)
        for name, rendered in _six_surfaces(node, exc).items():
            _assert_clean(rendered, f"r6-suffix[{size}][{path}] {name}")

    def test_prefix_side_is_unchanged(self):
        """前缀侧（词干紧贴分隔符）不受影响：超长良性前缀也照样命中。"""
        text = "x" * 5000 + " access_token=" + SECRET
        assert _scrubber()(text) == "x" * 5000 + " access_token=***"

    def test_benign_long_key_token_still_passes_through(self):
        """反方向：键 token 里没有凭据**分量**时，再长也不许掩（诊断文本主体）。"""
        for text in ("x" * 200 + "_status=500", "y" * 4096 + "_retries=3"):
            assert _scrubber()(text) == text

    def test_spans_cover_the_whole_key_token(self):
        size = 200
        key = "access_token_" + "a" * size
        got = _credential_spans()(f"{key}={SECRET}")
        assert got == [(len(key) + 1, len(key) + 1 + len(SECRET))], got


# --------------------------------------------------------------------------- #
# ㉑ r6（FINDING 6）：authority 疑似承载凭据 + 没有真掩码 ⇒ fail closed
# --------------------------------------------------------------------------- #
_R6_HOST_FAIL_CLOSED = (
    ("bare-secret-host", f"http://{SECRET}"),
    ("slash-secret-host", f"http://{SECRET}/"),
    ("double-slash-secret-host", f"http://{SECRET}//"),
    ("port-secret-host", f"http://{SECRET}:8080"),
    ("userinfo-secret-host", f"http://user:pw@{SECRET}"),
    ("fragment-only-secret-host", f"http://{SECRET}/#frag"),
    ("in-sentence", f"Failed to connect to http://{SECRET}"),
    ("quoted-in-sentence", f"boom url='http://{SECRET}'"),
)
_R6_HOST_KEPT = (
    ("bare-host", "http://example.com", "http://example.com"),
    ("host-port", "http://host:8080", "http://host:8080"),
    ("https-bare", "https://api.example.com", "https://api.example.com"),
    ("failed-to-connect", "Failed to connect to http://example.com",
     "Failed to connect to http://example.com"),
    ("single-label-lowercase", "http://h", "http://h"),
    ("single-label-trailing-slash", "http://h/", "http://h/"),
    ("localhost-port", "http://localhost:8080", "http://localhost:8080"),
    ("bracket-endpoint", "http://[web,hook]:8443", "http://[web,hook]:8443"),
    ("ipv4", "http://10.1.2.3", "http://10.1.2.3"),
)


class TestCredentialShapedAuthorityR6:
    """r6 FINDING 6：r5 恢复端点时把「host 位置就是密钥本体 + 无 payload」也放了进来。

    ``http://Ks9Qm4Zt`` 在 r4 是 ``<unprintable-url>``，r5 变成原文——**r5 新开的
    暴露面**（评审同时指出 r5 的 docstring 说「没有扩大」不准确：同类同形，
    但**输入集合扩大了**）。

    **判据（精确）**：一个 token 只有在「掩掉过字节」时才算合法不动点；
    payload 为空或只剩结构分隔符（``/``、``//``、``?``、``#``）时，authority
    必须**不像**承载凭据——host 位置出现大写 ASCII 字母即判 fail-closed。

    为什么 ``example.com`` 保留而 ``Ks9Qm4Zt`` 不：DNS 名大小写不敏感、惯例全小写，
    authority 位置出现大写字母在观察到的泄漏家族里就是驼峰 token，不是主机名；
    而 r5 已钉住的 ``http://h`` / ``http://host:8080`` / ``http://localhost:8080``
    全是小写，此判据与它们共存。

    方向（刻意）：``http://Example.com`` 这类合法但非常规的大写主机名也退化常量。
    """

    @pytest.mark.parametrize(
        "label,text", _R6_HOST_FAIL_CLOSED, ids=[c[0] for c in _R6_HOST_FAIL_CLOSED]
    )
    def test_credential_shaped_host_fails_closed(self, label, text):
        rendered = _scrubber()(text)
        _assert_clean(rendered, f"r6-host[{label}]")
        assert SECRET not in rendered, f"{label}: 密钥本体留在输出里：{rendered!r}"
        assert rendered.endswith(UNPRINTABLE_URL), f"{label}: 实际={rendered!r}"

    @pytest.mark.parametrize(
        "label,text,expected", _R6_HOST_KEPT, ids=[c[0] for c in _R6_HOST_KEPT]
    )
    def test_credential_free_endpoints_are_still_kept(self, label, text, expected):
        rendered = _scrubber()(text)
        _assert_clean(rendered, f"r6-host-free[{label}]")
        assert rendered == expected, f"{label}: 实际={rendered!r} 期望={expected!r}"

    @pytest.mark.parametrize("path", ("gen", "do"))
    def test_six_surfaces_through_the_real_entry_point(self, path):
        node, exc = _entry_exception(f"http://{SECRET}", path)
        for name, rendered in _six_surfaces(node, exc).items():
            _assert_clean(rendered, f"r6-host[{path}] {name}")

    def test_kept_endpoints_are_fixed_points(self):
        for label, text, _expected in _R6_HOST_KEPT:
            once = _scrubber()(text)
            assert _scrubber()(once) == once, f"{label}: 非幂等（{once!r}）"

    def test_frozen_contract_host_residual_is_restated(self):
        """与上面新判据**并列**的已声明残口（**r4 起冻结契约**，本单不动）：

        只要 token 有 payload（哪怕 payload 全被掩掉），host 位置的凭据就按 ``#88``
        的 ``_mask_url_for_log`` 契约保留（只抹 userinfo、保 host）。所以
        ``http://Ks9Qm4Zt/x`` / ``http://Ks9Qm4Zt/robot/send?access_token=X``
        仍然保留 host——r6 只收「**没有**真掩码」的那一格。
        """
        assert _scrubber()(f"http://{SECRET}/x") == f"http://{SECRET}/***"
        assert _scrubber()(f"http://{SECRET}/robot/send?access_token=X") == \
            f"http://{SECRET}/***/***?***"


# --------------------------------------------------------------------------- #
# ㉒ r6（FINDING 7）：诚实残口清单——未修的逐条钉住，修掉的不许回退
# --------------------------------------------------------------------------- #
class TestDeclaredGapsR6:
    """未修的残口（逐条给可达性）+ 本轮关掉的那一条（转义引号）。"""

    def test_cjk_key_is_declared_out_of_scope(self):
        """CJK 键名不命中：词干表全是 ASCII 分量，``_KEY_CHARS`` 也只含 ASCII。"""
        assert _scrubber()("密钥=" + SECRET) == "密钥=" + SECRET
        assert _scrubber()("令牌='" + SECRET + "'") == "令牌='" + SECRET + "'"

    def test_key_arrow_value_form_is_declared(self):
        """``key=>value``：``>`` 在 ``_VALUE_STOP`` 里 ⇒ 值立即结束、跨度为 0。"""
        assert _scrubber()("token=>" + SECRET) == "token=>" + SECRET
        assert _scrubber()("access_token=>" + SECRET) == "access_token=>" + SECRET

    def test_client_id_is_an_identifier_not_a_secret(self):
        """``client_id`` 分量 ``client`` / ``id`` 都不在词干表 ⇒ 刻意不掩。"""
        assert _scrubber()("client_id=" + SECRET) == "client_id=" + SECRET

    def test_access_key_id_is_masked_via_its_key_component(self):
        """**对评审假设的诚实更正**：``access_key_id`` **会**被掩——分量 ``key``
        命中词干表（与 ``x-api-key`` 同一条规则）。方向是 fail-closed：它是 AWS
        凭据**标识符**，多掩无害。若将来要「不掩标识符」，必须先给 ``key`` 分量
        加例外，那会同时影响 ``x-api-key``。"""
        assert _is_credential_key()("access_key_id") is True
        assert _scrubber()("access_key_id=" + SECRET) == "access_key_id=***"
        assert _is_credential_key()("client_id") is False

    def test_lowercase_opaque_host_is_no_longer_a_gap(self):
        """r6 的残口 **r7 已关闭**：全小写的不透明 token 落在 host 位置时不再穿过。

        r6 判据只看「host 里有没有大写 ASCII」，于是 ``http://ks9qm4zt`` 与 r5 钉住的
        ``http://h`` 一样被放行；r7 换成属性判据（非白名单单标签 fail closed），
        与 ``http://Ks9Qm4Zt`` 同一格。方向断言：**必须**变常量。
        """
        assert _scrubber()("http://ks9qm4zt") == UNPRINTABLE_URL

    def test_escaped_json_quotes_are_now_closed(self):
        """Java / 双重编码文本里的 ``\\"token\\":\\"K\\"`` 曾是残口（``\\`` 隔断回看），
        r6 关闭了键侧与值的开引号。

        **r7 更正了闭引号的判据**：r6 用 ``text.find``，把 ``\\"`` 当成闭引号 ⇒ 值里
        的转义引号会**截断掩码**。r7 要求闭引号是**未被转义**的那个（见
        :func:`_find_closing_quote`），代价是双重编码文本里「每个引号都写成 ``\\"``」
        时找不到闭引号 ⇒ 掩到串尾（**fail closed**，宁可多掩）。精确形态由
        ``TestEscapedQuoteInValueR7`` 逐条钉住。
        """
        assert _scrubber()('{\\"token\\":\\"' + SECRET + '\\"}') == \
            '{\\"token\\":\\"***'
        assert _scrubber()('fail \\"access_token\\"=\\"' + SECRET + '\\"') == \
            'fail \\"access_token\\"=\\"***'

    def test_escaped_form_is_linear(self):
        """关闭残口不许引入回溯：一遍 256 KB 的转义形态必须远低于上限。"""
        import time

        text = ('{\\"access_token\\":\\"' + "p" * 200 + '\\"}') * 1200
        started = time.monotonic()
        rendered = _scrubber()(text)
        elapsed = time.monotonic() - started
        _assert_clean(rendered, "r6-escaped-256k")
        assert elapsed < 2.0, f"转义形态 256 KB 耗时 {elapsed:.3f}s —— 退化了？"


# --------------------------------------------------------------------------- #
# ㉔ r7（F1）：host 判据从「大小写代理」换成**属性** —— 两个方向 + 大小写不变性
# --------------------------------------------------------------------------- #
def _host_identity():
    fn = getattr(http_mod, "_host_is_identifiable", None)
    assert fn is not None, (
        "缺少 plaita/node/http.py::_host_is_identifiable（r7 的 host 属性判据）"
    )
    return fn


def _closing_quote_scanner():
    fn = getattr(http_mod, "_find_closing_quote", None)
    assert fn is not None, (
        "缺少 plaita/node/http.py::_find_closing_quote（r7 的转义闭引号扫描）"
    )
    return fn


#: r6 的**假阴性**：r6 的「host 出现大写 ASCII 字母」代理放行、r4 曾是常量。
#: 全部是「全小写的不透明 token 落在 host 位置」这一类。
_R7_LOWERCASE_OPAQUE_HOSTS = (
    ("ks9qm4zt", "http://ks9qm4zt"),
    ("ks9qm4zt-slash", "http://ks9qm4zt/"),
    ("ks9qm4zt-port", "http://ks9qm4zt:8080"),
    ("hex-16", "http://7f3a9c2b8e1d4f6a"),
    ("hex-8", "http://deadbeef"),
    ("hex-12", "http://a1b2c3d4e5f6"),
    ("password-word", "http://hunter2"),
    ("secret-word", "http://secret"),
    ("b64url-ish", "http://eyjhbgcioi"),
    ("punycode", "http://xn--ks9qm4zt"),
    ("alnum-12", "http://abc123def456"),
    ("uuid", "http://550e8400-e29b-41d4-a716-446655440000"),
    ("in-sentence", "Failed to connect to http://deadbeef"),
    ("quoted", "boom url='http://hunter2'"),
    ("userinfo", "http://user:pw@ks9qm4zt"),
    ("port-and-slash", "http://7f3a9c2b8e1d4f6a:443/"),
)

#: r6 的**假阳性**：r6 打成常量、r4/r5 保留的**合法点分 DNS 名 / 白名单本地名**。
_R7_DOTTED_HOSTS_KEPT = (
    ("Example.com", "http://Example.com"),
    ("MyHost.local", "http://MyHost.local"),
    ("API.EXAMPLE.COM", "http://API.EXAMPLE.COM"),
    ("EXAMPLE.COM-slash", "http://EXAMPLE.COM/"),
    ("EXAMPLE.com-443-slash", "http://EXAMPLE.com:443/"),
    ("Localhost-8080", "http://Localhost:8080"),
    ("mixed-dot", "https://Api.Example.COM:8443"),
)

#: 单标签**闭合白名单**（大小写不敏感）：只有这些众所周知的本地名保留。
_R7_BENIGN_SINGLE_LABELS = ("h", "host", "localhost", "H", "HOST", "LocalHost")
#: 单标签但**不在**白名单：一律 fail closed（含真实机器名这一格代价）。
_R7_NON_BENIGN_SINGLE_LABELS = (
    "ks9qm4zt", "Ks9Qm4Zt", "deadbeef", "hunter2", "secret", "webhook",
    "DESKTOP-ABC123", "myhost", "internal", "xn--ks9qm4zt", "a1b2c3d4e5f6",
)


def _r7_lowercase_opaque_needs(label: str) -> list:
    """把 host 本体当密钥，逐条给 ≥3 字符前后缀 needle（与 ``leak_needles`` 同口径）。"""
    return leak_needles(label)


class TestHostIdentityPropertyR7:
    """F1：**属性判据**取代 r6 的「host 里有没有大写 ASCII」代理。

    属性（见 ``plaita/node/http.py::_host_is_identifiable``）：没有真掩码的 token 只在
    authority 是**语法上可识别的宿主标识**时保留端点——IPv4 字面量 / ≥2 标签的点分
    DNS 名（含 IPv4 点分四段）/ 闭合方括号端点 / 白名单里的本地单标签名；其余 fail closed。
    判据与大小写**无关**。
    """

    @pytest.mark.parametrize(
        "label,text", _R7_LOWERCASE_OPAQUE_HOSTS, ids=[c[0] for c in _R7_LOWERCASE_OPAQUE_HOSTS]
    )
    def test_lowercase_opaque_hosts_fail_closed(self, label, text):
        """假阴性方向（r6 放行、r4 曾是常量）：全小写的不透明 host 必须变常量。

        ``ks9qm4zt`` 这一格就是「非白名单单标签」，与 ``Ks9Qm4Zt`` 同一判据——
        r6 只看大小写，所以只抓到了后者。
        """
        rendered = _scrubber()(text)
        needles = _r7_lowercase_opaque_needs(label) if label not in ("in-sentence", "quoted", "userinfo", "port-and-slash") else []
        if needles:
            hits = find_needles(rendered, needles)
            assert hits == [], f"r7[{label}]：host 本体留在输出里（needles={hits}）：{rendered!r}"
        assert rendered.endswith(UNPRINTABLE_URL), f"r7[{label}]：实际={rendered!r}"
        assert rendered != text, f"r7[{label}]：原文穿过"

    @pytest.mark.parametrize(
        "label,text", _R7_DOTTED_HOSTS_KEPT, ids=[c[0] for c in _R7_DOTTED_HOSTS_KEPT]
    )
    def test_dotted_hosts_are_kept(self, label, text):
        """假阳性方向（r6 打成常量、r5/r4 保留）：合法点分 DNS 名必须**逐字保留**。

        这就是 F4 要的那条「会红」的断言：判据若退回大小写代理，这些行立刻变红。
        """
        rendered = _scrubber()(text)
        assert rendered == text, f"r7-kept[{label}]：实际={rendered!r} 期望={text!r}"

    @pytest.mark.parametrize(
        "label,text", _R7_DOTTED_HOSTS_KEPT, ids=[c[0] for c in _R7_DOTTED_HOSTS_KEPT]
    )
    def test_dotted_hosts_are_fixed_points(self, label, text):
        once = _scrubber()(text)
        assert _scrubber()(once) == once, f"r7-kept[{label}]：非幂等（{once!r}）"

    @pytest.mark.parametrize(
        "label,text", _R7_DOTTED_HOSTS_KEPT + _R7_LOWERCASE_OPAQUE_HOSTS,
        ids=[c[0] for c in _R7_DOTTED_HOSTS_KEPT + _R7_LOWERCASE_OPAQUE_HOSTS],
    )
    def test_six_surfaces_through_the_real_entry_point(self, label, text):
        """六个输出面**逐字**带着清洗后的形态（真节点 + 真 ``_handle_node_error``）。

        断言用「清洗器的输出必须原样出现在每个面里」而不是「面里有密钥」——前者同时
        钉住「保留了该保留的」与「掩掉了该掩的」，也让大小写代理回退时立刻变红。
        """
        expected = _scrubber()(text)
        node, exc = _entry_exception(text, "gen")
        for name, rendered in _six_surfaces(node, exc).items():
            assert expected in rendered, (
                f"r7[{label}] {name}: 期望 {expected!r} 出现在面里，实际 {rendered[:200]!r}"
            )

    @pytest.mark.parametrize("path", ("gen", "do"))
    def test_lowercase_opaque_host_is_constant_on_both_paths(self, path):
        node, exc = _entry_exception("http://ks9qm4zt", path)
        for name, rendered in _six_surfaces(node, exc).items():
            _assert_clean(rendered, f"r7-lower[{path}] {name}")
            assert "ks9qm4zt" not in rendered, f"r7-lower[{path}] {name}: {rendered!r}"

    def test_single_label_whitelist_is_closed(self):
        """单标签白名单：**闭合**、大小写不敏感、逐个可审。"""
        fn = _host_identity()
        for name in _R7_BENIGN_SINGLE_LABELS:
            assert fn(name) is True, name
        for name in _R7_NON_BENIGN_SINGLE_LABELS:
            assert fn(name) is False, name

    def test_single_label_non_whitelisted_is_declared_masked(self):
        """代价（已声明）：单标签但不在白名单里的**合法**端点也退化为常量。

        ``http://DESKTOP-ABC123`` 是真实机器名，r5 保留、r6 也保留（大写不受 r6 判据
        影响？——不，r6 的判据正是「有大写就 fail closed」，所以 r6 **已经**把它打成
        常量）。方向刻意：单标签是唯一与不透明凭据不可区分的形态，语法判据在这一格
        没有信息，只能 fail closed。
        """
        assert _scrubber()("http://DESKTOP-ABC123") == UNPRINTABLE_URL
        assert _scrubber()("http://webhook") == UNPRINTABLE_URL
        assert _host_identity()("DESKTOP-ABC123") is False
        assert _host_identity()("webhook") is False

    def test_case_invariance_of_the_decision(self):
        """属性判据必须**与大小写无关**——r6 的代理正是这一条不成立。

        对每个样本，``lower()`` / ``upper()`` / ``swapcase()`` 三种写法必须给出**同一
        个**判定（保留 ⇔ 保留）。这直接杀死「看字符类/大小写」的判据形态。
        """
        fn = _host_identity()
        samples = (
            "example.com", "API.EXAMPLE.COM", "MyHost.local", "ks9qm4zt", "Ks9Qm4Zt",
            "DESKTOP-ABC123", "h", "LOCALHOST", "10.1.2.3", "deadbeef",
            "EXAMPLE.com:443", "550e8400-e29b-41d4-a716-446655440000",
        )
        for sample in samples:
            verdicts = {fn(v) for v in (sample, sample.lower(), sample.upper(), sample.swapcase())}
            assert len(verdicts) == 1, f"{sample!r}: 判定随大小写变化 {verdicts}"
            rendered = {_scrubber()(f"http://{v}") for v in (sample, sample.lower())}
            assert all(r == UNPRINTABLE_URL for r in rendered) or all(
                r != UNPRINTABLE_URL for r in rendered
            ), f"{sample!r}: 清洗结果随大小写变化 {rendered}"

    def test_ip_literal_and_dns_name_structure(self):
        """结构判据本体：≥2 标签 DNS 名（含 IPv4 点分四段）/ 单标签白名单 / 空串。"""
        fn = _host_identity()
        assert fn("10.1.2.3") is True
        assert fn("255.255.255.255") is True
        # ``256.1.2.3`` 不是 IP，但**语法上**是 4 标签点分名 ⇒ 收下（这里不是 IP 校验器）
        assert fn("256.1.2.3") is True
        assert fn("10.1.2") is True                # 3 标签数字名同样是点分名
        assert fn("example.com") is True
        assert fn("a.b.c.d.e.f") is True
        assert fn("-bad.example") is False         # 标签以连字符开头
        assert fn("bad-.example") is False         # 标签以连字符结尾
        assert fn("example..com") is False         # 空标签
        assert fn("") is False
        assert fn("a" * 64 + ".example") is False   # 标签 >63 字符（RFC 1035 上界）
        assert fn("a" * 63 + ".example") is True    # 恰好 63 ⇒ 仍合式

    def test_dotted_opaque_host_is_a_declared_residual(self):
        """**已声明残口**：点分的不透明 token 只要每个标签 ≤63 字符且是 LDH 就仍保留。

        ``http://secret.example`` 是 r4 曾掩、r7 保留的一格（与「冻结契约的 host 残留」
        同类）。结构性理由：真实 JWT 的单段长度通常远超 63 字符 ⇒ 落在 fail-closed 侧
        （见下一条），但**短**的点分 token（如 ``secret.example``）留不住。
        """
        assert _scrubber()("http://secret.example") == "http://secret.example"
        short_jwt_host = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0"  # 两段、各 ≤63
        assert _scrubber()(f"http://{short_jwt_host}") == f"http://{short_jwt_host}"

    def test_real_jwt_host_with_long_segment_fails_closed(self):
        """63 字符上界的结构性收益：**真实** JWT（单段远超 63）落进 fail-closed 侧。"""
        header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        payload_len = 300
        payload = "A" * payload_len
        host = f"{header}.{payload}.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJVadQssw5c"
        assert any(len(part) > 63 for part in host.split("."))
        assert _host_identity()(host) is False
        assert _scrubber()(f"http://{host}") == UNPRINTABLE_URL

    def test_fuzz_single_label_family_all_fail_closed(self):
        """fuzz（**穷举**、无随机源）：单标签 host 族**全部** fail closed。

        生成器口径（写死便于复算）：``itertools.product`` over
        ``"abk9qm4zth"``，长度 1..5，共 ``8+64+512+4096+32768 = 37448`` 条。
        白名单里的 ``h`` 是唯一例外（长度 1，单独排除）。与该族对照，r6 的实现
        （大小写代理）会**全数放行**——见 ``host_fuzz.py`` 的四版对照：``[a-z0-9]``
        长度 ≤3 的 47988 条里 r6 放行 47988 条、r4 全判常量。
        """
        import itertools

        fn = _host_identity()
        scrub = _scrubber()
        alphabet = "abk9qm4zth"
        checked = 0
        for n in range(1, 6):
            for tup in itertools.product(alphabet, repeat=n):
                host = "".join(tup)
                if host in _R7_BENIGN_SINGLE_LABELS:
                    continue
                assert fn(host) is False, f"fuzz: {host!r} 被判为可识别宿主"
                checked += 1
        assert checked >= 37000, f"fuzz 覆盖面太小（{checked}）"

    def test_fuzz_dns_family_all_kept(self):
        """fuzz 的反方向（**穷举**）：点分 DNS 名 + 其大小写变体**全部**逐字保留。

        生成器口径：``alphabet="abk9"`` 上长度 1..3 的标签 × ``{com, local, COM}``
        的点分组合，共 ``(4+16+64)*3 = 252`` 条；再对每条做 ``upper/title/capitalize``
        变体。r6 的实现把其中带大写的全部打成常量（假阳性）。
        """
        import itertools

        scrub = _scrubber()
        alphabet = "abk9"
        tlds = ("com", "local", "COM")
        checked = 0
        for n in range(1, 4):
            for tup in itertools.product(alphabet, repeat=n):
                lab = "".join(tup)
                for tld in tlds:
                    for host in (f"{lab}.{tld}", f"{lab.upper()}.{tld}", f"{lab.title()}.{tld}"):
                        assert scrub(f"http://{host}") == f"http://{host}", host
                        checked += 1
        assert checked >= 250, f"fuzz 覆盖面太小（{checked}）"

    def test_fuzz_case_invariance_is_exhaustive_on_the_family(self):
        """判据必须**与大小写无关**——对穷举族逐条验四种写法给出同一判定。

        这正是「代理 vs 属性」的可判据化：r6 的大小写判据在这一条上必然变红。
        """
        import itertools

        fn = _host_identity()
        alphabet = "abk9"
        checked = 0
        for n in range(1, 5):
            for tup in itertools.product(alphabet, repeat=n):
                host = "".join(tup)
                verdicts = {fn(v) for v in (host, host.lower(), host.upper(), host.swapcase())}
                assert len(verdicts) == 1, f"{host!r}: 判定随大小写变化 {verdicts}"
                checked += 1
        assert checked >= 300, f"fuzz 覆盖面太小（{checked}）"


class TestBracketedAuthorityResidualR7:
    """F2：方括号 authority 的**已声明残口** + 死代码转活代码。

    ``http://[Ks9Qm4Zt]`` / ``http://[Ks9Qm4Zt]:8443`` 在 r4/r5/r6 都**逐字穿过**
    （未声明）。r7 的选择是**声明**而不是掩掉，理由是可证的**同串类**：
    ``http://[bad]``（``[`` + 单个合式 DNS 标签 + ``]``）的保留被既有测试钉住
    （``http://user:SECRET@[bad]:1/x?access_token=…`` → ``http://[bad]:1``，
    ``TestBracketedEndpointFallback::test_userinfo_is_dropped_but_endpoint_is_kept``），
    因此任何能掩掉 ``[Ks9Qm4Zt]`` 的规则都会一起掩掉 ``[bad]``。
    """

    def test_pinned_benign_bracket_endpoint_is_the_same_string_class(self):
        """同串类证明：``[bad]`` 与 ``[Ks9Qm4Zt]`` 都是「单标签方括号 authority」。

        两者都保留；下面的断言把「合法端点必须保留」与「可疑端点也保留」并排钉住——
        谁想掩掉后者，必须先让前者变红。
        """
        benign = "http://[bad]"
        suspicious = f"http://[{SECRET}]"
        assert _scrubber()(benign) == benign
        assert _scrubber()(suspicious) == suspicious
        assert _host_identity()("[bad]") is True
        assert _host_identity()(f"[{SECRET}]") is True

    @pytest.mark.parametrize("text", (
        f"http://[{SECRET}]",
        f"http://[{SECRET}]:8443",
        f"http://[{SECRET}]/x",
        f"http://[{SECRET}]/x?access_token=Q",
        "Failed to connect to http://[bad]",
        "http://[web,hook]:8443",
        "http://[webhook.internal]:8443",
        "http://[::1]:8080",
        "http://user:pw@[bad]:1/x?access_token=Q",
    ))
    def test_declared_bracket_forms_are_fixed_points(self, text):
        once = _scrubber()(text)
        assert _scrubber()(once) == once, f"方括号形态非幂等：{text!r} -> {once!r}"

    def test_bracket_branch_is_live_code(self):
        """F2 第二半：``_host_is_identifiable`` 的方括号分支必须是**活代码**。

        r6 的同一分支是死代码——唯一的调用者 ``_token_is_canonical`` 在调用之前就先
        ``return "]" in authority``。现在方括号**不**短路，分支被真实调用。
        """
        fn = _host_identity()
        assert fn("[bad]") is True
        assert fn(f"[{SECRET}]") is True
        assert fn("[web") is False            # 未闭合 ⇒ fail closed（r3 反例 A 的根因）
        assert fn("") is False
        # 与 ``_token_is_canonical`` 的入口一致：未闭合方括号不是不动点
        assert http_mod._token_is_canonical("http://[web") is False
        assert http_mod._token_is_canonical("http://[bad]") is True

    def test_unclosed_bracket_still_never_passes_through(self):
        assert _scrubber()(f"http://[web/robot/send?access_token={SECRET}") != \
            f"http://[web/robot/send?access_token={SECRET}"


#: F3 的逐条形态：值**内部**的转义引号（r4/r5/r6 三版都截断掩码）。
#: 显式 ``ids`` 是必须的——pytest 默认把整条文本塞进 node id，而变异声明的
#: ``expect_fail`` 行必须逐字可寻址（否则「声明行真红」无法机读校验）。
#: 上面七条里挑三条做幂等钉子（修法不得把掩码形态变成非不动点）。
_R7_IDEMPOTENT_F3_IDS = frozenset({"dq-inner-escape", "json-dq-inner-escape",
                                   "java-double-encoded"})

_R7_ESCAPED_VALUE_CASES = (
    ("dq-inner-escape", 'access_token="pre\\"' + SECRET + '"'),
    ("dq-inner-escape-2", 'token="a\\"b' + SECRET + '"'),
    ("sq-inner-escape", "password='pre\\'" + SECRET + "'"),
    ("json-dq-inner-escape", '{"token":"pre\\"' + SECRET + '"}'),
    ("json-dq-escape-then-plain", '{"token":"a\\"b' + SECRET + '"}'),
    ("java-double-encoded", '{\\"token\\":\\"a\\"b' + SECRET + '\\"}'),
    ("key-side-and-inner", 'failed "password"="pre\\"' + SECRET + '"'),
)


class TestEscapedQuoteInValueR7:
    """F3：值**内部**的转义引号不再截断掩码（r4/r5/r6 三版都漏）。

    r6 只关了键侧与值的**开**引号；``text.find(quote, v0)`` 仍把 ``\\"`` 当闭引号 ⇒
    掩码停在 ``\\"`` 处，其后原文（含密钥本体）整段留在输出里。r7 由
    :func:`_find_closing_quote` 找**未被反斜杠转义**的闭引号。
    """

    @pytest.mark.parametrize(
        "label,text", _R7_ESCAPED_VALUE_CASES, ids=[c[0] for c in _R7_ESCAPED_VALUE_CASES]
    )
    def test_escaped_quote_in_value_never_truncates_the_mask(self, label, text):
        rendered = _scrubber()(text)
        hits = find_needles(rendered, leak_needles(SECRET))
        assert hits == [], f"r7-f3[{label}]：转义引号截断了掩码（needles={hits}）：{rendered!r}"
        assert SECRET not in rendered, f"r7-f3[{label}]：{rendered!r}"

    @pytest.mark.parametrize(
        "label,text", tuple(c for c in _R7_ESCAPED_VALUE_CASES
                            if c[0] in _R7_IDEMPOTENT_F3_IDS),
        ids=[c[0] for c in _R7_ESCAPED_VALUE_CASES if c[0] in _R7_IDEMPOTENT_F3_IDS],
    )
    def test_escaped_quote_forms_are_idempotent(self, label, text):
        once = _scrubber()(text)
        assert _scrubber()(once) == once, f"r7-f3[{label}]：非幂等（{once!r}）"

    def test_exact_shapes_of_the_enumerated_f3_cases(self):
        """逐条钉住 r4/r5/r6 的泄漏形态与 r7 的修法（精确字符串）。"""
        assert _scrubber()('access_token="pre\\"' + SECRET + '"') == 'access_token="***"'
        assert _scrubber()('token="a\\"b' + SECRET + '"') == 'token="***"'
        assert _scrubber()('{"token":"pre\\"' + SECRET + '"}') == '{"token":"***"}'
        assert _scrubber()('{\\"token\\":\\"a\\"b' + SECRET + '\\"}') == '{\\"token\\":\\"***'

    def test_closing_quote_scanner_skips_escaped_quotes(self):
        """扫描器本体：只有**奇数个**紧邻反斜杠才算转义（``\\"`` 转义、``\\\\"`` 不转义）。"""
        scan = _closing_quote_scanner()
        assert scan('ab"cd', 0, '"') == 2
        assert scan('ab\\"cd"ef', 0, '"') == 6          # 第一个被转义（下标 3），跳到下标 6
        assert scan('ab\\\\"cd', 0, '"') == 4           # 两个反斜杠 ⇒ 不转义
        assert scan('ab\\\\\\"cd', 0, '"') == 8         # 三个 ⇒ 转义（无闭引号 ⇒ 串尾）
        assert scan("no quote here", 0, '"') == len("no quote here")
        assert scan('a"b', 3, '"') == 3                 # 起点即闭引号

    def test_escaped_quote_scan_is_linear(self):
        """线性钉子：长转义运行不许退化（反斜杠计数是单趟常数空间）。"""
        import time

        for text in (
            'token="' + "\\\\" * 20000 + '"',
            'token="' + 'a\\"' * 20000 + '"',
            'token="' + "\\" * 40000,
        ):
            started = time.monotonic()
            _scrubber()(text)
            elapsed = time.monotonic() - started
            assert elapsed < 2.0, f"转义运行 {len(text)} 字符耗时 {elapsed:.3f}s —— 退化了？"
