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

import logging
import traceback

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
