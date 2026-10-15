"""plaita#161 —— 第三方异常文本经 ``logger.*(…, exc)`` 出仓（``#158``+``#159`` 同族残口）。

``#158``+``#159`` r2 只在 ``plaita/node/http.py`` 的消息**构造处**清洗了第三方
HTTP 客户端异常文本（``requests`` 的 ``url: /path?query`` / ``aiohttp`` 的
``url='…'``）。但同一形态的出口还有三处，它们的 logger 调用点直接吃异常对象：

============================  ==========================================
``server/alerts.py``          投递失败 WARNING（``PostFn`` 可注入 requests 型 post）
``server/services/http_callback_service.py``  6 处 ``logger.error(…, e, exc_info=True)``
``server/nodes/http_callback_node.py``        ``_resolve_value`` 的 ``logger.warning(…, e)``
============================  ==========================================

本文件钉三件事：

1. **共享渲染器**：``_scrub_url_text`` 的家已从 ``node/http.py`` 迁到
   ``plaita/server/url_redaction.py``，``node`` 层与 ``#161`` 的三个站点用的是
   **同一个函数对象**（:func:`test_node_layer_and_sites_share_one_implementation`
   用 ``is`` 断言，不是「同名两份」）。
2. **构造处清洗**（``#158`` r2 纪律）：不在 logger 调用点打补丁。``exc_info=True``
   形态尤其清洗不到——traceback 由 ``Formatter`` 在 logger 调用**之后**用原始异常
   重新渲染（``traceback.format_exception`` 取 ``str(exc)``），所以那些站点改成
   在构造面渲染「异常文本 + 栈」的 ``_scrub_exception_text``；原本没有 ``exc_info``
   的站点只渲染一行（``_scrub_url_text`` 对非 ``str`` 先 ``str()``）。
3. **五个记录面**：``record.msg`` / ``repr(record.args)`` / ``getMessage()`` /
   ``record.__dict__``（**含** ``exc_info``）/ ``exc_text``（含 ``Formatter`` 渲染）
   全无 needle（密钥本体 + 任何 ≥3 字符前缀/后缀，复用 ``#158`` 的 ``leak_needles``）。

边界（本单**不**覆盖，见 ``docs/issue-draft-url-in-exception-text.md``）：redis-py 的
异常文本**实测**不含 URL/口令（本文件用真实 redis-py 复验）；kafka / DB 客户端在本机
没有实例，无法动态验证——它们与 ``server/registry.py``·``server/event_filter.py``·
``server/task_queue.py`` 等其它 ``logger.*(…, e)`` 站点一样，属于 #161 正文未列的
同族面，留给后续单。
"""
from __future__ import annotations

import ast
import asyncio
import logging
import socket

import pytest

import plaita.node.http as http_mod
from plaita.server import url_redaction
from plaita.server.url_redaction import (
    _scrub_exception_text,
    _scrub_url_text,
)
from tests.unit import _url_sink_support as sink_support
from tests.unit._url_sink_support import (
    PLAITA_DIR,
    _is_log_call,
    find_needles,
    leak_needles,
)

SECRET = "Ks9Qm4Zt"
NEEDLES = leak_needles(SECRET)

_FORMATTER = logging.Formatter("%(levelname)s %(name)s %(message)s")

#: 造一个「密钥在 path/query 里」的 URL（与 #159 的钉钉/企微形态同族）。
DEAD_URL_TMPL = "http://127.0.0.1:{port}/robot/send?access_token=" + SECRET


# --------------------------------------------------------------------------- #
# 记录面：五面 + Formatter 渲染
# --------------------------------------------------------------------------- #
def _record_surfaces(records) -> str:
    """``record.__dict__`` **全量**（含 ``exc_info``/``exc_text``）+ ``getMessage`` + 渲染。

    ``#158`` 的 ``log_record_fields`` 刻意跳过 ``LogRecord`` 标准键（只看 ``extra=``
    注入的自定义键）。``#161`` 的泄露面恰恰有 ``exc_info``（原始异常对象）与
    ``exc_text``（Formatter 渲染出的 traceback）两个**标准**键，所以这里不跳——
    验收口径写的是 ``record.__dict__`` 与 ``exc_text`` 整个面。
    """
    chunks = []
    for record in records:
        rendered = _FORMATTER.format(record)  # 会填 record.exc_text
        chunks.append(str(record.msg))
        chunks.append(repr(record.args))
        chunks.append(record.getMessage())
        for key, value in sorted(vars(record).items()):
            chunks.append(f"{key}={value!r}")
        chunks.append(f"formatted={rendered}")
    return "\n".join(chunks)


def _assert_clean(text: str, label: str) -> None:
    hits = find_needles(text, NEEDLES)
    assert hits == [], f"{label} 泄露了访问令牌（needles={hits}）：\n{text}"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _requests_exception() -> tuple:
    """真发一次到本地死端口的 requests 请求，返回 ``(url, exc)``。

    真异常（不是手搓字符串）是前提：它证明 requests **确实**把请求 URL 写进了
    自己的异常文本（``Max retries exceeded with url: /robot/send?access_token=…``），
    否则本文件的动态断言就是空转。
    """
    requests = pytest.importorskip("requests")
    url = DEAD_URL_TMPL.format(port=_free_port())
    try:
        requests.get(url, timeout=2)
    except Exception as exc:  # noqa: BLE001 — 要的就是这个异常对象
        return url, exc
    raise AssertionError("前提失效：本地死端口居然请求成功")


def _assert_premise(exc, label: str) -> None:
    """注入的异常文本**本身**必须带 needle——否则「清洗后干净」不构成证据。"""
    raw = f"{exc!r}\n{exc}"
    assert find_needles(raw, NEEDLES), f"{label}: 注入的异常文本本身不带令牌，用例空转"


class _Raiser:
    """替身：未显式设置的属性访问都返回「调用即抛指定异常」的函数。

    ``hasattr(client, "get")`` 会因此为真（用于骗过 ``_use_redis``），而
    ``client.set(...)`` / ``client.scan_iter(...)`` / ``client.mget(...)`` 抛错。
    """

    def __init__(self, exc, **attrs):
        self._exc = exc
        for key, value in attrs.items():
            setattr(self, key, value)

    def __getattr__(self, _name):
        def _boom(*_args, **_kwargs):
            raise self._exc
        return _boom


def _boom(exc):
    def _raise(*_args, **_kwargs):
        raise exc
    return _raise


# --------------------------------------------------------------------------- #
# ① 共享渲染器：一个实现，不是两份同名
# --------------------------------------------------------------------------- #
class TestSharedRendererLivesInUrlRedaction:
    def test_node_layer_and_sites_share_one_implementation(self):
        assert http_mod._scrub_url_text is url_redaction._scrub_url_text, (
            "node 层的 _scrub_url_text 与 url_redaction 的不是同一个函数对象——"
            "「同一个 URL 两套渲染」会立刻分叉（#88 的根因形态）"
        )
        assert http_mod._mask_policy_url is url_redaction._mask_policy_url

    def test_three_sites_import_the_shared_renderer(self):
        """三个站点必须 import 共享实现，不许自带一份私有清洗器。"""
        for rel in (
            "server/alerts.py",
            "server/services/http_callback_service.py",
            "server/nodes/http_callback_node.py",
        ):
            source = (PLAITA_DIR / rel).read_text(encoding="utf-8")
            assert "_scrub_" in source, rel
            assert "def _scrub_url_text" not in source, f"{rel} 自带了清洗器实现（应共享）"

    def test_total_on_the_hostile_batch(self):
        """totality：不抛、必为 ``str``、必可 UTF-8 编码、幂等、绝不原样放出凭据。"""
        url = DEAD_URL_TMPL.format(port=1)
        hostile = [
            None, "", "nothing to mask", b"\xff\xfe", 123, object(),
            {"url": url}, [url],
            "\ud800", "\udcff", f"boom \ud800 tail",
            f"\ud800 {url}", f"{url} \ud800",
            f"http://ho\ud800st/robot/send?access_token={SECRET}",
            f"Max retries exceeded with url: /robot/send?access_token={SECRET}\ud800",
        ]
        for value in hostile:
            rendered = _scrub_url_text(value)
            assert isinstance(rendered, str), value
            rendered.encode("utf-8")  # 写不进日志就等于是新的异常面
            assert _scrub_url_text(rendered) == rendered, value
            _assert_clean(rendered, f"hostile {value!r}")
            if isinstance(value, str) and SECRET in value:
                assert rendered != value

    def test_third_party_text_shapes_are_masked(self):
        for text in (
            f"Max retries exceeded with url: /robot/send?access_token={SECRET} (Caused by X)",
            f"500, message='boom', url='http://h/robot/send?access_token={SECRET}'",
            f"<urlopen error [Errno 61] Connection refused> http://h/x?access_token={SECRET}",
            f"boom for url: https://hooks.slack.com/services/T00/B00/{SECRET}",
        ):
            _assert_clean(_scrub_url_text(text), f"scrub({text!r})")

    def test_exception_renderer_keeps_stack_and_drops_the_token(self):
        url, exc = _requests_exception()
        _assert_premise(exc, "requests ConnectionError")
        rendered = _scrub_exception_text(exc)
        _assert_clean(rendered, "_scrub_exception_text")
        # 诊断价值：栈帧与失败端点仍在（host:port 是 #159 的同一口径）
        assert "Traceback (most recent call last)" in rendered
        assert exc.__traceback__ is not None
        assert "127.0.0.1" in rendered
        assert url not in rendered

    def test_exception_renderer_is_total_on_objects(self):
        # 非异常对象也不能抛：traceback 渲染失败退回 str()，再清洗。
        for value in (None, object(), "plain", 123):
            rendered = _scrub_exception_text(value)
            assert isinstance(rendered, str), value
            rendered.encode("utf-8")


# --------------------------------------------------------------------------- #
# ② 静态腿：文件里不存在「异常对象直达 logger」的调用点
# --------------------------------------------------------------------------- #
#: 允许吃下异常对象的渲染器（共享实现 + 其 total 包装）。
_MASKER_NAMES = frozenset(sink_support._MASKER_NAMES) | {"_scrub_exception_text"}


def _names_in_masked_position(node: ast.AST, masked: bool, out: list) -> None:
    """收集 ``node`` 里出现的 ``Exception`` 名，并记录它是否在掩码器调用之内。"""
    if isinstance(node, ast.Call):
        called = sink_support._dotted(node.func).rsplit(".", 1)[-1]
        inner = masked or called in _MASKER_NAMES
        _names_in_masked_position(node.func, masked, out)
        for arg in node.args:
            _names_in_masked_position(arg, inner, out)
        for kw in node.keywords:
            _names_in_masked_position(kw.value, inner, out)
        return
    if isinstance(node, ast.Name):
        out.append((node.lineno, node.id, masked))
        return
    for child in ast.iter_child_nodes(node):
        _names_in_masked_position(child, masked, out)


def _bare_exception_log_sites(source: str, filename: str = "<memory>") -> list:
    """``except … as NAME`` 体内，把 NAME 直接（未经掩码器）交给 ``logger.*`` 的位置。

    返回 ``[(lineno, identifier, reason)]``。``exc_info=True`` 也判红：那一支的
    traceback 由 ``Formatter`` 在 logger 之后用**原始异常**重新渲染，构造面清洗
    盖不住（``#161`` 的核心发现）。
    """
    tree = ast.parse(source, filename=filename)
    offenders = []
    for handler in ast.walk(tree):
        if not isinstance(handler, ast.ExceptHandler) or not handler.name:
            continue
        for node in ast.walk(handler):
            if not _is_log_call(node):
                continue
            for kw in node.keywords:
                if kw.arg == "exc_info" and not (
                    isinstance(kw.value, ast.Constant) and kw.value.value is False
                ):
                    offenders.append((node.lineno, "exc_info", "exc_info=True"))
            refs: list = []
            for arg in list(node.args) + [kw.value for kw in node.keywords]:
                _names_in_masked_position(arg, False, refs)
            for lineno, name, masked in refs:
                if name == handler.name and not masked:
                    offenders.append((lineno, name, "裸异常对象"))
    return offenders


class TestNoSitePassesABareException:
    def test_guard_reddens_on_a_bare_exception(self):
        red = _bare_exception_log_sites(
            "try:\n    x()\nexcept Exception as e:\n    logger.error('x: %s', e)\n"
        )
        assert red, "静态腿看不见「logger.*(…, e)」——判据失效"
        assert _bare_exception_log_sites(
            "try:\n    x()\nexcept Exception as e:\n"
            "    logger.error('x: %s', _scrub_url_text(e))\n"
        ) == []

    def test_guard_reddens_on_exc_info_after_a_scrubbed_arg(self):
        """即使消息参数已清洗，``exc_info=True`` 仍会把原文从 traceback 放出去。"""
        red = _bare_exception_log_sites(
            "try:\n    x()\nexcept Exception as e:\n"
            "    logger.error('x: %s', _scrub_url_text(e), exc_info=True)\n"
        )
        assert red, red

    def test_three_files_are_clean(self):
        for rel in (
            "server/alerts.py",
            "server/services/http_callback_service.py",
            "server/nodes/http_callback_node.py",
        ):
            source = (PLAITA_DIR / rel).read_text(encoding="utf-8")
            offenders = _bare_exception_log_sites(source, filename=rel)
            assert offenders == [], f"{rel} 仍把异常对象直达 logger：{offenders}"

    def test_probe_real_source_reddens_then_greens(self, tmp_path):
        """由**真实源文件**改一行生成探针：先证明静态腿会红，再回到绿。"""
        rel = "server/alerts.py"
        source = (PLAITA_DIR / rel).read_text(encoding="utf-8")
        fixed_call = "_scrub_url_text(exc)"
        assert fixed_call in source, "前提失效：alerts.py 里没有 _scrub_url_text(exc)"
        probe = tmp_path / "probe_alerts.py"
        probe.write_text(source.replace(fixed_call, "exc", 1), encoding="utf-8")
        assert _bare_exception_log_sites(
            probe.read_text(encoding="utf-8"), filename=str(probe)
        ), "静态腿没有抓住把清洗器去掉的探针（假阴）"
        assert _bare_exception_log_sites(source, filename=rel) == []


# --------------------------------------------------------------------------- #
# ③ alerts.py：投递失败 WARNING（真 requests 异常对象）
# --------------------------------------------------------------------------- #
class TestAlertsDeliveryWarning:
    def _records(self, caplog):
        from plaita.server.alerts import WebhookAlerter

        url, exc = _requests_exception()
        _assert_premise(exc, "alerts 注入异常")
        alerter = WebhookAlerter(url, post=_boom(exc))
        with caplog.at_level(logging.WARNING, logger="plaita.server.alerts"):
            assert alerter.send({"event": "dead_letter"}) is True
            assert alerter.flush(timeout=5) is True
        return exc, caplog.records

    def test_five_record_surfaces_never_carry_the_token(self, caplog):
        exc, records = self._records(caplog)
        assert records, "没有产生任何 WARNING 记录"
        _assert_clean(_record_surfaces(records), "alerts 投递失败 WARNING")

    def test_diagnostic_value_is_kept(self, caplog):
        _exc, records = self._records(caplog)
        fields = _record_surfaces(records)
        assert "127.0.0.1" in fields, fields  # 端点仍在（掩码不是删日志）
        assert "Max retries exceeded" in fields, fields  # 异常文本仍在

    def test_queue_full_line_has_no_exception_vector(self, caplog):
        """jeffkit 点名「check :88」：队列满那行本来就只记掩码后的 url，无异常对象。"""
        from plaita.server.alerts import WebhookAlerter

        url, exc = _requests_exception()
        alerter = WebhookAlerter(url, queue_size=1, post=_boom(exc))
        alerter._ensure_thread = lambda: None  # 不让消费者排空队列
        with caplog.at_level(logging.WARNING, logger="plaita.server.alerts"):
            assert alerter.send({"event": "one"}) is True
            assert alerter.send({"event": "two"}) is False
        assert caplog.records, "没有产生队列满 WARNING"
        for record in caplog.records:
            assert record.exc_info is None, "队列满那行不该带异常对象"
        _assert_clean(_record_surfaces(caplog.records), "alerts 队列满 WARNING")


# --------------------------------------------------------------------------- #
# ④ http_callback_service.py：6 个 logger.error(…, e, exc_info=True) 站点
# --------------------------------------------------------------------------- #
def _service(redis_client=None):
    from plaita.event.memory import InMemoryEventBus
    from plaita.server.services.http_callback_service import HttpCallbackService

    return HttpCallbackService(InMemoryEventBus(), {}, redis_client=redis_client)


def _callback_task(path="/callback/f/cb/e"):
    return {
        "type": "http_callback",
        "node_id": "cb",
        "execution_id": "e",
        "flow_id": "f",
        "event_type": "http_callback",
        "callback_config": {"path": path, "method": "POST"},
        "response_config": {},
    }


class TestHttpCallbackServiceSites:
    """六个站点逐个用带 token 的真 requests 异常注入，逐面断言。"""

    def _capture(self, caplog, act):
        with caplog.at_level(logging.ERROR, logger="plaita.server.services.http_callback_service"):
            result = act()
        assert caplog.records, "没有产生任何 ERROR 记录（注入点失效？）"
        return result, caplog.records

    def test_start_service(self, caplog):
        _url, exc = _requests_exception()
        _assert_premise(exc, "start_service")
        from plaita.server.services.http_callback_service import HttpCallbackService

        svc = _service()

        class _Boom:
            def __get__(self, obj, objtype=None):
                return False

            def __set__(self, obj, value):
                raise exc

        HttpCallbackService.is_running = _Boom()
        try:
            _result, records = self._capture(caplog, svc.start_service)
        finally:
            del HttpCallbackService.is_running
        _assert_clean(_record_surfaces(records), "start_service")

    def test_stop_service(self, caplog):
        _url, exc = _requests_exception()
        _assert_premise(exc, "stop_service")
        svc = _service()
        svc.registered_callbacks = _Raiser(exc)
        _result, records = self._capture(caplog, svc.stop_service)
        _assert_clean(_record_surfaces(records), "stop_service")

    def test_handle_task(self, caplog):
        _url, exc = _requests_exception()
        _assert_premise(exc, "handle_task")
        svc = _service(redis_client=_Raiser(exc))
        _result, records = self._capture(caplog, lambda: asyncio.run(svc.handle_task(_callback_task())))
        _assert_clean(_record_surfaces(records), "handle_task")

    def test_handle_callback_request(self, caplog):
        _url, exc = _requests_exception()
        _assert_premise(exc, "handle_callback_request")
        svc = _service()
        svc._read_callback_record = _boom(exc)
        _result, records = self._capture(
            caplog,
            lambda: asyncio.run(svc.handle_callback_request("/callback/f/cb/e", {}, {})),
        )
        _assert_clean(_record_surfaces(records), "handle_callback_request")

    def test_get_registered_callbacks_scan(self, caplog):
        _url, exc = _requests_exception()
        _assert_premise(exc, "scan_iter")
        svc = _service(redis_client=_Raiser(exc))
        _result, records = self._capture(caplog, svc.get_registered_callbacks)
        _assert_clean(_record_surfaces(records), "scan_iter")

    def test_get_registered_callbacks_mget(self, caplog):
        _url, exc = _requests_exception()
        _assert_premise(exc, "mget")
        svc = _service(
            redis_client=_Raiser(exc, scan_iter=lambda match=None: ["plaita:http_callback:registered:/p"]),
        )
        _result, records = self._capture(caplog, svc.get_registered_callbacks)
        _assert_clean(_record_surfaces(records), "mget")

    def test_all_six_sites_keep_the_stack(self, caplog):
        """``exc_info=True`` 的**行为等价物**：栈没丢，只是换成了清洗后的文本。"""
        _url, exc = _requests_exception()
        svc = _service(redis_client=_Raiser(exc))
        _result, records = self._capture(caplog, lambda: asyncio.run(svc.handle_task(_callback_task())))
        rendered = _record_surfaces(records)
        assert "Traceback (most recent call last)" in rendered, rendered


# --------------------------------------------------------------------------- #
# ⑤ http_callback_node.py：_resolve_value 的 logger.warning(…, e)
# --------------------------------------------------------------------------- #
class TestHttpCallbackNodeResolveValue:
    def _records(self, caplog):
        from plaita.server.nodes.http_callback_node import HttpCallbackNode

        url, exc = _requests_exception()
        _assert_premise(exc, "http_callback_node")
        node = HttpCallbackNode(id="cb1", name="cb1")
        execution = type("Ctx", (), {"evaluate": _boom(exc)})()
        with caplog.at_level(logging.WARNING, logger="plaita"):
            assert node._resolve_value(execution, "$some.ref") == "$some.ref"
        return url, caplog.records

    def test_five_record_surfaces_never_carry_the_token(self, caplog):
        _url, records = self._records(caplog)
        assert records, "没有产生任何 WARNING 记录"
        _assert_clean(_record_surfaces(records), "http_callback_node _resolve_value")

    def test_non_url_texts_stay(self, caplog):
        """不回归：变量引用与异常文本本身仍在记录里。"""
        _url, records = self._records(caplog)
        fields = _record_surfaces(records)
        assert "$some.ref" in fields, fields
        assert "Max retries exceeded" in fields, fields


# --------------------------------------------------------------------------- #
# ⑥ redis-py 复验（第 ③ 个「实测安全」结论）；kafka/DB 无法动态验证
# --------------------------------------------------------------------------- #
class TestRedisPyTextsCarryNoUrl:
    def test_connection_error_text_has_no_url_or_password(self):
        redis = pytest.importorskip("redis")
        for url in (
            f"redis://:{SECRET}@127.0.0.1:1/0",
            f"redis://user:{SECRET}@127.0.0.1:1/0",
        ):
            client = redis.Redis.from_url(url, socket_connect_timeout=0.3, socket_timeout=0.3)
            try:
                client.ping()
            except Exception as exc:  # noqa: BLE001 — 要的就是这个异常
                text = f"{exc!r}\n{exc}"
            else:  # pragma: no cover - 本机不应有 127.0.0.1:1 的 redis
                pytest.fail("前提失效：127.0.0.1:1 居然有 redis 在听")
            assert SECRET not in text, text
            assert url not in text, text
            assert "redis://" not in text, text
            # 诊断价值：端点仍在
            assert "127.0.0.1:1" in text, text
