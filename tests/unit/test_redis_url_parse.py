"""plaita#88 —— ``_parse_redis_url`` 的形态契约。

缺陷：手写正则只认 ``redis://[:password@]host:port[/db]``，``match is None`` 时
**静默**返回 ``localhost:6379/0``。于是 ``rediss://``、``user:pass@``、省略端口、
IPv6 字面量等形态被改写成 localhost，而同一进程的队列/事件总线仍用原始 URL
（``Redis.from_url``）——存储与队列连到两个 Redis。

本文件把「URL 形态 × 入口」的每一格钉成三态之一：

* **解析**（合法 URL → ``RedisTarget``，且与队列 ``Redis.from_url`` 同端点）；
* **具名拒绝**（畸形/不支持 → ``RedisUrlError``，带原因 + **掩码** URL）；
* **显式默认**（未配置：``None``/空串/纯空白 → ``DEFAULT_REDIS_URL``，非错误）。

没有第四态：任何形态都不得静默变成 localhost。

r2（P0）：拒绝消息里的 URL 一律经 ``factory._mask_url`` 渲染（密码 ``***``）——
它会被 ``flow_worker`` / ``event_filter`` 原样写进生产日志。

r3（P0）：r2 的 ``_mask_url`` 是正则（口令字符类排除 ``/`` 与空白、只掩第一个
``X:Y@``），口令含 ``@``/空格的形态照样原样落盘。本文件改用**结构化**掩码
（authority 里最后一个 ``@`` 分 userinfo、userinfo 第一个 ``:`` 分用户/口令），
并以 ``redis.connection.parse_url`` **接地**：只要 redis-py 认出口令，就断言
原文与 parse_url 口令的**长度 ≥3 前缀/后缀**都不出现在 ``str``/``repr``/
traceback 任何一处。

r4（P0）：r3 仍有三处「依赖输入形态」的漏点，本文件把它们钉死：

* 结构层只在**字面 ``://``** 之后找 userinfo ⇒ 单斜杠 ``redis:/user:pw@host``、
  大写 ``REDIS:/…``、凭据在 scheme 前 ``user:pw@redis://host`` 认不出凭据，
  **原样返回**（r2 反而掩住了，是 r3 的回归）。现在锚点是**全串最后一个 ``@``**，
  与 ``://`` 无关；
* 口令重复写在 userinfo 之外（``?socket_timeout=PwX``、``/PwX``）时活下来。
  现在识别到的口令以**原文 / percent-encoded / decoded** 三种形态在 userinfo 段
  与 path/query/fragment 上再洗一遍（scheme/host 是结构，不参与替换）；
* ``raise … from exc`` 的 ``__cause__`` 是 ``traceback`` **独立渲染的对象**，
  只洗消息文案不够。现在换成洗过的**同类型替身**。

三个渲染面（``str`` / ``repr`` / ``traceback.format_exception``）都有断言。
"""
from __future__ import annotations

import traceback
import unittest

from redis import Redis
from redis.connection import parse_url

from plaita.server import factory
from plaita.server.factory import (
    DEFAULT_REDIS_URL,
    RedisTarget,
    RedisUrlError,
    _REDACTED,
    _mask_url,
    _parse_redis_url,
    create_storage_component,
)

#: 合法形态（解析；端口/db 可省，凭据可带用户名）
ACCEPTED = [
    "redis://host:6379/0",
    "redis://host",
    "redis://host/3",
    "redis://host:6379",
    "redis://host:6379/",
    "redis://:pw@host:6379/2",
    "redis://user:pw@host:6379/0",
    "rediss://user:pw@host:6380/1",
    "rediss://host:6380",
    "redis://user%40d:p%40ss%3Aword@host:6379/0",
    "redis://[::1]:6379/0",
    "rediss://[2001:db8::1]:6380/2",
    "redis://host:6379/0?db=2",
    # r2：redis-py 认的 query 参数必须能进来（此前一律硬拒绝 ⇒ 生产启动崩）
    "redis://host:6379/0?socket_timeout=5",
    "redis://host:6379/0?socket_timeout=5&health_check_interval=30",
    "rediss://host:6380/0?ssl_min_version=771&health_check_interval=30",
    # r2：redis-py 不认识的 query 名字也不再判死（见 _parse_query_options）
    "redis://host:6379/0?foo=bar",
    "unix:///tmp/plaita-redis.sock",
    "unix:///tmp/plaita-redis.sock?db=1",
]

#: 畸形/不支持形态（具名拒绝）——文件里的每一种都曾被静默改写成 localhost
REFUSED = [
    ("empty target", "redis://"),
    ("host missing with db", "redis:///3"),
    ("missing scheme", "host:6379"),
    ("scheme-relative", "//host:6379"),
    ("wrong scheme http", "http://host:6379"),
    ("wrong scheme rediss-less typo", "redis2://host:6379/0"),
    ("uppercase scheme", "REDIS://host:6379/0"),
    ("port non-numeric", "redis://host:notaport"),
    ("port out of range", "redis://host:99999"),
    ("port zero", "redis://host:0"),
    ("invalid IPv6 literal", "redis://[::1:6379/0"),
    ("bad db path", "redis://host/foo"),
    ("negative db query", "redis://host:6379/0?db=-1"),
    ("non-int db query", "redis://host:6379/0?db=x"),
    ("invalid query value", "redis://host:6379/0?socket_timeout=abc"),
    # r2：parse_url 认 timeout，但 AbstractConnection 没有该形参——传下去会连接期炸
    ("timeout query param", "redis://host:6379/0?timeout=3"),
    ("fragment", "redis://:p#w@host:6379/0"),
    ("fragment after db", "redis://host:6379/0#frag"),
    ("unix without socket path", "unix://"),
]

#: 带凭据的拒绝形态（plaita#88 r2 P0 → r3 结构化掩码）：消息会进生产日志，
#: 口令一个字都不能出现。
#:
#: 每条 = ``(形态名, URL, 原文里出现的口令字面量)``。测试用
#: ``redis.connection.parse_url`` **接地**（复核 redis-py 真正会拿去连的口令），
#: 再断言「原文口令」与「parse_url 口令」的**长度 ≥3 前缀/后缀**都不出现在
#: ``str``/``repr``/traceback 里——r2 的正则只掩住普通中段口令，本表覆盖
#: 口令含 ``@``（前/后/多个）/空格/tab/nbsp/``:``/``%``/unicode、空用户、空口令、
#: authority 多个 ``@``、裸 ``#``、bytes、缺 scheme、畸形 authority 等全部形态。
SECRET = "sup3rsecret"
LEAK_SHAPES = [
    # r2 正则的四个漏网形态（r3 P0 反例，逐字保留）
    ("at leading password", "redis://user:@Pr0d2024x@redis.internal:0", "@Pr0d2024x"),
    ("space in password", "redis://user:Super Secret@redis.internal:0", "Super Secret"),
    ("at inside password", "redis://user:p@ssword@redis.internal:0", "p@ssword"),
    ("nbsp in password", "redis://user:Super\xa0Secret@redis.internal:0", "Super\xa0Secret"),
    # 口令里 '@' 的其余位置 / 多个 '@'
    ("at trailing password", "redis://user:Pr0d2024x@@redis.internal:0", "Pr0d2024x@"),
    ("multiple at in password", "redis://user:Pr0d2024x@c@redis.internal:0", "Pr0d2024x@c"),
    ("multiple at in authority", "redis://a@b:Pr0d2024x@c@redis.internal:0", "Pr0d2024x@c"),
    # 空白 / 结构字符 / percent-escape / unicode
    ("tab in password", "redis://user:Super\tSecret@redis.internal:0", "Super\tSecret"),
    ("colon in password", "redis://user:Su:per@redis.internal:0", "Su:per"),
    ("percent-encoded at", "redis://user:Pr0d%402024x@redis.internal:0", "Pr0d%402024x"),
    ("percent-encoded slash", "redis://user:Pr0d%2F2024x@redis.internal:0", "Pr0d%2F2024x"),
    ("unicode password", "redis://user:p\u00e4ssw\u00f6rd@redis.internal:0", "p\u00e4ssw\u00f6rd"),
    # 空用户 / 空口令 / 口令在裸 '#'
    ("empty user", "redis://:Pr0d2024x@redis.internal:0", "Pr0d2024x"),
    ("empty password", "redis://user:@redis.internal:0", ""),
    ("raw hash in password", "redis://:Pr0d2024x#x@host:6379/0", "Pr0d2024x#x"),
    # 各拒绝分支
    ("fragment", "redis://user:Pr0d2024x@redis.internal:6379/0#frag", "Pr0d2024x"),
    ("bad db query", "redis://user:Pr0d2024x@redis.internal:6379/0?db=x", "Pr0d2024x"),
    ("negative db query", "redis://user:Pr0d2024x@redis.internal:6379/0?db=-1", "Pr0d2024x"),
    ("bad db path", "redis://user:Pr0d2024x@redis.internal:6379/foo", "Pr0d2024x"),
    (
        "invalid query value",
        "redis://user:Pr0d2024x@redis.internal:6379/0?socket_timeout=abc",
        "Pr0d2024x",
    ),
    (
        "timeout query param",
        "redis://user:Pr0d2024x@redis.internal:6379/0?timeout=3",
        "Pr0d2024x",
    ),
    ("port non-numeric", "redis://user:Pr0d2024x@host:notaport", "Pr0d2024x"),
    ("port out of range", "redis://user:Pr0d2024x@host:99999", "Pr0d2024x"),
    ("port zero", "redis://user:Pr0d2024x@host:0", "Pr0d2024x"),
    ("missing host", "redis://user:Pr0d2024x@/3", "Pr0d2024x"),
    ("invalid IPv6 literal", "redis://user:Pr0d2024x@[::1:6379/0", "Pr0d2024x"),
    ("malformed authority", "redis://[::1:user:Pr0d2024x@host/0", "Pr0d2024x"),
    ("unix without socket path", "unix://user:Pr0d2024x@", "Pr0d2024x"),
    ("unsupported scheme", "REDIS://user:Pr0d2024x@host:6379/0", "Pr0d2024x"),
    ("missing scheme", "user:Pr0d2024x@host:6379", "Pr0d2024x"),
    ("wrong scheme", "http://user:Pr0d2024x@host:6379/0", "Pr0d2024x"),
    ("non-str bytes", b"redis://user:Pr0d2024x@host:6379/0", "Pr0d2024x"),
    # r4 Finding 1 逐字复现（验证者的五个形态）：r2 掩住了、r3 原样返回——
    # 结构性回归，因为 r3 只在**字面 '://'** 的 authority 里找 userinfo。
    ("r4 single-slash typo", "redis:/user:pw@host", "pw"),
    ("r4 single-slash typo with port/db", "redis:/user:pw@host:6379/0", "pw"),
    ("r4 single-slash uppercase scheme", "REDIS:/user:pw@host", "pw"),
    ("r4 credentials before scheme", "user:pw@redis://host", "pw"),
    ("r4 credentials before rediss", "user:pw@rediss://host:6380/0", "pw"),
    # r4 Finding 1 兜底层：口令重复出现在 userinfo 之外（query/path）
    ("r4 password repeated in query", "redis://user:PwX@host:6379/0?socket_timeout=PwX", "PwX"),
    (
        "r4 password repeated percent-encoded in query",
        "redis://user:@Pr0d2024x@host:6379/0?socket_timeout=%40Pr0d2024x",
        "@Pr0d2024x",
    ),
]


#: r4 Finding 1：验证者的五个形态 × **≥3 字符口令**（verbatim 那组口令只有 2 字符，
#: 走不到 ≥3 前缀/后缀 needle 方法）。这组必须有 ``://`` 之外的锚定能力。
GLOBAL_FAIL_CLOSED_URLS = [
    "redis:/user:Pr0d2024x@host",
    "redis:/user:Pr0d2024x@host:6379/0",
    "REDIS:/user:Pr0d2024x@host",
    "rediss:/:Pr0d2024x@host:6380/0",
    "user:Pr0d2024x@redis://host",
    "user:Pr0d2024x@rediss://host:6380/0",
    "user:Pr0d2024x@host:6379",
    # 空 authority 多写一个斜杠 / 口令含 '/' 的畸形态（r2 掩了、r3 漏了）
    "redis:///user:Pr0d2024x@host",
    "redis:/u@u:p/word@host:0/p/word",
]

#: r4 Finding 1 兜底层：口令在 userinfo 之外又被写了一遍（原文 / 编码 / path）。
RESIDUAL_REPEAT_URLS = [
    "redis://user:Pr0d2024x@host:6379/0?socket_timeout=Pr0d2024x",
    "redis://user:Pr0d2024x@host:6379/0?socket_timeout=%40Pr0d2024x",
    "redis://user:Pr0d2024x@host:6379/Pr0d2024x",
    "redis://user:Pr0d2024x@host:6379/0?db=Pr0d2024x",
]


def _redis_py_password(url) -> object:
    """``redis.connection.parse_url`` 真正会拿去连的口令；它解析不了时 None。

    测试的**接地**来源：不断言「我以为的口令」，只断言 redis-py 自己认的口令。
    """
    if not isinstance(url, str):
        return None
    try:
        return parse_url(url).get("password")
    except Exception:
        return None


def _partial_needles(secret: str) -> set:
    """口令的所有长度 ≥3 前缀/后缀——抓 r2 那种 ``ssword`` 式部分泄露。"""
    return {
        secret[i : i + length]
        for length in range(3, len(secret) + 1)
        for i in range(0, len(secret) - length + 1)
    }


def _exception_messages(traceback_text: str) -> str:
    """traceback 里**异常消息**的部分：剥掉 ``File "..."`` 源码路径。

    临时树路径（``.../worktrees/...``）可能恰好包含口令的 3 字子串；路径不是
    泄露面，真正的泄露面是异常消息与 ``from exc`` 的嵌套 cause 文案。
    """
    return "\n".join(
        line
        for line in traceback_text.splitlines()
        if not line.startswith('  File "') and line != "Traceback (most recent call last):"
    )


def _surfaces(exc: BaseException) -> dict:
    """异常在生产里的三个渲染面：``str`` / ``repr`` / ``traceback.format_exception``。

    后者的 ``File "..."`` 源码路径行被剥掉（见 ``_exception_messages``），
    ``from exc`` 的嵌套 cause 文案保留——它才是 r4 Finding 2 的泄露面。
    """
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return {"str": str(exc), "repr": repr(exc), "traceback-messages": _exception_messages(tb)}


def _effective(client: Redis) -> dict:
    """客户端 → 生效连接目标（``None`` 与「缺省」等价；端口缺省=6379）。"""
    kwargs = client.connection_pool.connection_kwargs
    return {
        "host": kwargs.get("host"),
        "port": kwargs.get("port") or 6379,
        "db": kwargs.get("db", 0),
        "username": kwargs.get("username"),
        "password": kwargs.get("password"),
        "path": kwargs.get("path"),
        "connection_class": client.connection_pool.connection_class.__name__,
    }


class TestParseAcceptedShapes(unittest.TestCase):
    """合法形态必须完整解析（端口/db/凭据/TLS/IPv6/unix）。"""

    def test_issue_reproduction_targets(self):
        """#88 复现命令里的三条：不得再出现 ('localhost', 6379, 0, None)。"""
        self.assertEqual(
            tuple(_parse_redis_url("redis://plaita-redis"))[:4],
            ("plaita-redis", 6379, 0, None),
        )
        self.assertEqual(
            tuple(_parse_redis_url("rediss://redis.internal:6380/0"))[:4],
            ("redis.internal", 6380, 0, None),
        )
        # username 必须透出，密码不得丢，host 不能再被用户名顶替
        user_pass = _parse_redis_url("redis://user:pass@host:6379/0")
        self.assertEqual(tuple(user_pass)[:4], ("host", 6379, 0, "pass"))
        self.assertEqual(user_pass.username, "user")

    def test_omitted_port_and_db_default(self):
        target = _parse_redis_url("redis://host")
        self.assertIsInstance(target, RedisTarget)
        self.assertEqual((target.host, target.port, target.db, target.password), ("host", 6379, 0, None))

    def test_db_from_path(self):
        self.assertEqual(_parse_redis_url("redis://host/3").db, 3)

    def test_password_only_userinfo(self):
        target = _parse_redis_url("redis://:pw@host:6379/2")
        self.assertEqual((target.host, target.db, target.password), ("host", 2, "pw"))
        self.assertIsNone(target.username)

    def test_username_and_password_userinfo(self):
        target = _parse_redis_url("redis://user:pw@host:6379/0")
        self.assertEqual((target.host, target.username, target.password), ("host", "user", "pw"))

    def test_rediss_keeps_endpoint_and_marks_ssl(self):
        target = _parse_redis_url("rediss://user:pw@host:6380/1")
        self.assertEqual(target.scheme, "rediss")
        self.assertTrue(target.ssl)
        self.assertEqual((target.host, target.port, target.db), ("host", 6380, 1))
        self.assertFalse(_parse_redis_url("redis://host:6379/0").ssl)

    def test_credentials_percent_decoded(self):
        target = _parse_redis_url("redis://user%40d:p%40ss%3Aword@host:6379/0")
        self.assertEqual((target.username, target.password), ("user@d", "p@ss:word"))

    def test_ipv6_literal(self):
        target = _parse_redis_url("redis://[::1]:6379/0")
        self.assertEqual((target.host, target.port, target.db), ("::1", 6379, 0))
        self.assertEqual(_parse_redis_url("rediss://[2001:db8::1]:6380/2").host, "2001:db8::1")

    def test_db_query_beats_path(self):
        self.assertEqual(_parse_redis_url("redis://host:6379/0?db=2").db, 2)

    def test_trailing_slash_is_default_db(self):
        self.assertEqual(_parse_redis_url("redis://host:6379/").db, 0)

    def test_unix_socket_path(self):
        target = _parse_redis_url("unix:///tmp/plaita-redis.sock")
        self.assertEqual(target.scheme, "unix")
        self.assertEqual(target.unix_socket_path, "/tmp/plaita-redis.sock")
        self.assertEqual(_parse_redis_url("unix:///tmp/plaita-redis.sock?db=1").db, 1)

    def test_accepted_shapes_agree_with_queue_parser(self):
        """解析结果必须与队列 ``Redis.from_url`` 同端点（否则又是两个 Redis）。"""
        for url in ACCEPTED:
            with self.subTest(url=url):
                target = _parse_redis_url(url)
                parsed = parse_url(url)  # 队列侧真正用的解析器
                if target.unix_socket_path is not None:
                    self.assertEqual(target.unix_socket_path, parsed["path"])
                else:
                    self.assertEqual(target.host, parsed["host"])
                    self.assertEqual(target.port, parsed.get("port", 6379))
                self.assertEqual(target.db, parsed.get("db", 0))
                self.assertEqual(target.username, parsed.get("username"))
                self.assertEqual(target.password, parsed.get("password"))


class TestAbsentUsesDocumentedDefault(unittest.TestCase):
    """「没配 Redis」必须保持既有行为：文档化默认，不是错误（#99/#95 先例）。"""

    def test_none_empty_and_whitespace(self):
        expected = _parse_redis_url(DEFAULT_REDIS_URL)
        for value in (None, "", "   ", "\t\n"):
            with self.subTest(value=value):
                self.assertEqual(_parse_redis_url(value), expected)

    def test_absent_matches_cli_default(self):
        self.assertEqual(
            _parse_redis_url(None).client_kwargs(),
            {"host": "localhost", "port": 6379, "db": 0},
        )

    def test_factory_without_redis_url_uses_default(self):
        for component_type in ("execution", "flow"):
            with self.subTest(component_type=component_type):
                storage = create_storage_component("redis", component_type, tenant_routing=True)
                pool = storage._current_storage().client.connection_pool.connection_kwargs
                self.assertEqual((pool.get("host"), pool.get("port") or 6379, pool.get("db", 0)),
                                 ("localhost", 6379, 0))


class TestMalformedIsNamedRefusal(unittest.TestCase):
    """畸形/不支持形态必须具名失败，绝不静默回落 localhost。"""

    def test_every_refused_shape_raises_named_error(self):
        for shape, url in REFUSED:
            with self.subTest(shape=shape, url=url):
                with self.assertRaises(RedisUrlError) as ctx:
                    _parse_redis_url(url)
                # 诊断仍要可用：消息里必须能认出是哪条 URL——但只允许**掩码**形态。
                # （r1 这里写的是 assertIn(url, ...)，等于把「原样回显口令」钉成契约。）
                self.assertIn(_mask_url(url), str(ctx.exception))

    def test_empty_target_raises(self):
        """#88 验收：``redis://`` 空目标抛异常而非返回默认值。"""
        with self.assertRaises(RedisUrlError):
            _parse_redis_url("redis://")

    def test_never_returns_localhost_for_malformed(self):
        for _shape, url in REFUSED:
            with self.subTest(url=url):
                try:
                    target = _parse_redis_url(url)
                except RedisUrlError:
                    continue
                self.assertNotEqual(
                    (target.host, target.port),
                    ("localhost", 6379),
                    f"畸形 URL {url!r} 静默回落 localhost",
                )

    def test_bytes_input_is_named_refusal(self):
        with self.assertRaises(RedisUrlError) as ctx:
            _parse_redis_url(b"redis://host:6379/0")
        self.assertIn("bytes", str(ctx.exception))

    def test_error_is_value_error_subclass(self):
        self.assertTrue(issubclass(RedisUrlError, ValueError))


class TestRefusalMessagesNeverLeakCredentials(unittest.TestCase):
    """plaita#88 r2 P0：拒绝消息会原样进生产日志，口令一个字都不许出现。

    ``flow_worker.py`` `logger.error("流程工作器启动失败: %s", e)`、
    ``event_filter.py`` `logger.error("...: %s", e, exc_info=True)`——后者连
    traceback（含 ``from exc`` 的嵌套 cause）一起落盘，所以三处都要钉。
    """

    def test_no_refusal_message_contains_the_password(self):
        # 不用 subTest：变异重放脚本只认 ``FAILED tests/...`` 行，SUBFAILED-only
        # 的死法它解析不出 killed-by（见 replay.py 的 detail 提取）。
        for shape, url, literal in LEAK_SHAPES:
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            exc = ctx.exception
            traceback_text = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            )
            rendered = {
                "str": str(exc),
                "repr": repr(exc),
                "traceback-messages": _exception_messages(traceback_text),
            }
            # 接地：redis-py 真正会用的口令（它自己解析不了的形态不参与）
            secrets = {literal}
            redis_py_password = _redis_py_password(url)
            if isinstance(redis_py_password, str) and redis_py_password:
                secrets.add(redis_py_password)
            secrets.discard("")
            for secret in secrets:
                for kind, text in rendered.items():
                    self.assertNotIn(secret, text, f"{shape}/{kind}: 口令原文泄露")
                    for needle in _partial_needles(secret):
                        self.assertNotIn(
                            needle, text, f"{shape}/{kind}: 口令片段 {needle!r} 泄露"
                        )
                self.assertNotIn(secret, traceback_text, f"{shape}: traceback 泄露口令")
            # 只掩码、不删光：诊断仍能认出是哪条 URL
            self.assertIn(_mask_url(url), str(exc), f"{shape}: 掩码形态丢失")
            self.assertIn(_REDACTED, str(exc), f"{shape}: 缺少掩码")

    def test_mask_helper_itself(self):
        self.assertEqual(
            _mask_url("redis://user:sup3rsecret@host:6379/0"),
            "redis://user:***@host:6379/0",
        )
        # 历史畸形：凭据里写了裸 '#'（urlsplit 会把它当 fragment），掩码也不能漏
        self.assertEqual(
            _mask_url("redis://:sup3rsecret#x@host:6379/0"),
            "redis://:***@host:6379/0",
        )
        # 没有凭据 → 原样；没有 scheme 的形态也要能掩码
        self.assertEqual(_mask_url("redis://host:6379/0"), "redis://host:6379/0")
        self.assertEqual(_mask_url("user:sup3rsecret@host:6379"), "user:***@host:6379")
        # r3 P0：authority 里**最后**一个 '@' 才是分隔符，口令整体（含 '@'/空白）抹掉
        self.assertEqual(
            _mask_url("redis://user:@Pr0d2024x@redis.internal:0"),
            "redis://user:***@redis.internal:0",
        )
        self.assertEqual(
            _mask_url("redis://user:p@ssword@redis.internal:0"),
            "redis://user:***@redis.internal:0",
        )
        self.assertEqual(
            _mask_url("redis://user:Super Secret@redis.internal:0"),
            "redis://user:***@redis.internal:0",
        )
        self.assertEqual(
            _mask_url("redis://user:Super\xa0Secret@redis.internal:0"),
            "redis://user:***@redis.internal:0",
        )

    def test_mask_helper_fails_closed(self):
        """r3：认不出 userinfo 也绝不原样返回——r2 正是「失败即返回原文」才漏的。"""
        # urlsplit 直接抛错的畸形 authority（redis-py 也解析不了）
        raw = "redis://[::1:user:Pr0d2024x@host/0"
        masked = _mask_url(raw)
        self.assertNotIn("Pr0d2024x", masked)
        self.assertIn("@host", masked)  # host 仍在，诊断不丢
        # 认不出用户名的形态：userinfo 整体抹成 ***，host 保留
        self.assertEqual(_mask_url("redis://user@host:6379"), "redis://***@host:6379")
        self.assertNotEqual(
            _mask_url("redis://user@host:6379"), "redis://user@host:6379"
        )

    def test_mask_helper_keeps_path_at_out_of_userinfo(self):
        """path/query 里的 '@' 不是 userinfo 分隔符——不得误抹 host。"""
        self.assertEqual(
            _mask_url("redis://host:6379/0@x"), "redis://host:6379/0@x"
        )
        self.assertEqual(
            _mask_url("redis://host:6379/0?ref=a@b"), "redis://host:6379/0?ref=a@b"
        )

    def test_nested_cause_is_scrubbed_too(self):
        """嵌套 cause：底层异常文案里若带裸 URL/口令，也必须先洗掉再进 traceback。"""
        text = "redis://user:sup3rsecret@host:6379/0?db=x"
        embedded = factory._safe_reason(ValueError(f"bad value in {text}"), text)
        self.assertNotIn(SECRET, embedded)
        self.assertIn("redis://user:***@host:6379/0?db=x", embedded)
        # 只带口令字面量（没带整条 URL）也要抹掉
        bare = factory._safe_reason(ValueError(f"bad password {SECRET}"), text)
        self.assertNotIn(SECRET, bare)
        # r3：带 '@' 的完整口令，_passwords 也必须整体取出并洗掉（r2 正则只取到一截）
        at_text = "redis://user:@Pr0d2024x@redis.internal:0"
        scrubbed = factory._safe_reason(ValueError("port needs @Pr0d2024x"), at_text)
        self.assertNotIn("@Pr0d2024x", scrubbed)

    def test_unknown_query_param_warning_is_masked(self):
        """未知 query 只警告不判死——警告本身也是日志，同样不许带口令。"""
        with self.assertLogs("plaita.server.factory", level="WARNING") as logs:
            target = _parse_redis_url("redis://user:sup3rsecret@host:6379/0?foo=bar")
        self.assertEqual((target.host, target.port), ("host", 6379))
        self.assertTrue(any("foo" in line for line in logs.output))
        for line in logs.output:
            self.assertNotIn(SECRET, line)
            self.assertIn("redis://user:***@host:6379/0", line)


class TestMaskerCannotLeakByConstruction(unittest.TestCase):
    """plaita#88 r4 P0：掩码不再依赖「输入是合法 URL」。

    r3 只认字面 ``://``、只替换一次、且把 ``__cause__`` 原样留给 traceback；
    这里把三条漏点（全局 fail-closed / 兜底重洗 / cause 替身）都钉在
    ``str`` / ``repr`` / ``traceback.format_exception`` 三个渲染面上。
    """

    def test_global_fail_closed_rule_table(self):
        """锚点是「凭据形状的最后一个 '@'」，不依赖字面 '://'。"""
        expected = {
            "redis:/user:pw@host": "redis:/user:***@host",
            "redis:/user:pw@host:6379/0": "redis:/user:***@host:6379/0",
            "REDIS:/user:pw@host": "REDIS:/user:***@host",
            "user:pw@redis://host": "user:***@redis://host",
            "user:pw@rediss://host:6380/0": "user:***@rediss://host:6380/0",
            # 空 authority 多写一个斜杠 / 口令含 '#'、'?'
            "redis:///user:pw@host": "redis:///user:***@host",
            "redis:///:p#w@host": "redis:///:***@host",
            "redis:///:p?w@host": "redis:///:***@host",
            # 口令本身含 '/'：结构定位只能整段前缀当凭据（只多掩不少掩）
            "redis:/u@u:p/w@/0@x": "redis:***@x",
            # keep：path/query 的 '@' 不是 userinfo
            "redis://host:6379/0@x": "redis://host:6379/0@x",
            "redis://host:6379/0?ref=a@b": "redis://host:6379/0?ref=a@b",
            "redis://host/0?x=1:2@y": "redis://host/0?x=1:2@y",
        }
        for url, want in expected.items():
            self.assertEqual(_mask_url(url), want, url)

    def test_global_fail_closed_never_returns_input_verbatim(self):
        for url in GLOBAL_FAIL_CLOSED_URLS:
            masked = _mask_url(url)
            self.assertNotEqual(masked, url, url)
            self.assertNotIn("Pr0d2024x", masked, url)
            self.assertNotIn("p/word", masked, url)
            for needle in _partial_needles("Pr0d2024x") | _partial_needles("p/word"):
                self.assertNotIn(needle, masked, f"{url}: {needle!r}")

    def test_global_fail_closed_message_surfaces(self):
        for url in GLOBAL_FAIL_CLOSED_URLS:
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            exc = ctx.exception
            self.assertIn(_mask_url(url), str(exc), url)
            for kind, text in _surfaces(exc).items():
                for secret in ("Pr0d2024x", "p/word"):
                    self.assertNotIn(secret, text, f"{url}/{kind}")
                    for needle in _partial_needles(secret):
                        self.assertNotIn(needle, text, f"{url}/{kind}/{needle}")

    def test_residual_repeat_in_query_or_path_is_redacted(self):
        for url in RESIDUAL_REPEAT_URLS:
            masked = _mask_url(url)
            self.assertNotIn("Pr0d2024x", masked, url)
            for needle in _partial_needles("Pr0d2024x"):
                self.assertNotIn(needle, masked, f"{url}: {needle!r}")
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            exc = ctx.exception
            self.assertIn(masked, str(exc), url)
            for kind, text in _surfaces(exc).items():
                for needle in _partial_needles("Pr0d2024x"):
                    self.assertNotIn(needle, text, f"{url}/{kind}/{needle}")

    def test_cause_object_is_independently_sanitized(self):
        """r4 Finding 2：``from exc`` 的 ``__cause__`` 是 traceback 独立渲染的对象。"""
        url = "redis://user:@Pr0d2024x@host:6379/0?socket_timeout=%40Pr0d2024x"
        with self.assertRaises(RedisUrlError) as ctx:
            _parse_redis_url(url)
        exc = ctx.exception
        chain = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        # 链仍在（诊断价值），但底层文案换成了洗过的同类型替身
        self.assertIsNotNone(exc.__cause__, "cause 链不该被丢掉")
        self.assertIn("socket_timeout", chain)
        self.assertIn(_mask_url(url), str(exc))
        for text in (chain, str(exc.__cause__), repr(exc.__cause__)):
            for needle in ("@Pr0d2024x", "%40Pr0d2024x", "Pr0d2024x"):
                self.assertNotIn(needle, text, f"cause 链泄露 {needle!r}")
        self.assertIn("float", chain.lower())

    def test_message_fields_that_are_not_urls_are_scrubbed(self):
        """``?db=<口令>`` / ``/<口令>`` / ``?<口令>=1`` 这些字段也是消息面。"""
        for url in (
            "redis://user:Pr0d2024x@host:6379/0?db=Pr0d2024x",
            "redis://user:Pr0d2024x@host:6379/Pr0d2024x",
        ):
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            for kind, text in _surfaces(ctx.exception).items():
                self.assertNotIn("Pr0d2024x", text, f"{url}/{kind}")
                for needle in _partial_needles("Pr0d2024x"):
                    self.assertNotIn(needle, text, f"{url}/{kind}/{needle}")
        # 口令写在 query **名**里：未知参数只警告不判死——warning 同样是日志面
        with self.assertLogs("plaita.server.factory", level="WARNING") as logs:
            target = _parse_redis_url("redis://user:Pr0d2024x@host:6379/0?Pr0d2024x=1")
        self.assertEqual((target.host, target.port), ("host", 6379))
        for line in logs.output:
            self.assertNotIn("Pr0d2024x", line)
            for needle in _partial_needles("Pr0d2024x"):
                self.assertNotIn(needle, line)

    def test_password_substring_of_scheme_or_host_is_not_a_false_red(self):
        """口令恰好是 scheme/host 的子串：朴素 needle 检查给的是**假红**。

        兜底重洗只作用于 userinfo 段与 host 之后的 path/query/fragment（见
        ``_redact_residuals``）——scheme/host 是结构、也是诊断信息，不参与替换，
        否则口令 ``redis`` 会把 scheme 抹成 ``***://``，且没有任何安全收益。
        """
        url = "redis://user:redis@redis.example.com:6379/0"
        masked = _mask_url(url)
        self.assertEqual(masked, "redis://user:***@redis.example.com:6379/0")
        self.assertNotIn(":redis@", masked)
        self.assertIn("@redis.example.com:6379/0", masked)
        # 同一个 needle 在**无凭据**的同类 URL 里也在 ⇒ needle 命中不等于泄露
        self.assertIn("redis", _mask_url("redis://redis.example.com:6379/0"))

    def test_masker_is_linear_on_long_input(self):
        """~70k 字符：口令 70k 且在 query 里重复一遍，掩码必须一趟做完。

        （任何退化成回溯的实现都会在这里挂死；实现只做 O(n) 的 find/rfind/replace。）
        """
        secret = "P" * 69990 + " Secret"
        url = f"redis://user:{secret}@host:6379/0?socket_timeout={secret}"
        self.assertEqual(
            _mask_url(url), "redis://user:***@host:6379/0?socket_timeout=***"
        )
        self.assertNotIn(secret, _mask_url(url))


class TestQueryOptionsPassThrough(unittest.TestCase):
    """r2：「redis-py 认的 query 参数」必须真的进客户端 kwargs，不再一律拒绝。"""

    #: 透传后能在存储客户端 ``connection_kwargs()`` 里直接观察到的参数
    PASSTHROUGH = [
        ("socket_timeout", "redis://user:pw@host:6379/0?socket_timeout=5"),
        ("socket_connect_timeout", "redis://host:6379/0?socket_connect_timeout=1.5"),
        ("socket_keepalive", "redis://host:6379/0?socket_keepalive=true"),
        ("socket_read_size", "redis://host:6379/0?socket_read_size=32768"),
        ("health_check_interval", "redis://host:6379/0?health_check_interval=30"),
        ("protocol", "redis://host:6379/0?protocol=2"),
        ("legacy_responses", "redis://host:6379/0?legacy_responses=true"),
        ("retry_on_error", "redis://host:6379/0?retry_on_error=ConnectionError"),
        ("ssl_min_version", "rediss://host:6380/0?ssl_min_version=771"),
        ("ssl_check_hostname", "rediss://host:6380/0?ssl_check_hostname=false"),
        (
            "ssl_include_verify_flags",
            "rediss://host:6380/0?ssl_include_verify_flags=VERIFY_X509_PARTIAL_CHAIN",
        ),
        (
            "ssl_exclude_verify_flags",
            "rediss://host:6380/0?ssl_exclude_verify_flags=VERIFY_X509_STRICT",
        ),
    ]

    #: 客户端/Pool 自己消化：值不在 ``connection_kwargs``，但确实生效
    CONSUMED_ELSEWHERE = [
        ("max_connections", "redis://host:6379/0?max_connections=10"),
        ("retry_on_timeout", "redis://host:6379/0?retry_on_timeout=true"),
    ]

    def _storage_client(self, url: str) -> Redis:
        storage = create_storage_component(
            "redis", "execution", tenant_routing=True, redis_url=url
        )
        return storage._current_storage().client

    def test_supported_query_params_reach_client_kwargs(self):
        for param, url in self.PASSTHROUGH:
            with self.subTest(param=param):
                kwargs = self._storage_client(url).connection_pool.connection_kwargs
                queue = Redis.from_url(url).connection_pool.connection_kwargs
                self.assertIn(param, kwargs)
                # 与队列侧逐值一致（含 redis-py 的 int/float/bool/VerifyFlags 转换）
                self.assertEqual(kwargs[param], queue[param])

    def test_consumed_query_params_still_build_a_client(self):
        for param, url in self.CONSUMED_ELSEWHERE:
            with self.subTest(param=param):
                self.assertIsNotNone(self._storage_client(url))

    def test_max_connections_reaches_the_pool(self):
        client = self._storage_client("redis://host:6379/0?max_connections=10")
        self.assertEqual(client.connection_pool.max_connections, 10)

    def test_combined_production_url_no_longer_crashes(self):
        """验证者实测的生产形态：``?socket_timeout=5&health_check_interval=30``。"""
        target = _parse_redis_url(
            "redis://user:sup3rsecret@host:6379/0?socket_timeout=5&health_check_interval=30"
        )
        self.assertEqual(target.query_options["socket_timeout"], 5.0)
        self.assertEqual(target.query_options["health_check_interval"], 30)

    def test_unknown_query_param_is_ignored_not_fatal(self):
        target = _parse_redis_url("redis://host:6379/0?foo=bar")
        self.assertEqual((target.host, target.port, target.db), ("host", 6379, 0))
        # 未知名字无法透传（redis.Redis.__init__ 没有 **kwargs），但不判死
        self.assertNotIn("foo", target.client_kwargs())

    def test_invalid_value_for_known_param_is_still_refused(self):
        with self.assertRaises(RedisUrlError) as ctx:
            _parse_redis_url("redis://host:6379/0?ssl_min_version=abc")
        self.assertIn("ssl_min_version", str(ctx.exception))

    def test_passthrough_table_covers_every_redis_py_query_parser(self):
        """枚举不许漂移：本模块认的参数名必须与 redis-py 的表**逐名**对齐。

        redis-py 将来新增 query 参数时这条会红，提醒把它加进 ``_QUERY_PARAMS``
        （透传）或 ``_UNSUPPORTED_QUERY_PARAMS``（具名拒绝）。
        """
        from redis.connection import URL_QUERY_ARGUMENT_PARSERS

        ours = set(factory._QUERY_PARAMS) | {"db"} | set(factory._UNSUPPORTED_QUERY_PARAMS)
        self.assertEqual(ours, set(URL_QUERY_ARGUMENT_PARSERS))


class TestFactoryCallerBehaviour(unittest.TestCase):
    """入口行为：解析器是 execution/flow 存储连接参数的唯一来源。"""

    def test_execution_and_flow_go_through_parser(self):
        calls = []
        original = factory._parse_redis_url

        def spy(url=None):
            calls.append(url)
            return original(url)

        factory._parse_redis_url = spy
        try:
            create_storage_component("redis", "execution", tenant_routing=True, redis_url="redis://h1:6379/0")
            create_storage_component("redis", "flow", tenant_routing=True, redis_url="redis://h2:6379/0")
        finally:
            factory._parse_redis_url = original
        self.assertEqual(calls, ["redis://h1:6379/0", "redis://h2:6379/0"])

    def test_malformed_refused_before_any_client_is_built(self):
        """具名失败发生在建客户端之前（fail-closed，不是连接期的偶然报错）。"""
        import plaita.storage.redis as storage_mod

        built = []
        real_client = storage_mod.redis.Redis

        def spy(*args, **kwargs):
            built.append(kwargs)
            return real_client(*args, **kwargs)

        storage_mod.redis.Redis = spy
        try:
            for url in ("http://h:6379", "redis://", "redis://host:notaport"):
                for component_type in ("execution", "flow"):
                    with self.subTest(url=url, component_type=component_type):
                        with self.assertRaises(RedisUrlError):
                            create_storage_component(
                                "redis", component_type, redis_url=url
                            )
        finally:
            storage_mod.redis.Redis = real_client
        self.assertEqual(built, [])

    def test_storage_and_queue_end_up_on_same_endpoint(self):
        """同一个 URL → 同一个端点：存储侧客户端与队列 ``Redis.from_url`` 等价。"""
        for url in ACCEPTED:
            with self.subTest(url=url):
                storage = create_storage_component(
                    "redis", "execution", tenant_routing=True, redis_url=url
                )
                inner = storage._current_storage()
                self.assertEqual(_effective(inner.client), _effective(Redis.from_url(url)))

    def test_no_more_second_endpoint_for_rediss(self):
        """历史缺陷：``rediss://redis.internal:6380/0`` 的存储侧被改写成明文 localhost。"""
        storage = create_storage_component(
            "redis", "flow", tenant_routing=True, redis_url="rediss://redis.internal:6380/0"
        )
        pool = storage._current_storage().client.connection_pool
        self.assertEqual(pool.connection_kwargs["host"], "redis.internal")
        self.assertEqual(pool.connection_kwargs["port"], 6380)
        self.assertEqual(pool.connection_class.__name__, "SSLConnection")

    def test_non_tenant_execution_and_flow_storage(self):
        """非租户路由路径（库内直接调用）同样走解析器、同端点。"""
        from plaita.storage.redis import RedisExecutionStorage, RedisFlowStorage

        for component_type, cls in (("execution", RedisExecutionStorage), ("flow", RedisFlowStorage)):
            with self.subTest(component_type=component_type):
                url = "redis://user:pw@host:6380/4"
                storage = create_storage_component("redis", component_type, redis_url=url)
                self.assertIsInstance(storage, cls)
                self.assertEqual(_effective(storage.client), _effective(Redis.from_url(url)))

    def test_subscription_branch_untouched(self):
        """边界：subscription 分支本来就走原始 URL（redis-py 解析），不在本单范围。"""
        from plaita.event.redis import RedisEventSubscriptionStorage

        storage = create_storage_component(
            "redis", "subscription", redis_url="redis://host:6379/0"
        )
        self.assertIsInstance(storage, RedisEventSubscriptionStorage)
        self.assertEqual(storage.redis_url, "redis://host:6379/0")


class TestSingleParser(unittest.TestCase):
    """全仓只有一个 Redis URL 解析器（#88 的根因是存储侧另有一套）。"""

    def test_parse_redis_url_defined_once(self):
        import pathlib

        root = pathlib.Path(__file__).resolve().parents[2] / "plaita"
        hits = [
            path
            for path in root.rglob("*.py")
            if "def _parse_redis_url" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual([p.name for p in hits], ["factory.py"])


if __name__ == "__main__":
    unittest.main()
