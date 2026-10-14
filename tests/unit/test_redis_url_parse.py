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
它会被 ``flow_worker`` / ``event_filter`` 原样写进生产日志；本文件用
``LEAK_SHAPES`` 表钉死「口令不出现在 ``str``/``repr``/traceback 任何一处」。
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

#: 带凭据的拒绝形态（plaita#88 r2 P0）：消息会进生产日志，口令一个字都不能出现。
SECRET = "sup3rsecret"
LEAK_SHAPES = [
    ("non-str bytes", b"redis://user:sup3rsecret@host:6379/0"),
    ("unsupported scheme", "REDIS://user:sup3rsecret@host:6379/0"),
    ("missing scheme", "user:sup3rsecret@host:6379"),
    ("wrong scheme", "http://user:sup3rsecret@host:6379/0"),
    ("bad db query", "redis://user:sup3rsecret@host:6379/0?db=x"),
    ("negative db query", "redis://user:sup3rsecret@host:6379/0?db=-1"),
    ("bad db path", "redis://user:sup3rsecret@host:6379/foo"),
    ("invalid query value", "redis://user:sup3rsecret@host:6379/0?socket_timeout=abc"),
    ("timeout query param", "redis://user:sup3rsecret@host:6379/0?timeout=3"),
    ("fragment", "redis://user:sup3rsecret@host:6379/0#frag"),
    ("invalid IPv6 literal", "redis://user:sup3rsecret@[::1:6379/0"),
    ("port non-numeric", "redis://user:sup3rsecret@host:notaport"),
    ("port out of range", "redis://user:sup3rsecret@host:99999"),
    ("port zero", "redis://user:sup3rsecret@host:0"),
    ("missing host", "redis://user:sup3rsecret@/3"),
    ("unix without socket path", "unix://user:sup3rsecret@"),
]


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
        for shape, url in LEAK_SHAPES:
            with self.subTest(shape=shape):
                with self.assertRaises(RedisUrlError) as ctx:
                    _parse_redis_url(url)
                exc = ctx.exception
                rendered = {
                    "str": str(exc),
                    "repr": repr(exc),
                    "traceback": "".join(
                        traceback.format_exception(type(exc), exc, exc.__traceback__)
                    ),
                }
                for kind, text in rendered.items():
                    with self.subTest(shape=shape, kind=kind):
                        self.assertNotIn(SECRET, text)
                # 只掩码、不删光：诊断仍能认出是哪条 URL
                self.assertIn(_mask_url(url), str(exc))
                self.assertIn(_REDACTED, str(exc))

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

    def test_nested_cause_is_scrubbed_too(self):
        """嵌套 cause：底层异常文案里若带裸 URL/口令，也必须先洗掉再进 traceback。"""
        text = "redis://user:sup3rsecret@host:6379/0?db=x"
        embedded = factory._safe_reason(ValueError(f"bad value in {text}"), text)
        self.assertNotIn(SECRET, embedded)
        self.assertIn("redis://user:***@host:6379/0?db=x", embedded)
        # 只带口令字面量（没带整条 URL）也要抹掉
        bare = factory._safe_reason(ValueError(f"bad password {SECRET}"), text)
        self.assertNotIn(SECRET, bare)

    def test_unknown_query_param_warning_is_masked(self):
        """未知 query 只警告不判死——警告本身也是日志，同样不许带口令。"""
        with self.assertLogs("plaita.server.factory", level="WARNING") as logs:
            target = _parse_redis_url("redis://user:sup3rsecret@host:6379/0?foo=bar")
        self.assertEqual((target.host, target.port), ("host", 6379))
        self.assertTrue(any("foo" in line for line in logs.output))
        for line in logs.output:
            self.assertNotIn(SECRET, line)
            self.assertIn("redis://user:***@host:6379/0", line)


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
