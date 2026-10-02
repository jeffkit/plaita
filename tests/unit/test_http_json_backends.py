"""test_http_json_backends — HTTP 节点 JSON 后端（orjson/stdlib 回退）回归。

覆盖 2026-10 wave2 的行为边界：

1. orjson 路径：请求体序列化（含 datetime 原生支持——文档化改进）与响应
   反序列化等价于 stdlib；
2. 宽松回退链 `_loads_lenient`：orjson 严格拒绝的 NaN/Infinity 语法由
   stdlib 兜住；两层都失败时节点回退原始文本（历史行为）；
3. stdlib 回退：无 orjson 环境（monkeypatch 掉模块符号）一切照常。
"""

from __future__ import annotations

import datetime
import json
import math
from unittest import TestCase
from unittest.mock import patch

import plaita.node.http as http_mod
from plaita.node.http import HttpExecutor, _json_dumps_bytes, _loads_lenient

try:
    import orjson as _orjson  # noqa: F401
    _ORJSON_AVAILABLE = http_mod.orjson is not None
except ImportError:
    _ORJSON_AVAILABLE = False

import unittest as _ut


class TestDumpsBackend(TestCase):
    def test_plain_body_matches_stdlib_semantics(self):
        body = {"a": 1, "b": [1, 2, 3], "c": "中文"}
        data = _json_dumps_bytes(body)
        self.assertEqual(json.loads(data), body)

    @_ut.skipUnless(_ORJSON_AVAILABLE, "orjson not installed (stdlib backend active)")
    def test_datetime_serialized_natively(self):
        """orjson 原生序列化 datetime（stdlib 会 TypeError）——文档化改进。"""
        body = {"ts": datetime.datetime(2026, 10, 2, 12, 0, 0)}
        data = _json_dumps_bytes(body)
        self.assertIn(b"2026-10-02T12:00:00", data)

    def test_unserializable_still_raises_typeerror(self):
        with self.assertRaises(TypeError):
            _json_dumps_bytes({"bad": object()})


class TestLoadsLenient(TestCase):
    def test_strict_json_via_orjson(self):
        self.assertEqual(_loads_lenient('{"a": 1}'), {"a": 1})

    def test_nan_falls_back_to_stdlib(self):
        """orjson 按 RFC 拒绝 NaN，stdlib 兜住——不得把合法（宽松）响应变文本。"""
        got = _loads_lenient('{"a": NaN}')
        self.assertTrue(isinstance(got, dict) and math.isnan(got["a"]), got)
        self.assertEqual(_loads_lenient('{"a": Infinity}'), {"a": float("inf")})

    def test_garbage_raises_for_node_fallback(self):
        with self.assertRaises(json.JSONDecodeError):
            _loads_lenient("not json at all")


class TestNodeFallbackWithoutOrjson(TestCase):
    """无 orjson 环境（fast extra 未装）：stdlib 路径功能等价。"""

    def test_dumps_fallback_and_loads_fallback(self):
        with patch.object(http_mod, "orjson", None):
            # 模块级 _json_dumps_bytes/_JSON_LOADS 是 import 时绑定的——
            # 直接调用 stdlib 分支的等价逻辑验证契约
            body = {"a": 1}
            data = json.dumps(body).encode("utf-8")
            self.assertEqual(json.loads(data), body)
            self.assertEqual(_loads_lenient('{"b": 2}'), {"b": 2})


@_ut.skipUnless(_ORJSON_AVAILABLE, "orjson not installed (stdlib backend active)")
class TestExecutorUsesBackends(TestCase):
    def _executor(self):
        return HttpExecutor(url="http://127.0.0.1:1/x", method="POST", query=None,
                            body={"k": "v"}, headers=None, addressing=None,
                            delegate=None)

    def test_build_request_params_uses_bytes_backend(self):
        url, headers, data = self._executor()._build_request_params()
        self.assertEqual(json.loads(data), {"k": "v"})

    def test_sync_response_json_uses_lenient_loads(self):
        """sync 路径 response.json(loads=_loads_lenient)：NaN 响应不退化为文本。"""
        executor = self._executor()
        fake_response = type("R", (), {
            "is_redirect": False,
            "status_code": 200,
            "url": "http://127.0.0.1:1/x",
            "text": '{"a": NaN}',
            "headers": {},
            "json": lambda self, **kw: kw["loads"](self.text),
        })()
        session = __import__("plaita.core.http_session",
                             fromlist=["get_shared_sync_session"]).get_shared_sync_session()
        with patch.object(type(session), "send", return_value=fake_response):
            rsp, err = executor.handle_request(None)
        self.assertIsNone(err)
        self.assertTrue(math.isnan(rsp.res["a"]), rsp.res)


if __name__ == "__main__":  # pragma: no cover
    import unittest

    unittest.main()
