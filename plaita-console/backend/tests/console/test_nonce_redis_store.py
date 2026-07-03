"""P0-3 回归: ``RedisNonceStore`` 跨 worker 重放保护 + ``NonceStore`` 抽象。

用进程内 ``_FakeRedis`` 模拟 ``redis.Redis`` 的 ``SET key NX EX ttl`` 原子语义
与 ``scan_iter``/``delete``, 不引入 fakeredis 依赖。验证:
- RedisNonceStore 首次 nonce 放行, 重放拒绝;
- 不同 nonce 各自一次性;
- ``enable_replay_protection`` 在无 redis_url 时 warning、有 redis_url 时配 Redis store;
- ``verify_authorization`` 可注入自定义 store。
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from time import time
from typing import Dict

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import signature  # noqa: E402
from plaita.client import generate_signature  # noqa: E402

SECRET_ID = "test-id"
SECRET_KEY = "test-secret-key"


class _FakeRedis:
    """极简 in-process fake, 只实现 RedisNonceStore 用到的 3 个操作。

    ``set(... nx=True, ex=ttl)``: 仅当 key 不存在时设置并标记过期时间, 返回 True;
    已存在返回 None。``scan_iter`` / ``delete`` 用于 reset。
    """

    def __init__(self) -> None:
        self._data: Dict[str, str] = {}
        self._expire: Dict[str, float] = {}

    def set(self, key, value, nx=False, ex=None):
        import time as _time
        now = _time.time()
        # 惰性过期
        for k in list(self._expire):
            if self._expire[k] <= now:
                self._data.pop(k, None)
                self._expire.pop(k, None)
        if nx and key in self._data:
            return None
        self._data[key] = value
        if ex is not None:
            self._expire[key] = now + ex
        return True

    def scan_iter(self, pattern):
        # pattern 形如 "plaita:nonce:*"
        prefix = pattern.rstrip("*")
        for k in list(self._data):
            if k.startswith(prefix):
                yield k

    def delete(self, *keys):
        n = 0
        for k in keys:
            if k in self._data:
                del self._data[k]
                self._expire.pop(k, None)
                n += 1
        return n


@pytest.fixture
def redis_store(monkeypatch):
    """构造一个用 _FakeRedis 的 RedisNonceStore, 注入 redis 模块。"""
    fake = _FakeRedis()

    class _RedisStub:
        @staticmethod
        def from_url(url):
            return fake

    monkeypatch.setattr(signature, "redis", _RedisStub, raising=False)
    # RedisNonceStore 构造里 ``import redis`` 取的是真模块; 改成 monkeypatch sys.modules
    monkeypatch.setitem(sys.modules, "redis", type("R", (), {"Redis": _RedisStub}))
    store = signature.RedisNonceStore("redis://localhost:0")
    return store, fake


def _make_auth(nonce=None, validity=3, when=None):
    return generate_signature(SECRET_KEY, SECRET_ID, validity, int(when or time()),
                              nonce=nonce)


class TestRedisNonceStore:
    def test_first_use_passes_replay_rejected(self, redis_store):
        store, _ = redis_store
        now = time()
        expire = now + 3
        assert store.check_and_record("n1", expire) is True
        assert store.check_and_record("n1", expire) is False  # 重放

    def test_different_nonces_each_valid_once(self, redis_store):
        store, _ = redis_store
        expire = time() + 3
        assert store.check_and_record("a", expire) is True
        assert store.check_and_record("b", expire) is True
        assert store.check_and_record("a", expire) is False
        assert store.check_and_record("b", expire) is False

    def test_expired_nonce_ttl_drops_key(self, redis_store):
        store, fake = redis_store
        # ttl=1, 等过期后同一 nonce 应可再次设置 (TTL 已过)
        assert store.check_and_record("eph", time() + 1) is True
        # 模拟过期: 直接清 expire 标记并等 fake 惰性清理
        fake._expire["plaita:nonce:eph"] = time() - 0.01
        # 再次 set nx, 因惰性清理把过期 key 清掉, 应能重新设置
        assert store.check_and_record("eph", time() + 1) is True


class TestVerifyAuthorizationWithInjectedStore:
    def test_injected_redis_store_blocks_replay(self, redis_store):
        store, _ = redis_store
        auth = _make_auth(nonce="uuid-1")
        # 首次放行
        assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY,
                                              nonce_store=store) is True
        # 重放拒绝 (同一 store 记录了 nonce)
        assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY,
                                              nonce_store=store) is False

    def test_default_store_still_used_when_not_injected(self):
        signature.reset_nonce_cache()
        auth = _make_auth(nonce="uuid-default")
        assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY) is True
        assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY) is False


class TestEnableReplayProtection:
    def test_warning_when_no_redis_url(self, caplog):
        with caplog.at_level(logging.WARNING, logger="services.signature"):
            signature.enable_replay_protection(redis_url=None)
        assert any("IN-MEMORY" in r.message for r in caplog.records)

    def test_redis_store_configured_when_url_given(self, monkeypatch, caplog):
        monkeypatch.setitem(sys.modules, "redis",
                            type("R", (), {"Redis": type("C", (), {"from_url": staticmethod(lambda u: _FakeRedis())})}))
        with caplog.at_level(logging.INFO, logger="services.signature"):
            signature.enable_replay_protection(redis_url="redis://localhost:0")
        assert any("multi-worker safe" in r.message for r in caplog.records)

        # 配置后默认 store 应是 RedisNonceStore; 验证一次 nonce 流程
        signature.reset_nonce_cache()
        auth = _make_auth(nonce="uuid-cfg")
        assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY) is True
        assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY) is False
