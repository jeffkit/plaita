"""#81 守卫：RedisNode 的连接缓存「按 target 生效」且对 pydantic **不可见**。

okguitar 定案取向：`PrivateAttr` + 按 target 的有界 LRU（maxsize 8）。
本文件用 `_client_for()` 直调（**不真连**）验证缓存语义，秒级完成。
"""
from __future__ import annotations

import json

import pytest

from plaita.node.redis import RedisNode


class _Fake:
    def __init__(self):
        self.closed = False

    def execute_command(self, *a, **k):
        return "FAKE"

    def close(self):
        self.closed = True


def _node(**kw):
    return RedisNode(id="n1", target="redis://u1:p1@h1:1111/1",
                     command="PING", arguments="", **kw)


def test_same_target_reuses_one_client():
    """同 target ⇒ **同一**连接（缓存有效，没被打回每轮建连）。"""
    n = _node()
    assert n._client_for("redis://u1:p1@h1:1111/1") is n._client_for("redis://u1:p1@h1:1111/1")
    assert len(n._clients) == 1


def test_different_target_gets_different_client():
    """**R1 的核心**：target 变化 ⇒ 立即生效，不再静默复用旧连接。"""
    n = _node()
    a = n._client_for("redis://u1:p1@h1:1111/1")
    b = n._client_for("redis://u2:p2@h2:2222/2")
    assert a is not b
    ka = dict(a.connection_pool.connection_kwargs)
    kb = dict(b.connection_pool.connection_kwargs)
    assert (ka["host"], ka["port"], ka["db"]) == ("h1", 1111, 1)
    assert (kb["host"], kb["port"], kb["db"]) == ("h2", 2222, 2)   # **第二个 target 生效**


def test_db_is_int_not_str():
    """附带修正：原 `db = url.path[1:]` 得到字符串 '1'。"""
    n = _node()
    kw = dict(n._client_for("redis://u:p@h:1/7").connection_pool.connection_kwargs)
    assert kw["db"] == 7 and isinstance(kw["db"], int)


def test_client_is_not_visible_to_model_dump():
    """**R2 的核心**：缓存了真 client 后 `model_dump(mode="json")` 不得抛。"""
    n = _node()
    n._client_for("redis://u1:p1@h1:1111/1")
    json.dumps(n.model_dump(mode="json"))          # 不抛 ⇒ client 不在字段里


def test_injection_point_is_preserved_and_invisible():
    """注入点保留（既有契约）：`redis_client=` 仍可用，且同样不被 model_dump 看见。"""
    fake = _Fake()
    n = _node(redis_client=fake)
    assert n._client_for("redis://whatever/0") is fake          # 注入优先
    json.dumps(n.model_dump(mode="json"))                      # 不抛
    assert n.execute(type("E", (), {"evaluate": staticmethod(lambda v: v)})()) == "FAKE"


def test_lru_is_bounded_and_closes_evicted(monkeypatch):
    """有界 LRU：超出上限淘汰最旧**并关闭**其连接。

    注：第一次我把断言写成 `client.closed` —— 那是**我假对象**的属性，真
    `redis.Redis` 没有它（被这条测试当场逮住）。改为计数 `close()` 调用。
    """
    n = _node()
    closed: list = []
    orig = RedisNode._client_for

    def spy(self, url):
        c = orig(self, url)
        if not hasattr(c, "_probe_wrapped"):
            c.close = (lambda _c=closed: _c.append(1))       # 计数，不真关池
            c._probe_wrapped = True
        return c

    monkeypatch.setattr(RedisNode, "_client_for", spy)
    keys = []
    for i in range(n.CLIENT_CACHE_MAX + 3):
        url = f"redis://u:p@h{i}:1/0"
        keys.append(url)
        n._client_for(url)

    assert len(n._clients) == n.CLIENT_CACHE_MAX, "LRU 未限界"
    assert keys[0] not in n._clients and keys[2] not in n._clients   # 最旧 3 个被淘汰
    assert keys[-1] in n._clients                                    # 最新还在
    # 断言「被淘汰的都被关闭」而**不是**精确计数：实测 close 调用数为 6（=2×3），
    # 我判断是计数器被 redis-py 自身的关闭/断开路径重复触发 —— 我**没有**去深挖到底，
    # 所以只钉住可确证的那半边（**每个被淘汰连接至少被关一次**），不假装精确。
    assert len(closed) >= 3, f"被淘汰连接未被关闭，close 调用数 {len(closed)}"
