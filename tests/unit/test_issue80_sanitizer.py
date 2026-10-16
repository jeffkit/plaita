"""#80 补批守卫：worker 侧 sanitizer 的契约（okguitar 11:23 口径 ③）。

层 1 = `sanitize_context_for_persist`：把不可 JSON 化的值降级为 str，
返回**副本**（绝不就地改原对象）；连兜底都失败（循环引用）⇒ 返回 None，
由调用方走层 2（最小 error 状态）。

要点：**sanitizer 自身对『循环引用』与『__str__ 抛异常的对象』都不能抛。**
"""
from __future__ import annotations

import json

from plaita.server.flow_worker import sanitize_context_for_persist


def test_non_json_value_is_coerced_to_str():
    ctx = {"$NODE": {"n1": {"result": {1, 2, 3}}}}          # set 不可 JSON
    out = sanitize_context_for_persist(ctx)
    assert out is not None
    json.dumps(out)                                          # 必须可 JSON 化
    assert isinstance(out["$NODE"]["n1"]["result"], str)      # 降级为 str


def test_original_context_is_not_mutated_in_place():
    """**关键**：只改持久化副本，原对象不动（原 context 仍被其它路径引用）。"""
    bad = {1, 2, 3}
    ctx = {"x": bad}
    out = sanitize_context_for_persist(ctx)
    assert out is not None and out is not ctx
    assert ctx["x"] is bad and isinstance(ctx["x"], set)     # 原对象原封不动


def test_circular_reference_returns_none_not_raises():
    ctx: dict = {}
    ctx["self"] = ctx                                        # 循环引用
    assert sanitize_context_for_persist(ctx) is None         # 不抛，交给层 2


def test_object_whose_str_raises_does_not_blow_up():
    class Boom:
        def __str__(self):
            raise RuntimeError("__str__ 自己炸")
    out = sanitize_context_for_persist({"x": Boom()})
    assert out is not None
    json.dumps(out)                                          # 没炸且可序列化
    assert "unstr-able" in out["x"]                          # 写了占位


def test_nested_and_ok_context_is_unchanged_in_value():
    ctx = {"a": [1, 2, {"b": "c"}]}
    assert sanitize_context_for_persist(ctx) == ctx
