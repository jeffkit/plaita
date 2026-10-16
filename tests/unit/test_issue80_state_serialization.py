"""#80 回归守卫：写侧「不可序列化（永久）」与「Redis 瞬断（瞬态）」必须可区分。

基线 `65f48f2` 的缺陷：`RedisExecutionStorage.save_execution_state` 用一个
`except Exception -> return False` 把两类故障混成同一返回值 ⇒ 调用方无法区分
「这个 state 永远存不下去」与「Redis 抖了一下」，一律按瞬态重投 ⇒ 执行反复从
checkpoint 重跑同一节点、副作用重放、永无终态。

本文件钉住修复后的契约（**每一条都能单独让它红**）：
  * 不可序列化 ⇒ 抛 `StateSerializationError`（**不再**返回裸 `False`）；
  * Redis 瞬断 ⇒ **仍然**返回 `False`（瞬态路径不得回退，`test_worker_save_guard.py` 的
    不 ack 重投契约依赖它）；
  * 两者**可区分**（这就是本单的全部意义）；
  * `StateSerializationError` **不是** `ValueError`（否则会被 worker 当畸形消息 poison ack）。
"""
from __future__ import annotations

import pytest
import redis as redis_mod

from plaita.storage.base import ExecutionState, StateSerializationError
from plaita.storage.fenced import FencedExecutionStorage
from plaita.storage.redis import RedisExecutionStorage

URL = "redis://127.0.0.1:6379/9"
NS = "issue80test"


def _state(tag: str, bad: bool) -> ExecutionState:
    ctx: dict = {"tag": tag}
    if bad:
        ctx["as_set"] = {1, 2, 3}          # json.dumps 必抛
    return ExecutionState(
        execution_id=f"exec-{tag}", flow_id="f", flow_name="n", flow_version="1",
        context=ctx, status="running",
        start_time="2026-10-16T00:00:00", last_update_time="2026-10-16T00:00:00",
    )


@pytest.fixture()
def client():
    c = redis_mod.Redis.from_url(URL)
    c.flushdb()
    yield c
    c.flushdb()


@pytest.fixture()
def store(client):
    return RedisExecutionStorage(client=client, namespace=NS)


class _BoomClient:
    """模拟 Redis 挂掉：任何 pipeline 都抛瞬断错误。"""

    def pipeline(self, *a, **k):
        raise redis_mod.exceptions.ConnectionError("simulated redis down")


# ── 验收 1：不可序列化不再被吞成 False ───────────────────────────────────
def test_unserializable_state_raises_not_false(store):
    with pytest.raises(StateSerializationError):
        store.save_execution_state("exec-bad", _state("bad", bad=True))


def test_error_names_the_execution_id(store):
    with pytest.raises(StateSerializationError) as ei:
        store.save_execution_state("exec-named", _state("bad", bad=True))
    assert "exec-named" in str(ei.value)
    assert ei.value.execution_id == "exec-named"


# ── 验收 2（负例不得回退）：Redis 瞬断仍然返回 False ──────────────────────
def test_redis_transient_still_returns_false():
    b = RedisExecutionStorage(client=_BoomClient(), namespace=NS)
    assert b.save_execution_state("exec-x", _state("good", bad=False)) is False


def test_transient_and_serialization_are_distinguishable(store):
    """本单的核心：两类故障在返回路径上必须可区分。"""
    try:
        store.save_execution_state("exec-bad", _state("bad", bad=True))
        serialization = ("returned", None)
    except StateSerializationError:
        serialization = ("raised", "serialization")
    b = RedisExecutionStorage(client=_BoomClient(), namespace=NS)
    transient = ("returned", b.save_execution_state("exec-y", _state("good", bad=False)))
    assert serialization != transient
    assert serialization == ("raised", "serialization")
    assert transient == ("returned", False)


# ── 边界：类型不得是 ValueError（worker 把它当畸形消息 poison ack）────────
def test_error_is_not_value_error():
    assert not issubclass(StateSerializationError, ValueError)
    assert issubclass(StateSerializationError, RuntimeError)


# ── happy path 不得回归 ─────────────────────────────────────────────────
def test_json_safe_state_still_saves(store):
    assert store.save_execution_state("exec-good", _state("good", bad=False)) is True


# ── 第二位点：fenced 路径同判据 ──────────────────────────────────────────
def test_fenced_path_also_raises_on_unserializable(store):
    f = FencedExecutionStorage(store)
    # 未持 fenced 租约时 fenced 会退化普通写——两条路都必须抛，不得返回 False
    with pytest.raises(StateSerializationError):
        f.save_execution_state("exec-fbad", _state("bad", bad=True))
