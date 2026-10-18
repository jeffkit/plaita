"""#224 回归：``plaita.event.normalize_event`` 的 dict 分支必须能构造 Event。

修前该分支调用 pydantic v2 模型上不存在的 ``Event.from_dict`` ⇒ 公开导出函数
对 dict 形态输入必抛 ``AttributeError: from_dict``，db 事件总线的
``publish({"event_type": ...})`` 100% 失败。

本模块钉住 #224 验收的三点：
1. dict 输入返回 ``Event``，``event_type`` / ``data`` 保真（不再 AttributeError）；
2. ``correlation_id`` 随 dict 保真（redis 同形入口取它做路由；dict 分支此前丢字段）；
3. 不就地改写调用方传入的字典（含 str 分支的 ``data.update(kwargs)``）。

另钉住全分支的三态输入：Event 实例 / str / dict 正常构造；list 等不可识别类型
抛 ``TypeError``；dict 里字段形态畸形时抛 pydantic ``ValidationError``（而非
``AttributeError`` 或静默吞掉）。
"""

from __future__ import annotations

import pytest

pytest.importorskip("pydantic")

from pydantic import ValidationError

from plaita.event import Event, normalize_event


# ---------------------------------------------------------------------------
# 正常三态：Event / str / dict
# ---------------------------------------------------------------------------

def test_event_instance_is_returned_as_is():
    original = Event(event_type="a.done", data={"x": 1}, correlation_id="cid-1")
    assert normalize_event(original) is original


def test_str_branch_builds_event():
    event = normalize_event("a.done", {"x": 1})
    assert isinstance(event, Event)
    assert event.event_type == "a.done"
    assert event.data == {"x": 1}


def test_dict_branch_is_public_export():
    """公开导出面：``from plaita.event import normalize_event`` 可直接吃 dict。"""
    event = normalize_event({"event_type": "a.done", "data": {"x": 1}})
    assert isinstance(event, Event)
    assert event.event_type == "a.done"
    assert event.data == {"x": 1}


def test_dict_preserves_correlation_id():
    event = normalize_event(
        {"event_type": "a.done", "data": {"x": 1}, "correlation_id": "cid-42"}
    )
    assert event.correlation_id == "cid-42"


def test_dict_without_data_defaults_to_empty_dict():
    event = normalize_event({"event_type": "a.done"})
    assert event.data == {}


def test_dict_with_none_data_defaults_to_empty_dict():
    event = normalize_event({"event_type": "a.done", "data": None})
    assert event.data == {}


def test_dict_json_string_data_is_decoded():
    event = normalize_event({"event_type": "a.done", "data": '{"x": 2}'})
    assert event.data == {"x": 2}


def test_dict_json_string_data_falls_back_to_value():
    event = normalize_event({"event_type": "a.done", "data": "not-json"})
    assert event.data == {"value": "not-json"}


def test_dict_accepts_correlation_id_kwarg():
    event = normalize_event(
        {"event_type": "a.done", "data": {"x": 1}}, correlation_id="cid-kw"
    )
    assert event.correlation_id == "cid-kw"


def test_dict_without_event_type_is_treated_as_data():
    event = normalize_event({"x": 1}, event_type="custom.event")
    assert event.event_type == "custom.event"
    assert event.data == {"x": 1}


# ---------------------------------------------------------------------------
# 不改写调用方入参
# ---------------------------------------------------------------------------

def test_dict_input_is_not_mutated():
    payload = {"event_type": "a.done", "data": {"x": 1}, "correlation_id": "cid-42"}
    snapshot = {"event_type": "a.done", "data": {"x": 1}, "correlation_id": "cid-42"}
    normalize_event(payload)
    assert payload == snapshot


def test_str_branch_does_not_mutate_caller_data():
    data = {"x": 1}
    event = normalize_event("a.done", data, correlation_id="cid-7")
    assert event.data == {"x": 1, "correlation_id": "cid-7"}
    assert data == {"x": 1}


# ---------------------------------------------------------------------------
# 畸形输入：不可识别类型 / 字段形态非法
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", [[1, 2], ["a.done"], None, 42])
def test_unsupported_types_raise_type_error(value):
    with pytest.raises(TypeError, match="无法从类型"):
        normalize_event(value)


def test_dict_with_non_dict_data_raises_validation_error():
    """data 形态非法走 pydantic 校验，而不是修前的 AttributeError。"""
    with pytest.raises(ValidationError):
        normalize_event({"event_type": "a.done", "data": [1, 2]})


def test_dict_with_non_str_event_type_raises_validation_error():
    with pytest.raises(ValidationError):
        normalize_event({"event_type": None, "data": {}})


# ---------------------------------------------------------------------------
# db 事件总线入口（#224 的实际触发点：SqlalchemyEventBus.publish 吃 dict）
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sqlalchemy_bus_publish_accepts_dict(tmp_path):
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("aiosqlite")

    from sqlalchemy.ext.asyncio import create_async_engine

    from plaita.event.sqlalchemy import SqlalchemyEventBus

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'events.db'}",
        connect_args={"timeout": 30},
    )
    bus = SqlalchemyEventBus(engine=engine, create_tables=True)
    payload = {
        "event_type": "a.done",
        "data": {"x": 1},
        "correlation_id": "cid-db",
    }
    try:
        event_id = await bus.publish(payload)
        stored = await bus.get_event(event_id)
        assert stored.event_type == "a.done"
        assert stored.data == {"x": 1}
        assert stored.correlation_id == "cid-db"
    finally:
        await engine.dispose()
    # 发布会话不得改写调用方字典
    assert payload == {
        "event_type": "a.done",
        "data": {"x": 1},
        "correlation_id": "cid-db",
    }
