"""plaita#224 repro: normalize_event 的 dict 分支调用不存在的 Event.from_dict。

运行（worktree 内，修前红 / 修后 ALL OK）::

    .venv/bin/python docs/proofs/data/224-repro.py

公开导出面 `plaita.event.normalize_event` 文档化三类输入（Event / str / dict），
dict 形态必抛 AttributeError（pydantic v2 BaseModel 无 from_dict）。

本脚本同时钉住 #224 验收的三点：
  1. dict 输入不再抛 AttributeError（返回 Event，event_type/data 保真）；
  2. correlation_id 保真（dict 分支此前直接丢字段）；
  3. 不修改调用方传入的字典（str 分支的 data 也不得被就地改写）。
"""
import sys

from plaita.event import Event, normalize_event

failures = []


def check(name, fn):
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - repro 需要打印真实异常
        failures.append(f"{name}: {type(exc).__name__}: {exc}")
        print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    else:
        print(f"ok   {name}")


def dict_with_correlation():
    payload = {
        "event_type": "a.done",
        "data": {"x": 1},
        "correlation_id": "cid-42",
    }
    original = {"event_type": payload["event_type"],
                "data": dict(payload["data"]),
                "correlation_id": payload["correlation_id"]}
    event = normalize_event(payload)
    assert isinstance(event, Event), type(event)
    assert event.event_type == "a.done", event.event_type
    assert event.data == {"x": 1}, event.data
    assert event.correlation_id == "cid-42", event.correlation_id
    assert payload == original, f"入参 dict 被改写: {payload!r}"


def dict_without_data():
    payload = {"event_type": "a.done"}
    event = normalize_event(payload)
    assert isinstance(event, Event), type(event)
    assert event.event_type == "a.done"
    assert event.data == {}, event.data
    assert payload == {"event_type": "a.done"}, payload


def dict_json_string_data():
    payload = {"event_type": "a.done", "data": '{"x": 2}'}
    event = normalize_event(payload)
    assert event.data == {"x": 2}, event.data


def str_branch_does_not_mutate_data():
    data = {"x": 1}
    event = normalize_event("a.done", data, correlation_id="cid-7")
    assert event.data == {"x": 1, "correlation_id": "cid-7"}, event.data
    assert data == {"x": 1}, f"str 分支改写了调用方 data: {data!r}"


def dict_kwargs_correlation():
    payload = {"event_type": "a.done", "data": {"x": 1}}
    event = normalize_event(payload, correlation_id="cid-kw")
    assert event.correlation_id == "cid-kw", event.correlation_id


check("dict+correlation_id/data 保真且不改入参", dict_with_correlation)
check("dict 无 data -> data={}", dict_without_data)
check("dict data 为 JSON 字符串", dict_json_string_data)
check("str 分支不改写调用方 data", str_branch_does_not_mutate_data)
check("dict 分支接收 correlation_id kwarg", dict_kwargs_correlation)

if failures:
    print(f"\n{len(failures)} FAILURE(S)")
    sys.exit(1)
print("\nALL OK")
