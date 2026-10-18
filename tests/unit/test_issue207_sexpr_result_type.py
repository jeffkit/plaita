"""plaita#207：sexpr 写侧 ``end`` 只看 camelCase ``resultType``。

同一 IR 的 snake_case 形态（``--format ir`` / ``Flow.model_dump()``）用
``result_type``：写侧看不到它就不发射 ``:result-type``，读侧 ``_c_end`` 缺省回填
``success`` ⇒ **失败出口往返后变成成功**（``error`` 载荷还在，语义已错）。

验收：snake IR 往返后 ``resultType == "error"`` 且 ``error`` 逐字保留；camel IR
行为不变；``success`` 出口不发射 ``:result-type``（保持存量字节）。
"""
from __future__ import annotations

from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr

ERROR_PAYLOAD = {"code": 4001, "message": "bad"}


def _flow(result_key: str, value: str = "error") -> dict:
    return {"flow_id": "f-err", "nodes": [
        {"type": "start", "id": "s", "next": "e"},
        {"type": "end", "id": "e", result_key: value,
         "error": dict(ERROR_PAYLOAD)},
    ]}


def _end_after(src: str) -> dict:
    return [n for n in compile_sexpr(src)["nodes"] if n.get("id") == "e"][0]


def test_snake_case_error_exit_survives_roundtrip():
    """#207 主症：``result_type="error"`` 不得被洗成 ``success``。"""
    src = flow_to_sexpr(_flow("result_type"))
    assert ":result-type error" in src
    end = _end_after(src)
    assert end.get("resultType") == "error"
    assert end.get("error") == ERROR_PAYLOAD


def test_snake_case_nop_exit_survives_roundtrip():
    """同根另一取值：``nop`` 也是非 ``success`` 出口。"""
    src = flow_to_sexpr(_flow("result_type", "nop"))
    assert ":result-type nop" in src
    assert _end_after(src).get("resultType") == "nop"


def test_camel_case_error_exit_unchanged():
    """camel IR（sexpr 自己的产物 / 手写 JSON）行为不变。"""
    src = flow_to_sexpr(_flow("resultType"))
    assert ":result-type error" in src
    end = _end_after(src)
    assert end.get("resultType") == "error"
    assert end.get("error") == ERROR_PAYLOAD


def test_snake_success_is_not_emitted():
    """``success`` 是读侧缺省值，不发射 ``:result-type``（存量字节不变）。"""
    src = flow_to_sexpr(_flow("result_type", "success"))
    assert ":result-type" not in src
    assert _end_after(src).get("resultType") == "success"


def test_both_keys_present_prefers_camel():
    """两个键同时存在时 camel 优先（与 codeflow 发射器同口径）。"""
    ir = _flow("resultType", "error")
    ir["nodes"][1]["result_type"] = "success"
    assert ":result-type error" in flow_to_sexpr(ir)
