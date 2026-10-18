"""plaita#227 repro: @flow 发射/编译丢弃 loop.condition（JSON→@flow→JSON 后条件静默消失）。

运行（worktree 内，修前红 / 修后 ALL OK）::

    PYTHONPATH=/tmp/w47 /Users/jeff.jie/.venvs/plaita/bin/python \
        docs/proofs/data/227-repro.py


sexpr 链保留 :when；@flow 链的 `_emit_collection` LOOP 分支不发 `condition=`，
`_compile_for` 也不读它 ⇒ 往返后 loop 节点无 condition（无报错，语义静默退化）。

验收（#227）：
  1. 含 condition 的 loop 经 @flow 往返后 condition 逐字段相等；
  2. sexpr 链保持（不回归）；
  3. 无 condition 的 loop 发射结果不变（不带多余 condition=）。
"""
import copy
import sys

from plaita.dsl.codeflow import compile_source, emit_source
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr

failures = []


def check(name, fn):
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - repro 需要打印真实异常
        failures.append(f"{name}: {type(exc).__name__}: {exc}")
        print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    else:
        print(f"ok   {name}")


def _ir(condition=True):
    loop = {
        "type": "loop",
        "id": "n",
        "collection": "$INPUT.items",
        "childFlow": {
            "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "cs", "next": "ce"},
                {"type": "end", "id": "ce", "resultType": "success"},
            ],
        },
        "next": "e",
    }
    if condition:
        loop["condition"] = {"field": "$INPUT.y", "operator": "eq", "value": 1}
    return {
        "nodes": [
            {"type": "start", "id": "s", "next": "n"},
            loop,
            {"type": "end", "id": "e", "resultType": "success"},
        ]
    }


def _loop_node(ir):
    return next(n for n in ir["nodes"] if n["type"] == "loop")


def _emit_compile_keeps_condition():
    ir = _ir(condition=True)
    source = emit_source(copy.deepcopy(ir))
    print("--- emitted source ---")
    print(source)
    back = compile_source(source)
    node = _loop_node(back)
    assert "condition" in node, f"condition 丢失，回读 loop 节点键={sorted(node)}"
    assert node["condition"] == {
        "field": "$INPUT.y", "operator": "eq", "value": 1,
    }, node["condition"]


def _condition_group_roundtrip():
    ir = _ir(condition=False)
    _loop_node(ir)["condition"] = {
        "relation": "and",
        "conditions": [
            {"field": "$LOOP-ITEM.n", "operator": "lt", "value": 3},
            {"field": "$LOOP-INDEX", "operator": "ne", "value": 1},
        ],
    }
    source = emit_source(copy.deepcopy(ir))
    back = compile_source(source)
    assert _loop_node(back).get("condition") == _loop_node(ir)["condition"], _loop_node(back)


def _sexpr_chain_keeps_condition():
    ir = _ir(condition=True)
    text = flow_to_sexpr(copy.deepcopy(ir))
    back = compile_sexpr(text)
    assert _loop_node(back).get("condition") == {
        "field": "$INPUT.y", "operator": "eq", "value": 1,
    }, _loop_node(back)


def _no_condition_loop_unchanged():
    ir = _ir(condition=False)
    source = emit_source(copy.deepcopy(ir))
    assert "condition=" not in source, source
    back = compile_source(source)
    assert "condition" not in _loop_node(back), _loop_node(back)


check("JSON→@flow→JSON 保留 loop.condition", _emit_compile_keeps_condition)
check("condition 组往返逐字段相等", _condition_group_roundtrip)
check("sexpr 链保留 loop.condition", _sexpr_chain_keeps_condition)
check("无 condition 的 loop 行为不变", _no_condition_loop_unchanged)

if failures:
    print(f"\n{len(failures)} FAILURE(S)")
    sys.exit(1)
print("\nALL OK")
