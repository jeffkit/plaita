#!/usr/bin/env python3
"""plaita#220 repro / before-after probe — @flow / sexpr 往返丢失面（6 面）。

    PYTHONPATH=/tmp/w44 .venvs/plaita/bin/python docs/proofs/data/220-repro.py

> L3（sexpr code `sandbox_backend`）已由 88eb1340 落地，不在本补丁范围；
> 其证据见 `docs/proofs/220-L3-sexpr-sandbox-backend.md`。

每个面都跑它所涉及的前端链（JSON→@flow→JSON、JSON→sexpr→JSON）：

- L1  @flow 集合节点 id 未绑定：map/filter/find/loop/reduce 下游引用
      `$NODE.<id>` 时 `_CodeflowError: 未知名字 '<id>'`（5/5 硬失败）；
- L2  @flow parallel 分支 `input` / `condition` 丢失；
- L4  HTTP `output` / `query` / `addressing` / `delegate` 两链都丢；
- L5a assignment `upstreamOutput` 两链都丢；
- L5b assignment `outputType` @flow 链丢；
- L6  sexpr HTTP headers/body 值类型漂移（"1"→1、"true"→True、"42"→42）。
"""
from __future__ import annotations

import json
import sys

from plaita.dsl.codeflow import compile_source, emit_source
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr


def _child(output="$INPUT.item"):
    return {
        "runtime": "python",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "cs", "next": "ce"},
            {"type": "end", "id": "ce", "output": output, "resultType": "success"},
        ],
    }


def _flow(nodes, flow_id="p220"):
    return {"flow_id": flow_id, "inputType": {"dataType": "object"}, "nodes": nodes}


def _start(nid="s", nxt="x"):
    return {"type": "start", "id": nid, "next": nxt}


def _end(nid="e", output="done"):
    return {"type": "end", "id": nid, "output": output, "resultType": "success"}


def _emit_rt(ir):
    source = emit_source(ir)
    return compile_source(source), source


def _sexpr_rt(ir):
    source = flow_to_sexpr(ir)
    return compile_sexpr(source), source


def _nodes_of(ir, ntype):
    return [n for n in ir.get("nodes", []) if n.get("type") == ntype]


# ---------------------------------------------------------------- L1
def _l1():
    out = {}
    for kind in ("map", "filter", "find", "loop", "reduce"):
        child = _child("$INPUT[0]" if kind == "reduce" else "$INPUT.item")
        nodes = [
            _start(nxt="m"),
            {"type": kind, "id": "m", "collection": "$INPUT.items",
             "childFlow": child, "next": "e"},
            _end(output="$NODE.m"),
        ]
        if kind == "reduce":
            nodes[1]["initial"] = 0
        try:
            back, source = _emit_rt(_flow(nodes))
            ends = _nodes_of(back, "end")
            out[kind] = {
                "ok": len(ends) == 1 and ends[0].get("output") == "$NODE.m",
                "output": ends[0].get("output") if ends else None,
            }
        except Exception as exc:  # noqa: BLE001 - repro 要把红原因原样报出
            out[kind] = {"ok": False, "exc": type(exc).__name__, "msg": str(exc)}
    return {"ok": all(v["ok"] for v in out.values()), "types": out}


# ---------------------------------------------------------------- L2
def _l2():
    branch = {
        "name": "a",
        "flow": _child(),
        "input": "$INPUT.x",
        "condition": {"field": "$INPUT.flag", "operator": "truthy", "value": True},
    }
    ir = _flow([
        _start(nxt="p"),
        {"type": "parallel", "id": "p", "mode": "thread", "joinBranches": ["a"],
         "isConditional": True, "branches": [branch, {"name": "b", "flow": _child()}],
         "next": "e"},
        _end(),
    ])
    flow_back, flow_src = _emit_rt(ir)
    sexpr_back, sexpr_src = _sexpr_rt(ir)
    fb = _nodes_of(flow_back, "parallel")[0]["branches"][0]
    sb = _nodes_of(sexpr_back, "parallel")[0]["branches"][0]
    expected_input = "$INPUT.x"
    expected_cond = branch["condition"]
    return {
        "ok": (fb.get("input") == expected_input and fb.get("condition") == expected_cond
               and sb.get("input") == expected_input and sb.get("condition") == expected_cond),
        "flow": {"input": fb.get("input"), "condition": fb.get("condition")},
        "sexpr": {"input": sb.get("input"), "condition": sb.get("condition")},
    }


# ---------------------------------------------------------------- L3
# 已由 88eb1340 落地（不在本补丁范围），此处不再探测。


# ---------------------------------------------------------------- L4
_HTTP_FIELDS = ("output", "query", "addressing", "delegate")


def _l4():
    ir = _flow([
        _start(nxt="h"),
        {"type": "http", "id": "h", "method": "GET", "url": "https://example.com",
         "output": "$INPUT.pick", "query": {"q": "1"},
         "headers": {"X-A": "b"}, "body": {"k": "v"},
         "addressing": {"name": "domain"},
         "delegate": {"name": "proxy", "params": {"p": 1}}, "next": "e"},
        _end(),
    ])
    expected = ir["nodes"][1]
    flow_back, flow_src = _emit_rt(ir)
    sexpr_back, sexpr_src = _sexpr_rt(ir)
    fn = _nodes_of(flow_back, "http")[0]
    sn = _nodes_of(sexpr_back, "http")[0]
    return {
        "ok": all(fn.get(f) == expected[f] for f in _HTTP_FIELDS)
              and all(sn.get(f) == expected[f] for f in _HTTP_FIELDS),
        "flow_lost": [f for f in _HTTP_FIELDS if fn.get(f) != expected[f]],
        "sexpr_lost": [f for f in _HTTP_FIELDS if sn.get(f) != expected[f]],
    }


# ---------------------------------------------------------------- L5
def _l5():
    upstream = [{"upstream": "u", "value": "$NODE.prev"},
                {"upstream": "v", "value": "literal"}]
    ir = _flow([
        _start(nxt="prev"),
        {"type": "assignment", "id": "prev", "output": "$INPUT.x", "next": "a"},
        {"type": "assignment", "id": "a", "output": "$INPUT.x",
         "outputType": {"dataType": "string"},
         "upstreamOutput": upstream, "next": "e"},
        _end(),
    ])
    flow_back, flow_src = _emit_rt(ir)
    sexpr_back, sexpr_src = _sexpr_rt(ir)
    fn = _nodes_of(flow_back, "assignment")[-1]
    sn = _nodes_of(sexpr_back, "assignment")[-1]
    return {
        "l5a_ok": fn.get("upstreamOutput") == upstream and sn.get("upstreamOutput") == upstream,
        "l5b_ok": fn.get("outputType") == {"dataType": "string"}
                  and sn.get("outputType") == {"dataType": "string"},
        "flow": {"outputType": fn.get("outputType"),
                 "upstreamOutput": fn.get("upstreamOutput")},
        "sexpr": {"outputType": sn.get("outputType"),
                  "upstreamOutput": sn.get("upstreamOutput")},
    }


# ---------------------------------------------------------------- L6
def _l6():
    headers = {"X-Num": "42", "X-Bool": "true", "X-Str": "ok"}
    body = {"n": "1", "b": "false", "s": "42"}
    ir = _flow([
        _start(nxt="h"),
        {"type": "http", "id": "h", "method": "POST", "url": "https://example.com",
         "headers": headers, "body": body, "next": "e"},
        _end(),
    ])
    flow_back, flow_src = _emit_rt(ir)
    sexpr_back, sexpr_src = _sexpr_rt(ir)
    fn = _nodes_of(flow_back, "http")[0]
    sn = _nodes_of(sexpr_back, "http")[0]
    return {
        "ok": fn.get("headers") == headers and fn.get("body") == body
              and sn.get("headers") == headers and sn.get("body") == body,
        "flow_headers": fn.get("headers"), "flow_body": fn.get("body"),
        "sexpr_headers": sn.get("headers"), "sexpr_body": sn.get("body"),
    }


def run() -> dict:
    faces = {"L1": _l1(), "L2": _l2(), "L4": _l4(),
             "L5": _l5(), "L6": _l6()}
    faces["L5"]["ok"] = faces["L5"].pop("l5a_ok") and faces["L5"].pop("l5b_ok")
    return {
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "faces": faces,
        "all_ok": all(face.get("ok") for face in faces.values()),
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
