#!/usr/bin/env python3
"""plaita#203 repro / before-after probe.

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    PYTHONPATH=. .venv/bin/python docs/proofs/data/203-repro.py

The script only uses the public expression parser and the default/scoped
function registries, so the same file runs on the pre-fix tree (``d66db5d``)
and on the fixed tree.  It prints one JSON document; live environment values are
recorded by type / ``same_as_home`` only, never echoed.
"""
from __future__ import annotations

import json
import os
import sys
from collections import ChainMap, UserDict
from collections.abc import Mapping
from types import MappingProxyType, SimpleNamespace

from plaita.core.expression import (
    ExpressionRegistry,
    FunctionCategory,
    _fn_get,
)
from plaita.core.expression_parser import ExpressionParser
from plaita.core.state import CheckpointState


class PoisonMapping(Mapping):
    def __init__(self, data):
        self._data = dict(data)
        self.reads = 0

    def __getitem__(self, key):
        self.reads += 1
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def get(self, key, default=None):
        self.reads += 1
        return self._data.get(key, default)


class OverrideDict(dict):
    def __init__(self, data):
        super().__init__(data)
        self.override_reads = 0

    def get(self, key, default=None):
        self.override_reads += 1
        return "OVERRIDE"

    def __getitem__(self, key):
        self.override_reads += 1
        return "OVERRIDE"


def _describe(value):
    if isinstance(value, str):
        if value == os.environ.get("HOME"):
            return {"type": "str", "same_as_home": True, "len": len(value)}
        if len(value) > 80:
            return {"type": "str", "len": len(value), "prefix": value[:40] + "..."}
        return {"type": "str", "value": value}
    if value is None or isinstance(value, (bool, int, float)):
        return {"type": type(value).__name__, "value": value}
    if isinstance(value, (list, tuple, set, frozenset, dict)):
        return {"type": type(value).__name__, "len": len(value)}
    if isinstance(value, PoisonMapping):
        return {"type": type(value).__name__, "reads": value.reads}
    if isinstance(value, OverrideDict):
        return {"type": type(value).__name__, "override_reads": value.override_reads}
    return {"type": type(value).__name__}


def _run(parser, expr, ctx, registry=None):
    try:
        value = parser.evaluate(expr, ctx, registry)
        return {"expr": expr, "ok": True, **_describe(value)}
    except Exception as exc:  # noqa: BLE001 - probe records every refusal
        return {
            "expr": expr,
            "ok": False,
            "exc": type(exc).__name__,
            "msg": str(exc)[:400],
        }


def _base_ctx():
    return {
        "$NODE": {
            "h": SimpleNamespace(env=os.environ, text="hello"),
            "d": {"env": os.environ, "k": "v"},
            "cm": ChainMap({"SECRET": "s1"}),
            "ud": UserDict({"SECRET": "s2"}),
            "mproxy": MappingProxyType({"SECRET": "s3"}),
            "lst": [os.environ],
            "od": OverrideDict({"k": "real"}),
        },
        "$INPUT": {"x": "input-x"},
        "$ENV": {"HOME": "exposed-home"},
    }


def main() -> int:
    parser = ExpressionParser.for_prefix("$")
    cases = []

    # data controls
    ctx = _base_ctx()
    for expr in ("$NODE.d.k", "$INPUT.x", "$F.get($NODE.d, 'k')"):
        cases.append(_run(parser, expr, ctx))

    # container as value / base
    for expr in ("$NODE.h.env", "$NODE.h.env.HOME",
                 "$NODE.d.env", "$NODE.d.env.HOME",
                 "$NODE.cm.SECRET", "$NODE.ud.SECRET",
                 "$NODE.mproxy.SECRET",
                 "$NODE.lst[0]", "$NODE.lst[0].HOME",
                 "$F.get($NODE.h.env, 'HOME')",
                 "$F.getDictValue($NODE.h.env, 'HOME')",
                 "$F.get($NODE.cm, 'SECRET')",
                 "$F.get($NODE.d, 'env')"):
        cases.append(_run(parser, expr, ctx))

    # $F.get with a function result (the pre-call gate)
    reg = ExpressionRegistry()
    reg.register("probe_env", lambda: os.environ, FunctionCategory.DICT)
    reg.register("get", _fn_get, FunctionCategory.DICT)
    cases.append(_run(parser, "$F.get($F.probe_env(), 'HOME')", {}, reg))

    # poisoned mapping: zero-read proof
    pm = PoisonMapping({"SECRET": "s"})
    pctx = {"$NODE": {"pm": pm, "d": {"pm": pm}}}
    for expr in ("$NODE.pm.SECRET", "$NODE.d.pm",
                 "$F.get($NODE.d, 'pm')"):
        case = _run(parser, expr, pctx)
        case["poison_reads"] = pm.reads
        cases.append(case)

    # CheckpointState-backed $INPUT
    state = CheckpointState()
    state["$INPUT"] = {"x": "input-x"}
    for expr in ("$INPUT.x", "$F.get($INPUT, 'x')"):
        cases.append(_run(parser, expr, state))

    # $ENV allowlist semantics
    cases.append(_run(parser, "$ENV.HOME", {"$ENV": {}}))
    cases.append(_run(parser, "$ENV.HOME", {"$ENV": {"HOME": "exposed-home"}}))

    # dict subclass overrides: real storage only
    od = OverrideDict({"k": "real"})
    octx = {"$NODE": {"od": od}}
    for expr in ("$NODE.od.k", "$F.get($NODE.od, 'k')"):
        case = _run(parser, expr, octx)
        case["override_reads"] = od.override_reads
        cases.append(case)

    print(json.dumps({
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "cases": cases,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
