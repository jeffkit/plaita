#!/usr/bin/env python3
"""plaita#133 repro / before-after probe.

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    PYTHONPATH=. .venv/bin/python docs/proofs/data/133-repro.py

It evaluates the public-attribute environment chain and a set of data controls,
and prints one JSON document to stdout.  Values are **not** dumped: live
``os.environ`` objects are recorded by type only, and a string equal to
``os.environ['HOME']`` is recorded as ``same_as_home: true`` without echoing
it.  The same script is run against the pre-fix tree and the fixed tree; the
two documents are merged into ``133-before-after.json``.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from types import SimpleNamespace

from plaita.core.expression_parser import ExpressionParser


@dataclass
class Payload:
    text: str = "hello"


class Plain:
    def __init__(self):
        self.text = "hello"


def _make_traceback():
    try:
        raise RuntimeError("issue133 probe")
    except RuntimeError:
        return sys.exc_info()[2]


def _make_coroutine():
    async def body():
        return 1

    return body()


def _make_async_generator():
    async def body():
        yield 1

    return body()


def _make_generator():
    yield 1


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
    return {"type": type(value).__name__}


CASES = [
    # data controls
    "$NODE.h.text",
    "$NODE.d.k",
    "$NODE.payload.text",
    "$INPUT.payload.text",
    "$NODE.ns.text",
    "$F.get($NODE.d, 'k')",
    # the issue's public-attribute chain
    "$NODE.h.os",
    "$NODE.h.os.environ",
    "$NODE.h.os.environ.HOME",
    "$NODE.h.os.__class__",
    # every rejected type as a base object (attribute traversal from it)
    "$NODE.h.mod.environ",
    "$NODE.h.fn.x",
    "$NODE.h.cls.some_attr",
    "$NODE.h.gen.gi_frame",
    "$NODE.h.coro.cr_frame",
    "$NODE.h.agen.ag_frame",
    "$NODE.h.frame.f_locals",
    "$NODE.h.tb.tb_next",
    "$NODE.h.code.co_consts",
    "$NODE.h.method.__self__",
    # adjacent same-family route (mapping-like value, not attribute traversal)
    "$NODE.h.env",
    "$NODE.h.env.HOME",
    "$NODE.d.env.HOME",
]


def main() -> int:
    gen = _make_generator()
    coro = _make_coroutine()
    agen = _make_async_generator()
    ctx = {
        "$NODE": {
            "h": SimpleNamespace(
                os=os, env=os.environ, text="hello",
                mod=os, fn=os.getcwd, cls=os._Environ,
                gen=gen, coro=coro, agen=agen,
                frame=sys._getframe(), tb=_make_traceback(),
                code=(lambda: 1).__code__, method="x".upper,
            ),
            "d": {"env": os.environ, "k": "v"},
            "payload": Payload(),
            "ns": SimpleNamespace(text="hello"),
        },
        "$INPUT": {"payload": Payload()},
    }
    parser = ExpressionParser.for_prefix("$")
    cases = []
    for expr in CASES:
        try:
            value = parser.evaluate(expr, ctx)
            cases.append({"expr": expr, "ok": True, **_describe(value)})
        except Exception as exc:  # noqa: BLE001 - the probe records every refusal
            cases.append({
                "expr": expr,
                "ok": False,
                "exc": type(exc).__name__,
                "msg": str(exc)[:400],
            })
    payload = {
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "cases": cases,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    coro.close()
    asyncio.run(agen.aclose())
    gen.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
