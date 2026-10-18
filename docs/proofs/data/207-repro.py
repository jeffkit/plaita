#!/usr/bin/env python3
"""plaita#207 repro / before-after probe.

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    PYTHONPATH=. .venv/bin/python docs/proofs/data/207-repro.py

The same failure exit is expressed twice: with camelCase ``resultType`` (the
shape sexpr itself produces) and with snake_case ``result_type`` (the shape
``--format ir`` / ``Flow.model_dump()`` produce).  The pre-fix writer only
recognises the camel spelling, so the snake variant loses ``:result-type`` and
reads back as ``success`` — the error payload stays, the terminal *type* is
washed.  On the fixed tree both spellings keep ``resultType == "error"``.
"""
from __future__ import annotations

import json
import sys

from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr

ERROR_PAYLOAD = {"code": 4001, "message": "bad"}


def _flow(result_key: str, value: str) -> dict:
    return {"flow_id": "f-err", "nodes": [
        {"type": "start", "id": "s", "next": "e"},
        {"type": "end", "id": "e", result_key: value,
         "error": dict(ERROR_PAYLOAD)},
    ]}


def _probe(result_key: str, value: str = "error") -> dict:
    src = flow_to_sexpr(_flow(result_key, value))
    end = [n for n in compile_sexpr(src)["nodes"] if n.get("id") == "e"][0]
    got = end.get("resultType", end.get("result_type"))
    return {
        "ir_key": result_key,
        "source": src,
        "resultType_after": got,
        "error_after": end.get("error"),
        "ok": got == value and end.get("error") == ERROR_PAYLOAD,
    }


def run() -> dict:
    return {
        "issue": 207,
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "snake_error": _probe("result_type", "error"),
        "camel_error": _probe("resultType", "error"),
        "snake_nop": _probe("result_type", "nop"),
    }


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    ok = all(result[key]["ok"]
             for key in ("snake_error", "camel_error", "snake_nop"))
    print("ROUNDTRIP_OK" if ok else "RESULT_TYPE_WASHED")
