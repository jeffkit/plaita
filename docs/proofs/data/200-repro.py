#!/usr/bin/env python3
"""plaita#200 repro / before-after probe.

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    PYTHONPATH=. .venv/bin/python docs/proofs/data/200-repro.py

The issue's IR carries every HTTP access-policy field (SSRF face) plus the EVENT
``subscriptionTimeout``.  The probe round-trips it twice — through sexpr and
through ``@flow`` — and reports, per field, what survived.  On the pre-fix tree
every policy field reads back as ``None`` (the flow silently falls back to an
unrestricted HTTP client); on the fixed tree both routes are field-for-field
equal.

The script only uses public DSL entry points, so the same file runs on the
pre-fix tree and on the fixed tree.
"""
from __future__ import annotations

import json
import sys

from plaita.dsl.codeflow import compile_source, emit_source
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr

HTTP_POLICY = ("allowedHosts", "deniedHosts", "blockPrivateNetworks",
               "requestTimeout", "maxRedirects")
SUBSCRIPTION_TIMEOUT = 120

IR = {"flow_id": "f-policy", "nodes": [
    {"type": "start", "id": "s", "next": "h"},
    {"type": "http", "id": "h", "method": "GET", "url": "http://example.com",
     "allowedHosts": ["example.com", "*.internal.test"],
     "deniedHosts": ["evil.example.com"],
     "blockPrivateNetworks": True, "requestTimeout": 5, "maxRedirects": 2,
     "next": "w"},
    {"type": "event", "id": "w", "eventType": "quote.ready",
     "subscriptionTimeout": SUBSCRIPTION_TIMEOUT, "next": "e"},
    {"type": "end", "id": "e", "resultType": "success"},
]}


def _by_id(nodes, node_id):
    return [n for n in nodes if n.get("id") == node_id][0]


def _http_policy(node):
    return {key: node.get(key) for key in HTTP_POLICY}


def _expected_policy():
    return {key: IR["nodes"][1][key] for key in HTTP_POLICY}


def _verdict(policy, sub_timeout):
    return {
        "http_policy_equal": policy == _expected_policy(),
        "subscription_timeout_equal": sub_timeout == SUBSCRIPTION_TIMEOUT,
        "ok": policy == _expected_policy()
        and sub_timeout == SUBSCRIPTION_TIMEOUT,
    }


def _sexpr():
    src = flow_to_sexpr(IR)
    nodes = compile_sexpr(src)["nodes"]
    policy = _http_policy(_by_id(nodes, "h"))
    sub_timeout = _by_id(nodes, "w").get("subscriptionTimeout")
    return {"source": src, "after": policy,
            "subscriptionTimeout": sub_timeout, **_verdict(policy, sub_timeout)}


def _codeflow():
    src = emit_source(IR)
    nodes = compile_source(src)["nodes"]
    policy = _http_policy(_by_id(nodes, "h"))
    event = _by_id(nodes, "w")
    sub_timeout = event.get("subscriptionTimeout",
                            event.get("subscription_timeout"))
    return {"source": src, "after": policy,
            "subscriptionTimeout": sub_timeout, **_verdict(policy, sub_timeout)}


def run() -> dict:
    return {
        "issue": 200,
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "expected": {"http_policy": _expected_policy(),
                     "subscriptionTimeout": SUBSCRIPTION_TIMEOUT},
        "sexpr": _sexpr(),
        "codeflow": _codeflow(),
    }


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    ok = result["sexpr"]["ok"] and result["codeflow"]["ok"]
    print("ROUNDTRIP_OK" if ok else "ROUNDTRIP_LOST")
