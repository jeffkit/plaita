#!/usr/bin/env python3
"""plaita#210 source-level before/after probe.

The console frontend has no Vitest/MSW runner in this repo, so the regression
is pinned at the source level (same approach as the reporter's r10 spec) plus
``npm run build``.  Scan the three supervisor fetch blocks for:

* ``API_BASE`` (SUB_PATH-aware base),
* ``authHeaders()`` (Authorization / X-Admin-API-Key / X-Tenant-ID),
* explicit headers.

Usage::

    python docs/proofs/data/210-repro.py [repo-root]

Without an argument the current repo root is used.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

MARKERS = [
    ("config", "/supervisor/config"),
    ("datasets", "/supervisor/datasets"),
    ("iterate", "/supervisor/iterate"),
]
WINDOW = 420


def scan(tsx: str) -> dict:
    calls = []
    for label, marker in MARKERS:
        idx = tsx.find(marker)
        block = tsx[max(0, idx - 180): idx + WINDOW] if idx >= 0 else ""
        calls.append({
            "call": label,
            "found": idx >= 0,
            "has_api_base": "API_BASE" in block,
            "has_auth_headers": "authHeaders()" in block,
            "has_headers": "headers:" in block,
            "raw_api_literal": "fetch('/api" in block or "fetch(`/api" in block,
        })
    return {
        "raw_fetch_calls": calls,
        "all_missing_auth": all(not c["has_auth_headers"] for c in calls),
        "all_missing_base": all(not c["has_api_base"] for c in calls),
    }


def main() -> int:
    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parents[3]
    tsx_path = root / "plaita-console" / "frontend" / "src" / "pages" / "Supervisor.tsx"
    if not tsx_path.is_file():
        print(json.dumps({"error": f"missing {tsx_path}"}, ensure_ascii=False))
        return 1
    print(json.dumps({
        "tsx": str(tsx_path),
        **scan(tsx_path.read_text(encoding="utf-8")),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
