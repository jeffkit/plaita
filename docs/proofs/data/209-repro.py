#!/usr/bin/env python3
"""plaita#209 repro / before-after probe.

Run from ``plaita-console/backend`` (or with that directory on ``PYTHONPATH``)::

    cd plaita-console/backend
    PYTHONPATH=.:../.. <venv>/python ../../docs/proofs/data/209-repro.py

Creates a dataset root, a same-prefix sibling directory and an outside
directory, then calls the real ``api.supervisor._resolve_dataset_path`` for
normal / sibling-prefix / absolute / ``..`` cases.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

from fastapi import HTTPException

from api import supervisor as supervisor_api


def _call(name: str) -> dict:
    try:
        path = supervisor_api._resolve_dataset_path(name)
    except HTTPException as exc:
        return {"ok": False, "status": exc.status_code, "detail": str(exc.detail)}
    return {"ok": True, "path": str(path)}


def run() -> dict:
    with tempfile.TemporaryDirectory(prefix="plaita209-") as tmp:
        base = Path(tmp)
        root = base / "supervisor-datasets"
        root.mkdir()
        (root / "case.json").write_text('{"cases": []}', encoding="utf-8")
        (root / "sub").mkdir()
        (root / "sub" / "case.json").write_text('{"cases": []}', encoding="utf-8")
        (root / "_hidden").mkdir()
        (root / ".hidden").mkdir()

        sibling = base / "supervisor-datasets-evil"
        sibling.mkdir()
        (sibling / "x.json").write_text('{"evil": true}', encoding="utf-8")

        outside = base / "other"
        outside.mkdir()
        (outside / "x.json").write_text('{"outside": true}', encoding="utf-8")

        os.environ["PLAITA_SUPERVISOR_DATASET_DIR"] = str(root)

        return {
            "plaita_file": __import__("plaita").__file__,
            "python": sys.version.split()[0],
            "root": str(root.resolve()),
            "normal": _call("case.json"),
            "subdir": _call("sub/case.json"),
            "sibling_prefix": _call("../supervisor-datasets-evil/x.json"),
            "absolute": _call(str(outside / "x.json")),
            "dotdot": _call("a/../../x.json"),
            "missing": _call("missing.json"),
            "listed": [
                item["name"]
                for item in supervisor_api.list_datasets("f")["datasets"]
            ],
        }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
