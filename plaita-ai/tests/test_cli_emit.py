"""CLI `plaita-ai emit`——JSON flow definition → @flow 源码（cmd_emit）。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from plaita_ai.cli.main import build_parser, cmd_emit

LINEAR_IR = {
    "runtime": "python",
    "flow_id": "legacy_flow",
    "inputType": {"dataType": "object"},
    "nodes": [
        {"type": "start", "id": "start", "next": "fetch"},
        {"type": "http", "id": "fetch", "method": "GET",
         "url": "https://api.example.com", "next": "ret"},
        {"type": "end", "id": "ret", "output": "$NODE.fetch.status",
         "resultType": "success"},
    ],
}


def _run(argv):
    args = build_parser().parse_args(["emit", *argv])
    return cmd_emit(args)


def test_emit_from_file(tmp_path: Path, capsys):
    src_file = tmp_path / "flow.json"
    src_file.write_text(json.dumps(LINEAR_IR), encoding="utf-8")
    code = _run([str(src_file)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"]
    assert "fetch = HTTP" in payload["source"]
    assert "legacy_flow" in payload["source"]


def test_emit_out_writes_source_file(tmp_path: Path, capsys):
    src_file = tmp_path / "flow.json"
    out_file = tmp_path / "flow.py"
    src_file.write_text(json.dumps(LINEAR_IR), encoding="utf-8")
    code = _run([str(src_file), "--out", str(out_file)])
    assert code == 0
    assert "fetch = HTTP" in out_file.read_text(encoding="utf-8")
    payload = json.loads(capsys.readouterr().out)
    assert "written to" in payload["source"]


def test_emit_unexpressible_returns_one(tmp_path: Path, capsys):
    ir = dict(LINEAR_IR, flow_id="sw")
    ir["nodes"] = [
        {"type": "start", "id": "start", "next": "sw"},
        {"type": "switch", "id": "sw", "expression": "$INPUT.x", "next": "e",
         "branches": [{"target": "e", "value": "1", "priority": 0}]},
        {"type": "end", "id": "e", "resultType": "success"},
    ]
    src_file = tmp_path / "switch.json"
    src_file.write_text(json.dumps(ir), encoding="utf-8")
    code = _run([str(src_file)])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert not payload["ok"]
    assert "switch" in payload["error"]
