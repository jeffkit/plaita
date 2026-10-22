"""Tests for plaita_ai.evals — dataset loading, scoring, evaluate, compare."""

from __future__ import annotations

import json

import httpx
import pytest

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai.evals import Dataset, compare, evaluate, load_dataset

from ._console_fake import FakeConsole


def make_client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport())


# -- dataset -----------------------------------------------------------------


def test_load_dataset_single_file(tmp_path):
    file = tmp_path / "cases.json"
    file.write_text(
        json.dumps(
            {
                "cases": [
                    {"id": "c1", "input": {"name": "ada"}, "expect": {"contains": "ada"}},
                    {"input": {"name": "bob"}},  # id auto-generated
                ]
            }
        ),
        encoding="utf-8",
    )
    dataset = Dataset.load(str(file))
    assert [c.id for c in dataset.cases] == ["c1", "case-2"]
    assert dataset.cases[0].expect == {"contains": "ada"}


def test_load_dataset_directory_skips_metadata(tmp_path):
    (tmp_path / "a.json").write_text(json.dumps({"cases": [{"id": "a1", "input": {}}]}), encoding="utf-8")
    (tmp_path / "_meta.json").write_text(json.dumps({"note": "not cases"}), encoding="utf-8")
    (tmp_path / "b.json").write_text(json.dumps([{"id": "b1", "input": {}}]), encoding="utf-8")
    dataset = Dataset.load(str(tmp_path))
    assert [c.id for c in dataset.cases] == ["a1", "b1"]
    with pytest.raises(Exception):
        Dataset.load(str(tmp_path / "missing.json"))


# -- scoring -----------------------------------------------------------------


def test_expectation_variants():
    from plaita_ai.evals.evals import Case, assert_score

    ok = assert_score(Case("c", {}, {"contains": "hello"}), {"msg": "hello world"})
    assert ok["ok"] and ok["score"] == 1.0

    miss = assert_score(Case("c", {}, {"contains": "bye"}), {"msg": "hello world"})
    assert not miss["ok"] and "bye" in miss["reasons"][0]

    path_ok = assert_score(Case("c", {}, {"equals_path": {"path": "a.b", "value": 3}}), {"a": {"b": 3}})
    assert path_ok["ok"]
    path_miss = assert_score(Case("c", {}, {"equals_path": {"path": "a.b", "value": 3}}), {"a": {"b": 4}})
    assert not path_miss["ok"]

    empty = assert_score(Case("c", {}, {"not_empty": True}), None)
    assert not empty["ok"]

    # judge expectation without a configured judge → skipped, excluded from ok math
    skipped = assert_score(Case("c", {}, {"judge": "is polite"}), {"msg": "hi"})
    assert skipped["judge"] == "skipped"


# -- evaluate ------------------------------------------------------------------


def test_evaluate_dry_run_mode(tmp_path):
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"flow": "greet"}'}, published="1.0.0")

    def handler(flow_json, input_data):
        name = (input_data or {}).get("name", "")
        return {"result": {"greeting": f"hello {name}"}, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    file = tmp_path / "cases.json"
    file.write_text(
        json.dumps(
            {
                "cases": [
                    {"id": "pass", "input": {"name": "ada"}, "expect": {"contains": "ada"}},
                    {"id": "fail", "input": {"name": "x"}, "expect": {"contains": "abe"}},
                ]
            }
        ),
        encoding="utf-8",
    )
    report = evaluate(make_client(fake), "demo", "1.0.0", load_dataset(str(file)))
    assert report["flow_id"] == "demo"
    assert report["pass_rate"] == 0.5
    assert report["avg_score"] == 0.5
    by_id = {c["id"]: c for c in report["cases"]}
    assert by_id["pass"]["ok"]
    assert not by_id["fail"]["ok"]
    assert "abe" in by_id["fail"]["reasons"][0]


def test_evaluate_dry_run_error_counts_as_failure(tmp_path):
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": "broken"}, published="1.0.0")
    fake.dry_run_handler = lambda flow_json, input_data: {"result": None, "nodes": [], "error": "compile boom"}
    file = tmp_path / "cases.json"
    file.write_text(json.dumps({"cases": [{"id": "c1", "input": {}}]}), encoding="utf-8")
    report = evaluate(make_client(fake), "demo", "1.0.0", load_dataset(str(file)))
    assert report["pass_rate"] == 0.0
    assert "compile boom" in report["cases"][0]["error"]


def test_evaluate_execution_mode(tmp_path):
    fake = FakeConsole()
    fake.run_handler = lambda flow_id, version, params: ("completed", {"out": params.get("q")})
    file = tmp_path / "cases.json"
    file.write_text(
        json.dumps({"cases": [{"id": "c1", "input": {"q": 42}, "expect": {"equals_path": {"path": "out", "value": 42}}}]}),
        encoding="utf-8",
    )
    report = evaluate(make_client(fake), "demo", "1.0.0", load_dataset(str(file)), mode="execution")
    assert report["mode"] == "execution"
    assert report["pass_rate"] == 1.0


# -- compare -------------------------------------------------------------------


def test_compare_reports():
    base = {
        "flow_id": "demo",
        "version": "1.0.0",
        "pass_rate": 0.5,
        "avg_score": 0.5,
        "cases": [
            {"id": "kept", "ok": True, "score": 1.0},
            {"id": "regressed", "ok": True, "score": 1.0},
            {"id": "still-bad", "ok": False, "score": 0.0},
        ],
    }
    candidate = {
        "flow_id": "demo",
        "version": "1.0.1",
        "pass_rate": 0.6667,
        "avg_score": 0.6667,
        "cases": [
            {"id": "kept", "ok": True, "score": 1.0},
            {"id": "regressed", "ok": False, "score": 0.0},
            {"id": "still-bad", "ok": True, "score": 1.0},
        ],
    }
    verdict = compare(base, candidate)
    assert verdict["base_version"] == "1.0.0"
    assert verdict["candidate_version"] == "1.0.1"
    assert verdict["pass_rate"]["delta"] == round(0.6667 - 0.5, 4)
    assert verdict["improvements"] == ["still-bad"]
    assert verdict["regressions"] == ["regressed"]
    assert len(verdict["cases"]) == 3


def test_evaluate_execution_mode_carries_langfuse_trace(tmp_path):
    fake = FakeConsole()
    fake.run_handler = lambda flow_id, version, params: ("completed", {"ok": True})

    original = fake.handler

    def with_trace(request: "httpx.Request") -> "httpx.Response":
        resp = original(request)
        if request.method == "GET" and "/api/executions/" in request.url.path:
            data = json.loads(resp.content.decode("utf-8"))
            data["langfuse_trace_url"] = "https://langfuse.example/trace/abc123"
            return httpx.Response(resp.status_code, json=data)
        return resp

    import httpx as _httpx

    fake.handler = with_trace
    client = ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"),
                           transport=fake.transport())
    file = tmp_path / "cases.json"
    file.write_text(json.dumps({"cases": [{"id": "c1", "input": {"q": 1}, "expect": {"not_empty": True}}]}),
                    encoding="utf-8")
    report = evaluate(client, "demo", "1.0.0", load_dataset(str(file)), mode="execution")
    assert report["cases"][0]["langfuse_trace_url"] == "https://langfuse.example/trace/abc123"
    assert report["langfuse_traces"] == ["https://langfuse.example/trace/abc123"]


# -- 输入侧深嵌套（A2R46-01）----------------------------------------------------
#
# 候选 flow 的 dry-run 返回值与 execution 错误载荷都是候选自控的：深嵌套结构
# （~10k 层约 20KB）让 json.dumps 抛 RecursionError（RuntimeError 族，不是
# ValueError）。r73 只守了 judge 回复侧；输入侧这两处（assert_score 首行的
# json.dumps(output)、execution 错误载荷的 json.dumps）一旦炸穿 per-case 守卫
# （只捕 ConsoleClientError），整个 evaluate 崩、兄弟 case 已算出的结果一并
# 丢弃。修复口径与输出侧相同：单 case 降级 unavailable 剔出均值，兄弟保留，
# 报告本体保持 JSON 可序列化（下游 MCP 回复 / proposer prompt 还要 dumps 它）。
#
# 注入方式：深结构在客户端成形（resp.json() 的解析产物），故以 client 实例级
# monkeypatch 注入返回值——线上 json.loads 与 json.dumps 的深度极限对称，纯
# 线上文本到不了 scoring 面；实际窗口是「结构已成形 + 深调用栈耗尽预算」。


def _deep_output(depth: int = 10008) -> dict:
    root: dict = {}
    node = root
    for _ in range(depth):
        node["child"] = {}
        node = node["child"]
    return root


def _two_case_file(tmp_path):
    file = tmp_path / "cases.json"
    file.write_text(
        json.dumps(
            {
                "cases": [
                    {"id": "deep", "input": {"i": 0}, "expect": {"contains": "ada"}},
                    {"id": "sibling", "input": {"i": 1}, "expect": {"contains": "ada"}},
                ]
            }
        ),
        encoding="utf-8",
    )
    return load_dataset(str(file))


def test_deep_candidate_output_degrades_to_unavailable_siblings_kept(tmp_path, monkeypatch):
    """负控主体（dry-run 面）：深嵌套候选输出必须只倒下自己那一个 case。

    pre-fix：assert_score 首行 json.dumps(output) 抛 RecursionError，穿透
    evaluate（只捕 ConsoleClientError）——整个报告崩，兄弟 case 结果丢弃；
    post-fix：deep 单 case 降级 unavailable 剔出均值，sibling 照常计分，
    报告本体可 json.dumps。
    """
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}'}, published="1.0.0")
    client = make_client(fake)

    def deep_dry_run(flow_json, input=None, pinned=None, only_node=None):
        if (input or {}).get("i") == 0:
            return {"result": _deep_output(), "nodes": [], "error": None}
        return {"result": {"text": "hello ada"}, "nodes": [], "error": None}

    monkeypatch.setattr(client, "dry_run", deep_dry_run)
    report = evaluate(client, "demo", "1.0.0", _two_case_file(tmp_path))
    by_id = {c["id"]: c for c in report["cases"]}
    assert by_id["deep"]["unavailable"] is True
    assert by_id["deep"]["ok"] is False
    assert "RecursionError" in by_id["deep"]["reasons"][0]
    assert by_id["sibling"]["ok"] is True
    assert report["unavailable_cases"] == 1
    assert report["judge_unavailable_cases"] == 0
    assert report["pass_rate"] == 1.0
    assert report["avg_score"] == 1.0
    json.dumps(report)  # 报告本体不得携带序列化地雷


def test_deep_execution_error_payload_degrades_case_only(tmp_path, monkeypatch):
    """负控（execution 面：错误载荷 json.dumps）：同口径单 case 降级。"""
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}'}, published="1.0.0")
    client = make_client(fake)

    def start(flow_id, version=None, params=None, **kw):
        return {"execution_id": "exec-deep" if (params or {}).get("i") == 0 else "exec-ok"}

    def wait(execution_id, timeout_s=120.0, **kw):
        if execution_id == "exec-deep":
            return {"status": "failed", "error": _deep_output(), "execution_id": execution_id}
        return {"status": "completed", "output": {"text": "hello ada"}, "execution_id": execution_id}

    monkeypatch.setattr(client, "start_execution", start)
    monkeypatch.setattr(client, "wait_execution", wait)
    report = evaluate(client, "demo", "1.0.0", _two_case_file(tmp_path), mode="execution")
    by_id = {c["id"]: c for c in report["cases"]}
    assert by_id["deep"]["unavailable"] is True
    assert "RecursionError" in by_id["deep"]["error"]
    assert by_id["sibling"]["ok"] is True
    assert report["unavailable_cases"] == 1
    assert report["pass_rate"] == 1.0
    json.dumps(report)


def test_compare_treats_unavailable_case_as_blind():
    """unavailable case 的 ok/score 不是证据：compare 不得拿它算回归/进步。"""
    base = {"version": "1.0.0", "pass_rate": 1.0, "avg_score": 1.0,
            "cases": [{"id": "c0", "ok": True, "score": 1.0}, {"id": "c1", "ok": True, "score": 1.0}]}
    candidate = {"version": "1.0.1", "pass_rate": 1.0, "avg_score": 1.0,
                 "cases": [{"id": "c0", "unavailable": True, "ok": False, "score": 0.0},
                           {"id": "c1", "ok": True, "score": 1.0}]}
    verdict = compare(base, candidate)
    row = {r["id"]: r for r in verdict["cases"]}["c0"]
    assert row["judge_unavailable"] is True  # blind 复用既有标记面
    assert verdict["regressions"] == []
    assert verdict["improvements"] == []
