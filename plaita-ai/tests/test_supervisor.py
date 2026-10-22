"""Tests for plaita_ai.supervisor — the gated self-iteration loop."""

from __future__ import annotations

import json

import httpx
import pytest

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai.evals import Dataset
from plaita_ai.supervisor import PromptProposer, StaticProposer, Supervisor, SupervisorPolicy

from ._console_fake import FakeConsole


DEF_V1 = '{"version": 1}'
DEF_V2 = '{"version": 2}'  # the improvement
DEF_V3 = '{"version": 3}'  # a regression


def make_client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport())


def make_dataset(cases: int = 2) -> Dataset:
    return Dataset([{"id": f"c{i}", "input": {"i": i}} for i in range(cases)])  # type: ignore[list-item]


def seed_with_baseline(fake: FakeConsole, baseline_score: float = 0.5) -> FakeConsole:
    """Baseline version 1.0.0 whose dry-run output scores via 'contains'."""
    fake.seed_flow("demo", {"1.0.0": DEF_V1}, published="1.0.0")
    marker = "good" if baseline_score >= 1.0 else "bad"

    def handler(flow_json, input_data):
        # The improved definition (2) emits the passing marker; everything
        # else emits whatever its content says (3 → regressed marker).
        if flow_json == DEF_V2:
            out = {"text": "good output"}
        elif flow_json == DEF_V3:
            out = {"text": "worse output"}
        else:
            out = {"text": marker}
        return {"result": out, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    return fake


def expects_good(dataset: Dataset) -> Dataset:
    for case in dataset.cases:
        case.expect = {"contains": "good"}
    return dataset


# -- policy ---------------------------------------------------------------------


def test_policy_gate_validation():
    with pytest.raises(ValueError):
        SupervisorPolicy(promote_gate="yolo")


# -- single iteration -------------------------------------------------------------


def test_improved_candidate_returns_promotion_ticket():
    fake = seed_with_baseline(FakeConsole())
    client = make_client(fake)
    dataset = expects_good(make_dataset())
    proposer = StaticProposer([{"definition": DEF_V2, "rationale": "emit the good marker"}])
    sup = Supervisor(client, SupervisorPolicy(promote_gate="manual", min_improvement=0.02), proposer)

    outcome = sup.iterate("demo", dataset)
    assert outcome["status"] == "improved"
    assert outcome["candidate_version"] == "1.0.1"
    assert outcome["promoted"] is False
    ticket = outcome["promotion_ticket"]
    assert ticket["version"] == "1.0.1"
    assert "publish" in ticket["command"]
    # candidate saved as draft, nothing published by the loop
    detail = client.get_flow("demo")
    assert fake.published["demo"] == "1.0.0"
    assert {v["version"]: v["status"] for v in detail["versions"]}["1.0.1"] == "draft"


def test_auto_gate_publishes():
    fake = seed_with_baseline(FakeConsole())
    client = make_client(fake)
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        client,
        SupervisorPolicy(promote_gate="auto", min_improvement=0.02),
        StaticProposer([{"definition": DEF_V2, "rationale": "fix"}]),
    )
    outcome = sup.iterate("demo", dataset)
    assert outcome["promoted"] is True
    assert "promotion_ticket" not in outcome
    assert fake.published["demo"] == "1.0.1"


def test_regression_is_not_promoted():
    fake = seed_with_baseline(FakeConsole())
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(min_improvement=0.02),
        StaticProposer([{"definition": DEF_V3, "rationale": "oops"}]),
    )
    outcome = sup.iterate("demo", dataset)
    assert outcome["status"] == "no_improvement"
    assert "promotion_ticket" not in outcome
    assert fake.published["demo"] == "1.0.0"


def test_no_proposer_returns_no_proposal():
    fake = seed_with_baseline(FakeConsole())
    sup = Supervisor(make_client(fake))
    outcome = sup.iterate("demo", expects_good(make_dataset()))
    assert outcome["status"] == "no_proposal"


def test_baseline_needs_a_version():
    fake = FakeConsole()  # no flows at all
    sup = Supervisor(make_client(fake), proposer=StaticProposer())
    with pytest.raises(Exception, match="not found in the console"):
        sup.iterate("ghost", expects_good(make_dataset()))


# -- the loop -------------------------------------------------------------------


def test_run_loop_stops_at_target():
    fake = seed_with_baseline(FakeConsole())
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(target_score=1.0, promote_gate="manual", min_improvement=0.02),
        StaticProposer([{"definition": DEF_V2, "rationale": "fix"}]),
    )
    result = sup.run_loop("demo", dataset)
    assert result["final_status"] == "target_reached"
    assert result["iterations_run"] == 1


def test_run_loop_pauses_after_consecutive_failures():
    # A dataset with zero scored cases makes every candidate report avg=None,
    # which the loop counts as a failed iteration.
    fake = seed_with_baseline(FakeConsole())
    empty = Dataset([])
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(max_consecutive_failures=2, max_iterations=5),
        StaticProposer([{"definition": DEF_V2, "rationale": "x"} for _ in range(5)]),
    )
    result = sup.run_loop("demo", empty)
    assert result["final_status"] == "paused"
    assert result["iterations_run"] == 2


def test_run_loop_budget_exhaustion_without_improvement():
    # Proposer keeps offering regressions; loop should run to budget and stop.
    fake = seed_with_baseline(FakeConsole())
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(max_iterations=3, min_improvement=0.02),
        StaticProposer([{"definition": DEF_V3, "rationale": "nope"} for _ in range(3)]),
    )
    result = sup.run_loop("demo", dataset)
    assert result["final_status"] == "budget_exhausted"
    assert result["iterations_run"] == 3
    assert all(it["status"] == "no_improvement" for it in result["iterations"])


def test_static_proposer_exhaustion_ends_loop():
    fake = seed_with_baseline(FakeConsole())
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(max_iterations=5, min_improvement=0.02),
        StaticProposer([]),
    )
    result = sup.run_loop("demo", dataset)
    assert result["final_status"] == "no_proposal"
    assert result["iterations_run"] == 1


# -- prompt proposer parsing (offline) --------------------------------------------


def test_prompt_proposer_requires_config():
    with pytest.raises(Exception, match="PLAITA_AI_PROPOSER"):
        PromptProposer({})


def test_prompt_proposer_parse_tolerates_fences():
    content = 'Sure!\n```json\n{"definition": "{\\\"a\\\": 1}", "rationale": "tweak"}\n```\n'
    parsed = PromptProposer._parse(content)
    assert parsed is not None
    assert parsed["definition"] == '{"a": 1}'
    assert PromptProposer._parse("no json here") is None
    assert PromptProposer._parse('{"no_definition": true}') is None


# -- prompt proposer CoDeFlow parsing (offline) -----------------------------------


def test_prompt_proposer_parses_codeflow_block():
    content = (
        "```python\n"
        "# Rationale: tweak greeting suffix\n"
        '@flow("greeter")\n'
        "def greeter(INPUT):\n"
        '    return F.concat(INPUT.hi, "!")\n'
        "```\n"
    )
    parsed = PromptProposer._parse(content)
    assert parsed is not None
    definition = json.loads(parsed["definition"])
    assert definition["flow_id"] == "greeter"
    # 编译产物是标准 IR dict——与 legacy 提案走同一条 save/evaluate 下游
    assert definition["nodes"][0]["type"] == "start"
    assert "tweak greeting suffix" in parsed["rationale"]
    assert "@flow" in parsed["source"]


def test_prompt_proposer_rejects_uncompilable_source_without_legacy():
    content = (
        "```python\n"
        "@flow(\"broken\")\n"
        "def broken(INPUT):\n"
        "    return INPUT.a + INPUT.b * \n"  # 语法错误
        "```\n"
    )
    assert PromptProposer._parse(content) is None


def test_prompt_proposer_uncompilable_source_falls_back_to_legacy_json():
    content = (
        "```python\n"
        "@flow(\"broken\")\n"
        "def broken(INPUT):\n"
        "    return oops\n"
        "```\n"
        '{"definition": "{\\"fallback\\": true}", "rationale": "legacy path"}\n'
    )
    parsed = PromptProposer._parse(content)
    assert parsed is not None
    assert parsed["definition"] == '{"fallback": true}'
    assert parsed["rationale"] == "legacy path"


def test_prompt_proposer_current_source_from_json_string():
    definition = json.dumps({
        "runtime": "python", "flow_id": "legacy", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "e"},
            {"type": "end", "id": "e", "output": "$INPUT.x", "resultType": "success"},
        ],
    })
    source = PromptProposer._emit_current_source(definition)
    assert "def legacy(INPUT):" in source
    # dict 形态同样支持
    assert "def legacy(INPUT):" in PromptProposer._emit_current_source(json.loads(definition))


def test_prompt_proposer_current_source_degrades_gracefully():
    assert PromptProposer._emit_current_source("") == ""
    assert PromptProposer._emit_current_source(None) == ""
    assert PromptProposer._emit_current_source("not json at all") == ""
    # switch 节点不可表达 → 空串（LLM 退回看 JSON definition）
    switch_ir = {"flow_id": "sw", "nodes": [{"type": "switch", "id": "s"}]}
    assert PromptProposer._emit_current_source(switch_ir) == ""


# -- #229：regressions 闸（候选 avg 更高但打回既有 case） -------------------------


def _trading_handler(flow_json, input_data):
    """基线过 c0，候选过 c1/c2 但打回 c0：avg 1/3 → 2/3，regressions=[c0]。"""
    i = int(input_data.get("i", 0))
    if flow_json == DEF_V2:
        ok = i in (1, 2)
    else:
        ok = i == 0
    return {
        "result": {"text": "good output" if ok else "bad output"},
        "nodes": [],
        "error": None,
    }


def _trading_dataset() -> Dataset:
    return Dataset([
        {"id": f"c{i}", "input": {"i": i}, "expect": {"contains": "good"}}
        for i in range(3)
    ])


@pytest.mark.parametrize("gate", ["manual", "auto"])
def test_regressions_block_promotion_even_when_average_improves(gate):
    """#229-A：avg 更高但把既有 case 打回 ⇒ 不得出 ticket、不得 publish。

    修前该组合会走 ``improved`` 分支：manual 出 promotion ticket，auto 直接
    ``publish_version``。既有 ``test_regression_is_not_promoted`` 的候选是**未改进**
    （avg 不升），走不到「avg 升 + 有 regressions」这个真实风险面。
    """
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": DEF_V1}, published="1.0.0")
    fake.dry_run_handler = _trading_handler
    client = make_client(fake)

    published_calls = []
    real_publish = client.publish_version

    def recording_publish(*args, **kwargs):
        published_calls.append((args, kwargs))
        return real_publish(*args, **kwargs)

    client.publish_version = recording_publish
    sup = Supervisor(
        client,
        SupervisorPolicy(promote_gate=gate, min_improvement=0.02),
        StaticProposer([{"definition": DEF_V2, "rationale": "trade c0 for c1/c2"}]),
    )
    outcome = sup.iterate("demo", _trading_dataset())

    comparison = outcome["comparison"]
    assert comparison["avg_score"]["base"] < comparison["avg_score"]["candidate"]
    assert comparison["regressions"] == ["c0"]
    assert outcome["status"] == "no_improvement"
    assert "promotion_ticket" not in outcome
    assert outcome.get("promoted") is not True
    # 「没有调用 publish_version」必须直接断言，而不是只看 published 指针
    assert published_calls == []
    assert fake.published["demo"] == "1.0.0"
    detail = client.get_flow("demo")
    assert {v["version"]: v["status"] for v in detail["versions"]}["1.0.1"] == "draft"


# -- 输入侧深嵌套与孤儿候选版本（A2R46-01）---------------------------------------


def _deep_output(depth: int = 10008) -> dict:
    root: dict = {}
    node = root
    for _ in range(depth):
        node["child"] = {}
        node = node["child"]
    return root


def test_deep_candidate_output_never_promotes(monkeypatch):
    """负控（promote 门面）：深嵌套候选输出让该 case 无判定，两侧均值口径
    不一致——绝不许在 blind 上 promote。

    pre-fix：候选 evaluate 被 RecursionError 打穿，走 run_loop 兜底
    failed_iteration（红：既非降级也暴露不出 blind 面）；
    post-fix：单 case 降级 unavailable → no_improvement + 对账计数，
    无 promotion_ticket。
    """
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": DEF_V1}, published="1.0.0")

    def handler(flow_json, input_data):
        if flow_json == DEF_V2:
            return {"result": {"text": "good output"}, "nodes": [], "error": None}
        return {"result": {"text": "bad"}, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    client = make_client(fake)
    real_dry_run = client.dry_run

    def deep_dry_run(flow_json, input=None, pinned=None, only_node=None):
        if flow_json == DEF_V2 and (input or {}).get("i") == 0:
            return {"result": _deep_output(), "nodes": [], "error": None}
        return real_dry_run(flow_json, input=input, pinned=pinned, only_node=only_node)

    monkeypatch.setattr(client, "dry_run", deep_dry_run)
    dataset = expects_good(make_dataset(cases=2))
    sup = Supervisor(
        client,
        SupervisorPolicy(promote_gate="manual"),
        StaticProposer([{"definition": DEF_V2, "rationale": "deep"}]),
    )
    out = sup.run_loop("demo", dataset)
    it = out["iterations"][0]
    assert it["status"] == "no_improvement"
    assert it["unavailable_cases"] == {"baseline": 0, "candidate": 1}
    assert it["candidate_report"]["unavailable_cases"] == 1
    assert "not comparable" in it["reason"]
    assert "promotion_ticket" not in it
    assert it.get("promoted") is not True


class _CandidateVersionUnreadableConsole(FakeConsole):
    """candidate 版本（非 1.0.0）GET 一律 500：save_version 之后的评测段崩溃。"""

    def handler(self, request):
        path = request.url.path
        if request.method == "GET" and "/versions/" in path and not path.endswith("/versions/1.0.0"):
            return httpx.Response(500, json={"detail": "candidate version read failed"})
        return super().handler(request)


def test_failed_iteration_keeps_candidate_version_lineage(caplog):
    """负控主体（孤儿候选版本面）：评测段崩溃时 outcome 必须补记 candidate_version。

    版本在 save_version 已落库；异常穿透让 run_loop 兜底新建 outcome 时把这个
    字段丢掉，库里就留下无主孤儿（实弹：1.0.1/1.0.2 落库但 outcome 无
    candidate_version）。处置取「补记」不取「回滚」：console 版本无删除 API，
    版本谱系本来就是给人类审计的——失败候选保留谱系、outcome 指认它。
    pre-fix：candidate_version 丢失（红）；post-fix：逐迭代指认 + WARNING 留痕。
    """
    import logging

    caplog.set_level(logging.WARNING, logger="plaita_ai.supervisor")
    fake = seed_with_baseline(_CandidateVersionUnreadableConsole())
    dataset = expects_good(make_dataset(cases=1))
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(max_iterations=2, max_consecutive_failures=2),
        StaticProposer([{"definition": DEF_V2, "rationale": "x"} for _ in range(2)]),
    )
    out = sup.run_loop("demo", dataset)
    assert out["final_status"] == "paused"
    assert [it["status"] for it in out["iterations"]] == ["failed_iteration", "failed_iteration"]
    # 关键断言：落库的版本必须在 outcome 里指认得到（不成孤儿）
    assert [it["candidate_version"] for it in out["iterations"]] == ["1.0.1", "1.0.2"]
    assert sorted(fake.flows["demo"]["versions"]) == ["1.0.0", "1.0.1", "1.0.2"]
    for it in out["iterations"]:
        assert "ConsoleClientError" in it["reason"]
    assert "degraded to failed_iteration" in caplog.text
    assert "1.0.1" in caplog.text
