"""Judge unavailability must never read as an ordinary 0 score.

An outage (transport / HTTP / unparseable envelope) and a real "judge says no"
verdict have to stay distinguishable, otherwise a judge that comes back online
between the baseline and candidate runs looks like an improvement.
"""

from __future__ import annotations

import json

import httpx
import pytest

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai.evals import Dataset, compare, load_dataset
from plaita_ai.evals.evals import Case, assert_score, evaluate
from plaita_ai.evals.judges import judge_configured, judge_output
from plaita_ai.supervisor import StaticProposer, Supervisor, SupervisorPolicy

from ._console_fake import FakeConsole


FULL_ENV = {
    "PLAITA_AI_JUDGE_BASE_URL": "http://judge.local/v1",
    "PLAITA_AI_JUDGE_MODEL": "m",
    "PLAITA_AI_JUDGE_API_KEY": "k",
}
HALF_ENV = {"PLAITA_AI_JUDGE_BASE_URL": "http://judge.local/v1", "PLAITA_AI_JUDGE_MODEL": "m"}


def make_client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport())


def unavailable() -> dict:
    return {"ok": False, "score": 0.0, "unavailable": True, "reason": "judge endpoint unavailable"}


def healthy() -> dict:
    return {"ok": True, "score": 1.0, "reason": "looks fine"}


def judge_dataset(tmp_path, cases: int = 2) -> Dataset:
    file = tmp_path / "cases.json"
    file.write_text(
        json.dumps(
            {
                "cases": [
                    {"id": f"c{i}", "input": {"i": i}, "expect": {"judge": "is polite"}}
                    for i in range(cases)
                ]
            }
        ),
        encoding="utf-8",
    )
    return load_dataset(str(file))


def fake_openai_output(fake: FakeConsole) -> None:
    fake.dry_run_handler = lambda flow_json, input_data: {
        "result": {"text": "hi"}, "nodes": [], "error": None,
    }


# -- configured-ness -----------------------------------------------------------


def test_judge_configured_requires_api_key():
    assert judge_configured({}) is False
    assert judge_configured(HALF_ENV) is False
    assert judge_configured({**HALF_ENV, "PLAITA_AI_JUDGE_API_KEY": "   "}) is False
    assert judge_configured(FULL_ENV) is True


def test_keyless_judge_sends_no_request(monkeypatch):
    monkeypatch.setattr(httpx, "post", lambda *a, **k: pytest.fail("keyless judge must not be called"))
    assert judge_output("rubric", {}, {"a": 1}, env=HALF_ENV) is None


def test_transport_failure_is_marked_unavailable(monkeypatch):
    def boom(*args, **kwargs):
        raise httpx.ConnectError("no route to judge")

    monkeypatch.setattr(httpx, "post", boom)
    verdict = judge_output("rubric", {}, {"a": 1}, env=FULL_ENV)
    assert verdict is not None
    assert verdict["unavailable"] is True
    assert verdict["ok"] is False


@pytest.mark.parametrize("content", [None, [{"type": "text", "text": "hi"}]])
def test_textless_reply_is_marked_unavailable(monkeypatch, content):
    """A 200 whose message content is null / a parts list is a broken judge
    envelope, not a verdict — it must not escape as a TypeError and take the
    whole eval run down with it."""
    def reply(*args, **kwargs):
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": content}}]},
            request=httpx.Request("POST", "http://judge.local/v1/chat/completions"),
        )

    monkeypatch.setattr(httpx, "post", reply)
    verdict = judge_output("rubric", {}, {"a": 1}, env=FULL_ENV)
    assert verdict is not None
    assert verdict["unavailable"] is True
    assert verdict["ok"] is False


# -- verdict normalization (A2R32-04) -------------------------------------------
#
# A judge reply is only a verdict about the output when it is a JSON object
# with a finite-number ``score`` and a real-bool ``ok``. Everything else is a
# broken reply envelope and must degrade to ``unavailable`` — never fold a
# fake 0.0/1.0 into the average, never crash the eval run.


def _judge_reply(content: str):
    def handler(*args, **kwargs):
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": content}}]},
            request=httpx.Request("POST", "http://judge.local/v1/chat/completions"),
        )

    return handler


BROKEN_VERDICT_REPLIES = [
    # NaN/Infinity are JSON-parsable but not finite: max(0, min(1, nan))
    # keeps its first argument (NaN comparisons are False) → a full-score
    # pass out of a judge that never scored.
    '{"ok": true, "score": NaN, "reason": "bad"}',
    '{"ok": true, "score": Infinity, "reason": "bad"}',
    # "false" was bool()-truthy: an explicit rejection read as a pass.
    '{"ok": "false", "score": 1.0, "reason": "bad"}',
    '{"ok": "true", "score": 0.0, "reason": "bad"}',
    # Legal JSON but not an object: verdict.get used to raise AttributeError
    # straight through judge_output (outside its try / except only caught
    # TypeError/ValueError), killing the whole eval run.
    "[1, 2]",
    "null",
    "123",
    '"polite"',
    # Missing fields used to default (score→0.0 as a "real" 0 verdict,
    # ok→derived from score): both are broken replies, not judgments.
    '{"ok": true, "reason": "no score field"}',
    '{"score": 0.8, "reason": "no ok field"}',
    # Plain prose (no braces) used to score 0 with no unavailable stamp and
    # pollute the average through assert_score.
    "The output looks polite and correct to me.",
    # Braces present but no JSON inside — same broken-envelope family.
    "Verdict: {ok true score 1.0}",
]


@pytest.mark.parametrize("content", BROKEN_VERDICT_REPLIES)
def test_broken_verdict_shapes_degrade_to_unavailable(monkeypatch, content):
    monkeypatch.setattr(httpx, "post", _judge_reply(content))
    verdict = judge_output("rubric", {}, {"a": 1}, env=FULL_ENV)
    assert verdict is not None
    assert verdict["unavailable"] is True
    assert verdict["ok"] is False
    assert verdict["score"] == 0.0


def test_nan_reply_keeps_content_score_out_of_average(monkeypatch):
    """Through assert_score: NaN must not fold in as a full-score pass."""
    for key, value in FULL_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(
        httpx, "post", _judge_reply('{"ok": true, "score": NaN, "reason": "bad"}')
    )
    scored = assert_score(Case("c", {}, {"judge": "is polite"}), {"msg": "hi"})
    assert scored["judge"]["unavailable"] is True
    assert scored["score"] == 1.0  # content score untouched, nothing folded in


def test_prose_reply_keeps_content_score_out_of_average(monkeypatch):
    """Through assert_score: plain prose must not fold in as a 0 verdict."""
    for key, value in FULL_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(
        httpx, "post", _judge_reply("The output looks polite and correct to me.")
    )
    scored = assert_score(Case("c", {}, {"judge": "is polite"}), {"msg": "hi"})
    assert scored["judge"]["unavailable"] is True
    assert scored["score"] == 1.0  # was 0.5 (content 1.0 ⊕ judge 0.0) before
    assert scored["reasons"] == ["judge reply was not JSON"]


def test_non_object_reply_does_not_break_the_eval_run(monkeypatch):
    """[1,2] / null must degrade to unavailable, not raise AttributeError."""
    for content in ("[1, 2]", "null"):
        monkeypatch.setattr(httpx, "post", _judge_reply(content))
        verdict = judge_output("rubric", {}, {"a": 1}, env=FULL_ENV)
        assert verdict is not None
        assert verdict["unavailable"] is True


def test_ok_bool_wins_over_score(monkeypatch):
    """With ok strict-bool, an explicit judge rejection stays a rejection
    even when the score field contradicts it (ok 只由 bool 决定)."""
    monkeypatch.setattr(
        httpx, "post", _judge_reply('{"ok": false, "score": 1.0, "reason": "bad"}')
    )
    verdict = judge_output("rubric", {}, {"a": 1}, env=FULL_ENV)
    assert verdict["ok"] is False
    assert "unavailable" not in verdict


@pytest.mark.parametrize(
    "content, ok, score",
    [
        ('{"ok": true, "score": 0.9, "reason": "good"}', True, 0.9),
        ('{"ok": false, "score": 0.0, "reason": "no"}', False, 0.0),
        # fenced / trailing-prose JSON tolerance conserved
        ('Verdict: {"ok": true, "score": 1.0, "reason": "good"} — end', True, 1.0),
        # finite out-of-range scores still clamp; int scores accepted
        ('{"ok": true, "score": 2.5}', True, 1.0),
        ('{"ok": true, "score": 1}', True, 1.0),
    ],
)
def test_well_formed_verdicts_score_unchanged(monkeypatch, content, ok, score):
    monkeypatch.setattr(httpx, "post", _judge_reply(content))
    verdict = judge_output("rubric", {}, {"a": 1}, env=FULL_ENV)
    assert verdict is not None
    assert "unavailable" not in verdict
    assert verdict["ok"] is ok
    assert verdict["score"] == score


# -- scoring -------------------------------------------------------------------


def test_assert_score_marks_judge_unavailable(monkeypatch):
    for key, value in FULL_ENV.items():
        monkeypatch.setenv(key, value)

    def boom(*args, **kwargs):
        raise httpx.ConnectError("no route to judge")

    monkeypatch.setattr(httpx, "post", boom)
    scored = assert_score(Case("c", {}, {"judge": "is polite"}), {"msg": "hi"})
    assert scored["judge"]["unavailable"] is True
    assert scored["score"] == 1.0  # content score untouched, no 0.0 folded in
    assert scored["reasons"] == ["judge endpoint unavailable"]


def test_evaluate_excludes_judge_unavailable_case(tmp_path, monkeypatch):
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}'}, published="1.0.0")
    fake_openai_output(fake)
    verdicts = iter([unavailable(), healthy()])
    monkeypatch.setattr("plaita_ai.evals.evals.judge_output", lambda *a, **k: next(verdicts))

    report = evaluate(make_client(fake), "demo", "1.0.0", judge_dataset(tmp_path))
    assert report["judge_unavailable_cases"] == 1
    assert report["avg_score"] == 1.0
    assert report["pass_rate"] == 1.0


def test_evaluate_all_judge_unavailable_has_no_average(tmp_path, monkeypatch):
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}'}, published="1.0.0")
    fake_openai_output(fake)
    monkeypatch.setattr("plaita_ai.evals.evals.judge_output", lambda *a, **k: unavailable())

    report = evaluate(make_client(fake), "demo", "1.0.0", judge_dataset(tmp_path))
    assert report["judge_unavailable_cases"] == 2
    assert report["avg_score"] is None
    assert report["pass_rate"] is None


def test_compare_ignores_judge_unavailable_cases():
    base = {"flow_id": "demo", "version": "1.0.0", "pass_rate": 0.0, "avg_score": 0.5,
            "cases": [{"id": "c", "ok": False, "score": 0.5, "judge": unavailable()}]}
    candidate = {"flow_id": "demo", "version": "1.0.1", "pass_rate": 1.0, "avg_score": 1.0,
                 "cases": [{"id": "c", "ok": True, "score": 1.0, "judge": healthy()}]}
    verdict = compare(base, candidate)
    assert verdict["improvements"] == []
    assert verdict["regressions"] == []
    assert verdict["cases"][0]["judge_unavailable"] is True


# -- the promote gate ----------------------------------------------------------


def test_supervisor_refuses_promotion_when_judge_was_down(tmp_path, monkeypatch):
    """Baseline judged during an outage, candidate after recovery, identical
    definitions — the gate must not read the outage as an improvement."""
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}'}, published="1.0.0")
    fake_openai_output(fake)

    calls = {"n": 0}

    def judge(*args, **kwargs):
        calls["n"] += 1
        return unavailable() if calls["n"] <= 2 else healthy()

    monkeypatch.setattr("plaita_ai.evals.evals.judge_output", judge)
    dataset = judge_dataset(tmp_path)
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(promote_gate="manual", min_improvement=0.02),
        StaticProposer([{"definition": '{"v": 1}', "rationale": "unchanged rewrite"}]),
    )
    outcome = sup.iterate("demo", dataset)
    assert outcome["status"] != "improved"
    assert "promotion_ticket" not in outcome
    assert outcome["judge_unavailable_cases"] == {"baseline": 2, "candidate": 0}
    assert fake.published["demo"] == "1.0.0"


def test_judge_recovery_rebaselines_instead_of_staying_blind(tmp_path, monkeypatch):
    """Outage during the *baseline* must not blind the session for good.

    The baseline report is cached, but it is only refreshed on an improvement
    and a judge-unavailable baseline can never improve — so caching the
    outage report would pin ``judge_blind`` for the process lifetime.
    """
    def_v1, def_v2 = '{"version": 1}', '{"version": 2}'
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": def_v1}, published="1.0.0")
    fake.dry_run_handler = lambda flow_json, input_data: {
        "result": {"text": "good output" if flow_json == def_v2 else "bad output"},
        "nodes": [], "error": None,
    }

    calls = {"n": 0}

    def judge(*args, **kwargs):
        calls["n"] += 1
        return unavailable() if calls["n"] <= 2 else healthy()

    monkeypatch.setattr("plaita_ai.evals.evals.judge_output", judge)
    dataset = Dataset(
        [Case(f"c{i}", {"i": i}, {"contains": "good", "judge": "is polite"}) for i in range(2)],
        source="recovery",
    )
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(promote_gate="manual", min_improvement=0.02),
        StaticProposer([
            {"definition": def_v2, "rationale": "emit the good marker"},
            {"definition": def_v2, "rationale": "emit the good marker"},
        ]),
    )

    first = sup.iterate("demo", dataset)
    assert first["judge_unavailable_cases"] == {"baseline": 2, "candidate": 0}
    assert first["status"] == "no_improvement"
    assert "judge unavailable" in first["reason"]

    second = sup.iterate("demo", dataset)
    assert second["judge_unavailable_cases"] == {"baseline": 0, "candidate": 0}
    assert second["status"] == "improved"
    assert second["promotion_ticket"]["version"] == "1.0.2"


# -- #229：judge_blind 闸（候选 avg 更高 + 无 regressions，但 one side 盲评） -------


def _judge_trading_handler(flow_json, input_data):
    """基线过 c0/c2、挂 c1；候选全过（avg 0.5 → 1.0，无 regressions）。"""
    i = int(input_data.get("i", 0))
    if flow_json == '{"version": 2}':
        ok = True
    else:
        ok = i in (0, 2)
    return {
        "result": {"text": "good output" if ok else "bad output"},
        "nodes": [],
        "error": None,
    }


@pytest.mark.parametrize("gate", ["manual", "auto"])
def test_judge_blind_blocks_promotion_when_candidate_avg_is_higher(
    tmp_path, monkeypatch, gate
):
    """#229-B：baseline 有 judge_unavailable 用例、候选 avg 更高且无 regressions。

    judge 故障期间的分数与基线不可比（#60）：不能把「outage 结束」读成改进。
    既有 ``test_supervisor_refuses_promotion_when_judge_was_down`` 的基线/候选
    定义相同，走不到「avg 升 + 无 regressions + 盲评」的组合。
    """
    def_v1, def_v2 = '{"version": 1}', '{"version": 2}'
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": def_v1}, published="1.0.0")
    fake.dry_run_handler = _judge_trading_handler

    calls = {"n": 0}

    def judge(*args, **kwargs):
        calls["n"] += 1
        n = calls["n"]
        if n == 1:
            # baseline c0：judge 不可达 → 该 case 排除出均值（不计 0 分）
            return unavailable()
        if n in (2, 3):
            # baseline c1/c2 的健康判分镜像内容分，保持 avg=0.5
            return {"ok": n == 3, "score": 1.0 if n == 3 else 0.0, "reason": "healthy"}
        return healthy()

    monkeypatch.setattr("plaita_ai.evals.evals.judge_output", judge)

    dataset = Dataset(
        [
            {"id": f"c{i}", "input": {"i": i},
             "expect": {"contains": "good", "judge": "is polite"}}
            for i in range(3)
        ],
        source="judge-blind",
    )
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
        StaticProposer([{"definition": def_v2, "rationale": "outage ended"}]),
    )
    outcome = sup.iterate("demo", dataset)

    assert outcome["judge_unavailable_cases"] == {"baseline": 1, "candidate": 0}
    comparison = outcome["comparison"]
    assert comparison["avg_score"]["base"] == 0.5
    assert comparison["avg_score"]["candidate"] == 1.0
    assert comparison["regressions"] == []
    assert comparison["improvements"] == ["c1"]
    assert outcome["status"] == "no_improvement"
    assert "promotion_ticket" not in outcome
    assert outcome.get("promoted") is not True
    assert published_calls == []
    assert fake.published["demo"] == "1.0.0"
    assert "judge unavailable" in outcome.get("reason", "")
    assert "not comparable" in outcome.get("reason", "")
