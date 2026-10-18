"""plaita#226 A6：Supervisor 默认 `promote_gate` 必须是 manual（人工放行）。

自改进循环的默认策略若回归成 `auto`，`Supervisor.iterate` 会直接
`client.publish_version(...)`——绕过人工 promotion ticket。既有测试：
- `test_improved_candidate_returns_promotion_ticket` 显式传 manual；
- `test_regression_is_not_promoted` 用默认策略但候选**未改进**（走不到 gate）；
- `test_auto_gate_publishes` 显式传 auto。
所以「**默认值**是 manual」与「默认策略下改进候选不发布」两点无钉。

负控：把 `SupervisorPolicy.promote_gate` 默认值改为 `"auto"` ⇒ 本文件
两条用例红（默认值断言 + 默认策略实际发布了 1.0.1）。证据见
`docs/proofs/226-six-regression-gaps.md` 与 `docs/proofs/data/226-*`。
"""
from __future__ import annotations

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai.evals import Dataset
from plaita_ai.supervisor import StaticProposer, Supervisor, SupervisorPolicy

from ._console_fake import FakeConsole

DEF_V1 = '{"version": 1}'
DEF_V2 = '{"version": 2}'


def _client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(
        ConsoleConfig(base_url="http://fake", admin_api_key="test-key"),
        transport=fake.transport(),
    )


def _dataset() -> Dataset:
    dataset = Dataset([{"id": "c0", "input": {"i": 0}}])  # type: ignore[list-item]
    for case in dataset.cases:
        case.expect = {"contains": "good"}
    return dataset


def _seed_with_improvement(fake: FakeConsole) -> FakeConsole:
    fake.seed_flow("demo", {"1.0.0": DEF_V1}, published="1.0.0")

    def handler(flow_json, input_data):
        out = {"text": "good output"} if flow_json == DEF_V2 else {"text": "bad output"}
        return {"result": out, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    return fake


def test_default_promote_gate_is_manual():
    assert SupervisorPolicy().promote_gate == "manual", (
        "SupervisorPolicy 默认 promote_gate 必须是 manual（人工放行）；"
        "auto 会使自改进循环绕过 promotion ticket 直接发布"
    )


def test_default_policy_returns_ticket_and_does_not_publish():
    """默认策略（不传 promote_gate）下，改进候选只出 ticket，不发布。"""
    fake = _seed_with_improvement(FakeConsole())
    supervisor = Supervisor(
        _client(fake),
        SupervisorPolicy(min_improvement=0.02),  # 默认 promote_gate
        StaticProposer([{"definition": DEF_V2, "rationale": "emit good marker"}]),
    )
    outcome = supervisor.iterate("demo", _dataset())

    assert outcome["status"] == "improved"
    assert outcome["promoted"] is False, "默认策略不得自动发布（绕过人工放行）"
    assert outcome["promotion_ticket"]["version"] == "1.0.1"
    assert fake.published["demo"] == "1.0.0", (
        f"默认策略发布了候选版本: {fake.published['demo']!r}"
    )
