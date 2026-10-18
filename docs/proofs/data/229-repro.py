#!/usr/bin/env python3
"""plaita#229 repro / 探针：Supervisor 候选放行的两道条件闸。

A. **regressions 闸**：3 case 数据集，基线过 c0、挂 c1/c2；候选过 c1/c2、打回 c0
   （avg 1/3 → 2/3 更高）。预期：``status=no_improvement``、无 promotion ticket、
   未调用 ``publish_version``（``promote_gate`` 取 manual 与 auto 都一样）。
B. **judge_blind 闸**：baseline 有 1 个 judge 不可达 case（avg 0.5），候选 avg 1.0
   且无 regressions。预期：``status=no_improvement``、reason 保持
   ``judge unavailable ... not comparable``、不出 ticket、不 publish。

运行：:

    PYTHONPATH=/tmp/w47:/tmp/w47/plaita-ai /Users/jeff.jie/.venvs/plaita/bin/python \
        docs/proofs/data/229-repro.py

退出码 0 = 两道闸都成立；1 = 有闸失效（变异实现）。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "plaita-ai"))
sys.path.insert(0, str(REPO / "plaita-ai" / "tests"))

from _console_fake import FakeConsole  # noqa: E402
from plaita_ai.console_client import ConsoleClient, ConsoleConfig  # noqa: E402
from plaita_ai.evals import Dataset  # noqa: E402
from plaita_ai.supervisor import (  # noqa: E402
    StaticProposer,
    Supervisor,
    SupervisorPolicy,
)

DEF_V1 = '{"version": 1}'
DEF_V2 = '{"version": 2}'


def _client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(
        ConsoleConfig(base_url="http://fake", admin_api_key="test-key"),
        transport=fake.transport(),
    )


def _instrument_publish(client: ConsoleClient) -> list:
    calls: list = []
    real = client.publish_version

    def recording(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    client.publish_version = recording
    return calls


def _probe_regressions(gate: str) -> dict:
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": DEF_V1}, published="1.0.0")

    def handler(flow_json, input_data):
        i = int(input_data.get("i", 0))
        ok = i in (1, 2) if flow_json == DEF_V2 else i == 0
        return {"result": {"text": "good output" if ok else "bad output"},
                "nodes": [], "error": None}

    fake.dry_run_handler = handler
    client = _client(fake)
    calls = _instrument_publish(client)
    dataset = Dataset([
        {"id": f"c{i}", "input": {"i": i}, "expect": {"contains": "good"}}
        for i in range(3)
    ])
    sup = Supervisor(
        client,
        SupervisorPolicy(promote_gate=gate, min_improvement=0.02),
        StaticProposer([{"definition": DEF_V2, "rationale": "trade c0 for c1/c2"}]),
    )
    outcome = sup.iterate("demo", dataset)
    return {
        "gate": gate,
        "status": outcome["status"],
        "has_ticket": "promotion_ticket" in outcome,
        "promoted": outcome.get("promoted"),
        "publish_calls": len(calls),
        "published_version": fake.published.get("demo"),
        "avg": outcome["comparison"]["avg_score"],
        "regressions": outcome["comparison"]["regressions"],
    }


def _probe_judge_blind(gate: str) -> dict:
    import plaita_ai.evals.evals as evals_mod

    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": DEF_V1}, published="1.0.0")

    def handler(flow_json, input_data):
        i = int(input_data.get("i", 0))
        ok = True if flow_json == DEF_V2 else i in (0, 2)
        return {"result": {"text": "good output" if ok else "bad output"},
                "nodes": [], "error": None}

    fake.dry_run_handler = handler
    calls = {"n": 0}

    def judge(*args, **kwargs):
        calls["n"] += 1
        n = calls["n"]
        if n == 1:
            return {"ok": False, "score": 0.0, "unavailable": True,
                    "reason": "judge endpoint unavailable"}
        if n in (2, 3):
            return {"ok": n == 3, "score": 1.0 if n == 3 else 0.0, "reason": "healthy"}
        return {"ok": True, "score": 1.0, "reason": "looks fine"}

    original = evals_mod.judge_output
    evals_mod.judge_output = judge
    try:
        client = _client(fake)
        publish_calls = _instrument_publish(client)
        dataset = Dataset(
            [
                {"id": f"c{i}", "input": {"i": i},
                 "expect": {"contains": "good", "judge": "is polite"}}
                for i in range(3)
            ],
            source="judge-blind",
        )
        sup = Supervisor(
            client,
            SupervisorPolicy(promote_gate=gate, min_improvement=0.02),
            StaticProposer([{"definition": DEF_V2, "rationale": "outage ended"}]),
        )
        outcome = sup.iterate("demo", dataset)
    finally:
        evals_mod.judge_output = original
    return {
        "gate": gate,
        "status": outcome["status"],
        "has_ticket": "promotion_ticket" in outcome,
        "promoted": outcome.get("promoted"),
        "publish_calls": len(publish_calls),
        "published_version": fake.published.get("demo"),
        "judge_unavailable_cases": outcome["judge_unavailable_cases"],
        "avg": outcome["comparison"]["avg_score"],
        "regressions": outcome["comparison"]["regressions"],
        "reason": outcome.get("reason", ""),
    }


def main() -> int:
    report = {
        "regressions": [_probe_regressions(g) for g in ("manual", "auto")],
        "judge_blind": [_probe_judge_blind(g) for g in ("manual", "auto")],
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    ok = True
    for row in report["regressions"]:
        ok = ok and row["status"] == "no_improvement" and not row["has_ticket"]
        ok = ok and row["publish_calls"] == 0 and row["published_version"] == "1.0.0"
        ok = ok and row["regressions"] == ["c0"]
    for row in report["judge_blind"]:
        ok = ok and row["status"] == "no_improvement" and not row["has_ticket"]
        ok = ok and row["publish_calls"] == 0 and row["published_version"] == "1.0.0"
        ok = ok and row["judge_unavailable_cases"] == {"baseline": 1, "candidate": 0}
        ok = ok and "judge unavailable" in row["reason"] and "not comparable" in row["reason"]
    print("\nGATES HOLD" if ok else "\nGATE REGRESSION")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
