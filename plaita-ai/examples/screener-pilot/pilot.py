"""issue-keeper screener × plaita-ai supervisor 真实试点。

把 issue-keeper 前置安全过滤(decision 后端的判定配置)固化成一条 plaita flow,
用真实语料(ik-selftest 仓库的真实 issue + 典型注入样本)做评测集,跑 supervisor
自迭代:基线评测 → LLM 提案(改 question/choices/置信门控)→ 候选评测对比 →
promotion ticket(人工发布)。

前置:
    console 已启动且进程环境带 DEEPSEEK_API_KEY(flow 定义经 $ENV 引用);
    PLAITA_CONSOLE_URL / PLAITA_CONSOLE_ADMIN_API_KEY / PLAITA_AI_PROPOSER_* 已配置。

用法:
    python pilot.py           # 基线 + 一轮迭代,打印 ticket
    python pilot.py --apply   # 迭代后由脚本扮演人完成发布复评
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from plaita_ai.console_client import ConsoleClient, ConsoleClientError, client_from_env
from plaita_ai.evals import evaluate, load_dataset
from plaita_ai.supervisor import PromptProposer, Supervisor, SupervisorPolicy

FLOW_ID = "issue-screener"
HERE = Path(__file__).parent


def ensure_flow(client: ConsoleClient) -> None:
    definition = (HERE / "flow-1.0.0.json").read_text(encoding="utf-8")
    try:
        client.get_flow(FLOW_ID)
    except ConsoleClientError:
        print(f"[setup] 创建 flow {FLOW_ID}")
        client.create_flow(FLOW_ID, desc="issue-keeper screener decision as a plaita flow")
    try:
        client.get_version(FLOW_ID, "1.0.0")
    except ConsoleClientError:
        print("[setup] 写入生产判定配置 1.0.0 并发布")
        client.save_version(FLOW_ID, "1.0.0", definition, created_by="pilot-seed")
        client.publish_version(FLOW_ID, "1.0.0")


def main() -> int:
    apply = "--apply" in sys.argv
    client = client_from_env()
    ensure_flow(client)

    dataset = load_dataset(str(HERE / "dataset"))
    print(f"[dataset] {len(dataset)} cases")

    sup = Supervisor(
        client,
        policy=SupervisorPolicy(promote_gate="manual", min_improvement=0.02, eval_timeout_s=60),
        proposer=PromptProposer(),
    )
    baseline = sup.baseline(FLOW_ID, dataset)
    print(f"[baseline] v{baseline['version']} pass_rate={baseline['pass_rate']} avg={baseline['avg_score']}")
    for case in baseline["cases"]:
        if not case["ok"]:
            print(f"  ✗ {case['id']}: {case.get('reasons') or case.get('error')}")

    outcome = sup.iterate(FLOW_ID, dataset)
    print(f"[iterate] status={outcome['status']} candidate={outcome.get('candidate_version')}")
    print(f"[iterate] rationale: {outcome.get('rationale', '')[:300]}")
    comparison = outcome.get("comparison") or {}
    print(f"[compare] {json.dumps(comparison.get('pass_rate'), ensure_ascii=False)} "
          f"improvements={comparison.get('improvements')} regressions={comparison.get('regressions')}")

    ticket = outcome.get("promotion_ticket")
    if not ticket:
        print("[done] 无 ticket(基线全对或候选未改善)——这本身就是有效的试点结论")
        return 0

    print("[ticket]", ticket["command"])
    if not apply:
        return 0
    client.publish_version(FLOW_ID, ticket["version"])
    print(f"[human] 已发布 {ticket['version']}")
    final = evaluate(client, FLOW_ID, ticket["version"], dataset, mode="dry-run")
    print(f"[final] v{ticket['version']} pass_rate={final['pass_rate']} avg={final['avg_score']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
