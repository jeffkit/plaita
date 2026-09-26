"""Supervisor 真机端到端 demo:对着一个运行中的 plaita-console,跑一轮完整的
自迭代——基线评测 → LLM 提案 → 保存候选版本 → 评测对比 → promotion ticket →
(人工确认后)发布 → 复评。

前置:
    export PLAITA_CONSOLE_URL=http://127.0.0.1:8123
    export PLAITA_CONSOLE_ADMIN_API_KEY=...
    export PLAITA_AI_PROPOSER_BASE_URL=https://api.deepseek.com   # OpenAI 兼容
    export PLAITA_AI_PROPOSER_MODEL=deepseek-chat
    export PLAITA_AI_PROPOSER_API_KEY=...

用法:
    python demo_e2e.py             # 跑迭代,打印 promotion ticket(不发布)
    python demo_e2e.py --apply     # 脚本扮演"人",确认 ticket 并发布后复评
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from plaita_ai.console_client import ConsoleClient, ConsoleClientError, client_from_env
from plaita_ai.evals import evaluate
from plaita_ai.supervisor import PromptProposer, Supervisor, SupervisorPolicy

FLOW_ID = "greeter"
# 有意写死的 bug:v1 永远问候 "world",不读 INPUT.name —— 等着 supervisor 修。
BUGGY_V1 = json.dumps(
    {
        "runtime": "python",
        "flow_id": FLOW_ID,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "name"},
            {"type": "assignment", "id": "name", "output": "world", "next": "_n1"},
            {"type": "end", "id": "_n1", "output": '$F.concat("hello ", $NODE.name)', "resultType": "success"},
        ],
    },
    ensure_ascii=False,
)

DATASET = [
    {"id": "ada", "input": {"name": "ada"}, "expect": {"contains": "hello ada"}},
    {"id": "bob", "input": {"name": "bob"}, "expect": {"contains": "hello bob"}},
    {"id": "carol", "input": {"name": "carol"}, "expect": {"contains": "hello carol"}},
]


def ensure_flow_with_bug(client: ConsoleClient) -> None:
    try:
        client.get_flow(FLOW_ID)
    except ConsoleClientError:
        print(f"[setup] flow {FLOW_ID} 不存在,创建")
        client.create_flow(FLOW_ID, desc="supervisor e2e demo")
    try:
        client.get_version(FLOW_ID, "1.0.0")
    except ConsoleClientError:
        print("[setup] 写入有 bug 的 1.0.0 并发布")
        client.save_version(FLOW_ID, "1.0.0", BUGGY_V1, created_by="demo-seed")
        client.publish_version(FLOW_ID, "1.0.0")


def main() -> int:
    apply = "--apply" in sys.argv
    client = client_from_env()
    ensure_flow_with_bug(client)

    here = Path(__file__).parent
    dataset_file = here / "dataset.json"
    dataset_file.write_text(json.dumps({"cases": DATASET}, ensure_ascii=False), encoding="utf-8")

    from plaita_ai.evals import load_dataset

    dataset = load_dataset(str(dataset_file))

    baseline = evaluate(client, FLOW_ID, "1.0.0", dataset, mode="dry-run")
    print(f"[baseline] v1.0.0 pass_rate={baseline['pass_rate']} avg={baseline['avg_score']}")

    sup = Supervisor(
        client,
        policy=SupervisorPolicy(promote_gate="manual", min_improvement=0.02, eval_timeout_s=60),
        proposer=PromptProposer(),
    )
    outcome = sup.iterate(FLOW_ID, dataset)
    print(f"[iterate] status={outcome['status']} candidate={outcome.get('candidate_version')}")
    print(f"[iterate] rationale: {outcome.get('rationale', '')[:200]}")
    comparison = outcome.get("comparison") or {}
    print(f"[compare] pass_rate {comparison.get('pass_rate')}")

    ticket = outcome.get("promotion_ticket")
    if not ticket:
        print("[iterate] 没有 promotion ticket,到此为止")
        return 1

    print("[ticket] 需要人工确认:", ticket["command"])
    if not apply:
        print("[done] --apply 可让脚本扮演人完成发布复评")
        return 0

    # —— 以下是人类动作 ——
    client.publish_version(FLOW_ID, ticket["version"])
    print(f"[human] 已发布 {ticket['version']}")
    final = evaluate(client, FLOW_ID, ticket["version"], dataset, mode="dry-run")
    print(f"[final] v{ticket['version']} pass_rate={final['pass_rate']} avg={final['avg_score']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
