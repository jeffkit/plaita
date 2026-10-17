"""#59 回归：并发迭代的候选版本号必须由服务端原子分配。

旧实现的链路是「GET /flows/{id} 拿版本列表 → 客户端 ``next_patch_version``
自算 → 不带任何并发控制字段 PUT 回同一个槽位」。两个针对同一 ``flow_id``
的迭代只要读到同一份版本快照（下面用冻结的 GET 响应模拟），就会算出同一个
版本号并先后写进同一槽位，后写静默覆盖先写。

修复后 ``save_version`` 带 ``allocate_version=True``（服务端按库内最大 semver
原子分配 patch+1 并在响应 ``version`` 里通告实际号），断言：

1. 两次迭代拿到的 ``candidate_version`` 互不相同，且各自槽位里的定义与产出
   它的那次迭代一致（允许同一版本号只落一份的情况不再出现）；
2. 升级票的版本号取自服务端返回值（本地算出的占位号作废）；
3. ``ConsoleClient.save_version`` 把 ``base_updated_at`` / ``force`` /
   ``allocate_version`` 三个并发控制字段带进报文体（MCP 侧同一入口）。
"""

from __future__ import annotations

import json

import httpx

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai.evals import Dataset
from plaita_ai.supervisor import Supervisor, SupervisorPolicy

from ._console_fake import FakeConsole


DEF_V1 = '{"version": 1}'
DEF_V2 = '{"version": 2}'  # regression: emits the worse marker
DEF_V3 = '{"version": 3}'  # improvement: emits the good marker

POLICY = SupervisorPolicy(promote_gate="manual", min_improvement=0.02)
BASELINE = {"version": "1.0.0", "avg_score": 0.0, "judge_unavailable_cases": 0}


def make_dataset(cases: int = 2) -> Dataset:
    dataset = Dataset([{"id": f"c{i}", "input": {"i": i}} for i in range(cases)])  # type: ignore[list-item]
    for case in dataset.cases:
        case.expect = {"contains": "good"}
    return dataset


def seed_flow(fake: FakeConsole) -> FakeConsole:
    fake.seed_flow("demo", {"1.0.0": DEF_V1}, published="1.0.0")

    def handler(flow_json, input_data):
        if flow_json == DEF_V3:
            out = {"text": "good output"}
        elif flow_json == DEF_V2:
            out = {"text": "worse output"}
        else:
            out = {"text": "bad output"}
        return {"result": out, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    return fake


def frozen_flow_snapshot(fake: FakeConsole, flow_id: str = "demo") -> bytes:
    """The version list both concurrent iterations read *before* either writes."""
    response = fake.handler(
        httpx.Request(
            "GET",
            f"http://fake/api/flows/{flow_id}",
            headers={"X-Admin-API-Key": "test-key"},
        )
    )
    assert response.status_code == 200
    return response.content


def client_with_frozen_snapshot(
    fake: FakeConsole, snapshot: bytes, flow_id: str = "demo"
) -> ConsoleClient:
    """A client whose ``GET /flows/{id}`` always returns the frozen snapshot —
    i.e. the interleaving where both iterations read before either writes."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/api/flows/{flow_id}":
            return httpx.Response(200, content=snapshot)
        return fake.handler(request)

    return ConsoleClient(
        ConsoleConfig(base_url="http://fake", admin_api_key="test-key"),
        transport=httpx.MockTransport(handler),
    )


def test_concurrent_iterations_allocate_distinct_versions():
    fake = seed_flow(FakeConsole())
    snapshot = frozen_flow_snapshot(fake)
    client = client_with_frozen_snapshot(fake, snapshot)
    dataset = make_dataset()

    # A writes first (regression), B second (the improvement) — from B's point
    # of view the version list still only has 1.0.0.
    sup_a = Supervisor(client, POLICY)
    sup_b = Supervisor(client, POLICY)
    out_a = sup_a.evaluate_proposal(
        "demo", dataset, {"definition": DEF_V2, "rationale": "A"}, baseline=dict(BASELINE)
    )
    out_b = sup_b.evaluate_proposal(
        "demo", dataset, {"definition": DEF_V3, "rationale": "B"}, baseline=dict(BASELINE)
    )

    assert out_a["candidate_version"] != out_b["candidate_version"], (
        "两次并发迭代拿到了同一个候选版本号——后写会静默覆盖先写"
    )

    stored = {v: view["definition"] for v, view in fake.flows["demo"]["versions"].items()}
    assert stored[out_a["candidate_version"]] == DEF_V2
    assert stored[out_b["candidate_version"]] == DEF_V3, (
        "候选版本号与其定义不匹配：票面分数与将被发布的定义脱钩"
    )
    assert set(stored) == {"1.0.0", "1.0.1", "1.0.2"}, f"候选被覆盖: {sorted(stored)}"

    # The promote ticket must carry the *server-allocated* version: B still
    # computed 1.0.1 locally from the frozen snapshot, the server returned 1.0.2.
    assert out_b["status"] == "improved"
    assert out_b["candidate_version"] == "1.0.2"
    assert out_b["promotion_ticket"]["version"] == out_b["candidate_version"]
    assert "1.0.2" in out_b["promotion_ticket"]["command"]


def test_save_version_carries_concurrency_control_fields():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content.decode("utf-8"))
        seen["path"] = request.url.path
        return httpx.Response(
            200, json={"flow_id": "demo", "version": "1.0.2", "status": "draft"}
        )

    client = ConsoleClient(
        ConsoleConfig(base_url="http://fake", admin_api_key="test-key"),
        transport=httpx.MockTransport(handler),
    )
    client.save_version(
        "demo",
        "1.0.1",
        DEF_V2,
        base_updated_at="2026-10-16T00:00:00",
        force=True,
        allocate_version=True,
    )

    assert seen["path"] == "/api/flows/demo/versions/1.0.1"
    assert seen["body"]["base_updated_at"] == "2026-10-16T00:00:00"
    assert seen["body"]["force"] is True
    assert seen["body"]["allocate_version"] is True


def test_evaluate_proposal_asks_server_to_allocate_and_uses_returned_version():
    """Even when the server allocates a version the client never computed, the
    result and ticket follow the server (contract: response ``version`` wins)."""
    fake = seed_flow(FakeConsole())
    calls: list = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8")) if request.content else {}
        if request.method == "PUT" and "/versions/" in request.url.path:
            calls.append(body)
            # The server ignores the path version and allocates 1.0.7.
            view = fake.put_version("demo", "1.0.7", body["definition"])
            return httpx.Response(200, json=view)
        return fake.handler(request)

    client = ConsoleClient(
        ConsoleConfig(base_url="http://fake", admin_api_key="test-key"),
        transport=httpx.MockTransport(handler),
    )
    sup = Supervisor(client, POLICY)
    out = sup.evaluate_proposal(
        "demo", make_dataset(), {"definition": DEF_V3, "rationale": "x"}, baseline=dict(BASELINE)
    )

    assert calls and calls[0].get("allocate_version") is True
    assert out["candidate_version"] == "1.0.7"
    assert out["promotion_ticket"]["version"] == "1.0.7"
