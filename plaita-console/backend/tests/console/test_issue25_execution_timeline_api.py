"""plaita#25 —— 事件时间线可查 API（console）。

红先证：修复前 `GET /api/executions/{id}/timeline` 404（路由不存在），
且落盘状态里没有 timeline 字段可读（见 /tmp/w9-a-out/RED-PROOF.md）。

守卫点：
- 集群档读 worker 落盘的时间线，事件/排队时长原样透出；
- **老状态（无 timeline）必须与「有时间线但为空」可区分**：`collected=false`
  且 `queue_wait_ms=null`，绝不折算成 0；
- 本地档由 nodes_json 投影（本地执行的节点 trace 本就带 started_ms/ended_ms）。
"""

import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402

ADMIN_KEY = "w9a-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis():
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def client(redis, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers(tenant=None):
    h = {"X-Admin-API-Key": ADMIN_KEY}
    if tenant:
        h["X-Tenant-ID"] = tenant
    return h


def _seed(redis, execution_id, *, tenant=None, timeline="missing", status="completed"):
    ns = f"plaita:{tenant}" if tenant else "plaita"
    data = {
        "execution_id": execution_id,
        "flow_id": "f1",
        "flow_name": "Demo",
        "status": status,
        "start_time": "2026-09-01T00:00:00",
        "end_time": "2026-09-01T00:00:05",
        "context": {},
    }
    if timeline != "missing":
        data["timeline"] = timeline
    redis.set(f"{ns}:execution:{execution_id}", json.dumps(data))


_TIMELINE = {
    "events": [
        {"seq": 1, "type": "run_start", "at": "2026-09-01T00:00:01", "at_ms": 1000},
        {"seq": 2, "type": "node_start", "node_id": "a", "at_ms": 1100},
        {"seq": 3, "type": "node_end", "node_id": "a", "at_ms": 2100,
         "duration_ms": 1000, "ok": True},
        {"seq": 4, "type": "node_end", "node_id": "b", "at_ms": 3100,
         "duration_ms": 500, "ok": False, "error": "boom"},
        {"seq": 5, "type": "run_end", "at_ms": 3200, "status": "completed"},
    ],
    "dropped": 0,
    "enqueued_at": "2026-09-01T00:00:00",
    "queue_wait_ms": 1000,
}


class TestClusterTimelineEndpoint:
    def test_returns_persisted_timeline(self, redis, client):
        _seed(redis, "e1", timeline=_TIMELINE)
        resp = client.get("/api/executions/e1/timeline", headers=_headers())
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["collected"] is True
        assert body["queue_wait_ms"] == 1000
        assert [e["type"] for e in body["events"]][:2] == ["run_start", "node_start"]
        assert body["events"][3]["node_id"] == "b"
        assert body["events"][3]["ok"] is False
        assert body["dropped"] == 0

    def test_old_state_without_timeline_is_not_zero(self, redis, client):
        """未采集 ≠ 0：老状态必须显式说「没有」，不得折算成排队 0ms。"""
        _seed(redis, "e2", timeline="missing")
        resp = client.get("/api/executions/e2/timeline", headers=_headers())
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["collected"] is False
        assert body["events"] == []
        assert body["queue_wait_ms"] is None
        assert body["enqueued_at"] is None

    def test_unknown_execution_is_404(self, client):
        resp = client.get("/api/executions/nope/timeline", headers=_headers())
        assert resp.status_code == 404

    def test_tenant_scoped_lookup(self, redis, client):
        _seed(redis, "e3", tenant="acme", timeline=_TIMELINE)
        ok = client.get("/api/executions/e3/timeline", headers=_headers("acme"))
        assert ok.status_code == 200 and ok.json()["collected"] is True
        # 别的租户看不到（不得落到 default namespace 兜底）
        other = client.get("/api/executions/e3/timeline", headers=_headers("other"))
        assert other.status_code == 404


class TestLocalTimelineProjection:
    """本地档：由 nodes_json 投影（本地 trace 已带 started/ended ms）。"""

    def _info(self):
        return {
            "execution_id": "l1",
            "status": "completed",
            "start_time": "2026-09-01T00:00:00",
            "end_time": "2026-09-01T00:00:03",
            "nodes": [
                {"id": "start", "status": "success", "started_at": "2026-09-01T00:00:00.100",
                 "started_ms": 100, "ended_at": "2026-09-01T00:00:00.200",
                 "ended_ms": 200, "duration_ms": 100},
                {"id": "boom", "status": "error", "started_at": "2026-09-01T00:00:00.300",
                 "started_ms": 300, "ended_at": "2026-09-01T00:00:00.900",
                 "ended_ms": 900, "duration_ms": 600, "error": "kaboom"},
            ],
        }

    def test_projection_emits_node_events_and_marks_source(self):
        body = executions_api.project_local_timeline(self._info())
        assert body["collected"] is True
        assert body["source"] == "local_nodes_projection"
        types = [e["type"] for e in body["events"]]
        assert types == ["node_start", "node_end", "node_start", "node_end"]
        end = [e for e in body["events"] if e["type"] == "node_end"][1]
        assert end["node_id"] == "boom" and end["ok"] is False
        assert end["duration_ms"] == 600

    def test_projection_of_row_without_nodes_is_uncollected(self):
        body = executions_api.project_local_timeline(
            {"execution_id": "l2", "status": "running", "start_time": None, "nodes": []}
        )
        assert body["collected"] is False
        assert body["events"] == []
        assert body["queue_wait_ms"] is None

    def test_state_projection_of_missing_timeline_is_uncollected(self):
        body = executions_api.project_state_timeline({"execution_id": "e9", "status": "running"})
        assert body["collected"] is False
        assert body["events"] == []
        assert body["queue_wait_ms"] is None


# A2R49-02：timeline 信封是读面——events[*].error = str(exception)（worker 侧
# 构造无 scrub），集群/本地两路投影在修复前都原样透出；存量旧行（修复前落盘）
# 只能靠读面信封兜，故 scrub 落在信封构造处。
TL_NEEDLE = "sk-live-timeline-api-needle"


def _scrubbed_timeline():
    return {
        "events": [
            {"seq": 1, "type": "node_start", "node_id": "call", "attempt": 1, "at_ms": 1},
            {"seq": 2, "type": "node_end", "node_id": "call", "attempt": 1, "ok": False,
             "error": ("ConnectionError: Max retries exceeded with "
                       "url: /api/run?token=" + TL_NEEDLE),
             "duration_ms": 120},
            {"seq": 3, "type": "run_end", "status": "error", "final": True},
        ],
        "dropped": 0,
        "max_events": 500,
    }


class TestTimelineErrorFaceIsRedacted:
    """A2R49-02：``/timeline`` 信封对 ``events[*].error``（及 ``reason``）过 scrub。"""

    def test_cluster_timeline_error_has_no_needle(self, redis, client):
        _seed(redis, "tl-sec-1", timeline=_scrubbed_timeline())

        resp = client.get("/api/executions/tl-sec-1/timeline", headers=_headers())

        assert resp.status_code == 200, resp.text
        assert TL_NEEDLE not in resp.text
        body = resp.json()
        err = body["events"][1]["error"]
        assert "Max retries exceeded" in err, "诊断语义必须保留"

    def test_local_timeline_projection_error_has_no_needle(self):
        info = {
            "execution_id": "l-sec-1",
            "status": "error",
            "start_time": "2026-09-01T00:00:00",
            "nodes": [
                {"id": "boom", "status": "error", "started_at": "2026-09-01T00:00:00.300",
                 "started_ms": 300, "ended_at": "2026-09-01T00:00:00.900",
                 "ended_ms": 900, "duration_ms": 600,
                 "error": "ConnectionError: url /api/run?token=" + TL_NEEDLE},
            ],
        }

        body = executions_api.project_local_timeline(info)

        assert body["collected"] is True
        err = body["events"][-1]["error"]
        assert TL_NEEDLE not in err
        assert "ConnectionError" in err

    def test_noncredential_error_text_unchanged(self, redis, client):
        """负控：非凭据错误文本与事件形状（seq/node_id/duration）原样。"""
        timeline = _scrubbed_timeline()
        timeline["events"][1]["error"] = "boom: quota exceeded"
        _seed(redis, "tl-sec-2", timeline=timeline)

        body = client.get(
            "/api/executions/tl-sec-2/timeline", headers=_headers()
        ).json()

        end = body["events"][1]
        assert end["error"] == "boom: quota exceeded"
        assert end["node_id"] == "call" and end["duration_ms"] == 120
        assert [e["seq"] for e in body["events"]] == [1, 2, 3]


@pytest.mark.parametrize("path", ["/api/executions/e1/timeline"])
def test_endpoint_is_registered_without_auth_bypass(client, path):
    """路由挂在受 require_auth 保护的 router 上（不是 SSE 那种 raw Route）。"""
    resp = client.get(path)
    assert resp.status_code in (401, 403), resp.text
