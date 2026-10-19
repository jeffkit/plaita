"""A2R20-02/03/04：console 热读面「单请求成本有界」回归。

三条缺陷都是「无界读面 + 成本与数据量线性」：
1. ``/api/services`` 走无 COUNT 的 ``scan_iter``（默认 COUNT=10）+ 逐键 GET；
   services 多端点与 metrics 仍用 ``KEYS``。
2. ``/api/executions`` 先全量 SCAN+投影+内存排序，最后才分页；``size`` 无上限。
3. logs 每键 ``count=limit``（键数 × limit 相乘）、tenant 统计 ``XRANGE`` 无
   count，全局上限最后才截断。

本组判据：
- **调用面**：``KEYS`` 一调用就抛错的 Redis 替身上，上述端点全部 200；
- **有界**：SCAN 显式 count、索引分页不 SCAN、size>200 422、日志全局条目/
  墙钟预算前置；
- **可见性**：触界响应带 ``truncated=true``（不静默返回部分结果）。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import logs as logs_api  # noqa: E402
from api import metrics as metrics_api  # noqa: E402
from api import services as services_api  # noqa: E402
from api.redis_scan import PAGE_SIZE_MAX  # noqa: E402
from auth import require_auth  # noqa: E402
from plaita.storage.redis import execution_start_time_score  # noqa: E402

ADMIN_KEY = "a2r20-admin-key"
H = {"X-Admin-API-Key": ADMIN_KEY, "X-Tenant-ID": "default"}


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


class SpyRedis(FakeRedis):
    """记录读面调用形态；``KEYS`` 一调用就抛错（把无界扫描变成精确红）。"""

    def __init__(self) -> None:
        super().__init__(decode_responses=True)
        self.keys_calls: list[str] = []
        self.scan_calls: list[dict] = []
        self.eval_calls = 0
        self.zrevrange_calls = 0
        self.get_calls: list[str] = []
        self.mget_calls: list[list[str]] = []
        self.xrevrange_counts: list[int | None] = []
        self.xrange_counts: list[int | None] = []

    def keys(self, pattern: str = "*", *args, **kwargs):
        self.keys_calls.append(pattern)
        raise AssertionError(
            f"端点调用了 redis KEYS({pattern})——共享 Redis 全库阻塞扫描"
        )

    def scan(self, cursor=0, match=None, count=None, **kwargs):
        self.scan_calls.append({"match": match, "count": count})
        return super().scan(cursor=cursor, match=match, count=count, **kwargs)

    def eval(self, *args, **kwargs):
        self.eval_calls += 1
        return super().eval(*args, **kwargs)

    def zrevrange(self, *args, **kwargs):
        self.zrevrange_calls += 1
        return super().zrevrange(*args, **kwargs)

    def get(self, key):
        self.get_calls.append(key)
        return super().get(key)

    def mget(self, keys, *args, **kwargs):
        self.mget_calls.append(list(keys))
        return super().mget(keys, *args, **kwargs)

    def xrevrange(self, key, *args, **kwargs):
        self.xrevrange_counts.append(kwargs.get("count"))
        return super().xrevrange(key, *args, **kwargs)

    def xrange(self, key, *args, **kwargs):
        self.xrange_counts.append(kwargs.get("count"))
        return super().xrange(key, *args, **kwargs)


class _Cfg:
    infrastructure = {}
    eventbus = None
    queue = None
    storage = None


class _Mgr:
    config = _Cfg()

    def list_instances(self):
        return []


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    monkeypatch.setattr(
        "services.service_manager.get_service_manager", lambda: _Mgr()
    )
    redis = SpyRedis()
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(
        services_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        metrics_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        logs_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    client = TestClient(app)
    yield client, redis
    assert redis.keys_calls == [], f"端点仍走 KEYS：{redis.keys_calls}"


# ---------------------------------------------------------------- A2R20-02 services

def _seed_registry(redis: SpyRedis, count: int = 3) -> None:
    for i in range(count):
        redis.set(
            f"plaita:registry:flow_worker:w{i}",
            json.dumps({
                "instance_id": f"w{i}",
                "service_type": "flow_worker",
                "host": f"127.0.0.1:90{i}",
                "status": "running",
                "start_time": "2026-10-01T00:00:00",
                "active_tasks": 0,
                "metadata": {},
            }),
        )


def test_services_endpoints_use_bounded_scan_and_mget(env):
    client, redis = env
    _seed_registry(redis, 3)
    # 无关键：迫使 SCAN 走多轮，验证 count 显式且单页不退化成长阻塞
    for i in range(3000):
        redis.set(f"plaita:filler:{i}", "x")

    body = client.get("/api/services", headers=H).json()
    assert {s["instance_id"] for s in body["services"]} == {"w0", "w1", "w2"}
    assert body["truncated"] is False
    assert redis.scan_calls, "注册表读面没有走 SCAN"
    assert all(c["count"] is not None for c in redis.scan_calls)
    assert max(c["count"] for c in redis.scan_calls) <= PAGE_SIZE_MAX
    assert redis.mget_calls, "列表读面仍是逐键 GET（N+1）"
    # 3000 无关键 @count=1000：轮次应是个位数，而不是默认 COUNT=10 的 300 轮
    assert len(redis.scan_calls) <= 10

    topo = client.get("/api/services/topology", headers=H).json()
    assert {n["instance_id"] for n in topo["nodes"]} >= {"w0", "w1", "w2"}
    assert topo["truncated"] is False

    types = client.get("/api/services/types/list", headers=H).json()
    assert types["types"] == ["flow_worker"]

    detail = client.get("/api/services/w1", headers=H)
    assert detail.status_code == 200 and detail.json()["instance_id"] == "w1"
    status = client.get("/api/services/w1/status", headers=H)
    assert status.status_code == 200 and status.json()["status"] == "running"


def test_metrics_scrape_does_not_call_keys(env):
    client, redis = env
    redis.xadd("plaita:flow:queue:v2", {"payload": "{}"})
    redis.set("plaita:registry:flow_worker:w1", "{}")
    resp = client.get("/api/metrics", headers=H)
    assert resp.status_code == 200
    assert "plaita_workers_registered_total 1" in resp.text


# ---------------------------------------------------------------- A2R20-03 executions

def _seed_exec(
    redis: SpyRedis,
    execution_id: str,
    *,
    status: str = "completed",
    flow_id: str = "f1",
    start_time: str = "2026-10-01T00:00:00",
    namespace: str = "plaita",
    indexed: bool = True,
) -> None:
    payload = {
        "status": status,
        "flow_id": flow_id,
        "flow_name": "Demo",
        "flow_version": "1.0.0",
        "start_time": start_time,
        "last_update_time": start_time,
        "end_time": start_time,
        "invoker": "probe",
        "context": {"blob": "x" * 1000},
    }
    redis.set(f"{namespace}:execution:{execution_id}", json.dumps(payload))
    if indexed:
        redis.zadd(
            f"{namespace}:execution:index",
            {execution_id: execution_start_time_score(payload)},
        )


def test_executions_index_page_does_not_scan(env):
    client, redis = env
    for i in range(60):
        _seed_exec(
            redis, f"e{i:02d}", start_time=f"2026-10-01T00:{i:02d}:00"
        )
    redis.set("plaita:execution:index:ready", "1")

    body = client.get("/api/executions?page=1&size=20", headers=H).json()
    assert body["total"] == 60
    assert body["truncated"] is False
    assert [row["execution_id"] for row in body["executions"]] == [
        f"e{i:02d}" for i in range(59, 39, -1)
    ]
    assert redis.scan_calls == [], "索引就绪时列表不得全量 SCAN"
    assert redis.zrevrange_calls == 1
    assert redis.eval_calls == 1, "只投影当页（20 行 < 单块 50）"
    assert not [k for k in redis.get_calls if ":execution:e" in k]

    redis.eval_calls = 0
    redis.zrevrange_calls = 0
    page2 = client.get("/api/executions?page=2&size=20", headers=H).json()
    assert [row["execution_id"] for row in page2["executions"]] == [
        f"e{i:02d}" for i in range(39, 19, -1)
    ]
    assert redis.scan_calls == []
    assert redis.zrevrange_calls == 1


def test_executions_size_cap(env):
    client, _ = env
    assert client.get("/api/executions?size=200", headers=H).status_code == 200
    assert client.get("/api/executions?size=201", headers=H).status_code == 422
    assert client.get("/api/executions?page=0", headers=H).status_code == 422


def test_executions_fallback_backfills_index(env):
    client, redis = env
    for i in range(3):
        _seed_exec(redis, f"x{i}", indexed=False, start_time=f"2026-10-01T00:0{i}:00")

    body = client.get("/api/executions", headers=H).json()
    assert {row["execution_id"] for row in body["executions"]} == {"x0", "x1", "x2"}
    assert redis.exists("plaita:execution:index:ready") == 1
    assert redis.zcard("plaita:execution:index") == 3

    # 回填后的下一次列表走索引：不再 SCAN
    redis.scan_calls.clear()
    again = client.get("/api/executions", headers=H).json()
    assert again["total"] == 3
    assert redis.scan_calls == []


def test_executions_index_prunes_stale_members(env):
    """悬空索引成员（状态键已过期）不产生空洞/重复行，total 随之校正。"""
    client, redis = env
    for i in range(5):
        _seed_exec(
            redis, f"s{i}", start_time=f"2026-10-01T00:0{i}:00"
        )
    redis.set("plaita:execution:index:ready", "1")
    redis.delete("plaita:execution:s4")  # 最新成员悬空（模拟终态 TTL 清理）

    body = client.get("/api/executions?page=1&size=3", headers=H).json()
    ids = [row["execution_id"] for row in body["executions"]]
    assert ids == ["s3", "s2", "s1"]
    assert len(ids) == len(set(ids)), "悬空剪枝后不得重复返回同一成员"
    assert body["total"] == 4
    assert redis.zscore("plaita:execution:index", "s4") is None


def test_executions_filter_scan_marks_truncated(env, monkeypatch):
    client, redis = env
    for i in range(5):
        _seed_exec(redis, f"f{i}", status="completed")
    redis.set("plaita:execution:index:ready", "1")
    monkeypatch.setattr(executions_api, "EXECUTION_FILTER_MAX_CANDIDATES", 2)

    body = client.get("/api/executions?status=completed", headers=H).json()
    assert body["truncated"] is True
    assert body["total"] <= 2


def test_executions_tenant_view_uses_tenant_index(env):
    client, redis = env
    _seed_exec(redis, "a1", namespace="plaita:acme", status="running")
    redis.set("plaita:acme:execution:index:ready", "1")
    body = client.get(
        "/api/executions", headers={**H, "X-Tenant-ID": "acme"}
    ).json()
    assert [row["execution_id"] for row in body["executions"]] == ["a1"]
    assert body["executions"][0]["tenant_id"] == "acme"


# ---------------------------------------------------------------- A2R20-04 logs

def _seed_logs(
    redis: SpyRedis,
    *,
    keys: int = 3,
    entries: int = 10,
    tenant: str = "default",
) -> None:
    for k in range(keys):
        key = f"plaita:logs:flow_worker:inst{k}"
        for j in range(entries):
            redis.xadd(key, {
                "timestamp": f"2026-10-01T00:00:{j:02d}",
                "level": "ERROR" if j % 5 == 0 else "INFO",
                "service_type": "flow_worker",
                "instance_id": f"inst{k}",
                "message": f"log {k}/{j}",
                "tenant_id": tenant,
                "context": "{}",
            })


def test_logs_list_global_entry_budget_is_front_loaded(env, monkeypatch):
    client, redis = env
    _seed_logs(redis, keys=3, entries=10)
    monkeypatch.setattr(logs_api, "_LOG_LIST_MAX_ENTRIES", 5)

    body = client.get("/api/logs?limit=100", headers=H).json()
    assert body["truncated"] is True
    assert redis.xrevrange_counts == [5], "每键 count 必须被全局 remaining 约束"
    assert len(body["logs"]) <= 5


def test_logs_instance_entry_budget_is_front_loaded(env, monkeypatch):
    client, redis = env
    _seed_logs(redis, keys=1, entries=10)
    monkeypatch.setattr(logs_api, "_LOG_INSTANCE_MAX_ENTRIES", 4)

    body = client.get("/api/logs/instance/inst0?limit=10", headers=H).json()
    assert body["truncated"] is True
    assert redis.xrevrange_counts == [4]
    assert len(body["logs"]) == 4


def test_logs_stats_tenant_xrange_has_count(env):
    client, redis = env
    _seed_logs(redis, keys=1, entries=10)

    body = client.get("/api/logs/stats", headers=H).json()
    assert body["truncated"] is False
    assert redis.xrange_counts == [logs_api._LOG_STATS_PER_KEY_MAX]
    assert body["total_logs"] == 10


def test_logs_stats_budget_marks_truncated(env, monkeypatch):
    client, redis = env
    _seed_logs(redis, keys=1, entries=10)
    monkeypatch.setattr(logs_api, "_LOG_STATS_MAX_ENTRIES", 5)

    body = client.get("/api/logs/stats", headers=H).json()
    assert body["truncated"] is True
    assert body["total_logs"] == 5
    assert redis.xrange_counts == [5]
    assert all(c is not None and c <= 5 for c in redis.xrange_counts)
