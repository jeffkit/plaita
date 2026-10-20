"""A2R28-01/02/03：console 读面「键级下推 / 回填有界 / 页界预检」回归。

三条都是 O(总历史) 读面：
1. 实例日志先枚举全部 ``plaita:logs:*`` 再按条目过滤——800 流 × 0.3s 墙钟
   下只有前 ~170 个流轮得到，18 个采样实例 17 个返回空（``truncated=true``
   只在服务端 WARNING 可见，前端显示「共 0 条」）。
2. 索引回填依赖单次列表的 100k 键上限；>100k 执行时完整 SCAN 永远无法在
   单请求内完成 ⇒ ready 永不置位 ⇒ 每次列表重复扫描/投影前 100k。
3. 越界页仍按 ``need=offset+size`` 投影全部历史（120k 下 page=601 返回 0 行
   却 122k 命令 / 4.24s）。

本组判据：
- **下推**：实例查询的 SCAN 模式只匹配目标键（``plaita:logs:*:{instance}``）；
  800 流下任意真实实例都能返回自己的条目；零命中才回退全量并 WARNING。
- **有界**：回填按批 + 键数/墙钟上限推进，游标持久化续跑；完成才置 ready；
  中断时 ``truncated=true`` 且 total 是下界。
- **页界**：索引就绪时先 ZCARD 算总页界，越界页零投影命令直接空返回。
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
from auth import require_auth  # noqa: E402
from plaita.storage.redis import execution_start_time_score  # noqa: E402

ADMIN_KEY = "a2r28-admin-key"
H = {"X-Admin-API-Key": ADMIN_KEY, "X-Tenant-ID": "default"}


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


class SpyRedis(FakeRedis):
    """记录 SCAN 模式/游标与投影命令次数（把「全量扫描」变成精确红）。"""

    def __init__(self) -> None:
        super().__init__(decode_responses=True)
        self.scan_calls: list[dict] = []
        self.eval_calls = 0
        self.zrevrange_calls = 0
        self.get_calls: list[str] = []
        self.xrevrange_calls: list[str] = []

    def scan(self, cursor=0, match=None, count=None, **kwargs):
        self.scan_calls.append(
            {"cursor": cursor, "match": match, "count": count}
        )
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

    def xrevrange(self, key, *args, **kwargs):
        self.xrevrange_calls.append(key)
        return super().xrevrange(key, *args, **kwargs)


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    redis = SpyRedis()
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        logs_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app), redis


def _seed_log_stream(redis: SpyRedis, instance: str, entries: int = 100,
                     service: str = "flow_worker") -> str:
    key = f"plaita:logs:{service}:{instance}"
    for j in range(entries):
        redis.xadd(key, {
            "timestamp": f"2026-10-01T00:00:{j:02d}",
            "level": "ERROR" if j % 5 == 0 else "INFO",
            "service_type": service,
            "instance_id": instance,
            "message": f"log {instance}/{j}",
            "tenant_id": "default",
            "context": "{}",
        })
    return key


# ---------------------------------------------------------------- A2R28-01

def test_instance_logs_push_down_key_pattern(env):
    """实例查询必须只 SCAN 目标键模式，不枚举全部日志流。"""
    client, redis = env
    _seed_log_stream(redis, "inst0", entries=100)
    _seed_log_stream(redis, "inst700", entries=100)
    # 干扰流：800 流的量级用 50 个代表，关键是它们不在目标模式里
    for k in range(1, 50):
        if k not in (0, 700):
            _seed_log_stream(redis, f"inst{k}", entries=10)

    body = client.get("/api/logs/instance/inst700?limit=100", headers=H).json()
    assert body["total"] == 100
    assert body["truncated"] is False
    assert {row["instance_id"] for row in body["logs"]} == {"inst700"}
    matches = [c["match"] for c in redis.scan_calls]
    assert matches, "实例查询没有走 SCAN"
    assert all("inst700" in m for m in matches), (
        f"实例查询仍枚举全部日志流：{matches}"
    )
    # 只 XREVRANGE 命中的那一个流（不再逐流读取）
    assert redis.xrevrange_calls == ["plaita:logs:flow_worker:inst700"]

    redis.scan_calls.clear()
    redis.xrevrange_calls.clear()
    listed = client.get(
        "/api/logs?instance_id=inst0&limit=100", headers=H
    ).json()
    assert listed["total"] == 100
    assert {row["instance_id"] for row in listed["logs"]} == {"inst0"}
    assert all("inst0" in c["match"] for c in redis.scan_calls)
    assert redis.xrevrange_calls == ["plaita:logs:flow_worker:inst0"]


def test_instance_logs_service_type_uses_exact_key(env):
    """service_type 已知：精确键直读，零 SCAN。"""
    client, redis = env
    _seed_log_stream(redis, "instX", entries=3)
    body = client.get(
        "/api/logs?service_type=flow_worker&instance_id=instX", headers=H
    ).json()
    assert body["total"] == 3
    assert redis.scan_calls == [], "已知 service_type 仍走 SCAN"
    assert redis.xrevrange_calls == ["plaita:logs:flow_worker:instX"]


def test_instance_logs_zero_hit_falls_back_and_warns(env, caplog):
    """非规范键格式：精确键零命中 → 回退全量 SCAN + WARNING（可观测降级）。"""
    client, redis = env
    # 键尾不是实例 ID（历史/非规范格式），但条目 instance_id 仍是 instLegacy
    key = "plaita:logs:legacy:shared-bucket"
    redis.xadd(key, {
        "timestamp": "2026-10-01T00:00:00",
        "level": "INFO",
        "service_type": "legacy",
        "instance_id": "instLegacy",
        "message": "legacy row",
        "tenant_id": "default",
        "context": "{}",
    })
    with caplog.at_level("WARNING", logger=logs_api.logger.name):
        body = client.get(
            "/api/logs/instance/instLegacy?limit=10", headers=H
        ).json()
    assert body["total"] == 1
    matches = [c["match"] for c in redis.scan_calls]
    assert matches[0] == "plaita:logs:*:instLegacy"
    assert "plaita:logs:*" in matches[1:], f"零命中未回退全量 SCAN：{matches}"
    assert any("精确键零命中" in rec.message for rec in caplog.records)


def test_instance_logs_glob_is_escaped(env):
    """glob 元字符按字面量转义：实例 ``*`` 不得匹配全部日志流。"""
    client, redis = env
    _seed_log_stream(redis, "instA", entries=2)
    key = _seed_log_stream(redis, "*", entries=2)
    body = client.get("/api/logs/instance/%2A?limit=10", headers=H).json()
    assert body["total"] == 2
    assert [c["match"] for c in redis.scan_calls] == ["plaita:logs:*:\\*"]
    assert redis.xrevrange_calls == [key]


# ---------------------------------------------------------------- A2R28-02

def _seed_exec(
    redis: SpyRedis,
    execution_id: str,
    *,
    start_time: str = "2026-10-01T00:00:00",
    indexed: bool = False,
    namespace: str = "plaita",
) -> None:
    payload = {
        "status": "completed",
        "flow_id": "f1",
        "flow_name": "Demo",
        "flow_version": "1.0.0",
        "start_time": start_time,
        "last_update_time": start_time,
        "end_time": start_time,
        "invoker": "probe",
        "context": {"blob": "x" * 100},
    }
    redis.set(f"{namespace}:execution:{execution_id}", json.dumps(payload))
    if indexed:
        redis.zadd(
            f"{namespace}:execution:index",
            {execution_id: execution_start_time_score(payload)},
        )


def test_backfill_cursor_resumes_and_completes(env, monkeypatch):
    """> 单批上限的存量：第一次中断（truncated、ready=0、游标保留），
    第二次从游标续跑并完成（ready=1、total 完整）；命令不从头重复。"""
    client, redis = env
    for i in range(6):
        _seed_exec(redis, f"x{i}", start_time=f"2026-10-01T00:0{i}:00")

    monkeypatch.setattr(executions_api, "EXECUTION_BACKFILL_BATCH_SIZE", 2)
    monkeypatch.setattr(executions_api, "EXECUTION_BACKFILL_MAX_KEYS", 2)

    first = client.get("/api/executions?page=1&size=5", headers=H).json()
    assert first["truncated"] is True
    assert first["total"] == 2, "中断时 total 必须是已索引成员数（下界）"
    cursor_key = "plaita:execution:index:backfill:cursor"
    first_cursor = redis.get(cursor_key)
    assert first_cursor not in (None, "0")
    assert redis.exists("plaita:execution:index:ready") == 0
    assert redis.zcard("plaita:execution:index") == 2

    # 第二次：游标续跑（扫描游标不是 0），放宽预算后完成
    redis.scan_calls.clear()
    monkeypatch.setattr(executions_api, "EXECUTION_BACKFILL_MAX_KEYS", 100)
    second = client.get("/api/executions?page=1&size=5", headers=H).json()
    assert second["truncated"] is False
    assert second["total"] == 6
    assert redis.exists("plaita:execution:index:ready") == 1
    assert redis.zcard("plaita:execution:index") == 6
    assert redis.get(cursor_key) is None
    # 续跑：第一次 SCAN 从保存的游标开始，而不是 0
    assert redis.scan_calls[0]["cursor"] == int(first_cursor)


def test_backfill_never_marks_ready_before_scan_finishes(env, monkeypatch):
    """墙钟触界（deadline 已过期）不得置 ready、不得静默返回「完整」。"""
    client, redis = env
    for i in range(3):
        _seed_exec(redis, f"d{i}", start_time=f"2026-10-01T00:0{i}:00")
    monkeypatch.setattr(executions_api, "EXECUTION_BACKFILL_DEADLINE_S", 0.0)

    body = client.get("/api/executions", headers=H).json()
    assert body["truncated"] is True
    assert body["total"] == 0
    assert redis.exists("plaita:execution:index:ready") == 0


# ---------------------------------------------------------------- A2R28-03

def test_out_of_range_page_short_circuits_without_projection(env):
    """索引就绪时越界页：ZCARD 预检后空返回，零 ZREVRANGE / 零 EVAL。"""
    client, redis = env
    for i in range(6):
        _seed_exec(redis, f"p{i}", start_time=f"2026-10-01T00:0{i}:00", indexed=True)
    redis.set("plaita:execution:index:ready", "1")

    body = client.get(
        "/api/executions?page=100&size=20", headers=H
    ).json()
    assert body["executions"] == []
    assert body["total"] == 6
    assert body["truncated"] is False
    assert redis.zrevrange_calls == 0, "越界页仍从索引取行"
    assert redis.eval_calls == 0, "越界页仍投影历史行"
    assert not [k for k in redis.get_calls if ":execution:p" in k]
    # 越界页只花 EXISTS + ZCARD（每命名空间各一次），不 SCAN
    assert redis.scan_calls == []


def test_in_range_page_still_returns_rows(env):
    """守卫：页界预检不得误杀有效页。"""
    client, redis = env
    for i in range(6):
        _seed_exec(redis, f"q{i}", start_time=f"2026-10-01T00:0{i}:00", indexed=True)
    redis.set("plaita:execution:index:ready", "1")

    body = client.get("/api/executions?page=1&size=3", headers=H).json()
    assert [row["execution_id"] for row in body["executions"]] == ["q5", "q4", "q3"]
    assert body["total"] == 6
    assert redis.zrevrange_calls == 1
