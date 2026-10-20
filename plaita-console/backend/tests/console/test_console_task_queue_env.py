"""console 派发队列名可配置回归（新旧并行验证硬前置，P1）。

背景：``executions.py`` 把 ``TASK_QUEUE_NAME = "plaita:flow:queue"`` 硬编码为
模块常量，无法把「新系统」发到独立队列做新旧并行验证。

修法：改为在使用点读取 env（默认值不变）：
``os.getenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue")``。
注意不得在模块导入时求值（否则测试无法 monkeypatch）。
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest
from fakeredis import FakeRedis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from api import executions as executions_api  # noqa: E402


def _drain_stream_keys(redis) -> set[str]:
    return {k for k in redis.scan_iter() if "queue" in str(k)}


class TestConsoleTaskQueueEnv:
    def test_default_queue_unchanged(self, monkeypatch):
        monkeypatch.delenv("PLAITA_CONSOLE_TASK_QUEUE", raising=False)
        redis = FakeRedis(decode_responses=True)

        msg_id = executions_api._enqueue({"action": "start", "flow_id": "f1"}, redis)

        assert msg_id
        assert redis.xrange("plaita:flow:queue"), "默认队列名必须保持 plaita:flow:queue"
        # 不得落到其它队列
        assert _drain_stream_keys(redis) == {"plaita:flow:queue"}

    def test_env_overrides_queue(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
        redis = FakeRedis(decode_responses=True)

        executions_api._enqueue({"action": "start", "flow_id": "f1"}, redis)

        assert redis.xrange("plaita:flow:queue:v2"), "env 指定的队列必须收到消息"
        assert not redis.xrange("plaita:flow:queue"), "默认队列不得再收到消息"
        assert _drain_stream_keys(redis) == {"plaita:flow:queue:v2"}

    def test_env_read_at_use_point_not_import(self, monkeypatch):
        """env 必须在调用点读取——导入后再设 env 也要生效。"""
        redis = FakeRedis(decode_responses=True)
        # 模块已导入；此刻才设置 env
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:late")

        executions_api._enqueue({"action": "start", "flow_id": "f1"}, redis)

        assert redis.xrange("plaita:flow:queue:late")

    def test_payload_shape_preserved(self, monkeypatch):
        """回归：消息体字段/编码不变（stream key 之外的载荷语义不得动）。

        A69-04 判定表登记的唯一新增键是载体入队时间 ``timestamp``：只有缺失
        时才注入，且必须是合法 ISO。除此之外不得出现任何新键，原字段值逐字
        不变（``set`` 比较同时收紧「多键」与「缺键」两侧）。
        """
        monkeypatch.delenv("PLAITA_CONSOLE_TASK_QUEUE", raising=False)
        redis = FakeRedis(decode_responses=True)

        payload = {"action": "start", "flow_id": "f1", "execution_id": "exec-1"}
        executions_api._enqueue(payload, redis)

        raw = redis.xrange("plaita:flow:queue")[-1][1]
        body = json.loads(raw["payload"])
        assert set(body) == set(payload) | {"timestamp"}
        assert datetime.fromisoformat(body.pop("timestamp"))
        assert body == payload


# ---- A56-02：调度「立即触发」必须跟随 PLAITA_CONSOLE_TASK_QUEUE ----

ADMIN_KEY = "task-queue-env-key"


def _settings():
    from config import Settings
    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def cluster_env(tmp_path, monkeypatch):
    """集群档（local_mode=False）+ 独立 SQLite（audit 落库用）。"""
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    from services import flow_store
    flow_store.init_engine(f"sqlite:///{tmp_path}/queue-env.db")
    return FakeRedis(decode_responses=True)


class TestScheduleTriggerQueueEnv:
    def _client(self, redis):
        from fastapi import Depends, FastAPI
        from starlette.testclient import TestClient
        from api import schedules as schedules_api
        from auth import require_auth
        from services import flow_store

        app = FastAPI()
        app.state.redis = redis
        app.state.local_mode = False  # 集群档分支
        app.state.store = flow_store.get_flow_store()
        app.include_router(
            schedules_api.router, prefix="/api",
            dependencies=[Depends(require_auth)],
        )
        return TestClient(app)

    def test_trigger_default_queue_unchanged(self, cluster_env, monkeypatch):
        """缺省行为不变：未设 env 时仍入默认队列。"""
        redis = cluster_env
        monkeypatch.delenv("PLAITA_CONSOLE_TASK_QUEUE", raising=False)
        redis.hset("plaita:schedules", "s1", json.dumps({
            "schedule_id": "s1", "name": "默认调度", "flow_id": "f1",
            "cron": "* * * * *", "enabled": True, "params": {},
            "tenant_id": "default",
        }))
        r = self._client(redis).post(
            "/api/schedules/s1/trigger", headers={"X-Admin-API-Key": ADMIN_KEY},
        )
        assert r.status_code == 200, r.text
        assert redis.xlen("plaita:flow:queue") == 1
        assert redis.xlen("plaita:flow:queue:v2") == 0

    def test_trigger_follows_env_queue(self, cluster_env, monkeypatch):
        """A56-02 负控：env 设 v2 时 trigger 必须入 v2。

        pre-fix：``trigger_now`` 用静态 ``TASK_QUEUE_NAME``（默认队列）→
        消息落默认队列而 v2 为空，本用例精确红。
        """
        redis = cluster_env
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
        redis.hset("plaita:schedules", "s1", json.dumps({
            "schedule_id": "s1", "name": "v2调度", "flow_id": "f1",
            "cron": "* * * * *", "enabled": True, "params": {},
            "tenant_id": "default",
        }))
        r = self._client(redis).post(
            "/api/schedules/s1/trigger", headers={"X-Admin-API-Key": ADMIN_KEY},
        )
        assert r.status_code == 200, r.text
        assert redis.xlen("plaita:flow:queue:v2") == 1, "env 指定的队列必须收到触发消息"
        assert redis.xlen("plaita:flow:queue") == 0, "默认队列不得再收到触发消息"
