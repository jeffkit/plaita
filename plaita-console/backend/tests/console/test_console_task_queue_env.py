"""console 派发队列名可配置回归（新旧并行验证硬前置，P1）。

背景：``executions.py`` 把 ``TASK_QUEUE_NAME = "plaita:flow:queue"`` 硬编码为
模块常量，无法把「新系统」发到独立队列做新旧并行验证。

修法：改为在使用点读取 env（默认值不变）：
``os.getenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue")``。
注意不得在模块导入时求值（否则测试无法 monkeypatch）。
"""
import json
import sys
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
        """回归：消息体字段/编码不变（stream key 之外的载荷语义不得动）。"""
        monkeypatch.delenv("PLAITA_CONSOLE_TASK_QUEUE", raising=False)
        redis = FakeRedis(decode_responses=True)

        payload = {"action": "start", "flow_id": "f1", "execution_id": "exec-1"}
        executions_api._enqueue(payload, redis)

        raw = redis.xrange("plaita:flow:queue")[-1][1]
        assert json.loads(raw["payload"]) == payload
