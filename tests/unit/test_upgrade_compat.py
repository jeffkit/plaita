"""无损升级的兼容契约测试（L0：契约层）。

升级路径上最容易翻车的不是部署动作，而是三个**隐性兼容门**：
1. 执行状态 schema 跨版本反序列化；
2. 消息信封的语义版本（未知版本必须拒收而不是按旧语义硬跑）；
3. resume 的 flow 指纹校验（引擎升级会改变指纹算法口径时，历史挂起执行
   不能因此被判死）。

draining / 有界停机（L1）见 ``test_worker_drain.py``。
"""

import pytest

pytest.importorskip("cachetools")

from plaita.server.flow_worker import (
    ALLOW_FLOW_HASH_CHANGE_KEY,
    FLOW_HASH_ALGO,
    classify_flow_hash_change,
)
from plaita.server.task_queue import (
    SCHEMA_FIELD,
    TASK_SCHEMA_VERSION,
    RedisStreamTaskQueue,
    StreamTask,
    _schema_version_from_fields,
    enqueue_task,
)
from plaita.storage.base import ExecutionState


# ============ 1. 状态 schema 跨版本 ============


class TestStateSchemaCompat:
    """状态只增字段：老状态能被新代码读，新状态被老代码读也不会炸。"""

    def test_old_state_without_new_fields_loads_with_defaults(self):
        old_state = {
            "execution_id": "e1",
            "flow_id": "f",
            "context": {"$INPUT": {}},
            "status": "suspended",
            "start_time": "2026-01-01T00:00:00",
            "invoker": "worker",
        }
        state = ExecutionState(**old_state)
        # 新增字段必须落到缺省值，读取方据此判断「有没有这个能力」
        assert state.node_timings is None
        assert state.flow_hash_algo is None
        assert state.engine_version is None

    def test_new_state_with_unknown_future_field_is_ignored(self):
        """未来版本新增字段时，老代码（这里用同一模型模拟「不认识」）不得报错。"""
        future_state = {
            "execution_id": "e2",
            "context": {},
            "status": "running",
            "some_future_field": {"added": "later"},
        }
        state = ExecutionState(**future_state)
        assert not hasattr(state, "some_future_field")

    def test_roundtrip_carries_new_fields(self):
        state = ExecutionState(
            execution_id="e3",
            context={},
            flow_hash="abc",
            flow_hash_algo=FLOW_HASH_ALGO,
            engine_version="0.6.1",
            node_timings={"start": {"duration_ms": 3, "attempts": 1}},
        )
        dumped = state.model_dump()
        assert dumped["flow_hash_algo"] == FLOW_HASH_ALGO
        assert dumped["engine_version"] == "0.6.1"
        again = ExecutionState(**dumped)
        assert again.flow_hash_algo == FLOW_HASH_ALGO
        assert again.node_timings["start"]["duration_ms"] == 3


# ============ 2. flow 指纹判定表 ============


class TestFlowHashDecision:
    def test_missing_hash_skips_guard(self):
        assert (
            classify_flow_hash_change(
                stored_hash=None, current_hash="x", stored_algo=None, current_algo=FLOW_HASH_ALGO, allow_change=False
            )
            == "no_guard"
        )

    def test_same_hash_same_algo_is_match(self):
        assert (
            classify_flow_hash_change(
                stored_hash="h", current_hash="h", stored_algo=FLOW_HASH_ALGO, current_algo=FLOW_HASH_ALGO, allow_change=False
            )
            == "match"
        )

    def test_same_hash_new_algo_is_refresh_not_mismatch(self):
        """引擎升级只改了算法标记、定义没变：必须能续跑（不误伤）。"""
        assert (
            classify_flow_hash_change(
                stored_hash="h", current_hash="h", stored_algo="v0", current_algo=FLOW_HASH_ALGO, allow_change=False
            )
            == "refresh"
        )

    def test_same_algo_diff_hash_is_mismatch_even_with_override(self):
        """同算法下哈希不同 = 定义真的变了：override 不该放行。"""
        assert (
            classify_flow_hash_change(
                stored_hash="a", current_hash="b", stored_algo=FLOW_HASH_ALGO, current_algo=FLOW_HASH_ALGO, allow_change=True
            )
            == "mismatch"
        )

    def test_algo_changed_and_explicit_override_is_accepted(self):
        assert (
            classify_flow_hash_change(
                stored_hash="a", current_hash="b", stored_algo="v0", current_algo=FLOW_HASH_ALGO, allow_change=True
            )
            == "accepted"
        )

    def test_algo_changed_without_override_stays_mismatch(self):
        assert (
            classify_flow_hash_change(
                stored_hash="a", current_hash="b", stored_algo="v0", current_algo=FLOW_HASH_ALGO, allow_change=False
            )
            == "mismatch"
        )

    def test_legacy_state_without_algo_is_conservative(self):
        """老状态没有算法标记：按当前算法保守处理（不因 override 放行）。"""
        assert (
            classify_flow_hash_change(
                stored_hash="a", current_hash="b", stored_algo=None, current_algo=FLOW_HASH_ALGO, allow_change=True
            )
            == "mismatch"
        )

    def test_override_key_name_is_stable(self):
        # resume 的显式裁决入口是对外契约（console/脚本都按这个名字传），改名即破坏兼容
        assert ALLOW_FLOW_HASH_CHANGE_KEY == "allow_flow_hash_change"


# ============ 3. 消息信封 schema_version ============


class _FakeRedis:
    """只实现队列用到的几个命令，测信封语义足够。"""

    def __init__(self, read_response=None):
        self.read_response = read_response
        self.xadded = []
        self.acked = []

    def xgroup_create(self, *a, **kw):
        return True

    def xreadgroup(self, **kw):
        return self.read_response

    def xack(self, stream, group, msg_id):
        self.acked.append(msg_id)
        return 1

    def xdel(self, *a):
        return 1

    def xadd(self, stream, fields):
        self.xadded.append((stream, fields))
        return b"1-1"

    def xtrim(self, *a, **kw):
        return 1

    def xpending_range(self, *a, **kw):
        return []


class TestMessageEnvelope:
    def test_enqueue_stamps_schema_version(self):
        redis = _FakeRedis()
        enqueue_task(redis, "q", {"type": "start", "execution_id": "e"})
        _, fields = redis.xadded[0]
        assert fields[SCHEMA_FIELD] == str(TASK_SCHEMA_VERSION)
        # payload 仍是纯领域消息，信封不污染领域字段
        assert "schema_version" not in fields["payload"]

    def test_missing_field_is_treated_as_v1(self):
        # 历史消息（升级前入队）没有信封字段
        assert _schema_version_from_fields({}) == TASK_SCHEMA_VERSION

    def test_invalid_field_falls_back_to_v1(self):
        assert _schema_version_from_fields({SCHEMA_FIELD: "not-a-number"}) == TASK_SCHEMA_VERSION

    def test_parses_numeric_field(self):
        assert _schema_version_from_fields({SCHEMA_FIELD: "7"}) == 7

    def test_task_from_fields_reads_envelope(self):
        queue = RedisStreamTaskQueue(_FakeRedis(), "q", max_schema_version=99)
        task = queue._task_from_fields("1-1", {"payload": '{"type":"start"}', SCHEMA_FIELD: "3"}, 1)
        assert task.schema_version == 3
        assert task.body == {"type": "start"}

    def test_newer_message_is_dead_lettered_not_executed(self):
        """消费者看不懂的更新语义：必须死信+告警，绝不能按旧语义硬跑。"""
        payload = '{"type":"start","execution_id":"e"}'
        redis = _FakeRedis(read_response=[("q", [("1-1", {"payload": payload, SCHEMA_FIELD: "99"})])])
        queue = RedisStreamTaskQueue(redis, "q", max_schema_version=TASK_SCHEMA_VERSION)
        dead_letters = []
        queue.dead_letter = lambda task, *, reason: dead_letters.append((task.schema_version, reason))  # type: ignore[assignment]
        queue._reclaim_one = lambda: None  # type: ignore[assignment]

        assert queue.read(block_ms=1) is None
        assert len(dead_letters) == 1
        assert dead_letters[0][0] == 99
        assert "unsupported schema_version=99" in dead_letters[0][1]

    def test_same_version_message_is_served(self):
        payload = '{"type":"start","execution_id":"e"}'
        redis = _FakeRedis(read_response=[("q", [("1-1", {"payload": payload, SCHEMA_FIELD: str(TASK_SCHEMA_VERSION)})])])
        queue = RedisStreamTaskQueue(redis, "q")
        queue._reclaim_one = lambda: None  # type: ignore[assignment]
        task = queue.read(block_ms=1)
        assert task is not None and task.body["execution_id"] == "e"
