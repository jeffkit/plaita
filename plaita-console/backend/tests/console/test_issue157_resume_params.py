"""plaita#157：控制台 resume 路由缺 ``params`` 通道——手动 continue 静默丢参。

缺陷（与 #122 同族，但**另一条路由**，#122 修的是 start 幂等命中转 resume
载体时的丢参；本单是这条路由**从来传不了**）：

``ResumeFlowRequest`` 没有 ``params`` 字段 ⇒ 人工/值守用
``POST /executions/{id}/resume``(``resume_type=continue``) 抢在 pending start
被 reclaim 之前推进时，调度 params 永远到不了 ``$INPUT``：

    running 行 context={} + 派发 {"type":"resume","resume_type":"continue","data":null}
      -> final_status=completed
      -> final $INPUT = {}
      -> end 节点 $F.or($INPUT.watch_agent,'glm-default') 输出 'glm-default'

修法（#157 选项 1）：``ResumeFlowRequest`` 增加可选 ``params`` 并在
``:862`` 的 resume 载体里透传，**形状不变量与 #122 逐字对齐**——空/未传
不落 ``params`` 键（``if isinstance(params, dict) and params`` 同款构造）。

本组断言：
1. 带 params → 载体携带（worker ``_dispatch_task`` 读 ``message_data["params"]``，
   经 #122 已透传到 ``run_distributed``，只在盘上 checkpoint 为空时生效）；
2. 不传 params → 载体**不新增** ``params`` 键，字段集与修前逐字一致；
3. 显式空 dict → 同样不落键（形状不变量以"非空"为界）；
4. 跨路径形状一致：与 #122（start 幂等命中 ``_handle_start_dedup_hit``）转出的
   resume 载体在 ``params`` 键的**有无/取值**上同判。
"""
import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.memory import (  # noqa: E402
    MemoryExecutionStorage,
    MemoryFlowStorage,
)

QUEUE = "plaita:flow:queue"
# #122 的 resume 载体落在 worker 自己的队列上；本用例用独立 stream key 隔离两路
QUEUE_122 = "test:issue157:worker-dedup-hit"

# 修前（无 params 字段）载体字段集：形状回归的基线
BASELINE_KEYS = {
    "type", "flow_id", "execution_id", "resume_type", "tenant_id", "data", "timestamp",
}


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app)


def _seed_running(redis, eid: str) -> None:
    """命中窗口：running 行是 ``start_flow`` 刚落的初始快照（首节点未成功）。"""
    redis.set(
        f"plaita:execution:{eid}",
        json.dumps(
            {
                "execution_id": eid,
                "flow_id": "f1",
                "flow_version": "1.0.0",
                "status": "running",
                "context": {},
            }
        ),
    )


def _carrier(redis, stream_key: str = QUEUE) -> dict:
    entries = redis.xrange(stream_key)
    assert len(entries) == 1, f"预期恰一条消息，实际 {len(entries)}"
    fields = entries[0][1]
    payload = fields["payload"] if "payload" in fields else fields[b"payload"]
    return json.loads(payload)


def _post_continue(redis, eid: str, body: dict):
    return _client(redis).post(f"/api/executions/{eid}/resume", json=body)


# ---------------- 1/2/3：控制台载体形状 ----------------

class TestConsoleResumeParamsCarrier:
    def test_continue_with_params_reaches_carrier(self):
        """带 params → resume 载体携带该 params（此前请求体无字段，整份丢弃）。"""
        redis = FakeRedis(decode_responses=True)
        _seed_running(redis, "exec-1")

        r = _post_continue(
            redis,
            "exec-1",
            {
                "resume_type": "continue",
                "data": None,
                "params": {"watch_agent": "deepseek-flash"},
            },
        )

        assert r.status_code == 200, r.text
        msg = _carrier(redis)
        assert msg["type"] == "resume"
        assert msg["resume_type"] == "continue"
        assert msg["data"] is None
        assert msg["params"] == {"watch_agent": "deepseek-flash"}
        # 载体形状：基线字段 + params，一个不多一个不少
        assert set(msg) == BASELINE_KEYS | {"params"}

    def test_continue_without_params_keeps_carrier_shape(self):
        """不传 params → 载体不得新增 ``params`` 键（与 #122 的形状不变量一致）。"""
        redis = FakeRedis(decode_responses=True)
        _seed_running(redis, "exec-2")

        r = _post_continue(redis, "exec-2", {"resume_type": "continue", "data": None})

        assert r.status_code == 200, r.text
        msg = _carrier(redis)
        assert "params" not in msg
        assert set(msg) == BASELINE_KEYS

    def test_continue_with_empty_params_keeps_carrier_shape(self):
        """显式 ``params={}`` → 同样不落键（"空"与"未传"不可区分，同 #122）。"""
        redis = FakeRedis(decode_responses=True)
        _seed_running(redis, "exec-3")

        r = _post_continue(
            redis, "exec-3", {"resume_type": "continue", "data": None, "params": {}}
        )

        assert r.status_code == 200, r.text
        msg = _carrier(redis)
        assert "params" not in msg
        assert set(msg) == BASELINE_KEYS

    def test_params_none_explicit_keeps_carrier_shape(self):
        """显式 ``params=null`` → 不落键。"""
        redis = FakeRedis(decode_responses=True)
        _seed_running(redis, "exec-4")

        r = _post_continue(
            redis, "exec-4", {"resume_type": "continue", "data": None, "params": None}
        )

        assert r.status_code == 200, r.text
        assert "params" not in _carrier(redis)


# ---------------- 4：与 #122 载体的形状一致 ----------------

def _carrier_from_122_dedup_hit(params):
    """#122 路径：start 幂等命中 → 重入队的 resume 载体（``_handle_start_dedup_hit``）。

    只喂 running + ``context={}`` 的盘上状态，让该方法走"重入队 resume"分支；
    不经过 ``start_flow``，无需 patch ``FlowExecution``。
    """
    pytest.importorskip("cachetools")
    pytest.importorskip("lupa")
    from plaita.server.flow_worker import RedisFlowWorker

    fake = FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    storage.save_execution_state(
        "exec-122",
        ExecutionState(
            execution_id="exec-122", flow_id="f1", status="running", context={}
        ),
    )
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE_122,
        execution_storage=storage,
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    worker._handle_start_dedup_hit("f1", "exec-122", params=params)
    return _carrier(fake, QUEUE_122)


class TestCarrierShapeAgreesWith122:
    """两条 resume 路径在 ``params`` 键上的形状判据必须一致（#157 验收）。"""

    @pytest.mark.parametrize(
        "params", [None, {}, {"watch_agent": "deepseek-flash"}], ids=["none", "empty", "set"]
    )
    def test_params_key_presence_predicate_matches(self, params):
        fake = FakeRedis(decode_responses=True)
        _seed_running(fake, "exec-5")
        body = {"resume_type": "continue", "data": None}
        if params is not None:
            body["params"] = params
        assert _post_continue(fake, "exec-5", body).status_code == 200
        console_msg = _carrier(fake)
        dedup_msg = _carrier_from_122_dedup_hit(params)

        # 判据同款：非空 dict 才落键
        expected_present = bool(params)
        assert ("params" in console_msg) is expected_present
        assert ("params" in dedup_msg) is expected_present
        if expected_present:
            assert console_msg["params"] == params == dedup_msg["params"]

    def test_empty_case_key_sets_are_identical_to_pre_fix(self):
        """空参下两路载体的字段集都与各自"修前形状"相同（无 params 键）。"""
        fake = FakeRedis(decode_responses=True)
        _seed_running(fake, "exec-6")
        _post_continue(fake, "exec-6", {"resume_type": "continue", "data": None})
        console_msg = _carrier(fake)
        dedup_msg = _carrier_from_122_dedup_hit({})

        assert set(console_msg) == BASELINE_KEYS
        # #122 载体没有 ``data`` 字段（start 转换路径不带 data）——只断言无 params
        assert set(dedup_msg) == {
            "type", "flow_id", "execution_id", "resume_type", "tenant_id", "timestamp",
        }
        assert "params" not in dedup_msg
