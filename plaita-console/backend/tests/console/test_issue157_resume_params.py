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
import os
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from plaita.core.errors import ResumeGuardError  # noqa: E402
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


def _seed_running(redis, eid: str, context=None) -> None:
    """命中窗口：running 行是 ``start_flow`` 刚落的初始快照（首节点未成功）。

    ``context`` 显式给出时按给定 checkpoint 落行（非空 checkpoint 用例用）；
    缺省 ``{}`` = 首节点未成功过的空盘面。
    """
    redis.set(
        f"plaita:execution:{eid}",
        json.dumps(
            {
                "execution_id": eid,
                "flow_id": "f1",
                "flow_version": "1.0.0",
                "status": "running",
                "context": {} if context is None else context,
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


# ============================================================================
# #157 修复轮：把引擎的**真实**受影响面钉进测试（修前 MIGRATION/README 把
# `retry` 也算进「现在绑定 $INPUT」、README 还把「空 checkpoint ⇒ 绑定
# $INPUT」写成 route 级规则——两者都不成立，且是**预存语义、非本单引入**）。
#
# 引擎事实（``plaita/core/strategies.py``，``DistributedStrategy.execute``）：
#   * ``saved_context`` 为假值（空 checkpoint）且 ``resume_type == continue``
#     ⇒ 走假值分支 ``context.clean() + setup_flow(flow, (), params or {})``
#     （:236-239）——**只有 continue** 在这里绑定 ``$INPUT``；
#   * 假值 + 非 continue ⇒ 在假值分支**之前**就被守卫拦下：
#     ``raise ResumeGuardError("resume_type='retry' requires a saved_context, ...")``
#     （:248-257）。worker 侧 ``_chain_has_resume_guard_error`` 把它归一为
#     ``ResumeProtocolError``：执行保持原状（running）、消息被 **ack（+XDEL）**
#     吞掉、``$INPUT`` 永不绑定；
#   * ``saved_context`` 为真值（checkpoint 非空）⇒ 真值分支
#     ``context.context = saved_context`` **整体替换**（:219-220），``params``
#     一路不被读取 ⇒ **params 惰性**，绑定值一律来自 checkpoint。
#
# 下面两组用「控制面真实载体 → ``worker._dispatch_task``（消费循环唯一的
# resume 入口，== 真实 worker 处理一条消息）→ 真引擎」的进程内链路钉住上述
# 语义：fakeredis + Memory*Storage，**无需任何网络**，但走的是生产同一条
# 分发/守卫/绑定代码。真 Redis 组另钉「消息层处置」（ack+XDEL）这一件
# fakeredis 覆盖不到的、必须真消费组才能观测的事实。
# ============================================================================

_RESUME_FLOW_ID = "f1"
_RESUME_FLOW_VERSION = "1"
# start → a1 → end；end 读 `$INPUT.watch_agent`，缺键回落 glm-default。
# 判据与 #157 报告里的 `$F.or($INPUT.watch_agent,'glm-default')` 同款：
# 「params 有没有真的到 $INPUT」在 end 输出上直接可观测。
_RESUME_FLOW = {
    "flow_id": _RESUME_FLOW_ID,
    "version": _RESUME_FLOW_VERSION,
    "runtime": "python",
    "inputType": {"dataType": "object"},
    "nodes": [
        {"type": "start", "id": "start", "next": "a1"},
        {"type": "assignment", "id": "a1", "output": {"step": 1}, "next": "end"},
        {
            "type": "end",
            "id": "end",
            "output": {"watch_agent": "$F.or($INPUT.watch_agent, 'glm-default')"},
        },
    ],
}

_ABSENT = object()


def _worker_mod():
    """惰性导入 worker 模块（可选依赖缺失时 skip，同 ``_carrier_from_122_dedup_hit``）。"""
    pytest.importorskip("cachetools")
    pytest.importorskip("lupa")
    from plaita.server import flow_worker

    return flow_worker


def _worker_on(redis, storage, *, queue: str = QUEUE, redis_url: str = "redis://localhost:6379/15"):
    """**真构造**的 worker（有 __init__ 的全部登记表），队列/存储由调用方注入。"""
    flow_worker = _worker_mod()
    flows = MemoryFlowStorage()
    flows.save_flow(_RESUME_FLOW)
    return flow_worker.RedisFlowWorker(
        redis_url=redis_url,
        queue_name=queue,
        execution_storage=storage,
        flow_storage=flows,
        redis_client=redis,
        enable_registry=False,
        enable_redis_logging=False,
        metrics_port=0,       # 绝不 bind 抓取端口（测试进程里并行会撞）
        alert_webhook="",     # 同上：不起告警线程
    )


def _save_worker_state(storage, eid: str, context) -> None:
    """worker 侧盘上 checkpoint（``state.context``）——空 dict = 假值分支。"""
    storage.save_execution_state(
        eid,
        ExecutionState(
            execution_id=eid,
            flow_id=_RESUME_FLOW_ID,
            flow_version=_RESUME_FLOW_VERSION,
            status="running",
            context=context,
        ),
    )


def _chain_has(exc, cls):
    """异常链（含 ``__cause__``）里是否有 ``cls``——worker 判据同款。"""
    node, depth = exc, 0
    while node is not None and depth <= 12:
        if isinstance(node, cls):
            return node
        node = node.__cause__
        depth += 1
    return None


def _spy_params(worker):
    """记录 ``worker.resume_flow`` **实收**的 params（以及它的返回值）。

    证据上的关键一步：断言里"绑定值不是 params"必须排除"params 根本没送到"
    这个替代解释——所以先证明载体里的 params 确实进了 resume_flow。
    ``_dispatch_task`` 自己不回传 ``resume_flow`` 的返回值，故一并捕获。
    """
    seen: dict = {}
    real = worker.resume_flow

    def spy(*args, **kwargs):
        seen["params"] = kwargs.get("params")
        seen["result"] = real(*args, **kwargs)
        return seen["result"]

    worker.resume_flow = spy
    return seen


def _post_resume(redis, eid: str, resume_type: str, params=_ABSENT, data=None):
    body = {"resume_type": resume_type, "data": data}
    if params is not _ABSENT:
        body["params"] = params
    return _post_continue(redis, eid, body)


class TestEmptyCheckpointResumeTypes:
    """空 checkpoint：**只有 ``continue``** 绑定 ``$INPUT``；其余被守卫拒绝。"""

    @pytest.mark.parametrize("resume_type", ["retry", "cancel", "timeout", "event"])
    def test_non_continue_rejected_and_input_never_bound(self, resume_type):
        """非 continue + 空 checkpoint ⇒ ResumeGuardError：执行仍 running、$INPUT 未绑定。

        钉住修复轮更正后的**受影响面**（只有 continue）；也钉住「其余
        resume_type 不是"现在会绑定 $INPUT"，而是被守卫拒绝」这一预存语义。
        """
        flow_worker = _worker_mod()
        fake = FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        eid = f"exec-empty-{resume_type}"
        _seed_running(fake, eid)                 # 控制面可查的 running 行（context={}）
        _save_worker_state(storage, eid, {})     # 盘上 checkpoint 为空（假值）
        worker = _worker_on(fake, storage)
        seen = _spy_params(worker)

        r = _post_resume(fake, eid, resume_type, params={"watch_agent": "from-params"})
        assert r.status_code == 200, r.text
        msg = _carrier(fake)
        assert msg["params"] == {"watch_agent": "from-params"}

        with pytest.raises(flow_worker.ResumeProtocolError) as excinfo:
            worker._dispatch_task(msg)
        guard = _chain_has(excinfo.value, ResumeGuardError)
        assert guard is not None, f"必须由策略层 ResumeGuardError 触发：{excinfo.value!r}"
        assert f"resume_type={resume_type!r} requires a saved_context" in str(guard)

        # params 确实送到了 worker——却**没有**被绑定（排除"没送到"的替代解释）
        assert seen["params"] == {"watch_agent": "from-params"}
        state = storage.load_execution_state(eid)
        assert state.status == "running", "守卫拒绝不得推进、也不得终态化"
        assert "$INPUT" not in (state.context or {}), "$INPUT 不得被绑定"
        row = json.loads(fake.get(f"plaita:execution:{eid}"))
        assert row["status"] == "running" and row["context"] == {}

    def test_continue_with_empty_checkpoint_binds_params_end_to_end(self):
        """对照组：同一空 checkpoint 下 ``continue`` 才真的把 params 绑进 ``$INPUT``。

        同时钉住此前"有断言、无测试"的「end-to-end ``$INPUT`` reaches」：
        end 节点读到的就是 params（而非回落 ``glm-default``）。
        """
        fake = FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        eid = "exec-empty-continue"
        _seed_running(fake, eid)
        _save_worker_state(storage, eid, {})
        worker = _worker_on(fake, storage)
        seen = _spy_params(worker)

        r = _post_resume(fake, eid, "continue", params={"watch_agent": "from-params"})
        assert r.status_code == 200, r.text
        worker._dispatch_task(_carrier(fake))

        assert seen["params"] == {"watch_agent": "from-params"}
        assert seen["result"]["result"] == {"watch_agent": "from-params"}
        state = storage.load_execution_state(eid)
        assert state.status == "completed"
        assert state.context["$INPUT"] == {"watch_agent": "from-params"}
        assert state.context["$NODE"]["end"] == {"watch_agent": "from-params"}

    def test_continue_with_empty_params_falls_back_to_default(self):
        """空 checkpoint + **无** params ⇒ ``$INPUT={}``、end 回落默认值（存量语义）。"""
        fake = FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        eid = "exec-empty-no-params"
        _seed_running(fake, eid)
        _save_worker_state(storage, eid, {})
        worker = _worker_on(fake, storage)
        seen = _spy_params(worker)

        r = _post_resume(fake, eid, "continue")
        assert r.status_code == 200, r.text
        worker._dispatch_task(_carrier(fake))

        assert seen["result"]["result"] == {"watch_agent": "glm-default"}
        state = storage.load_execution_state(eid)
        assert state.context["$INPUT"] == {}


class TestNonEmptyCheckpointWins:
    """checkpoint 非空 ⇒ 绑定值一律来自 checkpoint、``params`` 惰性。"""

    @staticmethod
    def _checkpoint(value):
        """**真引擎产物**：start+a1 步后的 checkpoint（非空、且未到终点）。"""
        from plaita.core.executor import FlowExecution
        from plaita.core.flow import Flow

        execution = FlowExecution()
        execution.mode = "distributed"
        result = execution.run_distributed(
            Flow.model_validate(_RESUME_FLOW), {"watch_agent": value}
        )
        cp = result["context"]
        assert cp["$INPUT"] == {"watch_agent": value}
        assert result.get("is_end") is False
        assert cp["$LAST_NODE"] == "a1", "必须停在半途，否则续跑无从观测"
        return cp

    @pytest.mark.parametrize("resume_type", ["continue", "retry"])
    def test_carrier_carries_params_but_binding_comes_from_checkpoint(self, resume_type):
        """载体照带 params；但 worker 真收了 params、``$INPUT`` 仍来自 checkpoint。"""
        fake = FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        eid = f"exec-cp-{resume_type}"
        checkpoint = self._checkpoint("from-checkpoint")
        _seed_running(fake, eid, context=checkpoint)
        _save_worker_state(storage, eid, checkpoint)
        worker = _worker_on(fake, storage)
        seen = _spy_params(worker)

        r = _post_resume(fake, eid, resume_type, params={"watch_agent": "from-params"})
        assert r.status_code == 200, r.text
        msg = _carrier(fake)
        assert msg["params"] == {"watch_agent": "from-params"}   # 载体照带
        worker._dispatch_task(msg)

        assert seen["params"] == {"watch_agent": "from-params"}  # 真送到了
        assert seen["result"]["result"] == {"watch_agent": "from-checkpoint"}
        state = storage.load_execution_state(eid)
        assert state.status == "completed"
        assert state.context["$INPUT"] == {"watch_agent": "from-checkpoint"}
        assert state.context["$NODE"]["end"] == {"watch_agent": "from-checkpoint"}


class TestParamsKeyAbsentOnEveryResumeType:
    """「空/未传 params ⇒ 载体不新增 params 键」在**每个** resume_type 上成立。

    修前只钉了 continue；verifier 确认该不变量对 retry/cancel/timeout/event
    同样成立但没有测试——这里补齐（判据 ``isinstance(params, dict) and params``
    与 resume_type 无关）。
    """

    @pytest.mark.parametrize(
        "resume_type", ["continue", "retry", "cancel", "timeout", "event"]
    )
    @pytest.mark.parametrize("params", [_ABSENT, {}, None], ids=["absent", "empty", "null"])
    def test_no_params_key_when_empty(self, resume_type, params):
        fake = FakeRedis(decode_responses=True)
        eid = f"exec-shape-{resume_type}"
        _seed_running(fake, eid)

        r = _post_resume(fake, eid, resume_type, params=params)

        assert r.status_code == 200, r.text
        msg = _carrier(fake)
        assert "params" not in msg
        assert set(msg) == BASELINE_KEYS      # 与修前字段集逐字一致


# ---------------- 真 Redis + 真消费循环（消息层处置） ----------------

_REAL_REDIS_URL = os.environ.get("PLAITA_TEST_REDIS_URL")

requires_real_redis = pytest.mark.skipif(
    not _REAL_REDIS_URL,
    reason=(
        "需要真实 Redis：本类断言的是**消息层**处置（守卫拒绝后消息被 ack 并 "
        "XDEL ⇒ XLEN=0/XPENDING=0）与真 worker 消费循环，fakeredis 的消费组"
        "语义不覆盖（见 tests/unit/test_worker_affinity.py 的实测记录）。"
        "设 PLAITA_TEST_REDIS_URL=redis://127.0.0.1:16379/15 后重跑"
        "（docker run -d --rm -p 16379:6379 redis:7）。"
    ),
)


@requires_real_redis
class TestRealRedisWorkerLoopE2E:
    """真 Redis + 真 ``RedisFlowWorker.run()``（XREADGROUP → XACK/XDEL）。

    与 fakeredis 组的分工：这里钉的是消息在**真实消费组**里如何被处置——
    拒绝路径 ack+XDEL 吞掉（XLEN=0/XPENDING=0）、执行保持 running、``$INPUT``
    永不绑定；接受路径真的把 params 绑进 ``$INPUT`` 并被 ack 回收。
    状态行用 ``RedisExecutionStorage``（与控制面同一个 ``plaita:execution:{id}``
    键），断言的就是运维在控制面看到的那一行。
    """

    @staticmethod
    def _client():
        import redis as _redis

        client = _redis.Redis.from_url(
            _REAL_REDIS_URL, decode_responses=True, socket_connect_timeout=2
        )
        try:
            client.ping()
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"PLAITA_TEST_REDIS_URL 不可达（{exc}）：{_REAL_REDIS_URL}")
        return client

    def _run_one(self, monkeypatch, *, resume_type, params=_ABSENT, checkpoint=None):
        """控制面 POST → 真 worker 消费循环处理这**一条**消息 → 观测结果。"""
        pytest.importorskip("redis")
        from plaita.storage.redis import RedisExecutionStorage

        client = self._client()
        tag = uuid.uuid4().hex[:10]
        eid = f"e2e157-{tag}-{resume_type}"
        queue = f"test157:e2e:{tag}:queue"
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", queue)

        storage = RedisExecutionStorage(client=client)
        _save_worker_state(storage, eid, {} if checkpoint is None else checkpoint)
        worker = _worker_on(client, storage, queue=queue, redis_url=_REAL_REDIS_URL)
        thread = None
        obs: dict = {"eid": eid, "queue": queue, "tag": tag}
        try:
            r = _post_resume(client, eid, resume_type, params=params)
            assert r.status_code == 200, r.text

            entries = client.xrange(queue)
            assert len(entries) == 1, f"预期恰一条消息，实际 {len(entries)}"
            carrier = json.loads(entries[0][1]["payload"])
            obs["carrier"] = carrier
            if params in (_ABSENT, {}, None):
                assert "params" not in carrier
                assert set(carrier) == BASELINE_KEYS
            else:
                assert carrier["params"] == params

            thread = threading.Thread(target=worker.run, name="e2e157-worker", daemon=True)
            thread.start()

            deadline = time.time() + 30
            while time.time() < deadline and client.xlen(queue) != 0:
                time.sleep(0.05)
            assert client.xlen(queue) == 0, "30s 内消息未被处置（ACk/XDEL 未发生）"

            task_queue = worker._get_task_queue()
            obs["stream_len"] = client.xlen(queue)
            # xpending 摘要形态：{"pending": N, "min":..., "max":..., "consumers":[...]}
            obs["pending"] = client.xpending(queue, task_queue.group_name)["pending"]
            obs["state"] = storage.load_execution_state(eid)
            return obs
        finally:
            worker.stop()
            if thread is not None:
                thread.join(timeout=10)
            # 只清自己造的键（不 flushdb：URL 可能指向共享 db）
            for key in list(client.scan_iter(match=f"*{tag}*")):
                client.delete(key)
            client.delete(queue)
            client.delete(f"plaita:execution:{eid}")

    def test_continue_empty_checkpoint_binds_params(self, monkeypatch):
        obs = self._run_one(
            monkeypatch, resume_type="continue", params={"watch_agent": "from-params"}
        )
        assert obs["stream_len"] == 0 and obs["pending"] == 0    # ack 且 XDEL，不悬 PEL
        state = obs["state"]
        assert state.status == "completed"
        assert state.context["$INPUT"] == {"watch_agent": "from-params"}
        assert state.context["$NODE"]["end"] == {"watch_agent": "from-params"}

    @pytest.mark.parametrize("params", [_ABSENT, {}, None], ids=["absent", "empty", "null"])
    def test_continue_empty_params_end_to_end(self, monkeypatch, params):
        obs = self._run_one(monkeypatch, resume_type="continue", params=params)
        assert obs["stream_len"] == 0 and obs["pending"] == 0
        state = obs["state"]
        assert state.status == "completed"
        assert state.context["$INPUT"] == {}
        assert state.context["$NODE"]["end"] == {"watch_agent": "glm-default"}

    @pytest.mark.parametrize("resume_type", ["retry", "cancel", "timeout", "event"])
    def test_non_continue_empty_checkpoint_acked_and_swallowed(self, monkeypatch, resume_type):
        """空 checkpoint + 非 continue：消息被 ack+XDEL 吞掉、执行仍 running、$INPUT 未绑定。"""
        obs = self._run_one(
            monkeypatch,
            resume_type=resume_type,
            params={"watch_agent": "from-params"},
        )
        assert obs["stream_len"] == 0, "守卫拒绝后消息必须被 ack+XDEL（XLEN=0）"
        assert obs["pending"] == 0, "不得留在 PEL（否则会被反复重投）"
        state = obs["state"]
        assert state.status == "running", "协议错误不终态化，执行保持原状"
        assert "$INPUT" not in (state.context or {})

    @pytest.mark.parametrize("resume_type", ["continue", "retry"])
    def test_non_empty_checkpoint_params_inert_end_to_end(self, monkeypatch, resume_type):
        """checkpoint 非空：params 被送达却惰性，``$INPUT``/end 输出取自 checkpoint。"""
        from plaita.core.executor import FlowExecution
        from plaita.core.flow import Flow

        execution = FlowExecution()
        execution.mode = "distributed"
        checkpoint = execution.run_distributed(
            Flow.model_validate(_RESUME_FLOW), {"watch_agent": "from-checkpoint"}
        )["context"]

        obs = self._run_one(
            monkeypatch,
            resume_type=resume_type,
            params={"watch_agent": "from-params"},
            checkpoint=checkpoint,
        )
        assert obs["stream_len"] == 0 and obs["pending"] == 0
        assert obs["carrier"]["params"] == {"watch_agent": "from-params"}
        state = obs["state"]
        assert state.status == "completed"
        assert state.context["$INPUT"] == {"watch_agent": "from-checkpoint"}
        assert state.context["$NODE"]["end"] == {"watch_agent": "from-checkpoint"}
