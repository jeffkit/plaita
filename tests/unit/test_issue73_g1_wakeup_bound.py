"""plaita#73 补丁回归：G1 唤醒必须**有界**，否则「唤醒重置预算」成无限循环。

## 缺陷（2026-10-10 早间实证）

`resume_flow` 的 `retry_wakeup` 分支（error 态 + `resume_type=retry`）会
**清零节点重试预算**（设计意图：人工唤醒后拿全新预算，否则唤醒后第一次
失败就立刻再耗尽，G1 形同虚设）。

但清零**没有次数上限**，于是与节点预算组成自毁循环：

```
inflight-watch（*/15）判定 stalled → POST resume {resume_type: retry}
   → worker 清零 noderetry 计数（全新 5 次预算）
   → impl 再失败 5 次 → 又终态化 error
   → 下一轮 inflight-watch 又 resume → 再清零 …
```

**实测证据**（VM worker-tcg1.log，2026-10-10）：

- `执行 c9aba1ce…` 的失败序列：`第 1/5` ×2、`2/5` ×13、`3/5` ×8、`4/5` ×4
  —— 预算**能累积到耗尽**（说明 #73 主修复生效），但随后：
- `07:20:53 预算耗尽（5/5），终态化 error` → `07:22:40 恢复类型: retry`（唤醒）
  → `07:23:07 又一次 恢复类型: retry`，同日该执行 `恢复类型: retry` 共 5 次；
- 后果：`engine_error 9`、`近 10min 重投 43 次`、14 次派发 0 落地。

## 修复契约

同一执行最多被 G1 唤醒 `G1_MAX_WAKEUPS`（默认 2）次；达限即**拒绝唤醒**并
保持 error 终态（幂等返回 `g1_wakeups_exhausted=True`，**不抛异常**——
抛异常会让调用方当失败并重试）。

反证要求：去掉上限判定，本测试必须变红。
"""
import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


FLOW_DEF = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "impl"},
        {"id": "impl", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

WAKEUP_KEY = "plaita:execution:g1wakeups:exec-1"


def _make_worker(fake) -> RedisFlowWorker:
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue73-g1wakeup",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


def _error_state() -> ExecutionState:
    st = ExecutionState(
        execution_id="exec-1",
        flow_id="f1",
        flow_version="1",
        status="error",
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )
    st.error = {"message": "boom"}
    st.end_time = "2026-10-10T00:00:00"
    return st


class TestG1WakeupIsBounded:
    def test_wakeup_budget_exhausts_after_max(self):
        """核心钉子：连续唤醒达上限后必须拒绝（保持 error），不再重置预算。

        旧行为：每次唤醒都放行（清零预算）⇒ 无限循环。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)

        # 上限前：可唤醒
        assert worker._g1_wakeup_budget_exhausted("exec-1") is False
        worker._record_g1_wakeup("exec-1")
        assert worker._read_g1_wakeup_count("exec-1") == 1
        assert worker._g1_wakeup_budget_exhausted("exec-1") is False

        worker._record_g1_wakeup("exec-1")
        assert worker._read_g1_wakeup_count("exec-1") == 2
        # 达到上限 → 拒绝
        assert worker._g1_wakeup_budget_exhausted("exec-1") is True, (
            "达 G1_MAX_WAKEUPS 后必须拒绝唤醒，否则「唤醒→重置预算」无限循环"
        )

    def test_key_is_tenant_routed_and_has_ttl(self):
        """键按租户路由且带 TTL（与 noderetry 同款契约，防永久泄漏）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        worker._record_g1_wakeup("exec-1")
        assert fake.get(WAKEUP_KEY) == "1"
        assert fake.ttl(WAKEUP_KEY) > 0, "G1 唤醒计数键必须带 TTL"

    def test_no_redis_client_never_blocks(self):
        """无 redis 客户端（内存 worker / 裸单测）→ 永不阻断唤醒。

        注意：`redis_client=None` 会被构造器补成真实 Redis 客户端
        （实测 `w.redis_client is None` → False），所以这里显式抹掉属性来
        模拟「无 redis」形态。语义：人工救援优先，读不到计数就不该拦。
        """
        worker = _make_worker(fakeredis.FakeRedis(decode_responses=True))
        worker.redis_client = None
        assert worker._g1_wakeup_budget_exhausted("exec-1") is False
        worker._record_g1_wakeup("exec-1")  # 不应抛异常

    def test_exhausted_returns_idempotent_not_raise(self):
        """达限必须是**幂等返回**而非抛异常——抛异常会让调用方当失败重试。"""
        import inspect

        src = inspect.getsource(RedisFlowWorker.resume_flow)
        assert "g1_wakeups_exhausted" in src, "达限分支必须返回可识别的幂等结果"
        # 该分支内不得 raise
        seg = src.split("_g1_wakeup_budget_exhausted(execution_id)")[1]
        seg = seg[: seg.index("if state_status in (")]
        assert "raise" not in seg, "达限分支不得抛异常（调用方会当失败并重试）"
