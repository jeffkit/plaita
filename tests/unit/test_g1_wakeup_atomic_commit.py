"""第七十一轮任务①：G1 唤醒「清预算 + 记配额」原子收口。

## 缺陷（duo-a 台账第九十七轮 a/P2→G1）

``resume_flow`` 的 ``retry_wakeup`` 分支三步非原子：

    落 running（_persist_state_or_raise）
      → 清 noderetry（_reset_node_retry_counter，失败被吞仅 WARNING）
        → 记 g1wakeups（_record_g1_wakeup，失败被吞仅 WARNING）

清计数（中步）失败时异常被吞、记数照常执行 ⇒ **g1 配额被消耗但执行没拿到
新预算**——执行翻 running 后预算仍是耗尽态，下一次节点失败直接终态化。
每次「半生效唤醒」净烧一次唤醒额度；反复瞬断下 G1 的唤醒上限被无意义耗尽。

## 修复契约

清预算与记配额收敛为**同一 Lua 脚本**（SCAN 清 noderetry 全族 + INCR/EXPIRE
g1wakeups），整体生效或整体不生效：

- 脚本失败 ⇒ 具名 ``G1WakeupAccountingError``（``StatePersistError`` 子族：
  resume_flow 对该族原样上抛、绝不终态化，消息不 ack 走 at-least-once 重投）
  + ERROR 留痕 + ``g1wakeups`` 不存在 + ``noderetry`` 原样（**配额守恒**）；
  重投见 running 行按「从断点续跑」正常推进（预算未清，下次失败照常终态化，
  届时 retry 唤醒不占额度——记账从未发生），收敛。
- 脚本成功 ⇒ 两键同轮生效（与修复后happy path 一致）。
- 落 running（``_persist_state_or_raise``）刻意**不**并入脚本：它走 fenced
  存储世代 CAS + timeline/usage 收口，先行失败抛 ``StatePersistError`` 时
  两个计数键分毫未动，本就守恒。

不支持 eval 的客户端（受限代理/桩）降级为旧两步序列，WARNING 留痕
（可观测降级纪律），行为与修复前一致。

反证要求：去掉原子收口（回到两步序列），清计数失败时 g1=1 且 noderetry
原样——本文件负控 ``test_script_failure_conserves_quota`` 必须变红。
"""
import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import (
    G1WakeupAccountingError,
    RedisFlowWorker,
    StatePersistError,
)
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

NODE_RETRY_KEY = "plaita:execution:noderetry:exec-1"
NODE_RETRY_SUBKEY = "plaita:execution:noderetry:exec-1:impl"
WAKEUP_KEY = "plaita:execution:g1wakeups:exec-1"


def _make_worker(fake) -> RedisFlowWorker:
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:g1-wakeup-atomic",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


class _EvalBrokenClient:
    """eval 必炸的桩：模拟 Lua 脚本失败（连接断/脚本错）。

    刻意**保留** eval 属性（抛错而非缺失）——缺失会走「无 eval 降级」路径，
    只有抛错才打得到被测的原子失败分支。其余方法透传 fakeredis。
    """

    def __init__(self, inner):
        self._inner = inner

    def eval(self, *args, **kwargs):
        raise ConnectionError("simulated script failure")

    def __getattr__(self, name):
        return getattr(self._inner, name)


class TestG1WakeupAtomicCommit:
    def test_commit_applies_both_keys_in_one_step(self):
        """happy path：一次提交同轮清掉 noderetry 全族并记一次唤醒。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.set(NODE_RETRY_KEY, "5")
        fake.set(NODE_RETRY_SUBKEY, "3")
        worker = _make_worker(fake)

        worker._commit_g1_wakeup("exec-1")

        assert fake.get(NODE_RETRY_KEY) is None, "执行维度预算键必须被清掉"
        assert fake.get(NODE_RETRY_SUBKEY) is None, "节点维度预算子键必须被清掉"
        assert fake.get(WAKEUP_KEY) == "1"
        assert fake.ttl(WAKEUP_KEY) > 0, "唤醒计数键必须带 TTL"

    def test_script_failure_conserves_quota(self):
        """核心负控：脚本失败 ⇒ 具名异常 + 配额守恒（g1 未记、预算原样）。

        修复前（两步序列）：清计数失败被吞、记数照常 ⇒ g1=1 且 noderetry
        原样——配额净烧一次。本用例钉住修复后契约。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.set(NODE_RETRY_KEY, "5")
        worker = _make_worker(_EvalBrokenClient(fake))

        with pytest.raises(G1WakeupAccountingError):
            worker._commit_g1_wakeup("exec-1")

        assert fake.get(WAKEUP_KEY) is None, (
            "脚本失败时唤醒计数绝不能已落——半生效唤醒=净烧配额"
        )
        assert fake.get(NODE_RETRY_KEY) == "5", (
            "脚本失败时预算键必须原样（整体不生效，重投后可再次完整唤醒）"
        )

    def test_accounting_error_is_transient_persist_family(self):
        """具名异常必须是 StatePersistError 子族：resume_flow 对该族原样
        上抛、不终态化——记账失败绝不能把已翻 running 的执行打成 error。"""
        assert issubclass(G1WakeupAccountingError, StatePersistError)

    def test_client_without_eval_degrades_with_old_two_step(self):
        """无 eval 客户端（受限代理/桩）降级为旧两步序列，不抛异常。

        降级必须可观测（WARNING），行为与修复前一致：这是显式声明的退化
        契约，不是静默换道。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.set(NODE_RETRY_KEY, "5")

        class _NoEvalClient:
            def __getattr__(self, name):
                if name == "eval":
                    raise AttributeError(name)
                return getattr(fake, name)

        worker = _make_worker(_NoEvalClient())
        worker._commit_g1_wakeup("exec-1")  # 不应抛异常

        assert fake.get(NODE_RETRY_KEY) is None
        assert fake.get(WAKEUP_KEY) == "1"

    def test_no_redis_client_noop(self):
        """无 redis 客户端（内存 worker 形态）→ no-op 不抛。"""
        worker = _make_worker(fakeredis.FakeRedis(decode_responses=True))
        worker.redis_client = None
        worker._commit_g1_wakeup("exec-1")  # 不应抛异常

    def test_wakeup_block_uses_atomic_commit(self):
        """源级钉子：resume_flow 的唤醒块必须走原子收口，不得再单独调
        「清预算」或「记配额」的散步（散步=非原子窗口回归）。"""
        import inspect

        src = inspect.getsource(RedisFlowWorker.resume_flow)
        seg = src.split("if retry_wakeup:")[1]
        seg = seg[: seg.index("# 复用同一个 FlowExecution")]
        assert "_commit_g1_wakeup" in seg, "唤醒块必须调用原子收口"
        assert "_reset_node_retry_counter" not in seg, (
            "唤醒块不得再散调清预算（非原子窗口回归）"
        )
        assert "_record_g1_wakeup" not in seg, (
            "唤醒块不得再散调记配额（非原子窗口回归）"
        )
