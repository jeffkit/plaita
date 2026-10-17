"""#42 期 1：派发台账 —— 「谁派给谁 / 为何」+ 台账条数 == 入队条数。

## 红先证（RED）

基线 `11c10162` 只有 ``shadow_dispatch_plan``（只算不发、**不写任何键**），
``plaita/server/dispatch_ledger.py`` 不存在 ⇒ ``ImportError``。

## 判据（期 1 §5）

- ② **台账条数 == 入队条数**（``ledger_queue_parity`` 可自检；入队失败**不得**
  留假台账，记账失败**不得**吞掉已入队的事实）；
- ③ **金丝雀队列 XLEN 独立**（派发到金丝雀队列不改变生产队列读数——即派发
  确实按调用方指定队列路由，而不是广播到所有队列）。
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("redis")
pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402

from plaita.server import dispatch_ledger as led  # noqa: E402

CANARY_QUEUE = "plaita:flow:queue:canary"
PROD_QUEUE = "plaita:flow:queue"
LEDGER_KEY = led.DEFAULT_LEDGER_KEY


class _FlakyRedis:
    """包装 fakeredis：让**第 N 次** ``xadd`` 抛错（模拟部分失败）。

    为什么要这个：期 1 的判据②只有在「入队成功但记账失败」「入队失败」两类
    路径下都成立才算成立。全绿的 fakeredis 只会覆盖 happy path。
    """

    def __init__(self, real, *, fail_xadd_on=(), fail_xlen=False):
        self._real = real
        self._fail_xadd_on = set(fail_xadd_on)
        self._fail_xlen = fail_xlen
        self._xadd_calls = 0

    def xadd(self, *args, **kwargs):
        self._xadd_calls += 1
        if self._xadd_calls in self._fail_xadd_on:
            raise RuntimeError(f"xadd #{self._xadd_calls} 故障（注入）")
        return self._real.xadd(*args, **kwargs)

    def xlen(self, *args, **kwargs):
        if self._fail_xlen:
            raise RuntimeError("xlen 故障（注入）")
        return self._real.xlen(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.fixture()
def redis():
    return fakeredis.FakeRedis(decode_responses=True)


def _entry(issue_id="issue-1", target="w-high", queue=CANARY_QUEUE, **over):
    base = dict(
        issue_id=issue_id,
        queue=queue,
        target_instance_id=target,
        reason="route: capacity",
        allowed=["w-high"],
        rejected=[{"instance_id": "w-low", "reason": "disk: low free space"}],
    )
    base.update(over)
    return led.build_ledger_entry(**base)


def _task(issue_id="issue-1"):
    return {
        "type": "start",
        "flow_id": issue_id,
        "params": {},
        "execution_id": issue_id,
    }


def _body_of(redis, queue, index=0):
    entries = redis.xrange(queue)
    return json.loads(entries[index][1]["payload"])


class TestLedgerEntrySchema:
    def test_targeted_entry_records_who_and_why(self):
        entry = _entry()
        assert entry["ledger_schema_version"] == led.LEDGER_SCHEMA_VERSION
        assert entry["decision"] == led.DECISION_TARGETED
        assert entry["target_instance_id"] == "w-high"
        assert entry["reason"] == "route: capacity"
        assert entry["enqueued"] is False, "构造期默认未入队（入队后由写侧改真）"
        assert entry["decided_by"], "必须留下派发方标识（谁派的）"
        assert entry["candidate_count"] == 2
        assert entry["rejected"][0]["instance_id"] == "w-low"

    def test_no_target_means_broadcast(self):
        """红线：无可派目标 ⇒ 台账如实记 broadcast（不臆造目标）。"""
        entry = _entry(target=None, reason="route: no-dispatchable-worker")
        assert entry["target_instance_id"] is None
        assert entry["decision"] == led.DECISION_BROADCAST

    def test_rows_accept_strings_dicts_and_garbage(self):
        entry = led.build_ledger_entry(
            issue_id="i", queue="q", target_instance_id="w",
            allowed=["w-a", "w-b"],
            rejected=[{"instance_id": "w-c", "reason": "draining"}, "w-d", 42, None],
        )
        assert [r["instance_id"] for r in entry["allowed"]] == ["w-a", "w-b"]
        assert [r["instance_id"] for r in entry["rejected"]] == ["w-c", "w-d"]
        assert entry["candidate_count"] == 4

    def test_decided_by_env_override(self, monkeypatch):
        monkeypatch.setenv(led.DISPATCHER_ID_ENV, "console-7")
        assert led.dispatcher_id_from_env() == "console-7"
        assert _entry()["decided_by"] == "console-7"

    def test_ledger_key_env_override(self, monkeypatch):
        monkeypatch.setenv(led.LEDGER_KEY_ENV, "test:ledger:canary")
        assert led.ledger_key_from_env() == "test:ledger:canary"
        monkeypatch.delenv(led.LEDGER_KEY_ENV)
        assert led.ledger_key_from_env() == led.DEFAULT_LEDGER_KEY


class TestEnqueueWithLedger:
    def test_target_is_injected_into_body_and_ledger_matches(self, redis):
        """台账不说谎：条目里的目标就是消息体里的目标（同一份输入派生）。"""
        out = led.enqueue_with_ledger(
            redis, stream_key=CANARY_QUEUE, task=_task(), entry=_entry(),
        )
        assert out["enqueued"] is True
        assert out["ledger_recorded"] is True
        assert out["error"] is None

        body = _body_of(redis, CANARY_QUEUE)
        assert body["target_instance_id"] == "w-high"
        rows = led.read_ledger(redis)
        assert len(rows) == 1
        assert rows[0]["target_instance_id"] == "w-high"
        assert rows[0]["decision"] == led.DECISION_TARGETED
        assert rows[0]["message_id"] == out["message_id"]
        assert rows[0]["enqueued"] is True
        assert rows[0]["queue"] == CANARY_QUEUE

    def test_broadcast_removes_stale_target_field(self, redis):
        """调用方误留了目标字段但路由无目标 ⇒ 体里必须**移除**该键（否则仍是
        显式派发，与台账声明的 broadcast 矛盾）。"""
        task = _task()
        task["target_instance_id"] = "w-stale"
        out = led.enqueue_with_ledger(
            redis, stream_key=CANARY_QUEUE, task=task,
            entry=_entry(target=None, reason="route: no-dispatchable-worker"),
        )
        assert out["enqueued"] is True
        body = _body_of(redis, CANARY_QUEUE)
        assert "target_instance_id" not in body
        assert led.read_ledger(redis)[0]["target_instance_id"] is None

    def test_ledger_count_equals_enqueue_count(self, redis):
        """判据②：N 次派发后 台账条数 == 入队条数。"""
        for i in range(7):
            out = led.enqueue_with_ledger(
                redis, stream_key=CANARY_QUEUE, task=_task(f"issue-{i}"),
                entry=_entry(issue_id=f"issue-{i}"),
            )
            assert out["enqueued"] and out["ledger_recorded"]

        parity = led.ledger_queue_parity(redis, CANARY_QUEUE)
        assert parity == {
            "ledger": 7, "queue": 7, "queue_name": CANARY_QUEUE,
            "expected": 7, "parity": True, "error": None,
        }

    def test_canary_queue_xlen_is_independent_from_production(self, redis):
        """判据③：派发到金丝雀队列 ⇒ 生产队列 XLEN 恒 0（物理隔离）。"""
        for i in range(3):
            led.enqueue_with_ledger(
                redis, stream_key=CANARY_QUEUE, task=_task(f"c-{i}"),
                entry=_entry(issue_id=f"c-{i}"),
            )
        assert redis.xlen(CANARY_QUEUE) == 3
        assert redis.xlen(PROD_QUEUE) == 0
        # 台账对照金丝雀队列成立；对照生产队列**不**成立（两者互不影响）
        assert led.ledger_queue_parity(redis, CANARY_QUEUE)["parity"] is True
        assert led.ledger_queue_parity(redis, PROD_QUEUE)["parity"] is False

    def test_no_redis_client_is_not_enqueued(self):
        out = led.enqueue_with_ledger(None, stream_key=CANARY_QUEUE, task=_task(), entry=_entry())
        assert out == {
            "enqueued": False, "message_id": None, "ledger_id": None,
            "ledger_recorded": False, "error": "no-redis-client",
        }

    def test_enqueue_failure_leaves_no_fake_ledger(self, redis):
        """入队失败 ⇒ **不能**留台账（否则台账条数 > 入队条数 = 假台账）。"""
        flaky = _FlakyRedis(redis, fail_xadd_on={1})
        out = led.enqueue_with_ledger(
            flaky, stream_key=CANARY_QUEUE, task=_task(), entry=_entry(),
        )
        assert out["enqueued"] is False
        assert out["ledger_recorded"] is False
        assert out["error"].startswith("enqueue-failed")
        assert redis.xlen(CANARY_QUEUE) == 0
        assert led.ledger_length(redis, ledger_key=LEDGER_KEY) == 0

    def test_ledger_failure_is_visible_not_silent(self, redis):
        """记账失败 ⇒ 派发**不回滚**、不静默：返回值里带 error + 判据变红。"""
        flaky = _FlakyRedis(redis, fail_xadd_on={2})
        out = led.enqueue_with_ledger(
            flaky, stream_key=CANARY_QUEUE, task=_task(), entry=_entry(),
        )
        assert out["enqueued"] is True, "已入队的任务不得因记账故障消失"
        assert out["ledger_recorded"] is False
        assert out["error"] == "ledger-write-failed"
        parity = led.ledger_queue_parity(redis, CANARY_QUEUE)
        assert parity["parity"] is False, "对账必须能看见这个缺口（不许静默变绿）"

    def test_parity_detector_has_teeth(self, redis, monkeypatch):
        """负控：摘掉记账 ⇒ 判据②必须转红。"""
        monkeypatch.setattr(led, "record_dispatch", lambda *a, **k: None)
        out = led.enqueue_with_ledger(
            redis, stream_key=CANARY_QUEUE, task=_task(), entry=_entry(),
        )
        assert out["enqueued"] is True and out["ledger_recorded"] is False
        assert led.ledger_queue_parity(redis, CANARY_QUEUE)["parity"] is False

    def test_parity_is_never_green_when_unreadable(self, redis):
        """红线：读失败 ⇒ ``parity=False`` + error（不把「读不到」当「相等」）。"""
        flaky = _FlakyRedis(redis, fail_xlen=True)
        parity = led.ledger_queue_parity(flaky, CANARY_QUEUE)
        assert parity["parity"] is False
        assert parity["error"]
        assert parity["ledger"] is None and parity["queue"] is None


class TestLedgerReadSide:
    def test_bad_rows_are_skipped_and_counted(self, redis):
        """下溢：坏条目（payload 缺失 / 不可解析 / 非对象）**不得**让整本台账
        读不出来；同时必须可观测（``ledger_rows_with_errors`` 计数）。"""
        led.enqueue_with_ledger(redis, stream_key=CANARY_QUEUE, task=_task(), entry=_entry())
        redis.xadd(LEDGER_KEY, {"schema_version": "1"})                # 无 payload
        redis.xadd(LEDGER_KEY, {"payload": "{not json}"})              # 不可解析
        redis.xadd(LEDGER_KEY, {"payload": "[1, 2, 3]"})               # 非对象
        led.enqueue_with_ledger(
            redis, stream_key=CANARY_QUEUE, task=_task("issue-2"), entry=_entry("issue-2"),
        )

        rows, bad = led.ledger_rows_with_errors(redis)
        assert len(rows) == 2, "两条好条目必须都读得出来"
        assert bad == 3
        assert [r["issue_id"] for r in led.read_ledger(redis)] == ["issue-1", "issue-2"]

    def test_read_recent_n_returns_latest_in_order(self, redis):
        for i in range(5):
            led.enqueue_with_ledger(
                redis, stream_key=CANARY_QUEUE, task=_task(f"i-{i}"),
                entry=_entry(issue_id=f"i-{i}"),
            )
        rows = led.read_ledger(redis, count=2)
        assert [r["issue_id"] for r in rows] == ["i-3", "i-4"]

    def test_read_on_dead_redis_is_empty_not_raise(self):
        class _Dead:
            def xrange(self, *a, **k):
                raise RuntimeError("redis down")

            def xrevrange(self, *a, **k):
                raise RuntimeError("redis down")

            def xlen(self, *a, **k):
                raise RuntimeError("redis down")

        assert led.read_ledger(_Dead()) == []
        assert led.ledger_length(_Dead()) is None
        assert led.queue_length(_Dead(), "q") is None
