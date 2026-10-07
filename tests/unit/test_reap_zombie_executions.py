"""僵尸 reaper（``scripts/reap_zombie_executions.py``）三重缺陷回归。

1. **构造签名炸**：``RedisExecutionStorage`` 连接参数是 host/port/db/password
   /client，传 ``redis_url=`` 会掉进 ``**kwargs`` 透给 ``redis.Redis(...)``，
   新版 redis-py（``Redis.__init__`` 无 ``**kwargs``）直接 TypeError；
2. **last_update_time 判据在长节点期间失真**：单节点执行期间无心跳，租约正被
   看门狗续的健康长 run 会被误标 error → 租约键存在则跳过；
3. **覆写战争**：裸 save error 会被活 worker 后续步界写覆盖回去 → 条件写
   （状态键与巡检读到的原始串一致才落），且落盘前对新副本复核判据。

基建：fakeredis（``eval`` 需要 lupa）。
"""
import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("redis")

import fakeredis
import redis

from plaita.storage.base import ExecutionState
from plaita.storage.redis import (
    RedisExecutionStorage,
    execution_index_key,
    execution_state_ttl_seconds,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "reap_zombie_executions", REPO_ROOT / "scripts" / "reap_zombie_executions.py"
)
reaper = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reaper)

NS = "plaita"
STALE = "2026-01-01T00:00:00"
NOW = datetime(2026, 6, 1, 12, 0, 0)


def _storage(client):
    return RedisExecutionStorage(client=client, namespace=NS)


def _seed(storage, execution_id, *, status="running", last_update=STALE):
    state = ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        context={"$LAST_NODE": "n1"},
        status=status,
        start_time=STALE,
        last_update_time=last_update,
    )
    assert storage.save_execution_state(execution_id, state) is True
    return state


def _reap(storage, dry_run=False):
    return reaper.reap(
        storage, idle_minutes=60, dry_run=dry_run, now=NOW, log=lambda _m: None
    )


class TestConstruction:
    def test_build_storage_wires_client_from_url(self):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        with patch.object(redis.Redis, "from_url", return_value=fake) as from_url:
            storage = reaper.build_storage("redis://localhost:6379/9", NS)
        from_url.assert_called_once_with("redis://localhost:6379/9", decode_responses=True)
        assert storage.client is fake
        assert storage.namespace == NS

    def test_main_smoke_end_to_end(self, capsys):
        """CLI 冒烟：旧写法（redis_url 透传 redis.Redis）会在此 TypeError。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-cli")
        argv = ["reap_zombie_executions.py", "--redis-url", "redis://localhost:6379/0",
                "--namespace", NS, "--idle-minutes", "60"]
        with patch.object(redis.Redis, "from_url", return_value=fake) as from_url, \
                patch.object(sys, "argv", argv):
            assert reaper.main() == 0
        assert from_url.call_args.args[0] == "redis://localhost:6379/0"
        assert "reaped: 1 of 1" in capsys.readouterr().out
        assert storage.load_execution_state("e-cli").status == "error"

    def test_main_dry_run_smoke(self, capsys):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-cli")
        argv = ["reap_zombie_executions.py", "--namespace", NS, "--dry-run"]
        with patch.object(redis.Redis, "from_url", return_value=fake), \
                patch.object(sys, "argv", argv):
            assert reaper.main() == 0
        assert "would reap: 1 of 1" in capsys.readouterr().out
        assert storage.load_execution_state("e-cli").status == "running"


class TestLeaseGate:
    def test_long_running_with_live_lease_is_skipped(self, capsys):
        """长节点：last_update 陈旧 3h，但租约在（看门狗续期中）→ 不得误杀。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-live", last_update="2026-06-01T09:00:00")
        fake.set(f"{NS}:execution:lease:e-live", "worker-1:7", ex=120)

        assert _reap(storage) == (0, 0)
        assert storage.load_execution_state("e-live").status == "running"


class TestReapDecision:
    def test_stale_running_without_lease_is_reaped(self):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-dead")

        assert _reap(storage) == (1, 1)
        state = storage.load_execution_state("e-dead")
        assert state.status == "error"
        assert "orphaned" in state.error["message"]
        assert state.end_time
        # 裸 SET 绕过 save_execution_state，索引成员必须在 Lua 内同步维护
        assert fake.zscore(execution_index_key(NS), "e-dead") is not None
        if execution_state_ttl_seconds() > 0:
            assert fake.ttl(f"{NS}:execution:e-dead") > 0

    def test_recent_running_is_untouched(self):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-fresh", last_update="2026-06-01T11:59:00")

        assert _reap(storage) == (0, 0)
        assert storage.load_execution_state("e-fresh").status == "running"

    @pytest.mark.parametrize("status", ["suspended", "completed", "error"])
    def test_non_running_statuses_untouched(self, status):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-other", status=status)

        assert _reap(storage) == (0, 0)
        assert storage.load_execution_state("e-other").status == status

    def test_dry_run_reports_without_writing(self):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-dead")

        assert _reap(storage, dry_run=True) == (1, 1)
        assert storage.load_execution_state("e-dead").status == "running"


class TestConditionalWrite:
    def test_worker_advance_between_read_and_write_aborts(self):
        """活 worker 的步界写插在巡检读与条件写之间 → 放弃落盘，不覆写终态。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-race")
        key = f"{NS}:execution:e-race"
        real_eval = fake.eval

        def racing_eval(script, numkeys, *args):
            advanced = ExecutionState(
                execution_id="e-race", flow_id="f1", context={"$LAST_NODE": "n2"},
                status="completed", start_time=STALE,
                last_update_time="2026-06-01T11:59:30",
            )
            fake.set(key, storage.serialize_state(advanced.model_dump()))
            return real_eval(script, numkeys, *args)

        fake.eval = racing_eval
        assert _reap(storage) == (0, 1)
        assert storage.load_execution_state("e-race").status == "completed"

    def test_lease_acquired_between_read_and_write_aborts(self):
        """哨兵读后活 worker 抢到租约（长节点续期窗口）→ Lua 闸拦下。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-lease-race")
        real_eval = fake.eval

        def racing_eval(script, numkeys, *args):
            fake.set(f"{NS}:execution:lease:e-lease-race", "worker-2:3", ex=120)
            return real_eval(script, numkeys, *args)

        fake.eval = racing_eval
        assert _reap(storage) == (0, 1)
        assert storage.load_execution_state("e-lease-race").status == "running"

    def test_state_advanced_since_listing_is_skipped(self):
        """列表扫描后、落盘前的复核：新副本已不超期 → 不写。"""
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        _seed(storage, "e-advanced", last_update="2026-06-01T11:59:30")
        cutoff = NOW.replace(hour=11, minute=0, second=0)

        assert reaper.orphan_execution(
            storage, "e-advanced", cutoff, 60, log=lambda _m: None
        ) is False
        assert storage.load_execution_state("e-advanced").status == "running"

    def test_missing_state_key_is_skipped(self):
        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = _storage(fake)
        cutoff = NOW.replace(hour=11, minute=0, second=0)

        assert reaper.orphan_execution(
            storage, "e-gone", cutoff, 60, log=lambda _m: None
        ) is False
