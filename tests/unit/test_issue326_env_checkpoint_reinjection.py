"""plaita#326（E1 T0 wave 2）：checkpoint 不落 $ENV + 恢复路径重注 + 存量清理。

## 缺陷（wave 1 只收了 start 闸；$ENV 面留 owner 裁决后本轮实施）

checkpoint 落 ``$ENV`` 快照（expose_env allowlist 命中的明文 env，可能含
密钥）随 context 进 Redis：终态行 TTL 30 天、非终态活动行不过期；且
flow_worker 对每个执行告警一次（死代码面）。

## 规格（正文 + A2 r60 评审五点 + owner 裁决 2026-10-24 五点全定）

- **①a 不落快照**：值**与 allowlist 键名**都不落（键名泄露技术栈，同源
  脱敏）；strip 机械化于持久化咽喉点 ``_persist_state_or_raise``（#320
  同款：不靠写点自觉），strip 落在**重绑副本**——引擎在飞 context 字典
  不动（在飞节点仍按进程内 $ENV 求值）。
- **①b 恢复重注**：恢复/重投路径按当前 worker 进程 env 重注（按
  allowlist），具名 WARNING 声明值漂移（owner 裁决①：非确定性接受——
  执行内一致性优势不抵「凭据冻结在 Redis」违反 T0 本意）。
- **①c 求值来源**：恢复路径 ``$ENV.XXX`` = 重注后的进程 env，不静默为空
  （0.5.0 语义空 ``$ENV`` 恰违「降级必须可观测」红线），也不吃旧快照值。
- **①d 存量清理**：一次性脚本（``scripts/purge_env_from_checkpoints.py``，
  对账器顺带面家族）；降级验收 = 脚本可跑 + 新执行 secret env grep 不到。
- **①e 告警面回收**：每执行一次的 $ENV 告警删除，不留死代码。

## 负控（还原修复后必须精确红）

- 去咽喉点 strip：a-1/a-2 由「持久化行无 $ENV」变红（快照照旧落盘）；
- 删 resume 重注点：b-1/c-1 由「saved_context 带当前 env 新值」变红
  （旧快照值存活 / strip 后静默为空）；
- 脚本 CAS 退化成裸 SET：并发写回滚面（cas_skipped 格）不可辨；
- 保留每执行告警：e-1 由「无 $ENV 告警记录」变红。
"""
from __future__ import annotations

import importlib.util
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

REPO_ROOT = Path(__file__).resolve().parents[2]

EXEC = "exec-issue326-env"

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "expose_env": ["P326_SECRET"],
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _worker(flow=None):
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(flow or TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue326-env",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage, fake


def _env_checkpoint(secret="old-snapshot"):
    return {
        "$LAST_NODE": "a",
        "$INPUT": {"k": 1},
        "$ENV": {"P326_SECRET": secret, "P326_BASE": "https://x"},
    }


def _seed_running(storage, context=None, status="running"):
    storage.save_execution_state(
        EXEC,
        ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status=status,
            context=context if context is not None else _env_checkpoint(),
        ),
    )


def _step_result(context=None):
    return {
        "execution_id": EXEC,
        "is_end": False,
        "id": "a",
        "context": dict(context if context is not None else _env_checkpoint()),
    }


def _end_result(context=None):
    return {
        "execution_id": EXEC,
        "is_end": True,
        "id": "end",
        "context": dict(context if context is not None else _env_checkpoint()),
    }


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(
        name, REPO_ROOT / "scripts" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class TestCheckpointStrip:
    def test_suspend_step_persist_strips_env_keeps_inflight_dict(self):
        """①a：咽喉点 strip 落在持久化副本——盘上行无 $ENV（值与键名），
        引擎在飞 context 字典原样（在飞求值不受打断）。"""
        worker, storage, _fake = _worker()
        inflight = _env_checkpoint()
        state = ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status="running",
            context=inflight,
        )
        worker._persist_state_or_raise(EXEC, state, "step_persist")
        reloaded = storage.load_execution_state(EXEC)
        assert isinstance(reloaded.context, dict)
        assert "$ENV" not in reloaded.context, "checkpoint 不得落 $ENV 键"
        assert "P326_SECRET" not in json.dumps(reloaded.model_dump()), (
            "secret 值不得落盘（grep 不到）"
        )
        assert reloaded.context.get("$LAST_NODE") == "a", "其余 context 原样"
        assert inflight["$ENV"]["P326_SECRET"] == "old-snapshot", (
            "在飞字典不被 strip 打断（恢复重注面另格）"
        )

    def test_terminal_legacy_row_stripped_via_same_choke(self):
        """①a：终态路径同一咽喉——带 $ENV 的 legacy context 终态化后行内
        无 $ENV（strip 不分 phase，机械化收口）。"""
        worker, storage, _fake = _worker()
        state = ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status="completed",
            context=_env_checkpoint(),
            end_time=datetime.now().isoformat(),
        )
        worker._persist_state_or_raise(EXEC, state, "completed")
        reloaded = storage.load_execution_state(EXEC)
        assert "$ENV" not in (reloaded.context or {})
        assert "P326_SECRET" not in json.dumps(reloaded.model_dump())


class TestResumeReinjection:
    def test_resume_reinjects_current_env_with_named_drift_warning(
        self, monkeypatch, caplog
    ):
        """①b：恢复路径在 run_distributed 前按当前 worker env 重注 $ENV，
        具名 WARNING（tenant_env.reinject + 值漂移声明）。"""
        monkeypatch.setenv("P326_SECRET", "rotated-new")
        worker, storage, _fake = _worker()
        _seed_running(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = [_step_result(), _step_result(), _end_result()]
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
                    worker.resume_flow("f1", EXEC, "continue")
        first_call = FE.return_value.run_distributed.call_args_list[0]
        saved_context = first_call.kwargs["saved_context"]
        assert saved_context["$ENV"]["P326_SECRET"] == "rotated-new", (
            "恢复路径求值来源=重注后的当前进程 env（不静默为空）"
        )
        assert "tenant_env.reinject" in caplog.text
        assert "值漂移" in caplog.text, "值漂移必须具名声明"

    def test_resume_value_drift_beats_stale_legacy_snapshot(
        self, monkeypatch, caplog
    ):
        """①c：legacy 行带旧快照 → 重注以当前 env 覆盖——不吃旧值，
        不静默为空；凭据轮换后重跑节点拿新值（owner 裁决①）。"""
        monkeypatch.setenv("P326_SECRET", "rotated-after-credential-roll")
        worker, storage, _fake = _worker()
        _seed_running(storage, context=_env_checkpoint(secret="stale-in-redis"))
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = [_end_result()]
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
                    worker.resume_flow("f1", EXEC, "continue")
        saved_context = FE.return_value.run_distributed.call_args.kwargs["saved_context"]
        assert saved_context["$ENV"]["P326_SECRET"] == "rotated-after-credential-roll"
        assert saved_context["$ENV"]["P326_SECRET"] != "stale-in-redis"

    def test_resume_without_expose_env_no_injection_no_warning(self, caplog):
        """①b 边界：flow 未声明 allowlist → 不注入不告警（0.5.0 空语义
        守恒，resume 路径零噪音）。"""
        flow = dict(TEST_FLOW)
        flow.pop("expose_env")
        worker, storage, _fake = _worker(flow=flow)
        _seed_running(storage, context={"$LAST_NODE": "a", "$INPUT": {}})
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = [_end_result(context={"$LAST_NODE": "end"})]
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
                    worker.resume_flow("f1", EXEC, "continue")
        saved_context = FE.return_value.run_distributed.call_args.kwargs["saved_context"]
        assert "$ENV" not in saved_context
        assert "tenant_env.reinject" not in caplog.text


class TestPerExecutionWarningRecycled:
    def test_old_per_execution_env_warning_gone(self, caplog):
        """①e：每执行一次的「checkpoint 包含 $ENV」告警回收——结果 context
        带 $ENV 也绝不出现该记录（不留死代码）；且终态落盘行 grep 不到。"""
        worker, storage, _fake = _worker()
        _seed_running(storage)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.side_effect = [
                _end_result(context=_env_checkpoint())
            ]
            with patch.object(RedisFlowWorker, "_cancel_requested", return_value=False):
                with caplog.at_level(logging.WARNING):
                    worker.resume_flow("f1", EXEC, "continue")
        assert "checkpoint 包含 $ENV" not in caplog.text
        reloaded = storage.load_execution_state(EXEC)
        assert "P326_SECRET" not in json.dumps(reloaded.model_dump())


class TestPurgeScript:
    def test_dry_run_reports_without_writing(self):
        """①d：干跑只报告——with_env 计数正确，Redis 字节原样。"""
        purge = _load_script("purge_env_from_checkpoints")
        fake = fakeredis.FakeRedis(decode_responses=True)
        legacy = {
            "execution_id": "e-legacy",
            "flow_id": "f1",
            "status": "completed",
            "context": _env_checkpoint(),
        }
        fake.set("plaita:execution:e-legacy", json.dumps(legacy))
        fake.set("plaita:execution:lease:e-legacy", json.dumps(legacy))
        fake.set("plaita:execution:index", json.dumps({"z": 1}))
        clean = {
            "execution_id": "e-clean",
            "flow_id": "f1",
            "status": "running",
            "context": {"$LAST_NODE": "a", "$INPUT": {}},
        }
        fake.set("plaita:execution:e-clean", json.dumps(clean))
        before = fake.get("plaita:execution:e-legacy")

        report = purge.purge_all(fake, "plaita", apply=False)

        assert report["scanned"] == 2, "index/机制键必须排除（成员判别同款）"
        assert report["with_env"] == 1 and report["purged"] == 1
        assert fake.get("plaita:execution:e-legacy") == before, "干跑不写"

    def test_apply_strips_env_preserves_rest_and_ttl(self):
        """①d：实清——$ENV 拔除、其余 context/字段原样、TTL 保持。"""
        purge = _load_script("purge_env_from_checkpoints")
        fake = fakeredis.FakeRedis(decode_responses=True)
        legacy = {
            "execution_id": "e-legacy",
            "flow_id": "f1",
            "status": "completed",
            "context": _env_checkpoint(),
        }
        fake.set("plaita:execution:e-legacy", json.dumps(legacy), ex=1000)

        report = purge.purge_all(fake, "plaita", apply=True)

        assert report["purged"] == 1
        raw = fake.get("plaita:execution:e-legacy")
        assert "$ENV" not in raw and "P326_SECRET" not in raw, "值与键名都不落"
        state = json.loads(raw)
        assert state["context"]["$LAST_NODE"] == "a", "其余 context 原样"
        assert state["context"]["$INPUT"] == {"k": 1}
        assert state["status"] == "completed"
        assert 0 < fake.ttl("plaita:execution:e-legacy") <= 1000, "TTL 保持"

    def test_cas_skips_row_advanced_by_concurrent_writer(self):
        """①d 原子性：worker 在脚本读后落盘推进（新值无 $ENV）→ 旧字节
        CAS 不中、绝不回滚 worker 的新值。"""
        purge = _load_script("purge_env_from_checkpoints")
        fake = fakeredis.FakeRedis(decode_responses=True)
        key = "plaita:execution:e-race"
        stale = json.dumps({"context": _env_checkpoint()})
        fake.set(key, stale)
        fresh = json.dumps({"context": {"$LAST_NODE": "end", "$INPUT": {}}})
        fake.set(key, fresh)  # 模拟并发 writer 已推进

        purged = purge.purge_one(fake, key, apply=True)

        assert purged == "kept", "purge_one 重读最新值：新值已无 $ENV → kept"
        assert json.loads(fake.get(key)) == json.loads(fresh), "新值不被旧快照回滚"
        # CAS 契约直证：拿陈旧字节对已推进的键 eval，必须 0（不写）。
        result = fake.eval(purge._CAS_LUA, 1, key, stale, '{"context":{}}', "0")
        assert int(result) == 0
        assert fake.get(key) == fresh
