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

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
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


# ---------------------------------------------------------------------------
# plaita#338：嵌套挂起帧面（$SUSPEND_FRAMES[*].context[.$PARENT]）同源扩展
# ---------------------------------------------------------------------------

def _frame(flow_id, node_id, context):
    """NS-M3 帧 schema（strategies._carrier_frame_dicts 同形）。"""
    return {"node_id": node_id, "flow_id": flow_id, "context": context}


#: 真引擎四面格的根 flow（根声明 allowlist + child 内 event 挂起 + 内层求值面）。
NESTED_REAL_ROOT_FLOW = {
    "flow_id": "root1", "version": "1", "runtime": "python",
    "expose_env": ["P326_NESTED_SECRET"],
    "nodes": [
        {"type": "start", "id": "start", "next": "call"},
        {"type": "child", "id": "call", "childFlow": {
            "flow_id": "child1", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "cstart", "next": "wait"},
                {"type": "event", "id": "wait", "event_type": "approval",
                 "next": "cap"},
                {"type": "assignment", "id": "cap",
                 "result": "$ENV.P326_NESTED_SECRET", "next": "cend"},
                {"type": "end", "id": "cend",
                 "output": {"seen": "$ENV.P326_NESTED_SECRET"}},
            ],
        }, "next": "end"},
        {"type": "end", "id": "end", "output": "root-done"},
    ],
}


def _nested_env_checkpoint(frame_secret="frame-snapshot", parent_secret="parent-snapshot",
                           root_secret="root-snapshot"):
    """嵌套挂起形状：根面 + 帧面双面带 $ENV 明文（legacy 行完整形态）。"""
    return {
        "$LAST_NODE": "call",
        "$INPUT": {"k": 1},
        "$ENV": {"P326_SECRET": root_secret, "P326_BASE": "https://x"},
        "$SUSPEND_FRAMES": [
            _frame("child1", "wait", {
                "$FLOW_ID": "child1",
                "$INPUT": {"x": 1},
                "$ENV": {"P326_SECRET": frame_secret, "P326_BASE": "https://y"},
                "$PARENT": {
                    "$FLOW_ID": "f1",
                    "$ENV": {"P326_SECRET": parent_secret, "P326_BASE": "https://x"},
                },
            }),
            _frame("f1", "call", None),
        ],
    }


class TestNestedFrameStrip:
    def test_persist_strips_env_from_frames_and_parent_chain(self):
        """#338 [B]：咽喉点 strip 覆盖帧面——帧 context 与帧内 $PARENT 的
        $ENV（值与键名）都不落盘；其余帧字段原样；引擎在飞对象不动。"""
        worker, storage, _fake = _worker()
        inflight = _nested_env_checkpoint()
        state = ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status="suspended",
            context=inflight,
        )
        worker._persist_state_or_raise(EXEC, state, "suspended")
        reloaded = storage.load_execution_state(EXEC)
        dumped = json.dumps(reloaded.model_dump())
        assert "$ENV" not in reloaded.context, "根面 strip（wave 2 守恒）"
        assert '"$ENV"' not in dumped, "帧面键名也不落盘（owner 裁决④）"
        assert "frame-snapshot" not in dumped and "parent-snapshot" not in dumped, (
            "帧面值不落盘"
        )
        assert "root-snapshot" not in dumped, "根面值不落盘"
        fc = reloaded.context["$SUSPEND_FRAMES"][0]["context"]
        assert fc["$INPUT"] == {"x": 1}, "帧 context 其余键原样"
        assert fc["$PARENT"]["$FLOW_ID"] == "f1", "$PARENT 面除 $ENV 外原样"
        # 引擎在飞对象不动（重绑副本语义的嵌套延伸；ExecutionState 浅拷贝
        # context 顶层，帧内嵌套容器与在飞对象共享引用）
        assert inflight["$SUSPEND_FRAMES"][0]["context"]["$ENV"]["P326_SECRET"] == (
            "frame-snapshot"
        )
        assert inflight["$SUSPEND_FRAMES"][0]["context"]["$PARENT"]["$ENV"][
            "P326_SECRET"
        ] == "parent-snapshot"
        assert inflight["$ENV"]["P326_SECRET"] == "root-snapshot", "根面在飞字典原样"

    def test_strip_noop_row_returned_unchanged_shape(self):
        """无 $ENV 面（新管线已净行）strip 后形状逐键不变（守恒负控）。"""
        from plaita.core.env_checkpoint import strip_env_snapshot

        clean = _nested_env_checkpoint()
        clean.pop("$ENV")
        clean["$SUSPEND_FRAMES"][0]["context"].pop("$ENV")
        clean["$SUSPEND_FRAMES"][0]["context"]["$PARENT"].pop("$ENV")
        snapshot = json.dumps(clean, sort_keys=True)
        out = strip_env_snapshot(clean)
        assert json.dumps(out, sort_keys=True) == snapshot, "净行零字节变化"


class TestNestedFrameReinjection:
    def test_frame_own_expose_env_wins_root_undeclared_no_empty_face(
        self, monkeypatch, caplog
    ):
        """#338 [D] 前置：帧面按**该帧生效 allowlist**重注——内层 flow 自声明
        优先（setup_flow 同判据）；根 flow 未声明时根面与父面不造空 $ENV。"""
        monkeypatch.setenv("P326_SECRET", "root-rotated")
        monkeypatch.setenv("P326_CHILD_ONLY", "child-rotated")
        root = Flow.model_validate({
            "flow_id": "f1", "version": "1",
            "nodes": [
                {"type": "start", "id": "start", "next": "call"},
                {"type": "child", "id": "call", "childFlow": {
                    "flow_id": "child1", "version": "1",
                    "expose_env": ["P326_CHILD_ONLY"],
                    "nodes": [
                        {"type": "start", "id": "cs", "next": "w"},
                        {"type": "event", "id": "w", "event_type": "approval",
                         "next": "ce"},
                        {"type": "end", "id": "ce", "output": "ok"},
                    ],
                }, "next": "end"},
                {"type": "end", "id": "end", "output": "ok"},
            ],
        })
        worker, _storage, _fake = _worker()
        ctx = {
            "$INPUT": {},
            "$SUSPEND_FRAMES": [
                _frame("child1", "w", {
                    "$FLOW_ID": "child1",
                    "$PARENT": {"$FLOW_ID": "f1"},
                }),
                _frame("f1", "call", None),
            ],
        }
        state = ExecutionState(
            execution_id=EXEC, flow_id="f1", flow_version="1",
            status="suspended", context=ctx,
        )
        with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
            worker._reinject_env_for_resume(root, state)
        # ExecutionState 浅拷贝 context 顶层——重注面一律从 state.context 读。
        rectx = state.context
        fc = rectx["$SUSPEND_FRAMES"][0]["context"]
        assert fc["$ENV"] == {"P326_CHILD_ONLY": "child-rotated"}, (
            "帧面=内层 flow 自声明 allowlist 的当前进程值"
        )
        assert "$ENV" not in fc["$PARENT"], "父面 allowlist 为空 → 不造空 $ENV 面"
        assert "$ENV" not in rectx, "根面 allowlist 为空 → 根面不动"
        assert "tenant_env.reinject" in caplog.text and "值漂移" in caplog.text, (
            "具名 WARNING 语义保留（帧面重注同样声明）"
        )

    def test_frame_inherits_root_allowlist_and_overwrites_stale(
        self, monkeypatch
    ):
        """内层 flow 未声明 expose_env → 继承根 allowlist（构造期继承语义）；
        legacy 帧面旧快照被当前 env 覆盖（不吃旧值，owner 裁决①）。"""
        monkeypatch.setenv("P326_SECRET", "root-rotated-current")
        root = Flow.model_validate({
            "flow_id": "f1", "version": "1", "expose_env": ["P326_SECRET"],
            "nodes": [
                {"type": "start", "id": "start", "next": "call"},
                {"type": "child", "id": "call", "childFlow": {
                    "flow_id": "child1", "version": "1",
                    "nodes": [
                        {"type": "start", "id": "cs", "next": "w"},
                        {"type": "event", "id": "w", "event_type": "approval",
                         "next": "ce"},
                        {"type": "end", "id": "ce", "output": "ok"},
                    ],
                }, "next": "end"},
                {"type": "end", "id": "end", "output": "ok"},
            ],
        })
        worker, _storage, _fake = _worker()
        ctx = _nested_env_checkpoint(frame_secret="stale-frame", parent_secret="stale-parent")
        state = ExecutionState(
            execution_id=EXEC, flow_id="f1", flow_version="1",
            status="suspended", context=ctx,
        )
        worker._reinject_env_for_resume(root, state)
        # ExecutionState 浅拷贝 context 顶层——重注面一律从 state.context 读。
        rectx = state.context
        assert rectx["$ENV"] == {"P326_SECRET": "root-rotated-current"}, (
            "根面重注且 legacy 旧快照被当前值覆盖（不吃旧值）"
        )
        fc = rectx["$SUSPEND_FRAMES"][0]["context"]
        assert fc["$ENV"] == {"P326_SECRET": "root-rotated-current"}, (
            "帧面继承根 allowlist 且不吃旧快照"
        )
        assert fc["$PARENT"]["$ENV"] == {"P326_SECRET": "root-rotated-current"}, (
            "帧内 $PARENT 面同源重注"
        )


class TestPurgeScriptNested:
    def test_dry_run_and_apply_reach_nested_frames(self):
        """#338 [C]：顶层无 $ENV 但帧面有的存量行——purge 不再 kept 短路；
        干跑不写字节，实清拔净三面（值与键名）且帧结构原样。"""
        purge = _load_script("purge_env_from_checkpoints")
        fake = fakeredis.FakeRedis(decode_responses=True)
        key = "plaita:execution:e-nested"
        row = {
            "execution_id": "e-nested",
            "flow_id": "f1",
            "status": "suspended",
            "context": _nested_env_checkpoint(),
        }
        fake.set(key, json.dumps(row))
        before = fake.get(key)

        assert purge.purge_one(fake, key, apply=False) == "purged", (
            "干跑即报告嵌套命中"
        )
        assert fake.get(key) == before, "干跑不写"

        assert purge.purge_one(fake, key, apply=True) == "purged"
        raw = fake.get(key)
        assert '"$ENV"' not in raw, "帧面键名拔除"
        assert "frame-snapshot" not in raw and "parent-snapshot" not in raw, (
            "帧面值拔除"
        )
        after = json.loads(raw)["context"]
        fc = after["$SUSPEND_FRAMES"][0]["context"]
        assert fc["$INPUT"] == {"x": 1} and fc["$FLOW_ID"] == "child1"
        assert fc["$PARENT"]["$FLOW_ID"] == "f1"
        assert after["$SUSPEND_FRAMES"][1] == {"node_id": "call", "flow_id": "f1",
                                              "context": None}, "根帧原样"
        # 已净行二次跑 = kept（no-op 幂等）
        assert purge.purge_one(fake, key, apply=True) == "kept"


class _NestedRecordingBus:
    """最小事件总线替身：登记/注销订阅（A1 r140 探针同款）。"""

    def __init__(self):
        self.subscriptions = {}
        self._seq = 0

    async def register_subscription(self, event_type=None, correlation_id=None,
                                    flow_id=None, node_id=None, **kw):
        self._seq += 1
        sid = f"sub-{self._seq}"
        self.subscriptions[sid] = {
            "event_type": event_type, "correlation_id": correlation_id,
        }
        return sid

    def unregister_subscription(self, subscription_id):
        return self.subscriptions.pop(subscription_id, None)


class TestNestedRealEngineFourGrid:
    def test_real_engine_nested_suspend_strip_purge_reinject_resume(
        self, monkeypatch, caplog
    ):
        """#338 真引擎四面格（A1 r140 探针 /tmp/a1r140 复跑等价）：
        [A] 平铺 strip（既有格家族守恒）→ [B] 嵌套帧面落盘无 $ENV（值与键名）
        → [C] purge 对净行 no-op → [D] 轮换 env 后重注达帧面，嵌套恢复内层
        求值 = 当前进程值（不静默为空、不吃旧快照）。"""
        monkeypatch.setenv("P326_NESTED_SECRET", "old-snapshot-value")
        flow = Flow.model_validate(NESTED_REAL_ROOT_FLOW)
        bus = _NestedRecordingBus()
        ex = FlowExecution(event_bus=bus)
        ex.mode = "distributed"
        r1 = ex.run_distributed(flow, {})
        assert r1.get("is_suspend"), "嵌套挂起应成立（NS-M3 正路）"
        frames = r1["context"].get("$SUSPEND_FRAMES")
        assert isinstance(frames, list) and frames, "帧路径应随挂起落 context"
        sid = next(iter(bus.subscriptions))

        worker, storage, fake = _worker(flow=dict(NESTED_REAL_ROOT_FLOW))
        state = ExecutionState(
            execution_id=EXEC, flow_id="root1", flow_version="1",
            status="suspended", context=r1["context"],
        )
        worker._persist_state_or_raise(EXEC, state, "suspended")
        loaded = storage.load_execution_state(EXEC)

        # [A]+[B]：根面与帧面都无 $ENV（值与键名）
        dumped = json.dumps(loaded.model_dump())
        assert "$ENV" not in (loaded.context or {})
        assert '"$ENV"' not in dumped
        assert "old-snapshot-value" not in dumped

        # [C] purge 对已净行 no-op（字节预滤+解析后递归双面守恒）
        purge = _load_script("purge_env_from_checkpoints")
        key = f"plaita:execution:{EXEC}"
        fake.set(key, json.dumps({"execution_id": EXEC, "context": loaded.context}))
        assert purge.purge_one(fake, key, apply=True) == "kept"

        # [D] 轮换 env → 重注达根面与帧面 → 嵌套恢复内层求值新值
        monkeypatch.setenv("P326_NESTED_SECRET", "new-rotated-value")
        with caplog.at_level(logging.WARNING, logger="plaita.server.flow_worker"):
            worker._reinject_env_for_resume(flow, loaded)
        assert "tenant_env.reinject" in caplog.text
        fc = loaded.context["$SUSPEND_FRAMES"][0]["context"]
        assert loaded.context["$ENV"] == {
            "P326_NESTED_SECRET": "new-rotated-value"
        }
        assert fc["$ENV"] == {"P326_NESTED_SECRET": "new-rotated-value"}
        assert fc["$PARENT"]["$ENV"] == {"P326_NESTED_SECRET": "new-rotated-value"}

        ex2 = FlowExecution(event_bus=bus)
        ex2.mode = "distributed"
        r2 = ex2.run_distributed(
            flow, saved_context=loaded.context, resume_type="event",
            resume_data={"subscription_id": sid, "event_type": "approval"})
        assert not r2.get("is_suspend"), "恢复后不应再挂起"
        cap_out = (r2.get("context") or {}).get("$NODE", {}).get("call")
        assert isinstance(cap_out, dict) and cap_out.get("seen") == (
            "new-rotated-value"
        ), f"[D] 内层帧 $ENV 求值应为当前进程值，实际 {cap_out!r}"
