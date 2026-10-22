"""#300 触发面池路由收口（第七十五轮）——console 调度 API 面。

r71/r73 台账留痕的两个 #300 已知缺口之一（触发面）：

- **调度行冗余 runtime**：cron 派发进程读不到 console store，create /
  update（version 变化）/ enable 按 start 同源的版本行解析把 runtime 盖进
  调度行；读面（``_view``）如实投影。
- **manual 触发与手动 POST /executions 同语义**：先过受理闸（能力门，
  不支持即 422 结构化拒绝、零入队零占位行），再按版本行 runtime 选池队列
  （此前恒落默认 python 队列——go-runtime flow 的调度触发串池）。

负控先红（基线 1dd62a3）：盖章用例红在「行/视图无 runtime 键」；触发用例
红在「422 变 200 + 落默认队列」。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from plaita.server.services.schedule_service import (  # noqa: E402
    SCHEDULES_KEY,
)

ADMIN_KEY = "cluster-admin-key"
DEFAULT_QUEUE = "plaita:flow:queue"
GO_QUEUE = "plaita:flow:queue:go"

# 与 test_issue300_runtime_capability_gate.py 同源的真值子集（B3 r77 对齐）
GO_SUPPORTED = ["assignment", "code", "end", "http", "parallel", "start", "switch"]
GO_UNSUPPORTED = ["calculate", "case", "filter", "find", "map", "mock", "reference",
                  "reduce", "while"]


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    monkeypatch.setenv("PLAITA_CONSOLE_GO_TASK_QUEUE", GO_QUEUE)
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue300-schedule-pool.db")
    return FakeRedis(decode_responses=True)


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    auth = [Depends(require_auth)]
    app.include_router(flows_api.router, prefix="/api", dependencies=auth)
    app.include_router(schedules_api.router, prefix="/api", dependencies=auth)
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _def(node_types, flow_id="f1"):
    """start → *middles → end 的最小定义；门只消费 type 集合。``map`` 按
    定义闸要求补最小 child_flow（内嵌 start/end；code 已被默认 registry
    除名，不可作发布面样本）。"""
    middles = []
    for i, t in enumerate(node_types):
        if t in ("start", "end"):
            continue
        node = {"type": t, "id": f"n{i}", "next": "end"}
        if t == "map":
            node["over"] = "$INPUT.items"
            node["child_flow"] = {
                "flow_id": "child",
                "nodes": [
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "output": "$INPUT",
                     "resultType": "success"},
                ],
            }
        middles.append(node)
    return json.dumps({
        "flow_id": flow_id,
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start",
             "next": middles[0]["id"] if middles else "end"},
            *middles,
            {"type": "end", "id": "end", "output": "$INPUT",
             "resultType": "success"},
        ],
    })


def _publish_go_flow(client, flow_id, node_types=("start", "end"), version="1.0.0"):
    """发布 go-runtime flow。``with_manifest=False`` 时先发布后注册清单
    （fail-open 路径：清单缺席时能力门放行 + WARNING）。"""
    store = flow_store.get_flow_store()
    store.ensure_flow(flow_id, tenant_id="default")
    resp = client.put(
        f"/api/flows/{flow_id}/versions/{version}",
        json={"definition": _def(list(node_types), flow_id), "created_by": "t",
              "runtime": "go"},
        headers=_headers(),
    )
    assert resp.status_code == 200, resp.text
    resp = client.post(f"/api/flows/{flow_id}/publish",
                       json={"version": version}, headers=_headers())
    assert resp.status_code == 200, resp.text


def _publish_python_flow(client, flow_id, version="1.0.0"):
    store = flow_store.get_flow_store()
    store.ensure_flow(flow_id, tenant_id="default")
    resp = client.put(
        f"/api/flows/{flow_id}/versions/{version}",
        json={"definition": _def(["start", "end"], flow_id), "created_by": "t"},
        headers=_headers(),
    )
    assert resp.status_code == 200, resp.text
    resp = client.post(f"/api/flows/{flow_id}/publish",
                       json={"version": version}, headers=_headers())
    assert resp.status_code == 200, resp.text


def _register_go_worker(redis, *, supported=None, with_manifest=True):
    from plaita.server.capability import build_capability_manifest

    metadata = {"queue_name": GO_QUEUE}
    if with_manifest:
        metadata["capability_manifest"] = build_capability_manifest(
            supported if supported is not None else GO_SUPPORTED,
            engine="plaita-go",
            unsupported_node_types=GO_UNSUPPORTED,
        )
    redis.setex(
        "plaita:registry:flow_worker:go-1", 30,
        json.dumps({
            "instance_id": "go-1",
            "service_type": "flow_worker",
            "host": "localhost",
            "status": "running",
            "metadata": metadata,
            "active_tasks": 0,
        }),
    )


def _create_schedule(client, redis, **overrides):
    body = {
        "name": "n",
        "flow_id": "f1",
        "version": "1.0.0",
        "cron": "* * * * *",
        "params": {},
        "enabled": True,
        "created_by": "t",
    }
    body.update(overrides)
    resp = client.post("/api/schedules", json=body, headers=_headers())
    assert resp.status_code == 200, resp.text
    view = resp.json()
    row = json.loads(redis.hget(SCHEDULES_KEY, view["schedule_id"]))
    return view, row


def _start_messages(redis, queue):
    out = []
    for entry in redis.xrange(queue):
        payload = json.loads(entry[1]["payload"])
        if payload.get("type") == "start":
            out.append(payload)
    return out


class TestScheduleRowRuntimeStamp:
    def test_create_stamps_runtime_from_version_row(self, redis):
        """先红：基线调度行/视图无 runtime——go flow 的调度触发因此恒落默认池。"""
        client = _client(redis)
        _publish_go_flow(client, "f1")
        view, row = _create_schedule(client, redis)
        assert view["runtime"] == "go"
        assert row["runtime"] == "go"

    def test_create_version_none_uses_latest_published(self, redis):
        client = _client(redis)
        _publish_python_flow(client, "f1", version="1.0.0")
        _publish_go_flow(client, "f1", version="2.0.0")
        view, row = _create_schedule(client, redis, version=None)
        assert view["runtime"] == "go"
        assert row["runtime"] == "go"

    def test_update_version_restamps(self, redis):
        client = _client(redis)
        _publish_go_flow(client, "f1", version="2.0.0")
        _publish_python_flow(client, "f1", version="1.0.0")
        view, row = _create_schedule(client, redis, version="2.0.0")
        assert row["runtime"] == "go"
        resp = client.put(
            f"/api/schedules/{view['schedule_id']}",
            json={"version": "1.0.0"}, headers=_headers(),
        )
        assert resp.status_code == 200, resp.text
        row = json.loads(redis.hget(SCHEDULES_KEY, view["schedule_id"]))
        assert row["version"] == "1.0.0"
        assert row["runtime"] == "python"

    def test_enable_restamps_stale_row(self, redis):
        """启用即重盖章：老行（无 runtime 键）按版本行现算补齐。"""
        client = _client(redis)
        _publish_go_flow(client, "f1")
        view, _row = _create_schedule(client, redis)
        redis.hdel(SCHEDULES_KEY, view["schedule_id"])
        stale = {
            "schedule_id": view["schedule_id"], "tenant_id": "default",
            "name": "n", "flow_id": "f1", "version": "1.0.0",
            "cron": "* * * * *", "params": {}, "enabled": False,
            "created_at": "t0", "updated_at": "t0", "next_run_at": "",
        }
        redis.hset(SCHEDULES_KEY, view["schedule_id"], json.dumps(stale))
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/enable", headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        row = json.loads(redis.hget(SCHEDULES_KEY, view["schedule_id"]))
        assert row["runtime"] == "go"

    def test_unresolvable_version_defaults_python(self, redis):
        """version=None 且尚无已发布版本：合法态，落默认池（等于历史行为）。"""
        client = _client(redis)
        store = flow_store.get_flow_store()
        store.create_flow("f1", tenant_id="default")
        view, row = _create_schedule(client, redis, version=None)
        assert row["runtime"] == "python"


class TestManualTriggerGateAndRouting:
    def test_unsupported_flow_rejected_before_enqueue(self, redis):
        """先红：基线无门——422 变 200 且消息落默认队列。发布在清单注册前
        （fail-open），触发时清单已判明不支持——与 start 同语义在受理前拒。"""
        client = _client(redis)
        _publish_go_flow(client, "f1", node_types=("start", "map", "end"))
        _register_go_worker(redis, supported=GO_SUPPORTED)  # map ∉ go 清单
        view, _row = _create_schedule(client, redis)
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/trigger", headers=_headers()
        )
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert detail["error"] == "unsupported_node_types"
        assert detail["runtime"] == "go"
        assert detail["unsupported"] == ["map"]
        assert redis.xlen(GO_QUEUE) == 0
        assert redis.xlen(DEFAULT_QUEUE) == 0

    def test_go_flow_trigger_lands_in_go_queue(self, redis):
        """先红：基线恒传 _task_queue_name()——go flow 触发落默认池（串池）。"""
        client = _client(redis)
        _publish_go_flow(client, "f1")
        _register_go_worker(redis)
        view, _row = _create_schedule(client, redis)
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/trigger", headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["success"] is True
        assert len(_start_messages(redis, GO_QUEUE)) == 1
        assert redis.xlen(DEFAULT_QUEUE) == 0

    def test_python_flow_trigger_stays_in_default_queue(self, redis):
        # 正控：python flow 触发照旧落默认池
        client = _client(redis)
        _publish_python_flow(client, "f1")
        _register_go_worker(redis)
        view, _row = _create_schedule(client, redis)
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/trigger", headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        assert len(_start_messages(redis, DEFAULT_QUEUE)) == 1
        assert redis.xlen(GO_QUEUE) == 0

    def test_trigger_without_manifests_fails_open(self, redis):
        # 正控：池内无清单（旧 build 部署）→ 门放行，行为与上线前一致
        client = _client(redis)
        _publish_go_flow(client, "f1", node_types=("start", "map", "end"))
        _register_go_worker(redis, with_manifest=False)
        view, _row = _create_schedule(client, redis)
        resp = client.post(
            f"/api/schedules/{view['schedule_id']}/trigger", headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        assert len(_start_messages(redis, GO_QUEUE)) == 1


class TestPublishRestampsScheduleRows:
    """A2R50-03：发布改 runtime 后重盖同 flow 调度行的陈旧章。

    r50 台账实锤：章只在 create/update/enable 盖，publish 全路径零调度触点
    ——version=None（跟随 latest）调度在发布不同 runtime 新版本后，cron 按
    行上陈旧章选池、manual trigger 按版本行现值选池，两面分裂（go 版本被
    cron 反复打进 python 池）。修后 publish 即重章，选型与范围见
    ``flows._restamp_schedules_for_publish``。
    """

    def _row(self, redis, schedule_id):
        return json.loads(redis.hget(SCHEDULES_KEY, schedule_id))

    def _manual_queue(self, flow_id, version):
        """manual trigger_now 的选池面（schedules.py:742-749 同构：按版本行
        现值 resolve）。"""
        from services.runtime_pools import resolve_runtime_queue

        vrow = flow_store.get_flow_store().get_version(
            flow_id, version, tenant_id="default"
        )
        return resolve_runtime_queue(getattr(vrow, "runtime", None))

    def test_runtime_switch_publish_reunifies_cron_and_manual(self, redis):
        """r50 探针 R50-04 剧本复刻：python 1.0.0 已发布 → version=None 调度
        盖 python 章 → 发布 go 2.0.0。修前：行章停留 python，cron 落默认池、
        manual 落 go 池（实弹两队列分裂）；修后行章=go，cron 与 manual 同池。"""
        from plaita.server.services import schedule_service as svc

        client = _client(redis)
        _publish_python_flow(client, "f1", version="1.0.0")
        view, row = _create_schedule(client, redis, version=None)
        assert row["runtime"] == "python"
        _publish_go_flow(client, "f1", version="2.0.0")
        row = self._row(redis, view["schedule_id"])
        assert row["version"] is None
        assert row["runtime"] == "go"
        # 台账验收原句：runtime 切换发布后，version=None 调度的 cron fire
        # 队列与 manual trigger 一致。
        cron_queue = svc._resolve_schedule_queue(row, None)
        manual_queue = self._manual_queue("f1", "2.0.0")
        assert cron_queue == manual_queue == GO_QUEUE
        assert redis.xlen(DEFAULT_QUEUE) == 0  # 默认池零误投（分裂面收口）

    def test_publish_restamps_legacy_row_missing_runtime_key(self, redis):
        """钉在本次发布版本上的行顺带自愈：legacy 行缺 runtime 键（旧行为
        落历史默认池），重复发布该版本（幂等路径）补齐章。"""
        client = _client(redis)
        _publish_go_flow(client, "f1", version="1.0.0")
        view, _row = _create_schedule(client, redis, version="1.0.0")
        stale = self._row(redis, view["schedule_id"])
        stale.pop("runtime", None)
        redis.hset(SCHEDULES_KEY, view["schedule_id"], json.dumps(stale))
        # 幂等重发布（已 published → publish_version 早退，但端点继续走重章）
        resp = client.post(
            "/api/flows/f1/publish", json={"version": "1.0.0"}, headers=_headers()
        )
        assert resp.status_code == 200, resp.text
        assert self._row(redis, view["schedule_id"])["runtime"] == "go"

    def test_publish_keeps_rows_pinned_to_other_versions(self, redis):
        """钉在**其他**版本的行生效版本未变，章不随本次发布漂移。"""
        client = _client(redis)
        _publish_python_flow(client, "f1", version="1.0.0")
        view, row = _create_schedule(client, redis, version="1.0.0")
        assert row["runtime"] == "python"
        _publish_go_flow(client, "f1", version="2.0.0")
        row = self._row(redis, view["schedule_id"])
        assert row["version"] == "1.0.0"
        assert row["runtime"] == "python"

    def test_publish_restamp_does_not_bump_updated_at(self, redis):
        """重章=服务端路由校正，非用户编辑：不碰 updated_at，与 #198 乐观锁
        零干连（发布后带旧 expected_updated_at 的编辑仍 200）。"""
        client = _client(redis)
        _publish_python_flow(client, "f1", version="1.0.0")
        view, row = _create_schedule(client, redis, version=None)
        base_updated_at = row["updated_at"]
        _publish_go_flow(client, "f1", version="2.0.0")
        row = self._row(redis, view["schedule_id"])
        assert row["runtime"] == "go"
        assert row["updated_at"] == base_updated_at
        resp = client.put(
            f"/api/schedules/{view['schedule_id']}",
            json={"name": "renamed", "expected_updated_at": base_updated_at},
            headers=_headers(),
        )
        assert resp.status_code == 200, resp.text

    def test_publish_restamp_isolated_by_tenant(self, redis):
        """他租户同 flow_id 的调度行不被误重章（flow_id 跨租户可重名）。"""
        client = _client(redis)
        _publish_python_flow(client, "f1", version="1.0.0")
        other = {
            "schedule_id": "s-other", "tenant_id": "other", "name": "n",
            "flow_id": "f1", "version": None, "runtime": "python",
            "cron": "* * * * *", "params": {}, "enabled": True,
            "created_at": "t0", "updated_at": "t0", "next_run_at": "",
        }
        redis.hset(SCHEDULES_KEY, "s-other", json.dumps(other))
        view, _row = _create_schedule(client, redis, version=None)
        _publish_go_flow(client, "f1", version="2.0.0")
        assert self._row(redis, "s-other")["runtime"] == "python"
        assert self._row(redis, view["schedule_id"])["runtime"] == "go"

    def test_publish_survives_restamp_failure(self, redis, monkeypatch):
        """重章 best-effort：CAS 失败只 WARNING，发布已成功不回滚不 500，
        行章停留旧值（陈旧窗留待下次发布/重启用收敛）。"""
        import plaita.server.services.schedule_service as svc

        def _boom(*args, **kwargs):
            raise RuntimeError("redis down")

        monkeypatch.setattr(svc, "cas_update_schedule", _boom)
        client = _client(redis)
        _publish_python_flow(client, "f1", version="1.0.0")
        view, _row = _create_schedule(client, redis, version=None)
        # _publish_go_flow 内含 POST /publish：重章在其内部失败，发布仍 200
        _publish_go_flow(client, "f1", version="2.0.0")
        assert self._row(redis, view["schedule_id"])["runtime"] == "python"
