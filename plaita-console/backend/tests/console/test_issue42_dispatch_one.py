"""#42 期 1（W23-a）console 侧：``dispatch_one`` 真实派发 + 派发台账 + 队列隔离。

## 红先证（RED）

基线 `11c10162`（v1 竖切）console 只有 ``dispatch/plan``（影子只算不发）：
``services/dispatcher.py`` 无 ``dispatch_one``、``api/capacity.py`` 无
``/dispatch/one`` 与 ``/dispatch/ledger`` 路由 ⇒ 本组首个断言
（``dispatch_one`` 可调用）在基线必然失败（``AttributeError``）。

## 断言要点（期 1 §5）

1. **默认零行为变化**：``dispatch_one`` 默认 ``dry_run=True``、影子模式默认开
   ⇒ 调用前后 **Redis 键集合完全一致**（不入队、不写台账）；
2. **容量路由真的生效**：低盘 + 高盘并存时，真实派发的消息体带
   ``target_instance_id = 高盘 worker``（低盘机拿到会走既有交接让出）；
3. **判据②**：台账条数 == 入队条数（``/dispatch/ledger`` 的 ``parity`` 为真）；
4. **判据③**：金丝雀队列与生产队列 XLEN **互相独立**；
5. **红线**：池里没有可派 worker（全低盘 / 存量无容量上报）⇒ 照派，只是不带
   目标字段（广播抢单），**不因上报缺失停摆**。
"""
import json
import logging
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
PLAITA_ROOT = str(BACKEND_DIR.parents[1])
for _p in (str(BACKEND_DIR), PLAITA_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from api import capacity as capacity_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import dispatcher as dispatcher_svc  # noqa: E402

from plaita.server.capacity import CAPACITY_METADATA_KEY  # noqa: E402
from plaita.server.dispatch_ledger import DEFAULT_LEDGER_KEY  # noqa: E402
from plaita.server.registry import ServiceInfo, ServiceRegistry  # noqa: E402

ADMIN_KEY = "w23a-admin-key"
PROD_QUEUE = "plaita:flow:queue"
CANARY_QUEUE = "plaita:flow:queue:canary"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def prod_shadow_off(monkeypatch):
    """显式关掉影子模式（真发单的第二道闸）。"""
    monkeypatch.setenv("PLAITA_CONSOLE_DISPATCH_SHADOW", "0")


def _capacity_block(**over):
    block = {
        "schema_version": 1,
        "free_disk_gib": 120.0,
        "min_free_disk_gib": 20.0,
        "disk_blocked": False,
        "active_tasks": 0,
        "concurrency": 1,
        "sampled_at": datetime.now(timezone.utc).isoformat(),
    }
    block.update(over)
    return block


def _register_worker(redis, instance_id, *, block=None, queue_name=CANARY_QUEUE):
    """注册到本次派发队列。

    A64-05 之后路由带队列维度：worker 必须与 ``dispatch_one`` 的队列一致
    才可能成为 target。本文件的主场景是金丝雀队列，故缺省注册在 CANARY。
    """
    reg = ServiceRegistry(redis)
    metadata = {} if queue_name is None else {"queue_name": queue_name}
    if block is not None:
        metadata[CAPACITY_METADATA_KEY] = json.dumps(block)
    info = ServiceInfo(
        instance_id=instance_id,
        service_type="flow_worker",
        host=f"{instance_id}.host",
        metadata=metadata,
    )
    reg.register(info)
    return info


@pytest.fixture()
def redis():
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def client(redis, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(
        capacity_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _pool(redis):
    """低盘 + 高盘各一台（红先证里的关键对照）。"""
    _register_worker(redis, "w-low", block=_capacity_block(
        free_disk_gib=3.0, min_free_disk_gib=20.0, disk_blocked=True,
    ))
    _register_worker(redis, "w-high", block=_capacity_block(free_disk_gib=120.0))


def _message(redis, queue, index=0):
    return json.loads(redis.xrange(queue)[index][1]["payload"])


class TestDispatchOneDefaultsAreSafe:
    def test_default_posture_writes_nothing(self, redis, monkeypatch):
        """默认姿态（影子开 + dry_run 默认真）⇒ 决策可见、Redis 零写入。"""
        monkeypatch.delenv("PLAITA_CONSOLE_DISPATCH_SHADOW", raising=False)
        _pool(redis)
        before = set(redis.keys("*"))
        res = dispatcher_svc.dispatch_one(redis, "issue-1", queue_name=CANARY_QUEUE)
        assert res["dry_run"] is True
        assert res["decision"] == "shadow", "影子模式默认开（验收 5 的第一道闸）"
        assert res["target"] == "w-high", "只算不发也要给出真实的路由结论"
        assert res["enqueued"] is False and res["ledger_recorded"] is False
        assert set(redis.keys("*")) == before, "只算不发不得写任何键"

    def test_dry_run_is_the_second_gate(self, redis, prod_shadow_off):
        """影子关了也还不够：``dry_run`` 默认真 ⇒ 仍然零写入。"""
        _pool(redis)
        before = set(redis.keys("*"))
        res = dispatcher_svc.dispatch_one(redis, "issue-1", queue_name=CANARY_QUEUE)
        assert res["shadow"] is False
        assert res["dry_run"] is True
        assert res["decision"] == "dry_run"
        assert res["target"] == "w-high"
        assert res["enqueued"] is False and res["ledger_recorded"] is False
        assert set(redis.keys("*")) == before, "干跑不得写任何键"

    def test_shadow_mode_still_blocks_real_dispatch(self, redis, monkeypatch):
        """回归验收 5：影子模式开着时，即便 ``dry_run=False`` 也零新增。"""
        monkeypatch.delenv("PLAITA_CONSOLE_DISPATCH_SHADOW", raising=False)
        _pool(redis)
        before = set(redis.keys("*"))
        res = dispatcher_svc.dispatch_one(
            redis, "issue-1", queue_name=CANARY_QUEUE, dry_run=False,
        )
        assert res["shadow"] is True
        assert res["decision"] == "shadow"
        assert res["enqueued"] is False and res["ledger_recorded"] is False
        assert set(redis.keys("*")) == before, "影子期生产队列零新增"


class TestDispatchOneRoutesByCapacity:
    def test_message_targets_high_disk_worker(self, redis, prod_shadow_off):
        """判据①：真实派发的消息体带容量路由选中的目标（不是随便一台）。"""
        _pool(redis)
        res = dispatcher_svc.dispatch_one(
            redis, "issue-1", queue_name=CANARY_QUEUE, dry_run=False,
        )
        assert res["enqueued"] is True and res["ledger_recorded"] is True
        assert res["target"] == "w-high"
        assert res["decision"] == "targeted"
        assert res["reason"].startswith("route: capacity")

        body = _message(redis, CANARY_QUEUE)
        assert body["target_instance_id"] == "w-high"
        assert body["type"] == "start"
        assert body["execution_id"] == "issue-1"
        # 低盘机被容量门拒绝，且**没有**成为目标
        assert [r["instance_id"] for r in res["rejected"]] == ["w-low"]
        assert res["rejected"][0]["reason"].startswith("disk")

    def test_ledger_records_decision_who_and_why(self, redis, prod_shadow_off):
        _pool(redis)
        dispatcher_svc.dispatch_one(redis, "issue-1", queue_name=CANARY_QUEUE, dry_run=False)
        rows = dispatcher_svc.ledger_rows(redis)
        assert len(rows) == 1
        row = rows[0]
        assert row["issue_id"] == "issue-1"
        assert row["target_instance_id"] == "w-high"
        assert row["decision"] == "targeted"
        assert row["reason"].startswith("route: capacity")
        assert row["queue"] == CANARY_QUEUE
        assert row["decided_by"], "台账必须能回答「谁派的」"
        assert row["enqueued"] is True
        assert row["ledger_schema_version"] == 1

    def test_ledger_count_equals_enqueue_count(self, redis, prod_shadow_off):
        """判据②：批量派发 N 条 ⇒ 台账 N 条 == 队列 XLEN N。"""
        _pool(redis)
        out = dispatcher_svc.dispatch_many(
            redis, ["issue-1", "issue-2", "issue-3", "issue-4"],
            queue_name=CANARY_QUEUE, dry_run=False,
        )
        assert out["requested"] == 4
        assert out["enqueued_count"] == 4
        assert out["ledger_count"] == 4
        assert out["targets"] == ["w-high"]

        parity = dispatcher_svc.dispatch_parity(redis, CANARY_QUEUE)
        assert parity["parity"] is True
        assert parity["ledger"] == parity["queue"] == redis.xlen(CANARY_QUEUE) == 4

    def test_canary_xlen_independent_from_production(self, redis, prod_shadow_off):
        """判据③：派发到金丝雀队列 ⇒ 生产队列 XLEN 恒 0，两队列互不影响。"""
        _pool(redis)
        dispatcher_svc.dispatch_many(
            redis, ["c-1", "c-2"], queue_name=CANARY_QUEUE, dry_run=False,
        )
        assert redis.xlen(CANARY_QUEUE) == 2
        assert redis.xlen(PROD_QUEUE) == 0
        # 反向：派生产队列不影响金丝雀，且只在生产队列的 worker 中选目标
        _register_worker(
            redis, "w-prod", block=_capacity_block(), queue_name=PROD_QUEUE,
        )
        prod = dispatcher_svc.dispatch_one(
            redis, "p-1", queue_name=PROD_QUEUE, dry_run=False,
        )
        assert prod["target"] == "w-prod"
        assert redis.xlen(PROD_QUEUE) == 1
        assert redis.xlen(CANARY_QUEUE) == 2
        assert redis.xlen(DEFAULT_LEDGER_KEY) == 3, "台账条目 == 入队条数（2+1）"


class TestDispatchQueueScoping:
    """A64-05：容量路由带队列维度，投递队列 == 目标 worker 消费队列。"""

    V2 = "plaita:flow:queue:v2"

    def test_cross_queue_candidate_is_rejected_not_targeted(
        self, redis, prod_shadow_off, caplog,
    ):
        """只注册 v2 worker 时派 v1：不得选中它，消息留 v1 广播且不带 target。"""
        _register_worker(
            redis, "w-v2", block=_capacity_block(free_disk_gib=500.0),
            queue_name=self.V2,
        )
        with caplog.at_level(logging.WARNING, logger="services.dispatcher"):
            res = dispatcher_svc.dispatch_one(
                redis, "issue-cq", queue_name=CANARY_QUEUE, dry_run=False,
            )
        assert res["target"] is None, "跨队列 worker 不得成为目标（A64-05）"
        assert res["decision"] == "broadcast"
        assert res["reason"].startswith("route: no-dispatchable-worker")
        assert [r["instance_id"] for r in res["rejected"]] == ["w-v2"]
        assert res["rejected"][0]["reason"].startswith("queue: mismatch")
        assert "没有消费该队列的 worker" in caplog.text, "排除必须可观测（WARNING）"
        assert "target_instance_id" not in _message(redis, CANARY_QUEUE)
        assert redis.xlen(CANARY_QUEUE) == 1
        assert redis.xlen(self.V2) == 0
        row = dispatcher_svc.ledger_rows(redis)[0]
        assert row["target_instance_id"] is None
        assert row["queue"] == CANARY_QUEUE

    def test_targeted_dispatch_lands_on_target_queue(self, redis, prod_shadow_off):
        """同队列派发：目标写进消息体，消息落在该 worker 消费的队列。"""
        _register_worker(
            redis, "w-v2", block=_capacity_block(free_disk_gib=500.0),
            queue_name=self.V2,
        )
        res = dispatcher_svc.dispatch_one(
            redis, "issue-v2", queue_name=self.V2, dry_run=False,
        )
        assert res["target"] == "w-v2"
        assert res["queue"] == self.V2, "投递队列以目标 worker 的 queue_name 为准"
        assert _message(redis, self.V2)["target_instance_id"] == "w-v2"
        assert redis.xlen(self.V2) == 1 and redis.xlen(CANARY_QUEUE) == 0

    def test_plan_is_queue_scoped_and_reports_mismatch(self, redis):
        """影子计划同样按队列过滤，跨队列候选带稳定原因进入 rejected。"""
        _register_worker(
            redis, "w-v2", block=_capacity_block(free_disk_gib=500.0),
            queue_name=self.V2,
        )
        plan = dispatcher_svc.build_dispatch_plan(redis, queue_name=CANARY_QUEUE)
        assert plan["target"] is None
        assert plan["allowed"] == []
        assert [r["instance_id"] for r in plan["rejected"]] == ["w-v2"]
        assert plan["rejected"][0]["reason"].startswith("queue: mismatch")

    def test_worker_without_queue_metadata_is_unknown_not_default(
        self, redis, prod_shadow_off, monkeypatch,
    ):
        """B75-01：缺 metadata 的 worker 是 unknown，不再按默认队列假设。

        默认 skip：无论派哪个队列都不定向（unknown ≠ default），但消息仍按
        调用方队列**广播**——legacy default worker 仍能抢到（不带 target 字段）。
        assume-default 兼容开关只对默认队列生效，且必须标记 queue_source。
        """
        _register_worker(redis, "w-legacy", queue_name=None)
        miss = dispatcher_svc.dispatch_one(
            redis, "issue-legacy-v1", queue_name=CANARY_QUEUE, dry_run=False,
        )
        assert miss["target"] is None
        assert miss["decision"] == "broadcast"
        assert miss["rejected"][0]["reason"].startswith("queue: unknown")

        broadcast = dispatcher_svc.dispatch_one(
            redis, "issue-legacy-default", queue_name=PROD_QUEUE, dry_run=False,
        )
        assert broadcast["target"] is None
        assert broadcast["decision"] == "broadcast"
        assert broadcast["queue_source"] == "declared"
        assert broadcast["rejected"][0]["reason"].startswith("queue: unknown")
        assert "target_instance_id" not in _message(redis, PROD_QUEUE)

        monkeypatch.setenv("PLAITA_DISPATCH_UNKNOWN_QUEUE", "assume-default")
        assumed = dispatcher_svc.dispatch_one(
            redis, "issue-legacy-assumed", queue_name=PROD_QUEUE, dry_run=False,
        )
        assert assumed["target"] == "w-legacy"
        assert assumed["queue"] == PROD_QUEUE
        assert assumed["queue_source"] == "assumed-default"
        assert _message(redis, PROD_QUEUE, index=1)["target_instance_id"] == "w-legacy"


class TestRedLinesAllowWhenRouteUnavailable:
    def test_all_workers_rejected_still_dispatches_broadcast(self, redis, prod_shadow_off):
        """红线：全池被容量门拒 ⇒ **照派**（不带目标 = 广播抢单），不停摆。"""
        _register_worker(redis, "w-only-low", block=_capacity_block(
            free_disk_gib=1.0, min_free_disk_gib=20.0, disk_blocked=True,
        ))
        res = dispatcher_svc.dispatch_one(
            redis, "issue-bc", queue_name=CANARY_QUEUE, dry_run=False,
        )
        assert res["enqueued"] is True
        assert res["target"] is None
        assert res["decision"] == "broadcast"
        assert res["reason"].startswith("route: no-dispatchable-worker")
        assert "target_instance_id" not in _message(redis, CANARY_QUEUE)
        assert dispatcher_svc.ledger_rows(redis)[0]["decision"] == "broadcast"

    def test_legacy_worker_without_capacity_is_still_dispatchable(self, redis, prod_shadow_off):
        """红线：存量 worker（无容量上报）仍可被派单（不因上报缺失停摆）。"""
        _register_worker(redis, "w-legacy")  # block=None ⇒ 无 worker_capacity
        res = dispatcher_svc.dispatch_one(
            redis, "issue-legacy", queue_name=CANARY_QUEUE, dry_run=False,
        )
        assert res["enqueued"] is True
        assert res["target"] == "w-legacy"
        assert _message(redis, CANARY_QUEUE)["target_instance_id"] == "w-legacy"

    def test_empty_pool_still_dispatches_broadcast(self, redis, prod_shadow_off):
        """红线：注册表为空（Redis 有、worker 未注册）⇒ 广播放行。"""
        res = dispatcher_svc.dispatch_one(
            redis, "issue-none", queue_name=CANARY_QUEUE, dry_run=False,
        )
        assert res["enqueued"] is True and res["target"] is None


class TestDispatchHttpSurface:
    def test_requires_admin_key(self, client):
        resp = client.post("/api/dispatch/one", json={"issue_id": "i-1"})
        assert resp.status_code in (401, 403)

    def test_one_endpoint_defaults_to_dry_run(self, client, redis):
        _pool(redis)
        before = set(redis.keys("*"))
        resp = client.post(
            "/api/dispatch/one", json={"issue_id": "i-1", "queue_name": CANARY_QUEUE},
            headers=_headers(),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["dry_run"] is True
        assert body["enqueued"] is False
        assert body["target"] == "w-high"
        assert set(redis.keys("*")) == before

    def test_one_endpoint_enqueues_when_both_gates_open(
        self, client, redis, prod_shadow_off,
    ):
        _pool(redis)
        resp = client.post(
            "/api/dispatch/one",
            json={"issue_id": "i-1", "queue_name": CANARY_QUEUE, "dry_run": False},
            headers=_headers(),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["enqueued"] is True and body["ledger_recorded"] is True
        assert body["target"] == "w-high"
        assert body["message_id"]
        assert redis.xlen(CANARY_QUEUE) == 1
        assert redis.xlen(PROD_QUEUE) == 0

    def test_ledger_endpoint_reports_parity_and_rows(self, client, redis, prod_shadow_off):
        _pool(redis)
        dispatcher_svc.dispatch_many(
            redis, ["i-1", "i-2"], queue_name=CANARY_QUEUE, dry_run=False,
        )
        resp = client.get(
            "/api/dispatch/ledger",
            params={"queue_name": CANARY_QUEUE, "count": 10},
            headers=_headers(),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["queue"] == CANARY_QUEUE
        assert body["ledger"] == body["queue_length"] == 2
        assert body["parity"] is True
        assert body["error"] is None
        assert [r["issue_id"] for r in body["rows"]] == ["i-1", "i-2"]

    def test_ledger_endpoint_parity_false_when_queue_differs(self, client, redis, prod_shadow_off):
        _pool(redis)
        dispatcher_svc.dispatch_one(redis, "i-1", queue_name=CANARY_QUEUE, dry_run=False)
        resp = client.get(
            "/api/dispatch/ledger", params={"queue_name": PROD_QUEUE}, headers=_headers(),
        )
        body = resp.json()
        assert body["ledger"] == 1 and body["queue_length"] == 0
        assert body["parity"] is False, "对账必须如实报缺口（不许粉饰）"
