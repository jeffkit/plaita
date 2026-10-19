"""台账 A50-01/A50-02/A50-05：租户删除与并发写/共享队列的收敛。

同一根因族：``delete_tenant`` 的提交瞬间与并发写路径没有共同串行化点。

- A50-01：save_credential 通过「租户存在」检查后、删除提交，save 仍写
  凭据行 + 旁文件；同名重建后旧密钥可解密。
- A50-02：add_member 同窗 TOCTOU，留下孤儿 admin；SQLite ``foreign_keys=0``
  使声明的级联不生效；同名重建后该用户直接以租户 admin 登录。
- A50-05：共享队列 ``plaita:flow:queue`` 里的旧 start 消息删除时不清，
  同名重建把墓碑覆盖回 active 后重新执行。

修法（``services/tenant_write_guard.py``）：SQLite 删除事务先
``BEGIN IMMEDIATE``；写路径 flush（取得写锁）后在同一事务内复检租户。
PostgreSQL 两侧对租户行 ``SELECT ... FOR UPDATE``。删除同时按消息体
``tenant_id`` 排空共享队列（含消费组 XACK）。

负控（必红）：
1. 去掉 save_credential 的 flush 后复检 → A50-01 复活（凭据行 + 旁文件）；
2. 去掉 add_member 的 flush 后复检 → A50-02 孤儿 admin；
3. 去掉 ``_purge_tenant_queue`` 调用 → A50-05 旧 start 仍在流里；
4. 去掉 delete_tenant 的 ``begin_guarded_transaction`` → 写提交落在删除
   「读凭据之后、flush 之前」的反向窗口重新出现；
5. 仅存在性复检、不比对世代令牌 → delete→同名 rebuild 的 ABA 用例红
   （旧密钥/旧成员进入新租户）。
"""
from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from sqlalchemy import select

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import Credential, TenantMember  # noqa: E402
from plaita.server.task_queue import enqueue_task  # noqa: E402
from plaita.tenant_context import (  # noqa: E402
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    set_current_tenant,
)
from services import credentials_svc, flow_store, tenants_svc, users_svc  # noqa: E402

TENANT = "acme"
OTHER = "beta"
QUEUE = "plaita:flow:queue"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "a50.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.delenv("PLAITA_CONSOLE_TASK_QUEUE", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    fake = FakeRedis(decode_responses=True)
    tenants_svc.create_tenant(store, TENANT, "Acme", redis_client=fake)
    tenants_svc.create_tenant(store, OTHER, "Beta", redis_client=fake)
    return {"store": store, "redis": fake, "tmp": tmp_path}


def test_delete_tenant_beats_late_save_credential(env, monkeypatch):
    """A50-01：save 已过租户检查 → delete 完成 → save 必须整体失败且零残留。"""
    store = env["store"]
    checked, release = threading.Event(), threading.Event()
    outcome = {}
    real_encrypt = credentials_svc._encrypt

    def paused_encrypt(data):
        # save_credential 在 _encrypt 之前已完成「租户存在」检查。
        checked.set()
        assert release.wait(5.0), "probe deadlock: release never signalled"
        return real_encrypt(data)

    monkeypatch.setattr(credentials_svc, "_encrypt", paused_encrypt)

    def save():
        set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
        try:
            credentials_svc.save_credential(
                "db-pass", "generic", {"password": "s3cret"}, "probe",
                tenant_id=TENANT,
            )
            outcome["result"] = "returned-ok"
        except Exception as exc:  # noqa: BLE001
            outcome["result"] = f"{type(exc).__name__}: {exc}"

    worker = threading.Thread(target=save)
    worker.start()
    assert checked.wait(5.0), "save never reached _encrypt"
    assert tenants_svc.delete_tenant(store, TENANT, redis_client=None) is True
    release.set()
    worker.join(10.0)
    assert not worker.is_alive(), "save thread did not finish"
    assert outcome["result"].startswith("ValueError:"), outcome
    assert "租户不存在" in outcome["result"]

    sidecar = credentials_svc.credentials_file(TENANT)
    with store._session_local() as session:
        assert session.scalars(
            select(Credential).where(Credential.tenant_id == TENANT)
        ).all() == []
    assert not sidecar.exists(), "迟到的 save 不得写出旧密钥旁文件"

    tenants_svc.create_tenant(store, TENANT, "Acme recreated")
    assert credentials_svc.get_credential_record("db-pass", tenant_id=TENANT) is None


def test_delete_tenant_beats_late_add_member(env, monkeypatch):
    """A50-02：add_member 已过租户检查 → delete 完成 → 不得留孤儿 admin。"""
    store = env["store"]
    users_svc.create_user(store, "bob", "bob-password-1", "viewer")

    entered, release = threading.Event(), threading.Event()
    outcome = {}
    real_is_member = users_svc._is_member

    def paused_is_member(session, username, tenant_id):
        # 调用点位于 add_member 的租户存在检查之后、插入之前。
        if username == "bob" and tenant_id == TENANT:
            entered.set()
            assert release.wait(5.0), "probe deadlock: release never signalled"
        return real_is_member(session, username, tenant_id)

    monkeypatch.setattr(users_svc, "_is_member", paused_is_member)

    def add():
        try:
            users_svc.add_member(store, TENANT, "bob", "admin")
            outcome["result"] = "returned-ok"
        except Exception as exc:  # noqa: BLE001
            outcome["result"] = f"{type(exc).__name__}: {exc}"

    worker = threading.Thread(target=add)
    worker.start()
    assert entered.wait(5.0), "add_member never reached _is_member"
    assert tenants_svc.delete_tenant(store, TENANT, redis_client=None) is True
    release.set()
    worker.join(10.0)
    assert not worker.is_alive(), "add_member thread did not finish"
    assert outcome["result"].startswith("UserError:"), outcome

    with store._session_local() as session:
        members = session.scalars(
            select(TenantMember).where(TenantMember.tenant_id == TENANT)
        ).all()
    assert members == [], "删除后不得留下孤儿成员行"

    tenants_svc.create_tenant(store, TENANT, "Acme recreated")
    login = users_svc.login(store, "bob", "bob-password-1")
    assert login is not None
    assert login["role"] is None, "同名重建后旧成员不得继承租户 admin"
    assert login["active_tenant"] is None
    assert login["memberships"] == []


def test_save_credential_cannot_attach_to_recreated_tenant(env, monkeypatch):
    """A50-01 ABA：delete + 同名 create 在 save 复检前完成，旧 save 必须失败。

    只查「租户存在」会放行——新租户行存在但世代不同。世代令牌
    （created_at + 随机 contract_secret_id）在同一写事务内复核后拒绝。
    """
    store = env["store"]
    checked, release = threading.Event(), threading.Event()
    outcome = {}
    real_encrypt = credentials_svc._encrypt

    def paused_encrypt(data):
        checked.set()
        assert release.wait(5.0), "probe deadlock: release never signalled"
        return real_encrypt(data)

    monkeypatch.setattr(credentials_svc, "_encrypt", paused_encrypt)

    def save():
        set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
        try:
            credentials_svc.save_credential(
                "db-pass", "generic", {"password": "s3cret"}, tenant_id=TENANT
            )
            outcome["result"] = "returned-ok"
        except Exception as exc:  # noqa: BLE001
            outcome["result"] = f"{type(exc).__name__}: {exc}"

    worker = threading.Thread(target=save)
    worker.start()
    assert checked.wait(5.0), "save never reached _encrypt"
    old_token = None
    with store._session_local() as session:
        from models.flow import Tenant
        row = session.get(Tenant, TENANT)
        old_token = (row.created_at, row.contract_secret_id)

    assert tenants_svc.delete_tenant(store, TENANT, redis_client=None) is True
    tenants_svc.create_tenant(store, TENANT, "Acme recreated")  # ABA
    with store._session_local() as session:
        from models.flow import Tenant
        row = session.get(Tenant, TENANT)
        assert (row.created_at, row.contract_secret_id) != old_token

    release.set()
    worker.join(10.0)
    assert not worker.is_alive()
    assert outcome["result"].startswith("ValueError:"), outcome
    with store._session_local() as session:
        assert session.scalars(
            select(Credential).where(Credential.tenant_id == TENANT)
        ).all() == []
    assert not credentials_svc.credentials_file(TENANT).exists()


def test_add_member_cannot_attach_to_recreated_tenant(env, monkeypatch):
    """A50-02 ABA：delete + 同名 create 在 add_member 复检前完成 → 不得入组。"""
    store = env["store"]
    users_svc.create_user(store, "bob", "bob-password-1", "viewer")
    entered, release = threading.Event(), threading.Event()
    outcome = {}
    real_is_member = users_svc._is_member

    def paused_is_member(session, username, tenant_id):
        if username == "bob" and tenant_id == TENANT:
            entered.set()
            assert release.wait(5.0), "probe deadlock: release never signalled"
        return real_is_member(session, username, tenant_id)

    monkeypatch.setattr(users_svc, "_is_member", paused_is_member)

    def add():
        try:
            users_svc.add_member(store, TENANT, "bob", "admin")
            outcome["result"] = "returned-ok"
        except Exception as exc:  # noqa: BLE001
            outcome["result"] = f"{type(exc).__name__}: {exc}"

    worker = threading.Thread(target=add)
    worker.start()
    assert entered.wait(5.0), "add_member never reached _is_member"
    assert tenants_svc.delete_tenant(store, TENANT, redis_client=None) is True
    tenants_svc.create_tenant(store, TENANT, "Acme recreated")  # ABA
    release.set()
    worker.join(10.0)
    assert not worker.is_alive()
    assert outcome["result"].startswith("UserError:"), outcome

    with store._session_local() as session:
        assert session.scalars(
            select(TenantMember).where(TenantMember.tenant_id == TENANT)
        ).all() == []
    login = users_svc.login(store, "bob", "bob-password-1")
    assert login is not None
    assert login["role"] is None
    assert login["memberships"] == []


def test_delete_lock_covers_write_committed_between_reads(env, monkeypatch):
    """反向窗口：save 在删除读完凭据后、flush 前提交。

    只靠写路径的 flush 后复检挡不住这一侧（复检时租户还在）；删除必须从
    第一次读起就持有串行化点，随后清理才能看到并删掉刚提交的凭据行。
    """
    store = env["store"]
    delete_entered, release_delete = threading.Event(), threading.Event()
    save_encrypted, save_done = threading.Event(), threading.Event()
    outcome = {}
    real_purge = tenants_svc._purge_local_tenant_runtime

    def paused_purge(session, tenant_id):
        # 调用点在「凭据 SELECT 之后、session.flush 之前」。
        delete_entered.set()
        assert release_delete.wait(5.0), "probe deadlock: delete never released"
        return real_purge(session, tenant_id)

    monkeypatch.setattr(tenants_svc, "_purge_local_tenant_runtime", paused_purge)

    real_encrypt = credentials_svc._encrypt

    def signaled_encrypt(data):
        token = real_encrypt(data)
        save_encrypted.set()
        return token

    monkeypatch.setattr(credentials_svc, "_encrypt", signaled_encrypt)

    def delete():
        try:
            outcome["delete"] = tenants_svc.delete_tenant(
                store, TENANT, redis_client=None
            )
        except Exception as exc:  # noqa: BLE001
            outcome["delete"] = f"{type(exc).__name__}: {exc}"

    def save():
        set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
        try:
            credentials_svc.save_credential(
                "late", "generic", {"password": "x"}, tenant_id=TENANT
            )
            outcome["save"] = "returned-ok"
        except Exception as exc:  # noqa: BLE001
            outcome["save"] = f"{type(exc).__name__}: {exc}"
        finally:
            save_done.set()

    deleter = threading.Thread(target=delete)
    deleter.start()
    assert delete_entered.wait(5.0), "delete never reached the purge step"

    writer = threading.Thread(target=save)
    writer.start()
    assert save_encrypted.wait(5.0), "save never reached encryption"
    # 修法生效时 save 阻塞在删除持有的写锁上（save_done 不置位）；负控
    # （删除不持锁）时 save 会在这里完成提交。
    save_done.wait(0.5)
    release_delete.set()
    deleter.join(10.0)
    writer.join(10.0)
    assert not deleter.is_alive() and not writer.is_alive()

    assert outcome["delete"] is True, outcome
    assert outcome["save"].startswith("ValueError"), outcome
    with store._session_local() as session:
        assert session.scalars(
            select(Credential).where(Credential.tenant_id == TENANT)
        ).all() == []


def test_delete_tenant_purges_shared_queue(env):
    """A50-05：删除按消息体 tenant_id 清共享 Stream，pending 一并 XACK。"""
    store, redis = env["store"], env["redis"]
    old = enqueue_task(redis, QUEUE, {
        "type": "start", "flow_id": "f1", "execution_id": "old-acme",
        "tenant_id": TENANT, "params": {"secret_ref": "old-tenant-input"},
    })
    keep = enqueue_task(redis, QUEUE, {
        "type": "start", "flow_id": "f1", "execution_id": "keep-beta",
        "tenant_id": OTHER, "params": {},
    })
    old2 = enqueue_task(redis, QUEUE, {
        "type": "start", "flow_id": "f1", "execution_id": "old-acme-2",
        "tenant_id": TENANT, "params": {},
    })
    redis.xgroup_create(QUEUE, "plaita-workers", id="0", mkstream=True)
    read = redis.xreadgroup(
        "plaita-workers", "worker-1", {QUEUE: ">"}, count=10
    )
    assert {entry[0] for entry in read[0][1]} == {old, keep, old2}
    assert redis.xpending(QUEUE, "plaita-workers")["pending"] == 3

    assert tenants_svc.delete_tenant(store, TENANT, redis_client=redis) is True

    remaining = [
        json.loads(fields["payload"]) for _, fields in redis.xrange(QUEUE)
    ]
    assert [(b["tenant_id"], b["execution_id"]) for b in remaining] == [
        (OTHER, "keep-beta")
    ]
    pending = redis.xpending_range(
        QUEUE, "plaita-workers", min="-", max="+", count=10
    )
    assert [p["message_id"] for p in pending] == [keep], (
        "被删租户的 pending 必须 XACK，不得留 PEL 幽灵"
    )

    # 同名重建：墓碑翻回 active，但旧消息已不在流里，不会重执行。
    tenants_svc.create_tenant(store, TENANT, "Acme recreated", redis_client=redis)
    assert redis.xlen(QUEUE) == 1
    assert all(
        json.loads(fields["payload"])["tenant_id"] == OTHER
        for _, fields in redis.xrange(QUEUE)
    )


def test_delete_nonexistent_tenant_also_purges_queue(env):
    """404 补清扫路径同样排空队列（旧版本删除残留的 start）。"""
    store, redis = env["store"], env["redis"]
    enqueue_task(redis, QUEUE, {
        "type": "start", "flow_id": "f1", "execution_id": "orphan",
        "tenant_id": "gone", "params": {},
    })
    assert tenants_svc.delete_tenant(store, "gone", redis_client=redis) is False
    assert redis.xrange(QUEUE) == []
