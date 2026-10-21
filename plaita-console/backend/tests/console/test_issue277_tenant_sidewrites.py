"""#277 (r54) console probe: delete_tenant → in-flight DLQ write must not revive.

Real ``tenants_svc.create_tenant`` / ``delete_tenant`` with fakeredis:
1. create acme ⇒ Redis status active + generation meta published;
2. capture the generation an in-flight worker would have (old gen);
3. delete acme ⇒ disabled tombstone + purge;
4. the old carrier's dead-letter write is rejected and the DLQ stays empty;
5. recreate acme (new generation) ⇒ old generation still rejected; a new
   generation carrier can dead-letter normally.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")

import fakeredis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store, tenants_svc  # noqa: E402
from plaita.server.task_queue import RedisStreamTaskQueue, StreamTask  # noqa: E402
from plaita.server.tenant_status import read_tenant_generation  # noqa: E402

TENANT = "acme"
STREAM = "test:r54-console-stream"
DLQ = "test:r54-console-dlq"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "issue277.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    return flow_store.get_flow_store(), fakeredis.FakeRedis(decode_responses=True)


def _queue(fake):
    return RedisStreamTaskQueue(
        fake,
        STREAM,
        group_name="g",
        consumer_name="c",
        dlq_key=DLQ,
        claim_min_idle_ms=0,
    )


def _task(generation):
    return StreamTask(
        message_id="1-0",
        body={
            "type": "start",
            "tenant_id": TENANT,
            "execution_id": "e",
            "params": {"secret": "OLD-SECRET"},
        },
        tenant_generation=generation,
    )


def test_delete_then_inflight_dlq_write_rejected(env):
    store, fake = env
    tenants_svc.create_tenant(store, TENANT, redis_client=fake)
    old_generation = read_tenant_generation(fake, TENANT)
    assert old_generation

    assert tenants_svc.delete_tenant(store, TENANT, redis_client=fake) is True
    queue = _queue(fake)
    assert queue.dead_letter_if_allowed(_task(old_generation), reason="inflight") is False
    assert fake.xlen(DLQ) == 0, "已删租户的在途死信不得复活 DLQ"

    # 同名重建：新世代，旧世代仍拒绝；新世代载体可正常死信。
    tenants_svc.create_tenant(store, TENANT, redis_client=fake)
    new_generation = read_tenant_generation(fake, TENANT)
    assert new_generation and new_generation != old_generation
    assert queue.dead_letter_if_allowed(_task(old_generation), reason="inflight") is False
    assert fake.xlen(DLQ) == 0
    assert queue.dead_letter_if_allowed(_task(new_generation), reason="new") is True
    assert fake.xlen(DLQ) == 1
