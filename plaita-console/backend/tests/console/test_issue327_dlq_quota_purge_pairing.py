"""plaita#327：per-tenant DLQ 配额计数 × delete purge 两键成对。

## 缺陷（A2R58-05 的配额版延长线）

配额计数（``plaita:tenant_dlq_count``）只按 tenant_id 记账。
``delete_tenant`` 若只 purge DLQ 条目、不清计数，同 slug 重建的新世代会
**继承幻影计数**——新租户开局即配额吃紧，爆炸半径保护反噬受害租户。

## 修复契约

``_purge_runtime_after_commit`` 的清理步骤在 ``dlq`` 之后新增
``dlq_quota_count`` 步：计数清零与 DLQ purge 成对。隔离流条目刻意保留
（A125 证据保全），但其计数随世代退役——新世代从干净预算开局。

## 负控（还原修复后必须精确红）

- 去掉 ``dlq_quota_count`` 步：``test_purge_pairs_with_quota_count_clear``
  由「计数清零」变红（幻影计数留存）；
- 去掉成对后的放行面：``test_recreated_tenant_starts_with_clean_quota``
  的 allowed=True 剧本变红（满格拒绝，A2 r58 实弹剧本复刻）。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest
from fakeredis import FakeRedis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import tenants_svc  # noqa: E402

from plaita.server.task_queue import RedisStreamTaskQueue  # noqa: E402
from plaita.server.task_queue import StreamTask  # noqa: E402
from plaita.server import tenant_dlq_quota  # noqa: E402
from plaita.server.tenant_dlq_quota import TENANT_DLQ_COUNT_KEY  # noqa: E402

QUEUE = "plaita:flow:queue"
DLQ = f"{QUEUE}:dlq"
TENANT = "acme"


def _dlq_envelope(tenant: str) -> str:
    return json.dumps(
        {
            "reason": "max_deliveries=5",
            "source_stream": QUEUE,
            "source_id": "1-0",
            "delivery_count": 5,
            "dead_lettered_at": datetime.now().timestamp(),
            "schema_version": 1,
            "payload": {
                "type": "start",
                "flow_id": "f1",
                "execution_id": "exec-1",
                "tenant_id": tenant,
            },
        }
    )


def test_purge_pairs_with_quota_count_clear():
    """删除清理跑完，配额计数必须一并清零（两键成对）。"""
    fake = FakeRedis(decode_responses=True)
    fake.xadd(DLQ, {"payload": _dlq_envelope(TENANT)})
    fake.hset(TENANT_DLQ_COUNT_KEY, TENANT, 42)

    counts, first_error = tenants_svc._purge_runtime_after_commit(fake, TENANT)
    assert first_error is None
    assert counts.get("dlq_quota_count") in (0, 1), "计数清理步骤必须入账"
    assert int(fake.xlen(DLQ) or 0) == 0, "DLQ 条目照旧 purge（#264 守恒）"
    assert fake.hget(TENANT_DLQ_COUNT_KEY, TENANT) is None, "幻影计数必须清零"


def test_recreated_tenant_starts_with_clean_quota():
    """A2 r58 allowed=True 剧本：delete → 同 slug 重建 → 配额闸放行。"""
    fake = FakeRedis(decode_responses=True)
    # 前代：配额已打满（幻影计数源）。
    fake.hset(TENANT_DLQ_COUNT_KEY, TENANT, 200)

    counts, first_error = tenants_svc._purge_runtime_after_commit(fake, TENANT)
    assert first_error is None

    # 同名重建（新世代）：无状态记录/无世代 = 机制前租户，purity 放行；
    # 配额闸必须从干净计数开局——不继承前代幻影。
    queue = RedisStreamTaskQueue(
        fake, QUEUE, group_name="plaita-workers", consumer_name="c1",
        dlq_key=DLQ,
    )
    assert queue.tenant_dlq_max_entries == tenant_dlq_quota.DEFAULT_TENANT_DLQ_MAX_ENTRIES
    task = StreamTask(
        message_id="1-1",
        body={
            "type": "resume",
            "flow_id": "f1",
            "execution_id": "exec-new",
            "resume_type": "continue",
            "tenant_id": TENANT,
        },
        delivery_count=5,
    )
    assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is True, (
        "重建租户的死信写必须 allowed=True（幻影计数不得继承）"
    )
    assert int(fake.xlen(DLQ) or 0) == 1
    assert tenant_dlq_quota.tenant_dlq_count(fake, TENANT) == 1, (
        "新世代从 0 起算（前代 200 不入账）"
    )
