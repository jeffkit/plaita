"""plaita#28 红先证（缺陷形状，baseline 原生）：单 stream FIFO + 串行消费 ⇒
一个租户的重积压把后到租户的任务完整阻塞。

判据（issue #28 指认的缺陷形态）：
- 租户 A 先入队 K 条 start、租户 B 的 1 条 start 随后入队（同一条共享
  stream ``plaita:flow:queue``）；
- 串行消费下派发顺序 = 入队顺序（FIFO），B 的派发开始时间 ≥ A 最后一条
  的派发结束时间——B 为 A 的全部积压买单，等待时长随 A 积压规模线性增长；
- 全仓无任何按租户的准入/配额：A 可以制造任意大的积压（该半边由
  ``test_issue28_tenant_quota.py`` 守卫，baseline 上因无配额代码而为红）。

修复后本形状**仍然成立**：#28 最小修复在「入口」拦积压的形成（超配额
429/延迟），单 stream 内的相对顺序仍是 FIFO（按租户分 stream + 加权轮询是
issue 的进阶项）。本测试作为判据常驻：固定「队列内 FIFO 顺序」这一事实，
防止未来改动悄悄破坏顺序语义。
"""
from __future__ import annotations

import threading
import time

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _make_worker(fake) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue28-fifo",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


def test_single_stream_fifo_tenant_b_waits_for_tenant_a_backlog():
    """A×4 慢任务 + B×1 快任务（同 stream）：B 必须等满 A 的全部积压。"""
    fake = fakeredis.FakeRedis(decode_responses=True)
    worker = _make_worker(fake)
    queue = worker._get_task_queue()
    queue.ensure_group()

    TASK_COST = 0.02
    records: list = []  # (tenant, seq, t_start, t_end)
    lock = threading.Lock()

    def slow_dispatch(message_data, delivery_count=None, schema_version=None,
                      stream_message_id=None):
        t0 = time.monotonic()
        time.sleep(TASK_COST)  # 模拟重任务占住唯一的串行消费位
        t1 = time.monotonic()
        with lock:
            records.append((message_data.get("tenant_id"), message_data["seq"], t0, t1))

    worker._dispatch_task = slow_dispatch

    def _enqueue(tenant: str, seq: int) -> None:
        queue.enqueue({
            "type": "start",
            "flow_id": "f1",
            "tenant_id": tenant,
            "seq": seq,
        })

    for i in range(4):
        _enqueue("acme", i)
    _enqueue("beta", 100)

    worker._running = True
    loop = threading.Thread(target=worker._consume_loop, args=(queue,), daemon=True)
    loop.start()
    deadline = time.monotonic() + 10
    while len(records) < 5 and time.monotonic() < deadline:
        time.sleep(0.01)
    worker._running = False
    loop.join(timeout=5)
    assert not loop.is_alive(), "消费线程未在期限内退出"

    assert len(records) == 5, f"应处理完 5 条任务，实际 {len(records)}"
    records.sort(key=lambda r: r[2])  # 按派发开始时间

    # 1) 顺序 = 入队顺序（FIFO）：acme 0..3 全部先于 beta
    order = [(t, seq) for t, seq, _, _ in records]
    assert order == [("acme", 0), ("acme", 1), ("acme", 2), ("acme", 3), ("beta", 100)], order

    # 2) 饿死形状：beta 的派发开始 ≥ acme 最后一条的派发结束——
    #    beta 为 acme 的全部积压买单（等待 ≥ 4×单任务成本）。
    acme_last_end = records[3][3]
    beta_start = records[4][2]
    assert beta_start >= acme_last_end, (
        "beta 未等满 acme 积压（不应发生：串行 FIFO 下不可能插队）"
    )
    wait = beta_start - records[0][2]
    assert wait >= 4 * TASK_COST * 0.8, (
        f"beta 等待 {wait:.3f}s，应 ≥ acme 积压总成本 "
        f"{4 * TASK_COST:.3f}s 的大部分（#28 饿死形状）"
    )
