"""duo40 / A56-02 探针：console env 设 v2 时调度派发落到哪个 Stream。

用法：``PL_SRC=<plaita 仓库根路径> python duo40-probe-a56-02.py``。
pre-fix 树（d64ff67）与修复后树的输出对比见 ``duo40-probe-a56-02.txt``。
"""
import os
import sys

sys.path.insert(0, os.environ["PL_SRC"])
os.environ["PLAITA_QUEUE_NAME"] = "plaita:flow:queue"
os.environ["PLAITA_CONSOLE_TASK_QUEUE"] = "plaita:flow:queue:v2"

import fakeredis  # noqa: E402

import plaita.server.services.schedule_service as sched  # noqa: E402

calls = []
sched.fire_schedule = (
    lambda redis, schedule, queue, **kwargs: calls.append(queue) or "1-0"
)
service = sched.ScheduleService(
    event_bus=object(),
    redis_client=fakeredis.FakeRedis(decode_responses=True),
    service_config={},
)
service._fire_if_due({
    "schedule_id": "s1", "name": "s1", "flow_id": "f1",
    "cron": "* * * * *", "enabled": True, "next_run_at": "0",
    "tenant_id": "default",
}, now_ms=10_000)
print("queue_used", calls)
