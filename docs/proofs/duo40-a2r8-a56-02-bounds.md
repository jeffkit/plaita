# duo40 — A2R8-04/05 性能上界 + A56-02 调度队列解析

日期：2026-10-20（B1 第四十轮）
基线：plaita-nodes `663dbd0`、plaita `d64ff67`
探针/输出：`docs/proofs/data/duo40-probe-a2r8-04.{py,txt}`、`duo40-probe-a2r8-05.{py,txt}`、
`duo40-probe-a56-02.{py,txt}`

## 1. A2R8-04 — sql_query 必须先物化整批再判限

**现状（plaita-nodes `663dbd0`）**：`database.py` 用固定 `fetchmany(256)`，
逐行判限发生在批次**已物化之后**。300 行 × 128 KiB、`max_bytes=1024` 时：
第一行即超限，但 256 行已在内存里。

```text
pre-fix:  limit error: observed_rows=1 observed_bytes=131096
          peak=33,905,971 bytes = 32.335 MiB = 33,111.3x budget
```

**修法**：首行单读校准行宽；此后每批行数 = `min(256, 剩余字节预算 / 已观测
最大行宽, 剩余行数)`，且至少 1 行（用于判定「恰好等于上限后是否还有更多行」）。
超限仍抛 `SqlResultLimitError`（fail-closed、不返回部分结果），
`observed_rows/observed_bytes` 语义不变。

```text
post-fix: limit error: observed_rows=1 observed_bytes=131096
          peak=453,737 bytes = 0.433 MiB = 443.1x budget
```

峰值 = 单行 128 KiB + SQLAlchemy 驱动开销；不再随结果行数增长。

**负控**：只回退 `src/plaita_nodes/database.py`、保留新测试
`tests/test_a2r8_04_sql_stream_bounds.py` → **2 failed / 1 passed**
（峰值断言 33.9 MB；上限内结果不变的对照用例保持绿）。恢复后 3 passed。

## 2. A2R8-05 — protocol_lines 无界驻留

**现状（`663dbd0`）**：`agent_run.py` 直跑与沙箱两条路径都直接
`protocol_lines.append(...)`。20k 条 ~1 KiB 的新字符串（真实回调每行一个新
对象）驻留 21.4 MiB（A2 台账口径 19.8 MiB）。

**修法**：新增 `_ProtocolLineBuffer`，行数 10,000 / UTF-8 字节 4 MiB 双闸，
两条路径共用。超限后继续消费但不驻留，末尾追加一条非 JSON 标记行
（`parse_stream_details` 与 usage 扫描会跳过），并打 WARNING：

```text
post-fix: agent 协议行超过上限，已截断：kept=3908 lines/4193284 bytes，
          limits=10000 lines/4194304 bytes，首个越界行=3909/4194357
          protocol_peak=4,392,661 bytes = 4.19 MiB
```

**负控**：只回退 `src/plaita_nodes/agent_run.py` → 新测试收集期
`ImportError`（`MAX_PROTOCOL_BYTES` 不存在）；行为面用 pre/post 探针钉住
21.42 MiB → 4.19 MiB。恢复后 6 passed。

## 3. A56-02 查漏 — plaita 服务端调度派发仍是静态队列名

**现状核对**：console 侧 `plaita-console/backend/api/schedules.py` 已在 main
使用 `executions._task_queue_name()`（B3 r54），`TASK_QUEUE_NAME` 仅作默认值。
同名问题残留在服务端 `ScheduleService`：`__init__` 固化
`PLAITA_QUEUE_NAME`，完全忽略 console 的 `PLAITA_CONSOLE_TASK_QUEUE`——同一
部署里「立即触发」（v2）与 cron（默认/worker env）会落到不同 Stream。

**修法**：新增 `resolve_dispatch_queue_name()`，在**每次派发**时解析：
`service_config["queue_name"]` > `PLAITA_CONSOLE_TASK_QUEUE` >
`PLAITA_QUEUE_NAME` > `QUEUE_NAME` > 默认值。注册元数据同样刷新；两侧 env
同时存在且不同时打一次 WARNING（消息可能入无人消费队列）。

```text
pre-fix:  queue_used ['plaita:flow:queue']
post-fix: queue_used ['plaita:flow:queue:v2']
```

**负控**：只回退 `plaita/server/services/schedule_service.py` → 新测试
`tests/unit/test_issue_a56_02_schedule_queue_dynamic.py` 收集期 `ImportError`；
行为面用 pre/post 探针钉住（默认 → v2）。恢复后 8 passed。

**日志守卫**：`test_issue151_log_hygiene` 腿 B 命中 WARNING 直接插值队列名。
队列 **Stream 键**属判定表 A 类「运行实体标识」（跨日志可回查、非凭据值），
按该守卫既有的「显式豁免并登记理由」路径在 `_EXEMPT_ENV_NAMES` 登记两侧
`console_queue`/`worker_queue`——掩掉键名会让分裂队列告警失去可操作性。

## 4. 测试与门禁

- plaita-nodes 定向 6 文件 **73 passed**（9.51s）；全量 **383 passed / 2 skipped**（55.36s）。
- plaita 定向 3 文件 **33 passed**（0.94s）；`tests/unit` 全量 **8426 passed / 12 skipped /
  3498 subtests（rc=0，736.40s）**。
- `landing_gate.py ci` **rc=0**（main `d64ff67` CI success）。
- ruff：新增/改动测试文件全绿；生产文件仅既有 BLE001（改动行无新增）。
