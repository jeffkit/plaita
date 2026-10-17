# RED-PROOF — #195 redis_queue/kafka 事件带 correlation_id

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `574e9f3` · 单 commit
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| redis_queue 普通/错误事件 | `correlation_id`: `None/None → e1/e1`（data.execution_id=e1） |
| kafka_queue 普通/错误事件 | `correlation_id`: `None/None → e1/e1` |
| EventFilter 端到端 | 修复前 `plaita:flow:queue` **0 条**；修复后 1 条 `type=resume, resume_type=event, execution_id=e1` |
| EventFilter 守卫 | 无 correlation_id 的 raw Event 仍被丢弃（守卫未改） |
| 直发频道/落盘 | 有 `redis_client` 时 `plaita:events:{type}` 带 correlation_id，并写入 `plaita:event:events:*` |
| 负控 | 两服务恢复基类构造 ⇒ 新文件 **5 failed / 1 passed**（守卫对照绿） |
| 验收面 | unit 快车道 **7746 passed / 3429 subtests**；e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；`ci-gate.sh` **PASSED**（8046 passed / 覆盖率 93.83%）；`landing_gate.py ci` **success** |

## §1 缺陷与规格

`EventFilter.handle_event` 第一件事是无 `correlation_id` 直接返回
（`plaita/server/event_filter.py:115-118`，正确防线）。redis_queue / kafka
两个服务的**所有**事件（普通消息 + 错误）都走基类 `trigger_event`
（`plaita/server/services/base_service.py:316`）：
`Event(event_type=event_type, data=event_data)`——`correlation_id` 缺省 None，
data 里明明有 `execution_id`。事件到达 EventFilter 即被静默丢弃；等待
`redis_message` / `kafka_message` 的 EVENT 节点永不 resume。

提报席规格（#195 comment）：两服务普通+错误事件从
`event_data["execution_id"]` 构造 correlation_id，建议走
`publish_resume_event` 同源；单测 fake bus + 端到端 EVENT 挂起 + 队列消息 →
resume 入队；负控移除赋值必红，EventFilter 丢弃守卫保持不变。

## §2 实现

- `RedisQueueService.trigger_event` / `KafkaQueueService.trigger_event`
  override：
  ```python
  async def trigger_event(self, event_type, event_data):
      await self.publish_resume_event(event_type, event_data)
  ```
  `publish_resume_event` 构造
  `Event(..., correlation_id=event_data.get("execution_id"))`；普通/错误事件
  同一入口，全部覆盖。
- 两个服务构造函数新增可选 `redis_client`（service_manager 已有签名探测）：
  - 有客户端：直发 `plaita:events:{type}`（引擎 RedisEventBus 频道）并按
    `plaita:event:events:*` 尽力落盘（EventReconciler 可补偿）；
  - 无客户端：回退 `event_bus.publish`；
  - `enable_registry=False`：队列服务历史行为不变，不因拿到客户端而启用
    registry/control。
- **不改** `EventFilter` 的无 correlation_id 丢弃守卫（它是正确防线）。
- 不改事件 `data` 形状、租户/订阅匹配、消费/重连逻辑。

## §3 修复前 / 修复后

探针：`docs/proofs/data/195-repro.py`。修复前在干净 worktree `574e9f3` 跑
（`PYTHONPATH=/tmp/wt195-before`），修复后在本树跑；机读结果
`docs/proofs/data/195-before-after.json`。

| 观测 | 修复前 | 修复后 |
|---|---|---|
| RedisQueue normal / error `correlation_id` | `None` / `None` | `e1` / `e1` |
| KafkaQueue normal / error `correlation_id` | `None` / `None` | `e1` / `e1` |
| EventFilter `plaita:flow:queue` | `[]` | 1 条 `type=resume, resume_type=event, execution_id=e1, flow_id=f1` |
| raw Event（无 correlation_id）经 EventFilter | `[]` | `[]`（守卫不变） |
| 直发频道（单测） | n/a | `plaita:events:redis_message` 带 `correlation_id=e1` + 落盘键 |

修复后的 resume 任务（节选）：

```json
{"type": "resume", "flow_id": "f1", "execution_id": "e1",
 "resume_type": "event", "tenant_id": "default",
 "data": {"event_type": "redis_message", "event_data": {"execution_id": "e1", ...}}}
```

## §4 验收

### §4.1 定向

```
pytest tests/unit/test_issue195_queue_event_correlation.py -q  ⇒ 6 passed
pytest tests/unit/test_approval_callback_event_correlation.py \
       tests/unit/test_resume_event_persistence.py \
       tests/unit/test_service_manager_redis_wiring.py \
       tests/unit/test_clean_c4_delay_service.py \
       tests/unit/test_issue195_queue_event_correlation.py -q  ⇒ 27 passed
```

覆盖：两服务普通/错误 correlation_id、直发频道 + 落盘、EventFilter resume
入队、无 correlation_id 守卫对照；delay/approval/http_callback 既有
correlation 用例与 service_manager 接线全绿。

### §4.2 unit 快车道

```
pytest tests/unit -m "not slow" -q ⇒ 7746 passed, 11 skipped, 13 deselected,
3429 subtests passed
```

### §4.3 e2e / integration

```
pytest tests/e2e -q          ⇒ 74 passed, 3 skipped
pytest tests/integration -q  ⇒ 213 passed, 1 skipped
```

### §4.4 本地门禁与 landing gate

- `ci-gate.sh`：**PASSED**。`pytest tests/` ⇒ **8046 passed / 15 skipped /
  3429 subtests**（120 warnings），覆盖率 **93.83%**（阈值 80%）；import
  layering 3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足。
- `landing_gate.py ci`：**success**——`origin/main` 最近完成 run `574e9f3`
  conclusion=success，放行。

## §5 负控（变异纪律）

在 `86b73cb` 的临时 worktree 中把两服务 override 恢复为基类构造：

```diff
-        await self.publish_resume_event(event_type, event_data)
+        await super().trigger_event(event_type, event_data)
```

结果：

```
tests/unit/test_issue195_queue_event_correlation.py ⇒ 5 failed, 1 passed
```

- **红**：两服务普通/错误 correlation_id（None 而非 e1）、两服务直发频道
  （基类不直发，频道无消息/无落盘）、EventFilter 端到端（queue 0 条）；
- **绿**：`test_event_without_correlation_id_is_still_dropped`——EventFilter
  的守卫在修复前也应按预期丢弃；它证明修的是服务发布侧，不是放松入口。

变异 diff 与红名单：`docs/proofs/data/195-negative-control.diff` /
`195-negative-control.txt`；SHA256 见 `195-before-after.json`。

## §6 关系与边界

- **#35/#56**：delay / approval / http_callback 的 `publish_resume_event`
  同源修法保持不变；本单补齐 redis_queue / kafka 两条同族残面。
- **#87/#153**：去重键、租户/订阅匹配、EventFilter 入口守卫均未改。
- **#198**：schedule CAS 独立规格单，未触碰。
- 不改 redis_queue/kafka 的消费/重连逻辑；只动事件发布面。

## §7 复跑

```bash
# 修复前/后探针
PYTHONPATH=/tmp/wt195-before .venv/bin/python docs/proofs/data/195-repro.py
PYTHONPATH=.                .venv/bin/python docs/proofs/data/195-repro.py

# 定向
.venv/bin/python -m pytest tests/unit/test_issue195_queue_event_correlation.py -q
.venv/bin/python -m pytest tests/unit/test_approval_callback_event_correlation.py \
    tests/unit/test_resume_event_persistence.py \
    tests/unit/test_service_manager_redis_wiring.py \
    tests/unit/test_clean_c4_delay_service.py -q

# 验收面
.venv/bin/python -m pytest tests/unit -m "not slow" -q
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita/server/services/redis_queue_service.py`：`trigger_event` override、
  `redis_client` 构造参数；
- `plaita/server/services/kafka_queue_service.py`：同上；
- `tests/unit/test_issue195_queue_event_correlation.py`：6 条性质/直发/端到端/
  守卫测试；
- `docs/proofs/data/195-repro.py` / `195-before-after.json` /
  `195-negative-control.{diff,txt}`：机读旁证。
