# 幂等 Resume 指南

任务队列是 **at-least-once**：worker 崩溃后同一 `start`/`resume` 可能再投递一次。  
execution **lease** 只防止**并发**双 resume，不防止「持有者死后 TTL 过期 → 再 resume」。

因此：**挂起节点之后的副作用必须可安全重入。**

## 什么时候会重复？

1. Worker 在 `XACK` 前崩溃 → pending 被 `XCLAIM` → 再执行。
2. 处理抛错未 ack，达到 `max_deliveries` 前多次重试。
3. 人工从 DLQ 回灌。

## 推荐模式

### 1. 用 execution_id + 业务键做去重

```python
# 伪代码：审批回调副作用
def apply_approval(execution_id: str, decision_id: str, approved: bool):
    # Redis SET NX：同一决策只落地一次
    key = f"plaita:side_effect:approval:{execution_id}:{decision_id}"
    if not redis.set(key, "1", nx=True, ex=86400 * 7):
        return  # 已处理过
    write_to_db(...)
```

把「是否已处理」绑在 **稳定业务 id**（订单号、审批单号、`event_id`），不要只靠「跑到了哪一步」。

### 2. EventNode / 扩展节点保持无状态

节点只读写 `execution` 上下文；外部 IO 放在可幂等的服务里。`EventFilter` 已用  
`plaita:event_filter:dedup:{event_id}:{subscription_id}` 做入队去重——**入队去重 ≠ 副作用去重**。

### 3. resume 前检查状态机

```python
state = storage.load_execution_state(execution_id)
if state.status in ("completed", "error"):
    return  # 幂等：已终态则忽略重复 resume
if state.status != "suspended":
    log.warning("unexpected status=%s", state.status)
```

`FlowWorker.resume_flow` 本身会加载状态；自定义入口应同样校验。

### 4. 外部写操作使用幂等 API

HTTP 回调带 `Idempotency-Key: {execution_id}:{node_id}:{event_id}`；  
DB upsert 用唯一约束 `(execution_id, step_key)`。

## 反例

- 「每次 resume 都发一封邮件」且无去重表  
- 依赖「恰好执行一次」扣款  
- 用递增计数器当业务主键却允许重跑

## 最小可运行示例

见仓库 `examples/server_demo/idempotent_resume_demo.py`：用内存存储演示  
「同一 resume 数据执行两次，副作用只生效一次」。

## 相关

- [运维 Runbook](ops-runbook.md)
- [FlowWorker 可靠性边界](flow-worker.md#可靠性边界必读)
- [事件系统匹配语义](event-system.md#eventsubscription-与匹配)
