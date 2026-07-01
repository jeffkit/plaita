# 扩展节点

扩展节点（`plaita.server.nodes`，需 `server` extra）是基于 `EventNode` 的预置节点，覆盖四类常见长时等待场景。它们通过 `plaita.nodes` entry_points 自动注册。

## 总览

| type | 类 | 用途 |
|------|------|------|
| `delay` | `DelayNode` | 延迟指定时间后触发 |
| `redis_queue` | `RedisQueueNode` | 等待 Redis 队列消息 |
| `kafka_queue` | `KafkaQueueNode` | 等待 Kafka 队列消息 |
| `http_callback` | `HttpCallbackNode` | 等待外部 HTTP 回调 |
| `approval` | `ApprovalNode` | 等待人工审批决策 |

## BaseExtendedNode

所有扩展节点继承 `BaseExtendedNode`（继承自 `EventNode`）。它们的 `execute` 在挂起前生成一份 **service_config**，由外延服务消费：

```python
def execute(self, execution):
    service_config = self.generate_service_config(execution)  # 子类实现
    result = super().execute(execution)          # EventNode 挂起逻辑
    result.update({"service_config": service_config, "node_subtype": self.node_type})
    return result
```

`generate_service_config` 是子类必须实现的抽象方法，产出外延服务触发恢复所需的信息（如延迟时长、队列名、审批人等）。默认重试配置：`max_retries=3, retry_delay_ms=1000, exponential_backoff=True`。

## delay

延迟节点，到点后由 `DelayService` 发布 `delay_trigger` 事件恢复流程。

```json
{
    "type": "delay",
    "id": "wait_5m",
    "delaySeconds": 5,
    "delayUnit": "minutes",
    "eventType": "delay_trigger"
}
```

| 字段 | 说明 |
|------|------|
| `delaySeconds` | 延迟数值，支持 `$` 表达式 |
| `delayUnit` | `seconds` / `minutes` / `hours` / `days` |
| `eventType` | 触发事件类型，默认 `delay_trigger` |

`generate_service_config` 产出 `{type, delay_ms, trigger_timestamp, node_id, execution_id, flow_id, event_type, event_filter, retry_config}`。

## redis_queue / kafka_queue

等待队列消息到达后恢复。配置队列地址与消费参数，对应 `RedisQueueService` / `KafkaQueueService` 监听队列、把消息包装成事件 `publish`。

```json
{
    "type": "redis_queue",
    "id": "wait_msg",
    "queueName": "orders",
    "eventType": "redis_queue_message"
}
```

## http_callback

等待外部系统回调一个 URL，回调到达后恢复。`HttpCallbackService` 暴露 HTTP 端点接收回调，转成事件。

```json
{
    "type": "http_callback",
    "id": "wait_cb",
    "path": "/callback/order/{request_id}",
    "method": "POST",
    "eventType": "http_callback_received"
}
```

## approval

人工审批节点，发起审批并等待决策。`ApprovalService` 通知审批人、接收决策、发布 `approval_decision` 事件恢复流程。

```json
{
    "type": "approval",
    "id": "manager_approve",
    "approvalTitle": "请假申请",
    "approvalContent": "{% $INPUT.reason %}",
    "approvalType": "manual",
    "approvers": ["alice", "bob"],
    "approvalStrategy": "any",
    "autoEscalation": false,
    "formFields": [],
    "allowComments": true
}
```

| 字段 | 说明 |
|------|------|
| `approvalTitle` / `approvalContent` | 标题 / 内容（支持 `$` 表达式） |
| `approvalType` | `manual` / `auto` |
| `approvers` | 审批人列表（用户 id 或邮箱，支持表达式） |
| `approvalStrategy` | `any`（任一）/ `all`（全部）/ `majority`（多数） |
| `autoEscalation` / `escalationTimeoutHours` / `escalationApprovers` | 自动升级配置 |
| `formFields` | 审批表单字段 |
| `allowComments` / `requireComments` | 审批意见开关 |

`ApprovalNode` 默认监听 `approval_decision` 事件；`generate_service_config` 产出审批实例 id、审批人、表单、通知配置等供 `ApprovalService` 使用。详见 [审批流场景](../scenarios/approval-flow.md)。

## 在流程中使用

扩展节点通过 entry_points 自动注册，`pip install plaita[server]` 后直接在 JSON 里用即可。在 Distributed 模式下执行到这类节点会自动挂起：

```python
from plaita import Flow, FlowExecution

flow = Flow.from_string(open("approval_flow.json").read())
execution = FlowExecution(event_bus=bus)
step = execution.run_distributed(flow, {"applicant": "alice", "reason": "请假"})
# step["is_suspend"] == True
```

## 自定义扩展节点

继承 `BaseExtendedNode` 实现 `generate_service_config`，并配套一个外延服务（见 [外延服务](services.md)）处理触发：

```python
from plaita.server.nodes.base_extended_node import BaseExtendedNode

class MyWaitNode(BaseExtendedNode):
    node_type = "my_wait"
    node_name = "我的等待"

    def generate_service_config(self, execution):
        return {"type": "my_wait", "node_id": self.id, ...}
```

## 下一步

- [外延服务](services.md) —— 节点挂起后谁来触发恢复
- [审批流场景](../scenarios/approval-flow.md)
- [API: plaita.server.flow_worker](../api/server.md)
