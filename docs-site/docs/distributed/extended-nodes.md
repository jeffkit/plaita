# 扩展节点

扩展节点（`plaita.server.nodes`）是基于 `EventNode` 的预置节点，覆盖四类常见长时等待场景。它们经 `plaita.nodes` entry_points **随包自动注册**（无需额外安装即可解析；实际执行需相应基础设施，通常配合 `server` extra 的外延服务）。

!!! note "字段名与事件类型"

    扩展节点字段均为**小写下划线**形式（无驼峰归一化）；字段值不支持 `{{ }}` 模板插值，支持 `$` 前缀表达式。除 `delay` 外，其余扩展节点会在构造时把 `event_type` **固定**为约定值——外延服务按该类型发布事件。

## 总览

| type | 类 | 触发事件（固定） | 用途 |
|------|------|------|------|
| `delay` | `DelayNode` | `delay_trigger`（默认，可改） | 延迟指定时间后触发 |
| `redis_queue` | `RedisQueueNode` | `redis_message` | 等待 Redis 队列消息 |
| `kafka_queue` | `KafkaQueueNode` | `kafka_message` | 等待 Kafka 队列消息 |
| `http_callback` | `HttpCallbackNode` | `http_callback` | 等待外部 HTTP 回调 |
| `approval` | `ApprovalNode` | `approval_decision` | 等待人工审批决策 |

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
    "delay_seconds": 5,
    "delay_unit": "minutes"
}
```

| 字段 | 说明 |
|------|------|
| `delay_seconds` | 延迟数值（必填），支持 `$` 表达式 |
| `delay_unit` | `seconds`（默认）/ `minutes` / `hours` / `days` |
| `event_type` | 触发事件类型，默认 `delay_trigger`（此节点可自定义） |

`generate_service_config` 产出 `{type, delay_ms, trigger_timestamp, node_id, execution_id, flow_id, event_type, event_filter, retry_config}`。

## redis_queue / kafka_queue

等待队列消息到达后恢复。配置队列地址与消费参数，对应 `RedisQueueService` / `KafkaQueueService` 监听队列、把消息包装成事件 `publish`。

```json
{
    "type": "redis_queue",
    "id": "wait_msg",
    "event_type": "redis_message",
    "queue_name": "orders"
}
```

`redis_queue` 必填 `queue_name`（另有 `redis_host` / `redis_port` / `queue_type` 等可选字段）；`event_type` 会被固定为 `redis_message`。`kafka_queue` 必填 `bootstrap_servers` / `topic` / `group_id`，`event_type` 固定为 `kafka_message`。

## http_callback

等待外部系统回调一个 URL，回调到达后恢复。`HttpCallbackService` 暴露 HTTP 端点接收回调，转成事件。

```json
{
    "type": "http_callback",
    "id": "wait_cb",
    "callback_path": "/callback/order/{request_id}",
    "callback_method": "POST"
}
```

主要字段：`callback_path`（为空自动生成）/ `callback_method`（默认 `POST`）/ `callback_timeout_minutes`（默认 60）及认证相关 `require_auth` / `auth_type` / `auth_token` 等；`event_type` 固定为 `http_callback`。

## approval

人工审批节点，发起审批并等待决策。`ApprovalService` 通知审批人、接收决策、发布 `approval_decision` 事件恢复流程。

```json
{
    "type": "approval",
    "id": "manager_approve",
    "event_type": "approval_decision",
    "approval_title": "请假申请",
    "approval_content": "{% $INPUT.reason %}",
    "approval_type": "manual",
    "approvers": ["alice", "bob"],
    "approval_strategy": "any",
    "auto_escalation": false,
    "form_fields": [],
    "allow_comments": true
}
```

| 字段 | 说明 |
|------|------|
| `approval_title` / `approval_content` | 标题 / 内容（均必填，支持 `$` 表达式） |
| `event_type` | 必填（继承自事件节点）；构造后固定为 `approval_decision` |
| `approval_type` | `manual`（默认）/ `auto` |
| `approvers` | 审批人列表（用户 id 或邮箱，支持表达式） |
| `approval_strategy` | `any`（默认，任一）/ `all`（全部）/ `majority`（多数） |
| `auto_escalation` / `escalation_timeout_hours` / `escalation_approvers` | 自动升级配置 |
| `form_fields` | 审批表单字段 |
| `allow_comments` / `require_comments` | 审批意见开关 |

`ApprovalNode` 恢复事件固定为 `approval_decision`；`generate_service_config` 产出审批实例 id、审批人、表单、通知配置等供 `ApprovalService` 使用。详见 [审批流场景](../scenarios/approval-flow.md)。

## 在流程中使用

扩展节点通过 entry_points 自动注册，`pip install plaita[server]` 后直接在 JSON 里用即可。在 Distributed 模式下执行到这类节点会自动挂起：

```python
from plaita import Flow, FlowExecution
from plaita.event.memory import InMemoryEventBus

flow = Flow.from_string(open("approval_flow.json").read())
execution = FlowExecution(event_bus=InMemoryEventBus())
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
