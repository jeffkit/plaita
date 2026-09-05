# 队列触发

流程跑到某节点后挂起，等 Redis/Kafka 队列收到消息再继续。典型用途：下单后等支付消息到达再发货。需 `server` extra（`redis_queue`）或 `redis` extra（redis）。

## 流程定义

```json
{
    "flow_id": "order_fulfill",
    "inputType": { "dataType": "object" },
    "nodes": [
        { "type": "start", "id": "start", "next": "wait_pay" },
        {
            "type": "redis_queue",
            "id": "wait_pay",
            "event_type": "redis_message",
            "queue_name": "payment_notifications",
            "next": "decide"
        },
        {
            "type": "if",
            "id": "decide",
            "condition": { "field": "$NODE.wait_pay.event_data.paid", "operator": "eq", "value": true },
            "next": "ship",
            "else_next": "cancel"
        },
        { "type": "end", "id": "ship", "output": "已发货", "resultType": "success" },
        { "type": "end", "id": "cancel", "output": "已取消", "resultType": "success" }
    ]
}
```

!!! note "队列节点的事件类型是固定的"

    `redis_queue` / `kafka_queue` 节点继承自事件节点，`event_type` 为**必填字段**，但节点会在构造时把它**固定**为 `redis_message` / `kafka_message`——外延服务（`RedisQueueService` 等）把队列消息包装成对应类型的 `Event` 发布。业务数据放在事件 `data` 里，恢复后经 `$NODE.<id>.event_data` 引用。

## 提交订单并挂起

```python
from plaita import FlowExecution
from plaita.event.memory import InMemoryEventBus

flow = Flow.from_string(open("order_fulfill.json").read())
bus = InMemoryEventBus()
execution = FlowExecution(event_bus=bus)

step = execution.run_distributed(flow, {"order_id": "o-1"})
assert step["is_suspend"] is True
exec_id = step["execution_id"]
save_context(exec_id, step["context"])
```

## 队列消息到达触发恢复

生产中 `RedisQueueService` 阻塞监听 `payment_notifications` 队列，消息到达后包装成 `redis_message` 事件 `publish`。演示里直接 publish：

```python
import asyncio
from plaita.event.core import Event

async def pay(exec_id):
    await bus.publish(Event(
        event_type="redis_message",
        data={"paid": True, "order_id": "o-1"},
        correlation_id=exec_id,
        source="payment_system",
    ))

asyncio.run(pay(exec_id))
```

## 恢复

`resume_type="event"` 只消费事件并恢复挂起节点本身，后续节点用 `resume_type="continue"` 推进：

```python
ctx = load_context(exec_id)
step = execution.run_distributed(
    flow, None, saved_context=ctx,
    resume_type="event",
    resume_data={"paid": True, "order_id": "o-1"},
)
while not step["is_end"]:
    step = execution.run_distributed(
        flow, None, saved_context=step["context"], resume_type="continue",
    )
print(step["result"])  # => "已发货"
```

## Kafka 队列

把节点 `type` 换成 `kafka_queue`，配 `bootstrap_servers` / `topic` / `group_id` 等（`event_type` 会被固定为 `kafka_message`），配套 `KafkaQueueService` 监听 Kafka topic。流程侧写法不变。

```json
{
    "type": "kafka_queue",
    "id": "wait_event",
    "event_type": "kafka_message",
    "bootstrap_servers": "localhost:9092",
    "topic": "user_events",
    "group_id": "flow_consumer",
    "next": "end"
}
```

## 生产闭环

```mermaid
flowchart LR
    App["下单"] --> FW["FlowWorker"]
    FW -->|"挂起 wait_pay"| Bus[("EventBus")]
    QSvc["RedisQueueService"] -->|监听队列| Q[("payment_notifications")]
    Q --> QSvc
    QSvc -->|publish payment_received| Bus
    Bus -->|恢复| FW
    FW -->|"if paid -> ship"| Done["已发货"]
```

`ServiceManager` 启动 `RedisQueueService`，把队列消息转成事件；`FlowWorker` 监听事件并恢复对应挂起流程。

## 要点回顾

- `redis_queue` / `kafka_queue` 节点声明等待，外延服务负责触发
- `correlation_id=execution_id` 保证消息路由到正确的挂起流程
- 流程侧与触发侧通过 `EventBus` 解耦，可多实例负载均衡

## 下一步

- [生成器调试器](debug-with-generator.md)
- [扩展节点](../distributed/extended-nodes.md)
