# 扩展节点和外延服务框架

基于Plaita流程引擎EventNode的扩展节点框架，提供延迟节点、消息队列节点、HTTP回调节点、审批节点等多种类型的异步事件节点，以及相应的外延服务支持。

## 框架概述

### 架构设计

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Flow Engine   │    │  Extended Nodes │    │  External Svcs  │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │ EventNode │◄─┼────┼─►│DelayNode  │  │    │  │DelayService│ │
│  └───────────┘  │    │  ├───────────┤  │    │  ├───────────┤  │
│  ┌───────────┐  │    │  │QueueNode  │  │    │  │QueueService│ │
│  │EventExec  │◄─┼────┼─►├───────────┤  │    │  ├───────────┤  │
│  └───────────┘  │    │  │Callback   │  │    │  │CallbackSvc│ │
│                 │    │  ├───────────┤  │    │  ├───────────┤  │
│                 │    │  │Approval   │  │    │  │ApprovalSvc│ │
│                 │    │  └───────────┘  │    │  └───────────┘  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────┐
                    │   Event Bus     │
                    └─────────────────┘
```

### 核心组件

1. **扩展节点 (Extended Nodes)**
   - `BaseExtendedNode`: 扩展节点基础类
   - `DelayNode`: 延迟节点
   - `RedisQueueNode`: Redis队列节点
   - `KafkaQueueNode`: Kafka队列节点
   - `HttpCallbackNode`: HTTP回调节点
   - `ApprovalNode`: 审批节点

2. **外延服务 (External Services)**
   - `BaseExtendedService`: 服务基础类
   - `DelayService`: 延迟服务
   - `RedisQueueService`: Redis队列服务
   - `KafkaQueueService`: Kafka队列服务
   - `HttpCallbackService`: HTTP回调服务
   - `ApprovalService`: 审批服务

3. **服务管理器 (Service Manager)**
   - `ServiceManager`: 统一管理所有外延服务的生命周期

## 扩展节点

### 1. 延迟节点 (DelayNode)

在指定时间后触发事件继续流程执行。

```python
from plaita.server.nodes import DelayNode

# 创建延迟节点
delay_node = DelayNode(
    id="delay_5min",
    delay_seconds=300,
    delay_unit="seconds"
)
```

**配置参数:**
- `delay_seconds`: 延迟时间（支持变量引用）
- `delay_unit`: 时间单位 (seconds, minutes, hours, days)

### 2. Redis队列节点 (RedisQueueNode)

监听Redis队列消息并触发流程继续执行。

```python
from plaita.server.nodes import RedisQueueNode

# 创建Redis队列节点
redis_node = RedisQueueNode(
    id="redis_listener",
    redis_host="localhost",
    redis_port=6379,
    queue_name="task_queue",
    queue_type="list"
)
```

**配置参数:**
- `redis_host/port/db/password`: Redis连接配置
- `queue_name`: 队列名称
- `queue_type`: 队列类型 (list, stream, pubsub)
- `timeout_seconds`: 监听超时时间
- `message_format`: 消息格式 (json, text, raw)

### 3. Kafka队列节点 (KafkaQueueNode)

监听Kafka主题消息并触发流程继续执行。

```python
from plaita.server.nodes import KafkaQueueNode

# 创建Kafka队列节点
kafka_node = KafkaQueueNode(
    id="kafka_listener",
    bootstrap_servers=["localhost:9092"],
    topic="order_events",
    group_id="order_processor"
)
```

**配置参数:**
- `bootstrap_servers`: Kafka服务器地址
- `topic`: 主题名称
- `group_id`: 消费者组ID
- `security_protocol`: 安全协议
- `message_format`: 消息格式 (json, text, avro)

### 4. HTTP回调节点 (HttpCallbackNode)

等待HTTP回调请求并触发流程继续执行。

```python
from plaita.server.nodes import HttpCallbackNode

# 创建HTTP回调节点
callback_node = HttpCallbackNode(
    id="payment_callback",
    callback_method="POST",
    callback_timeout_minutes=30,
    require_auth=True,
    auth_type="token"
)
```

**配置参数:**
- `callback_path`: 回调路径（可自动生成）
- `callback_method`: HTTP方法
- `callback_timeout_minutes`: 回调超时时间
- `require_auth`: 是否需要认证
- `auth_type`: 认证类型 (token, basic, signature)

### 5. 审批节点 (ApprovalNode)

发起人工审批流程并等待审批决策。

```python
from plaita.server.nodes import ApprovalNode

# 创建审批节点
approval_node = ApprovalNode(
    id="resource_approval",
    approval_title="资源申请审批",
    approval_content="请审批CPU 4核、内存 8GB的资源申请",
    approvers=["manager1", "manager2"],
    approval_strategy="any"
)
```

**配置参数:**
- `approval_title/content`: 审批标题和内容
- `approvers`: 审批人列表
- `approval_strategy`: 审批策略 (any, all, majority)
- `auto_escalation`: 是否自动升级
- `form_fields`: 审批表单字段

## 外延服务

### 服务管理器使用

```python
from plaita.server.services import ServiceManager
from plaita.event.core import EventBus

# 创建事件总线和服务管理器
event_bus = EventBus()
service_manager = ServiceManager(event_bus)

# 启动所有服务
service_configs = {
    "delay": {"max_workers": 10},
    "redis_queue": {"max_workers": 5},
    "approval": {"max_workers": 3}
}

await event_bus.start()
service_manager.start_all_services(service_configs)

# 处理节点配置
task_id = service_manager.handle_node_config(node_service_config)

# 获取服务状态
status = service_manager.get_all_services_status()

# 停止所有服务
service_manager.stop_all_services()
await event_bus.stop()
```

### 服务特性

1. **异步处理**: 所有服务都支持异步任务处理
2. **错误重试**: 支持自定义重试策略
3. **资源管理**: 自动管理线程池和连接资源
4. **状态监控**: 提供实时服务状态查询
5. **优雅关闭**: 支持超时等待任务完成

## 流程集成

### 在Flow中使用扩展节点

```python
# 流程定义JSON
flow_definition = {
    "flow_id": "order_process",
    "nodes": [
        {
            "id": "start",
            "type": "start",
            "next": "delay_check"
        },
        {
            "id": "delay_check",
            "type": "delay",
            "delay_seconds": 300,
            "delay_unit": "seconds",
            "next": "approval"
        },
        {
            "id": "approval",
            "type": "approval",
            "approval_title": "订单审批",
            "approval_content": "请审批订单 #12345",
            "approvers": ["supervisor"],
            "approval_strategy": "any",
            "next": "payment_callback"
        },
        {
            "id": "payment_callback",
            "type": "http_callback",
            "callback_method": "POST",
            "callback_timeout_minutes": 10,
            "next": "end"
        },
        {
            "id": "end",
            "type": "end"
        }
    ]
}

# 创建并执行流程
from plaita.flow import Flow, FlowExecution

flow = Flow.model_validate(flow_definition)
execution = FlowExecution(event_bus=event_bus)

# 分布式模式执行（支持挂起和恢复）
result = FlowExecution.run(
    flow, 
    params={"order_id": "12345"},
    mode="distributed",
    event_bus=event_bus
)
```

### 分布式执行模式

扩展节点支持分布式执行模式，可以在不同进程间挂起和恢复流程：

```python
# 首次执行
result1 = FlowExecution.run(
    flow, 
    params={"order_id": "12345"},
    mode="distributed"
)

# 检查是否挂起
if result1.get("is_suspend"):
    context = result1["context"]
    execution_id = result1["execution_id"]
    
    # 稍后恢复执行（可能在另一个进程中）
    result2 = FlowExecution.run(
        flow,
        mode="distributed",
        context=context,
        resume_type="event",
        resume_data={"approval_result": "approved"}
    )
```

## 事件处理

### 事件类型

- `delay_trigger`: 延迟触发事件
- `redis_message`: Redis消息事件
- `kafka_message`: Kafka消息事件
- `http_callback`: HTTP回调事件
- `approval_decision`: 审批决策事件

### 事件数据格式

```python
{
    "node_id": "节点ID",
    "execution_id": "执行ID",
    "flow_id": "流程ID",
    "trigger_type": "触发类型",
    "timestamp": 1234567890,
    "success": True,
    # 节点特定数据...
}
```

## 运行演示

```bash
# 进入项目目录
cd plaita/runtime/python

# 运行演示
python -m examples.server_demo.extended_nodes_demo
```

演示将展示：
1. 延迟节点的创建和执行
2. 审批节点的工作流程
3. HTTP回调节点的注册和触发
4. Kafka队列节点的模拟消息处理
5. 服务管理器的状态监控

## 扩展开发

### 添加新的扩展节点

1. 继承 `BaseExtendedNode`
2. 实现 `generate_service_config` 方法
3. 定义节点特有的配置参数

```python
from plaita.server.nodes.base_extended_node import BaseExtendedNode

class CustomNode(BaseExtendedNode):
    node_type: ClassVar[str] = "custom"
    node_name: ClassVar[str] = "自定义节点"
    
    # 自定义配置参数
    custom_param: str = Field(description="自定义参数")
    
    def generate_service_config(self, execution) -> Dict[str, Any]:
        return {
            "type": "custom",
            "node_id": self.id,
            "custom_param": self.custom_param,
            # 其他配置...
        }
```

### 添加新的外延服务

1. 继承 `BaseExtendedService`
2. 实现必要的抽象方法
3. 注册到服务管理器

```python
from plaita.server.services.base_service import BaseExtendedService

class CustomService(BaseExtendedService):
    def get_service_type(self) -> str:
        return "custom"
    
    def start_service(self) -> bool:
        # 服务启动逻辑
        return True
    
    def stop_service(self) -> bool:
        # 服务停止逻辑
        return True
    
    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        # 任务处理逻辑
        return True

# 注册到服务管理器
service_manager.register_service_class("custom", CustomService)
```

## 依赖要求

- `redis`: Redis队列节点需要
- `confluent-kafka`: Kafka队列节点需要
- `aiohttp`: HTTP回调服务需要（可选）

```bash
pip install redis confluent-kafka aiohttp
```

## 注意事项

1. **事件总线**: 所有扩展节点都依赖事件总线进行通信
2. **服务生命周期**: 确保在流程执行前启动服务管理器
3. **资源清理**: 及时清理不再使用的服务和资源
4. **错误处理**: 实现适当的错误处理和重试机制
5. **监控日志**: 关注服务状态和错误日志

## 许可证

本框架遵循Plaita项目的许可证协议。 