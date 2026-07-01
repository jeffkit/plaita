# 断点续执（Checkpoint）架构设计文档

本文档详细介绍 `plaita` 的断点续执（Checkpoint）功能架构设计，这是支持长时间运行工作流的核心特性。

## 1. 概述

### 1.1 设计背景

传统的流程引擎采用同步阻塞执行模式，无法支持需要长时间等待的工作流场景。例如：

- **外部 HTTP 回调**：等待第三方系统的异步通知
- **人工审批流程**：等待人工干预和决策
- **消息队列触发**：等待 Redis/Kafka 消息到达
- **定时延迟执行**：在指定时间后继续执行

这些场景要求流程能够在某个节点**挂起**执行状态、**持久化**上下文数据，并在外部事件到达后**恢复**执行。

### 1.2 设计目标

1. **状态持久化**：完整保存流程执行上下文，支持跨进程恢复
2. **事件驱动恢复**：基于事件机制触发流程恢复
3. **可扩展架构**：支持多种外延服务（延迟、队列、回调、审批等）
4. **分布式支持**：支持多实例部署和负载均衡

### 1.3 核心概念

| 概念 | 说明 |
|------|------|
| **Checkpoint** | 流程执行的断点，包含完整的执行上下文 |
| **挂起 (Suspend)** | 流程在事件节点处暂停执行，保存状态 |
| **恢复 (Resume)** | 接收到外部事件后，从断点继续执行 |
| **事件节点 (EventNode)** | 可触发流程挂起的特殊节点类型 |
| **外延服务 (Extended Service)** | 处理外部事件的后台服务组件 |

## 2. 整体架构

### 2.1 架构总览

![断点续执整体架构](images/checkpoint-architecture-overview.svg)

断点续执架构由四个核心层次组成：

```
┌─────────────────────────────────────────────────────────────────┐
│                      流程执行层 (Flow Execution)                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    FlowExecution                         │    │
│  │  • run_distributed() - 分布式执行入口                     │    │
│  │  • _handle_resume_operation() - 恢复操作处理              │    │
│  │  • _subscribe_event() - 事件订阅                         │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      事件系统层 (Event System)                    │
│  ┌──────────────┐  ┌──────────────────┐  ┌─────────────────┐   │
│  │   EventBus   │  │  EventStorage    │  │ Subscription    │   │
│  │  • publish   │  │  • store_event   │  │   Storage       │   │
│  │  • subscribe │  │  • get_event     │  │                 │   │
│  └──────────────┘  └──────────────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      状态存储层 (State Storage)                   │
│  ┌──────────────────┐  ┌────────────────┐  ┌────────────────┐  │
│  │ ExecutionStorage │  │  FlowStorage   │  │  Context Data  │  │
│  │  • save_state    │  │  • get_flow    │  │  (序列化上下文)  │  │
│  │  • load_state    │  │  • save_flow   │  │                │  │
│  └──────────────────┘  └────────────────┘  └────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    外延服务层 (Extended Services)                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐    │
│  │  Delay   │  │  Redis   │  │  Kafka   │  │    HTTP      │    │
│  │ Service  │  │  Queue   │  │  Queue   │  │  Callback    │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 组件关系图

![组件关系图](images/checkpoint-component-relationship.svg)

## 3. 核心组件设计

### 3.1 FlowExecution（流程执行引擎）

`FlowExecution` 是流程执行的核心引擎，在断点续执场景下提供以下关键功能：

#### 3.1.1 分布式执行模式

```python
def _run_distributed(
    self,
    flow: Flow,
    params: Optional[Dict] = None,
    timeout: Optional[int] = None,
    context: Optional[Dict] = None,
    resume_type: str = "continue",
    resume_data: Optional[Dict] = None,
    **options,
) -> Dict[str, Any]:
    """
    分布式执行模式
    
    关键逻辑：
    1. 如果 context 为空，从头开始执行
    2. 执行到事件节点时，保存上下文并挂起
    3. 如果 context 存在，从上次挂起点恢复执行
    """
```

**执行状态输出格式**：

```python
{
    "id": "node_id",           # 当前节点ID
    "type": "event",           # 节点类型
    "name": "等待审批",         # 节点名称
    "result": {...},           # 节点执行结果
    "branch": "",              # 分支名称
    "context": {...},          # 完整执行上下文
    "is_end": False,           # 是否结束
    "is_suspend": True,        # 是否挂起
    "execution_id": "xxx"      # 执行ID
}
```

#### 3.1.2 恢复操作处理

```python
def _handle_resume_operation(
    self, 
    flow: Flow, 
    resume_type: str, 
    resume_data: Optional[Dict] = None
) -> Dict:
    """
    处理恢复操作
    
    支持的恢复类型：
    - continue: 继续执行下一个节点
    - cancel: 取消当前等待
    - timeout: 超时处理
    - event: 收到外部事件
    """
```

#### 3.1.3 事件订阅机制

当流程执行到 `EventNode` 时，`FlowExecution` 负责向事件总线注册订阅：

```python
def _subscribe_event(self, node, flow, node_state):
    """
    为 EventNode 节点订阅事件
    
    订阅参数：
    - event_type: 事件类型
    - filter_condition: 过滤条件
    - correlation_id: 关联ID（执行ID）
    - flow_id: 流程ID
    - node_id: 节点ID
    """
```

### 3.2 EventNode（事件节点）

`EventNode` 是支持挂起/恢复的特殊节点类型，是断点续执的核心节点基类。

#### 3.2.1 状态枚举

```python
class EventNodeStatus(Enum):
    PENDING = "pending"       # 等待事件
    COMPLETED = "completed"   # 事件正常完成
    ERROR = "error"           # 处理出错
    TIMEOUT = "timeout"       # 等待超时
    CANCELLED = "cancelled"   # 监听取消
```

#### 3.2.2 核心接口

| 方法 | 说明 |
|------|------|
| `execute(execution)` | 执行节点，返回事件监听信息 |
| `on_event(execution, event_data)` | 事件到达时的处理逻辑 |
| `on_timeout(execution)` | 超时处理 |
| `on_cancel(execution)` | 取消处理 |
| `on_error(execution, error_message)` | 错误处理 |

#### 3.2.3 执行结果格式

```python
{
    "event_type": "approval.completed",
    "event_filter": {"order_id": "12345"},
    "event_id": "event_node1_1703945678000",
    "status": "pending",
    "is_async": True
}
```

### 3.3 事件系统（Event System）

事件系统是实现流程恢复的核心基础设施，采用发布/订阅模式。

#### 3.3.1 核心接口

```python
class EventBus(ABC):
    """事件总线接口"""
    
    async def publish(self, event: Event) -> str:
        """发布事件"""
        pass
    
    async def register_subscription(
        self,
        event_type: str,
        filter_condition: Optional[Dict] = None,
        correlation_id: Optional[str] = None,
        flow_id: Optional[str] = None,
        node_id: Optional[str] = None
    ) -> str:
        """注册事件订阅"""
        pass
    
    async def register_handler(
        self,
        event_type: Optional[str] = None,
        handler: EventHandler = None,
        retry_policy: Optional[RetryPolicy] = None
    ) -> str:
        """注册事件处理器"""
        pass
```

#### 3.3.2 事件匹配机制

事件匹配支持多种模式：

```python
# 1. 精确匹配
"user.login" -> 只匹配 "user.login"

# 2. 前缀通配符
"user.*" -> 匹配 "user.login", "user.logout" 等

# 3. 后缀通配符
"*.login" -> 匹配 "user.login", "admin.login" 等

# 4. 全局匹配
"*" 或 None -> 匹配所有事件
```

#### 3.3.3 多后端实现

| 实现 | 适用场景 | 特点 |
|------|----------|------|
| `InMemoryEventBus` | 单机/测试 | 简单、高性能、无持久化 |
| `RedisEventBus` | 分布式 | 支持多实例、PubSub机制 |
| `SqlalchemyEventBus` | 强持久化 | 事务支持、复杂查询 |

### 3.4 状态存储（State Storage）

状态存储负责持久化流程执行状态，是断点恢复的数据基础。

#### 3.4.1 执行状态模型

```python
class ExecutionState(BaseModel):
    execution_id: str           # 执行ID，唯一标识
    flow_id: Optional[str]      # 流程ID
    flow_name: Optional[str]    # 流程名称
    flow_version: Optional[str] # 流程版本
    context: Dict[str, Any]     # 执行上下文
    status: str                 # 状态: running/suspended/completed/error
    start_time: Optional[str]   # 开始时间
    last_update_time: Optional[str]  # 最后更新时间
    end_time: Optional[str]     # 结束时间
    error: Optional[Dict]       # 错误信息
    invoker: Optional[str]      # 调用者
```

#### 3.4.2 存储接口

```python
class ExecutionStorage(ABC):
    def save_execution_state(self, execution_id: str, state: ExecutionState) -> bool:
        """保存执行状态"""
        pass
    
    def load_execution_state(self, execution_id: str) -> Optional[ExecutionState]:
        """加载执行状态"""
        pass
    
    def delete_execution_state(self, execution_id: str) -> bool:
        """删除执行状态"""
        pass
    
    def list_executions(self, query=None, order_by=None, limit=100, offset=0) -> List[ExecutionState]:
        """列出执行状态"""
        pass
```

### 3.5 FlowWorker（流程工作器）

`FlowWorker` 是后台工作器，负责监听事件并恢复挂起的流程。

#### 3.5.1 核心功能

```python
class FlowWorker:
    """流程工作器"""
    
    def start_flow(self, flow_id: str, params: Dict, version: str = None) -> Dict:
        """启动流程执行"""
        pass
    
    def resume_flow(
        self, 
        flow_id: str, 
        execution_id: str, 
        resume_type: str, 
        data: Dict = None
    ) -> Dict:
        """恢复流程执行"""
        pass
    
    def _process_execution_result(self, flow, result, state) -> Dict:
        """处理执行结果，循环执行直到结束或挂起"""
        pass
```

#### 3.5.2 Redis 工作器

```python
class RedisFlowWorker(FlowWorker):
    """基于 Redis 的流程工作器"""
    
    def run(self):
        """
        从 Redis 队列获取任务并处理
        消息类型：
        - start: 启动新流程
        - resume: 恢复挂起流程
        """
        while True:
            message = self.redis_client.blpop(self.queue_name, timeout=10)
            if message:
                self._process_message(message)
```

## 4. 扩展节点与外延服务

### 4.1 扩展节点体系

扩展节点继承自 `EventNode`，为特定场景提供开箱即用的解决方案。

#### 4.1.1 继承关系

```
Node (基础节点)
  └── EventNode (事件节点)
        └── BaseExtendedNode (扩展节点基类)
              ├── DelayNode (延迟节点)
              ├── RedisQueueNode (Redis队列节点)
              ├── KafkaQueueNode (Kafka队列节点)
              ├── HttpCallbackNode (HTTP回调节点)
              └── ApprovalNode (审批节点)
```

#### 4.1.2 BaseExtendedNode

```python
class BaseExtendedNode(EventNode):
    """扩展节点基类"""
    
    def execute(self, execution):
        """执行逻辑"""
        # 生成服务配置
        service_config = self.generate_service_config(execution)
        
        # 调用父类执行
        result = super().execute(execution)
        
        # 添加服务配置
        result.update({
            "service_config": service_config,
            "node_subtype": self.node_type
        })
        
        return result
    
    @abstractmethod
    def generate_service_config(self, execution) -> Dict[str, Any]:
        """生成外延服务配置（子类必须实现）"""
        pass
```

#### 4.1.3 扩展节点清单

| 节点类型 | 说明 | 事件类型示例 |
|----------|------|--------------|
| `DelayNode` | 延迟指定时间后继续 | `delay.completed` |
| `RedisQueueNode` | 等待 Redis 队列消息 | `redis_queue.message` |
| `KafkaQueueNode` | 等待 Kafka 消息 | `kafka.message` |
| `HttpCallbackNode` | 等待 HTTP 回调 | `http.callback` |
| `ApprovalNode` | 等待人工审批 | `approval.decision` |

### 4.2 外延服务体系

外延服务是处理扩展节点任务的后台组件。

#### 4.2.1 服务基类

```python
class BaseExtendedService(ABC):
    """外延服务基类"""
    
    def __init__(self, event_bus: EventBus, service_config: Dict = None):
        self.event_bus = event_bus
        self.service_config = service_config or {}
        self.active_tasks: Set[str] = set()
        self.thread_pool = ThreadPoolExecutor(max_workers=self.get_max_workers())
    
    @abstractmethod
    def get_service_type(self) -> str:
        """获取服务类型"""
        pass
    
    @abstractmethod
    def start_service(self) -> bool:
        """启动服务"""
        pass
    
    @abstractmethod
    def stop_service(self) -> bool:
        """停止服务"""
        pass
    
    @abstractmethod
    async def handle_task(self, task_config: Dict) -> bool:
        """处理任务"""
        pass
    
    async def trigger_event(self, event_type: str, event_data: Dict):
        """触发事件，通知流程恢复"""
        event = Event(event_type=event_type, data=event_data)
        await self.event_bus.publish(event)
```

#### 4.2.2 服务管理器

```python
class ServiceManager:
    """服务管理器，统一管理所有外延服务的生命周期"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.services: Dict[str, BaseExtendedService] = {}
        self.service_classes = {
            "delay": DelayService,
            "redis_queue": RedisQueueService,
            "kafka_queue": KafkaQueueService,
            "http_callback": HttpCallbackService,
            "approval": ApprovalService,
        }
    
    def start_all_services(self, configs: Dict = None) -> bool:
        """启动所有服务"""
        pass
    
    def stop_all_services(self, timeout: float = None) -> bool:
        """停止所有服务"""
        pass
    
    def submit_task(self, service_type: str, task_config: Dict) -> str:
        """提交任务到指定服务"""
        pass
```

## 5. 执行流程详解

### 5.1 完整执行流程图

![断点续执完整流程](images/checkpoint-execution-flow.svg)

### 5.2 流程启动阶段

```
1. 用户调用 FlowExecution.run(mode="distributed")
2. FlowExecution 初始化执行上下文
3. 生成唯一的 execution_id
4. 从 start_node 开始执行流程
5. 保存初始执行状态到 ExecutionStorage
```

### 5.3 事件节点处理阶段

```
1. FlowExecution 执行到 EventNode
2. EventNode.execute() 生成事件监听配置
3. FlowExecution._subscribe_event() 向 EventBus 注册订阅
4. 更新节点状态为 pending
5. 返回挂起状态 (is_suspend=True)
6. FlowWorker 保存执行状态
7. 外延服务开始处理任务
```

### 5.4 等待与恢复阶段

```
1. 外延服务处理任务（如等待延迟时间、监听队列）
2. 条件满足时，外延服务发布事件到 EventBus
3. EventBus 匹配订阅，找到对应的流程执行
4. FlowWorker 接收事件通知
5. FlowWorker 加载执行状态
6. FlowWorker 调用 resume_flow()
7. FlowExecution._handle_resume_operation() 处理恢复
8. EventNode.on_event() 更新节点状态
9. 继续执行后续节点
```

### 5.5 流程结束阶段

```
1. FlowExecution 执行到 EndNode
2. 更新执行状态为 completed
3. 触发 flow_end 回调
4. 返回最终执行结果
5. 可选：清理订阅和临时数据
```

## 6. 状态机与生命周期

### 6.1 流程执行状态机

```
                    ┌──────────────┐
                    │   created    │
                    └──────┬───────┘
                           │ start
                           ▼
┌──────────────────────────────────────────────┐
│                                              │
│  ┌──────────────┐       ┌──────────────┐    │
│  │   running    │◄──────│   resumed    │    │
│  └──────┬───────┘       └──────▲───────┘    │
│         │                      │            │
│         │ suspend              │ resume     │
│         ▼                      │            │
│  ┌──────────────┐              │            │
│  │  suspended   │──────────────┘            │
│  └──────────────┘                           │
│         │                                   │
│         │ timeout/cancel/error              │
│         ▼                                   │
└─────────┬────────────────────────────────────
          │
          ▼
    ┌──────────────┐       ┌──────────────┐
    │  completed   │       │    error     │
    └──────────────┘       └──────────────┘
```

### 6.2 事件节点状态机

```
┌──────────────┐
│   initial    │
└──────┬───────┘
       │ execute
       ▼
┌──────────────┐
│   pending    │───────────────┬───────────────┐
└──────┬───────┘               │               │
       │                       │               │
       │ on_event              │ on_timeout    │ on_cancel
       ▼                       ▼               ▼
┌──────────────┐        ┌──────────────┐ ┌──────────────┐
│  completed   │        │   timeout    │ │  cancelled   │
└──────────────┘        └──────────────┘ └──────────────┘
```

## 7. 回调与事件通知

### 7.1 FlowCallback 接口

断点续执引入了新的回调方法：

```python
class FlowCallback(ABC):
    # 流程级回调
    def on_flow_start(self, flow, **kwargs) -> None: ...
    def on_flow_end(self, flow, result, error, exception, **kwargs) -> None: ...
    def on_flow_suspend(self, flow, **kwargs) -> None: ...   # 新增
    def on_flow_resume(self, flow, **kwargs) -> None: ...    # 新增
    
    # 节点级回调
    def on_node_start(self, flow, node, **kwargs) -> None: ...
    def on_node_end(self, flow, node, result, error, exception, **kwargs) -> None: ...
    def on_node_suspend(self, flow, node, **kwargs) -> None: ...  # 新增
    def on_node_resume(self, flow, node, **kwargs) -> None: ...   # 新增
```

### 7.2 回调触发时机

| 回调方法 | 触发时机 |
|----------|----------|
| `on_flow_suspend` | 流程在事件节点处挂起时 |
| `on_flow_resume` | 流程从挂起状态恢复时 |
| `on_node_suspend` | 事件节点开始等待时 |
| `on_node_resume` | 事件节点收到事件恢复时 |

## 8. 部署架构

### 8.1 单机部署

```
┌─────────────────────────────────────────────────┐
│                   单机部署                       │
│                                                 │
│  ┌──────────────────────────────────────────┐  │
│  │           Application Process             │  │
│  │  ┌─────────────┐  ┌─────────────────────┐│  │
│  │  │ FlowWorker  │  │  Extended Services  ││  │
│  │  └─────────────┘  └─────────────────────┘│  │
│  └──────────────────────────────────────────┘  │
│                      │                         │
│                      ▼                         │
│  ┌──────────────────────────────────────────┐  │
│  │   InMemory EventBus / MemoryStorage      │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

### 8.2 分布式部署

```
┌─────────────────────────────────────────────────────────────────┐
│                        分布式部署                                 │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  FlowWorker #1  │  │  FlowWorker #2  │  │  FlowWorker #N  │ │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘ │
│           │                    │                    │          │
│           └────────────────────┼────────────────────┘          │
│                                │                               │
│                                ▼                               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                 Redis / PostgreSQL                       │   │
│  │  ┌───────────┐  ┌───────────────┐  ┌─────────────────┐  │   │
│  │  │ EventBus  │  │ExecutionStore │  │  FlowStore      │  │   │
│  │  └───────────┘  └───────────────┘  └─────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Extended Services Cluster                   │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │   │
│  │  │  Delay   │  │  Redis   │  │  Kafka   │  │   HTTP   │ │   │
│  │  │ Service  │  │  Queue   │  │  Queue   │  │ Callback │ │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## 9. 使用示例

### 9.1 定义包含事件节点的流程

```json
{
  "flow_id": "approval_workflow",
  "version": "1.0.0",
  "nodes": [
    {
      "id": "start",
      "type": "start",
      "next": "submit_request"
    },
    {
      "id": "submit_request",
      "type": "assignment",
      "assignments": [
        {"target": "request_id", "value": "${INPUT.request_id}"}
      ],
      "next": "wait_approval"
    },
    {
      "id": "wait_approval",
      "type": "event",
      "event_type": "approval.decision",
      "event_filter": {
        "request_id": "${NODE.submit_request.request_id}"
      },
      "next": "check_result"
    },
    {
      "id": "check_result",
      "type": "switch",
      "condition": "${NODE.wait_approval.event_data.approved}",
      "branches": [
        {"name": "approved", "next": "process_approved"},
        {"name": "rejected", "next": "process_rejected"}
      ]
    },
    {
      "id": "process_approved",
      "type": "assignment",
      "assignments": [{"target": "status", "value": "approved"}],
      "next": "end"
    },
    {
      "id": "process_rejected",
      "type": "assignment",
      "assignments": [{"target": "status", "value": "rejected"}],
      "next": "end"
    },
    {
      "id": "end",
      "type": "end"
    }
  ]
}
```

### 9.2 启动和恢复流程

```python
from plaita.flow import FlowExecution, Flow, ExecutionMode
from plaita.server.flow_worker import FlowWorker
from plaita.storage.redis import RedisExecutionStorage, RedisFlowStorage
from plaita.event.redis import RedisEventBus

# 创建存储和事件总线
execution_storage = RedisExecutionStorage(host="localhost")
flow_storage = RedisFlowStorage(host="localhost")
event_bus = RedisEventBus(redis_url="redis://localhost:6379")

# 创建工作器
worker = FlowWorker(execution_storage, flow_storage, event_bus)

# 启动流程
result = worker.start_flow(
    flow_id="approval_workflow",
    params={"request_id": "REQ-001"},
    version="1.0.0"
)
# result: {"execution_id": "xxx", "is_suspend": True, ...}

# 模拟审批完成，发送事件
await event_bus.publish({
    "event_type": "approval.decision",
    "data": {
        "request_id": "REQ-001",
        "approved": True,
        "approver": "manager"
    },
    "correlation_id": result["execution_id"]
})

# FlowWorker 自动接收事件并恢复流程
```

### 9.3 使用延迟节点

```json
{
  "id": "delay_5_minutes",
  "type": "delay",
  "delay_seconds": 300,
  "next": "continue_processing"
}
```

### 9.4 使用 Redis 队列节点

```json
{
  "id": "wait_message",
  "type": "redis_queue",
  "queue_name": "order_updates",
  "timeout": 3600,
  "next": "process_message"
}
```

## 10. 最佳实践

### 10.1 流程设计

1. **明确等待点**：在流程设计时明确标识所有可能的等待点
2. **设置超时**：为所有事件节点设置合理的超时时间
3. **处理边界情况**：设计超时、取消、错误的处理分支

### 10.2 状态管理

1. **最小化上下文**：只在上下文中存储必要的数据
2. **避免大对象**：不要在上下文中存储大型二进制数据
3. **定期清理**：定期清理已完成的执行状态

### 10.3 事件设计

1. **唯一事件类型**：使用有意义且唯一的事件类型命名
2. **包含关联ID**：事件中包含 correlation_id 便于追踪
3. **幂等处理**：确保事件处理的幂等性

### 10.4 运维监控

1. **监控挂起流程**：监控长时间挂起的流程
2. **告警超时**：设置超时告警机制
3. **日志追踪**：完整记录流程执行日志

## 11. 常见问题

### Q1: 流程挂起后服务重启会怎样？

A: 执行状态已持久化到存储中，服务重启后 FlowWorker 会自动加载挂起的流程，等待事件到达后继续执行。

### Q2: 如何处理事件丢失？

A: 建议使用 Redis 或数据库作为事件存储后端，确保事件持久化。同时为事件节点设置超时，超时后可以重试或走异常分支。

### Q3: 多个 FlowWorker 实例如何协调？

A: 使用 Redis 作为事件总线和状态存储时，多个 FlowWorker 实例可以自动协调，事件会被其中一个实例处理。

### Q4: 如何调试断点续执流程？

A: 使用 Generator 模式单步执行，或添加详细的日志回调来跟踪流程执行过程。

## 12. 参考资料

- [事件系统架构文档](../plaita/event/ARCHITECTURE.md)
- [扩展节点使用指南](../plaita/server/README.md)
- [需求规格文档](../requirements/checkpoint-requirements.md)

