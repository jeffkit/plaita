# Checkpoint（断点续执）功能需求文档

## 1. 概述

### 1.1 背景

plaita 当前的流程执行是同步阻塞的，无法支持长时间运行的工作流。例如：
- 需要等待外部 HTTP 回调的流程
- 需要人工审批的流程
- 需要等待消息队列消息的流程
- 需要延迟执行的流程

这些场景需要流程能够在某个节点"挂起"，保存当前执行状态，等待外部事件触发后"恢复"执行。

### 1.2 目标

实现一个完整的 Checkpoint 机制，使 plaita 支持：
1. 流程执行状态的持久化
2. 基于事件的流程恢复
3. 多种外延服务（延迟、队列、回调、审批等）
4. 分布式执行模式

### 1.3 涉及模块

- `plaita/event/` - 事件系统
- `plaita/storage/` - 状态持久化
- `plaita/server/` - 扩展节点和外延服务
- `plaita/node/event_node.py` - 事件节点基类
- `plaita/flow.py` - 流程执行引擎

---

## 2. 需求列表

### 2.1 事件系统核心

#### 2.1.1 事件总线 (EventBus)
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 实现事件发布/订阅机制，支持事件分发和处理器管理
- **相关文件**: 
  - `plaita/event/core.py`
  - `plaita/event/memory.py`
  - `plaita/event/redis.py`
  - `plaita/event/sqlalchemy.py`

#### 2.1.2 内存事件后端
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 实现内存版事件后端，用于单机环境和测试
- **验收标准**: 通过 `test_memory_eventbus.py` 所有测试
- **相关文件**: `plaita/event/memory.py`

#### 2.1.3 Redis 事件后端
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现 Redis 版事件后端，支持分布式环境
- **验收标准**: 通过 `test_redis_eventbus.py` 所有测试
- **相关文件**: `plaita/event/redis.py`

#### 2.1.4 SQLAlchemy 事件后端
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现数据库版事件后端，支持持久化存储
- **验收标准**: 通过 `test_sqlalchemy_eventbus.py` 所有测试
- **相关文件**: `plaita/event/sqlalchemy.py`

#### 2.1.5 事件过滤器
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现基于条件的事件过滤，支持通配符匹配
- **验收标准**: 通过 `test_event_filter_wildcard.py` 测试
- **相关文件**: `plaita/server/event_filter.py`

---

### 2.2 状态持久化

#### 2.2.1 存储基础接口
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 定义状态存储的基础接口
- **相关文件**: `plaita/storage/base.py`

#### 2.2.2 内存存储实现
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 实现内存版状态存储
- **相关文件**: `plaita/storage/memory.py`

#### 2.2.3 Redis 存储实现
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现 Redis 版状态存储，支持分布式
- **相关文件**: `plaita/storage/redis.py`

#### 2.2.4 SQLAlchemy 存储实现
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现数据库版状态存储
- **相关文件**: `plaita/storage/sqlalchemy.py`

---

### 2.3 事件节点

#### 2.3.1 EventNode 基类
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 实现事件节点基类，支持挂起和恢复
- **相关文件**: `plaita/node/event_node.py`

#### 2.3.2 HTTP 节点
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现 HTTP 请求节点
- **验收标准**: 通过 `tests/test_http.py` 测试
- **相关文件**: `plaita/node/http.py`

---

### 2.4 扩展节点

#### 2.4.1 BaseExtendedNode 基类
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 扩展节点的基础类
- **相关文件**: `plaita/server/nodes/base_extended_node.py`

#### 2.4.2 DelayNode 延迟节点
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现延迟执行节点
- **相关文件**: `plaita/server/nodes/delay_node.py`

#### 2.4.3 RedisQueueNode Redis 队列节点
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 监听 Redis 队列消息
- **相关文件**: `plaita/server/nodes/redis_queue_node.py`

#### 2.4.4 KafkaQueueNode Kafka 队列节点
- **状态**: [x] 已完成
- **优先级**: P2
- **描述**: 监听 Kafka 主题消息
- **相关文件**: `plaita/server/nodes/kafka_queue_node.py`

#### 2.4.5 HttpCallbackNode HTTP 回调节点
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 等待 HTTP 回调
- **相关文件**: `plaita/server/nodes/http_callback_node.py`

#### 2.4.6 ApprovalNode 审批节点
- **状态**: [x] 已完成
- **优先级**: P2
- **描述**: 发起人工审批并等待决策
- **相关文件**: `plaita/server/nodes/approval_node.py`

---

### 2.5 外延服务

#### 2.5.1 BaseExtendedService 服务基类
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 外延服务的基础类
- **相关文件**: `plaita/server/services/base_service.py`

#### 2.5.2 ServiceManager 服务管理器
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 统一管理所有外延服务
- **相关文件**: `plaita/server/services/service_manager.py`

#### 2.5.3 DelayService 延迟服务
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 处理延迟触发
- **相关文件**: `plaita/server/services/delay_service.py`

#### 2.5.4 RedisQueueService Redis 队列服务
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 处理 Redis 队列消息
- **已完善**: 
  - [x] 优化 `_check_flow_completion` 方法，通过 event_bus 订阅存储查询流程状态
  - [x] 移除对 `sys.modules` 和 `inspect` 的不健壮依赖
- **相关文件**: `plaita/server/services/redis_queue_service.py`

#### 2.5.5 KafkaQueueService Kafka 队列服务
- **状态**: [x] 已完成
- **优先级**: P2
- **描述**: 处理 Kafka 消息
- **已完善**: 
  - [x] 消费者组管理（创建、销毁、监控）
  - [x] 偏移量手动/自动提交
  - [x] 连接健康检查和自动重连
  - [x] 优化 `_check_flow_completion` 方法，移除 sys.modules 和 inspect 依赖
- **相关文件**: `plaita/server/services/kafka_queue_service.py`

#### 2.5.6 HttpCallbackService HTTP 回调服务
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 处理 HTTP 回调
- **相关文件**: `plaita/server/services/http_callback_service.py`

#### 2.5.7 ApprovalService 审批服务
- **状态**: [x] 已完成
- **优先级**: P2
- **描述**: 处理审批流程
- **相关文件**: `plaita/server/services/approval_service.py`

---

### 2.6 流程执行引擎集成

#### 2.6.1 分布式执行模式
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 在 FlowExecution 中实现真正的分布式执行模式
- **实现内容**: 
  - `_run_distributed` 方法完整实现
  - 上下文初始化和恢复 (`_initialize_context`)
  - resume 操作处理 (`_handle_resume_operation`)
  - 支持 cancel、timeout、event 三种恢复类型
  - 执行 ID 管理 (`_get_execution_id`)
- **相关文件**: `plaita/flow.py`

#### 2.6.2 FlowWorker 流程工作器
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现后台流程工作器，监听事件并恢复流程
- **验收标准**: 通过 `test_flow_worker.py` 测试
- **相关文件**: `plaita/server/flow_worker.py`

#### 2.6.3 上下文序列化
- **状态**: [x] 已完成
- **优先级**: P0
- **描述**: 实现执行上下文的序列化和反序列化
- **实现内容**: 
  - 上下文通过参数传递 (`context` 参数)
  - `_initialize_context` 方法处理上下文恢复
  - 与存储系统集成
- **相关文件**: `plaita/flow.py`

---

### 2.7 测试和文档

#### 2.7.1 单元测试
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 完善各模块的单元测试
- **已完成**:
  - [x] `tests/test_flow_worker_scenarios.py` 补充更多 FlowWorker 测试场景
  - [x] `tests/test_extended_nodes.py` 扩展节点单元测试
  - [x] `tests/test_performance_benchmark.py` 性能基准测试
- **相关文件**: `tests/`, `plaita/server/test_*.py`

#### 2.7.2 集成测试
- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 端到端的集成测试
- **已完成**:
  - [x] `tests/test_delay_integration.py` 延迟流程集成测试
  - [x] `tests/test_approval_integration.py` 审批流程集成测试
  - [x] 队列消息流程集成测试（包含在 test_extended_nodes.py）
- **相关文件**: `tests/test_delay_integration.py`, `tests/test_approval_integration.py`

#### 2.7.3 文档更新
- **状态**: [x] 已完成
- **优先级**: P2
- **描述**: 更新相关文档
- **相关文件**: 
  - `plaita/event/ARCHITECTURE.md`
  - `plaita/event/USER_GUIDE.md`
  - `plaita/server/README.md`

---

## 3. 技术设计

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Flow Execution                          │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                  FlowExecution                       │    │
│  │  - run()           // 普通执行                       │    │
│  │  - run_distributed() // 分布式执行                   │    │
│  │  - resume()        // 恢复执行                       │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Event System                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  EventBus    │  │EventStorage  │  │Subscription  │      │
│  │  - publish   │  │  - save      │  │  Storage     │      │
│  │  - subscribe │  │  - get       │  │              │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Extended Services                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Delay    │  │ Redis    │  │ Kafka    │  │ HTTP     │    │
│  │ Service  │  │ Queue    │  │ Queue    │  │ Callback │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 执行流程

1. **启动执行**: `FlowExecution.run(mode="distributed")`
2. **遇到事件节点**: 节点生成服务配置，发布订阅
3. **挂起执行**: 保存上下文到存储，返回挂起状态
4. **等待事件**: 外延服务处理任务，发布事件
5. **恢复执行**: FlowWorker 接收事件，加载上下文，恢复执行
6. **继续执行**: 从挂起点继续执行后续节点

---

## 4. 依赖项

### 4.1 Python 依赖

```
redis>=4.0.0
aiokafka>=0.7.0
sqlalchemy>=2.0.0
aiosqlite>=0.17.0
asyncpg>=0.27.0  # PostgreSQL 异步驱动
```

### 4.2 外部服务

- Redis (用于队列节点和分布式事件)
- Kafka (用于 Kafka 队列节点，可选)
- PostgreSQL/MySQL/SQLite (用于持久化存储)

---

## 5. 测试要求

### 5.1 单元测试覆盖

- 事件系统各后端: > 80%
- 存储系统各后端: > 80%
- 扩展节点: > 70%
- 外延服务: > 70%

### 5.2 集成测试场景

1. 延迟 5 秒后继续执行的流程
2. 等待 Redis 消息后继续执行的流程
3. 等待 HTTP 回调后继续执行的流程
4. 审批通过/拒绝后继续执行的流程
5. 混合多种事件节点的复杂流程

---

## 6. 更新记录

| 日期 | 更新内容 | 更新人 |
|------|----------|--------|
| 2024-12-30 | 初始版本，整理现有实现状态 | AI Assistant |
| 2024-12-30 | 完成 P1/P2 优化任务：1) 优化 `_check_flow_completion` 通过 event_bus 查询；2) 实现真正的异步流程执行；3) 优化 PlaitaClient Redis 初始化；4) 优化 parse_function 性能 | AI Assistant |
| 2024-12-30 | 完成所有剩余任务：1) 补充 FlowWorker 测试场景；2) 延迟流程集成测试；3) 完善 KafkaQueueService 消费者组管理；4) 审批流程集成测试；5) 扩展节点单元测试；6) 性能基准测试 | AI Assistant |

---

## 7. 待完成任务汇总

以下是当前任务完成状态，按优先级排序：

### P0 - 必须完成

1. [x] ~~完善 `FlowExecution._run_distributed()` 实现~~ ✅ 已完成
2. [x] ~~实现上下文的完整序列化和反序列化~~ ✅ 已完成
3. [x] ~~完善 FlowWorker 与 FlowExecution 的集成~~ ✅ 已完成

### P1 - 高优先级

4. [x] ~~完善 RedisQueueService 错误处理和重连机制~~ ✅ 已完成
   - 优化 `_check_flow_completion` 方法，通过 event_bus 查询流程状态
5. [x] ~~实现真正的异步流程执行~~ ✅ 已完成
   - 实现 `FlowExecution.arun_compatible` 真正的异步执行
   - 添加 `_arun`、`_aprocess_node`、`_aexecute_node_with_retry`、`_arun_node_with_timeout` 异步方法
6. [x] ~~补充 FlowWorker 的更多测试场景~~ ✅ 已完成
   - 新增 `tests/test_flow_worker_scenarios.py` 覆盖更多边界情况
7. [x] ~~实现延迟流程的端到端集成测试~~ ✅ 已完成
   - 新增 `tests/test_delay_integration.py` 延迟节点和服务集成测试

### P2 - 中优先级

8. [x] ~~优化 PlaitaClient 的 Redis 客户端初始化~~ ✅ 已完成
   - 添加 `RedisConfig` 配置类
   - 添加 Redis 连接验证和更好的错误处理
   - 支持 TTL 配置和缓存清除方法
9. [x] ~~优化 parse_function 性能~~ ✅ 已完成
   - 缓存 pyparsing 解析器基础组件
   - 添加快速检查跳过非函数表达式
10. [x] ~~完善 KafkaQueueService 消费者组管理~~ ✅ 已完成
    - 添加 ConsumerGroupInfo 数据类
    - 实现消费者组创建、销毁、监控
    - 实现偏移量手动/自动提交
    - 优化 `_check_flow_completion` 移除 sys.modules 依赖
11. [x] ~~实现审批流程的集成测试~~ ✅ 已完成
    - 新增 `tests/test_approval_integration.py` 审批节点和服务测试
12. [x] ~~添加扩展节点的单元测试~~ ✅ 已完成
    - 新增 `tests/test_extended_nodes.py` 覆盖所有扩展节点
13. [x] ~~添加性能基准测试~~ ✅ 已完成
    - 新增 `tests/test_performance_benchmark.py` 流程执行、存储、事件总线性能测试

