# Plaita 事件系统架构设计文档

> **诚实性更正（2026-07-14）**  
> 下文部分表述仍偏「目标态」。**以代码与 docs-site 为准**：  
> - FlowWorker 任务队列为 Redis `blpop` → **at-most-once**，不是至少一次。  
> - EventBus handler 去重为「成功后再 mark」；挂起订阅的 `event_type` 为**全等**匹配。  
> - 控制面（Registry/Queue/EventFilter）硬绑 Redis；「自由组合后端」不等于可换部署拓扑。  
> - 用户向文档见 [docs-site/distributed/flow-worker.md](../docs-site/docs/distributed/flow-worker.md) 与 [event-system.md](../docs-site/docs/distributed/event-system.md)。

## 1. 系统概述

Plaita 事件系统基于发布/订阅模式，连接挂起流程与外部触发源。提供 memory / Redis / SQLAlchemy 三种实现；生产编排路径（FlowWorker）另有可靠性限制，见上文更正。

![Plaita 事件系统架构](./images/arch-overview.svg)

### 1.1 设计目标

- **解耦**：实现系统组件间的松耦合通信
- **可扩展**：支持横向扩展，适应不同规模的应用
- **可靠性（目标 / 现状）**：handler 路径支持重试与成功后去重；**任务队列尚未提供至少一次投递**
- **灵活性**：支持过滤与匹配（订阅全等 / handler fnmatch，二者不同）
- **可维护性**：清晰的接口和简洁的代码结构
- **可配置性**：EventBus 存储组件可组合；控制面仍以 Redis 为默认硬依赖

### 1.2 核心特性

- 事件发布和订阅
- 基于条件的事件过滤（订阅侧浅层 dict；handler 侧可带 fnmatch）
- 事件去重（handler 成功后再 mark）
- 事件处理重试机制（handler `RetryPolicy`）
- 异步处理和并发控制
- 多种 EventBus 存储后端
- 与 FlowWorker / EventFilter 的挂起恢复接线

## 2. 核心组件

### 2.1 基础模型

![事件系统核心组件](./images/core-components.svg)

### 2.2 接口

![事件系统核心接口](./images/core-interfaces.svg)

### 2.3 实现类

系统提供三种基本实现方式，并支持混合模式：

- **内存实现**：适用于单机、单进程应用和测试环境
  - `InMemoryEventBus`
  - `MemoryEventStorage`
  - `InMemoryEventSubscriptionStorage`
  - `InMemoryProcessingTracker`

- **Redis实现**：适用于分布式应用和高并发场景
  - `RedisEventBus`
  - `RedisEventStorage`
  - `RedisEventSubscriptionStorage`
  - `RedisProcessingTracker`

- **SQLAlchemy实现**：适用于需要强持久化和复杂查询的场景
  - `SqlalchemyEventBus`
  - `SqlalchemyEventStorage`
  - `SqlalchemyEventSubscriptionStorage`
  - `SqlalchemyEventProcessingTracker`

- **混合模式**：自由组合上述三种实现的组件

## 3. 事件流程

### 3.1 事件发布流程

![事件发布流程](./images/publish-flow.svg)

### 3.2 事件处理流程

![事件处理流程](./images/process-flow.svg)

## 4. 实现细节

### 4.1 事件匹配机制

事件匹配基于两个核心要素：
- **事件类型匹配**：订阅特定类型的事件
- **过滤条件匹配**：基于事件数据内容的过滤

`EventSubscription.matches_event` 方法实现了匹配逻辑：
1. 检查事件类型是否匹配订阅的类型列表
2. 检查事件数据是否满足过滤条件
   - 支持嵌套属性访问（通过点表示法）
   - 支持正则表达式匹配（`regex:` 前缀）

### 4.2 去重和幂等处理

系统通过多层机制确保事件处理的幂等性：

1. **订阅级别去重**：`EventSubscription` 记录已处理事件ID
2. **全局去重**：`EventProcessingTracker` 记录每个处理器对事件的处理状态
3. **原子操作**：使用锁或Redis原子操作保证去重操作的一致性

### 4.3 重试机制

事件处理失败时的重试由 `RetryPolicy` 控制，支持：

- 最大重试次数
- 初始延迟时间
- 退避因子（每次重试增加的延迟倍数）
- 最大延迟时间

重试过程：
1. 尝试处理事件
2. 捕获异常时增加重试计数
3. 计算下一次重试延迟
4. 超过最大重试次数时放弃并记录失败

### 4.4 Redis实现优化

Redis实现针对分布式环境进行了优化，使用以下数据结构和存储模式：

#### 4.4.1 键值结构

1. **事件存储**
   - 键格式：`{prefix}:events:{event_id}`
   - 类型：Hash
   - 字段：
     - `event_type`: 事件类型
     - `data`: JSON格式的事件数据
     - `timestamp`: 事件时间戳
     - `source`: 事件来源
     - `correlation_id`: 关联ID
     - `created_at`: 创建时间

2. **事件订阅**
   - 键格式：`{prefix}:subscriptions:{subscription_id}`
   - 类型：Hash
   - 字段：
     - `event_types`: JSON数组格式的事件类型列表
     - `filter_condition`: JSON格式的过滤条件
     - `correlation_id`: 关联ID
     - `flow_id`: 流程ID
     - `node_id`: 节点ID
     - `created_at`: 创建时间

3. **已处理事件记录**
   - 键格式：`{prefix}:processed:{subscription_id}:{event_id}`
   - 类型：String
   - 值：处理时间戳

4. **事件处理历史**
   - 键格式：`{prefix}:history:{event_id}:{handler_id}`
   - 类型：List
   - 元素：JSON格式的处理记录，包含：
     - `status`: 处理状态
     - `error`: 错误信息
     - `timestamp`: 处理时间

5. **事件类型索引**
   - 键格式：`{prefix}:type_index:{event_type}`
   - 类型：Set
   - 成员：事件ID列表

6. **订阅索引**
   - 键格式：`{prefix}:subscription_index:{event_type}`
   - 类型：Set
   - 成员：订阅ID列表

#### 4.4.2 优化策略

1. **管道（Pipeline）**：批量操作使用Redis管道减少网络往返
2. **原子操作**：使用Redis原子命令（如HSETNX）确保线程安全
3. **PubSub机制**：使用Redis PubSub实现实时事件通知
4. **过期时间**：为所有Redis键设置合理的过期时间，避免无限增长

#### 4.4.3 性能优化

1. **批量操作**
   - 使用MSET/MGET进行批量读写
   - 使用Pipeline减少网络往返
   - 批量订阅和取消订阅

2. **内存优化**
   - 使用Hash结构存储事件数据，减少内存占用
   - 设置合理的过期时间，自动清理过期数据
   - 使用压缩列表（ziplist）优化小数据存储

3. **查询优化**
   - 使用Set结构维护事件类型索引
   - 使用Sorted Set实现时间范围查询
   - 利用Redis的原子操作实现并发控制

4. **可靠性保证**
   - 使用WATCH/MULTI/EXEC实现事务
   - 实现重试机制和错误恢复
   - 使用Lua脚本保证原子性

### 4.5 SQLAlchemy实现

SQLAlchemy实现提供了基于关系数据库的事件存储方案，支持多种数据库后端（如PostgreSQL、MySQL、SQLite等）。以下是主要表结构：

#### 4.5.1 事件表 (events)

| 字段名 | 类型 | 说明 | 索引 |
|--------|------|------|------|
| id | VARCHAR(36) | 事件ID，主键 | 主键 |
| event_type | VARCHAR(255) | 事件类型 | 是 |
| data | JSON | 事件数据 | 否 |
| timestamp | FLOAT | 事件时间戳 | 是 |
| source | VARCHAR(255) | 事件来源 | 否 |
| correlation_id | VARCHAR(36) | 关联ID | 是 |
| created_at | FLOAT | 创建时间 | 否 |

#### 4.5.2 事件订阅表 (event_subscriptions)

| 字段名 | 类型 | 说明 | 索引 |
|--------|------|------|------|
| id | VARCHAR(36) | 订阅ID，主键 | 主键 |
| event_types | JSON | 事件类型列表 | 否 |
| filter_condition | JSON | 过滤条件 | 否 |
| correlation_id | VARCHAR(36) | 关联ID | 是 |
| flow_id | VARCHAR(36) | 流程ID | 是 |
| node_id | VARCHAR(36) | 节点ID | 否 |
| created_at | FLOAT | 创建时间 | 否 |

#### 4.5.3 已处理事件表 (processed_events)

| 字段名 | 类型 | 说明 | 索引 |
|--------|------|------|------|
| subscription_id | VARCHAR(36) | 订阅ID | 主键 |
| event_id | VARCHAR(36) | 事件ID | 主键 |
| processed_at | FLOAT | 处理时间 | 否 |

#### 4.5.4 事件处理历史表 (event_processing_history)

| 字段名 | 类型 | 说明 | 索引 |
|--------|------|------|------|
| id | VARCHAR(36) | 记录ID，主键 | 主键 |
| event_id | VARCHAR(36) | 事件ID | 是 |
| handler_id | VARCHAR(36) | 处理器ID | 是 |
| status | VARCHAR(20) | 处理状态 | 否 |
| error | TEXT | 错误信息 | 否 |
| timestamp | FLOAT | 记录时间 | 否 |

SQLAlchemy实现的主要特点：

1. **事务支持**：利用数据库事务确保数据一致性
2. **索引优化**：为常用查询字段创建索引
3. **JSON支持**：使用数据库的JSON类型存储灵活的事件数据
4. **异步操作**：基于SQLAlchemy的异步API实现
5. **批量操作**：支持批量插入和更新操作
6. **初始化流程**：通过`initialize`方法支持按需创建表结构，确保应用启动时数据库就绪

## 5. 扩展点

系统设计提供了多个扩展点：

1. **新存储后端**：实现 `EventStorage` 和 `EventSubscriptionStorage` 接口
2. **自定义事件总线**：扩展 `EventBus` 接口实现特定需求
3. **过滤机制增强**：扩展 `EventSubscription.matches_event` 方法
4. **处理器装饰器**：使用 `event_handler` 装饰器简化注册

## 6. 性能考虑

### 6.1 内存使用

- 使用批量操作减少对象创建和销毁
- 实现自动清理机制，定期删除旧的处理记录
- 为历史记录设置最大数量限制

### 6.2 并发处理

- 所有操作设计为异步，支持高并发
- 使用锁和原子操作确保线程安全
- 通过 `asyncio.create_task` 并发处理多个事件

### 6.3 扩展性

- 分布式部署时可横向扩展处理节点
- Redis实现支持多个实例同时订阅和处理事件
- 处理器注册与事件分发机制支持动态扩展

## 7. 最佳实践

1. 使用 `retry_policy` 为关键处理器设置合理的重试策略
2. 设计精确的事件类型和过滤条件，避免不必要的事件处理
3. 定期清理过期的处理记录和历史数据
4. 适当使用批量操作提高性能
5. 在分布式环境中使用Redis实现 

## 8. 混合实现方案

Plaita事件系统支持混合使用不同的组件实现，为不同应用场景提供最优解决方案。这种灵活的架构设计使系统能够同时利用不同后端的优势。

### 8.1 组件混合模式

系统支持以下四个核心组件的自由组合：

1. **EventBus**: 事件总线组件，负责事件分发和处理器管理
2. **EventStorage**: 事件存储组件，负责事件的持久化
3. **EventSubscriptionStorage**: 订阅存储组件，管理事件订阅
4. **EventProcessingTracker**: 处理跟踪器，记录事件处理状态和历史

每个组件都可以独立选择以下三种实现方式之一：
- Memory: 内存实现，适合单机测试和轻量级应用
- Redis: Redis实现，适合分布式和高并发场景
- DB(SQLAlchemy): 数据库实现，适合需要强持久化的场景

### 8.2 常见混合模式示例

1. **Redis EventBus + DB EventStorage**
   - 使用Redis的高性能特性处理实时事件分发
   - 使用关系数据库存储事件历史和处理记录
   - 实现方式：
     ```python
     # 创建混合事件总线
     event_bus = MixedEventBus(
         bus_type="redis",
         storage_type="db",
         subscription_type="redis",
         tracker_type="db",
         redis_url="redis://localhost:6379/0",
         database_url="postgresql+asyncpg://user:pass@localhost/dbname"
     )
     ```

2. **Memory EventBus + Redis EventStorage**
   - 单机事件处理与分布式存储结合
   - 适合开发测试环境
   - 实现方式：
     ```python
     # 创建混合事件总线
     event_bus = MixedEventBus(
         bus_type="memory",
         storage_type="redis",
         subscription_type="memory",
         tracker_type="redis",
         redis_url="redis://localhost:6379/0"
     )
     ```

3. **Redis EventBus + Redis Storage + DB Tracker**
   - Redis处理实时事件和订阅
   - 数据库记录详细的处理历史，便于分析和审计
   - 实现方式：
     ```python
     # 创建混合事件总线
     event_bus = MixedEventBus(
         bus_type="redis",
         storage_type="redis",
         subscription_type="redis",
         tracker_type="db",
         redis_url="redis://localhost:6379/0",
         database_url="sqlite+aiosqlite:///events.db"
     )
     ```

### 8.3 混合模式优势

1. **针对性能优化**
   - 选择最适合特定组件需求的后端
   - 平衡内存消耗、性能和持久化需求

2. **灵活部署**
   - 支持从单机开发环境平滑迁移到分布式生产环境
   - 可以逐步升级系统组件而非一次性重构

3. **资源优化**
   - 将高频访问组件放在高性能后端
   - 将需要长期存储的数据放在持久化后端

4. **可靠性与性能平衡**
   - 通过混合Redis与数据库实现，在保证高性能的同时提供数据持久化
   - 通过跟踪器的可靠存储确保事件处理的可追溯性

### 8.4 使用示例（命令行）

混合模式可以通过统一的演示程序来测试和使用：

```shell
# 使用Redis事件总线和DB存储
python -m examples.event_demo.demo_eventbus --backend mixed --bus-type redis --storage-type db --subscription-type redis --tracker-type db

# 使用Memory事件总线和Redis存储
python -m examples.event_demo.demo_eventbus --backend mixed --bus-type memory --storage-type redis --subscription-type memory --tracker-type redis
```

### 8.5 事件处理流程详解

事件处理流程中的ProcessingTracker工作过程：

1. **事件分发阶段**
   - EventBus接收到新事件
   - 创建事件处理任务
   - 调用`_dispatch_event`方法

2. **去重检查阶段**
   - 调用`mark_event_processed`方法
   - 检查事件是否已被处理
   - 使用原子操作标记处理状态
   - 返回处理状态（新事件/已处理）

3. **事件处理阶段**
   - 如果事件未处理，执行处理器
   - 处理器处理事件
   - 返回处理结果

4. **结果记录阶段**
   - 调用`record_processing_attempt`方法
   - 记录处理结果（成功/失败）
   - 存储处理历史
   - 更新事件状态

5. **存储实现差异**
   - **Redis实现**：
     - 使用Hash结构存储事件数据
     - 使用Set结构维护处理状态
     - 使用List结构记录处理历史
     - 通过原子操作保证一致性

   - **DB实现**：
     - 使用事务保证原子性
     - 通过外键关联维护数据完整性
     - 使用索引优化查询性能
     - 支持复杂的历史记录查询

6. **错误处理**
   - 处理失败时记录错误信息
   - 支持重试机制
   - 维护处理状态
   - 提供错误追踪能力 

## 9. 单元测试指南

Plaita事件系统采用全面的单元测试策略，确保各组件的功能正确性和稳定性。本节介绍如何运行和扩展事件系统的单元测试。

### 9.1 测试文件组织

事件系统的测试文件按照实现后端分为三类：

1. **内存实现测试**：`test_memory_eventbus.py`
2. **Redis实现测试**：`test_redis_eventbus.py`
3. **SQLAlchemy实现测试**：`test_sqlalchemy_eventbus.py`

每个测试文件都包含对应后端实现的全面测试，包括事件发布、订阅、处理器注册、过滤条件、等待机制、重试策略等功能。

### 9.2 运行单元测试

#### 9.2.1 运行所有测试

使用pytest运行所有事件系统测试：

```shell
python -m pytest plaita/event/test_*.py -v
```

#### 9.2.2 运行特定后端实现的测试

测试内存实现：

```shell
python -m pytest plaita/event/test_memory_eventbus.py -v
```

测试Redis实现：

```shell
python -m pytest plaita/event/test_redis_eventbus.py -v
```

测试SQLAlchemy实现：

```shell
python -m pytest plaita/event/test_sqlalchemy_eventbus.py -v
```

#### 9.2.3 运行特定功能测试

可以使用pytest的`-k`参数运行特定功能的测试：

```shell
# 运行所有与事件发布相关的测试
python -m pytest plaita/event/test_*.py -k "publish" -v

# 运行所有与重试策略相关的测试
python -m pytest plaita/event/test_*.py -k "retry_policy" -v
```

### 9.3 测试环境设置

各后端实现使用不同的测试环境：

#### 9.3.1 内存实现

内存实现测试不需要外部依赖，直接在内存中运行。

#### 9.3.2 Redis实现

Redis实现测试使用`fakeredis`模拟Redis服务器，无需实际的Redis实例。相关设置在`test_redis_eventbus.py`的`redis_mock`和`event_bus`固件中实现。

#### 9.3.3 SQLAlchemy实现

SQLAlchemy实现测试使用SQLite内存数据库，配置如下：

```python
# 使用SQLite内存数据库URL
SQLITE_URL = "sqlite+aiosqlite:///:memory:"

@pytest_asyncio.fixture
async def event_bus():
    # 创建带有表创建功能的事件总线
    bus = SqlalchemyEventBus(
        database_url=SQLITE_URL,
        create_tables=True,
        min_retry_interval=0.1  # 缩短重试间隔用于测试
    )
    
    # 显式调用初始化方法并等待表创建完成
    await bus.initialize()
    
    yield bus
    
    # 清理资源
    await bus.close()
```

### 9.4 常见测试用例

所有后端实现共享一组标准测试用例，以确保功能一致性：

1. **基本事件发布与获取**：`test_publish_event`
2. **事件处理器注册与调用**：`test_event_handler_registration`
3. **多处理器处理同一事件**：`test_multiple_handlers`
4. **过滤条件测试**：`test_filter_condition`
5. **等待事件功能**：`test_wait_for_event`和`test_wait_for_event_timeout`
6. **事件去重功能**：`test_event_deduplication`和`test_event_not_deduplicated`
7. **重试策略功能**：`test_retry_policy`
8. **批量发布事件**：`test_batch_publish`
9. **订阅管理**：`test_subscription_registration`和`test_unregister_subscription`
10. **处理历史记录**：`test_processing_history`

### 9.5 测试最佳实践

1. **隔离测试**：每个测试函数应该独立执行，不依赖于其他测试的状态
2. **使用Fixture**：利用pytest的Fixture机制设置和清理测试环境
3. **异步测试**：所有测试函数应使用`@pytest.mark.asyncio`标记，并使用`async/await`语法
4. **模拟外部依赖**：使用模拟对象替代实际的外部服务（如Redis）
5. **缩短等待时间**：在测试中使用较小的超时和间隔值，加快测试速度
6. **全面覆盖**：确保测试覆盖所有API方法和错误处理路径

### 9.6 编写新测试

编写新测试时，请遵循以下步骤：

1. **确定测试目标**：明确要测试的功能或组件
2. **选择合适的测试文件**：根据实现类型选择对应的测试文件
3. **编写测试函数**：使用`@pytest.mark.asyncio`装饰器标记异步测试函数
4. **使用断言验证**：使用`assert`语句验证结果是否符合预期
5. **适当添加延迟**：在异步测试中可能需要使用`await asyncio.sleep()`等待事件处理完成
6. **清理资源**：确保测试完成后释放所有资源

例如，添加一个新的测试用例：

```python
@pytest.mark.asyncio
async def test_custom_feature(event_bus):
    """测试自定义功能"""
    # 准备测试数据
    test_data = {"key": "value"}
    
    # 执行被测试的功能
    result = await event_bus.some_method(test_data)
    
    # 验证结果
    assert result is not None
    assert result.get("status") == "success"
    
    # 等待异步处理完成
    await asyncio.sleep(0.1)
    
    # 进一步验证状态
    final_state = await event_bus.get_state()
    assert final_state.is_completed
```

## 10. 常见问题解答

**Q: 事件系统是否支持多个实例并发运行？**

A: 是的，使用Redis实现时，多个实例可以并发运行并共享事件。

**Q: 如何确保事件不会丢失？**

A: 事件总线会持久化存储事件，即使系统崩溃，在恢复后也能处理未完成的事件。

**Q: 处理器抛出异常会发生什么？**

A: 默认情况下，异常会被记录但不会中断处理流程。如果设置了重试策略，则会按照策略进行重试。

**Q: 如何在大量事件下保持性能？**

A: 使用批量操作、精确的过滤条件、合理的索引设计，以及适当的清理策略。

**Q: 如何选择合适的后端组合？**

A: 根据应用场景特点选择：
- 高并发场景优先使用Redis实现
- 需要可靠存储和复杂查询使用SQLAlchemy实现
- 对事件历史有分析需求时使用DB作为跟踪器
- 开发测试环境可使用内存实现或混合模式

**Q: 混合模式会带来额外开销吗？**

A: 混合模式确实会引入不同后端之间的转换开销，但通过合理组合可最大化各后端优势，总体收益通常大于开销。

**Q: 如何从单一后端迁移到混合后端？**

A: 系统设计支持平滑迁移：
1. 先引入MixedEventBus，配置为全部使用原后端
2. 逐步替换单个组件到新后端
3. 验证系统稳定性后继续替换其他组件 