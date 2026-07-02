# 扩展节点和外延服务框架演示

这个演示展示了如何使用基于EventNode的扩展节点框架，包括延迟节点、审批节点、HTTP回调节点和队列节点等。

## 功能特性

### 扩展节点类型

1. **延迟节点 (DelayNode)** - 支持指定延迟时间后触发事件
2. **审批节点 (ApprovalNode)** - 支持人工审批流程
3. **HTTP回调节点 (HttpCallbackNode)** - 支持HTTP回调等待
4. **Redis队列节点 (RedisQueueNode)** - 支持Redis队列消息监听
5. **Kafka队列节点 (KafkaQueueNode)** - 支持Kafka消息监听

### 外延服务

- **延迟服务 (DelayService)** - 处理延迟任务
- **审批服务 (ApprovalService)** - 处理审批流程
- **HTTP回调服务 (HttpCallbackService)** - 处理HTTP回调
- **队列服务** - 处理消息队列监听

## 运行演示

### 完整演示

运行所有扩展节点的完整演示：

```bash
python -m examples.server_demo.extended_nodes_demo
```

### 简化演示

运行核心功能的简化演示：

```bash
python -m examples.server_demo.extended_nodes_demo simple
```

## 演示内容

### 延迟节点演示

- 创建延迟节点，设置延迟时间
- 提交延迟任务到延迟服务
- 等待延迟完成，触发事件

### 审批节点演示

- 创建审批节点，配置审批人和策略
- 提交审批任务到审批服务
- 模拟审批决策，触发审批完成事件

### HTTP回调节点演示

- 创建HTTP回调节点
- 注册回调端点
- 模拟HTTP回调请求

### Kafka队列节点演示

- 创建Kafka队列节点
- 监听指定topic的消息
- 模拟消息触发（在模拟模式下）

### 完整流程演示

- 创建包含延迟节点的完整流程
- 使用分布式模式执行流程
- 演示流程挂起和恢复机制

## 架构说明

### 扩展节点架构

```
BaseExtendedNode (继承自 EventNode)
├── DelayNode - 延迟节点
├── ApprovalNode - 审批节点
├── HttpCallbackNode - HTTP回调节点
├── RedisQueueNode - Redis队列节点
└── KafkaQueueNode - Kafka队列节点
```

### 服务管理架构

```
ServiceManager
├── DelayService - 延迟服务
├── ApprovalService - 审批服务
├── HttpCallbackService - HTTP回调服务
├── RedisQueueService - Redis队列服务
└── KafkaQueueService - Kafka队列服务
```

### 事件驱动流程

1. **节点执行** - 扩展节点生成服务配置
2. **任务提交** - 服务管理器处理节点配置，创建异步任务
3. **事件触发** - 外延服务完成处理后触发事件
4. **流程恢复** - 事件总线通知流程引擎恢复执行

## 配置说明

### 延迟节点配置

```python
DelayNode(
    id="delay_node",
    delay_seconds=5,           # 延迟秒数
    delay_unit="seconds",      # 时间单位
    event_type="delay_trigger" # 事件类型
)
```

### 审批节点配置

```python
ApprovalNode(
    id="approval_node",
    event_type="approval_trigger",
    approval_title="审批标题",
    approval_content="审批内容",
    approvers=["user1", "user2"],    # 审批人列表
    approval_strategy="any"          # 审批策略: any/all
)
```

### HTTP回调节点配置

```python
HttpCallbackNode(
    id="callback_node",
    event_type="http_callback_trigger",
    callback_method="POST",          # HTTP方法
    require_auth=False,              # 是否需要认证
    service_config={"base_url": "http://localhost:8080"}
)
```

## 扩展开发

### 创建新的扩展节点

1. 继承 `BaseExtendedNode`
2. 实现 `generate_service_config` 方法
3. 定义节点特有的配置属性
4. 注册节点类型到解析器

### 创建新的外延服务

1. 继承 `BaseExtendedService`
2. 实现 `process_task` 方法
3. 处理异步任务逻辑
4. 触发相应的事件

## 注意事项

- 确保事件总线正常运行
- 服务配置要与节点配置匹配
- 异步任务需要正确处理异常
- 事件类型要保持一致性

## 故障排除

### 常见问题

1. **节点注册失败** - 检查节点类型是否正确注册
2. **服务启动失败** - 检查服务配置和依赖
3. **事件未触发** - 检查事件类型和过滤条件
4. **任务执行失败** - 查看服务日志和异常信息

### 日志查看

演示程序会输出详细的日志信息，包括：
- 节点注册状态
- 服务启动状态
- 任务执行过程
- 事件触发情况
- 错误和异常信息 