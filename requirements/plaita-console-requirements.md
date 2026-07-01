# Plaita Console 管理台需求文档

## 1. 概述

### 1.1 项目背景

plaita 是一个支持断点续执的分布式流程执行引擎，包含以下核心组件：

- **FlowWorker** - 流程执行工作器，负责执行和恢复流程
- **扩展服务** - 包括 DelayService、RedisQueueService、KafkaQueueService、HttpCallbackService、ApprovalService
- **事件总线** - 支持 Memory、Redis、SQLAlchemy 多种后端
- **状态存储** - 支持 Memory、Redis、SQLAlchemy 多种后端

目前缺乏统一的可视化管理界面来控制这些分布式组件的运行状态。

### 1.2 项目目标

开发 **plaita-console** 可视化管理台，提供：

1. **服务拓扑视图** - 实时展示集群服务结构与关联关系
2. **服务生命周期管理** - 启动/停止各类服务
3. **执行实例管理** - 流程执行的查看、启动、停止
4. **实时监控** - 执行状态、日志流、任务队列
5. **可视化流程查看器** - 图形化展示流程执行进度

### 1.3 设计原则

- **最小依赖** - 复用 plaita 现有 Storage，使用 Redis 作为服务注册中心
- **容器化优先** - 支持 Docker Compose 和 K8s 部署
- **实时响应** - 使用 SSE 实现实时数据推送
- **简洁易用** - 无需登录认证，开箱即用

---

## 2. 需求列表

### 2.1 服务发现与注册

#### 2.1.1 服务注册机制

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 为 FlowWorker 和各扩展服务实现 Redis 注册机制

**功能要求**：
1. 服务启动时向 Redis 注册，记录以下信息：
   - 服务类型 (flow_worker, delay_service, redis_queue_service 等)
   - 实例 ID (唯一标识)
   - 主机名 / IP 地址
   - 启动时间
   - 配置信息（队列名、连接参数等）
   - 状态 (starting, running, stopping, stopped)

2. 注册 key 格式：`plaita:registry:{service_type}:{instance_id}`
3. 使用 TTL 自动过期（默认 30 秒）
4. 服务正常关闭时主动注销

- **验收标准**:
  - FlowWorker 启动后可在 Redis 中查看注册信息
  - 各扩展服务启动后可在 Redis 中查看注册信息
  - 服务停止 30 秒后注册信息自动过期
- **相关文件**:
  - `plaita/server/flow_worker.py`
  - `plaita/server/services/base_service.py`

#### 2.1.2 心跳机制

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 实现服务心跳，保持注册信息有效

**功能要求**：
1. 后台线程定期刷新 TTL（每 10 秒）
2. 心跳包含最新的运行状态信息：
   - 当前处理的任务数
   - 最后活跃时间
   - 资源使用情况（可选）

- **验收标准**:
  - 服务运行期间注册信息持续有效
  - 服务崩溃后注册信息在 TTL 后过期
- **相关文件**:
  - `plaita/server/services/base_service.py` (新增心跳逻辑)

#### 2.1.3 服务发现 API

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 后端 API 实现服务发现接口

**API 设计**：
```
GET /api/services
  - 返回所有已注册服务列表
  - 支持按类型筛选 ?type=flow_worker

GET /api/services/{instance_id}
  - 返回指定服务详情

GET /api/services/topology
  - 返回服务拓扑结构（含关联关系）
```

- **验收标准**:
  - API 能正确返回所有在线服务
  - 能区分服务类型和状态
- **相关文件**:
  - `plaita-console/backend/api/services.py` (新建)

---

### 2.2 服务拓扑图

#### 2.2.1 拓扑数据模型

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 定义服务拓扑的数据结构

**数据结构**：
```python
class ServiceNode:
    instance_id: str          # 实例唯一标识
    service_type: str         # 服务类型
    name: str                 # 显示名称
    host: str                 # 主机地址
    status: str               # running, stopped, error
    start_time: datetime      # 启动时间
    metadata: dict            # 额外配置信息

class ServiceEdge:
    source_id: str            # 源服务 ID
    target_id: str            # 目标服务 ID
    edge_type: str            # 关联类型：uses_queue, uses_storage, publishes_event
    label: str                # 边标签

class ServiceTopology:
    nodes: List[ServiceNode]
    edges: List[ServiceEdge]
    timestamp: datetime
```

- **验收标准**:
  - 能表达 FlowWorker -> Redis Queue 的关系
  - 能表达 Service -> EventBus 的关系
  - 能表达 Service -> Storage 的关系
- **相关文件**:
  - `plaita-console/backend/models/topology.py` (新建)

#### 2.2.2 拓扑关系推断

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 根据服务配置自动推断拓扑关系

**推断规则**：
1. **FlowWorker**:
   - 使用 Redis 队列 -> 边类型 `uses_queue`
   - 使用执行存储 -> 边类型 `uses_storage`
   - 使用事件总线 -> 边类型 `uses_eventbus`

2. **扩展服务**:
   - 监听事件总线 -> 边类型 `subscribes_event`
   - 发布事件 -> 边类型 `publishes_event`

3. **共享资源**（Redis/DB）作为中间节点展示

- **验收标准**:
  - 能自动生成服务间的拓扑连接
  - 拓扑图能反映实际的数据流向
- **相关文件**:
  - `plaita-console/backend/services/topology_service.py` (新建)

#### 2.2.3 前端拓扑可视化

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 使用 React 实现交互式拓扑图

**功能要求**：
1. 使用图形库（如 React Flow / D3.js / vis.js）绘制拓扑图
2. 节点样式区分服务类型和状态：
   - 不同服务类型使用不同图标/颜色
   - 在线状态：绿色边框
   - 离线状态：红色边框 + 虚线
   - 异常状态：黄色边框
3. 边样式表示关联类型：
   - 实线：活跃连接
   - 虚线：非活跃连接
   - 箭头表示数据流向
4. 支持交互：
   - 点击节点查看详情
   - 拖拽调整布局
   - 缩放/平移
5. 实时更新：服务上下线自动刷新拓扑

- **验收标准**:
  - 能清晰展示集群中所有服务的分布
  - 能一眼看出哪些服务在线/离线
  - 能理解服务间的依赖关系
- **相关文件**:
  - `plaita-console/frontend/src/components/Topology/` (新建)

---

### 2.3 服务启停控制

#### 2.3.1 控制指令通道

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 使用 Redis Pub/Sub 实现控制指令下发

**设计**：
1. 控制 channel 格式：`plaita:control:{instance_id}`
2. 支持的指令：
   - `{"command": "stop", "graceful": true}` - 优雅停止
   - `{"command": "stop", "graceful": false}` - 强制停止
   - `{"command": "status"}` - 请求状态上报
   - `{"command": "reload_config"}` - 重新加载配置（可选）

3. 服务端响应：
   - 收到 stop 指令后，完成当前任务再停止
   - 通过注册信息更新状态

- **验收标准**:
  - 管理台能向指定服务发送停止指令
  - 服务能正确响应指令
- **相关文件**:
  - `plaita/server/services/base_service.py` (添加指令监听)
  - `plaita/server/flow_worker.py` (添加指令监听)
  - `plaita-console/backend/services/control_service.py` (新建)

#### 2.3.2 控制 API

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 后端 API 实现服务控制接口

**API 设计**：
```
POST /api/services/{instance_id}/stop
  - 请求体: {"graceful": true}
  - 发送停止指令

POST /api/services/{instance_id}/start
  - 注：启动需要外部编排器（Docker/K8s）支持
  - 返回启动指引或触发编排器 API

GET /api/services/{instance_id}/status
  - 获取服务实时状态
```

- **验收标准**:
  - 能通过 API 停止指定服务
  - 能获取服务实时状态
- **相关文件**:
  - `plaita-console/backend/api/services.py`

#### 2.3.3 前端控制界面

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 在拓扑图和服务列表中提供控制操作

**功能要求**：
1. 服务详情面板显示控制按钮
2. 停止前确认对话框
3. 操作结果反馈（成功/失败提示）
4. 批量操作支持（可选）

- **验收标准**:
  - 能从界面停止单个服务
  - 有操作确认和结果反馈
- **相关文件**:
  - `plaita-console/frontend/src/components/ServiceControl/` (新建)

---

### 2.4 执行实例管理

#### 2.4.1 执行列表 API

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 提供执行实例的列表和详情 API

**API 设计**：
```
GET /api/executions
  - 返回执行实例列表
  - 支持分页: ?page=1&size=20
  - 支持筛选: ?status=running&flow_id=xxx

GET /api/executions/{execution_id}
  - 返回执行详情，包含：
    - 基本信息（ID、流程ID、状态、时间）
    - 执行上下文
    - 当前节点信息
    - 错误信息（如有）

POST /api/executions
  - 启动新的流程执行
  - 请求体: {"flow_id": "xxx", "version": "1.0.0", "params": {...}}

DELETE /api/executions/{execution_id}
  - 取消/终止执行

POST /api/executions/{execution_id}/resume
  - 恢复暂停的执行
  - 请求体: {"resume_type": "continue", "data": {...}}
```

- **验收标准**:
  - 能查询所有执行实例
  - 能按状态筛选
  - 能启动和停止执行
- **相关文件**:
  - `plaita-console/backend/api/executions.py` (新建)

#### 2.4.2 执行列表页面

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 前端实现执行实例列表页面

**功能要求**：
1. 表格展示执行列表：
   - 执行 ID
   - 流程 ID / 名称
   - 状态（带颜色标识）
   - 开始时间
   - 持续时间
   - 当前节点
   - 操作按钮
2. 筛选器：状态、流程 ID、时间范围
3. 分页和排序
4. 自动刷新（可配置间隔）

- **验收标准**:
  - 能看到所有执行实例
  - 能快速筛选感兴趣的执行
- **相关文件**:
  - `plaita-console/frontend/src/pages/Executions/` (新建)

#### 2.4.3 启动流程对话框

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 提供 JSON 编辑器输入流程参数

**功能要求**：
1. 流程 ID 输入（支持下拉选择已存储的流程）
2. 版本选择
3. JSON 编辑器输入参数：
   - 语法高亮
   - 格式验证
   - 格式化按钮
4. 启动确认

- **验收标准**:
  - 能手动输入 JSON 参数启动流程
  - 能检测 JSON 格式错误
- **相关文件**:
  - `plaita-console/frontend/src/components/StartFlowDialog/` (新建)

---

### 2.5 执行监控

#### 2.5.1 执行详情页面

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 展示单个执行的详细信息

**功能要求**：
1. 基本信息卡片：
   - 执行 ID、流程 ID、版本
   - 状态（大图标 + 文字）
   - 时间信息（开始、更新、结束）
   - 输入参数（可折叠 JSON）
2. 执行上下文（可折叠 JSON）
3. 错误信息（如有）
4. 操作按钮：停止、恢复

- **验收标准**:
  - 能看到执行的完整信息
  - 能快速判断执行状态
- **相关文件**:
  - `plaita-console/frontend/src/pages/ExecutionDetail/` (新建)

#### 2.5.2 实时状态更新（SSE）

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 使用 SSE 推送执行状态变化

**API 设计**：
```
GET /api/executions/{execution_id}/stream
  - SSE 端点
  - 事件类型：status_changed, context_updated, completed, error
  - 数据格式：JSON
```

- **验收标准**:
  - 执行状态变化时页面实时更新
  - 无需手动刷新
- **相关文件**:
  - `plaita-console/backend/api/executions.py`
  - `plaita-console/frontend/src/hooks/useExecutionStream.ts` (新建)

---

### 2.6 可视化流程查看器

#### 2.6.1 流程图渲染

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 将流程定义渲染为可视化图形

**功能要求**：
1. 从执行上下文提取流程结构
2. 渲染节点：
   - 不同节点类型使用不同形状/图标
   - Start 节点：圆形
   - End 节点：双圆形
   - 普通节点：矩形
   - 事件节点：带暂停标识
   - 分支节点：菱形
3. 渲染连接线：
   - 顺序执行：实线箭头
   - 条件分支：带标签
4. 自动布局（从上到下或从左到右）

- **验收标准**:
  - 能正确渲染流程结构
  - 节点类型一目了然
- **相关文件**:
  - `plaita-console/frontend/src/components/FlowViewer/` (新建)

#### 2.6.2 执行进度高亮

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 在流程图上高亮显示执行进度

**功能要求**：
1. 根据执行上下文确定：
   - 已执行节点：绿色填充
   - 当前节点：高亮边框 + 动画
   - 等待中节点：黄色填充
   - 未执行节点：灰色
   - 错误节点：红色
2. 实时更新高亮状态（结合 SSE）
3. 点击节点查看该节点的执行详情

- **验收标准**:
  - 能清楚看到流程执行到哪一步
  - 能区分不同节点状态
- **相关文件**:
  - `plaita-console/frontend/src/components/FlowViewer/`

---

### 2.7 任务队列查看

#### 2.7.1 队列状态 API

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 提供各服务任务队列的状态查询

**API 设计**：
```
GET /api/queues
  - 返回所有队列概览：
    - 队列名称
    - 待处理任务数
    - 关联的服务

GET /api/queues/{queue_name}
  - 返回队列详情：
    - 任务列表（分页）
    - 每个任务的基本信息
```

- **验收标准**:
  - 能查看各队列的任务积压情况
  - 能看到队列中的具体任务
- **相关文件**:
  - `plaita-console/backend/api/queues.py` (新建)

#### 2.7.2 队列监控页面

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 前端展示任务队列状态

**功能要求**：
1. 队列卡片列表：
   - 队列名称
   - 任务数量（数字 + 颜色）
   - 关联服务状态
2. 点击卡片展开任务列表
3. 支持查看任务详情

- **验收标准**:
  - 能快速了解队列健康状态
  - 能发现任务积压
- **相关文件**:
  - `plaita-console/frontend/src/pages/Queues/` (新建)

---

### 2.8 日志查看

#### 2.8.1 日志收集

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 为服务添加日志输出到可查询的存储

**设计考虑**：
1. 方案 A：服务将日志发送到 Redis Stream
2. 方案 B：使用文件日志 + 容器日志驱动
3. 方案 C：集成外部日志系统（如 Plaita/ELK）

**建议**：使用 Redis Stream 作为轻量级方案：
- Key 格式：`plaita:logs:{service_type}:{instance_id}`
- 日志格式：`{timestamp, level, message, context}`
- 保留策略：按数量或时间自动裁剪

- **验收标准**:
  - 服务日志可通过 API 查询
  - 支持按时间范围查询
- **相关文件**:
  - `plaita/logger.py` (添加 Redis 日志处理器)
  - `plaita-console/backend/api/logs.py` (新建)

#### 2.8.2 日志流 API（SSE）

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 提供实时日志流

**API 设计**：
```
GET /api/logs/stream
  - SSE 端点
  - 支持筛选：?service_type=flow_worker&instance_id=xxx&level=ERROR
  - 返回实时日志事件
```

- **验收标准**:
  - 能实时查看服务日志
  - 能按级别筛选
- **相关文件**:
  - `plaita-console/backend/api/logs.py`

#### 2.8.3 日志查看页面

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 前端日志查看界面

**功能要求**：
1. 日志列表（虚拟滚动处理大量日志）：
   - 时间戳
   - 级别（带颜色）
   - 服务来源
   - 消息内容
2. 筛选器：
   - 服务类型/实例
   - 日志级别
   - 关键词搜索
   - 时间范围
3. 实时模式开关（SSE）
4. 暂停/继续滚动

- **验收标准**:
  - 能实时查看日志流
  - 能快速定位问题日志
- **相关文件**:
  - `plaita-console/frontend/src/pages/Logs/` (新建)

---

### 2.9 告警通知（P2）

#### 2.9.1 告警规则引擎

- **状态**: [ ] 未开始
- **优先级**: P2
- **描述**: 定义告警规则并触发告警

**功能要求**：
1. 内置规则：
   - 服务离线
   - 执行错误
   - 执行超时
   - 队列积压
2. 告警通知方式：
   - 页面通知（Toast）
   - Webhook（可选）
   - 邮件（可选）

- **验收标准**:
  - 异常发生时能自动告警
  - 能在页面看到告警通知
- **相关文件**:
  - `plaita-console/backend/services/alert_service.py` (新建)
  - `plaita-console/frontend/src/components/Alerts/` (新建)

---

### 2.10 指标仪表盘（P2）

#### 2.10.1 指标收集

- **状态**: [ ] 未开始
- **优先级**: P2
- **描述**: 收集和存储关键指标

**指标列表**：
1. 执行指标：
   - 总执行数
   - 成功/失败比例
   - 平均执行时间
2. 服务指标：
   - 在线服务数
   - 服务健康率
3. 队列指标：
   - 队列长度
   - 处理速率

- **验收标准**:
  - 能查询历史指标数据
  - 能计算统计数据
- **相关文件**:
  - `plaita-console/backend/services/metrics_service.py` (新建)

#### 2.10.2 仪表盘页面

- **状态**: [ ] 未开始
- **优先级**: P2
- **描述**: 可视化指标展示

**功能要求**：
1. 概览卡片：
   - 在线服务数
   - 运行中执行数
   - 今日执行数
   - 错误数
2. 图表：
   - 执行趋势（折线图）
   - 状态分布（饼图）
   - 服务负载（柱状图）
3. 时间范围选择器

- **验收标准**:
  - 能一眼了解系统整体健康状态
  - 能发现趋势变化
- **相关文件**:
  - `plaita-console/frontend/src/pages/Dashboard/` (新建)

---

## 3. 技术架构

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        plaita-console                              │
├────────────────────────────┬────────────────────────────────────┤
│         Frontend           │              Backend               │
│  (React + TypeScript)      │          (Python FastAPI)          │
│                            │                                    │
│  ┌──────────────────────┐  │  ┌──────────────────────────────┐  │
│  │  Pages               │  │  │  API Routes                  │  │
│  │  - Dashboard         │  │  │  - /api/services             │  │
│  │  - Topology          │◄─┼──┤  - /api/executions           │  │
│  │  - Executions        │  │  │  - /api/queues               │  │
│  │  - Logs              │  │  │  - /api/logs                 │  │
│  │  - Queues            │  │  └──────────────────────────────┘  │
│  └──────────────────────┘  │                │                   │
│           │                │                ▼                   │
│           ▼                │  ┌──────────────────────────────┐  │
│  ┌──────────────────────┐  │  │  Services                    │  │
│  │  Components          │  │  │  - TopologyService           │  │
│  │  - FlowViewer        │  │  │  - ControlService            │  │
│  │  - TopologyGraph     │  │  │  - LogService                │  │
│  │  - LogStream         │  │  │  - MetricsService            │  │
│  └──────────────────────┘  │  └──────────────────────────────┘  │
└────────────────────────────┴───────────────┬────────────────────┘
                                             │
                                             ▼
                    ┌────────────────────────────────────────────┐
                    │              Shared Resources              │
                    ├────────────────────────────────────────────┤
                    │  Redis                                     │
                    │  ├── plaita:registry:*    (服务注册)          │
                    │  ├── plaita:control:*     (控制指令)          │
                    │  ├── plaita:logs:*        (日志流)           │
                    │  └── plaita:flow:queue    (任务队列)          │
                    │                                            │
                    │  plaita Storage (Redis/SQLAlchemy)         │
                    │  ├── ExecutionStorage                      │
                    │  └── FlowStorage                           │
                    └────────────────────────────────────────────┘
```

### 3.2 技术选型

| 层级 | 技术 | 说明 |
|-----|------|-----|
| 前端框架 | React 18+ | 组件化开发 |
| 前端构建 | Vite + pnpm | 快速构建 |
| 前端状态 | Zustand / React Query | 轻量状态管理 |
| 前端 UI | Tailwind CSS + shadcn/ui | 现代化 UI |
| 前端图形 | React Flow | 拓扑图和流程图 |
| 后端框架 | FastAPI | 异步 API + SSE |
| 后端存储 | 复用 plaita Storage | Redis / SQLAlchemy |
| 实时通信 | SSE | 单向推送 |
| 容器化 | Docker | 统一部署 |
| 编排 | Docker Compose / K8s | 多环境支持 |

### 3.3 项目结构

```
plaita-console/
├── frontend/
│   ├── src/
│   │   ├── components/        # 可复用组件
│   │   │   ├── FlowViewer/   # 流程可视化
│   │   │   ├── Topology/     # 拓扑图
│   │   │   ├── LogStream/    # 日志流
│   │   │   └── ...
│   │   ├── pages/            # 页面组件
│   │   │   ├── Dashboard/
│   │   │   ├── Executions/
│   │   │   ├── Logs/
│   │   │   └── ...
│   │   ├── hooks/            # 自定义 Hooks
│   │   ├── services/         # API 调用
│   │   ├── stores/           # 状态管理
│   │   └── App.tsx
│   ├── package.json
│   └── vite.config.ts
├── backend/
│   ├── api/                  # API 路由
│   │   ├── services.py
│   │   ├── executions.py
│   │   ├── queues.py
│   │   └── logs.py
│   ├── models/               # 数据模型
│   ├── services/             # 业务逻辑
│   └── main.py
├── docker/
│   ├── Dockerfile.frontend
│   ├── Dockerfile.backend
│   └── docker-compose.yml
└── README.md
```

---

### 2.11 基础设施服务管理（P1）

#### 2.11.1 基础设施服务定义

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 在集群配置中支持管理依赖的基础设施服务（存储、队列等）

**背景说明**：
目前集群配置只管理程序服务（如 flow_worker、delay_service 等），但这些程序依赖底层的基础设施服务：
- Redis（用于状态存储、队列、事件总线）
- Kafka（可选，用于消息队列）
- PostgreSQL/MySQL（可选，用于持久化存储）

这些基础设施服务也应该纳入配置管理，以便：
1. 在拓扑图中展示完整的服务依赖关系
2. 监控基础设施服务的健康状态
3. 支持 Docker 模式下一键启动所有依赖

**功能要求**：
1. 扩展 `cluster_config.yaml` 支持 `infrastructure` 配置节：
   ```yaml
   infrastructure:
     redis:
       display_name: Redis 缓存/队列
       url: redis://localhost:6379/0
       docker:
         image: redis:7-alpine
         ports:
           - "6379:6379"
     kafka:
       display_name: Kafka 消息队列
       enabled: false  # 可选，默认禁用
       bootstrap_servers: localhost:9092
       docker:
         image: confluentinc/cp-kafka:7.5.0
         ports:
           - "9092:9092"
     database:
       display_name: 数据库
       enabled: false
       type: postgresql  # 或 mysql
       url: postgresql://localhost:5432/plaita
       docker:
         image: postgres:15-alpine
         ports:
           - "5432:5432"
   ```

2. 后端解析并管理基础设施配置
3. 支持检测基础设施服务的可用性（健康检查）

- **验收标准**:
  - cluster_config.yaml 支持 infrastructure 配置
  - 后端能读取并返回基础设施配置信息
  - 能检测 Redis 等服务的连接状态
- **相关文件**:
  - `plaita-console/cluster_config.yaml`
  - `plaita-console/backend/services/service_manager.py`
  - `plaita-console/backend/api/cluster.py`

#### 2.11.2 基础设施状态展示

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 在前端展示基础设施服务的配置和状态

**功能要求**：
1. 集群配置页面显示基础设施服务列表：
   - 服务名称和类型
   - 连接 URL
   - 健康状态（可用/不可用）
   - 操作按钮（Docker 模式下可启动/停止）
2. 拓扑图中展示基础设施节点
3. 服务依赖关系可视化

- **验收标准**:
  - 能在界面上看到所有配置的基础设施服务
  - 能看到基础设施的健康状态
- **相关文件**:
  - `plaita-console/frontend/src/pages/Cluster.tsx`
  - `plaita-console/frontend/src/services/api.ts`

---

### 2.12 服务日志快捷查看（P1）

#### 2.12.1 托管实例日志入口

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 在托管实例列表中添加查看日志的快捷入口

**背景说明**：
目前托管实例列表只有"移除"操作，当服务状态异常时，用户无法快速查看该服务的日志。
需要添加"查看日志"按钮，点击后可以快速查看该服务实例的日志。

**功能要求**：
1. 在托管实例表格的操作列添加"查看日志"按钮
2. 支持两种查看方式：
   - **弹窗查看**：在当前页面弹出日志查看窗口
   - **跳转查看**：跳转到日志页面，自动筛选该实例的日志
3. 日志弹窗功能：
   - 显示最近 100 条日志
   - 支持实时刷新（SSE）
   - 支持日志级别筛选
   - 支持关键词搜索
   - 支持复制日志

- **验收标准**:
  - 托管实例列表显示"查看日志"按钮
  - 点击按钮可以查看该实例的日志
  - 日志支持实时更新
- **相关文件**:
  - `plaita-console/frontend/src/pages/Cluster.tsx`
  - `plaita-console/backend/api/logs.py`

#### 2.12.2 服务日志 API 增强

- **状态**: [x] 已完成
- **优先级**: P1
- **描述**: 增强日志 API，支持按服务实例精确筛选

**功能要求**：
1. 优化日志查询接口，确保可以按 instance_id 精确筛选
2. 添加日志统计接口，返回各服务的日志数量分布
3. 支持日志时间范围查询

**API 设计**：
```
GET /api/logs
  - 支持 instance_id 参数精确筛选
  - 支持 start_time / end_time 时间范围筛选
  - 新增 order 参数（asc/desc）

GET /api/logs/stats
  - 返回各服务的日志统计
  - 按服务类型/实例/级别分组
```

- **验收标准**:
  - API 支持按实例精确筛选日志
  - 日志统计接口可用
- **相关文件**:
  - `plaita-console/backend/api/logs.py`

---

## 4. 依赖项

### 4.1 plaita 依赖

本项目依赖 plaita 的以下组件：
- `plaita.storage` - ExecutionStorage, FlowStorage
- `plaita.event` - EventBus
- `plaita.server.flow_worker` - FlowWorker
- `plaita.server.services` - 各扩展服务

### 4.2 需要对 plaita 的修改

| 修改 | 描述 | 优先级 |
|-----|------|-------|
| 服务注册机制 | 在 BaseService 和 FlowWorker 中添加 Redis 注册 | P1 |
| 心跳机制 | 定期刷新注册 TTL | P1 |
| 控制指令监听 | 监听 Redis Pub/Sub 接收控制指令 | P1 |
| 日志输出 | 添加 Redis Stream 日志处理器 | P1 |

---

## 5. 测试要求

### 5.1 后端测试

- 单元测试覆盖率 > 80%
- API 集成测试
- SSE 端点测试

### 5.2 前端测试

- 组件测试（React Testing Library）
- E2E 测试（可选，Playwright）

### 5.3 集成测试

- 完整流程测试：启动服务 -> 拓扑展示 -> 执行流程 -> 监控状态

---

## 6. 更新记录

| 日期 | 更新内容 | 更新人 |
|------|----------|--------|
| 2025-12-30 | 初始版本 | AI Assistant |
| 2025-12-30 | 完成所有 P1 需求开发 | AI Assistant |
| 2025-12-31 | 添加基础设施服务管理功能 (2.11) | AI Assistant |
| 2025-12-31 | 添加服务日志快捷查看功能 (2.12) | AI Assistant |


