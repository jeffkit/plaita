# AI 开发助手指南

本文档是给 AI 开发助手（如 Claude、GPT 等）的开发指南，说明如何根据 `requirements/` 目录下的需求文档进行开发。

---

## 1. 开发流程

### 1.1 理解需求

1. **首先阅读** `requirements/README.md` 了解需求文档的组织方式
2. **扫描所有需求文档**，找到状态为 `[ ]` 未开始 或 `[~]` 进行中的任务
3. **按优先级排序**：P0 > P1 > P2 > P3
4. **理解上下文**：阅读相关的已完成需求，了解整体设计

### 1.2 选择任务

1. 优先处理 P0（必须完成）的任务
2. 同一优先级内，按文档中的顺序处理
3. 如果任务有依赖，先完成依赖项
4. 一次只专注一个任务

### 1.3 实现任务

```
开始 -> 阅读需求 -> 阅读相关代码 -> 设计方案 -> 实现代码 -> 测试 -> 更新文档 -> 提交
```

### 1.4 验证完成

1. 运行相关测试，确保通过
2. 确保没有引入新的 linter 错误
3. 更新需求文档中的状态标记
4. 提交代码并更新需求文档

---

## 2. 代码规范

### 2.1 Python 代码风格

```python
# 使用 Python 3.10+ 语法
# 使用 Pydantic 进行数据验证
# 使用 async/await 进行异步编程
# 使用 logging 进行日志记录，避免 print

from typing import Optional, Dict, List, Any
from pydantic import BaseModel, Field
from plaita.logger import logger

class MyClass(BaseModel):
    """类文档字符串 - 描述类的用途"""
    
    field: str = Field(..., description="字段描述")
    optional_field: Optional[int] = None
    
    def my_method(self, param: str) -> Dict[str, Any]:
        """
        方法文档字符串
        
        Args:
            param: 参数描述
            
        Returns:
            返回值描述
        """
        logger.info(f"Processing: {param}")
        return {"result": param}
```

### 2.2 测试规范

```python
import pytest
import pytest_asyncio

@pytest.mark.asyncio
async def test_feature():
    """测试功能描述"""
    # Arrange - 准备测试数据
    input_data = {"key": "value"}
    
    # Act - 执行被测试的功能
    result = await some_function(input_data)
    
    # Assert - 验证结果
    assert result is not None
    assert result["status"] == "success"
```

### 2.3 提交信息规范

```
<type>(<scope>): <subject>

<body>

<footer>
```

类型：
- `feat`: 新功能
- `fix`: Bug 修复
- `docs`: 文档更新
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建/工具相关

示例：
```
feat(event): implement Redis event storage

- Add RedisEventStorage class
- Implement publish/subscribe methods
- Add unit tests

Closes #123
```

---

## 3. 项目结构

```
plaita/
├── plaita/                   # 主代码目录
│   ├── __init__.py         # 包入口；通过 __getattr__ 懒 re-export 公共 API
│   ├── core/               # 执行核心层（不依赖 event/storage/server）
│   │   ├── flow.py         # Flow 模型、parse()、parse_and_run()
│   │   ├── executor.py     # FlowExecution、ExecutionMode（同步/分布式执行）
│   │   ├── runner.py       # 节点运行器与执行策略
│   │   ├── callback.py     # 回调管理（FlowCallback/FlowEvent/CallbackManager）
│   │   ├── context.py      # 执行上下文与默认 event-bus provider
│   │   ├── expression.py   # 表达式求值与函数注册表
│   │   ├── types.py        # 类型定义（STRING/BOOL/...）
│   │   ├── errors.py       # 异常与错误处理（FlowExecutionException 等）
│   │   └── async_utils.py  # 异步工具
│   ├── flow.py             # 兼容 shim（转发到 plaita.core.*，导入时 DeprecationWarning）
│   ├── errors.py           # 兼容 shim（转发到 plaita.core.errors）
│   ├── types.py            # 兼容 shim（转发到 plaita.core.types）
│   ├── io.py               # 输入输出与表达式求值
│   ├── logger.py           # 共享 logger（仅 NullHandler，不强制级别/输出）
│   ├── client.py           # 远程客户端
│   ├── node/               # 节点实现与 NodeRegistry
│   │   ├── basic.py        # Node 基类
│   │   ├── start.py / end.py
│   │   ├── event_node.py   # 事件节点基类
│   │   └── ...
│   ├── event/              # 事件系统
│   │   ├── core.py         # EventBus 抽象与接口
│   │   ├── memory.py       # 内存实现
│   │   ├── redis.py        # Redis 实现（需 redis extra）
│   │   └── sqlalchemy.py   # 数据库实现（需 server extra）
│   ├── storage/            # 状态存储
│   │   ├── base.py / memory.py / redis.py / sqlalchemy.py
│   └── server/             # 服务端功能（需 server extra）
│       ├── nodes/          # 扩展节点（delay/redis_queue/kafka_queue/http_callback/approval）
│       ├── services/       # 外延服务
│       └── flow_worker.py  # 分布式流程工作器
├── tests/                  # 测试目录（unit / integration / e2e）
├── docs/                   # 文档目录
├── requirements/           # 需求文档目录
└── pyproject.toml          # 依赖与构建配置（权威来源）
```

> 公共 API 推荐从顶层导入：`from plaita import Flow, Node, parse, node_register, types`，等价于从 `plaita.core.*` / `plaita.node` 懒加载。旧的 `plaita.flow` / `plaita.errors` / `plaita.types` 仍可用但会触发 `DeprecationWarning`。

---

## 4. 常用命令

### 4.1 运行测试

```bash
# 运行所有测试
python -m pytest tests/ -v

# 运行特定测试文件
python -m pytest tests/test_flow.py -v

# 运行特定测试函数
python -m pytest tests/test_flow.py::test_function_name -v

# 运行单元测试 / 集成测试 / E2E
python -m pytest tests/unit/ -v
python -m pytest tests/integration/ -v
python -m pytest tests/e2e/ -v

# 运行带覆盖率的测试
python -m pytest tests/ --cov=plaita --cov-report=html
```

### 4.2 代码检查（可选工具）

类型检查与风格工具不在默认依赖中，按需安装：

```bash
# 安装开发工具
pip install logic-plaita[dev]      # pytest / pytest-asyncio / fakeredis / pytest-cov
pip install logic-plaita[lint]     # mypy / flake8 / black（可选的静态检查与格式化）

# 类型检查
python -m mypy plaita/

# 代码风格检查
python -m flake8 plaita/

# 格式化代码
python -m black plaita/
```

### 4.3 运行演示

```bash
# 事件系统演示（内存后端）
python -m plaita.event.demo_eventbus --backend memory

# 扩展节点演示
python -m plaita.server.extended_nodes_demo
```

---

## 5. 开发任务模板

当开始一个新任务时，请按以下步骤进行：

### 步骤 1: 阅读需求

```markdown
任务: [任务名称]
优先级: [P0/P1/P2/P3]
状态: [~] 进行中
相关文件: [文件列表]
```

### 步骤 2: 理解现有代码

1. 阅读相关文件的代码
2. 理解接口和数据结构
3. 查看相关测试用例

### 步骤 3: 设计方案

1. 确定实现方式
2. 考虑边界情况
3. 规划测试策略

### 步骤 4: 实现代码

1. 编写代码
2. 添加必要的注释和文档字符串
3. 处理错误情况

### 步骤 5: 编写测试

1. 编写单元测试
2. 覆盖正常和异常情况
3. 运行测试确保通过

### 步骤 6: 更新文档

1. 更新需求文档状态
2. 更新相关代码文档
3. 如有需要，更新 README

### 步骤 7: 提交代码

```bash
git add <files>
git commit -m "feat(module): description"
```

---

## 6. 特别注意事项

### 6.1 异步编程

- 所有 I/O 操作应使用 `async/await`
- 使用 `asyncio.create_task` 并发执行独立任务
- 注意正确处理异步资源的生命周期

### 6.2 错误处理

- 使用项目定义的异常类型（如 `FlowExecutionException`）
- 提供有意义的错误信息
- 记录错误日志

### 6.3 测试

- 使用 `@pytest.mark.asyncio` 标记异步测试
- 使用 fixture 管理测试资源
- 测试应该是独立的，不依赖执行顺序

### 6.4 兼容性

- 保持向后兼容
- 使用 Pydantic 的 `alias` 支持多种字段名格式
- 注意 Python 版本兼容性（3.10+）

---

## 7. 常见问题

### Q: 如何确定任务的优先级？

A: 查看需求文档中的优先级标记：
- P0: 阻塞其他功能，必须立即处理
- P1: 当前迭代的核心功能
- P2: 重要但不紧急的功能
- P3: 可以延后的改进

### Q: 如何处理需求不明确的情况？

A: 
1. 查看相关的已完成需求了解上下文
2. 阅读相关代码理解现有实现
3. 如果仍不确定，记录问题并向用户确认

### Q: 如何处理发现的 Bug？

A:
1. 如果 Bug 简单且在当前任务范围内，直接修复
2. 如果 Bug 复杂，创建新的需求文档记录
3. 始终添加测试用例防止回归

### Q: 如何处理代码重构需求？

A:
1. 确保有充分的测试覆盖
2. 小步重构，每步都能通过测试
3. 分离重构和功能修改的提交

---

## 8. 示例：完成一个任务

以下是完成 "完善 FlowExecution._run_distributed() 实现" 任务的示例：

### 8.1 阅读需求

```
从 requirements/checkpoint-requirements.md 中找到:
- 任务: 完善 FlowExecution._run_distributed() 实现
- 优先级: P0
- 当前状态: [~] 进行中
- 相关文件: plaita/core/executor.py, plaita/core/flow.py
```

### 8.2 阅读相关代码

```python
# 阅读 plaita/core/executor.py 中的 FlowExecution 与分布式执行策略
# 阅读 plaita/node/event_node.py 了解事件节点
# 阅读 plaita/server/flow_worker.py 了解流程恢复
```

### 8.3 实现方案

```python
def _run_distributed(self, flow, params, timeout, context, **options):
    """
    分布式执行模式
    
    1. 如果 context 为空，从头开始执行
    2. 执行到事件节点时，保存上下文并挂起
    3. 如果 context 存在，从上次挂起点恢复执行
    """
    # 实现代码...
```

### 8.4 更新需求文档

```markdown
#### 2.6.1 分布式执行模式
- **状态**: [x] 已完成  # 更新状态
```

### 8.5 提交代码

```bash
git add plaita/core/executor.py requirements/checkpoint-requirements.md
git commit -m "feat(flow): implement _run_distributed for checkpoint support

- Add context serialization
- Implement suspend and resume logic
- Integrate with EventNode
- Add unit tests

Refs: checkpoint-requirements.md 2.6.1"
```

---

*本文档最后更新: 2026-06-30*

