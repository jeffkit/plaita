# 安装

plaita 要求 Python **3.10+**（推荐 3.10）。

## 从 PyPI 安装

```bash
pip install plaita
```

## 从源码安装

```bash
git clone <repository_url>
cd plaita
pip install .
```

## 验证安装

```bash
python3 -m plaita
```

若输出版本号（当前 **0.5.0**）即安装成功。

## 可选依赖（extras）

plaita 核心极轻，只依赖 `pydantic`、`pyparsing`、`isodate`。其余能力按需安装对应的 extra：

| extra | 安装命令 | 提供能力 |
|-------|---------|---------|
| `redis` | `pip install plaita[redis]` | Redis 存储 / Redis EventBus / Redis 节点 |
| `server` | `pip install plaita[server]` | FastAPI 服务端、SQLAlchemy 存储、FlowWorker、扩展节点 |
| `code` | `pip install plaita[code]` | `CodeNode` 的 JavaScript 执行（PyExecJS） |
| `http` | `pip install plaita[http]` | `HTTP` 节点（requests + aiohttp） |
| `all` | `pip install plaita[all]` | 上述全部 |

当你尝试使用未安装 extra 的功能时，plaita 会抛出**可操作的 `ImportError`**，明确提示应安装哪个 extra，例如：

```
ImportError: The 'http' extra is required for this feature but is not installed.
Install it with: pip install plaita[http]
```

## 开发依赖

贡献代码或本地构建文档时安装：

```bash
pip install plaita[dev]    # pytest / pytest-asyncio / fakeredis / pytest-cov
pip install plaita[lint]   # mypy / flake8 / black（可选）
```

详见 [配置与可选依赖](../reference/configuration.md)。
