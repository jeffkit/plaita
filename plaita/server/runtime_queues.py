"""runtime → 派发队列解析（plaita#300）的**单源真值**，落在 server 层。

## 为什么在 plaita/server 而不是 console

#300 第七十五轮收口调度触发面时发现：runtime pool 队列解析原本只在
console 后端（``plaita-console/backend/services/runtime_pools.py``），而
cron 调度循环是独立 plaita server 进程（无 console store、不允许反向
import console）——触发面要按 flow 的 runtime 选池就必须共享这一份解析。
依赖方向（core ← server ← console）决定真值只能放这里；console 侧模块
保留为薄再导出（既有 import 面不动），两侧不存在第二份映射逻辑。

## 池模型

- ``python``：历史池，队列经 ``plaita.runtime_config.resolve_queue_name("console")``
  解析（``PLAITA_CONSOLE_TASK_QUEUE`` 覆盖继续生效）——存量部署零行为变化；
- ``go``：``PLAITA_CONSOLE_GO_TASK_QUEUE`` 覆盖，缺省 ``plaita:flow:queue:go``
  （Go worker 以 ``PLAITA_QUEUE_NAME`` 指到同一队列，部署侧对齐）；
- ``rust``：plaita-rs worker 池（plaita-rs#17 在建）；队列暂走通用推导
  ``plaita:flow:queue:rust``（``PLAITA_CONSOLE_RUNTIME_QUEUES`` 可显式覆盖），
  不设专用 env——等 worker 落地后按实际部署再定；
- 任意其他 runtime：env ``PLAITA_CONSOLE_RUNTIME_QUEUES``（JSON 映射）显式
  给队列，未给的键按 ``plaita:flow:queue:{runtime}`` 推导。

``target_instance_id`` 语义不变：同池内的容量路由（capacity/dispatcher），
池间隔离由「不同 runtime → 不同 Stream」表达，不改 queue/lease 协议。
"""
from __future__ import annotations

import json
import os
from typing import Optional

#: 合法 runtime 值（flow_versions.runtime 与调度行 runtime 冗余的取值域；
#: 保存/触发路径据此 422 或具名 WARNING）。
VALID_RUNTIMES = ("go", "python", "rust")

#: 默认 runtime（存量版本/旧调度行的迁移值；历史队列语义即 Python worker 池）。
DEFAULT_RUNTIME = "python"

GO_QUEUE_ENV = "PLAITA_CONSOLE_GO_TASK_QUEUE"
RUNTIME_QUEUES_ENV = "PLAITA_CONSOLE_RUNTIME_QUEUES"

DEFAULT_GO_QUEUE = "plaita:flow:queue:go"


def normalize_runtime(runtime: Optional[str]) -> str:
    """校验/归一 runtime 值；``None``/空 → 默认 ``python``，非法值抛 ``ValueError``。"""
    value = str(runtime or "").strip() or DEFAULT_RUNTIME
    if value not in VALID_RUNTIMES:
        raise ValueError(
            f"非法 runtime {value!r}；可选值: {list(VALID_RUNTIMES)}"
        )
    return value


def _env_runtime_queues() -> dict:
    raw = os.environ.get(RUNTIME_QUEUES_ENV, "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def resolve_runtime_queue(runtime: Optional[str]) -> str:
    """runtime → 派发队列名（使用点解析 env，模块导入期零求值，A56-02 同纪律）。"""
    value = normalize_runtime(runtime)
    if value == DEFAULT_RUNTIME:
        from plaita.runtime_config import resolve_queue_name

        return resolve_queue_name("console")
    explicit = _env_runtime_queues().get(value)
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    if value == "go":
        go_queue = os.environ.get(GO_QUEUE_ENV, "").strip()
        if go_queue:
            return go_queue
        return DEFAULT_GO_QUEUE
    return f"plaita:flow:queue:{value}"


__all__ = [
    "DEFAULT_GO_QUEUE",
    "DEFAULT_RUNTIME",
    "GO_QUEUE_ENV",
    "RUNTIME_QUEUES_ENV",
    "VALID_RUNTIMES",
    "normalize_runtime",
    "resolve_runtime_queue",
]
