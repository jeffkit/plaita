"""保留期常量与环境解析（A69-04）。

只依赖 stdlib 与 core 内的日志/脱敏工具，**不 import storage/server**——分层
方向是 ``core`` ← 其余，墓碑 resolver（storage）与派发闸（server）都从这里
取同一个 ``task_max_age_seconds``，避免两份 env 解析口径分叉。

语义（与 ``docs-site/docs/distributed/ops-runbook.md`` 同步）：

- ``PLAITA_TASK_MAX_AGE_SECONDS`` 缺省 = 30 天；
- 非法值 → 回退默认 + WARNING；
- ``<=0`` = **关闭年龄闸**（存量语义，启动校验会 WARNING 提示「墓碑到期后
  没有年龄兜底」）。
"""
from __future__ import annotations

import os

from plaita.core.url_redaction import _redact_env_value
from plaita.logger import logger

#: start/resume 载体默认最大年龄（30 天），与墓碑默认 TTL 同 horizon。
DEFAULT_TASK_MAX_AGE_SECONDS = 30 * 86400


def task_max_age_seconds() -> int:
    """消息最大年龄（秒）；未配置/非法回退 30 天，``<=0`` 关闭。"""
    raw = (os.environ.get("PLAITA_TASK_MAX_AGE_SECONDS") or "").strip()
    if not raw:
        return DEFAULT_TASK_MAX_AGE_SECONDS
    try:
        seconds = int(raw)
    except ValueError:
        logger.warning(
            "PLAITA_TASK_MAX_AGE_SECONDS=%s 非法，回退默认 %d 秒",
            _redact_env_value(raw),
            DEFAULT_TASK_MAX_AGE_SECONDS,
        )
        return DEFAULT_TASK_MAX_AGE_SECONDS
    return seconds if seconds > 0 else 0
