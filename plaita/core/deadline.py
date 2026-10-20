"""执行级 deadline（plaita#267）——单次执行的总墙钟预算。

背景：``Flow.timeout`` 缺省时引擎完全无限额。共享 worker 上一个执行可以被
``requestTimeout=600`` × ``maxRedirects=20`` × ``retryTimes=10`` 的合法配置
拖住 38.6 小时（issue #267）。本模块给**未显式配置**的执行补一个具名总预算：

- 常量 :data:`DEFAULT_EXECUTION_DEADLINE_SECONDS`（1800s，与运维 runbook
  「末节点停滞 >1800s 判死」同口径）；
- 环境变量 ``PLAITA_EXECUTION_DEADLINE_SECONDS`` 可调（运维显式放宽）；
- 显式 ``flow.timeout`` / ``execution.timeout`` / ``run_distributed(timeout=...)``
  仍然优先（更严或更宽都由调用方决定），本模块只在**全部缺省**时兜底。

非法 env（非数字 / 非有限 / ``<= 0``）绝不静默变成「无限额」：回退默认预算
并打 WARNING——降级必须可观测（AGENTS.md 开发约定）。
"""
from __future__ import annotations

import logging
import math
import os
from typing import Optional

from plaita.core.url_redaction import _redact_env_value

logger = logging.getLogger("plaita.core.deadline")

#: 未配置任何 timeout 时的执行总预算（秒）。与 ops-runbook 的 1800s 停滞判死
#: 同口径；显式配置（flow/execution/调用期）优先于本默认。
DEFAULT_EXECUTION_DEADLINE_SECONDS = 1800.0

#: 运维级覆盖：秒（支持小数）。``<=0`` / 非数字 / 非有限一律回退默认并告警。
EXECUTION_DEADLINE_ENV = "PLAITA_EXECUTION_DEADLINE_SECONDS"


def default_execution_deadline_ms() -> int:
    """解析运维级默认执行预算（毫秒）；非法值回退默认并留 WARNING。"""
    default_ms = int(DEFAULT_EXECUTION_DEADLINE_SECONDS * 1000)
    raw = (os.environ.get(EXECUTION_DEADLINE_ENV) or "").strip()
    if not raw:
        return default_ms
    try:
        seconds = float(raw)
    except ValueError:
        logger.warning(
            "%s=%s 不是数字，回退默认执行 deadline %ss（未静默变成无限额）",
            EXECUTION_DEADLINE_ENV, _redact_env_value(raw),
            DEFAULT_EXECUTION_DEADLINE_SECONDS,
        )
        return default_ms
    if not math.isfinite(seconds) or seconds <= 0:
        logger.warning(
            "%s=%s 必须是正有限数（秒），回退默认执行 deadline %ss",
            EXECUTION_DEADLINE_ENV, _redact_env_value(raw),
            DEFAULT_EXECUTION_DEADLINE_SECONDS,
        )
        return default_ms
    return int(seconds * 1000)


def execution_deadline_ms(configured_ms: Optional[int]) -> int:
    """执行级总预算：显式配置优先，缺省回退 :func:`default_execution_deadline_ms`。

    ``0`` 是**显式零预算**（最严），不得被当成缺省（plaita#138 契约）。
    """
    if configured_ms is not None:
        return configured_ms
    return default_execution_deadline_ms()
