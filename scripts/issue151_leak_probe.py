#!/usr/bin/env python3
"""plaita#151 **现状证实探针**（W12-A 交付，只读跑一次，不改产品代码）。

目的：从 #168 判定表的 C 类里挑出**会真泄漏**的代表站点，实证「作者数据 / 凭据形状 /
环境变量值」在修复前以明文进入日志信道。探针自身不修任何东西，只打印并断言
needle（密钥本体 + 任意 ≥3 字符前缀/后缀）出现在捕获到的 `LogRecord` 五个面上。

用法::

    PYTHONPATH=<repo> python scripts/issue151_leak_probe.py

修复前：每个用例都打印 `LEAK`（退出码 1）；修复后：每个用例都打印 `CLEAN`（退出码 0）。
同一探针双向可用——这就是红先证 / 负向对照的载体（把任一已修站点的掩码摘掉，
对应用例立刻回到 `LEAK`）。

覆盖的 C 类站点（#168 判定表）：``core/runner.py:149``（节点执行路径回显作者数据，
issue 点名的核心）+ 7 处「环境变量**值**」回显（``core/expression.py:302/305``、
``server/task_queue.py:48``、``server/flow_worker.py:246/373/505``、
``storage/redis.py:105``）。
"""
from __future__ import annotations

import logging
import os
import sys
import types

# --- 环境值回显：必须在 import 目标模块**之前**注入（模块级常量在导入期解析） ---
SECRET = "Ks9Qm4Zt"
os.environ["PLAITA_DLQ_MAX_LEN"] = f"access_token={SECRET}"          # 非 int ⇒ 走 warning
os.environ["PLAITA_EXECUTION_STATE_TTL_DAYS"] = f"token={SECRET}"    # 非 int ⇒ 走 warning
os.environ["PLAITA_EXPR_MAX_RESULT_SIZE"] = f"?access_token={SECRET}"  # 非 int ⇒ 走 warning

from plaita.core.errors import ErrorHandler, ErrorStrategy  # noqa: E402
from plaita.core.runner import _handle_node_error  # noqa: E402
from plaita.core import expression as expr_mod  # noqa: E402
from plaita.server import flow_worker as fw_mod  # noqa: E402
from plaita.server import task_queue as tq_mod  # noqa: E402
from plaita.storage import redis as redis_mod  # noqa: E402

NEEDLES = sorted(
    {SECRET}
    | {SECRET[:n] for n in range(3, len(SECRET) + 1)}
    | {SECRET[-n:] for n in range(3, len(SECRET) + 1)},
    key=lambda s: (-len(s), s),
)

LOG_NAME = "plaita"  # plaita.logger 的共享 logger（子 logger 全部上浮到此）


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    #: 五个记录面（#158/#161 口径）：msg / args / getMessage / __dict__ / exc_text
    def surfaces(self) -> str:
        chunks: list[str] = []
        fmt = logging.Formatter("%(levelname)s %(name)s %(message)s")
        for record in self.records:
            chunks.append(str(record.msg))
            chunks.append(repr(record.args))
            chunks.append(record.getMessage())
            for key, value in sorted(vars(record).items()):
                chunks.append(f"{key}={value!r}")
            chunks.append(fmt.format(record))
        return "\n".join(chunks)


def _run(label: str, logger_name: str, site: str, act) -> bool:
    cap = _Capture()
    target = logging.getLogger(logger_name)
    prev_level, prev_prop = target.level, target.propagate
    target.setLevel(logging.DEBUG)
    target.addHandler(cap)
    try:
        act()
    finally:
        target.removeHandler(cap)
        target.setLevel(prev_level)
        target.propagate = prev_prop
    text = cap.surfaces()
    hits = [n for n in NEEDLES if n in text]
    leaked = bool(hits)
    tag = "LEAK  " if leaked else "CLEAN "
    print(f"[{tag}] {label:22s} {site}")
    if leaked:
        print(f"         命中 needle: {hits[:4]}（共 {len(hits)} 个）")
        for line in text.splitlines():
            if any(n in line for n in hits):
                print("         记录面: " + line.strip()[:160])
                break
    else:
        print(f"         记录面（无 needle）: {text.strip().splitlines()[-1][:160] if text.strip() else '(无记录)'}")
    return leaked


def main() -> int:
    results: dict[str, bool] = {}

    # ① 节点执行路径：core/runner.py:149 -- 被 errorHandler 兜住的错误，e 的文本回显
    def _runner_path() -> None:
        node = types.SimpleNamespace(id="n1", name="n1")
        exc = ValueError(f"node payload rejected: http://127.0.0.1:1/x?access_token={SECRET}")
        _handle_node_error(None, node, ErrorHandler(strategy=ErrorStrategy.CONTINUE), exc)

    results["runner_error_text"] = _run(
        "节点执行路径", LOG_NAME, "core/runner.py:149", _runner_path
    )

    # ② 环境投影：server/task_queue.py:48
    results["env_dlq"] = _run(
        "env 投影", LOG_NAME, "server/task_queue.py:48",
        tq_mod._dlq_max_len_from_env,
    )

    # ③ 环境投影：storage/redis.py:105
    results["env_ttl"] = _run(
        "env 投影", LOG_NAME, "storage/redis.py:105",
        redis_mod.execution_state_ttl_seconds,
    )

    # ④ 环境投影：core/expression.py:302
    results["env_expr"] = _run(
        "env 投影", LOG_NAME, "core/expression.py:302",
        lambda: expr_mod._resolve_positive_int("PLAITA_EXPR_MAX_RESULT_SIZE", 1000),
    )

    # ⑤⑥⑦ 环境投影：server/flow_worker.py:246 / 373 / 505
    results["env_metrics_port"] = _run(
        "env 投影", LOG_NAME, "server/flow_worker.py:246",
        lambda: (os.environ.__setitem__("PLAITA_METRICS_PORT", f"Bearer {SECRET}"),
                 fw_mod._metrics_port_from_env())[1],
    )
    results["env_disk"] = _run(
        "env 投影", LOG_NAME, "server/flow_worker.py:373",
        lambda: (os.environ.__setitem__("PLAITA_WORKER_MIN_FREE_DISK_GIB", f"token={SECRET}"),
                 fw_mod._min_free_disk_gib_from_env())[1],
    )
    results["env_drain"] = _run(
        "env 投影", LOG_NAME, "server/flow_worker.py:505",
        lambda: (os.environ.__setitem__("PLAITA_WORKER_DRAIN_TIMEOUT", f"password={SECRET}"),
                 fw_mod._drain_timeout_from_env())[1],
    )

    leaked = {k: v for k, v in results.items() if v}
    print()
    print(f"探针结果：{len(leaked)}/{len(results)} 个用例把明文写进了日志记录面")
    for key in leaked:
        print(f"  - {key}")
    return 1 if leaked else 0


if __name__ == "__main__":
    sys.exit(main())
