#!/usr/bin/env python3
"""沙箱孤儿回收守护（沙箱资源生命周期）—— **控制面入口**。

## 为什么这个入口在 plaita，而不在 plaita-nodes

2026-10-13 的职责边界评审（见大仓 `docs/AGS_RESPONSIBILITY_BOUNDARY.md`）确定：

- **plaita-nodes 保留"沙箱语义与机制"**：`decide()`（什么算孤儿/僵尸/合法挂起）、
  `reap_one()`、`enumerate_resources()`、`run_cycle()` —— 这些**任何宿主都能复用**；
- **plaita 承接"调度与部署绑定"**：本文即其一 ——
  「**什么时候跑**」「**从哪拿 execution 状态**（连哪个 Redis）」是**部署相关**的，
  不该由通用节点仓承担。

原先这段（`build_storage` + `main`）位于 `plaita_nodes/sandbox_reaper.py`，
其中 `from plaita.storage.redis import RedisExecutionStorage` 让
**通用节点仓反向依赖了具体部署的存储后端**。现搬到这里。

## 与 `flow_worker._paused_sweeper` 的关系（重要）

plaita 侧**已有**一个清扫线程（`flow_worker._paused_sweeper`），它调
**driver 的 `sweep_paused()`**，覆盖的是「**暂停实例**」这一类。

本脚本调用的是 plaita-nodes 的 `run_cycle()`，覆盖**更广**：
`reap-unknown` / `reap-zombie` / `reap-terminal`（并**明确跳过** `skip-suspended`
= 等人审批的合法挂起，`skip-fresh` = 还新鲜）。

⇒ **二者不是重复，是覆盖面不同**：
| 路径 | 覆盖 |
|---|---|
| `flow_worker._paused_sweeper`（常驻线程） | 暂停实例 |
| 本脚本（周期性/人工） | 全部资源类别（含僵尸、终态遗留） |

## ⚠ 已知缺口（2026-10-13 实测，尚未修）

`plaita_nodes.sandbox_reaper.enumerate_resources()` 通过
`sandbox_docker` / `sandbox_krunvm` / `sandbox_ssh` 的 `enumerate_workspaces()` 枚举资源，
**而 `sandbox_ags` 没有实现 `enumerate_workspaces`** ⇒
**AGS 的孤儿不会被本脚本回收**（AGS 目前只有 `sweep_paused`，即上表第一行）。

⇒ 若要覆盖 AGS，需给 `sandbox_ags` 补 `enumerate_workspaces`
（AGS 侧可用 `GET /v2/sandboxes` 列实例），并把它纳入这里的 `--drivers`。

用法::

    # 单次巡检（dry-run 先看决策）
    python scripts/reap_sandboxes.py --redis-url redis://… --drivers docker,krunvm --dry-run
    # 常驻（每 300s 一轮）
    python scripts/reap_sandboxes.py --redis-url redis://… --interval 300
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Optional, Sequence

logger = logging.getLogger("plaita.reap_sandboxes")


def build_storage(redis_url: Optional[str]):
    """按 URL 构造 ExecutionStorage；未给 URL 返回 None（全部按无主处理）。

    这一句就是**原先放在 plaita-nodes 的那个边界违规** ——
    它让通用节点仓知道了本部署使用的 Redis 后端，故搬到 plaita 侧。
    """
    if not redis_url:
        return None
    from redis import Redis  # noqa: PLC0415  lazy：plaita[redis]
    from plaita.storage.redis import RedisExecutionStorage  # noqa: PLC0415
    return RedisExecutionStorage(client=Redis.from_url(
        redis_url,
        # A2R20-05：CLI 运维路径同样不得无限阻塞（worker 口径）。
        socket_connect_timeout=5.0,
        socket_timeout=30.0,
    ))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python scripts/reap_sandboxes.py",
        description="沙箱孤儿回收守护（status-aware）")
    parser.add_argument("--redis-url", default=None,
                        help="ExecutionStorage 后端（缺省无存储=全部按无主处理）")
    parser.add_argument("--drivers", default="docker,krunvm",
                        help="逗号分隔的 driver 列表（默认 docker,krunvm；"
                             "注意 ags 暂无 enumerate_workspaces，见模块 docstring）")
    parser.add_argument("--data-root", default=None,
                        help="krunvm 数据根覆盖（默认 ~/.plaita/sandbox-data）")
    parser.add_argument("--stale-minutes", type=float, default=30.0,
                        help="running 判僵尸的阈值（默认 30 分钟）")
    parser.add_argument("--interval", type=float, default=0.0,
                        help="巡检间隔秒；0=单次巡检退出")
    parser.add_argument("--keep-data", action="store_true", default=True)
    parser.add_argument("--purge-data", dest="keep_data", action="store_false",
                        help="终态/僵尸回收时连数据层一起清（默认保留）")
    parser.add_argument("--dry-run", action="store_true", help="只打印决策，不动作")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s")

    # 沙箱语义全部来自 plaita-nodes（通用、可复用）
    try:
        from plaita_nodes.sandbox_reaper import enumerate_resources, run_cycle
    except ImportError as exc:  # pragma: no cover - 参数面防御
        logger.error("未安装 plaita-nodes（或版本过旧）：%s", exc)
        return 3

    storage = build_storage(args.redis_url)
    if args.redis_url and storage is None:  # pragma: no cover - 参数面防御
        logger.error("--redis-url 提供但存储构建失败")
        return 2
    if storage is None:
        logger.warning("未提供 --redis-url：无执行状态可查，所有资源按无主处理")

    drivers = [d.strip() for d in args.drivers.split(",") if d.strip()]
    while True:
        resources = enumerate_resources(drivers, data_root=args.data_root)
        results = run_cycle(storage, resources, stale_minutes=args.stale_minutes,
                            keep_data=args.keep_data, dry_run=args.dry_run)
        reaped = sum(1 for r in results if r["decision"].startswith("reap"))
        logger.info("巡检完成：资源 %d，处置 %d%s", len(results), reaped,
                    "（dry-run）" if args.dry_run else "")
        if args.interval <= 0:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
