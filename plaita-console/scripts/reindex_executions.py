#!/usr/bin/env python
"""显式重建执行列表索引：状态键 → ``{ns}:execution:index`` ZSET（A2R25-07）。

背景
====
``RedisExecutionStorage.list_executions`` 依赖执行索引 ZSET；存量回填
（``index:ready``）只在就绪标记缺失时跑一次。灾难恢复若只把执行状态键
SET 回来（或先恢复索引、后恢复状态），就绪标记仍在，回填不会再跑——直读
状态 200，但 console 列表看不到该执行。

读取路径（``load_execution_state``）已做成员自愈；本脚本是**全量**兜底：
清掉索引与就绪标记后按 ``{ns}:execution:*`` 状态键重新回填，适用于
「索引与状态键已分叉、且不确定哪些执行被读取过」的恢复现场。

用法
====
    PLAITA_CONSOLE_REDIS_URL=redis://127.0.0.1:6379/1 \\
    python plaita-console/scripts/reindex_executions.py --tenant acme
    # 或按 console DB 里的全部租户重建（需要 PLAITA_CONSOLE_DB_URL）
    PLAITA_CONSOLE_REDIS_URL=... PLAITA_CONSOLE_DB_URL=... \\
    python plaita-console/scripts/reindex_executions.py --all-tenants

退出码：0 = 全部成功；1 = 至少一个租户重建失败；2 = 参数/依赖错误。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
for _p in (str(_REPO_ROOT), str(_BACKEND_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _all_tenant_ids() -> list[str]:
    """从 console DB 读租户 id（与列表 API 同源）。"""
    try:
        from services import flow_store, tenants_svc
    except ImportError:
        from plaita_console.services import flow_store, tenants_svc  # type: ignore
    try:
        from config import get_settings
    except ImportError:
        from plaita_console.config import get_settings  # type: ignore
    settings = get_settings()
    flow_store.init_engine(settings.db_url)
    store = flow_store.get_flow_store()
    return [t["id"] for t in tenants_svc.list_tenants(store)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="重建执行列表索引（A2R25-07 恢复工具）"
    )
    parser.add_argument(
        "--redis-url",
        default=(
            os.environ.get("PLAITA_CONSOLE_REDIS_URL")
            or os.environ.get("REDIS_URL")
        ),
        help="引擎 Redis URL（默认读 PLAITA_CONSOLE_REDIS_URL / REDIS_URL）",
    )
    parser.add_argument(
        "--tenant",
        action="append",
        default=[],
        help="租户 slug；可重复。缺省只重建 default",
    )
    parser.add_argument(
        "--all-tenants",
        action="store_true",
        help="从 console DB 枚举全部租户（需要 PLAITA_CONSOLE_DB_URL）",
    )
    args = parser.parse_args()

    if not args.redis_url:
        print(
            "[FATAL] 需要 --redis-url 或 PLAITA_CONSOLE_REDIS_URL / REDIS_URL",
            file=sys.stderr,
        )
        return 2

    try:
        from plaita.redis_client import make_redis_client
        from plaita.server.tenant_context import tenant_namespace
        from plaita.storage.redis import RedisExecutionStorage
    except ImportError as exc:
        print(f"[FATAL] 无法导入 plaita/redis（{exc}）；请用已装依赖的 venv 运行",
              file=sys.stderr)
        return 2

    tenants = list(args.tenant)
    if args.all_tenants:
        try:
            tenants = _all_tenant_ids()
        except Exception as exc:  # noqa: BLE001 — 参数面错误：loud + 退出码
            print(f"[FATAL] 枚举租户失败（{exc}）；可改用 --tenant", file=sys.stderr)
            return 2
    if not tenants:
        tenants = ["default"]

    # A2R20-05：不裸 from_url，连接/读超时由仓内统一声明。
    client = make_redis_client(args.redis_url, decode_responses=True)
    failures = 0
    for tenant in tenants:
        namespace = tenant_namespace(tenant)
        try:
            count = RedisExecutionStorage(
                client=client, namespace=namespace
            ).rebuild_execution_index()
        except Exception as exc:  # noqa: BLE001 — 单租户失败不中断其余租户
            failures += 1
            print(f"[FAIL] tenant={tenant} namespace={namespace}: {exc}",
                  file=sys.stderr)
            continue
        if count < 0:
            failures += 1
            print(f"[FAIL] tenant={tenant} namespace={namespace}: 重建失败（见日志）",
                  file=sys.stderr)
        else:
            print(f"[OK] tenant={tenant} namespace={namespace}: indexed={count}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
