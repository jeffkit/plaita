#!/usr/bin/env python
"""回填已发布流程定义：console PG（权威库）→ 引擎 Redis 流程存储（worker 读的那份）。

背景
====
console 迁移到 PostgreSQL 后，流程定义存 console 自己的 PG（``flows`` /
``flow_versions``，status=published）。FlowWorker 不走 PG——它从引擎的 Redis
流程存储（``plaita.storage.redis.RedisFlowStorage``）解析定义。发布时的同步
钩子（``plaita-console/backend/services/engine_sync.py::sync_flow_to_engine``）
只在「发布那一刻」写引擎存储；**迁移后的历史已发布版本从未重跑发布**，于是
Redis 里是空的，worker 启动任何历史流程都报「找不到流程定义」。

本脚本把 PG 里**全部** status=published 的版本批量回填进引擎 Redis 存储，
一次性补齐迁移缺口。与发布钩子共用 ``sync_flow_to_engine``，键空间与 worker
读取端严格一致（多租户：``{ns}:flow:{flow_id}:{version}``，default/空租户
namespace 为历史前缀 ``plaita``）。

幂等
====
``save_flow`` 是 SET + SADD（覆盖写），重复跑只会把同样的定义再写一遍，
不产生脏数据、不报错。末尾按 written/skipped/error 汇总，可安全定时重跑。

用法
====
    PLAITA_CONSOLE_DB_URL=postgresql+psycopg://... \\
    PLAITA_CONSOLE_REDIS_URL=redis://:pw@127.0.0.1:6379/1 \\
    python plaita-console/scripts/sync_published_to_engine.py

参数化（均从 env 读，便于远端跑）：
    PLAITA_CONSOLE_DB_URL      console PG/SQLite DSN（SQLAlchemy URL）
    PLAITA_CONSOLE_REDIS_URL   引擎 Redis URL（worker 读的同一个 db）

可选：
    --tenant <slug>            只回填指定租户（默认全部租户）
    --flow <flow_id>           只回填指定流程（默认全部，可重复）
    --dry-run                  只打印将写入的内容，不实际写 Redis

退出码：0 = 全部成功 / 无非终态错误；1 = 至少一条写入失败。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# 让脚本既能从 backend/ 平铺布局（仓库/dev 运行时）跑，也能从 wheel 安装
# （包名 plaita_console）跑，还能从仓根跑。plaita SDK 在仓根；console backend
# 既可作平铺顶层模块（cwd=backend 时 import services/...），也可作 plaita_console。
_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
for _p in (str(_REPO_ROOT), str(_BACKEND_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from plaita.server.tenant_context import tenant_namespace
    from plaita.storage.redis import RedisFlowStorage
except ImportError as e:  # pragma: no cover - 环境缺依赖时给出可执行指引
    print(f"[FATAL] 无法导入 plaita SDK（{e}）；请从仓根或用已装 plaita 的 venv 运行",
          file=sys.stderr)
    raise SystemExit(1)


def _import_console_modules():
    """导入 console backend 的 flow_store / models / engine_sync。

    仓库/dev 布局下 backend/ 是顶层模块目录（``import services``）；wheel 安装
    后是包 ``plaita_console``。两种都试一遍，任一成功即可。
    """
    try:  # 包布局（wheel 安装 / cwd 在 plaita-console 父目录时）
        from plaita_console.backend.models.flow import FlowVersion
        from plaita_console.backend.services import flow_store
        from plaita_console.backend.services.engine_sync import sync_flow_to_engine
    except ImportError:  # 平铺布局（cwd=backend，或本脚本已把 backend 加入 sys.path）
        from models.flow import FlowVersion  # type: ignore
        from services import flow_store  # type: ignore
        from services.engine_sync import sync_flow_to_engine  # type: ignore
    return FlowVersion, flow_store, sync_flow_to_engine


FlowVersion, flow_store, sync_flow_to_engine = _import_console_modules()


def _load_published_versions(db_url: str, tenant: str | None,
                             flows: list[str] | None):
    """从 console PG/SQLite 读出全部已发布版本（authoritative 定义）。

    复用 console 自己的 SQLAlchemy 引擎与 ORM 模型，保证与运行时同一套表结构/
    迁移探测逻辑；不手写 SQL（列名漂移会静默漏数据）。
    """
    from sqlalchemy import select

    flow_store.init_engine(db_url)
    store = flow_store.get_flow_store()
    session_local = store._session_local

    with session_local() as session:  # type: ignore[union-attr]
        stmt = select(FlowVersion).where(FlowVersion.status == "published")
        if tenant:
            stmt = stmt.where(FlowVersion.tenant_id == tenant)
        if flows:
            stmt = stmt.where(FlowVersion.flow_id.in_(flows))
        stmt = stmt.order_by(FlowVersion.tenant_id, FlowVersion.flow_id,
                             FlowVersion.version)
        rows = session.scalars(stmt).all()
        # 脱离 session 前取出需要的字段
        return [
            (r.tenant_id or "", r.flow_id, r.version, r.definition or "")
            for r in rows
        ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tenant", default=None,
                        help="只回填指定租户 slug（默认全部租户）")
    parser.add_argument("--flow", action="append", dest="flows", default=None,
                        help="只回填指定 flow_id（可重复；默认全部）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印将要写入的定义，不实际写 Redis")
    args = parser.parse_args(argv)

    db_url = os.environ.get("PLAITA_CONSOLE_DB_URL", "sqlite:///./plaita_console.db")
    redis_url = os.environ.get("PLAITA_CONSOLE_REDIS_URL", "redis://localhost:6379/0")

    print(f"[sync] console DB : {db_url}")
    print(f"[sync] engine Redis: {redis_url}")

    rows = _load_published_versions(db_url, args.tenant, args.flows)
    if not rows:
        print("[sync] 没有找到已发布版本——无可回填。")
        return 0

    import redis as _redis
    client = _redis.Redis.from_url(redis_url, decode_responses=True)
    client.ping()

    written = skipped = errors = 0
    for tenant_id, flow_id, version, definition in rows:
        ns = tenant_namespace(tenant_id)
        key = f"{ns}:flow:{flow_id}:{version}"
        if args.dry_run:
            print(f"[dry-run] {flow_id}@{version} (tenant={tenant_id or 'default'}) "
                  f"-> {key} ({len(definition)} bytes)")
            skipped += 1
            continue
        if not definition.strip():
            print(f"[sync] {flow_id}@{version} -> skipped（定义为空）")
            skipped += 1
            continue
        ok = sync_flow_to_engine(client, flow_id, version, definition,
                                 tenant_id=tenant_id or None)
        if ok:
            print(f"[sync] {flow_id}@{version} (tenant={tenant_id or 'default'}) "
                  f"-> written")
            written += 1
        else:
            print(f"[sync] {flow_id}@{version} (tenant={tenant_id or 'default'}) "
                  f"-> ERROR")
            errors += 1

    # 汇总：附带各 namespace 的 flow_list 计数，便于人工核对
    if not args.dry_run:
        for ns in sorted({tenant_namespace(t or None) for t, *_ in rows}):
            store = RedisFlowStorage(client=client, namespace=ns)
            keys = client.keys(f"{ns}:flow:*:*")
            # 排除注册集合/队列键
            flow_keys = [k for k in keys if ":flow_versions:" not in k
                         and not k.startswith(f"{ns}:flow:queue")]
            print(f"[sync] namespace {ns}: {len(flow_keys)} 个 flow 定义键已就位")

    print(f"[sync] 汇总：written={written} skipped={skipped} errors={errors} "
          f"(共 {len(rows)} 条已发布版本)")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
