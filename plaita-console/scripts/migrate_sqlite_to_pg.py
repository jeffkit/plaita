#!/usr/bin/env python
"""console 库迁移：SQLite → PostgreSQL（含**自增主键序列重置**）。

背景
====
console 从 SQLite 迁到 PostgreSQL 后，数据是「按行 INSERT 搬过去的」——
搬数时显式写入了 ``id`` 列（保留原主键），但 PostgreSQL 的
``<table>_id_seq``（serial/identity 序列）**不会**随之推进，仍停在初始值。
于是迁移后第一次通过应用插入（不显式给 id）：

* ``create_flow`` → ``flows`` 主键冲突 ``IntegrityError``（被误报为「流程已存在」）；
* publish 写审计 → ``audit_logs`` 主键冲突；
* 部署记录 → ``deployments`` 主键冲突。

**根因就在迁移路径**：搬完数据必须对每张有自增主键的表执行一次序列重置。
本脚本把「建表 → 搬数 → 校验 → 重置序列」固化为可复用、幂等的迁移工具，
而不是让运维在事后手工 ``setval`` 打补丁。

序列重置
========
对**每一张有「整型单列自增主键」的表**（从 SQLAlchemy metadata 自动枚举，
不硬编码表名、不假设列名一定是 ``id``）执行::

    SELECT setval(pg_get_serial_sequence('<table>', '<pk_col>'),
                  COALESCE((SELECT MAX(<pk_col>) FROM <table>), 1));

* 空表 → ``setval(seq, 1, false)``（下一个 nextval 返回 1，不撞已有行）；
* 非空表 → 推到 ``MAX(pk)``，下一个 nextval 从 max+1 起。

幂等：重复跑只会把序列推到同样的值；建表用 ``create_all``（已存在则跳过）。

用法
====
    python plaita-console/scripts/migrate_sqlite_to_pg.py \
        --sqlite /path/to/console.db \
        --pg "postgresql+psycopg://user:pw@127.0.0.1:5432/plaita_console"

参数：
    --sqlite <path>    源 SQLite 文件（默认 env PLAITA_CONSOLE_DB_URL 或
                       ./plaita_console.db）
    --pg <dsn>         目标 PostgreSQL SQLAlchemy URL（默认 env
                       PLAITA_CONSOLE_PG_URL，必填）
    --sqlite-url       直接用 SQLAlchemy URL 覆盖 --sqlite（sqlite:///...）
    --drop             迁移前 DROP SCHEMA public CASCADE 重建（**破坏性**：清空
                       目标库后从 SQLite 全新搬迁；用于演练/重跑全流程）
    --no-reset-seq     只搬数不重置序列（仅调试用；正常迁移**不要**加）
    --dry-run          只打印将搬的表与目标序列状态，不写库

退出码：0 = 成功；1 = 有表搬迁失败或校验不一致。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# 让脚本既能从仓根跑，也能从 backend/ 平铺布局跑：plaita SDK 在仓根，
# console backend 可作平铺顶层模块（cwd=backend 时 import models/services）。
_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
for _p in (str(_REPO_ROOT), str(_BACKEND_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from sqlalchemy import MetaData, create_engine, select, text  # noqa: E402
from sqlalchemy.dialects.postgresql import insert as pg_insert  # noqa: E402


def _load_console_base():
    """导入 console 自己的 declarative Base（含全部表定义）。

    仓库/dev 布局下 backend/ 是顶层模块目录（``from models.flow import Base``）；
    wheel 安装后是包 ``plaita_console``。两种都试一遍。
    """
    try:  # 包布局（wheel 安装）
        from plaita_console.backend.models.flow import Base  # type: ignore
    except ImportError:  # 平铺布局（本脚本已把 backend 加入 sys.path）
        from models.flow import Base  # type: ignore
    return Base


Base = _load_console_base()


def _autoincrement_pk_tables(metadata: MetaData) -> list[tuple[str, str]]:
    """枚举全部「单列整型自增主键」表，返回 [(table_name, pk_column), ...]。

    判据（跨后端稳健，不硬编码表名/列名）：
    - 表只有 1 个主键列；
    - 该列是 Integer 族（含 BigInteger/SmallInteger）；
    - 该列 autoincrement 未显式关闭。

    刻意**不**假设主键列名是 ``id``——console 里 ``local_schedules`` 的主键
    是 ``schedule_id``（String），``tenants`` 是 String，都没有 serial 序列，
    必须排除。
    """
    from sqlalchemy import BigInteger, Integer, SmallInteger

    int_types = (Integer, BigInteger, SmallInteger)
    out: list[tuple[str, str]] = []
    for table in metadata.sorted_tables:
        pk_cols = list(table.primary_key.columns)
        if len(pk_cols) != 1:
            continue
        col = pk_cols[0]
        if not isinstance(col.type, int_types):
            continue
        if col.autoincrement is False:
            continue
        out.append((table.name, col.name))
    return out


def _reset_sequences(engine, metadata: MetaData, dry_run: bool = False) -> int:
    """对每张自增主键表重置 PostgreSQL 序列到 max(pk)。返回处理表数。

    幂等；仅对 PostgreSQL 后端有效（SQLite 无独立序列，直接跳过）。
    """
    if engine.url.get_backend_name() != "postgresql":
        print(f"[seq] 后端 {engine.url.get_backend_name()} 无独立序列，跳过序列重置")
        return 0

    targets = _autoincrement_pk_tables(metadata)
    print(f"[seq] 待重置序列的自增主键表（{len(targets)} 张）: "
          f"{', '.join(f'{t}.{c}' for t, c in targets)}")
    n = 0
    with engine.begin() as conn:
        for table, pk_col in targets:
            seq = conn.execute(
                text("SELECT pg_get_serial_sequence(:t, :c)"),
                {"t": table, "c": pk_col},
            ).scalar()
            if not seq:
                # 非 serial/identity（如由其它方式建的表）：无可重置序列
                print(f"[seq]   skip {table}.{pk_col}（无 serial 序列）")
                continue
            max_id = conn.execute(
                text(f'SELECT MAX("{pk_col}") FROM "{table}"')
            ).scalar()
            if dry_run:
                print(f"[seq]   [dry-run] {table}.{pk_col}: seq={seq} "
                      f"max={max_id} → 将 setval={max_id or 1}")
                n += 1
                continue
            if max_id is None:
                # 空表：setval(seq,1,false) → 下一个 nextval 返回 1
                conn.execute(text("SELECT setval(:seq, 1, false)"), {"seq": seq})
                print(f"[seq]   {table}.{pk_col}: 空表 → setval({seq}, 1, false)")
            else:
                conn.execute(text("SELECT setval(:seq, :v, true)"),
                             {"seq": seq, "v": max_id})
                print(f"[seq]   {table}.{pk_col}: setval({seq}, {max_id})")
            n += 1
    return n


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--sqlite", default=None,
                        help="源 SQLite 文件路径（默认 ./plaita_console.db）")
    parser.add_argument("--sqlite-url", default=None,
                        help="源 SQLAlchemy URL（覆盖 --sqlite）")
    parser.add_argument("--pg", default=os.environ.get("PLAITA_CONSOLE_PG_URL"),
                        help="目标 PostgreSQL SQLAlchemy URL")
    parser.add_argument("--drop", action="store_true",
                        help="迁移前 DROP SCHEMA public CASCADE 重建（破坏性，清空目标）")
    parser.add_argument("--no-reset-seq", action="store_true",
                        help="跳过序列重置（调试用；正常迁移不要加）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印不写库")
    args = parser.parse_args(argv)

    if not args.pg:
        print("[FATAL] 必须提供 --pg（或 env PLAITA_CONSOLE_PG_URL）", file=sys.stderr)
        return 1

    sqlite_url = args.sqlite_url
    if not sqlite_url:
        sqlite_path = args.sqlite or os.environ.get(
            "PLAITA_CONSOLE_SQLITE", "./plaita_console.db")
        sqlite_url = f"sqlite:///{sqlite_path}"

    print(f"[migrate] 源  SQLite : {sqlite_url}")
    print(f"[migrate] 目标 PG     : {args.pg}")

    src = create_engine(sqlite_url, future=True)
    dst = create_engine(args.pg, future=True)

    if args.drop and not args.dry_run:
        print("[migrate] --drop：DROP SCHEMA public CASCADE + CREATE SCHEMA public")
        with dst.begin() as conn:
            conn.execute(text("DROP SCHEMA public CASCADE"))
            conn.execute(text("CREATE SCHEMA public"))

    # 1) 目标建表（console 自己的 metadata：含复合外键等全部约束）
    print("[migrate] 建表：Base.metadata.create_all(dst)")
    Base.metadata.create_all(dst)

    # 2) 源表清单与行数
    with src.connect() as c:
        src_tables = [r[0] for r in c.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"))]
        src_counts: dict[str, int] = {}
        for t in src_tables:
            src_counts[t] = c.execute(
                text(f'SELECT count(*) FROM "{t}"')).scalar()

    # 3) 按依赖顺序（拓扑排序）搬数
    dst_meta = MetaData()
    dst_meta.reflect(bind=dst)
    order = [t for t in dst_meta.sorted_tables if t.name in src_counts]
    print(f"[migrate] 搬迁顺序（拓扑排序）: {[t.name for t in order]}")

    migrated: dict[str, int] = {}
    failed = False
    with src.connect() as sc:
        for tbl in order:
            rows = sc.execute(select(tbl)).mappings().all()
            if not rows:
                migrated[tbl.name] = 0
                print(f"[migrate]   {tbl.name}: 0 行（跳过）")
                continue
            payload = [dict(r) for r in rows]
            # 每张表单独事务：一张表失败不会污染后续（PG 下事务一旦报错即进入
            # aborted 态，后续语句全被拒——必须隔离，否则一表失败即全表失败）。
            # 幂等：ON CONFLICT DO NOTHING，重跑只补缺失行、不撞已有主键。
            n_written = 0
            try:
                with dst.begin() as dc:
                    # executemany + ON CONFLICT DO NOTHING：重跑只补缺失行
                    stmt = pg_insert(tbl).on_conflict_do_nothing()
                    res = dc.execute(stmt, payload)
                    n_written = res.rowcount if (res.rowcount or -1) >= 0 else len(payload)
            except Exception as e:  # noqa: BLE001 —— 逐表报告，最后统一判失败
                failed = True
                print(f"[migrate]   {tbl.name}: 搬迁失败 ✗ {type(e).__name__}: {e}")
                continue
            migrated[tbl.name] = n_written
            skipped = len(payload) - n_written
            note = f"（跳过已存在 {skipped}）" if skipped else ""
            print(f"[migrate]   {tbl.name}: 写入 {n_written}/{len(payload)} 行 ✓{note}")

    # 4) 序列重置（本工具的核心修复：迁移必须重置自增序列）
    if args.no_reset_seq:
        print("[seq] --no-reset-seq：跳过序列重置（⚠ 略过即会在应用插入时主键冲突）")
    else:
        _reset_sequences(dst, dst_meta, dry_run=args.dry_run)

    # 5) 校验：行数一致
    print("[migrate] 校验行数：")
    ok = True
    with dst.connect() as dc:
        for t, n in src_counts.items():
            if t not in dst_meta.tables:
                continue
            got = dc.execute(text(f'SELECT count(*) FROM "{t}"')).scalar()
            flag = "✓" if got == n else "✗"
            if got != n:
                ok = False
            print(f"[migrate]   {flag} {t}: 源={n} 目标={got}")

    if failed or not ok:
        print("[migrate] 结果：失败 ✗（见上表）")
        return 1
    print("[migrate] 结果：ALL-OK ✓")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
