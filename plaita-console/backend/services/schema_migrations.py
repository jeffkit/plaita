"""Console flow store 的 schema 迁移引导（alembic 的唯一入口）。

## 为什么需要它

console 的流程库（flows / flow_versions / local_executions / users …）此前靠
``Base.metadata.create_all`` + 若干**一次性的 SQLite 补列脚本**演进。这在无损升级
里是硬伤：create_all 只建缺失的表、**从不加列**，而补列脚本各写各的、没有版本记录，
于是「升级到哪个 schema」无从判断，也无法回滚。

现在统一走 alembic，并解决最关键的一步——**接管存量库**：

- 库里已有业务表、但没有 ``alembic_version``：说明是历史库 → ``stamp`` 基线
  （**不重放** DDL：现状已被 create_all 与补列脚本定义，重放只会冲突）；
- 库里已建到一半（存在 alembic_version）：正常 ``upgrade head``；
- 空库：``create_all`` 建全量 schema 后 ``stamp`` 基线（新装的库一步到位）。

三种情形都收敛到「schema 正确 + alembic 有记录」，且**幂等**——每次启动都能跑。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import inspect

logger = logging.getLogger(__name__)

BASELINE_REVISION = "0001_baseline"
_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


def _alembic_config(engine, db_url: str):
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", str(_MIGRATIONS_DIR))  # alembic 只吃 str
    cfg.set_main_option("sqlalchemy.url", db_url)
    # env.py 会自行从 console 配置取 URL；这里给出是为了 offline 模式与日志可读
    cfg.attributes["engine"] = engine
    return cfg


def _table_names(engine) -> set:
    return set(inspect(engine).get_table_names())


def run_migrations(engine, db_url: str) -> Dict[str, Any]:
    """把库对齐到 head。返回摘要（供启动日志/健康检查使用）。

    失败即抛：schema 与代码不匹配时继续启动，只会在运行期以更难查的方式炸。
    """
    from alembic import command

    tables = _table_names(engine)
    has_version_table = "alembic_version" in tables
    # 业务表：除 alembic 自己的版本表外的任意已知表
    known = {"flows", "flow_versions", "node_descriptors", "users", "local_executions"}
    has_business_tables = bool(tables & known)

    # 空库：先由 create_all 建全量 schema，再认领基线
    if not has_business_tables:
        from .flow_store import create_all

        create_all()
        tables = _table_names(engine)
        has_version_table = "alembic_version" in tables
        summary_action = "create_all+stamp"
    elif has_version_table:
        summary_action = "upgrade"
    else:
        # 存量库：先按历史语义把 schema 对齐到「当前完整形态」——老库可能只含
        # 部分表或旧列定义（多租户迁移的重建逻辑都收敛在 create_all 路径里），
        # 这一步与引入 alembic 前的启动行为**完全一致**；随后再认领基线。
        # 不做的话会以 "no such table: tenants" 之类崩在启动路径上。
        from .flow_store import create_all

        _before = _table_names(engine)
        create_all()
        _added = _table_names(engine) - _before - {"alembic_version"}
        summary_action = "stamp-baseline"
        if _added:
            logger.info("存量库补齐缺失表 %d 张：%s", len(_added), ", ".join(sorted(_added)))

    cfg = _alembic_config(engine, db_url)
    if summary_action in ("create_all+stamp", "stamp-baseline"):
        # 存量/空库认领基线：不重放 DDL
        command.stamp(cfg, BASELINE_REVISION)
        if current_revision(engine) is None:
            # 认领没生效就必须炸出来：否则下次启动仍走 stamp-baseline，
            # 而正式迁移永远不执行（schema 静默停在旧版）
            raise RuntimeError(
                f"alembic stamp {BASELINE_REVISION} 未生效（current_revision 仍为空）"
            )
        logger.info(
            "console schema：%s（库内已有 %d 张表，认领基线 %s，不重放 DDL）",
            summary_action,
            len(_table_names(engine)),
            BASELINE_REVISION,
        )
    else:
        command.upgrade(cfg, "head")
        logger.info("console schema：已 upgrade 到 head")

    return {
        "action": summary_action,
        "baseline": BASELINE_REVISION,
        "tables": len(_table_names(engine)),
    }


def current_revision(engine) -> Optional[str]:
    """当前 alembic 版本（无版本表/无记录时 None）。"""
    try:
        from alembic.migration import MigrationContext

        with engine.connect() as conn:
            return MigrationContext.configure(conn).get_current_revision()
    except Exception as exc:  # noqa: BLE001 — 只读诊断，失败不该影响调用方
        logger.debug("读取 alembic 当前版本失败: %s", exc)
        return None
