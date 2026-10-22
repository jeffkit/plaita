"""Alembic 环境：复用 console 的运行期配置（PLAITA_CONSOLE_DB_URL）。

设计取向（与本仓「本地单机模式可用」的定位一致）：
- 不引入 alembic.ini：URL 只从 config.get_settings() 取，避免两处真相；
- offline/online 都支持；
- ``compare_type``/``compare_server_default`` 打开，让 autogenerate 能发现类型变更。

**存量库**（历史上由 create_all 建出、没有 alembic_version 表）用
``alembic stamp head`` 认领基线，不做重放——见 services/schema_migrations.py。
"""
from __future__ import annotations

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

try:
    from models.flow import Base  # type: ignore
    from config import get_settings  # type: ignore
except ImportError:  # 包内布局（plaita_console.migrations）
    from ..models.flow import Base  # type: ignore
    from ..config import get_settings  # type: ignore

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> str:
    """迁移目标 URL。

    **优先级：调用方传入的引擎 > console 配置**。引导路径
    （services/schema_migrations）已经把「真实在用的引擎」放进 config.attributes，
    必须以它为准——早期版本只读 get_settings()，导致传进来的引擎与迁移目标
    是两个不同的库（stamp 写到了配置指向的库，真实库永远没有版本记录，
    表面上却"成功"）。
    """
    engine = config.attributes.get("engine")
    if engine is not None:
        return str(engine.url)
    configured = config.get_main_option("sqlalchemy.url")
    if configured:
        return configured
    return get_settings().db_url


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    passed_engine = config.attributes.get("engine")
    if passed_engine is not None:
        # 直接用调用方的引擎：保证「迁移目标 == 应用真正在读写的库」
        connectable = passed_engine
    else:
        section = config.get_section(config.config_ini_section) or {}
        section["sqlalchemy.url"] = _database_url()
        connectable = engine_from_config(
            section, prefix="sqlalchemy.", poolclass=pool.NullPool
        )
    with connectable.connect() as connection:
        if connection.dialect.name == "sqlite":
            # #308：运行期连接每次 checkout 强制外键（flow_store 引擎监听），
            # 迁移会话反向关闭——recreate 型 batch 表重建/拷贝必须与存量孤儿
            # 行共存（老版本 delete_flow 残留的版本行不该卡死升级）。必须经
            # DBAPI 连接直拨（PRAGMA 需在事务外执行）：走 Connection 层的
            # exec_driver_sql 会触发 autobegin，alembic 随后的
            # begin_transaction 在已开事务的连接上直接炸、init_engine 被迫
            # 走 create_all-fallback。连接归还后下一次 checkout 会被运行期
            # 监听拨回 ON。
            dbapi_conn = connection.connection.dbapi_connection
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=OFF")
            cursor.close()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            render_as_batch=connection.dialect.name == "sqlite",
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
