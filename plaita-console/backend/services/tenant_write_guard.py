"""租户作用域写路径的串行化点（台账 A50-01/A50-02）。

根因是 ``delete_tenant`` 与写路径（``save_credential`` / ``add_member``）
各自「先查租户存在、后写行」：两个 autocommit 语句之间可以完整插入一个
删除事务。修法不是进程内每租户锁（console 可多进程），而是让双方在同一个
DB 串行化点上收口：

- SQLite：删除事务先 ``BEGIN IMMEDIATE``（写锁先于第一次读），读→删→提交
  期间其他写者拿不到 RESERVED；写路径在 flush（deferred 事务的第一次写，
  取得 RESERVED）之后 *同一事务内* 复检租户。锁只在写窗口内持有，加密等
  慢操作不持锁。
- 其他后端（PostgreSQL）：双方对 ``tenants`` 行 ``SELECT ... FOR UPDATE``。
  删除先锁行再清理；写路径先落自己的行再锁行复检——无论谁先拿到行锁，
  后者的检查都发生在对方提交之后，看到的就是「租户已删」。

``BEGIN IMMEDIATE`` 必须在 session 的第一次 DB 操作前调用（SQLite 不允许
在已有事务内再开事务）；``lock_tenant_row`` 在 SQLite 上只做存在性读取，
调用方必须已处于写事务（删除侧由 ``begin_guarded_transaction`` 保证，写
路径由 flush 保证）。
"""
from __future__ import annotations

from typing import Any, Optional, Tuple

from sqlalchemy import select

try:
    from ..models.flow import Tenant
except ImportError:  # 平铺布局（cwd=backend，services 为顶层包）
    from models.flow import Tenant  # type: ignore


def is_sqlite(session) -> bool:
    return session.get_bind().dialect.name == "sqlite"


def begin_guarded_transaction(session) -> None:
    """删除侧：在任何读之前取得 SQLite 写锁（其他后端由行锁负责）。"""
    if is_sqlite(session):
        session.connection().exec_driver_sql("BEGIN IMMEDIATE")


def lock_tenant_row(session, tenant_id: str) -> Optional[Tenant]:
    """读取租户行并在支持的 backend 上加写锁；不存在返回 None。

    SQLite：调用方必须已经 flush（写路径）或已 ``BEGIN IMMEDIATE``（删除
    侧），否则这里只是普通快照读，起不到串行化作用。
    """
    statement = select(Tenant).where(Tenant.id == tenant_id)
    if not is_sqlite(session):
        statement = statement.with_for_update()
    return session.scalars(statement).first()


def tenant_generation_token(session, tenant_id: str) -> Optional[Tuple[Any, Any]]:
    """租户世代令牌（``created_at`` + 随机 ``contract_secret_id``）。

    写路径在首次检查时记下令牌，flush 取得串行化点后在同一事务内复核。
    只查「租户是否存在」挡不住 delete → 同名 create 的 ABA：新租户行存在，
    但旧写请求不属于它。``create_tenant`` 每次生成新的随机 secret_id，
    重建必然换令牌；命令式读列值也绕过 ORM identity map 的陈旧对象。

    SQLite 上这是一次写事务内的快照读；其他 backend 加 ``FOR UPDATE``。
    """
    statement = select(Tenant.created_at, Tenant.contract_secret_id).where(
        Tenant.id == tenant_id
    )
    if not is_sqlite(session):
        statement = statement.with_for_update()
    row = session.execute(statement).first()
    return None if row is None else (row[0], row[1])
