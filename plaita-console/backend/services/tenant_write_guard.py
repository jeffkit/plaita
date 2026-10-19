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

from contextvars import ContextVar, Token
from typing import Any, Optional, Tuple

from sqlalchemy import select

try:
    from ..models.flow import DEFAULT_TENANT_ID, Tenant
except ImportError:  # 平铺布局（cwd=backend，services 为顶层包）
    from models.flow import DEFAULT_TENANT_ID, Tenant  # type: ignore

TenantGeneration = Tuple[Any, Any]


class TenantGenerationMismatch(ValueError):
    """请求认证时的租户世代与写入事务内当前世代不一致（A51-02）。

    与「租户不存在」区分：delete 后同名 create 已完成，行存在但身份已换。
    继承 ``ValueError`` 保持既有服务/路由的 4xx 语义（flows 等把 ValueError
    转 409；调用方可按类型名进一步区分）。
    """

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        super().__init__(
            f"租户 {tenant_id} 已被删除或重建（请求世代已失效，拒绝写入）"
        )


# 请求级世代绑定（A51-02）：require_auth 解析会话时把世代写进 request.state，
# 管理面依赖再 bind 到 ContextVar。值 = (tenant_id, 世代令牌)。无绑定
# （CLI/测试/未挂依赖的旧挂载）= None → 各服务回退「首次读捕获」的存量行为。
_request_generation_ctx: ContextVar[Optional[Tuple[str, TenantGeneration]]] = (
    ContextVar("plaita_request_tenant_generation", default=None)
)


def _normalize_tenant(tenant_id: Optional[str]) -> str:
    return tenant_id or DEFAULT_TENANT_ID


def bind_tenant_generation(
    tenant_id: Optional[str], generation: Optional[TenantGeneration]
) -> Token:
    """绑定请求级租户世代；返回 token 供 finally reset（A51-02）。"""
    if generation is None:
        return _request_generation_ctx.set(None)
    return _request_generation_ctx.set((_normalize_tenant(tenant_id), tuple(generation)))


def reset_tenant_generation(token: Token) -> None:
    _request_generation_ctx.reset(token)


def expected_tenant_generation(
    tenant_id: Optional[str] = None,
) -> Optional[TenantGeneration]:
    """取请求级世代令牌；未绑定或目标租户不匹配时返回 None。

    ``tenant_id`` 显式传入时只接受同一租户的绑定——平台身份代写其他租户
    （或服务处理与请求租户不同的行）不能把 A 的 token 套到 B 上。
    """
    bound = _request_generation_ctx.get()
    if bound is None:
        return None
    bound_tenant, generation = bound
    if tenant_id is not None and _normalize_tenant(tenant_id) != bound_tenant:
        return None
    return generation


def assert_tenant_generation(
    session,
    tenant_id: Optional[str],
    expected: Optional[TenantGeneration] = None,
) -> None:
    """同一事务内、写之后复核请求世代（A51-02 最小改造清单第 5 条）。

    未绑定请求世代（CLI/测试/旧挂载）→ no-op，存量行为零变化。绑定后：
    当前行缺失或令牌不等 → ``TenantGenerationMismatch``，事务回滚。调用点
    必须已执行写/flush（SQLite 取得 RESERVED；PG 由本查询的
    ``SELECT ... FOR UPDATE`` 收口），把复核放在写之前等于回到 A51-02 的
    认证内窗口。
    """
    if expected is None:
        # 目标租户未知（None = 平台全量视角）时无法把请求世代套到具体行：
        # 生产写路径都经 tenant_scope(required=True) 给出具体租户，这里保守
        # 跳过，避免把 acme 的世代误用于 default/跨租户扫描。"" 仍按
        # default 处理（旧调用方/存量租户）。
        if tenant_id is None:
            return
        expected = expected_tenant_generation(tenant_id)
    if expected is None:
        return
    current = tenant_generation_token(session, _normalize_tenant(tenant_id))
    if current is None or current != expected:
        raise TenantGenerationMismatch(_normalize_tenant(tenant_id))


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
