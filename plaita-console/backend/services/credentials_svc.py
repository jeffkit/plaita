"""凭据管理服务：加密落库 + 导出引擎可读的凭据文件。

密钥解析顺序（console 与引擎侧 plaita.credentials 一致）：
1. 环境变量 ``PLAITA_CREDENTIALS_KEY``（Fernet key）
2. 密钥文件 ``PLAITA_CREDENTIALS_KEY_FILE``（默认与 DB 同目录 ``.plaita-credentials.key``，
   首次使用自动生成 440 权限）

安全边界（如实说明）：密钥文件与 DB 同机存放，属静态加密（at-rest）而非
密钥管理服务；多机部署请把 KEY 放到密钥管理设施并统一注入。
"""
import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select

from plaita.credentials import (
    STORE_FORMAT,
    STORE_FORMAT_VERSION,
    STORE_META_KEY,
    credentials_base_file as _engine_credentials_base_file,
)
from plaita.tenant_context import guard_tenant_write

try:
    from ..models.flow import Base, Credential, DEFAULT_TENANT_ID, Tenant
    from .flow_store import get_flow_store
    from .tenant_write_guard import (
        assert_tenant_generation,
        begin_guarded_transaction,
        expected_tenant_generation,
        tenant_generation_token,
    )
except ImportError:  # 平铺布局（cwd=backend，services 为顶层包）
    from models.flow import Base, Credential, DEFAULT_TENANT_ID, Tenant  # type: ignore
    from services.flow_store import get_flow_store  # type: ignore
    from services.tenant_write_guard import (  # type: ignore
        assert_tenant_generation,
        begin_guarded_transaction,
        expected_tenant_generation,
        tenant_generation_token,
    )

logger = logging.getLogger(__name__)


class CredentialsDisabledError(RuntimeError):
    """密钥初始化失败等导致凭据功能不可用。"""


class CredentialsKeyMismatchError(CredentialsDisabledError):
    """当前密钥与库中已有密文不匹配（或根本没有可用密钥）。

    A2R25-10：错/缺 key 的恢复环境里，任何写路径都必须拒绝，否则新行会用
    错误密钥加密、并整文件重写导出，把库变成无 key id 的混合密钥状态。

    ``reason``：``mismatch``（有 key 但解不开）| ``missing``（没有 key）。
    两者都拒绝写入，但错误体给出可区分的排查方向。
    """

    def __init__(self, message: str, *, reason: str = "mismatch") -> None:
        super().__init__(message)
        self.reason = reason


def _db_dir() -> Path:
    """从 PLAITA_CONSOLE_DB_URL 推断 DB 所在目录（默认 cwd）。"""
    db_url = os.environ.get("PLAITA_CONSOLE_DB_URL", "sqlite:///./plaita_console.db")
    if db_url.startswith("sqlite:///"):
        raw = db_url[len("sqlite:///"):]
        return Path(raw).resolve().parent
    return Path.cwd()


def credentials_key_file() -> Path:
    return Path(os.environ.get("PLAITA_CREDENTIALS_KEY_FILE", _db_dir() / ".plaita-credentials.key"))


def credentials_file(tenant_id: str = DEFAULT_TENANT_ID) -> Path:
    """凭据导出文件路径：default 租户沿用历史文件（兼容既有引擎读取），
    其余租户写旁文件 ``<stem>.<tenant_id><suffix>``。

    基础路径与引擎侧 ``plaita.credentials.credentials_base_file`` **同源**：
    ``PLAITA_CREDENTIALS_FILE`` 显式设置时用它；否则 SQLite 落在 DB 同目录，
    非 SQLite（PG）或未配置时回退 cwd。导出文件是 DB 的派生缓存，按 DB 路径
    落盘让「同一 DB 的多个 console/worker 副本」共享同一份导出，而不是各按
    cwd 持有一份互不收敛的副本（A66-01 ~ A66-04）。

    旁文件由引擎侧按租户上下文（plaita.tenant_context，worker 消费任务时从
    消息体 tenant_id set）读取同一命名——多租户 flow 在 worker 上解析凭据时
    取到的是自己租户的文件，default 文件对它不可见。"""
    base = _engine_credentials_base_file()
    if tenant_id == DEFAULT_TENANT_ID:
        return base
    return base.with_name(f"{base.stem}.{tenant_id}{base.suffix}")


def _load_key_bytes(*, generate: bool) -> bytes:
    """读取 Fernet key；``generate=True`` 仅在首次加密时允许创建密钥文件。

    读已有密文（``generate=False``）绝不生成新 key：缺 key 时生成一把新
    key 只会让所有旧密文必然解密失败，并把「未配置」伪装成「密钥不一致」。
    """
    key = os.environ.get("PLAITA_CREDENTIALS_KEY")
    if key:
        return key.encode() if isinstance(key, str) else key
    key_file = credentials_key_file()
    if key_file.is_file():
        return key_file.read_bytes().strip()
    if not generate:
        raise CredentialsDisabledError(
            "未配置凭据解密密钥：请设置 PLAITA_CREDENTIALS_KEY（Fernet key），"
            f"或提供 {key_file} 指向密钥文件"
        )
    key = Fernet.generate_key()
    key_file.parent.mkdir(parents=True, exist_ok=True)
    key_file.write_bytes(key)
    try:
        os.chmod(key_file, 0o600)
    except OSError:
        pass
    logger.info("已生成凭据密钥文件: %s", key_file)
    return key


def _fernet(*, generate: bool = False) -> Fernet:
    key = _load_key_bytes(generate=generate)
    try:
        return Fernet(key)
    except Exception as e:  # noqa: BLE001
        raise CredentialsDisabledError(f"PLAITA_CREDENTIALS_KEY 不是合法 Fernet key: {e}") from e


def current_key_id() -> str:
    """当前密钥的稳定指纹（sha256 前 16 位），写入导出文件的版本头。"""
    return hashlib.sha256(_load_key_bytes(generate=False)).hexdigest()[:16]


def _encrypt(data: Dict[str, Any]) -> str:
    return _fernet(generate=True).encrypt(
        json.dumps(data, ensure_ascii=False).encode()
    ).decode()


def _decrypt(token: str) -> Dict[str, Any]:
    try:
        return json.loads(_fernet(generate=False).decrypt(token.encode()).decode())
    except InvalidToken as e:
        raise CredentialsKeyMismatchError(
            "凭据解密失败：PLAITA_CREDENTIALS_KEY/密钥文件与写入时不一致"
        ) from e


def _first_undecryptable(rows) -> Optional[str]:
    """返回第一条无法用当前密钥解密的凭据名；全部可解返回 None。

    空集合直接返回 None（首次保存正是生成密钥的时点）；有行但缺 key/错 key
    统一抛 :class:`CredentialsKeyMismatchError`，调用方据此拒绝写入。
    """
    rows = list(rows)
    if not rows:
        return None
    try:
        fernet = _fernet(generate=False)
    except CredentialsDisabledError as e:
        raise CredentialsKeyMismatchError(
            "凭据库已有条目，但当前没有可用的解密密钥"
            f"（{e}）；已拒绝写入以免覆盖既有导出（原文件保留）。"
            "请恢复 PLAITA_CREDENTIALS_KEY/密钥文件后重试。",
            reason="missing",
        ) from e
    for row in rows:
        try:
            fernet.decrypt(row.data_json.encode())
        except InvalidToken:
            return row.name
    return None


def _key_usable_for_rows(rows) -> bool:
    """当前 key 是否可解开这些行（不抛异常，供删除路径决定是否写版本头）。"""
    try:
        return _first_undecryptable(rows) is None
    except CredentialsKeyMismatchError:
        return False


def list_credentials(tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """列出凭据元信息（不含密文）。tenant_id=None 为平台全量视角。"""
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Credential).order_by(Credential.name)
        if tenant_id is not None:
            query = query.where(Credential.tenant_id == tenant_id)
        rows = session.scalars(query).all()
        return [
            {
                "name": r.name,
                "tenant_id": r.tenant_id,
                "type": r.type,
                "desc": r.desc,
                "updated_at": r.updated_at.isoformat(),
            }
            for r in rows
        ]


def get_credential_record(name: str, tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Credential).where(Credential.name == name)
        if tenant_id is not None:
            query = query.where(Credential.tenant_id == tenant_id)
        row = session.scalars(query).first()
        if row is None:
            return None
        return {
            "name": row.name,
            "tenant_id": row.tenant_id,
            "type": row.type,
            "desc": row.desc,
            # A2R24-07：与列表行同一投影，详情页不再缺更新时间。
            "updated_at": row.updated_at.isoformat(),
            "data": _decrypt(row.data_json),
        }


def save_credential(name: str, type_: str, data: Dict[str, Any], desc: str = "",
                    tenant_id: str = DEFAULT_TENANT_ID) -> None:
    if not name:
        raise ValueError("凭据名称不能为空")
    if name == STORE_META_KEY:
        # 保留键用于导出文件的 key id/版本头，不能被当成一条凭据（否则
        # 引擎侧过滤后就永远读不到它）。
        raise ValueError(f"凭据名 {name!r} 为导出文件保留键，请换一个名称")
    if not isinstance(data, dict) or not data:
        raise ValueError("凭据数据必须是非空对象")
    guard_tenant_write("credentials.save")  # #239 fail-closed
    store = get_flow_store()
    with store._session_local() as session:
        # plaita#257：已删除 / 从未存在的租户拒绝写回。否则删除后一个迟到的
        # save 会重新长出凭据行与旁文件（「复活 store」），重建同名租户即可
        # 继承——删除的清理语义被单次写调用整体绕过。
        tenant = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if tenant is None:
            raise ValueError(
                f"租户不存在或已删除: {tenant_id}（拒绝写入凭据）"
            )
        # 记住本次校验通过的租户世代；同名重建会换 token（A50-01 ABA）。
        # A51-02：请求认证时的世代优先——服务首次读若被拖到 delete+create
        # 之后，不能把重建租户的世代当成自己的（已护栏路径 0 新增读）。
        tenant_token = (
            expected_tenant_generation(tenant_id)
            or (tenant.created_at, tenant.contract_secret_id)
        )
        # A2R25-10：写前确认库中已有密文都能用当前 key 解开。错/缺 key 的
        # 恢复环境里直接拒绝，绝不「用新 key 加密新行 + 重写整个导出」——
        # 那会把可修复的 key 不匹配升级成同一 DB 混合密钥、谁都解不全。
        existing_rows = session.scalars(
            select(Credential).where(Credential.tenant_id == tenant_id)
        ).all()
        undecryptable = _first_undecryptable(existing_rows)
        if undecryptable is not None:
            logger.warning(
                "凭据保存被拒：既有条目 %s（租户 %s）无法用当前密钥解密；"
                "未写入 DB、未重写导出文件",
                undecryptable,
                tenant_id,
            )
            raise CredentialsKeyMismatchError(
                f"凭据库中的 {undecryptable!r} 无法用当前密钥解密："
                "PLAITA_CREDENTIALS_KEY/密钥文件与写入时不一致。已拒绝保存"
                "（DB 与导出文件保持不变）；请恢复原密钥后重试。"
            )
        encrypted = _encrypt(data)
        row = next((r for r in existing_rows if r.name == name), None)
        if row is None:
            session.add(
                Credential(
                    tenant_id=tenant_id, name=name, type=type_, data_json=encrypted, desc=desc
                )
            )
        else:
            row.type = type_
            row.data_json = encrypted
            if desc:
                row.desc = desc
        # plaita#251：先原子导出（同一事务内的 flush 让导出声看到新行），
        # 成功后再提交 DB；导出失败 ⇒ 事务回滚，DB 不留新行、旁文件保持旧值。
        session.flush()
        # A50-01：flush 是写事务的起点（SQLite 取得 RESERVED；PG 下随后的
        # SELECT ... FOR UPDATE 锁租户行）。在**同一事务内**复核租户世代：
        # 并发的 delete_tenant 已提交 ⇒ None；delete+同名 create 已提交 ⇒
        # 新 token。两种情况都回滚，不落凭据行也不导出旁文件。若删除/重建
        # 还没提交，对方会等本事务结束后处理，新行不会漏。首次检查只负责
        # 快速拒绝。
        current_token = tenant_generation_token(session, tenant_id)
        if current_token is None or current_token != tenant_token:
            raise ValueError(
                f"租户不存在或已删除: {tenant_id}（拒绝写入凭据）"
            )
        _export_then_commit(session, tenant_id)


def delete_credential(name: str, tenant_id: Optional[str] = None) -> bool:
    guard_tenant_write("credentials.delete")  # #239 fail-closed
    store = get_flow_store()
    with store._session_local() as session:
        query = select(Credential).where(Credential.name == name)
        if tenant_id is not None:
            query = query.where(Credential.tenant_id == tenant_id)
        row = session.scalars(query).first()
        if row is None:
            # A66-01/A66-04：DB 无行即权威，但导出文件可能仍含该名字——
            # #251 的「导出成功→commit」窗口被 SIGKILL 打断会留下文件超前
            # DB 的鬼凭据，旧版本按 cwd 留下的副本也会残留旧值。重试 DELETE
            # 若只返回 404 而不重写文件，操作员就没有凭据 API 可用的撤销路径
            # （引擎仍能解密）。这里按 DB 重导一次目标范围（tenant_id=None
            # 时全量）；写入失败 loud 上抛，不伪装成「本来就不存在」。
            cleanup_query = select(Credential)
            if tenant_id is not None:
                cleanup_query = cleanup_query.where(
                    Credential.tenant_id == tenant_id
                )
            cleanup_rows = session.scalars(cleanup_query).all()
            _export_store(
                tenant_id,
                session=session,
                verify_key=_key_usable_for_rows(cleanup_rows),
            )
            logger.info(
                "DELETE 未命中 DB（name=%s tenant=%s），已按 DB 重导导出文件以清除陈旧条目",
                name,
                tenant_id,
            )
            return False
        affected = row.tenant_id
        session.delete(row)
        # plaita#251：同一事务内 flush 后导出（导出声已看不到该行），成功才
        # commit；导出失败 ⇒ 回滚，DB 行与旁文件都保留（可安全重试）。
        session.flush()
        # A51-02：删除同属租户作用域写；请求世代失效时不得动重建租户的行。
        assert_tenant_generation(session, affected)
        # A2R25-10：删除不要求旧行可解（错 key 恢复时仍需撤销入口）。key
        # 可用时照常写版本头；不可用时省略头，绝不把「当前 key 的 key_id」
        # 写进仍含旧密文的导出文件（重写内容与原密文逐条一致）。
        remaining = session.scalars(
            select(Credential).where(Credential.tenant_id == affected)
        ).all()
        _export_then_commit(
            session,
            affected,
            verify_key=_key_usable_for_rows(remaining),
        )
    return True


def _atomic_write_text(path: Path, text: str) -> None:
    """同目录临时文件写满并 fsync 后原子替换 ``path``。

    先 truncate 再写目标文件时，写到一半失败（磁盘满/超配额/SIGKILL）会把
    上一份完好的文件毁成半截；这里失败路径只影响临时文件，旧文件逐字节不变。
    ``os.replace`` 与 ``mkstemp`` 同目录调用，保证替换在同一文件系统内原子生效。
    """
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        # mkstemp 已建成 0600；显式 chmod 保证目标权限与旧实现一致（含 umask 差异）。
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _export_payload(
    entries: Dict[str, Dict[str, Any]], *, include_meta: bool
) -> Dict[str, Any]:
    """导出文件的顶层对象：可选 key id/版本头 + 凭据条目。

    空租户仍写 ``{}``（不落版本头）：没有可失配的密文，且保持引擎旧版本
    对空 store 的解析零变化。有密文时才声明 ``key_id``——调用方必须先确认
    这些密文确实由当前 key 加密（``_first_undecryptable``）。
    """
    if not entries or not include_meta:
        return dict(entries)
    return {
        STORE_META_KEY: {
            "format": STORE_FORMAT,
            "version": STORE_FORMAT_VERSION,
            "key_id": current_key_id(),
        },
        **entries,
    }


def _export_store(
    tenant_id: Optional[str] = None, *, session=None, verify_key: bool = True
) -> None:
    """把凭据（密文）按租户导出为引擎可读文件，供节点运行时解密。

    tenant_id=None 导出全部租户（各写各的文件）；指定租户只重导该租户。
    default 租户沿用历史文件路径，保持既有引擎读取兼容。

    ``session``（plaita#251）：调用方事务内的 session。save/delete 在
    commit **之前**调用；调用方已 ``flush`` 的未提交变更必须被导出声看到
    （否则导出的还是旧行集）。传 session 时复用该事务、不自行开关 session。

    ``verify_key``（A2R25-10）：True 时逐条确认密文可由当前 key 解开，否则
    抛 :class:`CredentialsKeyMismatchError` 且不落盘。delete 传 False——
    撤销入口不要求旧 key，导出文件按 DB 现有密文重写但不声明版本头。
    """
    if session is not None:
        _export_from_session(session, tenant_id, verify_key=verify_key)
        return
    store = get_flow_store()
    with store._session_local() as own_session:
        _export_from_session(own_session, tenant_id, verify_key=verify_key)


def _export_from_session(
    session, tenant_id: Optional[str], *, verify_key: bool = True
) -> None:
    query = select(Credential)
    if tenant_id is not None:
        query = query.where(Credential.tenant_id == tenant_id)
    rows = session.scalars(query).all()
    by_tenant: Dict[str, Dict[str, Any]] = {}
    rows_by_tenant: Dict[str, List[Any]] = {}
    for r in rows:
        tid = r.tenant_id or DEFAULT_TENANT_ID
        rows_by_tenant.setdefault(tid, []).append(r)
        by_tenant.setdefault(tid, {})[r.name] = {
            "type": r.type,
            "data": r.data_json,
        }
    if tenant_id is not None:
        # 指定租户：即使已清空也要重写文件（删除最后一条凭据时清空残留）
        targets = {tenant_id: by_tenant.get(tenant_id, {})}
        if not rows_by_tenant.get(tenant_id):
            rows_by_tenant[tenant_id] = []
    else:
        # 全量：覆盖存在的租户 + default（历史文件路径始终刷新）
        targets = dict(by_tenant)
        targets.setdefault(DEFAULT_TENANT_ID, {})
    for tid, payload in targets.items():
        path = credentials_file(tid)
        path.parent.mkdir(parents=True, exist_ok=True)
        include_meta = False
        if verify_key and payload:
            undecryptable = _first_undecryptable(rows_by_tenant.get(tid, []))
            if undecryptable is not None:
                logger.warning(
                    "拒绝对账/重写租户 %s 的凭据导出：条目 %s 无法用当前密钥解密"
                    "（原文件保留）",
                    tid,
                    undecryptable,
                )
                raise CredentialsKeyMismatchError(
                    f"凭据库中的 {undecryptable!r}（租户 {tid}）无法用当前密钥"
                    "解密：拒绝重写导出文件（原文件保留）。请恢复原密钥后重试。"
                )
            include_meta = True
        text = json.dumps(
            _export_payload(payload, include_meta=include_meta),
            ensure_ascii=False,
            indent=1,
        )
        _atomic_write_text(path, text)
        logger.info("已导出租户 %s 凭据文件 %s（%d 条）", tid, path, len(payload))


def _export_then_commit(
    session, tenant_id: Optional[str], *, verify_key: bool = True
) -> None:
    """先原子导出旁文件，成功后再提交 DB（plaita#251）。

    - 导出失败 ⇒ 回滚事务：DB 不留新行/删除，旁文件由 ``_atomic_write_text``
      保证保持旧值；调用方可安全重试。
    - 导出成功但 commit 失败 ⇒ DB 回滚，再按回滚后的 DB 状态补导一次，
      避免旁文件（已含新状态）与 DB（仍旧状态）分叉；补导失败只记日志，
      原始 commit 异常照常上抛。
    """
    exported = False
    try:
        _export_store(tenant_id, session=session, verify_key=verify_key)
        exported = True
        session.commit()
    except Exception:
        session.rollback()
        if exported:
            try:
                _export_store(tenant_id, verify_key=verify_key)
            except Exception:
                logger.exception(
                    "凭据导出补偿失败（DB 已回滚，旁文件可能超前）: tenant=%s",
                    tenant_id,
                )
        raise


def reconcile_exports() -> Dict[str, Any]:
    """启动对账：以 DB 为权威重建/刷新导出文件（A66-03/A66-04）。

    - **缺失重建**：DB 有凭据而文件不存在（容器无卷/cwd 漂移/误删）→ 写入；
    - **陈旧刷新**：文件存在但内容可能落后或超前 DB（#251 导出→commit 窗口
      被 SIGKILL 打断；旧版本按 cwd 留副本）→ 按 DB 全量重写，空租户写
      ``{}`` 清掉鬼凭据；
    - **多余告警**：目录里出现没有对应租户的旁文件（旧版本残留/手工复制）
      → WARNING，不静默删除；
    - **孤儿行**：凭据行所属租户已不存在（旧版本删租户未清凭据）→ 不导出
      并 WARNING，避免同 slug 重建后经陈旧文件继承；
    - **fail-loud**：任何写入失败都上抛，由 lifespan 拒绝启动——否则引擎会
      带着缺失/陈旧的密钥继续跑，控制台却显示正常。

    DB 无凭据且文件不存在时不落盘（不为空部署凭空创建文件）。返回摘要供
    启动日志/测试断言。
    """
    store = get_flow_store()
    with store._session_local() as session:
        # SQLite：先取得写锁，读到写文件期间 save/delete/delete_tenant 的
        # flush 被阻塞（它们也先 flush 再导出）。否则启动对账可能读旧快照、
        # 在另一个副本提交新值之后才落盘，把文件写回旧世代（A66-01 复发）。
        # 其他后端由部署契约要求显式共享 PLAITA_CREDENTIALS_FILE；整文件
        # RMW 的并发丢更新是既有 A42-13，不在本轮扩面。
        begin_guarded_transaction(session)
        tenant_ids = set(session.scalars(select(Tenant.id)).all())
        known = tenant_ids | {DEFAULT_TENANT_ID}
        by_tenant: Dict[str, Dict[str, Any]] = {}
        rows_by_tenant: Dict[str, List[Any]] = {}
        for r in session.scalars(select(Credential)).all():
            tid = r.tenant_id or DEFAULT_TENANT_ID
            rows_by_tenant.setdefault(tid, []).append(r)
            by_tenant.setdefault(tid, {})[r.name] = {
                "type": r.type,
                "data": r.data_json,
            }
        orphan_tenants = sorted(set(by_tenant) - known)
        if orphan_tenants:
            logger.warning(
                "凭据库存在无对应租户行的凭据（旧版本删除残留，未导出）: %s",
                ", ".join(orphan_tenants),
            )
        exports = {tid: payload for tid, payload in by_tenant.items() if tid in known}

        # 有凭据的租户必须落盘；已有文件的租户（即使已清空）也要按 DB 刷新。
        targets = set(exports)
        for tid in known:
            if credentials_file(tid).is_file():
                targets.add(tid)

        rebuilt = refreshed = purged = key_mismatch = 0
        mismatched_tenants: List[str] = []
        for tid in sorted(targets):
            path = credentials_file(tid)
            payload = exports.get(tid, {})
            existed = path.is_file()
            include_meta = False
            if payload:
                # A2R25-10：对账不得在错/缺 key 时重写导出——那会把旧密文
                # 与「当前 key 的 key_id」拼在一起，并制造混合密钥的假象。
                # 无法解开就跳过该租户，原文件逐字节保留。
                try:
                    undecryptable = _first_undecryptable(
                        rows_by_tenant.get(tid, [])
                    )
                except CredentialsKeyMismatchError as e:
                    key_mismatch += 1
                    mismatched_tenants.append(tid)
                    logger.warning(
                        "启动对账跳过租户 %s 的凭据导出（原文件保留）：%s",
                        tid,
                        e,
                    )
                    continue
                if undecryptable is not None:
                    key_mismatch += 1
                    mismatched_tenants.append(tid)
                    logger.warning(
                        "启动对账跳过租户 %s 的凭据导出（原文件保留）：条目 %s "
                        "无法用当前密钥解密",
                        tid,
                        undecryptable,
                    )
                    continue
                include_meta = True
            path.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write_text(
                path,
                json.dumps(
                    _export_payload(payload, include_meta=include_meta),
                    ensure_ascii=False,
                    indent=1,
                ),
            )
            if existed:
                refreshed += 1
                if not payload:
                    purged += 1
            else:
                rebuilt += 1
            logger.info(
                "启动对账导出租户 %s 凭据文件 %s（%d 条）",
                tid,
                path,
                len(payload),
            )

        # 多余旁文件：没有对应租户的文件不被任何写入路径收敛，只能告警。
        base = credentials_file()
        expected = {credentials_file(tid).resolve() for tid in known}
        extras: List[str] = []
        if base.parent.is_dir():
            for candidate in sorted(
                base.parent.glob(f"{base.stem}.*{base.suffix}")
            ):
                if candidate.resolve() not in expected:
                    extras.append(str(candidate))
                    logger.warning(
                        "凭据导出目录存在无对应租户的文件（陈旧副本/手工残留）: %s",
                        candidate,
                    )
        session.commit()

    summary = {
        "rebuilt": rebuilt,
        "refreshed": refreshed,
        "purged": purged,
        "extra_files": extras,
        "orphan_tenants": orphan_tenants,
        "key_mismatch": key_mismatch,
        "mismatched_tenants": mismatched_tenants,
    }
    logger.info(
        "凭据导出对账完成：重建 %d，刷新 %d（其中清空 %d），多余文件 %d，"
        "密钥不匹配跳过 %d",
        rebuilt,
        refreshed,
        purged,
        len(extras),
        key_mismatch,
    )
    return summary
