"""console SQLite 敏感列的字段级加密（A2R25-08/09 余量）。

A2R25-08 在 r48 已把 DB 文件收紧到 0600、把凭据文件/默认库放进
``.gitignore``；但库里仍有三处**明文**列：

- ``local_executions.input_json``：本地执行的 ``params``（含历史明文凭据）；
- ``local_executions.context_json``：checkpoint（含 ``$INPUT``/节点输出）；
- ``tenants.contract_secret_key``：对外 HMAC 契约密钥。

0600 只挡同机其他 UID；备份、``shutil.copy2``、归档、误提交都不受它保护。
本模块在**持久化边界**做字段级加密：SQLAlchemy ``TypeDecorator`` 保证
所有 ORM 写入（含未来新增调用点）先加密、所有读取先解密；直接用
``sqlite3``/``SELECT`` 的「裸读」只能拿到 ``enc:v1:`` 密文。

密钥来源与凭据加密**同源**（``PLAITA_CREDENTIALS_KEY`` 或 DB 同目录的
``.plaita-credentials.key``），但按用途经 HKDF 派生独立子密钥——同一把
主密钥、不同用途的密文互不通用。丢失密钥会让历史执行/租户契约密钥不可
解（fail-loud），备份时必须同时保护密钥文件。

降级边界（必须可观测）：仅在**无法建立密钥**（只读目录、密钥文件损坏）
时退回明文并打 WARNING；已配置合法密钥后的加解密异常一律抛错，绝不静默
落明文。内存 SQLite 没有落盘面，按设计跳过加密（debug 级说明）。

存量行由 :func:`backfill_sensitive_columns` 在 ``init_engine`` 后一次性
回填（幂等；只处理没有 ``enc:v1:`` 前缀的行），SQLite 回填后 ``VACUUM``
清掉旧页残留。明文存量在回填前仍可读（解密侧对无前缀值按历史明文放行）。
"""
from __future__ import annotations

import base64
import logging
import os
import stat
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from sqlalchemy import MetaData, Text, select
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.types import TypeDecorator

logger = logging.getLogger(__name__)

#: 密文标记（同时作为 AES-GCM 的 AAD，防止跨格式/跨版本误读）。
ENCRYPTED_PREFIX = "enc:v1:"
_PREFIX_BYTES = ENCRYPTED_PREFIX.encode()
#: 96-bit 随机 nonce；AES-GCM 推荐长度。
_NONCE_BYTES = 12
#: 回填批大小：单事务过大在 SQLite 上会长时间持写锁。
_BACKFILL_BATCH = 200

#: (表, 列, 用途)。用途进入 HKDF info，不同列的子密钥互不通用。
SENSITIVE_COLUMNS: Tuple[Tuple[str, str, str], ...] = (
    ("local_executions", "input_json", "local-execution-input"),
    ("local_executions", "context_json", "local-execution-context"),
    ("tenants", "contract_secret_key", "tenant-contract-secret"),
)


class FieldCryptoError(RuntimeError):
    """字段加解密失败（密钥不匹配/密文损坏/无可解析密文格式）。

    消息面向运维：给修复方向，不回显明文或密钥材料。
    """


class _State:
    """一次 ``configure`` 的解析结果。

    ``enabled=False`` 时 ``encrypt_value`` 按历史语义落明文；``reason`` 只用于
    日志分级：``in-memory`` 是设计内跳过，``no-key`` 是必须告警的降级。
    """

    __slots__ = ("db_url", "enabled", "reason", "key_source")

    def __init__(
        self,
        db_url: Optional[str],
        enabled: bool,
        reason: str,
        key_source: Optional[Tuple[str, Any]],
    ) -> None:
        self.db_url = db_url
        self.enabled = enabled
        self.reason = reason
        self.key_source = key_source


_state = _State(None, False, "unconfigured", None)
_state_lock = threading.Lock()
_master_key: Optional[bytes] = None
_purpose_keys: Dict[str, bytes] = {}
_warned: set[str] = set()


def reset_for_tests() -> None:
    """清空进程级缓存（测试在同一进程里切 DB/密钥时使用）。"""
    global _state, _master_key, _purpose_keys
    with _state_lock:
        _state = _State(None, False, "unconfigured", None)
        _master_key = None
        _purpose_keys = {}
    _warned.clear()


def _warn_once(key: str, message: str, *args: Any) -> None:
    if key in _warned:
        return
    _warned.add(key)
    logger.warning(message, *args)


def _credentials_key_file() -> Path:
    """未显式配置时的密钥文件路径（与 ``plaita.credentials`` 同源）。

    只用于无法从引擎 URL 推断目录的后端（PG/``file:`` URI）：沿用凭据侧的
    解析规则（含一次性 cwd 回退 WARNING）。
    """
    from plaita.credentials import credentials_key_file

    return credentials_key_file()


def _resolve_key_source(db_url: Optional[str]) -> _State:
    """解析主密钥来源，不读取密钥内容（读取推迟到首次加/解密）。

    - ``PLAITA_CREDENTIALS_KEY``：内联 Fernet key，最高优先级（与凭据一致）；
    - ``PLAITA_CREDENTIALS_KEY_FILE``：显式密钥文件（相对路径按各进程 cwd
      解析，与凭据侧同一条 A67-04 WARNING）；
    - SQLite 文件库：DB 同目录 ``.plaita-credentials.key``（r48 的 0600 面；
      相对 DB URL 同样告警）；
    - 内存 SQLite：无落盘面，跳过加密；
    - 其他后端：沿用凭据侧路径解析（PG 无文件权限，密钥文件由部署注入）。
    """
    env_key = os.environ.get("PLAITA_CREDENTIALS_KEY", "").strip()
    if env_key:
        return _State(db_url, True, "env", ("env", env_key.encode()))
    explicit_file = os.environ.get("PLAITA_CREDENTIALS_KEY_FILE", "").strip()
    if explicit_file:
        key_path = Path(explicit_file)
        if not key_path.is_absolute():
            # 字段加密密钥与凭据密钥同源：相对显式路径按各进程 cwd 解析，
            # 多副本会各自加密/解密不同文件——不得静默（A67-04）。
            from plaita.credentials import _warn_relative_explicit_path

            _warn_relative_explicit_path(
                "PLAITA_CREDENTIALS_KEY_FILE", "密钥文件", key_path
            )
        return _State(db_url, True, "file", ("file", key_path))
    if not db_url:
        return _State(db_url, True, "file", ("file", _credentials_key_file()))
    try:
        url = make_url(db_url)
    except Exception:  # noqa: BLE001 — 非法 URL 交给引擎报错，这里只做密钥解析
        return _State(db_url, True, "file", ("file", _credentials_key_file()))
    if url.get_backend_name() != "sqlite":
        return _State(db_url, True, "file", ("file", _credentials_key_file()))
    database = url.database
    if not database or database == ":memory:":
        return _State(db_url, False, "in-memory", None)
    if database.startswith("file:"):
        return _State(db_url, True, "file", ("file", _credentials_key_file()))
    path = Path(database)
    if not path.is_absolute():
        # 与凭据导出/密钥解析同一条 A67-04 告警：相对 DB URL 的「同值」
        # 在不同进程 cwd 下会落到不同目录，字段加密密钥也会跟着分叉。
        from plaita.credentials import _warn_relative_sqlite_url

        _warn_relative_sqlite_url(db_url, path, "密钥文件")
        path = Path.cwd() / path
    from plaita.credentials import DEFAULT_KEY_FILE

    return _State(db_url, True, "file", ("file", path.parent / DEFAULT_KEY_FILE))


def configure(db_url: Optional[str]) -> None:
    """为本次引擎初始化选择密钥来源（``init_engine`` 在连接前调用）。"""
    global _state, _master_key, _purpose_keys
    resolved = _resolve_key_source(db_url)
    with _state_lock:
        _state = resolved
        _master_key = None
        _purpose_keys = {}


def _create_key_file(key_file: Path) -> bytes:
    """O_EXCL + 0600 原子创建主密钥文件（与 credentials_svc 同口径）。

    并发首建只允许一个赢家；落败者等待并复用赢家写入的 key（同一 DB 的
    多个副本必须共用一把主密钥，否则各自加密的行互不可解）。
    """
    from plaita.credentials import _read_key_file

    key = Fernet.generate_key()
    try:
        key_file.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise FieldCryptoError(
            f"无法创建字段加密密钥目录 {key_file.parent}: {exc}"
        ) from exc
    try:
        fd = os.open(key_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        logger.info("字段加密密钥文件已由另一进程创建，复用: %s", key_file)
        # 滚动升级边界：旧版本可能刚以 0644 写好了胜者 key（A67-05）。
        # 落败者复用前同样收紧，否则宽权限会一直留到下次读取/重启。
        from plaita.credentials import _tighten_key_file_permissions

        _tighten_key_file_permissions(key_file)
        return _read_key_file(key_file)
    except OSError as exc:
        raise FieldCryptoError(
            f"无法创建字段加密密钥文件 {key_file}: {exc}"
        ) from exc
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(key)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            key_file.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    logger.info("已生成字段加密密钥文件（0600）: %s", key_file)
    return key


def _load_master_key(*, generate: bool) -> bytes:
    """读取/生成主密钥原始字节（Fernet key 已是 32 字节的 urlsafe base64）。"""
    global _master_key
    with _state_lock:
        state = _state
        cached = _master_key
    if cached is not None:
        return cached
    if not state.enabled or state.key_source is None:
        raise FieldCryptoError(
            "未配置字段加密密钥：请设置 PLAITA_CREDENTIALS_KEY 或 "
            "PLAITA_CREDENTIALS_KEY_FILE 指向密钥文件"
        )
    kind, source = state.key_source
    if kind == "env":
        key = source
    else:
        key_file = Path(source)
        if key_file.is_file():
            from plaita.credentials import (
                _read_key_file,
                _tighten_key_file_permissions,
            )

            _tighten_key_file_permissions(key_file)
            key = _read_key_file(key_file)
        elif generate:
            key = _create_key_file(key_file)
        else:
            raise FieldCryptoError(
                f"字段加密密钥文件不存在: {key_file}；请恢复备份的密钥文件"
                "（没有密钥无法解密已加密的执行/租户数据）"
            )
    key = key.strip()
    try:
        Fernet(key)  # 只做格式校验，不用于本模块的 AES-GCM
        raw = base64.urlsafe_b64decode(key)
    except Exception as exc:  # noqa: BLE001 — 统一转成可操作的诊断
        raise FieldCryptoError(
            "字段加密密钥不是合法 Fernet key：请核对 PLAITA_CREDENTIALS_KEY "
            "或密钥文件内容"
        ) from exc
    with _state_lock:
        if _master_key is None:
            _master_key = raw
        return _master_key


def _purpose_key(purpose: str) -> bytes:
    """按用途派生 32 字节 AES-GCM key（HKDF-SHA256，info 含版本与用途）。"""
    with _state_lock:
        cached = _purpose_keys.get(purpose)
    if cached is not None:
        return cached
    derived = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=f"plaita-console:field:v1:{purpose}".encode(),
    ).derive(_load_master_key(generate=True))
    with _state_lock:
        _purpose_keys.setdefault(purpose, derived)
        return _purpose_keys[purpose]


def _encryption_available(*, generate: bool) -> bool:
    """密钥是否可用；不可用时按设计跳过或告警降级。"""
    with _state_lock:
        state = _state
    if not state.enabled:
        if state.reason != "in-memory":
            _warn_once(
                "disabled:" + str(state.reason),
                "字段加密未启用（%s）：敏感列将按历史语义落明文（A2R25-08 降级）",
                state.reason,
            )
        return False
    try:
        _load_master_key(generate=generate)
        return True
    except FieldCryptoError as exc:
        _warn_once(
            "no-key:" + str(state.db_url),
            "字段加密密钥不可用（%s）：敏感列将落明文（A2R25-08 降级，"
            "备份/导出按凭据泄露处置）",
            exc,
        )
        return False


def encrypt_value(value: Optional[str], *, purpose: str) -> Optional[str]:
    """加密一个文本列值。空值/已加密值原样返回（幂等，回填可重跑）。"""
    if value is None or value == "":
        return value
    if value.startswith(ENCRYPTED_PREFIX):
        return value
    if not _encryption_available(generate=True):
        return value
    nonce = os.urandom(_NONCE_BYTES)
    token = AESGCM(_purpose_key(purpose)).encrypt(
        nonce, value.encode("utf-8"), _PREFIX_BYTES
    )
    return ENCRYPTED_PREFIX + base64.urlsafe_b64encode(nonce + token).decode("ascii")


def decrypt_value(value: Optional[str], *, purpose: str) -> Optional[str]:
    """解密一个文本列值。无前缀 = 历史明文，原样返回（升级窗口兼容）。"""
    if value is None or value == "":
        return value
    if not value.startswith(ENCRYPTED_PREFIX):
        return value
    try:
        _load_master_key(generate=False)
    except FieldCryptoError as exc:
        raise FieldCryptoError(
            f"字段密文无法解密（{purpose}）：{exc}。请恢复写入时的 "
            "PLAITA_CREDENTIALS_KEY/密钥文件；缺失密钥时数据不可恢复"
        ) from exc
    try:
        raw = base64.urlsafe_b64decode(value[len(ENCRYPTED_PREFIX):])
        nonce, token = raw[:_NONCE_BYTES], raw[_NONCE_BYTES:]
        plain = AESGCM(_purpose_key(purpose)).decrypt(
            nonce, token, _PREFIX_BYTES
        )
    except Exception as exc:  # noqa: BLE001 — InvalidTag/格式错误统一具名
        raise FieldCryptoError(
            f"字段解密失败（{purpose}）：当前密钥与写入时不匹配或密文损坏；"
            "请恢复写入时的 PLAITA_CREDENTIALS_KEY/密钥文件"
        ) from exc
    return plain.decode("utf-8")


class EncryptedText(TypeDecorator):
    """透明加密的 TEXT 列。

    ``process_bind_param`` 覆盖所有 ORM 写入（含 ``insert_local_execution``、
    ``update_local_execution``、租户创建/轮换），``process_result_value`` 保证
    任何 ORM 读取拿到的都是明文；裸 SQL 仍看到 ``enc:v1:`` 密文。
    """

    impl = Text
    cache_ok = True

    def __init__(self, *args: Any, purpose: str = "default", **kwargs: Any) -> None:
        self.purpose = purpose
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        return encrypt_value(str(value), purpose=self.purpose)

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        return decrypt_value(str(value), purpose=self.purpose)


def _existing_tables(engine: Engine, wanted: Iterable[str]) -> set[str]:
    from sqlalchemy import inspect

    try:
        return set(inspect(engine).get_table_names()) & set(wanted)
    except Exception:  # noqa: BLE001 — 诊断只读，失败按「没有可回填表」处理
        logger.warning("读取表清单失败，跳过敏感列回填", exc_info=True)
        return set()


def _has_plaintext(engine: Engine, metadata: MetaData) -> bool:
    """库里是否还有需要回填的非空明文行（空串/已加密不算）。"""
    for table_name, column_name, _ in SENSITIVE_COLUMNS:
        table = metadata.tables.get(table_name)
        if table is None or column_name not in table.c:
            continue
        column = table.c[column_name]
        try:
            with engine.connect() as conn:
                found = conn.execute(
                    select(column)
                    .where(
                        ~column.like(ENCRYPTED_PREFIX + "%")
                        & (column != "")
                        & column.is_not(None)
                    )
                    .limit(1)
                ).first()
        except Exception:  # noqa: BLE001 — 诊断失败时保守回填（会走生成密钥）
            logger.warning(
                "检查 %s.%s 是否有明文失败（按有明文处理）",
                table_name,
                column_name,
                exc_info=True,
            )
            return True
        if found is not None:
            return True
    return False


def _has_ciphertext(engine: Engine, metadata: MetaData) -> bool:
    """库里是否已有任一 ``enc:v1:`` 密文（决定缺 key 时能否安全新建 key）。"""
    for table_name, column_name, _ in SENSITIVE_COLUMNS:
        table = metadata.tables.get(table_name)
        if table is None or column_name not in table.c:
            continue
        try:
            with engine.connect() as conn:
                found = conn.execute(
                    select(table.c[column_name])
                    .where(table.c[column_name].like(ENCRYPTED_PREFIX + "%"))
                    .limit(1)
                ).first()
        except Exception:  # noqa: BLE001 — 只读诊断，失败按「有密文」保守处理
            logger.warning(
                "检查 %s.%s 是否已有密文失败（按有密文处理）",
                table_name,
                column_name,
                exc_info=True,
            )
            return True
        if found is not None:
            return True
    return False


def backfill_sensitive_columns(engine: Engine) -> Dict[str, int]:
    """把存量明文列加密（幂等；启动路径 best-effort）。

    只处理**没有** ``enc:v1:`` 前缀的行；每批一个事务，避免长持 SQLite 写锁。
    回填失败只告警不阻塞启动——但明文因此仍在库中，属于必须可观测的降级。
    SQLite 回填后 ``VACUUM`` 重建文件，尽量清掉旧页里的明文残留。
    """
    with _state_lock:
        state = _state
    if not state.enabled:
        return {}

    wanted = {table for table, _, _ in SENSITIVE_COLUMNS}
    tables = _existing_tables(engine, wanted)
    if not tables:
        return {}

    metadata = MetaData()
    try:
        metadata.reflect(bind=engine, only=sorted(tables))
    except Exception:  # noqa: BLE001
        logger.warning("反射敏感列所在表失败，跳过回填", exc_info=True)
        return {}

    # 已有密文却没有可用密钥：绝不生成新 key（那会把「密钥丢失」伪装成
    # 「混合密钥」——新行可读、旧行永久不可解）。库内只有明文时生成 key
    # 是安全的首启用路径。
    try:
        _load_master_key(generate=False)
    except FieldCryptoError as exc:
        if _has_ciphertext(engine, metadata):
            logger.error(
                "检测到已加密的敏感列，但当前无可用密钥（%s）；跳过回填。"
                "请先恢复写入时的密钥文件，否则这些数据不可恢复",
                exc,
            )
            return {}
        if not _has_plaintext(engine, metadata):
            # 全新库/无敏感行：不生成密钥文件（首次加密写入时再建）。
            return {}
        try:
            _load_master_key(generate=True)
        except FieldCryptoError as gen_exc:
            _warn_once(
                "backfill-no-key:" + str(state.db_url),
                "敏感列回填无法建立密钥（%s）：存量明文保持原样（A2R25-08 降级）",
                gen_exc,
            )
            return {}

    summary: Dict[str, int] = {}
    changed = 0
    for table_name, column_name, purpose in SENSITIVE_COLUMNS:
        table = metadata.tables.get(table_name)
        if table is None or column_name not in table.c:
            continue
        pk_names = [col.name for col in table.primary_key.columns]
        if not pk_names:
            continue
        column = table.c[column_name]
        select_cols = [table.c[name] for name in pk_names] + [column]
        # 空串（default 租户的空契约密钥）不在加密范围；必须一起排除，
        # 否则每批都会选中同一批空值行 → 回填永不收敛。
        predicate = ~column.like(ENCRYPTED_PREFIX + "%") & (column != "") & column.is_not(None)
        moved = 0
        while True:
            try:
                with engine.begin() as conn:
                    rows = conn.execute(
                        select(*select_cols)
                        .where(predicate)
                        .order_by(*[table.c[name] for name in pk_names])
                        .limit(_BACKFILL_BATCH)
                    ).all()
                    if not rows:
                        break
                    for row in rows:
                        raw = row[-1]
                        if raw is None or raw == "":
                            continue
                        token = encrypt_value(str(raw), purpose=purpose)
                        if token == raw:
                            continue
                        keys = row[:-1]
                        condition = [
                            table.c[name] == keys[idx]
                            for idx, name in enumerate(pk_names)
                        ]
                        conn.execute(
                            table.update()
                            .where(*condition)
                            .values({column_name: token})
                        )
                        moved += 1
            except Exception:  # noqa: BLE001 — 回填不阻塞启动，但不能静默
                logger.error(
                    "敏感列回填失败（%s.%s，已加密 %d 行）：库内可能仍有明文",
                    table_name,
                    column_name,
                    moved,
                    exc_info=True,
                )
                break
        if moved:
            summary[f"{table_name}.{column_name}"] = moved
            changed += moved
            logger.warning(
                "敏感列回填：%s.%s 已加密 %d 行（A2R25-08）",
                table_name,
                column_name,
                moved,
            )

    if changed and engine.url.get_backend_name() == "sqlite":
        try:
            with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as conn:
                conn.exec_driver_sql("VACUUM")
            logger.info("敏感列回填后 VACUUM 完成（清理旧页明文残留）")
        except Exception:  # noqa: BLE001 — VACUUM 失败不阻塞，残留面已告警
            logger.warning(
                "敏感列回填后 VACUUM 失败：旧页可能仍含明文残留，"
                "请离线执行 VACUUM 或按凭据泄露处置备份",
                exc_info=True,
            )
    return summary
