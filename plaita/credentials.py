"""凭据解析——节点运行时按名取用外部服务的机密信息。

存储形态：加密 JSON 文件（由编排台 console 在凭据保存时**原子替换**导出），
``PLAITA_CREDENTIALS_FILE`` 指定路径；未显式设置时与 console 同规则解析：
``PLAITA_CONSOLE_DB_URL`` 为 SQLite 时落在 **DB 同目录**的
``.plaita-credentials.json``（同一 DB 的多个副本因此读写同一份导出文件），
否则回退当前工作目录并打一次性 WARNING（A67-04）——回退只保证单进程可用，
多副本/PG 部署必须显式给出绝对路径的 ``PLAITA_CREDENTIALS_FILE`` /
``PLAITA_CREDENTIALS_KEY_FILE``，否则各进程按各自 cwd 读写不同文件。
``PLAITA_CREDENTIALS_KEY`` 为 Fernet 密钥（未设时尝试同级
``.plaita-credentials.key`` 文件）。加密/解密依赖 ``cryptography``
（``pip install plaita[credentials]``）。

文件缺失视为「尚未配置凭据」（空 store）；文件存在但为空/损坏则抛
:class:`CredentialStoreCorruptError`（fail-loud），不会静默当成空 store。
导出写失败不会破坏旧文件——旧内容仍可正常解析。

多租户：解析时读租户上下文（``plaita.tenant_context.current_tenant``，与
分布式存储/日志同源）——非 default 租户读旁文件
``<stem>.<tenant_id><suffix>``（console 凭据页按租户导出的同一命名），
default/空租户沿用历史文件（零行为变化）。**公开 API 路由层**的租户隔离由
文件路径保证：非 default 租户只看得见自己旁文件里的凭据，default 文件对它
不可见；报错信息只列**当前租户文件内**的凭据名，不泄露其他租户的存在。
这不是对同用户任意代码的安全边界——所有租户文件共用同一把密钥，能读文件
就能解密（A42-06）；code 节点 ``subprocess`` 后端因此对非 default 租户
fail-closed，租户级 code 节点须用 ``docker``（不挂载宿主凭据）。

节点内用法::

    from plaita.credentials import get_credential
    cred = get_credential("feishu-bot")   # -> {"url": "https://...", ...}

跨进程用法（plaita#256，A42-06 收窄）：ContextVar 不跨进程，子进程里的
``get_credential`` 不会自动继承父进程的租户。**subprocess 沙箱不是凭据隔离
边界**——子进程与 worker 同用户、文件系统不受限，且所有租户共用同一把
Fernet 密钥，租户代码可以直接读取 default 凭据文件，因此 code 节点
``subprocess`` 后端对非 default 租户 fail-closed（spawn 前拒绝，用户代码
零执行机会）。当前 :func:`subprocess_credentials_env` 只服务 default 租户
子进程：显式固定基础文件与密钥路径，保持历史语义。租户级 code 节点请用
``docker`` 后端（容器内不挂载宿主凭据）。
"""
from __future__ import annotations

import json
import logging
import os
import stat
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class CredentialError(RuntimeError):
    """凭据缺失/未配置/解密失败。message 面向用户，给出可操作的修复指引。"""


class CredentialStoreCorruptError(CredentialError):
    """凭据文件存在但为空/损坏/不可读——与「文件不存在（尚未配置）」区分。

    文件缺失时返回空 store（合法的「还没有凭据」）；文件存在却读不出完整
    JSON 时不能静默当成空 store，否则所有凭据会表现为「不存在」，把排查
    方向引向重新创建凭据，掩盖真正的导出写失败/文件截断。
    """


DEFAULT_FILE = ".plaita-credentials.json"
DEFAULT_KEY_FILE = ".plaita-credentials.key"
DEFAULT_TENANT_ID = "default"

# 导出文件的保留元数据键（A2R25-10）。console 在有密文时写入
# ``{"format": "plaita-credentials", "version": 2, "key_id": "<sha256 前 16 位>"}``；
# 引擎读侧必须忽略它，绝不能把它当成一条凭据（否则错误信息会列出内部键名）。
# 空 store 仍写 ``{}``（无密文可失配，保持旧引擎解析零变化）。
STORE_META_KEY = "__plaita_meta__"
STORE_FORMAT = "plaita-credentials"
STORE_FORMAT_VERSION = 2


# 默认路径回退告警按 (原因, 组件, 解析结果) 去重：引擎每次读凭据都会解析路径，
# 不能每个节点调用都刷一条 WARNING。A67-04 要求「显式配置缺失即告警」，同一
# 进程同一原因只告警一次即可被启动日志/运维看到。
_PATH_FALLBACK_WARNINGS: set[tuple[str, str, str]] = set()


def _reset_path_fallback_warnings() -> None:
    """清空回退告警去重状态（测试用；生产进程 env/cwd 不变，无需清理）。"""
    _PATH_FALLBACK_WARNINGS.clear()


def _warn_path_once(key: tuple[str, str, str], message: str, *args: Any) -> None:
    if key in _PATH_FALLBACK_WARNINGS:
        return
    _PATH_FALLBACK_WARNINGS.add(key)
    logger.warning(message, *args)


def _warn_cwd_fallback(component: str, reason: str, detail: str) -> Path:
    cwd = Path.cwd()
    _warn_path_once(
        (reason, component, str(cwd)),
        "凭据%s默认回退到当前工作目录 %s：%s。回退只保证单进程可用；"
        "多副本/PG 部署必须给所有 console/worker 显式注入同一绝对路径的 "
        "PLAITA_CREDENTIALS_FILE 与 PLAITA_CREDENTIALS_KEY_FILE，"
        "否则各进程会读写各自 cwd 下的凭据文件（A67-04）",
        component,
        cwd,
        detail,
    )
    return cwd


def _default_credentials_dir(component: str = "文件") -> Path:
    """默认凭据文件/密钥目录：SQLite 时与 DB 同目录，否则 cwd + WARNING。

    与 console ``services.credentials_svc`` 同源（后者委托本函数）。导出文件是
    DB 的派生缓存，按 cwd 落盘会让「同 DB 多副本」各持一份互不收敛的副本
    （A66-01 ~ A66-04）；按 DB 路径落盘则同一 SQLite 库的副本天然共享同一个
    文件。无法从 DB URL 推断共享目录（未设置/PG/内存库）或 URL 为相对路径时，
    仍按历史语义回退，但必须留下 WARNING（A67-04：升级后无 DB env 的 worker
    会继续解旧 cwd 文件，静默回退会让已吊销/已轮换的旧值继续服务）。
    """
    db_url = os.environ.get("PLAITA_CONSOLE_DB_URL", "").strip()
    if not db_url:
        return _warn_cwd_fallback(
            component, "unset", "未设置 PLAITA_CONSOLE_DB_URL"
        )
    if db_url.startswith("sqlite:///"):
        raw = db_url[len("sqlite:///"):]
        if raw and raw != ":memory:":
            path = Path(raw)
            directory = path.resolve().parent
            if not path.is_absolute():
                # 相对 URL 的值即使两端相同，也会按各自进程 cwd 解析到不同 DB。
                _warn_path_once(
                    ("relative_sqlite", component, str(path.resolve())),
                    "PLAITA_CONSOLE_DB_URL=%r 是相对路径：凭据%s按当前工作目录"
                    "解析到 %s（父目录 %s）。值相同不代表同一数据库；多副本"
                    "部署请改用绝对路径（A67-04）",
                    db_url,
                    component,
                    path.resolve(),
                    directory,
                )
            return directory
        return _warn_cwd_fallback(
            component,
            "memory",
            "PLAITA_CONSOLE_DB_URL 指向内存 SQLite，无法推断共享导出目录",
        )
    # 只记录 scheme：DB URL 可能含 userinfo 口令，WARNING 不得把它写进日志。
    scheme = db_url.split("://", 1)[0] if "://" in db_url else db_url.split(":", 1)[0]
    return _warn_cwd_fallback(
        component,
        f"non_sqlite:{scheme}",
        f"PLAITA_CONSOLE_DB_URL 的 scheme={scheme!r} 不是 sqlite:/// 文件 URL，"
        "无法推断 DB 同目录（URL 值已省略，避免泄露口令）",
    )


def credentials_base_file() -> Path:
    """未按租户展开的基础凭据文件路径。

    ``PLAITA_CREDENTIALS_FILE`` 显式设置时直接使用；否则与 console 导出一致，
    落在 DB 同目录（SQLite）或 cwd。空字符串按未设置处理。
    """
    explicit = os.environ.get("PLAITA_CREDENTIALS_FILE", "").strip()
    if explicit:
        return Path(explicit)
    return _default_credentials_dir("文件") / DEFAULT_FILE


def tenant_credentials_file(tenant_id: Optional[str], base: Optional[Path] = None) -> Path:
    """租户 → 凭据文件路径。default/空 = 基础文件（历史兼容），
    其余租户 = 旁文件 ``<stem>.<tenant_id><suffix>``（与 console 导出同规则）。"""
    if base is None:
        base = credentials_base_file()
    tid = tenant_id or DEFAULT_TENANT_ID
    if tid == DEFAULT_TENANT_ID:
        return base
    return base.with_name(f"{base.stem}.{tid}{base.suffix}")


def credentials_file() -> Path:
    """当前租户上下文对应的凭据文件（节点运行时经这里路由）。"""
    from plaita.tenant_context import current_tenant

    return tenant_credentials_file(current_tenant())


def credentials_key_file() -> Path:
    """凭据解密密钥文件路径（``PLAITA_CREDENTIALS_KEY_FILE`` 或 DB 同目录）。

    未显式设置时与 console 的 ``credentials_key_file`` 同规则：SQLite 场景
    落在 DB 同目录，否则 cwd——console 首次生成的密钥因此能被同 DB 副本的
    引擎直接找到，不必每个 cwd 各放一份。

    只返回**路径**；``PLAITA_CREDENTIALS_KEY`` 的内联密钥不在此暴露——跨进程
    调用方应显式传路径，不要把密钥材料复制进子进程环境（plaita#256）。
    """
    explicit = os.environ.get("PLAITA_CREDENTIALS_KEY_FILE", "").strip()
    if explicit:
        return Path(explicit)
    return _default_credentials_dir("密钥文件") / DEFAULT_KEY_FILE


def subprocess_credentials_env() -> Dict[str, str]:
    """default 租户子进程的显式凭据路由 env（plaita#256，A42-06 收窄）。

    返回可直接并入 ``plaita.subprocess_env.build_subprocess_env(extra=...)`` 的
    ``PLAITA_CREDENTIALS_FILE`` / ``PLAITA_CREDENTIALS_KEY_FILE``，让 default
    租户子进程的 ``get_credential()`` 与父进程看到同一文件（历史上它依赖
    cwd 落回，显式固定避免 cwd 漂移）。

    **不要用于非 default 租户**：租户间只靠文件路径隔离，而子进程与 worker
    同用户、文件系统不受限，且所有租户共用同一把解密密钥——把 base/key
    路径交给租户子进程等于把可解密所有租户文件的密钥交出去。code 节点
    ``subprocess`` 后端因此在 spawn 前拒绝非 default 租户（A42-06）；需要
    租户级 code 节点请用 ``docker`` 后端（不挂载宿主凭据）。

    密钥只传**路径**，不传内联 ``PLAITA_CREDENTIALS_KEY``——后者只存在于
    宿主环境，复制给沙箱代码等于直接交出密钥材料。
    """
    return {
        "PLAITA_CREDENTIALS_FILE": str(credentials_base_file()),
        "PLAITA_CREDENTIALS_KEY_FILE": str(credentials_key_file()),
    }


# chmod 失败只告警一次/路径，避免每次解密都刷日志（只读挂载是常见部署形态）。
_KEY_PERMISSION_WARNINGS: set[str] = set()


def _tighten_key_file_permissions(key_file: Path) -> bool:
    """把已存在密钥文件的权限收紧到 0600；返回本次是否执行了收紧。

    A67-05：历史版本先用 ``write_bytes``（umask 下通常 0644）创建 key 再
    ``chmod``，存量文件也可能一直是 0644。密钥是解密所有租户凭据的唯一材料，
    任何读取（引擎）与启动对账（console）都应顺手收紧。只读挂载等无法
    chmod 的场景只 WARNING，不阻断读取。
    """
    try:
        mode = stat.S_IMODE(key_file.stat().st_mode)
    except OSError:
        return False
    if mode & 0o077 == 0:
        return False
    try:
        os.chmod(key_file, 0o600)
    except OSError as e:
        key = str(key_file)
        if key not in _KEY_PERMISSION_WARNINGS:
            _KEY_PERMISSION_WARNINGS.add(key)
            # #151 日志卫生：异常文本必须经构造面掩码，不能把裸异常交给 logger。
            from plaita.core.url_redaction import _scrub_exception_text

            logger.warning(
                "凭据密钥文件 %s 权限为 %o（宽于 0600）且收紧失败（%s）；"
                "请检查文件属主/只读挂载（A67-05）",
                key_file,
                mode,
                _scrub_exception_text(e),
            )
        return False
    logger.warning(
        "凭据密钥文件 %s 权限为 %o（宽于 0600），已收紧为 0600（A67-05）",
        key_file,
        mode,
    )
    return True


def _read_key_file(key_file: Path, *, timeout: float = 1.0) -> bytes:
    """读取非空密钥内容；创建者可能刚 O_EXCL 建好文件尚未写入，短暂等待。

    空文件不能当成有效密钥（Fernet 会抛不直观的 ValueError），也不能静默
    重新生成（会覆盖并发创建者刚写入的 key，A67-01）。等待超时后 fail-loud。
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            data = key_file.read_bytes().strip()
        except FileNotFoundError:
            data = b""
        except OSError as e:
            raise CredentialError(
                f"无法读取凭据密钥文件 {key_file}：{e}"
            ) from e
        if data:
            return data
        if time.monotonic() >= deadline:
            raise CredentialError(
                f"凭据密钥文件 {key_file} 为空或不可读（另一个进程可能正在"
                "创建）；请检查文件内容后重试"
            )
        time.sleep(0.02)


def _load_key() -> bytes:
    key = os.environ.get("PLAITA_CREDENTIALS_KEY")
    if key:
        return key.encode()
    key_file = credentials_key_file()
    if key_file.is_file():
        _tighten_key_file_permissions(key_file)
        return _read_key_file(key_file)
    raise CredentialError(
        "未配置凭据解密密钥：请设置 PLAITA_CREDENTIALS_KEY（Fernet key），"
        "或提供 PLAITA_CREDENTIALS_KEY_FILE 指向密钥文件"
    )


def _fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError as e:
        raise CredentialError(
            "凭据功能需要 cryptography：pip install plaita[credentials]"
        ) from e
    return Fernet(_load_key())


def get_credential(name: str) -> Dict[str, Any]:
    """按名解析凭据，返回其数据 dict（如 {"url": ...} / {"username":..., "password":...}）。

    找不到或解密失败抛 :class:`CredentialError`，message 可直接展示给编排用户。
    """
    data = _read_store()
    entry = data.get(name)
    if entry is None:
        # 只列当前租户文件内的名字——跨租户文件互不可见，报错不得泄露
        # 其他租户配置了哪些凭据。
        known = ", ".join(sorted(data)) or "（无）"
        raise CredentialError(f"凭据 {name!r} 不存在（当前租户可用: {known}）。请在编排台「凭据」页创建")
    token = entry.get("data") if isinstance(entry, dict) else entry
    if token is None:
        raise CredentialError(f"凭据 {name!r} 内容为空")
    try:
        plain = _fernet().decrypt(token.encode()).decode()
        return json.loads(plain)
    except CredentialError:
        raise
    except Exception as e:  # noqa: BLE001 — 解密失败统一给出密钥指引
        raise CredentialError(
            f"凭据 {name!r} 解密失败（{e}）：请核对 PLAITA_CREDENTIALS_KEY 与写入时一致"
        ) from e


def credential_type(name: str) -> Optional[str]:
    """仅取凭据类型标签（不触碰密钥内容），不存在返回 None。"""
    entry = _read_store().get(name)
    if isinstance(entry, dict):
        return entry.get("type")
    return None


def _corrupt(path: Path, reason: str) -> CredentialStoreCorruptError:
    return CredentialStoreCorruptError(
        f"凭据文件 {path} {reason}。导出文件由编排台在保存凭据时重建："
        "请核对磁盘空间与文件权限后，在编排台「凭据」页重新保存任意一条凭据"
    )


def _read_store() -> Dict[str, Dict[str, Any]]:
    path = credentials_file()
    if not path.is_file():
        return {}
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        # is_file() 与读取之间的外部删除：仍按「尚未配置」处理。
        return {}
    except OSError as e:
        raise _corrupt(path, f"无法读取（{e}）") from e
    if not raw.strip():
        raise _corrupt(path, "为空（导出写入可能被中断/截断）")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise _corrupt(
            path, f"不是合法 JSON（{e.msg}，line {e.lineno} column {e.colno}）"
        ) from e
    if not isinstance(data, dict):
        raise _corrupt(path, f"顶层必须是对象，实际为 {type(data).__name__}")
    # A2R25-10：文件级 key id/版本头不是凭据。旧版本引擎没有这个过滤时，
    # 只会在「凭据不存在」的错误信息里多列一个内部键名；新版本直接剔除，
    # 凭据名集合与错误提示保持原样。
    data.pop(STORE_META_KEY, None)
    return data
