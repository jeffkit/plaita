"""第六十六轮 A66-01 ~ A66-04：凭据导出副本族收口（P2×4）。

审计事实（r66，探针 `run_matrix.py`/`run_fileloss.py`）：导出文件是 DB 的
派生缓存，却按 ``Path.cwd()`` 落盘且无启动对账。同一 SQLite DB + 同一
Fernet key、两个不同 cwd 的副本（``PLAITA_CREDENTIALS_FILE`` 不设）因此：

- **A66-01**：一个副本删除/轮换后，另一个副本的文件仍含旧值且引擎可解密；
  重试 DELETE 返回 404 且不重写文件；重启不重同步。
- **A66-02**：``delete_tenant`` 只 unlink 本 cwd 旁文件；同名重建（含 default
  删除后 bootstrap）让新租户经陈旧文件继承前代密钥。
- **A66-03**：文件丢失（容器无卷/cwd 漂移）后 DB/API 仍有凭据、引擎全部
  404，直到同租户下一次写入；启动无对账。
- **A66-04**：#251 的「导出成功→commit」窗口被 SIGKILL 打断：文件超前 DB
  留下不可撤销的鬼凭据，或落后 DB 使引擎读不到仍在控制台的凭据。

修复契约（本文件钉住）：

1. 导出文件默认路径与 DB 对齐（SQLite = DB 同目录），console 与引擎侧
   ``plaita.credentials`` 同源解析——同 DB 多副本天然共享同一份文件；
2. ``delete_credential`` 在 DB 无行时也按 DB 重导目标范围（404 也能撤销
   陈旧文件里的条目）；
3. ``create_tenant`` 清除同 slug 的孤儿凭据行与陈旧旁文件；default 行缺失时
   ``ensure_tenant_bootstrap`` 重建前清前代凭据行/导出文件（不允许继承）；
4. lifespan 启动对账：缺失重建、陈旧刷新、多余旁文件 WARNING、写失败
   拒绝启动（fail-loud）。

负控（必红）：
- 把 ``credentials_file`` / ``credentials_base_file`` 还原成 cwd 默认 →
  多副本用例的跨副本读取/删除/轮换断言红；
- 去掉 ``delete_credential`` 的无行重导 → 鬼凭据用例红；
- 去掉 ``create_tenant`` 的清理 → 旧旁文件继承用例红；
- 去掉启动对账 → 文件丢失/落后 DB 用例红。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
BACKEND_DIR = Path(__file__).resolve().parents[2]
PYTHON = sys.executable
MISSING = "MISSING"

_CONSOLE_PREFIX = textwrap.dedent(
    """
    import json, os
    from plaita.tenant_context import set_current_tenant
    from services import credentials_svc, flow_store, tenants_svc
    set_current_tenant("default", source="explicit")
    flow_store.init_engine(os.environ["PLAITA_CONSOLE_DB_URL"])
    store = flow_store.get_flow_store()
    """
)

_ENGINE_READ = textwrap.dedent(
    """
    import json, os
    from plaita.tenant_context import set_current_tenant
    from plaita.credentials import CredentialError, get_credential
    set_current_tenant(os.environ["PROBE_TENANT"], source="explicit")
    try:
        print(json.dumps(get_credential(os.environ["PROBE_NAME"])))
    except CredentialError:
        print("MISSING")
    """
)


class Replicas:
    """同一 SQLite DB + 同一 key，两个不同 cwd 的「副本」。

    每个动作都是独立 Python 进程、cwd 即副本目录，``PLAITA_CREDENTIALS_FILE``
    故意不设——与 r66 探针同前提；只共享 DB 与密钥文件。
    """

    def __init__(self, root: Path):
        self.root = root
        self.state = root / "state"
        self.copy1 = root / "copy1"
        self.copy2 = root / "copy2"
        for path in (self.state, self.copy1, self.copy2):
            path.mkdir(parents=True, exist_ok=True)
        self.db = self.state / "shared.db"
        self.key = self.state / "shared.key"
        self.creds = self.state / ".plaita-credentials.json"

        env = {k: v for k, v in os.environ.items() if not k.startswith("PLAITA_")}
        env.update(
            {
                "PLAITA_CONSOLE_DB_URL": f"sqlite:///{self.db}",
                "PLAITA_CREDENTIALS_KEY_FILE": str(self.key),
                "PYTHONPATH": os.pathsep.join([str(BACKEND_DIR), str(REPO_ROOT)]),
            }
        )
        self.env = env

    def _run(self, copy: str, code: str) -> str:
        cwd = self.copy1 if copy == "copy1" else self.copy2
        result = subprocess.run(
            [PYTHON, "-c", code],
            cwd=str(cwd),
            env=self.env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, (
            f"[{copy}] action failed rc={result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        return lines[-1] if lines else ""

    def console(self, copy: str, code: str, *, bootstrap: bool = False) -> str:
        prefix = _CONSOLE_PREFIX
        if bootstrap:
            prefix += "flow_store.ensure_tenant_bootstrap()\n"
        return self._run(copy, prefix + textwrap.dedent(code))

    def raw(self, copy: str, code: str) -> str:
        prefix = (
            _CONSOLE_PREFIX
            + "from sqlalchemy import delete\n"
            + "from models.flow import Credential, Tenant\n"
        )
        return self._run(copy, prefix + textwrap.dedent(code))

    def engine(self, copy: str, name: str, tenant: str = "default"):
        env = dict(self.env)
        env["PROBE_TENANT"] = tenant
        env["PROBE_NAME"] = name
        cwd = self.copy1 if copy == "copy1" else self.copy2
        result = subprocess.run(
            [PYTHON, "-c", _ENGINE_READ],
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        line = [ln for ln in result.stdout.splitlines() if ln.strip()][-1]
        return MISSING if line == MISSING else json.loads(line)

    def sidecar(self, tenant: str) -> Path:
        return self.creds.with_name(
            f"{self.creds.stem}.{tenant}{self.creds.suffix}"
        )

    def exported_names(self) -> dict:
        return json.loads(self.creds.read_text(encoding="utf-8"))


@pytest.fixture()
def replicas(tmp_path) -> Replicas:
    rep = Replicas(tmp_path)
    rep.console("copy1", "print('boot')", bootstrap=True)
    return rep


def _save(name: str, token: str, tenant: str = "default") -> str:
    return (
        f'credentials_svc.save_credential({name!r}, "api", '
        f'{{"token": {token!r}}}, tenant_id={tenant!r})\nprint("saved")'
    )


# -- A66-01：吊销 / 轮换 / 末条删除跨副本收敛 --------------------------------


def test_a66_01_export_path_follows_db_not_cwd(replicas):
    """默认路径落在 DB 同目录——两个副本的 cwd 里都没有导出文件。"""
    replicas.console("copy1", _save("cred-a", "SECRET-A"))

    assert replicas.creds.is_file(), "导出文件应与 DB 同目录"
    assert not (replicas.copy1 / ".plaita-credentials.json").exists()
    assert not (replicas.copy2 / ".plaita-credentials.json").exists()
    assert "PLAITA_CREDENTIALS_FILE" not in replicas.env


def test_a66_01_delete_and_rotation_converge_across_replicas(replicas):
    """p1 保存 → p2 可见；p2 删除/轮换 → p1 引擎立即失效/取新值。"""
    replicas.console("copy1", _save("cred-a", "SECRET-A"))
    assert replicas.engine("copy2", "cred-a") == {"token": "SECRET-A"}

    replicas.console("copy2", _save("cred-b", "SECRET-B"))
    assert replicas.engine("copy1", "cred-b") == {"token": "SECRET-B"}

    replicas.console("copy2", 'print(credentials_svc.delete_credential("cred-a"))')
    # 负控形态：旧实现里 p1 仍解出 SECRET-A
    assert replicas.engine("copy1", "cred-a") == MISSING
    assert replicas.engine("copy2", "cred-a") == MISSING
    assert "cred-a" not in replicas.exported_names()

    replicas.console("copy1", _save("cred-r", "OLD-SECRET"))
    replicas.console("copy2", _save("cred-r", "NEW-SECRET"))
    # 负控形态：旧实现里 p1 解出 OLD-SECRET
    assert replicas.engine("copy1", "cred-r") == {"token": "NEW-SECRET"}

    replicas.console("copy2", 'credentials_svc.delete_credential("cred-b")')
    replicas.console("copy2", 'credentials_svc.delete_credential("cred-r")')
    assert replicas.exported_names() == {}
    assert replicas.engine("copy1", "cred-b") == MISSING
    assert replicas.engine("copy1", "cred-r") == MISSING


# -- A66-02：租户删除 / 同名重建 / 默认租户 bootstrap 不继承前代 ------------


def test_a66_02_tenant_delete_is_shared_and_recreate_does_not_inherit(replicas):
    replicas.console(
        "copy1",
        'tenants_svc.create_tenant(store, "acme", "Acme")\nprint("created")',
    )
    replicas.console("copy1", _save("cred-t2", "TENANT-SECRET", tenant="acme"))
    sidecar = replicas.sidecar("acme")
    assert sidecar.is_file()
    assert replicas.engine("copy1", "cred-t2", tenant="acme") == {
        "token": "TENANT-SECRET"
    }

    replicas.console(
        "copy2",
        'tenants_svc.delete_tenant(store, "acme")\nprint("deleted")',
    )
    assert not sidecar.exists(), "共享导出路径下单副本删除必须清掉唯一副本"
    # 负控形态：旧实现里 p1 的 cwd 旁文件仍在，引擎解出 TENANT-SECRET
    assert replicas.engine("copy1", "cred-t2", tenant="acme") == MISSING

    replicas.console(
        "copy2",
        'tenants_svc.create_tenant(store, "acme", "Acme")\nprint("recreated")',
    )
    assert replicas.engine("copy1", "cred-t2", tenant="acme") == MISSING


def test_a66_02_recreate_clears_legacy_stale_sidecar_and_orphan_rows(replicas):
    """旧版本留下的「租户行已删、凭据行/旁文件仍在」：同名重建先清后建。"""
    replicas.console(
        "copy1",
        'tenants_svc.create_tenant(store, "legacy", "Legacy")\nprint("created")',
    )
    replicas.console("copy1", _save("cred-g", "LEGACY-SECRET", tenant="legacy"))
    sidecar = replicas.sidecar("legacy")
    assert sidecar.is_file()

    # 模拟旧版本删除：只删 Tenant 行，Credential 行与旁文件保留
    replicas.raw(
        "copy2",
        """
        with store._session_local() as session:
            session.execute(delete(Tenant).where(Tenant.id == "legacy"))
            session.commit()
        print("orphaned")
        """,
    )
    assert sidecar.is_file()
    assert replicas.engine("copy1", "cred-g", tenant="legacy") == {
        "token": "LEGACY-SECRET"
    }

    replicas.console(
        "copy2",
        'tenants_svc.create_tenant(store, "legacy", "Legacy")\nprint("created")',
    )
    assert not sidecar.exists(), "同名重建必须清掉前代旁文件"
    assert replicas.engine("copy1", "cred-g", tenant="legacy") == MISSING
    assert (
        replicas.console(
            "copy2",
            'print(len(credentials_svc.list_credentials("legacy")))',
        )
        == "0"
    ), "同名重建必须清掉前代孤儿凭据行（不得经 DB 继承）"


def test_a66_02_default_bootstrap_clears_legacy_orphan_credentials(replicas):
    """旧版本删除 default 只删租户行：bootstrap 重建前必须先清前代行/文件。"""
    replicas.console("copy1", _save("cred-d", "D-SECRET"))
    replicas.raw(
        "copy2",
        """
        with store._session_local() as session:
            session.execute(delete(Tenant).where(Tenant.id == "default"))
            session.commit()
        print("orphaned")
        """,
    )
    assert replicas.engine("copy1", "cred-d") == {"token": "D-SECRET"}

    replicas.console(
        "copy1",
        """
        flow_store.ensure_tenant_bootstrap()
        print(json.dumps(credentials_svc.reconcile_exports()))
        """,
    )
    assert replicas.engine("copy1", "cred-d") == MISSING
    assert not replicas.creds.exists(), "前代导出文件必须在重建前清掉"
    assert (
        replicas.console(
            "copy1",
            'print(len(credentials_svc.list_credentials("default")))',
        )
        == "0"
    ), "前代孤儿凭据行不得被重建的 default 继承"


def test_a66_02_default_delete_then_bootstrap_does_not_inherit(replicas):
    replicas.console("copy1", _save("cred-d", "D-SECRET"))
    assert replicas.engine("copy1", "cred-d") == {"token": "D-SECRET"}

    replicas.console(
        "copy2",
        'tenants_svc.delete_tenant(store, "default")\nprint("deleted")',
    )
    assert replicas.engine("copy1", "cred-d") == MISSING

    # 重启路径：bootstrap 重建 default 租户行 + 启动对账（DB 权威）
    replicas.console(
        "copy1",
        """
        flow_store.ensure_tenant_bootstrap()
        print(json.dumps(credentials_svc.reconcile_exports()))
        """,
    )
    # 负控形态：旧实现 bootstrap 后 p1 文件仍在，引擎解出 D-SECRET
    assert replicas.engine("copy1", "cred-d") == MISSING

    replicas.console("copy1", _save("cred-new", "NEW-SECRET"))
    assert replicas.engine("copy1", "cred-d") == MISSING
    assert replicas.engine("copy1", "cred-new") == {"token": "NEW-SECRET"}


# -- A66-03：启动对账（文件丢失重建 / 多余告警 / fail-loud） ------------------


def test_a66_03_reconcile_rebuilds_export_lost_from_disk(replicas):
    replicas.console("copy1", _save("cred-a", "SECRET-A"))
    replicas.creds.unlink()  # 容器无卷 / cwd 漂移 / 误删
    assert replicas.engine("copy1", "cred-a") == MISSING

    summary = json.loads(
        replicas.console(
            "copy2", "print(json.dumps(credentials_svc.reconcile_exports()))"
        )
    )
    assert summary["rebuilt"] == 1
    # 负控形态：无启动对账时引擎继续 404，直到同租户下一次写入
    assert replicas.engine("copy1", "cred-a") == {"token": "SECRET-A"}
    assert replicas.engine("copy2", "cred-a") == {"token": "SECRET-A"}


def test_a66_03_reconcile_warns_extra_export_without_touching_it(replicas):
    replicas.console("copy1", _save("cred-a", "SECRET-A"))
    extra = replicas.state / ".plaita-credentials.ghost.json"
    extra.write_text("{}", encoding="utf-8")

    summary = json.loads(
        replicas.console(
            "copy2", "print(json.dumps(credentials_svc.reconcile_exports()))"
        )
    )
    assert str(extra) in summary["extra_files"]
    assert summary["extra_files"] == [str(extra)]
    assert extra.exists(), "多余文件只告警，不得静默删除（可能是手工放置的共享密钥）"


def test_a66_03_reconcile_failure_is_fail_loud(replicas, monkeypatch):
    """写失败必须上抛（lifespan 据此拒绝启动），不得静默留下分叉。"""
    replicas.console("copy1", _save("cred-a", "SECRET-A"))

    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{replicas.db}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(replicas.key))
    monkeypatch.delenv("PLAITA_CREDENTIALS_FILE", raising=False)
    from services import credentials_svc, flow_store  # noqa: E402

    flow_store.init_engine(f"sqlite:///{replicas.db}")
    monkeypatch.setattr(
        credentials_svc,
        "_atomic_write_text",
        lambda path, text: (_ for _ in ()).throw(OSError("disk full")),
    )
    with pytest.raises(OSError, match="disk full"):
        credentials_svc.reconcile_exports()


# -- A66-04：#251 导出→commit 窗口的进程死亡残面 ------------------------------


def test_a66_04_crash_save_ghost_is_revocable_by_delete_retry(replicas):
    """文件超前 DB 的鬼凭据：DELETE 返回 404 也必须按 DB 重写并撤销。"""
    replicas.console("copy1", _save("cred-old", "OLD-SECRET"))
    replicas.console("copy1", _save("cred-ghost", "GHOST-SECRET"))
    # 模拟 SIGKILL 落在导出成功→commit 之间：DB 回滚，文件保留新条目
    replicas.raw(
        "copy2",
        """
        with store._session_local() as session:
            session.execute(delete(Credential).where(Credential.name == "cred-ghost"))
            session.commit()
        print("rolled-back")
        """,
    )
    assert replicas.engine("copy1", "cred-ghost") == {"token": "GHOST-SECRET"}

    deleted = replicas.console(
        "copy2", 'print(credentials_svc.delete_credential("cred-ghost"))'
    )
    assert deleted == "False", "DB 无行时 DELETE 仍返回 False（404 语义不变）"
    # 负控形态：旧实现直接 return False，文件不重写，引擎仍解出 GHOST-SECRET
    assert replicas.engine("copy1", "cred-ghost") == MISSING
    assert replicas.engine("copy1", "cred-old") == {"token": "OLD-SECRET"}


def test_a66_04_crash_save_ghost_is_removed_by_startup_reconcile(replicas):
    replicas.console("copy1", _save("cred-old", "OLD-SECRET"))
    replicas.console("copy1", _save("cred-ghost", "GHOST-SECRET"))
    replicas.raw(
        "copy2",
        """
        with store._session_local() as session:
            session.execute(delete(Credential).where(Credential.name == "cred-ghost"))
            session.commit()
        print("rolled-back")
        """,
    )
    assert replicas.engine("copy1", "cred-ghost") == {"token": "GHOST-SECRET"}

    summary = json.loads(
        replicas.console(
            "copy1", "print(json.dumps(credentials_svc.reconcile_exports()))"
        )
    )
    assert summary["refreshed"] == 1
    assert replicas.engine("copy1", "cred-ghost") == MISSING
    assert replicas.engine("copy1", "cred-old") == {"token": "OLD-SECRET"}


def test_a66_04_crash_delete_file_behind_db_is_rebuilt_on_startup(replicas):
    replicas.console("copy1", _save("cred-k", "K-SECRET"))
    # 模拟 SIGKILL 落在删除的导出→commit 之间：DB 回滚，文件已不含该条目
    data = json.loads(replicas.creds.read_text(encoding="utf-8"))
    data.pop("cred-k")
    replicas.creds.write_text(json.dumps(data), encoding="utf-8")
    assert replicas.engine("copy1", "cred-k") == MISSING

    summary = json.loads(
        replicas.console(
            "copy2", "print(json.dumps(credentials_svc.reconcile_exports()))"
        )
    )
    assert summary["refreshed"] == 1
    assert replicas.engine("copy1", "cred-k") == {"token": "K-SECRET"}


# -- 路径/密钥同源：未显式配置 KEY_FILE 时引擎也能找到 console 生成的 key ----


def test_a66_03_default_key_file_follows_db_dir(tmp_path):
    rep = Replicas(tmp_path)
    env = {k: v for k, v in rep.env.items() if k != "PLAITA_CREDENTIALS_KEY_FILE"}
    rep.env = env
    rep.console("copy1", "print('boot')", bootstrap=True)
    rep.console("copy1", _save("cred-k", "K-SECRET"))

    assert (rep.state / ".plaita-credentials.key").is_file()
    assert not (rep.copy1 / ".plaita-credentials.key").exists()
    assert rep.engine("copy2", "cred-k") == {"token": "K-SECRET"}
