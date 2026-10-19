"""plaita#256 回归：code 节点 subprocess 沙箱必须显式携带租户/凭据路由。

## 缺陷

``sandbox_backend="subprocess"`` 在**新解释器**里执行用户代码。ContextVar
不跨进程，子进程 ``current_tenant()`` 恒为 ``default``；``credentials_file()``
因此落回 cwd 的 default 基础文件。租户 B 的 code 节点调用公开 API
``get_credential("shared-secret")`` 会静默解出 default 租户的明文（#58 的
线程边界残面在进程边界复发）。

## 契约

- 非 default 租户：父进程显式经 env 传 ``PLAITA_TENANT_ID``（来源
  ``derived``）+ ``PLAITA_CREDENTIALS_FILE`` / ``PLAITA_CREDENTIALS_KEY_FILE``；
  子进程在**用户代码之前** ``set_current_tenant``，凭据 API 解析到本租户旁
  文件，default 独有凭据不可见。
- 子进程无法建立租户上下文（如沙箱解释器 import 不到 ``plaita``）：
  fail-closed，用户代码不执行、父进程抛
  :class:`TenantContextPropagationError`；绝不静默按 default 继续。
- default 租户：既有 subprocess 语义不变（不注入租户 env）。

负控：还原 pre-fix 的「不传 tenant/env」后，第一条用例必须精确红
（``shared-secret`` 解出 ``DEFAULT-TENANT-SECRET``）。
"""
import json
import os
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from plaita import subprocess_env as _env
from plaita.node.code import (
    TenantContextPropagationError,
    run_python_subprocess,
)
from plaita.tenant_context import reset_current_tenant, set_current_tenant

REPO_ROOT = Path(__file__).resolve().parents[2]

# 子进程是 fresh interpreter（sys.path[0] = cwd，env 白名单不含 PYTHONPATH）：
# 用例把 cwd 切到部署目录后，用公共 SUBPROCESS_ENV_EXTRA 显式给出源码树路径。
_PROBE = """
def run(_):
    import os
    from plaita.credentials import CredentialError, credentials_file, get_credential
    from plaita.tenant_context import current_tenant, current_tenant_source

    result = {
        "tenant": current_tenant(),
        "source": current_tenant_source(),
        "file": credentials_file().name,
        "env_tenant": os.environ.get("PLAITA_TENANT_ID"),
        "env_file": os.environ.get("PLAITA_CREDENTIALS_FILE"),
        "env_key_file": os.environ.get("PLAITA_CREDENTIALS_KEY_FILE"),
        "shared": get_credential("shared-secret")["token"],
    }
    try:
        get_credential("default-only")
    except CredentialError:
        result["default_only"] = "CredentialError"
    else:
        result["default_only"] = "READ_OK"
    return result
"""


def _write_store(path: Path, key: bytes, entries: dict) -> None:
    store = {
        name: {
            "type": "api",
            "data": Fernet(key).encrypt(json.dumps(data).encode()).decode(),
        }
        for name, data in entries.items()
    }
    path.write_text(json.dumps(store))


@pytest.fixture()
def deployment_dir(tmp_path, monkeypatch):
    """模拟 worker 部署目录：cwd 同时有 default 凭据/密钥 + tenant-b 旁文件。

    与探针 16 的真实布局一致（凭据文件与密钥就在 worker cwd），这样还原
    pre-fix runner 时子进程能按 cwd 的 default 文件解出明文。
    """
    key = Fernet.generate_key()
    (tmp_path / ".plaita-credentials.key").write_bytes(key)
    _write_store(tmp_path / ".plaita-credentials.json", key, {
        "shared-secret": {"token": "DEFAULT-TENANT-SECRET"},
        "default-only": {"token": "DEFAULT-ONLY-SECRET"},
    })
    _write_store(tmp_path / ".plaita-credentials.tenant-b.json", key, {
        "shared-secret": {"token": "TENANT-B-SECRET"},
    })

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(
        "PLAITA_CREDENTIALS_FILE", str(tmp_path / ".plaita-credentials.json"))
    monkeypatch.setenv(
        "PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / ".plaita-credentials.key"))
    monkeypatch.setitem(
        _env.SUBPROCESS_ENV_EXTRA, "PYTHONPATH", str(REPO_ROOT))
    return tmp_path


def _run_as(tenant: str):
    token = set_current_tenant(tenant)
    try:
        return run_python_subprocess(_PROBE, None)
    finally:
        reset_current_tenant(token)


def test_named_tenant_subprocess_reads_own_side_file(deployment_dir):
    """正控 1/2：租户 B 的 code 节点（subprocess）读到 B 的旁文件值。"""
    result = _run_as("tenant-b")

    assert result["tenant"] == "tenant-b", "子进程租户必须与所属执行一致"
    assert result["source"] == "derived", "跨进程传播来源应为 derived，不是 default"
    assert result["file"] == ".plaita-credentials.tenant-b.json"
    assert result["shared"] == "TENANT-B-SECRET"
    assert result["env_tenant"] == "tenant-b"
    # 传的是基础文件路径；子进程按显式租户展开一次，避免二次追加租户后缀。
    assert result["env_file"] == str(deployment_dir / ".plaita-credentials.json")
    assert result["env_key_file"] == str(deployment_dir / ".plaita-credentials.key")


def test_named_tenant_cannot_read_default_only_credential(deployment_dir):
    """#24/#215 口径：default 文件独有凭据对租户 B 不可见。"""
    result = _run_as("tenant-b")

    assert result["default_only"] == "CredentialError"
    assert result["shared"] != "DEFAULT-TENANT-SECRET"


def test_default_tenant_subprocess_unchanged(deployment_dir):
    """兼容性：default 租户既有 subprocess 语义零变化（不注入租户 env）。"""
    result = _run_as("default")

    assert result["tenant"] == "default"
    assert result["source"] == "default"
    assert result["file"] == ".plaita-credentials.json"
    assert result["env_tenant"] is None
    assert result["shared"] == "DEFAULT-TENANT-SECRET"
    assert result["default_only"] == "READ_OK"


def test_named_tenant_fails_closed_when_context_cannot_be_established(
    tmp_path, monkeypatch,
):
    """验收 3：子进程无法建立租户上下文时拒绝执行用户代码，不降级 default。"""
    # cwd 放一个 import 即失败的假 plaita：即使站点包已安装，python -c 的
    # sys.path[0]（cwd）也优先命中它，模拟沙箱解释器缺运行时。
    (tmp_path / "plaita.py").write_text(
        "raise ImportError('blocked for #256 fail-closed test')\n")
    monkeypatch.chdir(tmp_path)

    token = set_current_tenant("tenant-b")
    try:
        with pytest.raises(TenantContextPropagationError) as exc:
            run_python_subprocess(
                "def run(_):\n    return 'USER-CODE-RAN'", None)
    finally:
        reset_current_tenant(token)

    message = str(exc.value)
    assert "tenant-b" in message
    assert "PLAITA_TENANT_CONTEXT_UNAVAILABLE" in message
    assert "USER-CODE-RAN" not in message


def test_default_tenant_does_not_require_plaita_in_child(tmp_path, monkeypatch):
    """default 不注入租户 env：不 import plaita 的子进程仍按历史行为执行。"""
    (tmp_path / "plaita.py").write_text(
        "raise ImportError('blocked for #256 default test')\n")
    monkeypatch.chdir(tmp_path)

    assert run_python_subprocess("def run(_):\n    return 'ok'", None) == "ok"
