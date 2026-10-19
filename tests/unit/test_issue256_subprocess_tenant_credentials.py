"""plaita#256 / A42-06 回归：subprocess 沙箱不是凭据隔离边界。

## 沿革

``sandbox_backend="subprocess"`` 在**新解释器**里执行用户代码。ContextVar
不跨进程；#256 首版用 env 显式传播 ``PLAITA_TENANT_ID`` + 基础凭据文件/密钥
路径，并在子进程入口恢复租户。它解决了「租户 B 经公开 API 静默读到 default
文件」，但没有解决进程边界本身的信任问题（A42-06）：

- 子进程与 worker 同用户、**文件系统不受限**；
- 所有租户的凭据文件共用同一把 Fernet 密钥；
- 因此租户 B 的用户代码可以 ``set_current_tenant("default")`` 改写路由，
  或直接按 env 里的路径读取 default 文件并解密——探针拿到
  ``DEFAULT-ONLY-SECRET``。

把路径从 env 拿掉并不能修复：default 文件与密钥通常就在 worker cwd，子进程
仍可读；任何形式的「父进程解密后交给子进程」也绕不开同一文件系统。结论：
``subprocess`` 后端**不是**租户凭据隔离边界。

## 契约（A42-06 收窄）

- 非 default 租户：**spawn 之前**抛 :class:`TenantContextPropagationError`
  （消息含 ``PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE`` 与 ``docker``
  指引）；用户代码零执行机会，Popen 不被调用。
- 该拒绝同时覆盖 Python 与 JS subprocess 后端（同一文件系统边界）。
- default 租户：既有 subprocess 语义不变（凭据路由 env + cwd 文件照旧）。
- docker 后端不挂载宿主凭据；``unsafe`` / ``restricted`` 在宿主进程内运行，
  同样不是凭据边界（运营者不得向租户发放）。

负控（r42 探针 pre-fix 形态）：回退生产改动后，同一探针会返回
``{"default_only": "DEFAULT-ONLY-SECRET", "base": ..., "key_file": ...}``，
本文件的第一条用例必须精确红。
"""
import json
import os
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from plaita import subprocess_env as _env
from plaita.node import code as code_mod
from plaita.node.code import (
    TenantContextPropagationError,
    run_js_subprocess,
    run_python_subprocess,
)
from plaita.tenant_context import reset_current_tenant, set_current_tenant

REPO_ROOT = Path(__file__).resolve().parents[2]

# r42 探针形态：先落一个「用户代码已执行」标记，再按偷到的 env 路径直接解密
# default 文件，同时演示 set_current_tenant("default") 改写公开 API 路由。
# pre-fix 会返回 default-only 明文；修复后用户代码根本不会启动。
_PROBE = """
def run(input_value):
    with open(input_value["marker"], "w", encoding="utf-8") as f:
        f.write("USER-CODE-RAN")

    import os
    from plaita.credentials import get_credential
    from plaita.tenant_context import current_tenant, set_current_tenant

    set_current_tenant("default")
    return {
        "tenant_via_api": current_tenant(),
        "default_only_via_api": get_credential("default-only")["token"],
        "base_file": os.environ.get("PLAITA_CREDENTIALS_FILE"),
        "key_file": os.environ.get("PLAITA_CREDENTIALS_KEY_FILE"),
    }
"""

_READ_PROBE = """
def run(_):
    from plaita.credentials import CredentialError, credentials_file, get_credential
    from plaita.tenant_context import current_tenant, current_tenant_source

    result = {
        "tenant": current_tenant(),
        "source": current_tenant_source(),
        "file": credentials_file().name,
        "env_tenant": __import__("os").environ.get("PLAITA_TENANT_ID"),
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
    """worker 部署目录：cwd 同时有 default 凭据/密钥 + tenant-b 旁文件。

    与 r42 探针的真实布局一致（凭据文件与密钥就在 worker cwd）。修复前
    租户 B 子进程既能按 env 路径读 default 文件，也能按 cwd 相对路径读；
    修复后两者都不可达，因为用户代码不会启动。
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


def _run_as(tenant: str, code: str, input_value):
    token = set_current_tenant(tenant)
    try:
        return run_python_subprocess(code, input_value)
    finally:
        reset_current_tenant(token)


def test_tenant_b_subprocess_rejected_before_user_code(deployment_dir):
    """A42-06 主回归：租户 B 的 code 节点不被 spawn，拿不到 default 明文。

    负控（回退生产改动）时本用例精确红：probe 返回
    ``default_only_via_api == "DEFAULT-ONLY-SECRET"`` 且 marker 文件存在。
    """
    marker = deployment_dir / "user-code-ran.marker"

    with pytest.raises(TenantContextPropagationError) as exc:
        _run_as("tenant-b", _PROBE, {"marker": str(marker)})

    message = str(exc.value)
    assert "tenant-b" in message
    assert code_mod._SUBPROCESS_TENANT_ISOLATION_MARKER in message
    assert "docker" in message
    assert "DEFAULT-ONLY-SECRET" not in message
    assert not marker.exists(), "非 default 租户的用户代码不得执行"


def test_tenant_b_subprocess_never_spawns(monkeypatch):
    """拒绝发生在 Popen 之前：不创建子进程，连解释器都不启动。"""
    token = set_current_tenant("tenant-b")
    try:
        with monkeypatch.context() as m:
            m.setattr(
                code_mod.subprocess, "Popen",
                lambda *a, **k: pytest.fail("Popen must not be called"),
            )
            with pytest.raises(TenantContextPropagationError):
                run_python_subprocess("def run(_):\n    return 'x'", None)
    finally:
        reset_current_tenant(token)


def test_tenant_b_js_subprocess_rejected_before_spawn(monkeypatch):
    """JS subprocess 共享同一文件系统边界，同样 fail-closed（A42-06）。"""
    token = set_current_tenant("tenant-b")
    try:
        with monkeypatch.context() as m:
            m.setattr(
                code_mod.subprocess, "Popen",
                lambda *a, **k: pytest.fail("Popen must not be called"),
            )
            with pytest.raises(TenantContextPropagationError) as exc:
                run_js_subprocess("function run(i) { return i; }", 1)
    finally:
        reset_current_tenant(token)
    assert "tenant-b" in str(exc.value)
    assert "language=js" in str(exc.value)


def test_default_tenant_subprocess_unchanged(deployment_dir):
    """兼容正控：default 租户既有 subprocess 语义零变化。"""
    result = _run_as("default", _READ_PROBE, None)

    assert result["tenant"] == "default"
    assert result["source"] == "default"
    assert result["file"] == ".plaita-credentials.json"
    assert result["env_tenant"] is None
    assert result["shared"] == "DEFAULT-TENANT-SECRET"
    assert result["default_only"] == "READ_OK"


def test_default_tenant_does_not_require_plaita_in_child(tmp_path, monkeypatch):
    """default 不注入租户 env：不 import plaita 的子进程仍按历史行为执行。"""
    (tmp_path / "plaita.py").write_text(
        "raise ImportError('blocked for #256 default test')\n")
    monkeypatch.chdir(tmp_path)

    assert run_python_subprocess("def run(_):\n    return 'ok'", None) == "ok"
