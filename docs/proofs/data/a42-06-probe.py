"""A42-06 探针：tenant-b 的 subprocess 用户代码能否拿到 default 明文。

复现 r42 探针的两种取法：
1. 按子进程 env 里的 ``PLAITA_CREDENTIALS_FILE`` / ``PLAITA_CREDENTIALS_KEY_FILE``
   直接读文件并用 Fernet 解密（raw_read）；
2. ``set_current_tenant("default")`` 改写路由后走公开 API（api_read）。

修复前：两种都拿到 ``DEFAULT-ONLY-SECRET``。
修复后：spawn 前抛 ``TenantContextPropagationError``，用户代码零执行。

用法::

    .venv/bin/python docs/proofs/data/a42-06-probe.py
"""
import json
import os
import sys
import tempfile
from pathlib import Path

from cryptography.fernet import Fernet

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from plaita import subprocess_env as _env  # noqa: E402
from plaita.node.code import run_python_subprocess  # noqa: E402
from plaita.tenant_context import (  # noqa: E402
    reset_current_tenant,
    set_current_tenant,
)

PROBE = """
def run(_):
    import json
    import os

    from cryptography.fernet import Fernet

    from plaita.credentials import get_credential
    from plaita.tenant_context import set_current_tenant

    base = os.environ.get("PLAITA_CREDENTIALS_FILE")
    key_file = os.environ.get("PLAITA_CREDENTIALS_KEY_FILE")
    raw = None
    if base and key_file and os.path.isfile(base) and os.path.isfile(key_file):
        key = open(key_file, "rb").read().strip()
        store = json.load(open(base))
        raw = json.loads(
            Fernet(key).decrypt(store["default-only"]["data"].encode())
        )["token"]

    set_current_tenant("default")
    api = get_credential("default-only")["token"]
    return {
        "raw_read": raw,
        "api_read": api,
        "env_paths_visible": bool(base and key_file),
    }
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


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a42-06-"))
    key = Fernet.generate_key()
    (tmp / ".plaita-credentials.key").write_bytes(key)
    _write_store(tmp / ".plaita-credentials.json", key, {
        "shared-secret": {"token": "DEFAULT-TENANT-SECRET"},
        "default-only": {"token": "DEFAULT-ONLY-SECRET"},
    })
    _write_store(tmp / ".plaita-credentials.tenant-b.json", key, {
        "shared-secret": {"token": "TENANT-B-SECRET"},
    })

    os.chdir(tmp)
    os.environ["PLAITA_CREDENTIALS_FILE"] = str(tmp / ".plaita-credentials.json")
    os.environ["PLAITA_CREDENTIALS_KEY_FILE"] = str(tmp / ".plaita-credentials.key")
    _env.SUBPROCESS_ENV_EXTRA["PYTHONPATH"] = str(REPO)

    token = set_current_tenant("tenant-b")
    try:
        result = run_python_subprocess(PROBE, None)
    except Exception as exc:  # noqa: BLE001 — 探针输出修复后的具名拒绝
        print(json.dumps({
            "ok": False,
            "type": type(exc).__name__,
            "error": str(exc),
        }, ensure_ascii=False, indent=2))
        return 0
    finally:
        reset_current_tenant(token)
    print(json.dumps({"ok": True, "result": result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
