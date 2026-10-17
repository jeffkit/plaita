#!/usr/bin/env python3
"""plaita#212 repro / before-after probe.

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    PYTHONPATH=. .venv/bin/python docs/proofs/data/212-repro.py

Creates two dummy Fernet-encrypted credential files (default and ``tenant-b``),
sets the tenant + a custom fence ContextVar in the calling thread, and reads
them through ``async_gen_to_sync``'s running-loop worker.  Prints one JSON
document; credential values are dummy strings, not real secrets.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from contextvars import ContextVar
from pathlib import Path

from cryptography.fernet import Fernet

from plaita.core.async_utils import async_gen_to_sync
from plaita.credentials import CredentialError, credentials_file, get_credential
from plaita.tenant_context import current_tenant, set_current_tenant

_FENCE: ContextVar[str] = ContextVar("issue212_repro_fence", default="no-fence")


def _write(path: Path, key: bytes, entries: dict) -> None:
    store = {
        name: {
            "type": "generic",
            "data": Fernet(key).encrypt(json.dumps(data).encode()).decode(),
        }
        for name, data in entries.items()
    }
    path.write_text(json.dumps(store))


async def main() -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="plaita212-"))
    base = tmp / "creds.json"
    key = Fernet.generate_key()
    os.environ["PLAITA_CREDENTIALS_FILE"] = str(base)
    os.environ["PLAITA_CREDENTIALS_KEY"] = key.decode()
    _write(base.with_name("creds.tenant-b.json"), key, {
        "shared": {"url": "https://acme.example"},
    })
    _write(base, key, {
        "shared": {"url": "https://default.example"},
        "default-only": {"url": "https://default.example"},
    })

    set_current_tenant("tenant-b")
    _FENCE.set("fence-b")

    async def gen():
        default_only = "READ_OK"
        try:
            get_credential("default-only")
        except CredentialError:
            default_only = "CredentialError"
        yield {
            "tenant": current_tenant(),
            "fence": _FENCE.get(),
            "file": credentials_file().name,
            "shared": get_credential("shared")["url"],
            "default_only": default_only,
        }

    return list(async_gen_to_sync(gen()))[0]


def run() -> dict:
    return {
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "bridged": asyncio.run(main()),
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
