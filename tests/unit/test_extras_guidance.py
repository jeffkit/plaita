"""#99 回归：server extra 的探针必须覆盖 `plaita.server` 真正的导入期依赖。

旧实现里 ``_EXTRAS_GUIDE["server"]`` 只探 ``fastapi``——而 ``plaita/server/``
模块级真正 import 的是 ``cachetools`` / ``redis``。于是宿主恰好装了 fastapi
（例如宿主自己就是个 FastAPI 应用）时探针放行，``from plaita import FlowWorker``
抛裸 ``ModuleNotFoundError: No module named 'cachetools'``，全文没有
``pip install plaita[server]`` 指引。

同时仓库里躺着一份写对了多探针、零调用点的 ``plaita/server/__init__.py
::_require_server()``——「探针清单只能有一份」是这次漂移的教训，本文件把
「没有第二份清单」也钉成断言。
"""

from __future__ import annotations

import builtins
import importlib
import sys

import pytest

import plaita


SERVER_EXTRA_DEPS = ("fastapi", "uvicorn", "redis", "cachetools", "sqlalchemy")


def _block_import(monkeypatch, blocked: set[str]) -> None:
    """Simulate a host where *blocked* top-level distributions are absent."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.split(".")[0] in blocked:
            raise ImportError(f"No module named '{name}'", name=name.split(".")[0])
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    for module in [m for m in list(sys.modules) if m.split(".")[0] in blocked]:
        monkeypatch.delitem(sys.modules, module, raising=False)


@pytest.fixture
def uncached_flow_worker(monkeypatch):
    """Drop the lazy-export cache and any imported ``plaita.server.*`` modules so
    ``getattr(plaita, "FlowWorker")`` re-runs the probe + import path."""
    monkeypatch.delitem(plaita.__dict__, "FlowWorker", raising=False)
    for module in [m for m in list(sys.modules) if m == "plaita.server" or m.startswith("plaita.server.")]:
        monkeypatch.delitem(sys.modules, module, raising=False)
    return monkeypatch


def test_server_probe_covers_every_declared_server_dep():
    probe, install_cmd = plaita._EXTRAS_GUIDE["server"]
    probes = (probe,) if isinstance(probe, str) else tuple(probe)
    assert set(probes) == set(SERVER_EXTRA_DEPS), (
        "server 探针清单与 pyproject [project.optional-dependencies].server 漂移"
    )
    assert install_cmd == "pip install plaita[server]"


@pytest.mark.parametrize("missing", ["cachetools", "redis", "uvicorn", "sqlalchemy", "fastapi"])
def test_missing_server_dep_gets_actionable_extras_error(uncached_flow_worker, missing):
    _block_import(uncached_flow_worker, {missing})

    with pytest.raises(ImportError) as excinfo:
        getattr(plaita, "FlowWorker")

    message = str(excinfo.value)
    assert "server" in message
    assert "pip install plaita[server]" in message


def test_no_second_probe_list_in_the_server_package():
    """探针清单只能有一份：``plaita._EXTRAS_GUIDE``。"""
    server_pkg = importlib.import_module("plaita.server")
    assert not hasattr(server_pkg, "_require_server"), (
        "plaita/server/__init__.py 里又出现了一份（死）探针清单——"
        "续修方向是接进 plaita._EXTRAS_GUIDE，而不是并列维护第二份"
    )
