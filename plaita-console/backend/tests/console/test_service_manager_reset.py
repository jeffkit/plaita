"""#100：reset_service_manager() 不得丢弃托管实例表与 launcher 句柄。

修复前：reset 只把全局单例置空——已拉起实例（start_new_session 独立进程 /
docker 容器）的 pid/容器句柄随旧单例一起凭空消失。后果：
- /cluster/instances 显示为空，实际 worker 还在消费队列；
- /cluster/stop-all 遍历空表，端点 all({}) 把「没停任何东西」翻成 success:true；
- /cluster/stop/{id} 对脱管 id 回 404——stop-then-start 链路断裂，只能手工收尸。

本文件两条腿：
1) 单元级：构造带 fake-1 实例与进程句柄的管理器，reset 后断言实例/句柄被迁移
   而非无声消失，且 stop_service / stop_all 仍可用；
2) 端点级：instances 为空时 POST /api/cluster/stop-all 必须 success=false。
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import pytest  # noqa: E402
from fastapi import Depends, FastAPI  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

import auth as auth_module  # noqa: E402
from api import cluster as cluster_api  # noqa: E402
from services import cluster_registry as cluster_registry_module  # noqa: E402
from services import service_manager as service_manager_module  # noqa: E402

ADMIN_KEY = "sm-reset-admin-key"
HEADERS = {"X-Admin-API-Key": ADMIN_KEY}
# 任何真实系统都不存在的 pid/组 id（kill 路径在测试里被 monkeypatch 成 ESRCH）
GHOST_PID = 424242


def _settings():
    from config import Settings
    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _ghost_process(instance_id: str = "fake-1") -> SimpleNamespace:
    return SimpleNamespace(pid=GHOST_PID)


def _seed_old_manager(monkeypatch, tmp_path, instances=None, processes=None):
    """直接构造旧单例（不经 get_service_manager，避免加载仓库默认配置）。"""
    old = service_manager_module.ServiceManager()
    for iid in instances or {}:
        old.instances[iid] = service_manager_module.ManagedInstance(
            instance_id=iid,
            service_type="flow_worker",
            pid=GHOST_PID,
            status="running",
            start_time="2026-10-16T00:00:00",
        )
    for iid, proc in (processes or {}).items():
        old._process_launcher._processes[iid] = proc
    monkeypatch.setattr(service_manager_module, "_manager", old)
    return old


@pytest.fixture()
def registry_env(tmp_path, monkeypatch):
    """hermetic 集群注册表：reset 内部 get_service_manager() 会经它加载新配置。"""
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    registry = cluster_registry_module.ClusterRegistry(
        base_dir=str(tmp_path / "clusters"))
    registry.create_cluster("A", "集群A", redis_url="redis://cluster-a:6379/0")
    registry.switch_cluster("A")
    monkeypatch.setattr(cluster_registry_module, "_cluster_registry", registry)
    return registry


@pytest.fixture()
def no_real_kill(monkeypatch):
    """把 kill 路径打成确定性 ESRCH：单测不向任何真实进程/进程组发信号。"""
    import errno

    def _esrch(*args, **kwargs):
        raise ProcessLookupError(errno.ESRCH, "no such process (test stub)")

    monkeypatch.setattr(service_manager_module.os, "getpgid", _esrch)
    monkeypatch.setattr(service_manager_module.os, "killpg", _esrch)


# ---------------------------------------------------------------- 单元级

def test_reset_migrates_instances_and_handles(registry_env, monkeypatch, no_real_kill):
    """reset 后：实例表与 launcher 句柄迁移到新单例，stop 链路仍可用。"""
    old = _seed_old_manager(
        monkeypatch, tmp_path=None,
        instances=["fake-1"], processes={"fake-1": _ghost_process("fake-1")})

    counts = service_manager_module.reset_service_manager()

    new = service_manager_module.get_service_manager()
    assert new is not old
    # (a) 旧实例被迁移而非无声消失
    assert "fake-1" in new.instances
    assert new.instances["fake-1"].pid == GHOST_PID
    # 句柄跟着实例走
    assert "fake-1" in new._process_launcher._processes
    # 单实例收尸可用（句柄存在，kill 路径确定性 ESRCH → 视为已停止）
    assert asyncio.run(new.stop_service("fake-1")) is True
    # 迁移统计可观测
    assert counts["instances"] == 1
    assert counts["processes"] == 1


def test_stop_all_after_reset_not_empty(registry_env, monkeypatch, no_real_kill):
    """(b) reset 后 stop_all() 不再是空字典——脱管实例回到 stop 链路。"""
    _seed_old_manager(
        monkeypatch, tmp_path=None,
        instances=["fake-1"], processes={"fake-1": _ghost_process("fake-1")})
    service_manager_module.reset_service_manager()

    new = service_manager_module.get_service_manager()
    results = asyncio.run(new.stop_all())

    assert results == {"fake-1": True}


def test_reset_migrates_docker_handles(registry_env, monkeypatch):
    """容器句柄同样迁移；stop 按句柄类型选 launcher（mode 翻转也能收尸）。"""
    old = _seed_old_manager(monkeypatch, tmp_path=None, instances=["fake-d1"])
    old.instances["fake-d1"].container_id = "deadbeef"
    old._docker_launcher._containers["fake-d1"] = "deadbeef"
    monkeypatch.setattr(service_manager_module, "_manager", old)

    service_manager_module.reset_service_manager()

    new = service_manager_module.get_service_manager()
    assert "fake-d1" in new.instances
    assert new._docker_launcher._containers.get("fake-d1") == "deadbeef"

    calls = []

    async def fake_stop(inst, graceful=True):
        calls.append(inst.instance_id)
        return True

    new._docker_launcher.stop = fake_stop
    assert asyncio.run(new.stop_service("fake-d1")) is True
    assert calls == ["fake-d1"]


def test_reset_without_manager_creates_fresh(registry_env, monkeypatch):
    monkeypatch.setattr(service_manager_module, "_manager", None)
    counts = service_manager_module.reset_service_manager()
    assert counts == {"instances": 0, "processes": 0, "containers": 0}
    new = service_manager_module.get_service_manager()
    assert new.instances == {}


def test_reset_with_empty_manager_is_noop(registry_env, monkeypatch):
    _seed_old_manager(monkeypatch, tmp_path=None)
    counts = service_manager_module.reset_service_manager()
    assert counts == {"instances": 0, "processes": 0, "containers": 0}
    new = service_manager_module.get_service_manager()
    assert new.instances == {}


# ---------------------------------------------------------------- 端点级

def _app(registry_env, monkeypatch):
    app = FastAPI()
    app.state.local_mode = False
    app.include_router(cluster_api.router, prefix="/api",
                       dependencies=[Depends(auth_module.require_auth)])
    return app


def test_stop_all_empty_table_is_not_success(registry_env, monkeypatch):
    """instances == {} 时 /cluster/stop-all 不得把 all({}) 翻成 success:true。"""
    monkeypatch.setattr(service_manager_module, "_manager", None)
    client = TestClient(_app(registry_env, monkeypatch))

    r = client.post("/api/cluster/stop-all", json={"graceful": True}, headers=HEADERS)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["success"] is False
    assert body["results"] == {}


def test_stop_all_with_live_instance_reports_success(registry_env, monkeypatch, no_real_kill):
    """阳性对照：确有实例且停止成功时，success 必须仍是 true。"""
    _seed_old_manager(
        monkeypatch, tmp_path=None,
        instances=["fake-1"], processes={"fake-1": _ghost_process("fake-1")})
    client = TestClient(_app(registry_env, monkeypatch))

    r = client.post("/api/cluster/stop-all", json={"graceful": True}, headers=HEADERS)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["success"] is True
    assert body["results"] == {"fake-1": True}
