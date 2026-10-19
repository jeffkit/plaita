"""#211：``ServiceManager.start_service`` 的「计数 → 启动」必须原子。

修复前：计数只统计 ``status == "running"`` 的实例，``launcher.start`` 又是
``await``——两个并发请求都会在对方写入 ``instances`` 之前通过检查，
``max_instances=1`` 的 schedule_service 被拉起两份（cron 同一时点双触发）。

本组判据：
1. ``max_instances=1`` 两个并发 ⇒ ``launcher.start`` 恰好 1 次，另一个拿到
   「已在启动」类错误，``instances`` 只有 1 个；
2. ``max_instances=2`` 两个并发都成功（守卫：不许把并发启动锁死）；
3. 20 并发 / ``max_instances=3`` ⇒ 恰好 3 次启动；
4. 启动失败回滚预留：不留幽灵实例，同类型下一次请求能起来（占位泄漏会永久
   占满 ``max_instances``）；
5. 预留期间不得显示成实例（``instances`` / ``list_instances`` 无幽灵）；
6. API 处理函数（``POST /cluster/start`` 的 ``cluster.start_service``）把被拒
   请求翻成 ``success=False`` + 「已启动/最大实例数」，而不是双实例。
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import cluster as cluster_api  # noqa: E402
from services.service_manager import ServiceManager  # noqa: E402

SERVICE = "schedule_service"


class FakeLauncher:
    """在 ``start`` 里 ``await sleep`` 放大「计数 → 写入 instances」的竞态窗口。"""

    def __init__(self, *, delay: float = 0.02, fail_first: bool = False,
                 on_start=None) -> None:
        self.attempts = 0
        self.started = 0
        self.delay = delay
        self.fail_first = fail_first
        self.on_start = on_start

    async def start(self, service_type, config, env):
        self.attempts += 1
        attempt = self.attempts
        if self.on_start is not None:
            self.on_start(self)
        await asyncio.sleep(self.delay)
        if self.fail_first and attempt == 1:
            raise RuntimeError("launcher boom（模拟启动失败）")
        self.started += 1
        return SimpleNamespace(
            instance_id=f"{service_type}-{self.started}",
            service_type=service_type,
            status="running",
            error_message=None,
        )


def _manager(max_instances: int, launcher: FakeLauncher) -> ServiceManager:
    """绕开 ``__init__``（不读集群配置/不创建 docker launcher）。"""
    sm = ServiceManager.__new__(ServiceManager)
    sm.config = SimpleNamespace(
        services={SERVICE: SimpleNamespace(max_instances=max_instances, env={})}
    )
    sm.instances = {}
    sm._resolve_env = lambda env: {}
    sm.get_launcher = lambda: launcher
    return sm


def _run(coro):
    return asyncio.run(coro)


def _ok_err(results):
    ok = [r for r in results if not isinstance(r, BaseException)]
    err = [r for r in results if isinstance(r, BaseException)]
    return ok, err


def test_concurrent_start_single_slot_starts_exactly_once():
    launcher = FakeLauncher()

    async def scenario():
        sm = _manager(1, launcher)
        results = await asyncio.gather(
            sm.start_service(SERVICE),
            sm.start_service(SERVICE),
            return_exceptions=True,
        )
        return sm, results

    sm, results = _run(scenario())
    ok, err = _ok_err(results)
    assert launcher.attempts == 1, (
        f"max_instances=1 并发两次启动了 {launcher.attempts} 次（应 1 次）——双起 scheduler"
    )
    assert len(ok) == 1 and len(err) == 1, (ok, err)
    assert "启动" in str(err[0]), f"被拒请求的错误文案不含「启动」：{err[0]!r}"
    assert list(sm.instances) == [f"{SERVICE}-1"]


def test_concurrent_start_two_slots_both_succeed():
    launcher = FakeLauncher()

    async def scenario():
        sm = _manager(2, launcher)
        return sm, await asyncio.gather(
            sm.start_service(SERVICE), sm.start_service(SERVICE),
            return_exceptions=True,
        )

    sm, results = _run(scenario())
    ok, err = _ok_err(results)
    assert len(ok) == 2 and err == [], (ok, err)
    assert launcher.started == 2
    assert len(sm.instances) == 2


def test_many_concurrent_requests_cap_at_max_instances():
    launcher = FakeLauncher(delay=0.01)

    async def scenario():
        sm = _manager(3, launcher)
        return sm, await asyncio.gather(
            *[sm.start_service(SERVICE) for _ in range(20)],
            return_exceptions=True,
        )

    sm, results = _run(scenario())
    ok, err = _ok_err(results)
    assert launcher.attempts == 3, (
        f"20 并发 / max_instances=3 实际启动 {launcher.attempts} 次（应 3 次）"
        "——预留不原子，超出配额的请求全部穿过了计数检查"
    )
    assert launcher.started == 3
    assert len(ok) == 3 and len(err) == 17
    assert len(sm.instances) == 3


def test_failed_start_releases_reservation():
    launcher = FakeLauncher(delay=0.005, fail_first=True)
    sm = _manager(1, launcher)

    async def scenario():
        try:
            await sm.start_service(SERVICE)
        except RuntimeError:
            pass
        else:  # pragma: no cover - 回归时会走到这里
            raise AssertionError("launcher.start 抛错，start_service 却没有抛出")
        assert sm.instances == {}, f"失败的启动留下了实例记录：{sm.instances}"
        return await sm.start_service(SERVICE)

    instance = _run(scenario())
    assert instance.instance_id == f"{SERVICE}-1"
    assert len(sm.instances) == 1


def test_reservation_is_not_visible_as_instance():
    """预留只占名额，不许在 instances / list_instances 里造幽灵实例。"""
    seen = {}

    def _snapshot(launcher):
        seen.setdefault("instances", list(sm.instances))
        seen.setdefault("listed", sm.list_instances())

    sm = _manager(1, FakeLauncher(delay=0.02, on_start=_snapshot))
    _run(sm.start_service(SERVICE))
    assert seen["instances"] == []
    assert seen["listed"] == []


def test_manager_survives_multiple_event_loops():
    """同一 manager 跨多次 ``asyncio.run``（多个事件循环）仍可继续启动。

    ``asyncio.Lock`` 在 3.10+ 首次使用时绑定事件循环：锁若按 loop id 复用到
    已关闭的循环，第二次 ``asyncio.run`` 会抛 RuntimeError。实现按事件循环
    弱引用分桶——本用例守住这条路径。
    """
    launcher = FakeLauncher(delay=0.0)
    sm = _manager(2, launcher)
    _run(sm.start_service(SERVICE))
    _run(sm.start_service(SERVICE))
    assert sorted(sm.instances) == [f"{SERVICE}-1", f"{SERVICE}-2"]


def test_api_start_endpoint_reports_already_starting(monkeypatch):
    """API 处理函数：第二个并发请求 success=False + 「已启动」，不双起。"""
    launcher = FakeLauncher()
    sm = _manager(1, launcher)
    monkeypatch.setattr(cluster_api, "get_service_manager", lambda: sm)

    async def scenario():
        req = cluster_api.StartServiceRequest(service_type=SERVICE)
        # #261：handler 需要 Request + 平台身份（HTTP 层由 require_auth 注入）。
        request = SimpleNamespace()
        identity = {"actor": "test", "platform_admin": True}
        return await asyncio.gather(
            cluster_api.start_service(req, request, identity),
            cluster_api.start_service(req, request, identity),
        )

    responses = _run(scenario())
    assert launcher.attempts == 1
    successes = [r for r in responses if r.success]
    rejected = [r for r in responses if not r.success]
    assert len(successes) == 1, [r.model_dump() for r in responses]
    assert len(rejected) == 1
    assert "启动" in rejected[0].error, rejected[0].error
