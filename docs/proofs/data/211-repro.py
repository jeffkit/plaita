#!/usr/bin/env python3
"""#211 repro：``ServiceManager.start_service`` 先检后启非原子 → 并发双起。

口径（真实并发协程 + 在 ``launcher.start`` 里 ``await asyncio.sleep`` 放大
竞态窗口，与 issue 复现脚本同形）：

  A) ``max_instances=1``：两个并发 ``start_service`` ⇒ 恰好一次
     ``launcher.start``；另一个必须拿到「已达到最大实例数 / 已在启动」类错误，
     且 ``instances`` 里只有 1 个实例。
  B) ``max_instances=2``：两个并发都成功、两个实例（守卫：不许矫枉过正锁死）。
  C) 20 并发 + ``max_instances=3``：恰好 3 次启动，其余被拒。
  D) 启动失败必须回滚占位：失败后同一 service_type 的下一次请求能起来
     （守卫：占位泄漏会永久占满 max_instances）。

运行：``PYTHONPATH=/tmp/w40 /Users/jeff.jie/.venvs/plaita/bin/python \
/tmp/w40-out/repro-211.py``
"""
from __future__ import annotations

import asyncio
import sys
import traceback
from pathlib import Path
from types import SimpleNamespace

_HERE = Path(__file__).resolve()
# 仓内位置 docs/proofs/data/211-repro.py → parents[3] = 仓根；worktree 里也兼容
_CANDIDATES = [p for p in (_HERE.parents[3] if len(_HERE.parents) > 3 else None,) if p]
_CANDIDATES.append(Path("/tmp/w40"))
REPO_ROOT = next(
    (p for p in _CANDIDATES if (p / "plaita-console" / "backend").is_dir()),
    Path("/tmp/w40"),
)
BACKEND = REPO_ROOT / "plaita-console" / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from services.service_manager import ServiceManager  # noqa: E402

failures: list[str] = []


def fail(msg: str) -> None:
    failures.append(msg)
    print(f"  [FAIL] {msg}")


class FakeLauncher:
    def __init__(self, fail_first: bool = False, delay: float = 0.05) -> None:
        self.started = 0
        self.attempts = 0
        self.fail_first = fail_first
        self.delay = delay

    async def start(self, service_type, config, env):
        self.attempts += 1
        attempt = self.attempts
        # 竞态窗口：并发请求都能在对方写入 instances 之前越过「计数检查」
        await asyncio.sleep(self.delay)
        if self.fail_first and attempt == 1:
            raise RuntimeError("launcher boom（模拟启动失败）")
        self.started += 1
        return SimpleNamespace(
            instance_id=f"{service_type}-{self.started}",
            service_type=service_type,
            status="running",
        )


def _manager(max_instances: int, launcher: FakeLauncher) -> ServiceManager:
    sm = ServiceManager.__new__(ServiceManager)
    sm.config = SimpleNamespace(
        services={
            "schedule_service": SimpleNamespace(
                max_instances=max_instances, env={}
            )
        }
    )
    sm.instances = {}
    sm._resolve_env = lambda env: {}
    sm.get_launcher = lambda: launcher
    return sm


def _split(results):
    ok = [r for r in results if not isinstance(r, BaseException)]
    err = [r for r in results if isinstance(r, BaseException)]
    return ok, err


async def case_single_slot() -> None:
    print("\n== case A: max_instances=1，两个并发 start_service ==")
    launcher = FakeLauncher()
    sm = _manager(1, launcher)
    results = await asyncio.gather(
        sm.start_service("schedule_service"),
        sm.start_service("schedule_service"),
        return_exceptions=True,
    )
    ok, err = _split(results)
    print(f"  launcher.start 调用 = {launcher.attempts}/{launcher.started} 成功")
    print(f"  成功 {len(ok)} 个，失败 {len(err)} 个：{[str(e) for e in err]}")
    print(f"  instances = {sorted(sm.instances)}")
    if launcher.attempts != 1:
        fail(
            f"max_instances=1 下并发两次 start_service 实际调用了 "
            f"{launcher.attempts} 次 launcher.start（应恰好 1 次）——双起 scheduler，"
            "cron 同一时点会被两个调度循环各触发一次"
        )
    if len(ok) != 1:
        fail(f"并发两个请求成功了 {len(ok)} 个（应恰好 1 个：另一个必须被拒）")
    if len(err) != 1:
        fail(f"并发两个请求失败了 {len(err)} 个（应恰好 1 个：另一个必须得到「已在启动」）")
    elif "启动" not in str(err[0]) and "最大实例数" not in str(err[0]):
        fail(f"被拒请求的错误文案不含「启动/最大实例数」：{str(err[0])!r}")
    if len(sm.instances) != 1:
        fail(f"instances 里有 {len(sm.instances)} 个实例（应 1 个）：{sorted(sm.instances)}")


async def case_two_slots() -> None:
    print("\n== case B: max_instances=2，两个并发都该成功 ==")
    launcher = FakeLauncher()
    sm = _manager(2, launcher)
    results = await asyncio.gather(
        sm.start_service("schedule_service"),
        sm.start_service("schedule_service"),
        return_exceptions=True,
    )
    ok, err = _split(results)
    print(f"  launcher.start 调用 = {launcher.attempts}/{launcher.started} 成功，"
          f"失败 {[str(e) for e in err]}")
    if len(ok) != 2 or len(err) != 0:
        fail(f"max_instances=2 下两个并发请求应都成功（实际成功 {len(ok)}，失败 {len(err)}）")
    if len(sm.instances) != 2:
        fail(f"instances 里有 {len(sm.instances)} 个实例（应 2 个）")


async def case_many() -> None:
    print("\n== case C: max_instances=3，20 个并发请求 ==")
    launcher = FakeLauncher(delay=0.02)
    sm = _manager(3, launcher)
    results = await asyncio.gather(
        *[sm.start_service("schedule_service") for _ in range(20)],
        return_exceptions=True,
    )
    ok, err = _split(results)
    print(f"  launcher.start 调用 = {launcher.attempts}，成功 {len(ok)}，失败 {len(err)}")
    if launcher.attempts != 3:
        fail(
            f"20 并发 / max_instances=3 实际启动 {launcher.attempts} 次（应恰好 3 次）"
            "——预留不是原子的，超出的请求全部穿过了计数检查"
        )
    if len(sm.instances) != 3:
        fail(f"instances 里有 {len(sm.instances)} 个实例（应 3 个）")


async def case_rollback() -> None:
    print("\n== case D: 启动失败必须回滚占位 ==")
    launcher = FakeLauncher(fail_first=True, delay=0.01)
    sm = _manager(1, launcher)
    try:
        await sm.start_service("schedule_service")
    except Exception as exc:
        print(f"  首次启动按预期失败：{exc}")
    else:
        fail("launcher.start 抛错，start_service 却没有抛出")
    if sm.instances:
        fail(f"失败的启动在 instances 里留下了记录：{sorted(sm.instances)}")
    try:
        inst = await sm.start_service("schedule_service")
    except Exception as exc:
        fail(f"失败后重试被占位卡住（占位泄漏 = 永久占满 max_instances）：{exc}")
        return
    print(f"  失败后重试成功：{inst.instance_id}")
    if len(sm.instances) != 1:
        fail(f"重试成功后 instances 应有 1 个实例，实际 {sorted(sm.instances)}")


async def main() -> int:
    await case_single_slot()
    await case_two_slots()
    await case_many()
    await case_rollback()
    print()
    if failures:
        print(f"result: RED ({len(failures)} 项)")
        for i, msg in enumerate(failures, 1):
            print(f"  {i}. {msg}")
        return 1
    print("result: GREEN")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception:
        traceback.print_exc()
        print("\nresult: RED (repro 执行异常)")
        sys.exit(1)
