"""纯 guard 形状表：按文件路径加载 _selfclose_guard.py，用真实 stdlib 对象驱动。

在任何 CPython 3.10+ 上可跑（只依赖 stdlib）。退出码非 0 = 有形状被判 clean（漏检）。
"""
import asyncio
import contextvars
import importlib.util
import os
import sys

GUARD_PATH = os.environ.get(
    "GUARD_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                 "plaita", "event", "_selfclose_guard.py"))


def load_guard(path):
    # 探针硬门（与 scripts/selfclose_109_110.py 同口径）：加载到的必须是本树里真实存在
    # 的判据内核，realpath 不为 None 且落在指定的 TREE 之内，绝不允许 import 到别处的同名模块。
    real = os.path.realpath(path)
    assert real and os.path.isfile(real), f"guard 文件不存在: {real!r}"
    tree = os.environ.get("PLAITA_TREE")
    if tree:
        assert real.startswith(os.path.realpath(tree)), f"wrong tree: {real}"
    print(f"# guard: {real}")
    spec = importlib.util.spec_from_file_location("_standalone_selfclose_guard", real)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


g = load_guard(GUARD_PATH)
RESULTS = []


def record(name, verdict_triple, expect_refusal=True):
    verdict, visited, reason = verdict_triple
    ok = (verdict != "clean") if expect_refusal else (verdict == "clean")
    RESULTS.append((name, verdict, visited, reason, ok))
    print(f"{'OK ' if ok else 'MISS'} {name:28s} verdict={verdict:10s} "
          f"visited={visited:<6d} reason={reason}")


async def shape_taskgroup_sleep0():
    """验收者决定性反例：TaskGroup + await sleep(0) + 显式空 context（懒创建窗口）。"""
    holder, result = {}, {}

    async def close_task():
        result["v"] = g.close_graph_verdict(asyncio.current_task(), holder["pending"])

    async def handler():                      # D = 在飞分发任务
        async with asyncio.TaskGroup() as tg:
            tg.create_task(close_task(), context=contextvars.Context())
            await asyncio.sleep(0)

    d = asyncio.ensure_future(handler())
    holder["pending"] = {d: ("e", "h")}
    try:
        await d
    except BaseException:                     # noqa: BLE001
        pass
    return result.get("v")


async def shape_taskgroup_lazy():
    """确定性的懒创建窗口：D 已 __aenter__ 且建了子任务，但还没进 __aexit__。"""
    holder, result = {}, {}

    async def close_task():
        result["v"] = g.close_graph_verdict(asyncio.current_task(), holder["pending"])

    async def handler():
        tg = asyncio.TaskGroup()
        await tg.__aenter__()
        tg.create_task(close_task(), context=contextvars.Context())
        await asyncio.sleep(0)                # C 在 __aexit__ 之前跑完并读图
        try:
            await tg.__aexit__(None, None, None)
        except BaseException:                 # noqa: BLE001
            pass

    d = asyncio.ensure_future(handler())
    holder["pending"] = {d: ("e", "h")}
    try:
        await d
    except BaseException:                     # noqa: BLE001
        pass
    return result.get("v")


async def shape_two_level_taskgroup():
    """两级 TaskGroup：C 在 g1，D 卡在 g2 的 __aexit__。"""
    holder, result = {}, {}

    async def close_task():
        await asyncio.sleep(0)
        result["v"] = g.close_graph_verdict(asyncio.current_task(), holder["pending"])

    async def handler():                      # D
        async with asyncio.TaskGroup() as g2:
            async with asyncio.TaskGroup() as g1:
                g1.create_task(close_task(), context=contextvars.Context())
                await asyncio.sleep(0)
            await asyncio.sleep(0)

    d = asyncio.ensure_future(handler())
    holder["pending"] = {d: ("e", "h")}
    try:
        await d
    except BaseException:                     # noqa: BLE001
        pass
    return result.get("v")


async def shape_barrier():
    holder, result = {}, {}

    async def close_task():
        result["v"] = g.close_graph_verdict(asyncio.current_task(), holder["pending"])

    async def handler():
        barrier = asyncio.Barrier(2)
        c = asyncio.ensure_future(close_task())
        c.add_done_callback(
            lambda t: asyncio.ensure_future(barrier.abort()))
        try:
            await barrier.wait()
        except BaseException:                 # noqa: BLE001
            pass

    d = asyncio.ensure_future(handler())
    holder["pending"] = {d: ("e", "h")}
    try:
        await d
    except BaseException:                     # noqa: BLE001
        pass
    return result.get("v")


async def shape_streamreader():
    holder, result = {}, {}

    async def close_task():
        result["v"] = g.close_graph_verdict(asyncio.current_task(), holder["pending"])

    async def handler():
        reader = asyncio.StreamReader(limit=2 ** 16)
        c = asyncio.ensure_future(close_task())
        c.add_done_callback(lambda t: reader.feed_eof())
        try:
            await reader.read(1)
        except BaseException:                 # noqa: BLE001
            pass

    d = asyncio.ensure_future(handler())
    holder["pending"] = {d: ("e", "h")}
    try:
        await d
    except BaseException:                     # noqa: BLE001
        pass
    return result.get("v")


class Gate:
    """自定义 awaitable：future 藏在实例属性 self._f。"""

    def __init__(self):
        self._f = asyncio.get_running_loop().create_future()

    def __await__(self):
        return self._f.__await__()


async def shape_custom_awaitable():
    holder, result = {}, {}

    async def close_task():
        result["v"] = g.close_graph_verdict(asyncio.current_task(), holder["pending"])

    async def handler():
        gate = Gate()
        c = asyncio.ensure_future(close_task())
        c.add_done_callback(lambda t: gate._f.set_result(None))
        try:
            await gate
        except BaseException:                 # noqa: BLE001
            pass

    d = asyncio.ensure_future(handler())
    holder["pending"] = {d: ("e", "h")}
    try:
        await d
    except BaseException:                     # noqa: BLE001
        pass
    return result.get("v")


async def shape_as_completed():
    holder, result = {}, {}

    async def close_task():
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        result["v"] = g.close_graph_verdict(asyncio.current_task(), holder["pending"])

    async def handler():
        c = asyncio.ensure_future(close_task())
        try:
            for fut in asyncio.as_completed([c]):
                await fut
        except BaseException:                 # noqa: BLE001
            pass

    d = asyncio.ensure_future(handler())
    holder["pending"] = {d: ("e", "h")}
    try:
        await d
    except BaseException:                     # noqa: BLE001
        pass
    return result.get("v")


async def shape_budget(chain, expect_refusal):
    loop = asyncio.get_running_loop()
    root = loop.create_future()
    cur = root
    for _ in range(chain):
        nxt = loop.create_future()
        cur.add_done_callback(nxt.set_result)
        cur = nxt
    triple = g.close_graph_verdict(root, {}, budget=g.DISPATCH_GRAPH_MAX_NODES)
    root.cancel()
    return triple


async def shape_deep_legit_await_chain(depth):
    """合法形状：depth 层任务 await 下一个，最内层算唤醒锥（无在飞分发）⇒ clean。

    这正是验收者实测「512 预算下 ≥510 层合法 await 链被拒」的形状。
    """
    loop = asyncio.get_running_loop()
    gate = loop.create_future()

    async def leaf():
        await gate                          # 等链路搭好再读自己的唤醒锥
        return g.close_graph_verdict(asyncio.current_task(), {})

    async def worker(child):
        return await child

    task = loop.create_task(leaf())
    chain = [task]
    for _ in range(depth):
        task = loop.create_task(worker(task))
        chain.append(task)
    for _ in range(depth + 2):              # 让每一层都真的 await 到子任务
        await asyncio.sleep(0)
    gate.set_result(None)
    return await chain[-1]


async def main():
    print("PY", sys.version.split()[0])
    err = g.ensure_self_test()
    print(f"selftest result={err!r}")
    if err is not None:
        print("!! selftest FAILED on this interpreter")
        return 1
    if hasattr(asyncio, "TaskGroup"):       # 3.11+；3.10 不存在该形状
        record("taskgroup_sleep0", await shape_taskgroup_sleep0())
        record("taskgroup_lazy", await shape_taskgroup_lazy())
        record("two_level_taskgroup", await shape_two_level_taskgroup())
    if hasattr(asyncio, "Barrier"):
        record("barrier", await shape_barrier())
    record("streamreader", await shape_streamreader())
    record("custom_awaitable", await shape_custom_awaitable())
    record("as_completed", await shape_as_completed())
    record("budget_over", await shape_budget(g.DISPATCH_GRAPH_MAX_NODES + 1, True))
    record("budget_at_limit_clean",
           await shape_budget(g.DISPATCH_GRAPH_MAX_NODES - 1, False),
           expect_refusal=False)
    record("legit_deep_chain_600", await shape_deep_legit_await_chain(600),
           expect_refusal=False)
    record("legit_deep_chain_10000",
           await shape_deep_legit_await_chain(10000), expect_refusal=False)
    bad = [r for r in RESULTS if not r[4]]
    print(f"\n# shapes={len(RESULTS)} OK={len(RESULTS) - len(bad)} MISS={len(bad)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
