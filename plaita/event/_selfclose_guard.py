"""#109 自关停围栏（r6 架构）：**只依赖 stdlib**，可独立抽出在别的 CPython 上驱动。

本模块是 ``RedisEventBus.close()`` 的判据内核，按评审要求从 ``redis.py`` 拆出：
不 import plaita、不 import redis，因此可以在 3.12 / 3.13 的干净解释器里按文件加载、
用真实 stdlib 对象驱动（``scripts/selfclose_109_110_versions.py``）。

两层判据（**分工与边界见下，不宣称完备**）：

L1 —— dispatch lineage（廉价，危险方向 sound，不点名任何私有属性）
    分发 handler 运行时置 ``IN_DISPATCH_BUS``（contextvars）。``asyncio.create_task`` /
    ``TaskGroup.create_task`` 复制当前 context，因此 handler 里 create_task /
    ensure_future / shield 出来的任务——以及这些任务再 create 出来的任务——都继承
    该标记。``close()`` 在动任何关停副作用之前拒绝：
      * ``current_task()`` 就是本总线登记的在飞分发任务（``_pending_tasks``），或
      * 当前 context 的标记就是本总线（handler 内 / 其后代任务内发起关停）。
    显式 ``context=contextvars.Context()`` 换掉 context 会**绕过 L1**，交由 L2。

L2 —— 完成唤醒锥（defence in depth，**fail-closed**）
    从正在执行 ``close()`` 的任务出发，沿「事件循环真的会用来唤醒等待者」的引用做
    可达性遍历（``Future._callbacks`` 的每个表项 → 回调牵动的 Future/Task）。命中
    在飞分发任务 ⇒ ``in_graph``（必然 task-wait cycle）。**证明不出来 ⇒ 拒绝**，
    绝不静默跳过：
      * 已知 asyncio 簿记类型（Task/Future/Queue/Event/Condition/Semaphore/Lock/
        Barrier/StreamReader/TaskGroup）的期望属性缺失或被重命名 ⇒ ``incomplete``；
      * ``TaskGroup._on_completed_fut`` 为 ``None`` 而 ``_tasks`` 非空 ⇒
        ``incomplete``（``__aexit__`` **懒创建**窗口：属性存在但未物化）；
      * 未知持有者**直接**持有已武装的唤醒原语（未完成 Future/Task、有 getter 的
        Queue 等）⇒ ``incomplete``（例：3.13 ``as_completed`` 的
        ``_AsCompletedIterator`` 持有 ``_done`` Queue；自定义 awaitable 持有
        ``self._f``）；
      * 展开深度耗尽 / 节点预算耗尽 ⇒ ``incomplete`` / ``budget``。

**stdlib 自检（把「未来 Python 改名」从 fail-open 变成 fail-closed）**
    首次使用时（close() 在 running loop 内）对每个受支持类型**新建实例**并检查期望
    属性确实存在；任一缺失或版本不含该类型却本应含（3.11+ 的 TaskGroup/Barrier）⇒
    本进程内 ``close()`` 一律具名拒绝（``UNSUPPORTED_RUNTIME_REFUSAL``）。

**诚实边界（本模块不能证明什么）**
    1. 两层都盖不住：``close()`` 由一个**在 dispatch 之外创建**、却被某个分发 handler
       等待的任务发起。L1 无标记；L2 是 best-effort——它只认 asyncio 自己的唤醒机制，
       不认「作者用别的同步原语自行耦合同一循环」的形状（声明残差 R1，见文档）。
    2. 回调闭包 cell 里的**容器**不展开（``asyncio.wait`` / ``as_completed`` 的
       ``todo`` 兄弟簿记集合；展开它会把唤醒锥撑爆并误拒 600+ 在飞的合法外部关停）。
       因此「唤醒边只由一个存在闭包 cell 里的容器持有」是声明盲区（残差 R2）。
    3. L2 只对**已知类型**下钻其私有簿记属性；已知集合的完备性由上面的自检保证
       （缺名即全拒），但「asyncio 新增一种我们不知道的唤醒原语」无法被自检发现——
       这类未知原语只有在**直接**挂在未知持有者上时才被 fail-closed 拦下。
"""

from __future__ import annotations

import asyncio
import collections
import contextvars
import functools
import sys
import types
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

__all__ = [
    "DISPATCH_GRAPH_MAX_NODES",
    "WAKE_GRAPH_INCOMPLETE_KIND",
    "IN_DISPATCH_BUS",
    "WakeGraphIncomplete",
    "SELF_CLOSE_REFUSAL",
    "SELF_CLOSE_BUDGET_REFUSAL",
    "WAKE_GRAPH_INCOMPLETE_REFUSAL",
    "UNSUPPORTED_RUNTIME_REFUSAL",
    "close_graph_verdict",
    "ensure_self_test",
    "reset_self_test",
    "self_test_status",
]

# 唤醒锥节点预算。**fail-closed**：预算内枚举不完 ⇒ 具名拒绝。
#
# r5 用 512，验收者实测「合法 ≥510 深的 await 链被拒」——512 让
# 「只有病态/对抗形状才撞预算」这句话失真。r6 抬到 2**16：
# 合法外部关停的唤醒锥实测 O(1)（600 在飞仍 visited=1）；
# 而「close 任务上方叠 N 层 await」这种形状，合法与对抗在结构上无法区分，
# 只能给出**诚实边界**：N ≤ 65536 放行、N ≥ 65537 具名拒绝（预算拒绝）。
# 见 scripts/selfclose_109_110_versions.py 的 budget 格与文档的版本/边界表。
DISPATCH_GRAPH_MAX_NODES = 65536

# 未知持有者 / 缺失属性 / 懒创建窗口的 fail-closed 文案里的机器可读 kind。
WAKE_GRAPH_INCOMPLETE_KIND = "wake-graph-incomplete"

# 对象展开深度（partial → func → __self__ → 容器 → 安全阀）。耗尽 ⇒ fail-closed。
_WAKE_EXPAND_DEPTH = 12

# #109/F4：「本 coroutine 位于**某个总线实例**的分发任务图内」标记（值是总线自身）。
# 存总线实例而不是布尔：多总线同进程时，A 的 handler 关停 B 不应被误拒。
IN_DISPATCH_BUS: contextvars.ContextVar = contextvars.ContextVar(
    "plaita_redis_in_dispatch_bus", default=None)

SELF_CLOSE_REFUSAL = (
    "RedisEventBus.close() 不能从它自己的分发 handler 内调用："
    "该任务会把自身纳入 drain 并导致自取消/gather 递归。"
    "请在 handler 之外（监听循环/CLI 收尾）关停；"
    "如需在 handler 内请求关停，请先 return 再由外部调用。"
)
SELF_CLOSE_BUDGET_REFUSAL = (
    "RedisEventBus.close() 拒绝执行：无法证明它不在自己的分发图内——"
    "等待图在 {budget} 个节点的证明预算内未闭合（已访问 {visited} 个节点仍有待展开）。"
    "按 fail-closed 口径具名拒绝，未产生任何关停副作用。"
    "请在 handler 之外（监听循环/CLI 收尾）关停，或缩小 handler 内的等待构造规模。"
)
WAKE_GRAPH_INCOMPLETE_REFUSAL = (
    "RedisEventBus.close() 拒绝执行：error_kind=" + WAKE_GRAPH_INCOMPLETE_KIND +
    "——完成唤醒锥无法证明安全（{reason}）。"
    "按 fail-closed 口径具名拒绝（缺属性/懒创建/未知持有者都不静默放行），"
    "未产生任何关停副作用。请在 handler 之外（监听循环/CLI 收尾）关停。"
)
UNSUPPORTED_RUNTIME_REFUSAL = (
    "RedisEventBus.close() 拒绝执行：error_kind=wake-graph-incomplete——"
    "本 CPython 的 asyncio 唤醒簿记属性自检未通过（{detail}）。"
    "当前运行时不支持该自关停围栏：按 fail-closed 口径，本进程内 close() 一律具名拒绝，"
    "未产生任何关停副作用。请在有支持的 CPython 上运行，或升级本模块的期望属性表。"
)

# 供测试注入「模拟未来 Python 改名」的额外期望属性：{kind_name: (attr, ...)}。
# 生产恒为空；注入后首次 close() 的自检必然失败 ⇒ 全拒（fail-closed）。
_SELFTEST_EXTRA_ATTRS: Dict[str, Tuple[str, ...]] = {}

_SELFTEST_RAN = False
_SELFTEST_RESULT: Optional[str] = None


class WakeGraphIncomplete(Exception):
    """完成唤醒锥无法证明安全（fail-closed 的内部信号，不外泄到调用方）。"""

    error_kind = WAKE_GRAPH_INCOMPLETE_KIND


# --------------------------------------------------------------------------
# 受支持类型的期望属性表（字面量！attribute-absence 实验靠改名这些字面量）
# --------------------------------------------------------------------------
# name / asyncio 类名 / 必须能读到的属性 / 允许为 None 的属性 / 版本下限
_WAKE_KIND_SPECS: Tuple[Dict[str, Any], ...] = (
    dict(name="Task", cls_name="Task",
         attrs=("_callbacks",), nullable=("_callbacks",), since=None),
    dict(name="Future", cls_name="Future",
         attrs=("_callbacks",), nullable=("_callbacks",), since=None),
    dict(name="TaskGroup", cls_name="TaskGroup",
         attrs=("_tasks", "_on_completed_fut"),
         nullable=("_on_completed_fut",), since=(3, 11)),
    dict(name="Queue", cls_name="Queue",
         attrs=("_getters", "_putters"), nullable=(), since=None),
    dict(name="Event", cls_name="Event",
         attrs=("_waiters",), nullable=("_waiters",), since=None),
    dict(name="Condition", cls_name="Condition",
         attrs=("_waiters",), nullable=("_waiters",), since=None),
    dict(name="Semaphore", cls_name="Semaphore",
         attrs=("_waiters",), nullable=("_waiters",), since=None),
    dict(name="Lock", cls_name="Lock",
         attrs=("_waiters",), nullable=("_waiters",), since=None),
    dict(name="Barrier", cls_name="Barrier",
         attrs=("_cond",), nullable=(), since=(3, 11)),
    dict(name="StreamReader", cls_name="StreamReader",
         attrs=("_waiter",), nullable=("_waiter",), since=None),
)

_FUTURE_CLS = asyncio.Future

_PRIMITIVES = (str, bytes, bytearray, int, float, complex, bool)
_CONTAINERS = (list, tuple, set, frozenset, dict, collections.deque)
_BOUND_CALLABLES = (types.MethodType, types.BuiltinFunctionType,
                    types.MethodWrapperType)


def _resolve_cls(cls_name: str):
    """按名字取 asyncio 类型；不存在返回 None（老版本/被改名都走这里）。"""
    try:
        return getattr(asyncio, cls_name)
    except AttributeError:
        return None


def _kind_cls(kind: Dict[str, Any]):
    return _resolve_cls(kind["cls_name"])


def _require(obj: Any, attr: str) -> Any:
    """严格读一个**已知簿记属性**：不存在 ⇒ fail-closed（绝不回退成 None 跳过）。

    这是 r5→r6 的核心修复：r5 的 ``getattr(obj, attr, None)`` 让「属性被重命名」
    变成静默放行（验收者属性缺失表：present ⇒ in_graph，absent ⇒ clean）。
    """
    try:
        return getattr(obj, attr)
    except AttributeError as exc:
        raise WakeGraphIncomplete(
            f"{type(obj).__name__}.{attr} 不存在（可能被重命名）") from exc


def _match_kind(obj: Any) -> Optional[Dict[str, Any]]:
    for kind in _WAKE_KIND_SPECS:
        cls = _kind_cls(kind)
        if cls is not None and isinstance(obj, cls):
            return kind
    return None


def _bound_self(obj: Any) -> Any:
    """绑定方法 / 内建绑定方法的 ``__self__``（未绑定的内建函数返回 None）。"""
    if isinstance(obj, _BOUND_CALLABLES):
        try:
            return obj.__self__
        except AttributeError:
            return None
    return None


def _closure_values(fn: Any) -> Tuple[Any, ...]:
    closure = fn.__closure__
    if not closure:
        return ()
    out = []
    for cell in closure:
        try:
            out.append(cell.cell_contents)
        except ValueError:          # 尚未赋值的 cell：不持有任何东西
            continue
    return tuple(out)


def _kind_is_armed(obj: Any, kind: Dict[str, Any]) -> bool:
    """已知唤醒原语是否处于「有等待者」状态（未武装 ⇒ 不牵动任何 Future）。"""
    name = kind["name"]
    if name in ("Task", "Future"):
        return not obj.done()
    if name == "TaskGroup":
        return bool(_require(obj, "_tasks"))
    if name == "Queue":
        return bool(_require(obj, "_getters")) or bool(_require(obj, "_putters"))
    if name == "Barrier":
        cond = _require(obj, "_cond")
        return bool(_require(cond, "_waiters"))
    if name == "StreamReader":
        return _require(obj, "_waiter") is not None
    return bool(_require(obj, "_waiters"))


def _direct_armed_wake_state(obj: Any) -> Optional[str]:
    """未知持有者的 ``__dict__`` 里**直接**挂着的已武装唤醒原语。

    **只扫一层、不进容器**：RedisEventBus 自己的闭包会出现在回调里（``_track_task``
    的 lambda cell 是 bus），而 bus 的 ``_pending_tasks`` / ``listeners`` 是容器、
    ``lock`` 未武装——所以合法跨总线关停不会被误拒。反之，合法地把未完成 Future
    （自定义 awaitable 的 ``self._f``）或带 getter 的 Queue（3.13 ``as_completed``）
    挂在实例属性上的持有者会被 fail-closed 拦下。
    """
    try:
        state = vars(obj)
    except TypeError:               # 无 __dict__（slots / C 对象）
        return None
    if not state:
        return None
    for name, value in state.items():
        if isinstance(value, _FUTURE_CLS):
            if not value.done():
                return f"{name}=Future/Task"
            continue
        if value is None:
            continue
        kind = _match_kind(value)
        if kind is not None and _kind_is_armed(value, kind):
            return f"{name}={kind['name']}"
    return None


def _expand_kind(obj: Any, kind: Dict[str, Any], depth: int,
                 seen: Set[int]) -> Iterator[Any]:
    """读一个已知类型的全部期望属性，并展开其牵动的 Future/Task。"""
    values: Dict[str, Any] = {}
    for attr in kind["attrs"]:
        value = _require(obj, attr)
        if value is None:
            if attr not in kind["nullable"]:
                raise WakeGraphIncomplete(
                    f"{kind['name']}.{attr} 为 None（期望持有唤醒状态）")
            values[attr] = None
        else:
            values[attr] = value
    if kind["name"] == "TaskGroup":
        # 验收者决定性反例：``__aexit__`` **懒创建** ``_on_completed_fut``——
        # 属性存在但此刻是 None，而 ``_tasks`` 非空 ⇒ 「group 被谁 await」这条边
        # 读不出来。绝不当作「没有边」（r5 的 clean 就是死在这里）。
        if values["_on_completed_fut"] is None and values["_tasks"]:
            raise WakeGraphIncomplete(
                "TaskGroup._on_completed_fut 未物化（None）而 _tasks 非空："
                "__aexit__ 懒创建窗口，group 的等待边读不出来")
    for attr, value in values.items():
        if value is None:
            continue
        yield from _iter_wake_futures(value, depth - 1, seen, True)


def _iter_wake_futures(obj: Any, depth: int = _WAKE_EXPAND_DEPTH,
                       seen: Optional[Set[int]] = None,
                       containers: bool = True) -> Iterator[Any]:
    """枚举一个「回调/簿记引用到的对象」牵动的 Future/Task（fail-closed）。

    ``containers=False`` 只用于闭包 cell 的**直接内容**：那里的容器是 asyncio 的
    兄弟簿记（``asyncio.wait`` / ``as_completed`` 的 ``todo``），展开它既不是真实
    唤醒边，又会把唤醒锥撑爆（声明残差 R2）。
    """
    if isinstance(obj, _FUTURE_CLS):
        yield obj
        return
    if obj is None:
        return
    if depth < 0:
        raise WakeGraphIncomplete(
            f"展开深度耗尽（超过 {_WAKE_EXPAND_DEPTH} 层）")
    if seen is None:
        seen = set()
    if id(obj) in seen:
        return
    seen.add(id(obj))
    if isinstance(obj, _PRIMITIVES):
        return
    if isinstance(obj, functools.partial):
        yield from _iter_wake_futures(obj.func, depth - 1, seen, True)
        for arg in obj.args:
            yield from _iter_wake_futures(arg, depth - 1, seen, True)
        for value in (obj.keywords or {}).values():
            yield from _iter_wake_futures(value, depth - 1, seen, True)
        return
    if isinstance(obj, _CONTAINERS):
        if not containers:
            return
        items = obj.values() if isinstance(obj, dict) else obj
        for item in items:
            yield from _iter_wake_futures(item, depth - 1, seen, True)
        return
    own = _bound_self(obj)
    if own is not None and not isinstance(own, type):
        yield from _iter_wake_futures(own, depth - 1, seen, True)
        return
    if isinstance(obj, types.FunctionType):
        for value in _closure_values(obj):
            yield from _iter_wake_futures(value, depth - 1, seen, False)
        return
    kind = _match_kind(obj)
    if kind is not None:
        yield from _expand_kind(obj, kind, depth, seen)
        return
    hidden = _direct_armed_wake_state(obj)
    if hidden is not None:
        raise WakeGraphIncomplete(
            f"未知持有者 {type(obj).__name__} 直接持有唤醒状态（{hidden}），拒绝推断")
    return


def _callback_wake_targets(handle: Any) -> Iterator[Any]:
    """一个 ``Future._callbacks`` 表项会让**哪些** Future/Task 获得进展。

    3.11 起表项是 ``(callback, context)``；实现差异下是带 ``_callback`` 的 Handle。
    """
    if isinstance(handle, tuple):
        callback = handle[0] if handle else None
        handle_args: Sequence[Any] = ()
    else:
        callback = _require(handle, "_callback")
        handle_args = _require(handle, "_args") or ()
    refs = []
    if callback is not None:
        if isinstance(callback, types.FunctionType):
            for value in _closure_values(callback):
                refs.append((value, False))
        refs.append((callback, True))          # partial / 绑定方法由展开器解包
    for arg in handle_args:
        refs.append((arg, True))
    seen: Set[int] = set()
    for ref, containers in refs:
        for fut in _iter_wake_futures(ref, containers=containers):
            if id(fut) not in seen:
                seen.add(id(fut))
                yield fut


def close_graph_verdict(root: Any, pending: Any,
                        budget: int = DISPATCH_GRAPH_MAX_NODES
                        ) -> Tuple[str, int, Optional[str]]:
    """``root``（正在执行 close() 的任务）是否位于 ``pending``（在飞分发任务）的等待图内。

    返回 ``(verdict, visited, reason)``，``verdict`` ∈
    ``{"clean", "in_graph", "incomplete", "budget"}``：

      * ``in_graph``：命中在飞分发任务（必然 task-wait cycle）；
      * ``clean``：预算内**完全枚举完**且未命中（可放行）；
      * ``incomplete``：缺属性 / 懒创建窗口 / 未知持有者 / 深度耗尽 ⇒ **无法证明安全**；
      * ``budget``：节点预算内仍有待展开 ⇒ **无法证明安全**。
    ``incomplete`` 与 ``budget`` 都必须具名拒绝（fail-closed）。
    """
    if root is None:
        return "incomplete", 0, "拿不到当前任务（current_task() is None）"
    visited: Set[int] = set()
    stack: List[Any] = [root]
    count = 0
    while stack:
        if count >= budget:
            return "budget", count, None
        node = stack.pop()
        if id(node) in visited:
            continue
        visited.add(id(node))
        count += 1
        if node in pending:
            return "in_graph", count, None
        try:
            callbacks = _require(node, "_callbacks")
            if callbacks is not None:
                for handle in list(callbacks):
                    for fut in _callback_wake_targets(handle):
                        if fut in pending:
                            return "in_graph", count + 1, None
                        if id(fut) not in visited:
                            stack.append(fut)
        except WakeGraphIncomplete as exc:
            return "incomplete", count, str(exc)
    return "clean", count, None


# --------------------------------------------------------------------------
# stdlib 自检：把「未来 Python 改名」从 fail-open 变成 fail-closed
# --------------------------------------------------------------------------
async def _selftest_task_body() -> None:
    await asyncio.Event().wait()


def _build_selftest_instances() -> Dict[str, Any]:
    """在 running loop 内新建每个受支持类型的一个实例。"""
    loop = asyncio.get_running_loop()
    instances: Dict[str, Any] = {}
    for kind in _WAKE_KIND_SPECS:
        cls = _kind_cls(kind)
        if cls is None:
            continue
        name = kind["name"]
        if name == "Task":
            instances[name] = loop.create_task(_selftest_task_body())
        elif name == "Future":
            instances[name] = loop.create_future()
        elif name == "TaskGroup":
            instances[name] = asyncio.TaskGroup()
        elif name == "Queue":
            instances[name] = asyncio.Queue()
        elif name == "Event":
            instances[name] = asyncio.Event()
        elif name == "Condition":
            instances[name] = asyncio.Condition()
        elif name == "Semaphore":
            instances[name] = asyncio.Semaphore()
        elif name == "Lock":
            instances[name] = asyncio.Lock()
        elif name == "Barrier":
            instances[name] = asyncio.Barrier(2)
        elif name == "StreamReader":
            instances[name] = asyncio.StreamReader(limit=2 ** 16)
        else:                                    # pragma: no cover - 表驱动
            raise AssertionError(f"未注册的自检构造: {name}")
    return instances


def _run_self_test() -> Optional[str]:
    """返回 None=通过；否则返回具名失败原因（调用方必须 fail-closed）。"""
    running = sys.version_info[:2]
    if running < (3, 10):
        return f"CPython {sys.version.split()[0]} 低于该围栏支持的下限 3.10"
    try:
        instances = _build_selftest_instances()
    except Exception as exc:                     # noqa: BLE001 - 构造失败本身即不支持
        return f"构造自检实例失败: {type(exc).__name__}: {exc}"
    problems = []
    for kind in _WAKE_KIND_SPECS:
        name = kind["name"]
        since = kind["since"]
        required = since is None or running >= since
        if name not in instances:
            if required:
                problems.append(f"{name} 类型不存在（本版本本应存在）")
            continue
        obj = instances[name]
        attrs = list(kind["attrs"]) + list(_SELFTEST_EXTRA_ATTRS.get(name, ()))
        for attr in attrs:
            try:
                getattr(obj, attr)
            except AttributeError:
                problems.append(f"{name}.{attr} 不存在（可能被重命名）")
    task = instances.get("Task")
    if task is not None and not task.done():
        task.cancel()
    return "; ".join(problems) if problems else None


def ensure_self_test() -> Optional[str]:
    """首次调用时跑自检并缓存；返回 None=通过，否则为具名失败原因。"""
    global _SELFTEST_RAN, _SELFTEST_RESULT
    if not _SELFTEST_RAN:
        _SELFTEST_RESULT = _run_self_test()
        _SELFTEST_RAN = True
    return _SELFTEST_RESULT


def reset_self_test() -> None:
    """测试缝：清掉自检缓存（生产不调用）。"""
    global _SELFTEST_RAN, _SELFTEST_RESULT
    _SELFTEST_RAN = False
    _SELFTEST_RESULT = None


def self_test_status() -> Tuple[bool, Optional[str]]:
    """``(是否已跑过, 结果)``；供版本脚本/探针打印。"""
    return _SELFTEST_RAN, _SELFTEST_RESULT
