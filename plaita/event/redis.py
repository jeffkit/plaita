"""
基于Redis的事件总线和存储实现
"""
import asyncio
import contextvars
import functools
import inspect
import json
import logging
import time
import uuid
import sys
import os
from typing import Any, Callable, Dict, List, NoReturn, Optional, Set, Union

logger = logging.getLogger(__name__)

# #109（r7）：RedisEventBus 关停时**有界等待**在飞分发任务的秒数。超时后**不取消**
# 任何在飞分发任务（旧实现在此 cancel，正是自取消 / `Task.cancel` 沿 `_fut_waiter`
# 环无界递归的来源），只对每个未完成任务打一条含 event_id / handler_id 的 WARNING，
# 然后放手让它跑完；登记由任务自己的完成回调回收。监听任务在这一步**之后**才取消。
DISPATCH_DRAIN_TIMEOUT_SECONDS = 5.0

# #109（r8/G3）：teardown **没有完整跑完**（被取消 / 步骤抛异常）时给等待者的具名
# 错误。r7 的 finally 无条件 `set_result(None)`，于是「supervisor / 批量
# `all_tasks()` 取消 teardown」被报成**成功**（`comp_closes == [0,0,0,0,0]`），
# 再加上幂等早返回，总线此后永不释放。现在：未完成 ⇒ future 记具名错误并**清空
# 幂等凭据**，让后续 `close()` 真正重试。
# 机器可读 kind 与跨循环 / 自关停拒绝同一家族（`error_kind=`）。
TEARDOWN_INCOMPLETE_KIND = "teardown-incomplete"
_TEARDOWN_INCOMPLETE_MESSAGE = (
    "RedisEventBus 关停未完成：error_kind=" + TEARDOWN_INCOMPLETE_KIND +
    "——teardown 任务被取消或在步骤中抛异常，五个组件未必都已释放"
    "（本次关停**不报成功**）。已清空幂等凭据：再次调用 close() 会重新尝试关停；"
    "若总线仍被使用，请检查上一次关停被谁取消。"
)

# #109（r9/Finding 1）：`_spawn_teardown` 本身失败（自定义 task factory 抛异常、
# 循环已关闭）时的具名错误。r8 的顺序是「先落盘凭据、再 spawn」；spawn 抛异常时
# 凭据留在「已受理」状态而 teardown 任务根本不存在 ⇒ close future **永不 resolve**
# ⇒ **第二次 close() 永久挂起**。现在整个受理动作要么成功、要么回滚成「未受理」。
TEARDOWN_SPAWN_FAILED_KIND = "teardown-spawn-failed"
_TEARDOWN_SPAWN_FAILED_MESSAGE = (
    "RedisEventBus 关停未受理：error_kind=" + TEARDOWN_SPAWN_FAILED_KIND +
    "——在事件循环里创建 teardown 任务失败（自定义/抛异常的 task factory、"
    "交回**方法被覆写的敌对 Task**、把 teardown 建在**另一个事件循环**上，"
    "或已关闭的循环）。本次请求已**整体回滚**：`_closing` / `_close_future` / "
    "`_teardown_task` 都回到受理前的状态，总线仍可用；在健康的循环上再次调用 "
    "close() 会重新受理并真正跑完关停。具体 reason 见本异常的 `__cause__`。"
)

# #109（r11/A1）：工厂交回的是一个**真 Task**，但它并不跑我们的 ``_teardown_entry``
# （包着别的 coroutine、返回前已被 cancel、或我们的 coroutine 被别处驱动）。r10 会静默
# 收养它：我们的 coroutine 从未被 await、``_teardown`` 从未跑过 ⇒ close future
# **永不结算**（两次 close 都永久挂起，状态 `(True, PENDING, True, 0)`、五个组件
# `[0×5]`，外加 ``RuntimeWarning: coroutine ... _teardown_entry was never awaited``）。
# r11 在 ``_teardown_entry`` **第一行**写下「已进入」标记，并在收养到的 Task 结束时
# （含「返回时已 done/cancelled」的同步路径）检查该标记：结束而从未进入 ⇒ 具名结算，
# 绝不悬着。判据是「任务结束」这一事件，不引入任何更弱的计时假设。
TEARDOWN_NEVER_ENTERED_KIND = "teardown-never-entered"
_TEARDOWN_NEVER_ENTERED_MESSAGE = (
    "RedisEventBus 关停失败：error_kind=" + TEARDOWN_NEVER_ENTERED_KIND +
    "——受理时收养的 Task 已结束，但 teardown 入口 ``_teardown_entry`` 从未被执行"
    "（工厂交回的 Task 不跑我们的 coroutine，或返回前就被 cancel）。本次没有关停任何"
    "组件（`_teardown_runs` 保持 0）；已清空幂等凭据：修好/换掉 task factory 后再次 "
    "close() 会重新受理并真正跑完关停。"
)

# #109（r11/A2）：工厂把 teardown 任务建在**另一个**事件循环上。close future 属于本循环，
# 在别的循环上结算既不受本循环调度也非线程安全 ⇒ 绝不静默收养：拒绝受理（由
# ``request_close`` 的既有 except 整体回滚，error_kind=teardown-spawn-failed），
# 并在 ``__cause__`` 里点名本 reason。
TEARDOWN_FOREIGN_LOOP_REASON = "teardown-foreign-loop"
_TEARDOWN_FOREIGN_LOOP_MESSAGE = (
    "RedisEventBus 关停：工厂把 teardown 任务建在**另一个事件循环**上"
    "（reason=" + TEARDOWN_FOREIGN_LOOP_REASON + "）——close future 只在本循环结算，"
    "跨循环收养会让它永远悬着。已拒绝受理（并尽力中和那个任务）。"
)

# #109（r11/B）：工厂交回 ``asyncio.Task`` 的**子类**并覆写 ``add_done_callback`` /
# ``done`` / ``cancelled`` / ``exception`` 之一（让它抛异常）。``isinstance`` 会放行，
# 于是 ``request_close`` 对返回值的使用**裸抛且不回滚**——这曾证伪「对任意 factory 行为
# 都不会失败」。r11 在 ``_spawn_teardown`` 里把「这几个方法就是 stdlib 原版」证出来：
# 证不出就拒绝受理（同一家族具名回滚 + ``__cause__`` 点名本 reason）。
TEARDOWN_HOSTILE_TASK_REASON = "teardown-hostile-task-methods"
_TEARDOWN_HOSTILE_TASK_MESSAGE = (
    "RedisEventBus 关停：工厂交回的 Task 覆写了 request_close 需要使用的 stdlib 方法"
    "（reason=" + TEARDOWN_HOSTILE_TASK_REASON + "，例如 add_done_callback 直接抛异常）"
    "——把它交给 request_close 使用会裸抛且不回滚。已拒绝受理（并尽力中和那个任务）。"
)

# 本次受理的 holder 绑在**任务自己**身上（而不是总线的某个字段），因此上一轮的 Task 在
# 重试之后才回调时，读到的仍是它自己那一轮标记，不会拿新一轮的状态去判定（#109 r11/A1）。
_TEARDOWN_ACCEPTANCE_ATTR = "_plaita_teardown_acceptance"

# ``request_close`` 会落到 spawn 返回值上的**全部**方法（#109 r11/B）。任何一项被 Task
# 子类覆写，都可能让那一块裸抛——所以 spawn 只交出这几项均为 stdlib 原版的任务
# （``get_loop`` 由 ``_accept_spawned_task`` 自己用，同样不能是覆写版）。
_TEARDOWN_USED_TASK_METHODS = (
    "add_done_callback", "done", "cancelled", "exception", "get_loop")


def _swallow_task_result(task: "asyncio.Task") -> None:
    """done 回调：取用（避免 GC 时 "Task exception was never retrieved"）但**不**传播。

    只用于被拒绝受理、正在被中和的任务（#109 r11/A2、B）；纯清理路径，任何失败只留痕。
    """
    try:
        if not task.cancelled():
            task.exception()
    except BaseException as exc:                 # noqa: BLE001 - 纯清理路径
        logger.debug("事件总线关停：取用被拒任务的异常失败（忽略）: %r", exc)


def _guarded(what: str, fn: Callable, *args):
    """调用 ``fn(*args)``；失败返回 ``(False, None)`` 并 **debug 留痕**（#109 r11/B）。

    P0-2 门禁不允许静默吞咽异常，所以这里必须留下一行日志；判据只关心「这次访问是否
    可信」，异常细节不影响结论（访问不可信 ⇒ 判否 ⇒ 拒收受理）。
    """
    try:
        return True, fn(*args)
    except BaseException as exc:                 # noqa: BLE001 - 调用不可信即判否
        logger.debug("事件总线关停：%s 失败（视为不可信）: %r", what, exc)
        return False, None


def _task_uses_stdlib_methods(task: "asyncio.Task") -> bool:
    """``request_close`` 会用到的那几个方法在 ``task`` 上是不是 **stdlib 原版**（#109 r11/B）。

    ``isinstance(x, asyncio.Task)`` 对子类恒真，所以 r10 的「真 Task 就安全」并不成立：
    子类覆写 ``add_done_callback``（或 ``done`` / ``cancelled`` / ``exception``）就能让
    ``request_close`` 里对返回值的使用裸抛，且**绕过**所有回滚。这里逐项证明「类上没覆写、
    实例访问拿到的是绑定在 ``task`` 上的同一个 stdlib 方法（实例级影子属性也过不去）」。
    任一项证不出 ⇒ 由调用方拒绝受理（具名 + 整体回滚）。这是**结构性**判据：
    唯一被接受的对象类型是「这几个方法都没被动过的 Task」。C 实现的 ``Task`` 不暴露
    ``__dict__``（3.11–3.13 实测），所以「实例级影子属性」用**绑定比较**而不是字典查表
    来证：把类上的 stdlib 方法绑到本任务上，必须与实例访问到的那个相等。
    """
    cls = type(task)
    for name in _TEARDOWN_USED_TASK_METHODS:
        baseline = getattr(asyncio.Task, name, None)
        if baseline is None or not callable(baseline):
            return False
        ok, on_class = _guarded(f"读取 {cls.__name__}.{name}", getattr, cls, name)
        if not ok:
            return False
        if (on_class is not baseline
                and getattr(on_class, "__func__", on_class) is not baseline):
            return False
        ok, bound = _guarded(f"读取实例的 {name}", getattr, task, name)
        if not ok or not callable(bound):
            return False
        if getattr(bound, "__self__", None) is not task:
            return False
        func = getattr(bound, "__func__", None)
        if func is not None and func is not baseline:
            return False
        ok, baseline_bound = _guarded(
            f"绑定 asyncio.Task.{name}", baseline.__get__, task, cls)
        if not ok:
            return False
        ok, same = _guarded(f"比较实例的 {name}", lambda: bound == baseline_bound)
        if not ok or not same:
            return False
    return True


def _accepts_close_connection_pool(closer) -> bool:
    """``closer`` 是否接受 ``close_connection_pool`` 关键字（**看签名**，不猜）。

    redis-py 的 ``Redis.close`` / ``Redis.aclose`` 都带这个形参；``PubSub.close``
    与自研后端的 ``close``/``aclose`` 不带（4.5.5 的 ``PubSub.close(self)`` 传这个
    关键字会 ``TypeError``）。签名不可知（C 实现 / 内建 / 装饰器遮蔽）⇒ 按**不支持**
    处理，绝不赌。
    """
    try:
        sig = inspect.signature(closer)
    except (TypeError, ValueError):
        return False
    params = sig.parameters
    if "close_connection_pool" in params:
        return True
    return any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


def _pool_closing(closer) -> Callable[[], Any]:
    """把 closer 包成「连连接池一起关」的调用（#109 r9/Finding 2）。

    真实 redis-py 上，**裸** ``close()`` / ``aclose()`` 常常释放不了连接池：
    不传 ``close_connection_pool`` 时它们回落到 ``auto_close_connection_pool``，
    而该标志由**客户端的构造方式**决定——

      * 4.5.5：``Redis.from_url()`` 内部 ``Redis(connection_pool=pool)`` ⇒ 显式池
        ⇒ ``auto_close_connection_pool=False``（实测）⇒ 裸 ``close()`` **不动池**；
      * 8.1.0：``from_url()`` 事后把 ``auto_close_connection_pool`` 改回 ``True``
        ⇒ 裸 ``aclose()`` 可用；但用户显式传 ``connection_pool=`` 的客户端仍是
        ``False`` ⇒ 裸 ``aclose()`` 同样把池留在原地。

    所以只要签名接受，就显式传 ``close_connection_pool=True``；不接受则原样调用。
    """
    if _accepts_close_connection_pool(closer):
        return functools.partial(closer, close_connection_pool=True)
    return closer


def _resolve_closer(target) -> Callable[[], Any]:
    """按能力挑关闭方法（#109 r8/G2，r9/Finding 2 补「连池一起关」）。

    优先级 ``aclose`` → ``close`` → ``disconnect``：

      * ``aclose``：redis-py ≥5 的 asyncio 客户端 / fakeredis；
      * ``close``：redis-py 4.x 的 asyncio 客户端（``close(close_connection_pool=True)``
        才是真释放）与自定义后端；
      * ``disconnect``：**最后兜底，且在 redis-py 4.x 上是死分支**——实测 4.5.5 的
        ``Redis`` 与 ``PubSub`` 都**没有** ``disconnect`` 属性（r8 文档把它写成
        「redis-py ≥4.2 的 aclose 前身」是错的）；只有别的后端才可能命中。

    这里**不做属性访问的裸调用**：调用方必须在 ``_close_one`` / ``_release_one``
    的 try 内解析，否则缺失属性会以 ``AttributeError`` 逃出关停序列。
    都没有 ⇒ 抛具名 ``AttributeError``，由调用方记 WARNING。
    """
    for name in ("aclose", "close", "disconnect"):
        closer = getattr(target, name, None)
        if not callable(closer):
            continue
        if name == "disconnect":
            return closer
        return _pool_closing(closer)
    raise AttributeError(
        f"{type(target).__name__} 没有可调用的关闭方法（aclose/close/disconnect 均缺失）")


async def _release_one(target) -> None:
    """解析并调用 ``target`` 的关闭方法（#109 r9/Finding 2）；**只在可 await 时** await。

    ``isawaitable`` 不是为「``PubSub.close()`` 会返回 ``None``」准备的——那个说法
    （r9 文档）**不实**：4.5.5 的 ``PubSub.close()`` 虽是**同步**方法
    （``iscoroutinefunction`` 为 False），但在**真实构造**的 ``PubSub`` 上恒返回
    ``reset()`` 的协程（实测：``connection is None`` 时也返回 coroutine，不是
    ``None``）；只有构造中途崩溃、连 ``connection`` 属性都不存在的半成品才会
    ``return None``（而该对象连 ``__del__`` 都会炸）。检查仍然必要，因为**关闭方法
    是同步的**、且别的后端可以同步返回 ``None``/其它非 awaitable——不能假定一定可
    await。
    """
    result = _resolve_closer(target)()
    if inspect.isawaitable(result):
        await result


def _mark_future_retrieved(future: "asyncio.Future") -> None:
    """done 回调：把 future 上的异常标记为**已取用**（避免 GC 时的 never-retrieved 日志）。

    不影响真正的 await 者：``Task.__wakeup`` 仍会从 future 取到同一个异常。
    """
    if not future.cancelled():
        future.exception()


def _create_owner_task(loop: "asyncio.AbstractEventLoop", coro, context) -> "asyncio.Task":
    """经 ``loop.create_task`` 建 teardown 任务（**尊重**用户 task factory）。

    3.11+ 有 ``loop.create_task(..., context=)``；3.10 没有，但 ``Task.__init__``
    会 ``contextvars.copy_context()`` 捕获**当时**的 context，所以在 context 里调用
    即可（#109 r8/F1 的 owner-context 归属）。
    """
    if sys.version_info >= (3, 11):
        return loop.create_task(coro, context=context)
    return context.run(loop.create_task, coro)


def _own_task_directly(loop: "asyncio.AbstractEventLoop", coro, context) -> "asyncio.Task":
    """**绕开 task factory**，直接用 ``asyncio.Task`` 接管一个从未启动的 coroutine。

    #109 r10：工厂交回来的不是可跟踪的 ``asyncio.Task``（已完成的 ``Future`` /
    裸 awaitable / coroutine 本身）时，``close future`` 会永远悬着。此时若 coroutine
    还没被任何人启动/消费，就自己建一个真 Task 接管它——``loop.create_task`` 会再次
    咨询那个已经证明交不出 Task 的工厂，所以必须绕过。coroutine 启动过（
    ``coroutine is being awaited already`` / ``cannot reuse already awaited coroutine``）
    时构造会抛错，由 ``_spawn_teardown`` 的调用方回滚成具名失败。
    """
    if sys.version_info >= (3, 11):
        return asyncio.Task(coro, context=context)
    return context.run(lambda: asyncio.Task(coro, loop=loop))

# #109/F1/F4 判据内核（具名拒绝文案 + 两层判据）拆到 ``plaita/event/_selfclose_guard.py``（r6，r7 重新定位）：
# 该模块只依赖 stdlib，因此可在别的 CPython（3.12/3.13 的干净解释器）里按文件加载、
# 用真实 asyncio 对象驱动（版本表见 scripts/selfclose_109_110_versions.py）。
# **r7：判据不是安全机制，只是提前、响亮的拒绝**——安全性由下面 `close()` /
# `_teardown` 的**顺序与归属**保证（请求在总线构造 context 里调度、teardown 不取消在飞
# 分发、监听最后取消）；漏判只让关停更晚。判据的边界写在 guard 模块 docstring。
from ._selfclose_guard import (          # noqa: E402
    DISPATCH_GRAPH_MAX_NODES as _DISPATCH_GRAPH_MAX_NODES,
    IN_DISPATCH_BUS as _IN_DISPATCH_BUS,
    SELF_CLOSE_BUDGET_REFUSAL as _SELF_CLOSE_BUDGET_REFUSAL,
    SELF_CLOSE_REFUSAL as _SELF_CLOSE_REFUSAL,
    UNSUPPORTED_RUNTIME_REFUSAL as _SELF_CLOSE_UNSUPPORTED_REFUSAL,
    WAKE_GRAPH_INCOMPLETE_REFUSAL as _WAKE_GRAPH_INCOMPLETE_REFUSAL,
    WakeGraphIncomplete,                 # noqa: F401  （测试/repl 用）
    close_graph_verdict as _close_graph_verdict,
    ensure_self_test as _ensure_wake_selftest,
)

# 临时修改sys.path以避免本地文件冲突
original_path = list(sys.path)
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir in sys.path:
    sys.path.remove(current_dir)

# 需要安装的第三方库
try:
    import redis.asyncio as aioredis
    from redis.exceptions import TimeoutError as RedisTimeoutError
    from redis.exceptions import ConnectionError as RedisConnectionError
except ImportError:
    raise ImportError("请安装redis.asyncio依赖: pip install redis")
finally:
    # 恢复原始路径
    sys.path = original_path

from .core import (
    Event, EventBus, EventHandler, EventStorage, EventSubscription, 
    EventSubscriptionStorage, EventProcessingTracker, RetryPolicy
)
from .exceptions import EventError, EventNotFoundError, EventStorageError, EventTimeoutError


class RedisEventStorage(EventStorage):
    """
    基于Redis的事件存储实现
    """
    DEFAULT_TTL = 60 * 60 * 24 * 7  # 7 days

    def __init__(self, redis_url: str, key_prefix: str = "plaita:event:", ttl: Optional[int] = None):
        """
        初始化Redis事件存储
        
        Args:
            redis_url: Redis连接URL
            key_prefix: Redis键前缀
            ttl: 事件数据过期时间（秒），默认 7 天
        """
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.redis = None
        self.ttl = ttl if ttl is not None else self.DEFAULT_TTL
    
    async def initialize(self):
        """初始化Redis连接（按事件循环缓存客户端）。

        worker 的每个分布式步骤跑在 ``asyncio.run`` 新建的循环里
        （``core/async_utils.run_async_from_sync``），aioredis 客户端的连接
        绑定首次使用的循环——跨步骤复用会拿到已关闭循环上的连接，表现为
        随机的 "Event loop is closed"（2026-09 分布式评审 P0-1：约每两次
        挂起失败一次并丢单）。按循环重建客户端 + 刷新 asyncio.Lock。
        """
        loop = asyncio.get_running_loop()
        # 只重建**由本类创建**的客户端（_redis_loop 已登记）；外部注入的
        # client（如测试的 fakeredis、共用连接池）不接管。
        own_client_stale = (
            self.redis is not None
            and getattr(self, "_redis_loop", None) is not None
            and self._redis_loop is not loop
        )
        if self.redis is None or own_client_stale:
            self.redis = await aioredis.from_url(self.redis_url)
            self._redis_loop = loop
            # 旧循环上的连接交由 GC 回收：跨循环 aclose 会触碰死循环的
            # socket，反而报错。
        if hasattr(self, "lock") and getattr(self, "_lock_loop", None) is not None \
                and self._lock_loop is not loop:
            self.lock = asyncio.Lock()
            self._lock_loop = loop
    
    async def store_event(self, event: Event) -> str:
        """存储事件"""
        await self.initialize()
        
        # 将事件序列化为JSON
        event_data = event.model_dump_json()
        
        event_key = f"{self.key_prefix}events:{event.event_id}"
        await self.redis.set(event_key, event_data, ex=self.ttl)
        
        type_key = f"{self.key_prefix}types:{event.event_type}"
        await self.redis.zadd(type_key, {event.event_id: event.timestamp})
        await self.redis.expire(type_key, self.ttl)
        
        return event.event_id
    
    async def batch_store_events(self, events: List[Event]) -> List[str]:
        """批量存储事件"""
        await self.initialize()
        
        # 使用Redis管道批量操作提高性能
        pipe = self.redis.pipeline()
        event_ids = []
        
        for event in events:
            event_data = event.model_dump_json()
            event_key = f"{self.key_prefix}events:{event.event_id}"
            type_key = f"{self.key_prefix}types:{event.event_type}"
            
            pipe.set(event_key, event_data, ex=self.ttl)
            pipe.zadd(type_key, {event.event_id: event.timestamp})
            pipe.expire(type_key, self.ttl)
            event_ids.append(event.event_id)
        
        # 执行管道
        await pipe.execute()
        return event_ids
    
    async def get_event(self, event_id: str) -> Optional[Event]:
        """获取事件"""
        await self.initialize()
        
        event_key = f"{self.key_prefix}events:{event_id}"
        event_data = await self.redis.get(event_key)
        
        if not event_data:
            return None
            
        try:
            return Event.model_validate_json(event_data)
        except Exception as e:
            raise EventError(f"解析事件数据失败: {e}")
    
    @staticmethod
    def _zset_member_ids(event_ids) -> List[str]:
        """把 ``zrangebyscore`` 返回的 member 归一化为 ``str``。

        #109/#110（验收者补的相邻缺陷）：``initialize()`` 用
        ``aioredis.from_url(url)``（``decode_responses=False``，redis-py 默认），
        ``zrangebyscore`` / ``scan`` 返回 **bytes**。此前直接
        ``f"{prefix}events:{event_id}"`` 会拼出 ``...events:b'<id>'``，mget 永远
        落空 ⇒ ``list_events`` 在**生产连接配置下两个分支都返回空**（fakeredis
        ``decode_responses=False`` 已复现：显式类型 []、全量 []）。显式归一化后
        无论连接是否解码都正确。
        """
        return [
            eid.decode("utf-8") if isinstance(eid, (bytes, bytearray)) else str(eid)
            for eid in event_ids
        ]

    async def list_events(self, event_type: Optional[str] = None, 
                         start_time: Optional[float] = None,
                         end_time: Optional[float] = None,
                         limit: int = 100) -> List[Event]:
        """列出事件"""
        await self.initialize()
        
        result = []
        
        # 如果指定了事件类型，则只搜索该类型
        if event_type:
            type_key = f"{self.key_prefix}types:{event_type}"
            
            # 构建时间范围
            min_score = start_time if start_time is not None else "-inf"
            max_score = end_time if end_time is not None else "+inf"
            
            # 使用ZRANGEBYSCORE查询指定时间范围内的事件ID
            event_ids = await self.redis.zrangebyscore(
                type_key, min_score, max_score, 
                start=0, num=limit
            )
            
            # 批量获取事件数据
            if event_ids:
                event_keys = [
                    f"{self.key_prefix}events:{event_id}"
                    for event_id in self._zset_member_ids(event_ids)
                ]
                event_data_list = await self.redis.mget(*event_keys)
                
                for event_data in event_data_list:
                    if event_data:
                        try:
                            result.append(Event.model_validate_json(event_data))
                        except Exception:
                            logger.warning("skipping unparseable event payload in mget batch", exc_info=True)
        else:
            type_keys = []
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(cursor, match=f"{self.key_prefix}types:*", count=100)
                type_keys.extend(keys)
                if cursor == 0:
                    break
            
            type_prefix = f"{self.key_prefix}types:"
            for type_key in type_keys[:10]:
                type_key_str = type_key.decode('utf-8') if isinstance(type_key, bytes) else type_key
                # #110/F3：按**已知前缀**剥离 event_type，而不是 split(':')[-1]——
                # 后者在 event_type 自身含 ':' 时取到截断值（`order:created` →
                # `created`），重建的 type key 落空 ⇒ 该类型事件**静默丢失**
                # （fakeredis 复现：全量只回 'plain'，'order:created' 消失）。
                if type_key_str.startswith(type_prefix):
                    event_type = type_key_str[len(type_prefix):]
                else:
                    event_type = type_key_str
                
                # 修复: 直接使用单类型查询逻辑而不是递归调用
                sub_limit = max(1, limit // len(type_keys))
                type_key = f"{self.key_prefix}types:{event_type}"
                
                # 构建时间范围
                min_score = start_time if start_time is not None else "-inf"
                max_score = end_time if end_time is not None else "+inf"
                
                # 使用ZRANGEBYSCORE查询指定时间范围内的事件ID
                event_ids = await self.redis.zrangebyscore(
                    type_key, min_score, max_score, 
                    start=0, num=sub_limit
                )
                
                # 批量获取事件数据
                if event_ids:
                    event_keys = [
                        f"{self.key_prefix}events:{event_id}"
                        for event_id in self._zset_member_ids(event_ids)
                    ]
                    event_data_list = await self.redis.mget(*event_keys)
                    
                    for event_data in event_data_list:
                        if event_data:
                            try:
                                result.append(Event.model_validate_json(event_data))
                            except Exception:
                                logger.warning("skipping unparseable event payload in paged mget", exc_info=True)

                if len(result) >= limit:
                    break
        
        return result[:limit]
    
    async def delete_event(self, event_id: str) -> bool:
        """删除事件"""
        await self.initialize()
        
        # 获取事件数据，以确定其类型
        event = await self.get_event(event_id)
        if not event:
            return False
            
        # 从事件类型索引中移除
        type_key = f"{self.key_prefix}types:{event.event_type}"
        await self.redis.zrem(type_key, event_id)
        
        # 删除事件数据
        event_key = f"{self.key_prefix}events:{event_id}"
        result = await self.redis.delete(event_key)
        
        return result > 0

    async def close(self):
        """关闭 Redis 连接。

        #109 r9/Finding 2：这里也必须**按能力**释放。旧实现直接 ``await
        self.redis.aclose()``——redis-py 4.x 的 asyncio 客户端没有 ``aclose``，
        ``AttributeError`` 让三个存储各自的连接**一个都没释放**（总线那边只能
        记 WARNING 然后继续）。``_release_one`` 认 ``aclose`` → ``close`` →
        ``disconnect``，并在签名接受时传 ``close_connection_pool=True`` 真正关池。
        """
        if self.redis:
            await _release_one(self.redis)
            self.redis = None


class RedisEventSubscriptionStorage(EventSubscriptionStorage):
    """
    基于Redis的事件订阅存储实现
    """
    DEFAULT_TTL = 60 * 60 * 24 * 7  # 7 days

    def __init__(self, redis_url: str, key_prefix: str = "plaita:subscription:", ttl: Optional[int] = None):
        """
        初始化Redis事件订阅存储
        
        Args:
            redis_url: Redis连接URL
            key_prefix: Redis键前缀
            ttl: 订阅数据过期时间（秒），默认 7 天
        """
        self.redis_url = redis_url
        self.ttl = ttl if ttl is not None else self.DEFAULT_TTL
        self.key_prefix = key_prefix
        self.redis = None
        self.lock = asyncio.Lock()  # 用于原子操作

    async def initialize(self):
        """初始化Redis连接（按事件循环缓存客户端）。

        worker 的每个分布式步骤跑在 ``asyncio.run`` 新建的循环里
        （``core/async_utils.run_async_from_sync``），aioredis 客户端的连接
        绑定首次使用的循环——跨步骤复用会拿到已关闭循环上的连接，表现为
        随机的 "Event loop is closed"（2026-09 分布式评审 P0-1：约每两次
        挂起失败一次并丢单）。按循环重建客户端 + 刷新 asyncio.Lock。
        """
        loop = asyncio.get_running_loop()
        # 只重建**由本类创建**的客户端（_redis_loop 已登记）；外部注入的
        # client（如测试的 fakeredis、共用连接池）不接管。
        own_client_stale = (
            self.redis is not None
            and getattr(self, "_redis_loop", None) is not None
            and self._redis_loop is not loop
        )
        if self.redis is None or own_client_stale:
            self.redis = await aioredis.from_url(self.redis_url)
            self._redis_loop = loop
            # 旧循环上的连接交由 GC 回收：跨循环 aclose 会触碰死循环的
            # socket，反而报错。
        if hasattr(self, "lock") and getattr(self, "_lock_loop", None) is not None \
                and self._lock_loop is not loop:
            self.lock = asyncio.Lock()
            self._lock_loop = loop
    
    async def store_subscription(self, subscription: EventSubscription) -> str:
        """存储事件订阅"""
        await self.initialize()
        
        subscription_data = subscription.model_dump_json()
        pipe = self.redis.pipeline()
        
        subscription_key = f"{self.key_prefix}data:{subscription.subscription_id}"
        pipe.set(subscription_key, subscription_data, ex=self.ttl)
        
        type_key = f"{self.key_prefix}type:{subscription.event_type}"
        pipe.sadd(type_key, subscription.subscription_id)
        pipe.expire(type_key, self.ttl)
        
        if subscription.correlation_id:
            correlation_key = f"{self.key_prefix}correlation:{subscription.correlation_id}"
            pipe.sadd(correlation_key, subscription.subscription_id)
            pipe.expire(correlation_key, self.ttl)
        
        if subscription.flow_id:
            flow_key = f"{self.key_prefix}flow:{subscription.flow_id}"
            pipe.sadd(flow_key, subscription.subscription_id)
            pipe.expire(flow_key, self.ttl)
        
        if subscription.node_id:
            node_key = f"{self.key_prefix}node:{subscription.node_id}"
            pipe.sadd(node_key, subscription.subscription_id)
            pipe.expire(node_key, self.ttl)
        
        await pipe.execute()
        return subscription.subscription_id
    
    async def get_subscription(self, subscription_id: str) -> Optional[EventSubscription]:
        """获取事件订阅"""
        await self.initialize()
        
        subscription_key = f"{self.key_prefix}data:{subscription_id}"
        data = await self.redis.get(subscription_key)
        
        if not data:
            return None
            
        try:
            return EventSubscription.model_validate_json(data)
        except Exception as e:
            raise EventError(f"解析订阅数据失败: {e}")
    
    async def list_subscriptions(self, 
                               event_type: Optional[str] = None,
                               correlation_id: Optional[str] = None,
                               flow_id: Optional[str] = None,
                               node_id: Optional[str] = None) -> List[EventSubscription]:
        """列出事件订阅。

        索引口径（#110，与 memory / sqlalchemy 后端对齐）：
        ``correlation_id`` **优先于** ``event_type``——两者同时给出时走
        ``{prefix}correlation:{correlation_id}`` 索引，代价与**该 correlation_id
        的订阅数**成正比，而不是与该 event_type 的全部订阅数成正比。这与 memory 的
        「两个条件都过滤」和 sqlalchemy 的「correlation_id 收进 WHERE」得到同一集合：
        带 ``correlation_id`` 过滤时，``correlation_id`` 为空的订阅**不返回**
        （``matches_event`` 对空 correlation 是通配，但三个后端的 list 过滤口径都是
        「给了 correlation_id 就必须相等」，见 tests/unit/test_issue109_110_event_redis_parity.py）。
        ``correlation_id`` 为空时行为不变：依次退到 event_type / flow_id / node_id
        索引，四者都为空时走全量 ``SCAN``。

        **合取语义（#110 r5 决定）**：索引只用来**选候选集**；调用方给出的每一个
        过滤条件都必须真正生效（AND）。判据：``list_subscriptions`` 是列表/内省查询，
        memory 与 sqlalchemy 都按 AND 过滤，且本 docstring 一直声称三后端同一集合；
        redis r3 及以前只按 correlation 索引取行、不再校验 ``event_type`` ⇒
        ``list_subscriptions(event_type='shared.type', correlation_id='CID')`` 会把
        同 correlation 但 **event_type 不同**的行也返回（真实 Redis 实测 2 行，
        memory/sqlalchemy 1 行）——即「要 shared.type 却静默拿到 other.type」的错答。
        这里选择**让三后端一致（AND）**，而不是把错答写成声明边界。索引选择不变
        （correlation 优先，候选集仍只与该 correlation 的订阅数成正比，不增加 GET），
        只是对已取回的候选行做其余条件的过滤。
        """
        await self.initialize()
        
        # 确定使用哪个索引（#110：correlation_id 选择性更高，优先）
        if correlation_id:
            key = f"{self.key_prefix}correlation:{correlation_id}"
            subscription_ids = await self.redis.smembers(key)
        elif event_type:
            key = f"{self.key_prefix}type:{event_type}"
            subscription_ids = await self.redis.smembers(key)
        elif flow_id:
            key = f"{self.key_prefix}flow:{flow_id}"
            subscription_ids = await self.redis.smembers(key)
        elif node_id:
            key = f"{self.key_prefix}node:{node_id}"
            subscription_ids = await self.redis.smembers(key)
        else:
            data_keys = []
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(cursor, match=f"{self.key_prefix}data:*", count=100)
                data_keys.extend(keys)
                if cursor == 0:
                    break
            # #110/F3：按**已知前缀**剥离 id，而不是 split(':')[-1]——后者在
            # subscription_id 自身含 ':' 时取到截断值，GET 落空 ⇒ 全量路径**静默
            # 返回空**（生产 uuid4 无冒号所以没暴露；语料的 global 格把它钉住）。
            _data_prefix = f"{self.key_prefix}data:"
            subscription_ids = [
                (key.decode('utf-8') if isinstance(key, bytes) else key)[len(_data_prefix):]
                for key in data_keys
            ]
        
        # 处理空集合的情况
        if not subscription_ids:
            return []
            
        # 将set转换为list
        subscription_ids_list = list(subscription_ids)
        
        # 转换字节字符串为普通字符串
        str_ids = []
        for sid in subscription_ids_list:
            if isinstance(sid, bytes):
                str_ids.append(sid.decode('utf-8'))
            else:
                str_ids.append(sid)
        
        # 批量获取订阅数据
        result = []
        for subscription_id in str_ids:
            subscription = await self.get_subscription(subscription_id)
            if subscription:
                result.append(subscription)

        # #110（r5）：索引只是候选集，给出的每个条件都要生效（AND），与 memory /
        # sqlalchemy 的 list 过滤同口径。过滤不产生额外 Redis 往返。
        if event_type or correlation_id or flow_id or node_id:
            def _keep(row: EventSubscription) -> bool:
                if event_type and row.event_type != event_type:
                    return False
                if correlation_id and row.correlation_id != correlation_id:
                    return False
                if flow_id and row.flow_id != flow_id:
                    return False
                if node_id and row.node_id != node_id:
                    return False
                return True
            result = [row for row in result if _keep(row)]

        return result
    
    async def delete_subscription(self, subscription_id: str) -> bool:
        """删除事件订阅"""
        await self.initialize()
        
        # 获取订阅数据，以移除索引
        subscription = await self.get_subscription(subscription_id)
        if not subscription:
            return False
            
        # 从各索引中移除
        pipe = self.redis.pipeline()
        
        # 事件类型索引
        type_key = f"{self.key_prefix}type:{subscription.event_type}"
        pipe.srem(type_key, subscription_id)
        
        # 关联ID索引
        if subscription.correlation_id:
            correlation_key = f"{self.key_prefix}correlation:{subscription.correlation_id}"
            pipe.srem(correlation_key, subscription_id)
        
        # 流程ID索引
        if subscription.flow_id:
            flow_key = f"{self.key_prefix}flow:{subscription.flow_id}"
            pipe.srem(flow_key, subscription_id)
        
        # 节点ID索引
        if subscription.node_id:
            node_key = f"{self.key_prefix}node:{subscription.node_id}"
            pipe.srem(node_key, subscription_id)
        
        # 删除主数据
        subscription_key = f"{self.key_prefix}data:{subscription_id}"
        pipe.delete(subscription_key)
        
        # 执行所有操作
        results = await pipe.execute()
        
        # 最后一个结果是删除主数据的结果
        return results[-1] > 0
        
    async def mark_event_processed(self, subscription_id: str, event_id: str) -> bool:
        """原子操作：标记事件为已处理状态（跨进程去重）。

        历史实现是 get 整份订阅 JSON → 内存标记 → set 回去：非原子——两个
        进程并发 mark 同一 (subscription, event) 时各自基于旧快照覆盖写回，
        双双返回 True，同一事件被重复 resume（评审 C3-3）。现改为 SET NX
        去重键（参照 server/event_filter.py 去重的既有模式）：仅首次生效；
        订阅 JSON 里的 processed_events 只在拿到 NX 时顺带同步（读侧
        get_subscription 依赖），权威判重以 NX 键为准。订阅注销不去重键
        ——subscription_id 全局唯一不复用，键随 TTL 自过期。
        """
        await self.initialize()

        # 获取订阅（存在性检查）
        subscription = await self.get_subscription(subscription_id)
        if not subscription:
            return False

        dedup_key = f"{self.key_prefix}processed:{subscription_id}:{event_id}"
        is_new = await self.redis.set(dedup_key, "1", nx=True, ex=self.ttl)
        if not is_new:
            return False

        # 同步订阅数据（best-effort 读侧视图；并发写仍可能互相覆盖，不影响判重）。
        # keepttl=True：不能抹掉 store_subscription 建立的 7 天过期时间，否则
        # 泄漏的挂起订阅键永不过期，被 timeout checker 每 10s 全量 SCAN 永久扫到。
        subscription.mark_event_processed(event_id)
        subscription_key = f"{self.key_prefix}data:{subscription_id}"
        await self.redis.set(subscription_key, subscription.model_dump_json(), keepttl=True)

        return True
    
    async def batch_mark_processed(self, subscription_id: str, event_ids: List[str]) -> bool:
        """批量标记事件为已处理（与单发 mark 同一原子去重原语）"""
        await self.initialize()
        
        # 获取订阅（存在性检查）
        subscription = await self.get_subscription(subscription_id)
        if not subscription:
            return False
            
        for event_id in event_ids:
            await self.mark_event_processed(subscription_id, event_id)
        
        return True
    
    async def close(self):
        """关闭 Redis 连接。

        #109 r9/Finding 2：这里也必须**按能力**释放。旧实现直接 ``await
        self.redis.aclose()``——redis-py 4.x 的 asyncio 客户端没有 ``aclose``，
        ``AttributeError`` 让三个存储各自的连接**一个都没释放**（总线那边只能
        记 WARNING 然后继续）。``_release_one`` 认 ``aclose`` → ``close`` →
        ``disconnect``，并在签名接受时传 ``close_connection_pool=True`` 真正关池。
        """
        if self.redis:
            await _release_one(self.redis)
            self.redis = None

    async def find_unprocessed_matching_subscriptions(self, event: Event) -> List[EventSubscription]:
        """查找未处理过此事件的匹配订阅，并原子地标记为已处理"""
        # 这个方法需要保证原子性，所以使用锁
        async with self.lock:
            await self.initialize()
            
            # 找出所有匹配的订阅
            matching_subscriptions = await self.find_matching_subscriptions(event)
            result = []
            
            # 使用管道批量更新
            pipe = self.redis.pipeline()
            
            for subscription in matching_subscriptions:
                if event.event_id not in subscription.processed_events:
                    # 标记为已处理
                    subscription.mark_event_processed(event.event_id)
                    
                    # 准备更新
                    subscription_key = f"{self.key_prefix}data:{subscription.subscription_id}"
                    pipe.set(subscription_key, subscription.model_dump_json(), keepttl=True)
                    
                    result.append(subscription)
            
            # 执行所有更新
            if result:
                await pipe.execute()
            
            return result


class RedisProcessingTracker(EventProcessingTracker):
    """基于Redis的事件处理记录跟踪器"""
    
    def __init__(self, redis_url: str, key_prefix: str = "plaita:processed:"):
        """
        初始化Redis事件处理记录
        
        Args:
            redis_url: Redis连接URL
            key_prefix: Redis键前缀
        """
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.redis = None
    
    async def initialize(self):
        """初始化Redis连接（按事件循环缓存客户端）。

        worker 的每个分布式步骤跑在 ``asyncio.run`` 新建的循环里
        （``core/async_utils.run_async_from_sync``），aioredis 客户端的连接
        绑定首次使用的循环——跨步骤复用会拿到已关闭循环上的连接，表现为
        随机的 "Event loop is closed"（2026-09 分布式评审 P0-1：约每两次
        挂起失败一次并丢单）。按循环重建客户端 + 刷新 asyncio.Lock。
        """
        loop = asyncio.get_running_loop()
        # 只重建**由本类创建**的客户端（_redis_loop 已登记）；外部注入的
        # client（如测试的 fakeredis、共用连接池）不接管。
        own_client_stale = (
            self.redis is not None
            and getattr(self, "_redis_loop", None) is not None
            and self._redis_loop is not loop
        )
        if self.redis is None or own_client_stale:
            self.redis = await aioredis.from_url(self.redis_url)
            self._redis_loop = loop
            # 旧循环上的连接交由 GC 回收：跨循环 aclose 会触碰死循环的
            # socket，反而报错。
        if hasattr(self, "lock") and getattr(self, "_lock_loop", None) is not None \
                and self._lock_loop is not loop:
            self.lock = asyncio.Lock()
            self._lock_loop = loop
    
    async def mark_event_processed(self, event_id: str, handler_id: str) -> bool:
        """标记事件已由特定处理器处理"""
        await self.initialize()
        
        key = f"{self.key_prefix}events:{event_id}"
        timestamp = time.time()
        
        # 使用HSETNX确保只设置一次，返回1表示新设置，0表示已存在
        is_new = await self.redis.hsetnx(key, handler_id, str(timestamp))
        
        # 更新最后更新时间
        await self.redis.hset(key, "last_updated", str(timestamp))
        
        # 设置过期时间（默认7天）
        await self.redis.expire(key, 60 * 60 * 24 * 7)
        
        return bool(is_new)
    
    async def is_event_processed(self, event_id: str, handler_id: str) -> bool:
        """检查事件是否已由特定处理器处理"""
        await self.initialize()
        
        key = f"{self.key_prefix}events:{event_id}"
        result = await self.redis.hexists(key, handler_id)
        
        return bool(result)
    
    async def cleanup_old_records(self, max_age_seconds: int = 86400) -> int:
        """清理超过指定时间的处理记录"""
        await self.initialize()
        
        now = time.time()
        oldest_time = now - max_age_seconds
        
        pattern = f"{self.key_prefix}*"
        all_keys = []
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
            all_keys.extend(keys)
            if cursor == 0:
                break
        
        removed = 0
        
        for key in all_keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            
            # 检查键类型是否为哈希表
            key_type = await self.redis.type(key)
            key_type_str = key_type.decode('utf-8') if isinstance(key_type, bytes) else key_type
            
            if key_type_str != 'hash':
                continue  # 跳过非哈希表类型的键
                
            try:
                # 安全地获取最后更新时间
                if not await self.redis.hexists(key, "last_updated"):
                    continue
                    
                last_updated_str = await self.redis.hget(key, "last_updated")
                last_updated = float(last_updated_str) if last_updated_str else None
                
                if last_updated and last_updated < oldest_time:
                    # 历史记录键与写入/读取对齐（评审 C3-1）：history 写在
                    # ``history:{event_id}``（record_processing_attempt /
                    # get_processing_history），清理必须按同一形状删——历史上
                    # 按 ``history:{handler_id}`` 删，形状不符等于不清理，且
                    # handler_id 与某个新鲜事件 event_id 同名时会误删它的历史。
                    event_id = key_str.rsplit(":", 1)[-1]
                    history_key = f"{self.key_prefix}history:{event_id}"
                    await self.redis.delete(history_key)
                    
                    # 删除记录本身
                    await self.redis.delete(key)
                    removed += 1
            except (ValueError, TypeError) as e:
                # 处理解析错误
                logger.warning("清理记录时出错: %s, key=%s", e, key_str)
        
        return removed
    
    async def close(self):
        """关闭 Redis 连接。

        #109 r9/Finding 2：这里也必须**按能力**释放。旧实现直接 ``await
        self.redis.aclose()``——redis-py 4.x 的 asyncio 客户端没有 ``aclose``，
        ``AttributeError`` 让三个存储各自的连接**一个都没释放**（总线那边只能
        记 WARNING 然后继续）。``_release_one`` 认 ``aclose`` → ``close`` →
        ``disconnect``，并在签名接受时传 ``close_connection_pool=True`` 真正关池。
        """
        if self.redis:
            await _release_one(self.redis)
            self.redis = None

    async def record_processing_attempt(self, event_id: str, handler_id: str, 
                                  status: str, error: Optional[str] = None) -> None:
        """记录处理尝试的历史"""
        await self.initialize()
        
        history_key = f"{self.key_prefix}history:{event_id}"
        timestamp = time.time()
        
        # 创建记录
        record = {
            "handler_id": handler_id,
            "timestamp": timestamp,
            "status": status
        }
        
        if error:
            record["error"] = error
            
        # 序列化记录
        record_json = json.dumps(record)
        
        # 新增历史记录时创建有序集合
        await self.redis.zadd(history_key, {record_json: timestamp})
        
        # 设置记录的过期时间
        await self.redis.expire(history_key, 86400)  # 24小时
    
    async def get_processing_history(self, event_id: str) -> List[Dict[str, Any]]:
        """获取事件的处理历史"""
        await self.initialize()
        
        history_key = f"{self.key_prefix}history:{event_id}"
        
        # 检查键是否存在
        if not await self.redis.exists(history_key):
            return []
            
        # 检查键类型
        key_type = await self.redis.type(history_key)
        key_type_str = key_type.decode('utf-8') if isinstance(key_type, bytes) else key_type
        
        # 根据存储类型读取数据
        result = []
        if key_type_str == 'zset':
            # 从有序集合读取
            items = await self.redis.zrange(history_key, 0, -1, withscores=True)
            for item, score in items:
                item_str = item.decode('utf-8') if isinstance(item, bytes) else item
                try:
                    record = json.loads(item_str)
                    result.append(record)
                except json.JSONDecodeError:
                    continue
        elif key_type_str == 'list':
            # 从列表读取 (兼容旧格式)
            items = await self.redis.lrange(history_key, 0, -1)
            for item in items:
                item_str = item.decode('utf-8') if isinstance(item, bytes) else item
                try:
                    record = json.loads(item_str)
                    result.append(record)
                except json.JSONDecodeError:
                    continue
        
        # 按时间戳排序
        result.sort(key=lambda x: x.get('timestamp', 0))
        return result


class RedisEventBus(EventBus):
    """
    基于Redis的事件总线实现
    """
    
    def __init__(self, redis_url: str, event_key_prefix: str = "plaita:event:", 
                subscription_key_prefix: str = "plaita:subscription:",
                processing_key_prefix: str = "plaita:processed:"):
        """
        初始化Redis事件总线
        
        Args:
            redis_url: Redis连接URL
            event_key_prefix: 事件存储的键前缀
            subscription_key_prefix: 订阅存储的键前缀
            processing_key_prefix: 处理记录的键前缀
        """
        self.redis_url = redis_url
        
        # 存储和记录
        self.event_storage = RedisEventStorage(redis_url, event_key_prefix)
        self.subscription_storage = RedisEventSubscriptionStorage(redis_url, subscription_key_prefix)
        self.processing_tracker = RedisProcessingTracker(redis_url, processing_key_prefix)
        
        # Redis pubsub对象
        self.redis = None
        self.pubsub = None
        
        # 事件类型到等待future的映射
        self.waiting_futures = {}
        
        # 事件监听器任务
        self.listeners = []

        # fire-and-forget 分发任务的强引用（#109，与 memory 后端的
        # InMemoryEventBus._pending_tasks / _track_task 同一机制）：
        # {task: (event_id, handler_id)}。task 完成回调自动移除，不阻碍回收。
        self._pending_tasks: Dict["asyncio.Task", tuple] = {}
        # close() 已开始排空后拒绝新分发（#109 收尾围栏，避免绕过 drain）
        self._closing = False

        # #109 r7：关停请求的**归属上下文**。在总线构造时捕获，teardown 任务显式以它
        # 创建（``loop.create_task(..., context=...)``）⇒ teardown **永远不是调用者的
        # context 后代**，无论调用者跑在什么 context（handler / 显式空 Context / 线程）。
        # 顺手把 dispatch 标记清成 None：teardown 自身不在任何总线的分发图内。
        self._owner_context = contextvars.copy_context()
        self._owner_context.run(_IN_DISPATCH_BUS.set, None)
        # close() 的幂等凭据：首个请求创建，重复请求返回**同一个** future。
        self._close_future: Optional["asyncio.Future"] = None
        self._teardown_task: Optional["asyncio.Task"] = None
        # 观测/断言用：teardown 真的跑了几次（必须恰好 1 次）。
        self._teardown_runs = 0
        # #109 r9/Finding 3：teardown **Task** 上逃逸的异常（步骤异常；被取消不算）在
        # 这里具名留档。done 回调顺手 ``task.exception()`` 取用它——不取用的话解释器
        # 会在 GC 时打印 "Task exception was never retrieved — ... RuntimeError(...)"
        # （r8 只给 close **Future** 挂了取用回调，Task 这一侧漏了）。
        self._teardown_task_error: Optional[BaseException] = None
        self._teardown_error_kind: Optional[str] = None

        # 事件处理器字典 {handler_id: (handler_func, event_type, filter_condition, retry_policy)}
        self.handlers = {}
        self.lock = asyncio.Lock()
    
    def _track_task(self, task: "asyncio.Task", event_id: str, handler_id: str) -> None:
        """持住 fire-and-forget 分发任务的强引用（#109）。

        裸 ``create_task`` 的返回值不落到任何地方时，loop 关闭 / ``asyncio.run``
        收尾会把在飞任务静默取消（事件既不进 handler、也不写处理记录）。这里与
        memory 后端 ``InMemoryEventBus._track_task`` 同形：登记强引用 + 完成回调
        自动丢弃；额外记录 ``(event_id, handler_id)`` 供 ``close()`` 在超时取消时
        点名（至少不出现「没处理、没记录、没日志」的第三态）。
        """
        self._pending_tasks[task] = (event_id, handler_id)
        task.add_done_callback(lambda t: self._pending_tasks.pop(t, None))

    async def _drain_pending_tasks(self) -> None:
        """有界等待已登记的在飞分发任务；**绝不取消**它们（#109 r7）。

        r6 及以前的实现是「等满 ``DISPATCH_DRAIN_TIMEOUT_SECONDS`` 后逐个
        ``task.cancel()`` 再 ``gather``」。那正是 P0 的一半：当某个在飞分发任务正在
        ``await bus.close()`` 时，取消它会沿 ``Task._fut_waiter`` 走进
        ``asyncio.wait``/``gather`` 的 ``_GatheringFuture.cancel`` 环（drain 的 wait
        等它、它等 close、close 等 drain）⇒ 无限递归 ``RecursionError`` +
        「Task was destroyed but it is pending」。r7 改成：

          * 超时的任务只打一条含 ``event_id`` / ``handler_id`` 的 WARNING
            （「未在 N 秒内完成，放手让它跑完」），**不 cancel、不清空登记**；
          * 登记表 ``_pending_tasks`` 由任务自己的完成回调回收——``close()`` 返回后
            仍在表里的，就是被点名过的慢任务（由调用方/事件循环收尾）。

        本方法自身**不抛**（除 CancelledError）：任何内部异常都具名记 WARNING 后返回，
        保证关停序列不会卡在这一步。
        """
        pending = [t for t in self._pending_tasks if not t.done()]
        if not pending:
            return
        try:
            _done, still = await asyncio.wait(
                pending, timeout=DISPATCH_DRAIN_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:                 # noqa: BLE001 - 关停序列不因 drain 卡住
            logger.warning("事件总线关停：等待在飞分发时异常（继续关停）: %s", exc)
            return
        for task in still:
            event_id, handler_id = self._pending_tasks.get(task, (None, None))
            logger.warning(
                "事件总线关停：分发任务未在 %.1fs 内完成，放手让它跑完并记录"
                "（event_id=%s, handler_id=%s）",
                DISPATCH_DRAIN_TIMEOUT_SECONDS, event_id, handler_id,
            )

    async def initialize(self):
        """初始化Redis连接（按事件循环缓存客户端）。

        worker 的每个分布式步骤跑在 ``asyncio.run`` 新建的循环里
        （``core/async_utils.run_async_from_sync``），aioredis 客户端的连接
        绑定首次使用的循环——跨步骤复用会拿到已关闭循环上的连接，表现为
        随机的 "Event loop is closed"（2026-09 分布式评审 P0-1：约每两次
        挂起失败一次并丢单）。按循环重建客户端 + 刷新 asyncio.Lock。
        """
        loop = asyncio.get_running_loop()
        # 只重建**由本类创建**的客户端（_redis_loop 已登记）；外部注入的
        # client（如测试的 fakeredis、共用连接池）不接管。
        own_client_stale = (
            self.redis is not None
            and getattr(self, "_redis_loop", None) is not None
            and self._redis_loop is not loop
        )
        if self.redis is None or own_client_stale:
            self.redis = await aioredis.from_url(self.redis_url)
            self._redis_loop = loop
            # pubsub 绑定客户端连接，必须随客户端一起重建
            if getattr(self, "pubsub", None) is not None:
                self.pubsub = self.redis.pubsub()
            # 旧循环上的连接交由 GC 回收：跨循环 aclose 会触碰死循环的
            # socket，反而报错。
        if hasattr(self, "lock") and getattr(self, "_lock_loop", None) is not None                 and self._lock_loop is not loop:
            self.lock = asyncio.Lock()
            self._lock_loop = loop
    
    async def publish(self, event: Union[Event, str, Dict[str, Any]], 
                    prevent_duplicate_consumption: bool = True,
                    **kwargs) -> str:
        """发布事件"""
        await self.initialize()
        
        # 标准化事件对象；correlation_id 可经关键字参数传入（EventFilter 的
        # correlation 匹配依赖它——dict/str 形式历史上无法设置，见评审 P2-1）
        correlation_id = kwargs.pop("correlation_id", None)
        if isinstance(event, str):
            event = Event(event_type=event, data=kwargs, correlation_id=correlation_id)
        elif isinstance(event, dict):
            if 'event_type' not in event:
                raise ValueError("事件字典必须包含event_type字段")
                
            event = dict(event)
            event_type = event.pop('event_type')
            if correlation_id is None:
                correlation_id = event.pop('correlation_id', None)
            else:
                event.pop('correlation_id', None)
            event = Event(event_type=event_type, data=event, correlation_id=correlation_id)
        
        # 存储事件
        event_id = await self.event_storage.store_event(event)
        
        # 使用PubSub发布事件通知
        channel = f"plaita:events:{event.event_type}"
        await self.redis.publish(channel, event.model_dump_json())
        
        # 通知等待的future
        if event.event_type in self.waiting_futures:
            futures = self.waiting_futures[event.event_type]
            for future in futures:
                if not future.done():
                    future.set_result(event)
            # 清理已完成的future
            self.waiting_futures[event.event_type] = [f for f in futures if not f.done()]
        
        return event_id
    
    async def batch_publish(self, events: List[Union[Event, str, Dict[str, Any]]],
                          prevent_duplicate_consumption: bool = True) -> List[str]:
        """批量发布事件"""
        await self.initialize()
        
        normalized_events = []
        
        # 标准化所有事件
        for event in events:
            if isinstance(event, str):
                normalized_events.append(Event(event_type=event, data={}))
            elif isinstance(event, dict):
                if 'event_type' not in event:
                    raise ValueError("事件字典必须包含event_type字段")
                event = dict(event)
                event_type = event.pop('event_type')
                normalized_events.append(Event(event_type=event_type, data=event))
            else:
                normalized_events.append(event)
        
        # 批量存储事件
        event_ids = await self.event_storage.batch_store_events(normalized_events)
        
        # 使用管道批量发布通知
        pipe = self.redis.pipeline()
        
        for event in normalized_events:
            channel = f"plaita:events:{event.event_type}"
            pipe.publish(channel, event.model_dump_json())
            
            # 通知等待的future
            if event.event_type in self.waiting_futures:
                futures = self.waiting_futures[event.event_type]
                for future in futures:
                    if not future.done():
                        future.set_result(event)
                # 清理已完成的future
                self.waiting_futures[event.event_type] = [f for f in futures if not f.done()]
        
        # 执行所有发布
        await pipe.execute()
        
        return event_ids
    
    async def register_subscription(self, 
                                  event_type: str, 
                                  filter_condition: Optional[Dict[str, Any]] = None,
                                  correlation_id: Optional[str] = None,
                                  flow_id: Optional[str] = None,
                                  node_id: Optional[str] = None,
                                  timeout: Optional[float] = None,
                                  tenant_id: Optional[str] = None) -> str:
        """注册事件订阅"""
        subscription = EventSubscription(
            event_type=event_type,
            filter_condition=filter_condition or {},
            correlation_id=correlation_id,
            flow_id=flow_id,
            node_id=node_id,
            timeout=timeout,
            # P3：订阅携带租户，超时恢复路径据此定位租户命名空间（None=default）
            tenant_id=tenant_id
        )
        
        subscription_id = await self.subscription_storage.store_subscription(subscription)
        return subscription_id
    
    async def unregister_subscription(self, subscription_id: str) -> bool:
        """取消事件订阅"""
        return await self.subscription_storage.delete_subscription(subscription_id)
    
    async def wait_for_event(self, event_type: str, 
                           timeout: Optional[float] = None,
                           condition: Optional[Callable[[Event], bool]] = None) -> Event:
        """等待特定类型的事件发生"""
        await self.initialize()
        
        # 创建future用于等待事件
        future = asyncio.get_running_loop().create_future()
        
        if event_type not in self.waiting_futures:
            self.waiting_futures[event_type] = []
        
        self.waiting_futures[event_type].append(future)
        
        # 创建一个监听器来接收事件
        async def listen_for_event():
            # 每个等待者独立的 pubsub（2026-10 评审 C3）：历史上用的是
            # self.pubsub——它恒为 None（initialize 只在非 None 时重绑），
            # 首个等待者在 subscribe 处 AttributeError 且异常滞留在任务里
            # （仅 "Task exception was never retrieved" 告警），future 永不
            # resolve 挂到超时；即便初始化了，多个等待者共享一条 pubsub，
            # 一个等待者 finally 的 unsubscribe 会拆掉别人的订阅。
            # 做法 _listen_for_events 相同：局部 pubsub，finally 里 aclose。
            pubsub = self.redis.pubsub()
            try:
                await pubsub.subscribe(f"plaita:events:{event_type}")

                while True:
                    try:
                        # 接收消息
                        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    except (RedisTimeoutError, RedisConnectionError, OSError):
                        # redis-py 5+ 空轮询超时抛异常而非返回 None，属正常节奏
                        continue
                    if message and message['type'] == 'message':
                        try:
                            # 解析事件数据
                            event_data = message['data']
                            event = Event.model_validate_json(event_data)

                            # 检查条件
                            if not condition or condition(event):
                                if not future.done():
                                    future.set_result(event)
                                break
                        except Exception as e:
                            logger.warning("解析事件数据失败: %s", e)

                    # 检查future是否已完成（可能来自其他来源）
                    if future.done():
                        break
            except Exception as e:
                # 监听协程的异常必须传播给等待者，否则只留一条
                # "Task exception was never retrieved"，调用方拿到的是
                # 掩盖真实原因的 EventTimeoutError
                if not future.done():
                    future.set_exception(e)
            finally:
                # 取消订阅并关闭本次等待者自己的 pubsub
                try:
                    await pubsub.unsubscribe(f"plaita:events:{event_type}")
                except Exception:
                    logger.warning("wait_for_event 取消订阅失败", exc_info=True)
                try:
                    await pubsub.aclose()
                except Exception:
                    logger.warning("wait_for_event 关闭 pubsub 失败", exc_info=True)
        
        # 启动监听器
        listen_task = asyncio.create_task(listen_for_event())
        
        try:
            if timeout:
                # 带超时等待
                event = await asyncio.wait_for(future, timeout=timeout)
            else:
                # 无限等待
                event = await future

            return event
        except asyncio.TimeoutError:
            # 超时时从等待列表中移除
            if event_type in self.waiting_futures:
                self.waiting_futures[event_type].remove(future)
            raise EventTimeoutError(event_type, timeout)
        except BaseException:
            # 监听协程失败（连接错误等）：真实异常已 set 到 future，这里
            # 清理等待列表后原样抛出（评审 C3：不再吞成 Task exception
            # was never retrieved + 永久超时）
            if event_type in self.waiting_futures:
                try:
                    self.waiting_futures[event_type].remove(future)
                except ValueError:
                    pass
            raise
        finally:
            # 取消监听任务
            if not listen_task.done():
                listen_task.cancel()
    
    async def register_handler(self, event_type: Optional[str] = None, 
                             handler: EventHandler = None,
                             filter_condition: Optional[Dict[str, Any]] = None,
                             retry_policy: Optional[RetryPolicy] = None) -> str:
        """注册事件处理器"""
        await self.initialize()
        
        handler_id = str(uuid.uuid4())
        
        async with self.lock:
            self.handlers[handler_id] = (handler, event_type, filter_condition, retry_policy)
            
            # 根据event_type决定监听策略
            if not event_type or event_type == "*":
                # 监听所有事件 - 使用模式订阅
                listener_task = asyncio.create_task(
                    self._listen_for_all_events(handler_id)
                )
            elif "*" in event_type:
                # 包含通配符 - 使用模式订阅  
                listener_task = asyncio.create_task(
                    self._listen_for_pattern_events(event_type, handler_id)
                )
            else:
                # 精确匹配 - 使用普通订阅
                listener_task = asyncio.create_task(
                    self._listen_for_events(event_type, handler_id)
                )
            
            self.listeners.append(listener_task)
        
        return handler_id
    
    async def unregister_handler(self, handler_id: str) -> bool:
        """取消注册事件处理器"""
        async with self.lock:
            if handler_id in self.handlers:
                del self.handlers[handler_id]
                return True
            return False
    
    async def _listen_for_events(self, event_type: str, handler_id: str):
        """监听特定类型的事件并分发到处理器"""
        await self.initialize()
        pubsub = self.redis.pubsub()
        
        try:
            await pubsub.subscribe(f"plaita:events:{event_type}")
            
            while handler_id in self.handlers:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                except (RedisTimeoutError, RedisConnectionError, OSError):
                    # redis-py 5+ 空轮询超时抛异常而非返回 None，属正常节奏
                    continue
                if message and message['type'] == 'message':
                    await self._process_message(message, handler_id)
        finally:
            await pubsub.unsubscribe(f"plaita:events:{event_type}")
    
    async def _listen_for_all_events(self, handler_id: str):
        """监听所有事件类型"""
        await self.initialize()
        pubsub = self.redis.pubsub()
        
        try:
            # 使用模式订阅监听所有事件
            await pubsub.psubscribe("plaita:events:*")
            
            while handler_id in self.handlers:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                except (RedisTimeoutError, RedisConnectionError, OSError):
                    continue
                if message and message['type'] == 'pmessage':
                    await self._process_message(message, handler_id)
        finally:
            await pubsub.punsubscribe("plaita:events:*")
    
    async def _listen_for_pattern_events(self, event_pattern: str, handler_id: str):
        """监听模式匹配的事件类型"""
        await self.initialize()
        pubsub = self.redis.pubsub()
        
        try:
            # 转换fnmatch模式为Redis模式（简单的*映射）
            redis_pattern = f"plaita:events:{event_pattern}"
            await pubsub.psubscribe(redis_pattern)
            
            while handler_id in self.handlers:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                except (RedisTimeoutError, RedisConnectionError, OSError):
                    # redis-py 5+ 空轮询超时抛异常而非返回 None，属正常节奏
                    continue
                if message and message['type'] == 'pmessage':
                    await self._process_message(message, handler_id)
        finally:
            await pubsub.punsubscribe(redis_pattern)
    
    async def _process_message(self, message, handler_id: str):
        """处理收到的消息。

        #109/F2：``close()`` 先取消监听任务，而监听器是**内联** ``await``
        ``_process_message`` 的；其首个 await（``is_event_processed``，真实
        Redis 往返）之前/之中被取消时，``CancelledError`` 是 BaseException，
        下面的 ``except Exception`` 接不住 ⇒ 事件既不完成、也不点名。这里显式
        捕获并在重新抛出前**点名**，保证「要么完成、要么被点名」在取消窗口内
        也成立（日志之外无副作用，事件不会因此被执行）。
        """
        event_id = None
        try:
            # 解析事件数据
            event_data = message['data']
            event = Event.model_validate_json(event_data)
            event_id = event.event_id
            
            # 获取处理器信息
            handler_info = self.handlers.get(handler_id)
            if not handler_info:
                return
                
            handler, event_type, filter_condition, retry_policy = handler_info
            
            # 使用统一的通配符匹配逻辑
            if not EventBus.matches_event_type(event_type, event.event_type):
                return
            
            # 检查过滤条件
            if filter_condition:
                subscription = EventSubscription(
                    event_type=event_type or "*",
                    filter_condition=filter_condition
                )
                if not subscription.matches_event(event, {}):
                    return
            
            # 去重：仅跳过已成功处理过的；成功后再 mark（见 _process_event）
            if await self.processing_tracker.is_event_processed(event.event_id, handler_id):
                return
            
            if self._closing:
                # #109 收尾围栏：close() 已开始排空后到达的分发不再静默创建
                # （否则它会绕过本次 drain，成为「没处理、没记录、没日志」的第三态）
                logger.warning(
                    "事件总线正在关停：拒绝新的分发（event_id=%s, handler_id=%s）",
                    event.event_id, handler_id,
                )
                return
            # 处理事件（#109：fire-and-forget 但持强引用，收尾由 close() 排空）
            if retry_policy:
                self._track_task(
                    asyncio.create_task(
                        self._process_with_retry(handler, event, handler_id, retry_policy)
                    ),
                    event.event_id, handler_id,
                )
            else:
                self._track_task(
                    asyncio.create_task(
                        self._process_event(handler, event, handler_id)
                    ),
                    event.event_id, handler_id,
                )
                
        except asyncio.CancelledError:
            # #109/F2：取消窗口点名——不改变取消语义（继续向上抛），只是不留
            # 「既不完成、也不点名」的静默结局。
            logger.warning(
                "事件总线关停：分发在取消窗口内被取消（event_id=%s, handler_id=%s）",
                event_id, handler_id,
            )
            raise
        except Exception as e:
            logger.warning("处理事件消息失败: %s", e)
    
    async def _process_event(self, handler: EventHandler, event: Event, handler_id: str) -> None:
        """处理事件并记录结果；成功后才 mark 去重。"""
        _IN_DISPATCH_BUS.set(self)   # #109/F4：本总线的任务图内标记（copy context 的子任务继承）
        try:
            await handler(event)
            await self.processing_tracker.mark_event_processed(event.event_id, handler_id)
            await self.processing_tracker.record_processing_attempt(
                event.event_id, handler_id, "success"
            )
        except Exception as e:
            await self.processing_tracker.record_processing_attempt(
                event.event_id, handler_id, "error", str(e)
            )
            # fire-and-forget task：吞掉异常，避免 "Task exception was never retrieved"
            logger.warning("handler %s 处理事件 %s 失败: %s", handler_id, event.event_id, e)
    
    async def _process_with_retry(self, handler: EventHandler, event: Event, 
                                handler_id: str, retry_policy: RetryPolicy) -> None:
        """带重试机制的事件处理；成功后才 mark 去重。"""
        _IN_DISPATCH_BUS.set(self)   # #109/F4：本总线的任务图内标记（copy context 的子任务继承）
        retries = 0
        delay = retry_policy.initial_delay
        
        while True:
            try:
                await handler(event)
                await self.processing_tracker.mark_event_processed(event.event_id, handler_id)
                await self.processing_tracker.record_processing_attempt(
                    event.event_id, handler_id, "success"
                )
                break  # 成功处理，退出循环
            except Exception as e:
                retries += 1
                await self.processing_tracker.record_processing_attempt(
                    event.event_id, handler_id, f"error (retry {retries})", str(e)
                )
                
                # 检查是否达到最大重试次数
                if retries >= retry_policy.max_retries:
                    await self.processing_tracker.record_processing_attempt(
                        event.event_id, handler_id, "failed", f"达到最大重试次数 ({retry_policy.max_retries})"
                    )
                    break
                
                # 计算下次重试的延迟
                delay = min(delay * retry_policy.backoff_factor, retry_policy.max_delay)
                await asyncio.sleep(delay)
    
    def _self_close_refusal(self, current) -> Optional[RuntimeError]:
        """(#109/F1/F4, r6；r7 重新定位) 关停前判据：返回要抛的具名 ``RuntimeError``，或 None=放行。

        **r7 语义**：判据只是**提前、响亮的拒绝**，不是安全机制。``close()`` 已按构造
        对任意调用者安全（见 ``request_close`` / ``_teardown``）；这里拒绝是为了在
        **最常见**的写法（handler 内直接关停）上尽早给出可执行的错误，而不是为了
        防住挂起。因此判据的适用前提缩小为「**确有在飞分发任务**」：没有可保护对象时
        一律放行（顺手修掉验收者报告的两类过度拒绝——合法 ``TaskGroup`` 里只有 close
        任务、以及分发结束后 detached 任务里的 close）。

        两层（细节与诚实边界见 ``plaita/event/_selfclose_guard.py`` 的模块 docstring）：

        L1 **dispatch lineage**：``current`` 就是本实例登记的在飞分发任务，或当前
           context 的 ``IN_DISPATCH_BUS`` 标记就是本总线（handler 内 / handler
           ``create_task`` 出的后代任务——context 复制传递）。**不点名任何私有属性**。
           handler 显式换 ``context=Context()`` 时 L1 失灵——L2 命中则提前拒绝，
           命不中则照常关停（只是更晚），两种结局都安全。
        L2 **完成唤醒锥**：从 ``current`` 沿「事件循环真的会用来唤醒等待者」的引用可达
           的人里含本实例在飞分发任务 ⇒ 拒绝；缺属性 / 重命名 / ``TaskGroup.
           _on_completed_fut`` 懒创建窗口 / 未知持有者 / 深度或节点预算耗尽 ⇒ 具名
           incomplete。首次调用先跑 stdlib 属性自检，未过则本进程内（**有在飞分发时**）
           拒绝。本方法**只读、无副作用**，必须在 ``_closing`` / cancel listener /
           drain **之前**调用。
        """
        if not [t for t in self._pending_tasks if not t.done()]:
            # r7：没有在飞分发 ⇒ 没有任何可纳入本次关停的任务，也就没有 task-wait
            # cycle 可言。放行（不再发生「合法 TaskGroup-only / 分发后 detached 调用
            # 被拒」的过度拒绝）。判据保留的唯一意义是拦常标记形状，见 docstring。
            return None
        if current is not None and current in self._pending_tasks:
            # #109 形状①（直接）：handler 内调用 close() ⇒ ``current`` 就是被登记的分发
            # 任务，会把**自身**纳入 drain —— 等满超时后自 cancel，再 await gather(含自身)
            # 触发 RecursionError + "Task was destroyed"。
            return RuntimeError(_SELF_CLOSE_REFUSAL)
        if _IN_DISPATCH_BUS.get() is self:
            # #109/F4 形状：handler 里 create_task/ensure_future/shield 出的子任务继承了
            # marker（context 复制），即使等待构造绕过了等待图也保守拒绝（声明口径）。
            return RuntimeError(_SELF_CLOSE_REFUSAL)
        selftest_error = _ensure_wake_selftest()
        if selftest_error is not None:
            # #109 r6：本 CPython 的 asyncio 簿记属性自检未过（未来 Python 改名）⇒
            # fail-closed：本进程内 close() 一律具名拒绝，绝不带着看不见的图放行。
            return RuntimeError(_SELF_CLOSE_UNSUPPORTED_REFUSAL.format(
                detail=selftest_error))
        verdict, visited, reason = _close_graph_verdict(
            current, self._pending_tasks)
        if verdict == "in_graph":
            # #109/F1：形状②③④……（嵌套 await create_task、gather/shield、显式空
            # context、``TaskGroup.create_task(..., context=Context())``、
            # ``functools.partial`` 的 ``func.__self__``、``asyncio.as_completed`` 的
            # 队列表项、Queue/Event/Condition 的等待者……）。判据是**事件循环的完成
            # 唤醒边**（见 ``close_graph_verdict``），不是回调写法清单。
            return RuntimeError(_SELF_CLOSE_REFUSAL)
        if verdict == "incomplete":
            # #109 r6 fail-closed：缺属性 / 懒创建 / 未知持有者 / 深度耗尽 ⇒
            # 「证明不出来」绝不等于「允许」（r5 的 ``getattr(..., None)`` 就是死在这里）。
            return RuntimeError(_WAKE_GRAPH_INCOMPLETE_REFUSAL.format(
                reason=reason))
        if verdict == "budget":
            # #109/F1 fail-closed：预算内证明不出「不在图内」⇒ 拒绝，绝不当作允许。
            return RuntimeError(_SELF_CLOSE_BUDGET_REFUSAL.format(
                budget=_DISPATCH_GRAPH_MAX_NODES, visited=visited))
        return None

    def request_close(self) -> "asyncio.Future":
        """受理关停请求（#109 r7；r8/F1 修赋值顺序）：**幂等、非阻塞**，返回「关停完成」的同一个 future。

        这是 r7 的核心：``close()`` 不再由调用者直接执行关停序列，而是

          1. 受理时同步置 ``_closing``（立刻停止受理新分发，先于 teardown 的第一个
             事件循环 tick），对**重复请求**直接返回首次创建的同一个 future；
          2. 用 ``context=self._owner_context`` 创建 teardown 任务——owner context 是
             **总线构造时**捕获的，因此 teardown 永远不是调用者的 context 后代：调用者
             是 handler、显式空 ``Context()``、还是线程里 ``run_coroutine_threadsafe``
             投递的协程，都改不了这一点；
          3. 立即返回 future（非阻塞）。等关停完成就 await 它（``close()`` 的做法），
             想 fire-and-forget 就丢掉——teardown 是独立任务，调用者被取消不连坐。

        **r8/F1（赋值顺序）**：teardown 需要读的每个属性都必须**先落盘再 spawn**。
        ``loop.set_task_factory(asyncio.eager_task_factory)``（3.12+ 的**公开**
        配置项）会让 ``create_task`` 同步把 coroutine 跑到第一个挂起点；空闲总线
        （没有在飞分发、没有监听、后端 close 不 await）会一路跑到 ``_teardown`` 的
        ``finally``。r7 先 spawn 再赋 ``_close_future``，此时 ``finally`` 读到
        ``None``，稍后赋值的 future 永不 resolve ⇒ ``await bus.close()`` **永久挂起**。
        故顺序固定为：``_close_future`` → ``_closing`` → ``_teardown_task`` 占位 →
        spawn → 回填真实 task 句柄。任何新的「teardown 会读的属性」都必须加在这之前。

        **r9/Finding 1（spawn 失败必须回滚）**：上面这个顺序自身有一个反面——
        ``_spawn_teardown`` 抛异常时（自定义/抛异常的 task factory、已关闭的循环），
        凭据已经落盘而 teardown 任务根本不存在：``_close_future`` 永不 resolve 且
        ``_closing=True`` ⇒ **第二次 close() 永久挂起**（r7 的旧顺序反而能重试）。
        所以受理是一个**全有或全无**的动作：spawn 抛异常时把 ``_close_future`` /
        ``_teardown_task`` 清回 ``None``、``_closing`` 恢复为受理前的值，并抛具名
        ``error_kind=teardown-spawn-failed``；之后在健康的循环上重试即可正常关停。

        **r10（回滚必须**真实**）**：r9 把「spawn 抛异常」直接等同于「什么都没发生」，
        但工厂完全可能**先把 coroutine 变成 Task 再抛**（``t = asyncio.Task(coro); raise``，
        或 eager factory 同步跑完再抛）。那时 teardown 是我们的、可能已经在跑：
        r9 却回滚成「未受理、总线仍可用」⇒ 非 eager 下留下一个无人取用的孤儿 Task
        （后来死在 ``cannot reuse already awaited coroutine``），eager 下组件已被关掉
        而 ``_closing=False``，第二次 ``close()`` 把三个存储**再关一遍**。
        r10 把「谁创建了 Task」的判定下沉到 ``_spawn_teardown``：**已启动就收养**
        （返回那个 Task，交给下面既有的 ``add_done_callback`` / ``done()`` 分支），
        **从未托管才回滚**。因此本方法既有 except 只在「确实没有 teardown 在跑」时触发，
        回滚后的状态是真实的「总线未动、仍可用」。

        ``_spawn_teardown`` 的返回类型因此收紧为「真正的 ``asyncio.Task``，否则抛异常」
        （非 Task 的工厂返回值在内部被收养或绕开工厂接管）——所以下面
        ``add_done_callback`` / ``done()`` / ``.done()`` 这几处**对返回值的
        使用**对任意 factory 行为都不会失败；会失败的部分全部在 spawn 内部，并已经
        走同一条回滚。这就是「把返回值的使​​用也纳入 try」的等价保证。

        **r9/Finding 3（Task 异常必须被取用）**：teardown 任务上逃逸的异常（步骤抛
        异常）由 done 回调 ``_on_teardown_done`` 立即取用并留档，避免 GC 时打印
        "Task exception was never retrieved"。收养来的任务同样挂这个回调
        （eager 同步跑完的路径由 ``_teardown_entry`` 的 holder 找回句柄）。

        **r9（死句柄）**：只有**仍在飞**的 teardown 任务才回填到 ``_teardown_task``。
        eager factory 下 spawn 可能同步跑完（成功或失败），此时它是 done 句柄；
        回填等于让监督者（``if bus._teardown_task is not None`` / ``.cancel()``）
        把一次**已结束**的 teardown 当成在飞的来跟。

        **r11/A1（「收养了但入口没跑」不再挂起）**：``isinstance(x, asyncio.Task)`` 对
        **任何**真 Task 都为真，但「是真 Task」不等于「它跑我们的 ``_teardown_entry``」。
        工厂交回一个包着别的 coroutine 的 Task（或我们的 coroutine 被别处驱动、或那个
        Task 在返回前就被 cancel）时，r10 会静默收养：入口从未 await、``_teardown``
        从未跑过 ⇒ ``_close_future`` **永不结算**，两次 ``close()`` 都永久挂起
        （``(True, PENDING, True, 0)``、五个组件 ``[0×5]``，外加
        ``RuntimeWarning: coroutine ... _teardown_entry was never awaited``）。
        r11 在 ``_teardown_entry`` **第一行**写「已进入」标记（绑在该次受理的 holder 上，
        holder 又绑在任务自己身上）；收养的 Task 结束时若标记未置位，就具名结算
        ``error_kind=teardown-never-entered``（``_teardown_runs`` 如实为 0；入口
        coroutine 若从未启动则一并 ``close()``，避免 "was never awaited"）。判据是
        「任务结束」这一事件：没有 busy-wait，也没有比它更弱的计时假设。返回时已
        done/cancelled 的 Task 走同一条同步判据（r11/A3）。合法包装协程
        （``async def wrap(): await <我们的 coro>``）跑到入口那一行即置位 ⇒ 照常完成。

        **r11/A2、B（不是我们的 Task 一律拒收）**：跨事件循环建出来的 Task（close future
        属于本循环，在别的循环上结算既非线程安全也永远等不到），以及覆写了
        ``add_done_callback`` / ``done`` / ``cancelled`` / ``exception`` 的**敌对 ``Task``
        子类**（``isinstance`` 会放行 ⇒ 下面这几行**裸抛且不回滚**），都由
        ``_spawn_teardown`` 具名拒绝：reason 写在异常的 ``__cause__``，由本节既有 except
        统一回滚（``error_kind=teardown-spawn-failed``），并尽力中和那个被拒的任务。
        于是下面 ``add_done_callback`` / ``done()`` 对返回值的**使用**对任意 factory 行为
        仍然不会失败——但保证方式从 r10 的「类型是 Task」升级为**结构性检查**：「本节
        用到的四个方法（``add_done_callback`` / ``done`` / ``cancelled`` / ``exception``）
        以及 ``_accept_spawned_task`` 自用的 ``get_loop`` 都是 stdlib 原版，且任务绑在本
        循环上」。

        **out of contract（合成形状；生产路径不使用 ``set_task_factory``，实测
        仓库内 0 处调用）**：①交回的真 Task 既不跑我们的 coroutine、又**永不结束** ⇒
        没有任何完成事件可依，close future 会一直等到那个任务结束（唯一仍可能长等的
        形状）；②我们的 coroutine 被别处驱动、而交回的 Task 又不跑它 ⇒ 按①的完成判据
        具名失败，若别处随后真的跑完 teardown，组件是关掉的、调用方却已看到
        ``teardown-never-entered``；③覆写 ``cancel`` / ``__setattr__`` /
        ``__getattribute__`` 等**未纳入检查**的钩子 ⇒ 拒收路径的「中和」只是尽力而为，
        可能留下一个在别处/别循环上仍会跑的 teardown（具名拒绝本身不变）。
        """
        if self._close_future is not None:
            return self._close_future
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            # #109 r8/G4：无运行循环不是裸 RuntimeError("no running event loop")，
            # 而是与跨循环路径同一家族的具名错误（跨循环分支在 close() 里）。
            raise RuntimeError(
                "RedisEventBus.request_close() 需要运行中的事件循环："
                "关停请求必须在**总线所属的事件循环**里受理（teardown 任务与 close "
                "future 都绑定该循环）。当前没有 running loop；"
                "请在事件循环内调用，或跨线程用 loop.call_soon_threadsafe 投递。"
                "error_kind=no-running-loop"
            ) from exc
        future = loop.create_future()
        # ---- spawn 之前：teardown 触碰的所有属性先就位（r8/F1）----
        previous_closing = self._closing
        self._close_future = future
        self._closing = True
        self._teardown_task = None          # 占位；同步完成时它就是真实的 None 语义
        # ---- spawn：eager task factory 下这里可能同步跑完整个 teardown ----
        try:
            teardown_task = self._spawn_teardown(loop)
        except BaseException as exc:
            # #109 r9/Finding 1：受理必须**全有或全无**。不变量被破坏时（凭据在、
            # 任务不在）第二次 close() 会早返回一个永不 resolve 的 future ⇒ 永久
            # 挂起；这里把状态整体回滚，让重试可行。
            self._close_future = None
            self._teardown_task = None
            self._closing = previous_closing
            if isinstance(exc, Exception):
                raise RuntimeError(_TEARDOWN_SPAWN_FAILED_MESSAGE) from exc
            raise
        # #109 r9/Finding 3：teardown Task 上逃逸的异常必须被取用（留档 + 取用），
        # 否则 GC 时打印 "Task exception was never retrieved"。
        teardown_task.add_done_callback(self._on_teardown_done)
        if teardown_task.done():
            # eager factory 同步跑完（成功或失败）：回调只会被 call_soon 排队，别赌
            # 循环还来得及跑它——当场取用一次。回调幂等，重复调用无害。
            self._on_teardown_done(teardown_task)
        else:
            # r9（死句柄）：只登记**仍在飞**的 teardown；done 句柄回填会让监督者
            # （`.cancel()` / `is not None` 判定）把已结束的关停当成在飞的来跟。
            self._teardown_task = teardown_task
        return future

    async def _teardown_entry(self, holder: Dict[str, Any]) -> None:
        """teardown 的入口包装（#109 r10）：**第一行**就把自己所在的 Task 记进 ``holder``。

        为什么需要：eager task factory（3.12+ 的公开配置）下 coroutine 在 ``create_task``
        内部同步跑到 ``StopIteration``；此后 ``Task.get_coro()`` 返回 ``None``（3.12/3.13
        实测），任务也不再出现在 ``asyncio.all_tasks()`` 里。工厂若在同步跑完之后抛异常，
        靠 coro 身份或 ``all_tasks()`` 都找不回句柄，它的异常就会无人取用
        （"Task exception was never retrieved"）。``holder`` 由调用方在 spawn 之前持有，
        eager 路径下这里第一行即写入 ⇒ 句柄总能拿回。

        #109 r11/A1：``holder['entered']`` 是「本入口**真的**跑过」的标记，写在**第一行**
        （先于取当前任务与 ``await self._teardown()``）。收养到的 Task 结束时若它仍是
        ``False``，就说明那个 Task 根本没跑我们的 coroutine ⇒ close future 必须具名
        失败（``teardown-never-entered``），绝不允许悬着。任何合法的包装协程
        （``async def wrap(): await <我们的 coro>``）一旦跑到这一行就会置位，因此判据
        容忍它——见 ``_on_teardown_done``。
        """
        holder["entered"] = True
        holder["task"] = asyncio.current_task()
        await self._teardown()

    @staticmethod
    def _started_teardown_task(coro, holder: Dict[str, Any]) -> Optional["asyncio.Task"]:
        """找回**已经启动**的 teardown 任务句柄（无则 ``None``）。

        两条线索：①入口包装在 eager 路径写下的 ``holder['task']``；②非 eager 路径下
        （工厂先 ``create_task`` 再抛、任务尚未跑）``asyncio.all_tasks()`` 里
        ``get_coro() is coro`` 的那一条。两者都没有 ⇒ 这个 coroutine 从未被托管。
        """
        task = holder.get("task")
        if isinstance(task, asyncio.Task):
            return task
        for live in asyncio.all_tasks():
            if live.get_coro() is coro:
                return live
        return None

    @staticmethod
    def _close_unstarted_coro(coro) -> None:
        """只对**从未启动**的 teardown coroutine 调 ``close()``（#109 r10 item 3）。

        r9 无条件 ``coro.close()``：工厂「先建 Task 再抛」时那个 Task 已经拥有该
        coroutine，``close()`` 不抛（实测），却把 Task 弄死在
        ``RuntimeError('cannot reuse already awaited coroutine')`` 上，且没人取用它的
        异常。现在只有确认没有 Task 接管过才关；关不掉也只留痕（P0-2 门禁不允许静默吞咽）。
        """
        try:
            coro.close()
        except Exception as exc:                 # noqa: BLE001 - 已在失败路径上
            logger.debug(
                "事件总线关停：关闭未启动的 teardown coroutine 失败（忽略）: %s", exc)

    def _close_entry_coro_if_unstarted(self, coro) -> None:
        """入口 coroutine **从未启动**时才 ``close()``（#109 r11/A1、A2、B）。

        ``_close_unstarted_coro`` 的调用方必须自己保证「从未启动」：已经启动/挂起说明
        别处正在驱动它，关掉会把它弄死在 ``cannot reuse already awaited coroutine``
        （r10 踩过的坑）。这里按 ``inspect.getcoroutinestate`` 如实判断。
        """
        if coro is None:
            return
        try:
            state = inspect.getcoroutinestate(coro)
        except Exception as exc:                 # noqa: BLE001 - 纯清理路径
            logger.debug("事件总线关停：读取入口 coroutine 状态失败（忽略）: %r", exc)
            return
        if state == "CORO_CREATED":
            self._close_unstarted_coro(coro)

    def _bind_acceptance(self, task: "asyncio.Task", holder: Dict[str, Any]) -> "asyncio.Task":
        """把本次受理的 holder 绑到要交出去的 Task 上（#109 r11/A1），返回同一个 Task。

        绑在**任务自己**身上而不是总线字段：上一轮的 Task 在重试之后才回调时，读到的仍是
        它自己那一轮的标记，绝不会拿新一轮的 holder 去判定。绑定失败（``__setattr__``
        被覆写）意味着这个 Task 不可跟踪 ⇒ 当场拒绝受理，由 ``request_close`` 的既有
        except 整体回滚——绝不交出一个「跑没跑过都不知道」的句柄。
        """
        try:
            setattr(task, _TEARDOWN_ACCEPTANCE_ATTR, holder)
            if getattr(task, _TEARDOWN_ACCEPTANCE_ATTR, None) is not holder:
                raise RuntimeError("回读到的受理标记不是刚写进去的那个")
        except BaseException as exc:             # noqa: BLE001 - 不可跟踪即拒收
            raise RuntimeError(
                "RedisEventBus 关停：无法把受理标记绑到 teardown 任务上"
                f"（{type(task).__name__} 的 __setattr__/__getattribute__ 不可信）: {exc!r}"
            ) from exc
        return task

    def _reject_spawned_task(self, task: "asyncio.Task", coro, reason: str) -> "NoReturn":
        """拒绝受理交回的 Task：具名（写在 ``__cause__``）+ 尽力中和 + 抛出（r11/A2、B）。

        「拒绝」必须配「中和」，否则等于把我们的 teardown coroutine 留给一个在别处/别的
        循环上可能跑起来的任务——那会变成二次 close。中和只有两件尽力而为的事：挂一个
        取用异常的回调（防 never retrieved），以及 ``cancel()``（尚未 step 的任务会在首次
        step 时就地取消 ⇒ 入口不会执行、``_teardown_runs`` 不变）。任何一步失败都只留痕，
        不掩盖具名拒绝本身。
        """
        logger.warning(
            "事件总线关停：拒绝受理工厂交回的 Task（reason=%s, type=%s）",
            reason, type(task).__name__)
        message = (_TEARDOWN_FOREIGN_LOOP_MESSAGE if reason == TEARDOWN_FOREIGN_LOOP_REASON
                   else _TEARDOWN_HOSTILE_TASK_MESSAGE)
        try:
            owns_ours = task.get_coro() is coro
        except BaseException as exc:             # noqa: BLE001 - 纯中和路径
            logger.debug("事件总线关停：读取被拒任务持有的 coroutine 失败（忽略）: %r", exc)
            owns_ours = False
        try:
            task.add_done_callback(_swallow_task_result)
        except BaseException as exc:             # noqa: BLE001 - 纯中和路径
            logger.debug("事件总线关停：给被拒任务挂取用回调失败（忽略）: %r", exc)
        try:
            task.cancel()
        except BaseException as exc:             # noqa: BLE001 - 纯中和路径
            logger.debug("事件总线关停：取消被拒任务失败（忽略）: %r", exc)
        if not owns_ours:
            self._close_entry_coro_if_unstarted(coro)
        raise RuntimeError(message)

    def _accept_spawned_task(self, loop: "asyncio.AbstractEventLoop",
                             spawned: "asyncio.Task", coro) -> "asyncio.Task":
        """校验工厂交回的真 Task 是否可以安全收养（#109 r11/A2、B）。

        两条硬条件：①它的循环必须是**我们**的（否则 close future 永远等不到结算）；
        ②``request_close`` 要用的那几个方法必须是 stdlib 原版（否则裸抛且不回滚）。
        任一条不满足 ⇒ 具名拒绝 + 尽力中和，由 ``request_close`` 的 except 整体回滚。
        """
        if not _task_uses_stdlib_methods(spawned):
            self._reject_spawned_task(spawned, coro, TEARDOWN_HOSTILE_TASK_REASON)
        ok, spawned_loop = _guarded("读取被收养任务的 loop", spawned.get_loop)
        if not ok:
            self._reject_spawned_task(spawned, coro, TEARDOWN_HOSTILE_TASK_REASON)
        if spawned_loop is not loop:
            self._reject_spawned_task(spawned, coro, TEARDOWN_FOREIGN_LOOP_REASON)
        return spawned

    def _spawn_teardown(self, loop: "asyncio.AbstractEventLoop") -> "asyncio.Task":
        """在 **owner context** 里创建 teardown 任务（#109 r8/F1；r10 分两条路径；r11 加硬条件）。

        契约（r11 收紧到**结构性可证明**的程度）：**返回值一定是「四个被
        ``request_close`` 使用的方法都是 stdlib 原版、且绑在同一个事件循环上」的真
        ``asyncio.Task``，否则抛异常**。因此 ``request_close`` 里对返回值的**使用**
        （``add_done_callback`` / ``done()`` / 取用 ``cancelled()`` / ``exception()``）
        不可能因为 factory 的行为而失败——这是「把返回值的使用也纳入 try」的等价保证：
        会失败的部分全部在**本方法内部**发生，由 ``request_close`` 既有的 except 统一回滚。

        四种 factory 行为：

          * **正常返回 Task**：先过 ``_accept_spawned_task``（同循环 + 方法未覆写），再绑
            上本次受理的标记。
          * **返回非 Task**（已完成的 ``Future`` / 裸 awaitable / coroutine）：它跟踪不了，
            close future 会永远悬着、coroutine 还会 "was never awaited"。若 coroutine
            已被启动则收养那个真任务；否则**绕开工厂**用 ``asyncio.Task`` 接管它
            （``_own_task_directly``）⇒ 真跑一次 teardown。
          * **抛异常**：工厂可能已经把 coroutine 变成 Task（先 create_task 再抛、eager
            factory 同步跑完再抛）。此时 teardown 是**我们的、可能正在跑**，必须**收养**
            （返回任务、不回滚），否则调用方会看到「未受理、总线仍可用」而组件其实已被
            关掉（r9 的假回滚 ⇒ 二次 close 双关三个存储）。只有确认 coroutine 从未被
            托管时才 ``close()`` 并让异常冒泡给调用方回滚。
          * **交回「不是我们的」Task**（#109 r11）：真 ``asyncio.Task`` 但①建在**另一个**
            事件循环上，或②覆写了 ``request_close`` 要用的 stdlib 方法 —— 拒绝受理
            （具名 reason 写在 ``__cause__``，整体回滚）并尽力中和它。**只**接受「方法未被
            动过 + 同循环」的 Task；被收养的 Task 结束时若入口标记未置位，由
            ``_on_teardown_done`` 具名结算 ``teardown-never-entered``，绝不悬着。

        契约之外（**out of contract**，见类文档的残差清单）：交回的真 Task 既不跑我们的
        coroutine、又**永不结束**时，没有任何完成事件可依，close future 会一直等到那个任务
        结束为止（这是唯一仍可能长等的合成形状；生产路径不使用 ``set_task_factory``）。
        """
        holder: Dict[str, Any] = {
            "entered": False, "task": None, "coro": None,
            "future": self._close_future,   # 本次受理那一轮的 future（晚到回调不误伤重试）
        }
        coro = self._teardown_entry(holder)
        holder["coro"] = coro
        context = self._owner_context.copy()
        try:
            spawned = _create_owner_task(loop, coro, context)
        except BaseException:
            owner = self._started_teardown_task(coro, holder)
            if owner is not None:
                logger.debug(
                    "事件总线关停：spawn 抛异常但 teardown 任务已存在，改为收养: %r", owner)
                return self._bind_acceptance(
                    self._accept_spawned_task(loop, owner, coro), holder)
            self._close_unstarted_coro(coro)
            raise
        if isinstance(spawned, asyncio.Task):
            return self._bind_acceptance(
                self._accept_spawned_task(loop, spawned, coro), holder)
        owner = self._started_teardown_task(coro, holder)
        if owner is not None:
            logger.debug(
                "事件总线关停：工厂返回 %r 而非 Task，收养已启动的 teardown 任务", type(spawned))
            return self._bind_acceptance(
                self._accept_spawned_task(loop, owner, coro), holder)
        logger.debug(
            "事件总线关停：工厂返回 %r 而非 Task，绕开工厂直接接管 teardown coroutine",
            type(spawned))
        return self._bind_acceptance(_own_task_directly(loop, coro, context), holder)

    def _on_teardown_done(self, task: "asyncio.Task") -> None:
        """teardown 任务的 done 回调：**取用**并具名留档它的异常（#109 r9/Finding 3）。

        r8 只给 close **Future** 挂了 ``_mark_future_retrieved``，Task 这一侧漏了：
        步骤抛异常（``RuntimeError('boom-step')``）时该 Task 的异常没有任何人取用，
        解释器在 GC 时经 ``loop.call_exception_handler`` 打印
        "Task exception was never retrieved"（3.11 + 3.12 实测；in-process 反复跑
        396 次出现 12 次）。取用**不影响**等待者：关停失败是经 close future 报的。

        被取消（``CancelledError``）不算「未取用」，但同样具名留档，便于归因。

        #109 r10（记录口径一致）：``_teardown_error_kind`` / ``_teardown_task_error``
        必须描述**同一次**收尾。r9 在取消分支只写 kind 不碰 error，于是
        `kind='cancelled'` 却仍挂着上一轮的 ``ValueError``；正常完成时也不清，重试
        成功后仍显示上一次的失败。现在三条分支各自把这一对**整体**改写：
        取消 ⇒ ('cancelled', None)；异常 ⇒ (type(exc).__name__, exc)；正常完成
        ⇒ (None, None)。第二次失败仍然覆盖第一次，但覆盖的是**成对**的。

        #109 r11/A1、A3（入口从未执行）：收尾的**前置判据**是本次受理的入口标记
        （``_teardown_entry`` 第一行置位，绑在任务自己身上）。若它从未置位——工厂交回的
        Task 不跑我们的 coroutine、返回前已被 cancel、或「返回时已 done/cancelled」——
        就具名结算 ``teardown-never-entered``（而不是留下一辈子 PENDING 的 close
        future），并如实保持 ``_teardown_runs == 0``。合法包装协程
        （``async def wrap(): await <我们的 coro>``）跑到入口那一行即置位，因此照常走下面
        三条分支——判据是「入口有没有跑」，不是「coro 是不是同一个对象」。
        """
        ok, holder = _guarded("读取受理标记", getattr, task,
                              _TEARDOWN_ACCEPTANCE_ATTR, None)
        if not ok:
            holder = None
        if isinstance(holder, dict) and not holder.get("entered"):
            self._on_teardown_never_entered(task, holder)
            return
        if task.cancelled():
            self._teardown_error_kind = "cancelled"
            self._teardown_task_error = None
            return
        exc = task.exception()
        if exc is not None:
            self._teardown_task_error = exc
            self._teardown_error_kind = type(exc).__name__
            logger.debug("事件总线关停：teardown 任务异常已取用: %r", exc)
            return
        self._teardown_task_error = None
        self._teardown_error_kind = None

    def _on_teardown_never_entered(self, task: "asyncio.Task",
                                   holder: Dict[str, Any]) -> None:
        """收养到的 Task 结束而 ``_teardown_entry`` 从未进入：具名结算（#109 r11/A1、A3）。

        覆盖形状：①工厂交回的 Task 不跑我们的 coroutine（包着别的、或我们的 coroutine
        被别处驱动）；②返回前已被 cancel；③同步路径上「返回时已 done/cancelled」。
        结算用的是**本受理那一轮**的 future（``holder['future']``），因此上一轮的晚到回调
        不会误伤重试那一轮；``_teardown_runs`` 不自增——一次都没跑过就必须如实是 0。
        """
        exception = None
        if not task.cancelled():
            exception = task.exception()
        future = holder.get("future")
        if future is not None and not future.done():
            self._settle_future(future, RuntimeError(_TEARDOWN_NEVER_ENTERED_MESSAGE))
            self._teardown_error_kind = TEARDOWN_NEVER_ENTERED_KIND
            self._teardown_task_error = exception
            logger.warning(
                "事件总线关停：收养的 teardown 任务结束时入口从未执行 ⇒ 具名失败 "
                "%s（task=%r）", TEARDOWN_NEVER_ENTERED_KIND, task)
        elif exception is not None:
            # 关闭已由别处结算（例如同一 coroutine 被别的驱动者跑完）：不覆盖真实结论，
            # 只取用本 Task 的异常，防 never retrieved。
            self._teardown_task_error = exception
            self._teardown_error_kind = type(exception).__name__
            logger.debug("事件总线关停：入口未执行的任务异常已取用: %r", exception)
        # 我们的入口 coroutine 从未被 await：若它**又从未启动过**，关掉它以免 GC 时打印
        # "coroutine ... was never awaited"；已启动/挂起说明别处正在驱动它，别碰。
        self._close_entry_coro_if_unstarted(holder.get("coro"))

    async def _teardown(self) -> None:
        """关停序列本体（#109 r7）：在 owner context 里跑，结束时 resolve close future。

        顺序（P0 是**顺序**的后果，不是检测的后果）：
          1. ``_drain_pending_tasks``：有界等待在飞分发，**不取消**任何任务；
          2. ``_stop_listeners``：**最后**才取消监听任务（此刻已不再产生新分发）；
          3. ``_close_backends``：释放连接/存储。

        ``_close_future`` 在 finally 里结算（r8/G3）：**完整跑完**才 ``set_result``；
        被取消 / 步骤抛异常时 ``set_exception``（具名 ``teardown-incomplete``）并清空
        幂等凭据，等待者看到的不是静默成功（r7 的无条件成功会让「被 supervisor
        批量取消」伪装成关停成功，且幂等早返回让总线永不释放）。
        """
        self._teardown_runs += 1
        completed = False
        try:
            await self._teardown_steps()
            completed = True
        finally:
            # 无论正常结束、步骤抛异常还是 CancelledError，等待者都不悬置：
            # 完成 ⇒ 成功；未完成 ⇒ 具名错误 + 允许重试。
            if completed:
                self._settle_close_future(None)
            else:
                self._settle_close_future(
                    RuntimeError(_TEARDOWN_INCOMPLETE_MESSAGE))

    async def _teardown_steps(self) -> None:
        # 顺序即契约（r8/F1(c) 复核）：drain 必须在 stop_listeners **之前**，
        # eager task factory 下 spawn 处可能同步跑完全部三步，因此这里不许依赖
        # 「先让出一次事件循环」的任何假设。
        await self._drain_pending_tasks()
        await self._stop_listeners()
        await self._close_backends()

    def _settle_close_future(self, error: Optional[BaseException]) -> None:
        """结算**当前** close future（幂等）；``error`` 非空时同时清空重试凭据。"""
        self._settle_future(self._close_future, error)

    def _settle_future(self, future: "asyncio.Future",
                       error: Optional[BaseException]) -> None:
        """结算**指定**的 close future（#109 r11/A1：晚到的回调只许结算它自己那一轮）。

        与 ``_settle_close_future`` 的差别只在「谁的 future」：受理标记绑在任务上，晚到的
        回调带的是**上一轮**的 future，绝不能拿 ``self._close_future``（可能是重试那一轮）
        去顶包。清理幂等凭据只在 ``future`` 恰好是当前凭据时发生。
        """
        if future is None or future.done():
            return
        if error is None:
            future.set_result(None)
            return
        future.set_exception(error)
        # 异常必须被**标记为已取用**：`request_close()` 是 fire-and-forget 的公开
        # 入口，此时可能没有任何人在 await；不标记就会在 GC 时打印
        # "Future exception was never retrieved"。这**不影响**真正的 await 者——
        # 它照样从 shield 收到这个异常。
        future.add_done_callback(_mark_future_retrieved)
        # 清空幂等凭据：后续 close() 不得对「已失败的本次关停」早返回，而要真正重试。
        # 只在 future 就是当前凭据时清——晚到的上一轮回调不许把重试那一轮的凭据清掉。
        if self._close_future is future:
            self._close_future = None
            self._teardown_task = None

    async def _stop_listeners(self) -> None:
        """取消监听任务：**逐个** cancel，再统一等落地。

        「不取消在飞分发」的前提下，这里是唯一还会 cancel 的地方；监听任务是生产方，
        不 await 在飞分发任务，因此 ``Task.cancel`` 的 ``_fut_waiter`` 链无环。仍然
        逐个 cancel（绝不用 ``gather(...)`` 的 cancel）——CE-2 的 ``RecursionError``
        正是 ``_GatheringFuture.cancel`` 沿「close↔drain↔dispatch」环重入。
        钉住（r8）：``Test109R8VacuousProperties`` 监听 ``asyncio.gather`` 调用 +
        顺序记录（监听取消必须发生在 drain 之后）。
        """
        listeners = [t for t in self.listeners if not t.done()]
        self.listeners.clear()
        for listener in listeners:
            listener.cancel()
        if listeners:
            try:
                await asyncio.wait(
                    listeners, timeout=DISPATCH_DRAIN_TIMEOUT_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception as exc:             # noqa: BLE001 - 关停序列不卡在这
                logger.warning("事件总线关停：等待监听任务落地时异常: %s", exc)

    async def _close_backends(self) -> None:
        """释放 pubsub / redis / 三个存储；任一失败具名 WARNING 后继续。

        r8/G2：closer 的**解析**也必须在 ``_close_one`` 的 try 内。redis-py 4.x 的
        异步客户端没有 ``aclose``（5.0 起才有），``self.pubsub.aclose`` 这种属性访问
        本身就会抛 ``AttributeError``；解析在 try 外时该异常会逃出 ``_teardown``，
        而 r7 的 finally 仍把 future resolve 成成功 ⇒ **调用方看到关停成功、五个组件
        一个都没释放**。现在把对象交给 ``_close_one``，由它按能力探测
        （``aclose`` → ``close`` → ``disconnect``）。

        r9/Finding 2：探测还包含「签名接受时传 ``close_connection_pool=True``」——
        裸 ``close()`` / 裸 ``aclose()`` 在 4.5.5（``from_url`` ⇒
        ``auto_close_connection_pool=False``）与「外部注入 connection_pool」的 5.x+
        上都**不关池**（连接数 1 → 1）。详见 ``_pool_closing``。

        r9 声明：``pubsub`` / ``redis`` 槽位释放后置 ``None``，重试不会重复释放；
        三个存储槽位保持对象引用，**重试会再调用一次它们的 ``close()``**（真实实现
        有 ``if self.redis:`` 幂等闸，第二次是 no-op；若 straggler 在两次尝试之间
        把客户端复活，第二次才关得掉）。这条是有意的，见 docs 与
        ``test_retry_recloses_storage_components_deliberately``。
        """
        if self.pubsub is not None:
            await self._close_one("pubsub", self.pubsub)
            self.pubsub = None
        if self.redis is not None:
            await self._close_one("redis", self.redis)
            self.redis = None
        await self._close_one("event_storage", self.event_storage)
        await self._close_one("subscription_storage", self.subscription_storage)
        await self._close_one("processing_tracker", self.processing_tracker)

    async def _close_one(self, label: str, target) -> None:
        """解析并调用 ``target`` 的关闭方法；解析失败/调用失败都具名 WARNING 后继续。

        支持范围（r8/G2 起，r9/Finding 2 修正）：

          * ``aclose``（redis-py ≥5 的 asyncio 客户端 / fakeredis）——签名接受时传
            ``close_connection_pool=True``；
          * ``close``（redis-py 4.x 的 asyncio 客户端与自定义后端）——同样在签名
            接受时传 ``close_connection_pool=True``（4.5.5 的
            ``Redis.close(close_connection_pool=None)`` 不传就不关池）；
          * ``disconnect``（**兜底，且在 redis-py 4.x 上是死分支**：4.5.5 的
            ``Redis`` / ``PubSub`` 都没有该属性，只有别的后端可能命中）。

        三者都没有 ⇒ 具名 ``AttributeError`` 被这里接住并记 WARNING，**不**逃出关停序列。
        """
        try:
            await _release_one(target)
        except asyncio.CancelledError:
            raise
        except Exception as exc:                 # noqa: BLE001 - 关停请求已受理，不让它炸
            logger.warning("事件总线关停：释放 %s 失败: %s", label, exc)

    async def close(self):
        """关闭所有子组件（#109 r7：**总体的、非阻塞的关停请求**）。

        r7 契约（取代 r6 的「先判据、再按旧顺序关停」）：

          * ``close()`` 对**任意**调用者安全：受理时同步置 ``_closing``（停新分发），
            在**总线构造 context** 里调度 teardown，并用 ``asyncio.shield`` 等待 ⇒
            teardown 不是调用者的 context 后代，调用者被取消也不会打断关停。
          * teardown **不取消**在飞分发任务：有界等待后只具名 WARNING 并放手让它们
            跑完（旧的「超时 cancel」正是自取消 / ``Task.cancel`` 沿 ``_fut_waiter``
            环无界递归的来源）。
          * 监听任务**最后**才取消。端到端因此不再有 hang / ``RecursionError`` /
            「Task was destroyed but it is pending」/ handler 被 ``_closing`` 取消。
          * 幂等：重复调用（含 handler 内那次被提前拒绝后的重试）返回同一个 future。
          * 判据（``_self_close_refusal``）仍作为**提前、响亮的拒绝**保留（拦最常见的
            handler 内直接关停）；漏判不影响安全性，只让关停更晚。没有在飞分发时判据
            直接放行（修掉验收者报告的过度拒绝）。

        r8 补充：

          * **失败不伪装成功**（G3）：teardown 没完整跑完（被 supervisor / 批量
            ``all_tasks()`` 取消、或步骤抛异常）时，``close()`` 抛具名
            ``RuntimeError(error_kind=teardown-incomplete)``，并清空幂等凭据 ⇒ 下一次
            ``close()`` 真正重试关停，而不是早返回「成功」。
          * **无运行循环具名拒绝**（G4）：``request_close()`` 在循环外调用给出
            ``error_kind=no-running-loop``（不再裸
            ``RuntimeError: no running event loop``）。
          * **释放按能力探测**（G2）：``aclose`` → ``close`` → ``disconnect``，单个组件
            解析/调用失败只记具名 WARNING 并继续释放其余组件。

        r9 补充：

          * **spawn 失败整体回滚**（Finding 1）：``_spawn_teardown`` 抛异常（自定义
            task factory / 已关闭循环）时抛具名 ``error_kind=teardown-spawn-failed``，
            并把 ``_closing`` / ``_close_future`` / ``_teardown_task`` 恢复到受理前
            ⇒ 之后重试**不会挂起**（r8 在这里会留下永不 resolve 的 future）。
          * **释放真的连池一起关**（Finding 2）：签名接受 ``close_connection_pool``
            时显式传 ``True``；4.5.5 与「外部注入 connection_pool」的 5.x+ 上，
            裸 ``close()`` / 裸 ``aclose()`` 都不动池。
          * **teardown Task 的异常被取用**（Finding 3）：done 回调取用并具名留档
            （``_teardown_task_error`` / ``_teardown_error_kind``），不再出现
            "Task exception was never retrieved"。
          * **重试的重复释放口径**：三个存储槽位在重试时会被**再次调用** ``close()``
            （幂等闸下是 no-op；revived 客户端才关得掉）；``pubsub``/``redis`` 置
            ``None`` 后不再重复。见 ``_close_backends``。

        r10 补充：

          * **回滚只发生在「真的没有 teardown 在跑」时**：工厂先建 Task 再抛（eager 或
            非 eager）时**收养**那个任务、保持关停已受理；不再出现「组件已关掉而
            ``_closing`` 被回滚成 False」导致的**双关**，也不再留下无人取用的孤儿 Task
            （r9 反例）。工厂返回非 Task（已完成的 ``Future`` / 无
            ``add_done_callback`` 的 awaitable）时绕开工厂接管 coroutine ⇒ close future
            不会永远悬着、也不会打 "coroutine was never awaited"。
          * **两条具名拒绝**：幂等分支里「已在**另一个**事件循环受理」现在也带
            ``error_kind=cross-loop-close``（与在飞跨循环分支一致）。
          * **记录口径一致**：``_teardown_error_kind`` / ``_teardown_task_error`` 成对
            改写（取消 ⇒ ``('cancelled', None)``，不再残留上一轮异常）。

        r11 补充（详见 ``request_close`` / ``_spawn_teardown`` / ``_on_teardown_done``）：

          * **「收养了但入口没跑」不再挂起**（A1/A3）：入口标记未置位就具名
            ``error_kind=teardown-never-entered``，不再留下永不 resolve 的 future。
          * **不是我们的 Task 一律拒收**（A2/B）：跨循环的 Task、覆写
            ``add_done_callback``/``done``/``cancelled``/``exception`` 的敌对 Task 子类
            走具名 ``teardown-spawn-failed`` + 整体回滚（reason 在 ``__cause__``）。
          * **out of contract 残差清单**（R-1/R-2/R-3）在 ``request_close`` 的 docstring
            与 ``docs-site/docs/distributed/event-system.md``；生产路径不使用
            ``set_task_factory``。

        ``close()`` 返回时：``_closing is True``、teardown 恰好跑过一次、监听到期被取消；
        已登记任务里被点名者允许继续跑完（登记由完成回调回收）。
        """
        running_loop = asyncio.get_running_loop()
        if self._close_future is not None:
            # 幂等分支：关停已受理，重复调用只等同一个 future（不再走判据/副作用）。
            if self._close_future.get_loop() is not running_loop:
                raise RuntimeError(
                    "RedisEventBus.close() 已在另一个事件循环里受理关停请求："
                    "重复请求必须来自同一个事件循环（teardown 归属该循环）。"
                    "error_kind=cross-loop-close"
                )
            await asyncio.shield(self._close_future)
            return
        current = asyncio.current_task()
        foreign = [
            task for task in self._pending_tasks
            if not task.done() and task.get_loop() is not running_loop
        ]
        if foreign:
            # #109/F2：线程 / executor 不继承 contextvars（marker 不跨线程），但
            # 「另一个事件循环里关停仍有在飞分发的总线」同样是未声明路径：该
            # 循环里的 drain 看不到别的循环的任务、跨循环 aclose 还会抛
            # "Event loop is closed"。有在飞分发时具名拒绝，且在**任何关停副作用
            # 之前**（``_closing`` 保持 False）。无在飞分发时不拦（无分发可丢）。
            raise RuntimeError(
                "RedisEventBus.close() 不能在另一个事件循环（线程/executor）内调用："
                f"本实例仍有 {len(foreign)} 个在飞分发任务属于另一个事件循环。"
                "请在该总线所属的事件循环里关停；"
                "如需跨线程请求关停，请用 loop.call_soon_threadsafe 投递。"
                "error_kind=cross-loop-close"
            )
        refusal = self._self_close_refusal(current)
        if refusal is not None:
            raise refusal
        # 请求受理后 teardown 独立跑（owner context + shield）：
        # 调用者被取消/超时不会打断关停，也不会把 teardown 变成调用者的后代。
        await asyncio.shield(self.request_close())

    async def get_event(self, event_id: str) -> Event:
        """获取事件"""
        event = await self.event_storage.get_event(event_id)
        if not event:
            raise EventNotFoundError(f"事件 {event_id} 不存在")
        return event 