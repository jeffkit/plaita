"""跨执行体（线程 / 子进程）的执行环境态传播。

节点执行会离开驱动线程：同步节点跑在共享节点池 worker 或超时裸线程上，
``Parallel`` / ``Map(concurrent)`` 的分支跑在线程池 worker 上，进程模式分支还
跨子进程。ContextVar 不自动跟随这些边界——线程 worker 自带空 context，子进程
是另一个解释器。

这里只传播**显式注册**的环境态（当前是租户与 console 本地档的凭据文件覆盖），
且进入方在**全新空 context** 里执行：调用方的其它 contextvar 一律不跟随，
回到各自的缺省值。

为什么不用 ``contextvars.copy_context()`` 整份带过去（plaita#24 首版的做法）：
那会把**与执行体绑定**的 contextvar 一并带过去，典型是 flow 级共享
``aiohttp.ClientSession``（``plaita.core.http_session``）——它绑定父 flow 的
event loop，分支线程里复用会撞 aiohttp 的跨 loop 硬约束
（``Timeout context manager should be used inside a task``），且分支收尾的
``close_flow_session()`` 关掉的是父 flow 的 session（其后 HTTP 节点静默退回
一次性会话）。同一 loop 内的 fan-out（``asyncio.gather``、协程模式分支）不经
本模块，天然继承 session。

注册项的值必须可 pickle：进程池入口要把快照送过进程边界。
"""
from __future__ import annotations

import contextvars
import functools
from typing import Any, Callable, Dict

_REGISTRY: Dict[str, contextvars.ContextVar] = {}


def register_contextvar(var: contextvars.ContextVar) -> None:
    """注册一个跨执行体传播的 ContextVar（以 ``var.name`` 为键，重复注册覆盖）。"""
    _REGISTRY[var.name] = var


def snapshot_env() -> Dict[str, Any]:
    """取当前环境态快照。

    必须在**提交侧**（持有环境态的那个线程）调用；返回值要能 pickle。
    """
    return {name: var.get() for name, var in _REGISTRY.items()}


def run_with_env(snapshot: Dict[str, Any], fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """在只含 *snapshot* 的全新 context 里执行 ``fn(*args, **kwargs)``。

    跨线程 / 跨进程的入口调用本函数（进程池要能按引用 pickle 它，故为模块级
    函数）。快照里未注册的名字、注册表里快照未覆盖的项都静默跳过——两端各自
    导入的模块集不同时不会因此炸掉。
    """
    ctx = contextvars.Context()
    for name, value in snapshot.items():
        var = _REGISTRY.get(name)
        if var is not None:
            ctx.run(var.set, value)
    return ctx.run(fn, *args, **kwargs)


def bind_env(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Callable[[], Any]:
    """把 ``fn(*args, **kwargs)`` 绑成携带当前环境态快照的无参可调用对象。

    用于只收"无参可调用对象"的入口（``loop.run_in_executor``、
    ``threading.Thread(target=...)``）。快照在调用本函数的线程取，故须在提交侧
    调用。
    """
    return functools.partial(run_with_env, snapshot_env(), fn, *args, **kwargs)
