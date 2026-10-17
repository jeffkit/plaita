#!/usr/bin/env python3
"""plaita#151 **表外 ``exc_info`` 面现状证实探针**（W19-A 交付，只读，不改产品代码）。

W12-A 把 #168 判定表内的 C 类站点接上掩码设施时，刻意留下**表外 ``exc_info`` 面
62 站点 / 24 文件**（``exc_info=True`` 不是插值、不进判定表，但 traceback 同样可能
携带作者数据—— ``#168`` §1）。本探针就是那一族的**红先证 / 修后 CLEAN** 载体：

* **静态腿**：``_BASELINE`` 钉住 W12-A 基线（62 条，含重复消息的出现序号）。对
  ``--root`` 指向的树逐条定位调用点，判定 ``LEAK`` / ``CLEAN``：
  - 带 ``exc_info``（非字面 False）⇒ ``LEAK``：``Formatter`` 会在 logger 调用**之后**
    用**原始异常**重渲染 traceback（``#161`` 的核心发现），构造面清洗盖不住；
  - 把裸异常名（``e`` / ``exc`` / ``error`` …）交给 logger 且未经掩码器 ⇒ ``LEAK``；
  - 异常文本经 ``_scrub_exception_text`` / ``_scrub_url_text`` 渲染、且无 ``exc_info``
    ⇒ ``CLEAN``（诊断价值保留：消息里带着**清洗后**的文本 + 栈）。
* **动态腿**：用**真实代码路径**驱动一族代表站点，注入带 needle 的异常，检查六个
  记录面（``msg`` / ``args`` / ``getMessage`` / ``__dict__`` / ``exc_text`` /
  ``Formatter`` 渲染）一个字节都不许带 needle。

用法（两次跑，同一脚本，双向可用）::

    # 修复前（W12-A 基线树）：静态 62/62 LEAK + 动态代表用例 LEAK ⇒ 退出码 1
    PYTHONPATH=/tmp/w12-a /Users/jeff.jie/.venvs/plaita/bin/python \
        scripts/issue151_exc_info_leak_probe.py --root /tmp/w12-a

    # 修复后（W19-A 树）：静态 0 LEAK + 动态 0 LEAK ⇒ 退出码 0
    PYTHONPATH=/tmp/w19-a ... --root /tmp/w19-a

判据实现**复用**守卫里的 ``_records_with_exc_info`` / ``_MASKER_NAMES``（不是第二份
检测器）；探针只负责"按站点打印 + 动态取证"。
"""
from __future__ import annotations

import argparse
import ast
import logging
import os
import pathlib
import sys
import types

# --- 路径：先脚本所在仓（拿 tests 里的守卫判据），再 --root（拿被测 plaita） ---
_SCRIPT_REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_SCRIPT_REPO))

#: needle：真实 URL 形状（`#159`/`#161` 实测会被 `_scrub_url_text` 掩掉的那一种）。
SECRET = "Ks9Qm4Zt"
NEEDLE_URL = f"http://127.0.0.1:1/robot/send?access_token={SECRET}"
NEEDLES = sorted(
    {SECRET}
    | {SECRET[:n] for n in range(4, len(SECRET) + 1)}
    | {SECRET[-n:] for n in range(4, len(SECRET) + 1)},
    key=lambda s: (-len(s), s),
)

#: W12-A 基线：62 站点 = ``(文件, 消息字面量前缀, 同 (文件,消息) 内的出现序号)``。
_BASELINE = [
    ("plaita/__init__.py", "PLAITA_LOOP=uvloop but uvloop is not installed", 0),
    ("plaita/core/async_utils.py", "async gen aclose failed during sync drain", 0),
    ("plaita/core/async_utils.py", "async gen aclose failed in worker thread", 0),
    ("plaita/core/context.py", "plaita.event 不可导入，默认 EventBus 未启用", 0),
    ("plaita/core/context.py", "Unable to get default event bus", 0),
    ("plaita/core/flow.py", "node model_dump failed during $ENV ref scan", 0),
    ("plaita/core/http_session.py", "aiohttp unavailable; flow http session scope is a no-op", 0),
    ("plaita/core/http_session.py", "flow http session close failed", 0),
    ("plaita/core/parallel_executor.py", "background pool %r shutdown raised during atexit", 0),
    ("plaita/core/runner.py", "handled node error traceback", 0),
    ("plaita/dsl/ir_validate.py", "outputType precheck skipped (invalid Property or match failure)", 0),
    ("plaita/event/redis.py", "skipping unparseable event payload in mget batch", 0),
    ("plaita/event/redis.py", "skipping unparseable event payload in paged mget", 0),
    ("plaita/event/redis.py", "wait_for_event 取消订阅失败", 0),
    ("plaita/event/redis.py", "wait_for_event 关闭 pubsub 失败", 0),
    ("plaita/node/code.py", "docker daemon probe failed", 0),
    ("plaita/node/code.py", "docker rm -f %s failed", 0),
    ("plaita/node/concurrent.py", "background future.exception() unavailable", 0),
    ("plaita/obs.py", "langfuse observer event failed", 0),
    ("plaita/obs.py", "读取 $EXECUTION_ID 失败", 0),
    ("plaita/obs.py", "langfuse update(level=ERROR) failed", 0),
    ("plaita/obs.py", "langfuse flush failed", 0),
    ("plaita/obs.py", "langfuse root update failed", 0),
    ("plaita/obs.py", "langfuse root end failed", 0),
    ("plaita/obs.py", "写入 trace 级 OTel 属性失败", 0),
    ("plaita/obs.py", "langfuse span start failed", 0),
    ("plaita/obs.py", "langfuse 子 observation 失败", 0),
    ("plaita/obs.py", "langfuse span end failed", 0),
    ("plaita/obs.py", "langfuse root end failed", 1),
    ("plaita/server/event_filter.py", "释放去重键失败: %s", 0),
    ("plaita/server/event_filter.py", "回收订阅 %s 失败", 0),
    ("plaita/server/event_filter.py", "事件 %s 入队失败，已释放去重键以便重放（订阅 %s，队列 %s）", 0),
    ("plaita/server/event_filter.py", "回收超时订阅 %s 失败", 0),
    ("plaita/server/event_filter.py", "订阅 %s 超时 resume 入队失败，已释放去重键以便重放（执行 %s，队列 %s）", 0),
    ("plaita/server/flow_worker.py", "读取引擎版本失败，按 unknown 记录", 0),
    ("plaita/server/flow_worker.py", "取消监听周期异常", 0),
    ("plaita/server/flow_worker.py", "取消监听调 execution.cancel() 失败: %s", 0),
    ("plaita/server/flow_worker.py", "flow_storage.diagnose_missing_flow(%s) failed", 0),
    ("plaita/server/flow_worker.py", "观测回调终态收尾失败: %r", 0),
    ("plaita/server/flow_worker.py", "观测回调 bind_execution 失败: %r", 0),
    ("plaita/server/flow_worker.py", "租约看门狗周期异常", 0),
    ("plaita/server/flow_worker.py", "在跑节点进度发布失败（忽略，节点边界仍会落盘）: %s", 0),
    ("plaita/server/flow_worker.py", "看门狗调 execution.cancel() 失败: %s", 0),
    ("plaita/server/metrics.py", "渲染 /metrics 失败", 0),
    ("plaita/server/node_timings.py", "节点标签构造失败，退回类型名", 0),
    ("plaita/server/node_timings.py", "节点耗时采集 on_node_start 失败: %s", 0),
    ("plaita/server/node_timings.py", "节点耗时采集 on_node_end 失败: %s", 0),
    ("plaita/server/services/approval_service.py", "event loop close failed during task cleanup", 0),
    ("plaita/server/services/approval_service.py", "审批锁释放失败（TTL 兜底）: %s", 0),
    ("plaita/server/services/base_service.py", "event loop close failed during task cleanup", 0),
    ("plaita/server/services/delay_service.py", "event loop close failed during task cleanup", 0),
    ("plaita/server/services/delay_service.py", "delay error-event trigger failed", 0),
    ("plaita/server/services/kafka_queue_service.py", "kafka error-event trigger failed", 0),
    ("plaita/server/services/kafka_queue_service.py", "flow completion callback raised", 0),
    ("plaita/server/services/redis_queue_service.py", "redis client close failed during stop (execution_id=%s)", 0),
    ("plaita/server/services/redis_queue_service.py", "pubsub unsubscribe/close failed", 0),
    ("plaita/server/services/schedule_service.py", "调度 %s 触发历史写入失败", 0),
    ("plaita/server/services/schedule_service.py", "调度 %s 状态回写失败", 0),
    ("plaita/server/tenant_status.py", "租户 %s 状态同步到 Redis 失败", 0),
    ("plaita/server/tenant_status.py", "租户 %s 状态清理失败", 0),
    ("plaita/server/tenant_status.py", "读取租户 %s 状态失败，按 active 处理", 0),
    ("plaita/storage/sqlalchemy.py", "model version sort failed, falling back to first", 0),
]


# --------------------------------------------------------------------------- #
# 静态腿
# --------------------------------------------------------------------------- #
def _is_log_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr not in {"debug", "info", "warning", "error", "critical", "exception", "log"}:
        return False
    inner = node.func.value
    base = inner.func if isinstance(inner, ast.Call) else inner
    try:
        dotted = ast.unparse(base)
    except Exception:  # noqa: BLE001
        return False
    return dotted.rsplit(".", 1)[-1].lower().endswith(("log", "logger"))


def _locate(source: str, message: str, occurrence: int) -> ast.Call | None:
    """按消息字面量前缀 + 出现序号定位调用点（行号会漂移，故不按行号钉）。"""
    tree = ast.parse(source)
    matches: list[ast.Call] = []
    for node in ast.walk(tree):
        if not _is_log_call(node) or not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str) and first.value.startswith(message):
            matches.append(node)
    matches.sort(key=lambda n: n.lineno)
    if occurrence >= len(matches):
        return None
    return matches[occurrence]


def _masked(node: ast.AST, masked: bool = False) -> tuple[bool, list[str]]:
    """``(异常面是否被掩码器吃下, 未掩码的疑似异常名)``。"""
    from tests.unit.test_issue161_third_party_exception_text_sinks import _MASKER_NAMES

    names: list[str] = []
    saw_masker = False

    def walk(n: ast.AST, masked_now: bool) -> None:
        nonlocal saw_masker
        if isinstance(n, ast.Call):
            called = ast.unparse(n.func).rsplit(".", 1)[-1]
            inner = masked_now or called in _MASKER_NAMES
            if called in _MASKER_NAMES:
                saw_masker = True
            walk(n.func, masked_now)
            for arg in n.args:
                walk(arg, inner)
            for kw in n.keywords:
                walk(kw.value, inner)
            return
        if isinstance(n, ast.Name):
            if not masked_now and n.id in {"e", "exc", "err", "error", "exception"}:
                names.append(n.id)
            return
        for child in ast.iter_child_nodes(n):
            walk(child, masked_now)

    walk(node, masked)
    return saw_masker, names


def _classify(call: ast.Call) -> tuple[str, str]:
    from tests.unit.test_issue151_log_hygiene import _records_with_exc_info

    if _records_with_exc_info(ast.unparse(call) + "\n"):
        return "LEAK", "exc_info ⇒ Formatter 用原始异常二次渲染 traceback"
    checked: list[ast.AST] = list(call.args[1:]) + [kw.value for kw in call.keywords]
    saw_masker = False
    names: list[str] = []
    for node in checked:
        got_masker, got_names = _masked(node)
        saw_masker = saw_masker or got_masker
        names.extend(got_names)
    if names:
        return "LEAK", "裸异常名直达 logger: " + ", ".join(sorted(set(names)))
    if saw_masker:
        return "CLEAN", "构造面 _scrub_exception_text 渲染（诊断保留 + 凭据掩掉）"
    return "CLEAN", "该调用点无异常面"


def static_leg(root: pathlib.Path) -> int:
    print("=" * 96)
    print(f"静态腿：W12-A 基线 62 站点逐条判定（root={root}）")
    print("=" * 96)
    leaks = 0
    for rel, message, occurrence in _BASELINE:
        path = root / rel
        if not path.is_file():
            print(f"[FAIL ] {rel}: 文件不存在")
            leaks += 1
            continue
        call = _locate(path.read_text(encoding="utf-8"), message, occurrence)
        if call is None:
            print(f"[FAIL ] {rel} :: {message!r}（找不到该站点）")
            leaks += 1
            continue
        verdict, why = _classify(call)
        if verdict == "LEAK":
            leaks += 1
        print(f"[{verdict:5s}] {rel}:{call.lineno} :: {message}  —— {why}")
    print(f"\n静态腿合计：{leaks} LEAK / {len(_BASELINE)} 站点")
    return leaks


# --------------------------------------------------------------------------- #
# 动态腿：真实代码路径
# --------------------------------------------------------------------------- #
class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.name.startswith("plaita"):
            self.records.append(record)

    def surfaces(self) -> str:
        fmt = logging.Formatter("%(levelname)s %(name)s %(message)s")
        chunks: list[str] = []
        for record in self.records:
            rendered = fmt.format(record)  # 先渲染 ⇒ 触发/填好 exc_text
            chunks.append(str(record.msg))
            chunks.append(repr(record.args))
            chunks.append(record.getMessage())
            for key, value in sorted(vars(record).items()):
                chunks.append(f"{key}={value!r}")
            chunks.append(rendered)
        return "\n".join(chunks)


def _needle_exc() -> Exception:
    return ValueError(f"remote rejected: {NEEDLE_URL}")


def _raise_needle(*_args, **_kwargs):
    raise _needle_exc()


class _BoomId:
    @property
    def id(self):  # noqa: N802 - 模拟 node.id 取用即炸
        raise _needle_exc()


class _BoomPool:
    def shutdown(self, **_kwargs):
        raise _needle_exc()


class _BoomFuture:
    def exception(self):
        raise _needle_exc()


class _BoomDumpNode:
    def model_dump(self):
        raise _needle_exc()


class _BoomFinder:
    """``sys.meta_path`` 钩子：让指定模块的 import 抛**带 needle 的** ImportError。

    ``sys.modules["x"] = None`` 只能拿到裸 ImportError（消息里没有 needle），
    无法证明"这条日志把异常文本原样写进了记录面"；find_spec 抛出的异常会原样
    冒泡给 ``import`` 语句的调用方。
    """

    def __init__(self, *names: str) -> None:
        self.names = set(names)

    def find_spec(self, name, path=None, target=None):  # noqa: ARG002
        if name in self.names:
            raise ImportError(f"blocked import {name}: {NEEDLE_URL}")
        return None


class _BoomRedis:
    def hset(self, *_a, **_k):
        raise _needle_exc()

    def hget(self, *_a, **_k):
        raise _needle_exc()


def _case_runner() -> None:
    from plaita.core.errors import ErrorHandler, ErrorStrategy
    from plaita.core.runner import _handle_node_error

    node = types.SimpleNamespace(id="n1", name="n1")
    # 真实调用点（core/runner.py:319 ``except … as e`` 体内）：``exc_info=True``
    # 取的是**环境异常**，故驱动必须真的站在 except 体内。
    try:
        raise _needle_exc()
    except Exception as exc:  # noqa: BLE001
        _handle_node_error(None, node, ErrorHandler(strategy=ErrorStrategy.CONTINUE), exc)


def _case_parallel_executor() -> None:
    from plaita.core import parallel_executor as pe

    saved = dict(pe._POOL_CACHE)
    pe._POOL_CACHE.clear()
    pe._POOL_CACHE["probe"] = (os.getpid(), _BoomPool())
    try:
        pe._shutdown_background_pools()
    finally:
        pe._POOL_CACHE.clear()
        pe._POOL_CACHE.update(saved)


def _case_flow_env_scan() -> None:
    from plaita.core.flow import Flow

    shim = types.SimpleNamespace(expose_env=None, nodes=[_BoomDumpNode()])
    Flow._warn_uncovered_env_refs(shim)


def _case_http_session_import() -> None:
    import asyncio

    from plaita.core import http_session as hs

    finder = _BoomFinder("aiohttp")
    sys.meta_path.insert(0, finder)
    sys.modules.pop("aiohttp", None)
    try:
        asyncio.run(hs.open_flow_session())
    finally:
        sys.meta_path.remove(finder)


def _case_tenant_status() -> None:
    from plaita.server.tenant_status import publish_tenant_status

    publish_tenant_status(_BoomRedis(), "t1", "active")


def _case_node_label() -> None:
    from plaita.server.node_timings import _node_label

    _node_label(_BoomId())


def _case_context_event_bus() -> None:
    import plaita.event as ev
    from plaita.core.context import _resolve_default_event_bus

    saved = ev.get_default_event_bus
    ev.get_default_event_bus = _raise_needle
    try:
        _resolve_default_event_bus()
    finally:
        ev.get_default_event_bus = saved


def _case_concurrent_future() -> None:
    from plaita.node.concurrent import Parallel

    errors: list = []
    state = {"futures": [], "done": 0}
    callback = Parallel._make_background_done_callback(
        types.SimpleNamespace(name="branch"), errors, state
    )
    callback(_BoomFuture())


def _case_uvloop_missing() -> None:
    import plaita

    finder = _BoomFinder("uvloop")
    saved_mod = sys.modules.get("uvloop", _MISSING)
    saved_env = os.environ.get("PLAITA_LOOP")
    sys.meta_path.insert(0, finder)
    sys.modules.pop("uvloop", None)
    os.environ["PLAITA_LOOP"] = "uvloop"
    try:
        plaita._maybe_install_uvloop()
    finally:
        sys.meta_path.remove(finder)
        if saved_mod is _MISSING:
            sys.modules.pop("uvloop", None)
        else:
            sys.modules["uvloop"] = saved_mod
        if saved_env is None:
            os.environ.pop("PLAITA_LOOP", None)
        else:
            os.environ["PLAITA_LOOP"] = saved_env


def _case_ir_validate() -> None:
    import plaita.io as io_mod
    from plaita.dsl.ir_validate import check_output_type_literal

    saved = io_mod.match
    io_mod.match = _raise_needle
    try:
        check_output_type_literal({"outputType": {"type": "string"}, "output": "x"}, "$", None)
    finally:
        io_mod.match = saved


def _case_metrics_render() -> None:
    import urllib.error
    import urllib.request

    from plaita.server.metrics import MetricsHttpServer

    server = MetricsHttpServer(_raise_needle, host="127.0.0.1", port=0)
    port = server.start()
    try:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
                resp.read()
        except urllib.error.HTTPError:
            pass  # 500 是预期（渲染失败）
    finally:
        server.stop()


def _case_async_utils() -> None:
    from plaita.core.async_utils import async_gen_to_sync

    async def gen_broken_close():
        try:
            yield 1
        finally:
            raise _needle_exc()

    gen = async_gen_to_sync(gen_broken_close())
    next(gen)
    gen.close()


_MISSING = object()

_DYNAMIC_CASES = [
    ("节点执行路径（函数参数 e）", "plaita/core/runner.py:154", _case_runner),
    ("atexit 池关停（裸 except）", "plaita/core/parallel_executor.py:134", _case_parallel_executor),
    ("$ENV 扫描 model_dump 失败", "plaita/core/flow.py:340", _case_flow_env_scan),
    ("aiohttp 缺失 no-op", "plaita/core/http_session.py:80", _case_http_session_import),
    ("租户状态同步失败", "plaita/server/tenant_status.py:32", _case_tenant_status),
    ("节点标签构造失败", "plaita/server/node_timings.py:48", _case_node_label),
    ("默认 EventBus 不可用", "plaita/core/context.py:134", _case_context_event_bus),
    ("future.exception() 不可用（已具名 e）", "plaita/node/concurrent.py:323", _case_concurrent_future),
    ("uvloop 未安装（包入口）", "plaita/__init__.py:219", _case_uvloop_missing),
    ("outputType 预检跳过", "plaita/dsl/ir_validate.py:463", _case_ir_validate),
    ("/metrics 渲染失败（HTTP 端到端）", "plaita/server/metrics.py:220", _case_metrics_render),
    ("async gen aclose 失败", "plaita/core/async_utils.py:172", _case_async_utils),
]


def dynamic_leg() -> int:
    print("=" * 96)
    print("动态腿：真实代码路径注入 needle 异常，检查六个记录面")
    print("=" * 96)
    import plaita  # noqa: F401  确认被测树可导入

    leaks = 0
    for label, site, case in _DYNAMIC_CASES:
        capture = _Capture()
        root = logging.getLogger()
        prev_level = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(capture)
        error: str | None = None
        try:
            case()
        except Exception as exc:  # noqa: BLE001 — 探针自身不许把 driver 异常当失败
            error = f"{type(exc).__name__}: {exc}"
        finally:
            root.removeHandler(capture)
            root.setLevel(prev_level)
        text = capture.surfaces()
        hits = [n for n in NEEDLES if n in text]
        leaked = bool(hits)
        if leaked:
            leaks += 1
        tag = "LEAK  " if leaked else "CLEAN "
        print(f"[{tag}] {label:34s} {site}  (records={len(capture.records)})")
        if error:
            print(f"          driver 异常（已忽略，仅记录）: {error[:120]}")
        if leaked:
            print(f"         命中 needle: {hits[:4]}（共 {len(hits)} 个）")
            for line in text.splitlines():
                if any(n in line for n in hits):
                    print("         记录面: " + line.strip()[:170])
                    break
    print(f"\n动态腿合计：{leaks} LEAK / {len(_DYNAMIC_CASES)} 用例")
    return leaks


def main() -> int:
    parser = argparse.ArgumentParser(description="plaita#151 表外 exc_info 面泄漏探针")
    parser.add_argument("--root", default=str(_SCRIPT_REPO), help="被测源码树（默认脚本所在仓）")
    parser.add_argument("--static-only", action="store_true", help="只跑静态腿")
    args = parser.parse_args()

    root = pathlib.Path(args.root).resolve()
    sys.path.insert(0, str(root))
    leaks = static_leg(root)
    if not args.static_only:
        leaks += dynamic_leg()
    print()
    print(f"探针结果：{leaks} 处 LEAK（静态 62 站点 + 动态 {len(_DYNAMIC_CASES)} 用例）")
    return 1 if leaks else 0


if __name__ == "__main__":
    sys.exit(main())
