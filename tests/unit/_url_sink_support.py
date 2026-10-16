"""``#158`` / ``#159`` 测试的共用支撑：**凭据 URL 的日志守卫** + 少量桩件。

**不是测试模块**（``_`` 前缀，pytest 默认 ``python_files=test_*.py`` 不收集它）。

守卫的来由：``#158`` r1 的守卫只认 ``redis_url`` 这个名字，于是
``alerts.py`` 的 ``self.url`` 与 ``flow_worker`` 的 ``self._alert_webhook``
（``#159`` 的两个 sink）在它眼里完全隐身。本版（``#159`` r2）把判据从「名字里有
``redis_url``」扩成「**任何形如 URL 的标识符**」，并补上**第二腿**（异常消息）：

* 名字（点分）里含 ``redis_url`` / ``webhook``，或末段以 ``url`` / ``uri``
  **结尾**（``self.url``、``request.url``、``full_url``、``callback_url``）；或
* 下标键 / ``getattr(x, "url")`` / ``x.get("url")`` 的字符串键是 URL 名；或
* 本文件内的**赋值别名**含 URL（``u = redis_url`` 之后 ``u`` 也是 URL）。

第二腿 :func:`exception_message_url_sites`：``raise`` 消息（与
``exc.add_note(...)``）里未掩码的 URL 名一律判红——URL 藏进**异常对象**时，
「``logger.*`` 实参名」判据结构性失明（``#159`` r2 的 finding）。

r1/r2 定验记录并修掉的**假阴**：

1. 白名单里的 ``_passwords`` 返回的是**原始口令**（``_passwords(url)`` 就是口令
   元组），把「返回原文」当成「已掩码」⇒ 已从 :data:`_MASKER_NAMES` 移除；
2. ``_collect_url_refs`` 从不访问 ``Call.func`` ⇒ ``redis_url.strip()`` /
   ``self.url.format(...)`` 这类现实写法漏判 ⇒ 现在递归 ``Call.func``；
3. ``_is_log_call`` 对 ``logging.getLogger(__name__).info(...)`` **恒漏**（func 是
   「Call 的属性」时 ``rsplit('.',1)[0]`` 得到非空的方法名 ``"info"``，回退分支是
   死代码）⇒ r2 改为先看 ``node.func.value`` 是不是 ``Call``；
4. 末段「恰是 url/uri」漏掉 ``full_url`` / ``base_url`` / ``callback_url`` ⇒
   放宽为「以 url/uri 结尾」。

**判据能力边界**（定验实测 MISS，刻意不判，靠第二腿/人工巡视）：
``adapter.info(...)``（LoggerAdapter 实例名任意）、``stream.write(str(url))`` /
``print(url)``（非 logger 输出面）、跨函数/参数流入的别名（只做本文件内赋值传播）。
"""
from __future__ import annotations

import ast
import logging
import pathlib
import re
from typing import Any, List, Set, Tuple

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
PLAITA_DIR = REPO_ROOT / "plaita"

#: ``LogRecord`` 的标准属性（外加 Formatter 会塞的 ``message``/``asctime``）——
#: 断言 ``record.__dict__`` 时排除它们，只保留调用方用 ``extra=`` 注入的自定义键。
_LOGRECORD_STANDARD_ATTRS = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | {"message", "asctime"}

#: ``logger.*`` 的方法名（``logging`` 全家族 + ``log``）。
_LOG_METHODS = frozenset(
    {"debug", "info", "warning", "warn", "error", "exception", "critical", "log"}
)

#: 允许「吃下」URL 的**安全渲染器**：只有经它们渲染过的标识符才允许进日志。
#: ⚠️ ``_passwords`` **不在**名单里——它返回**原始口令**，拿它当掩码器等于把
#: 「返回原文」判成「已掩码」（``#158`` r1 定验的假阴之一）。
_MASKER_NAMES = frozenset(
    {
        "_mask_url",
        "_mask_webhook_url",
        "_mask_url_for_log",
        "_mask_webhook_url_for_log",
        "_mask_policy_url",
        "_scrub_url_text",
        "_safe_render",
        "_scrub",
        "_safe_reason",
        "_sanitized_cause",
    }
)

#: 「凭据落在 URL 里」的名字特征：Redis 连接串、webhook（含 ``_alert_webhook``）。
_URL_NAME_RE = re.compile(r"redis_url|webhook", re.I)
#: 点分名末段以 ``url``/``uri`` 结尾 ⇒ 该标识符本身就是个 URL。r2 把判据从
#: 「恰是 url/uri」放宽到「以 url/uri 结尾」：定验实测 ``full_url`` /
#: ``callback_url`` / ``base_url`` / ``cfg["full_url"]`` 在旧判据下全部隐身
#: （掩码器自己的名字先被 :data:`_MASKER_NAMES` 摘掉，不会因此误红）。
_URL_TAIL_RE = re.compile(r"(?:url|uri)$", re.I)
#: ``getattr(cfg, "url")`` / ``cfg.get("url")`` 里的字符串键——同一件事的另一种写法。
_GETATTR_NAMES = frozenset({"getattr"})
_GET_NAMES = frozenset({"get"})


def leak_needles(secret: str) -> List[str]:
    """密钥本体 + **每一个 ≥3 字符的前缀/后缀**（长→短）。

    只断言「完整口令不在日志里」是不够的：掩码器从中间切开时（``...float:***@ssword``）
    整串确实不在了，却留下 ≥3 字符的尾巴——那截尾巴在日志聚合里照样可被拼回。
    """
    needles = {secret}
    for size in range(3, len(secret) + 1):
        needles.add(secret[:size])
        needles.add(secret[-size:])
    return sorted(needles, key=lambda item: (-len(item), item))


def find_needles(text: str, needles: List[str]) -> List[str]:
    """``text`` 里命中的 needle（长→短）。"""
    return [needle for needle in needles if needle in text]


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return ""


def _is_log_call(node: ast.AST) -> bool:
    """``logger.*`` / ``logging.getLogger(__name__).*`` / ``self.log.*`` 的调用。

    r2 定验的真发现：旧判据对 ``logging.getLogger(__name__).info(...)`` **恒漏**——
    ``_dotted(node.func)`` 在 func 是「``Call`` 的属性」时只拿得到方法名 ``"info"``，
    ``rsplit(".", 1)[0]`` 于是得到非空的 ``"info"``，``if not base`` 的回退分支是
    **死代码**。修法：先看 ``node.func.value`` 是不是 ``Call``，是则取那个 Call 的
    ``func`` 链（``logging.getLogger``）判尾。
    """
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr not in _LOG_METHODS:
        return False
    inner = node.func.value
    base = _dotted(inner.func) if isinstance(inner, ast.Call) else _dotted(inner)
    if not base:
        return False
    return base.lower().endswith(("log", "logger"))


def _is_credential_url_name(dotted: str, extra_names: Set[str] = frozenset()) -> bool:
    if not dotted:
        return False
    tail = dotted.rsplit(".", 1)[-1]
    # 掩码器**自己的名字**不是 URL（``_mask_webhook_url_for_log`` 里就带
    # "webhook"）：否则修复后的每一个调用点都会被守卫判红。
    if tail in _MASKER_NAMES:
        return False
    if tail in extra_names:
        return True
    if _URL_NAME_RE.search(dotted):
        return True
    return bool(_URL_TAIL_RE.search(tail))


def _string_key_is_url_name(node: ast.AST, extra_names: Set[str]) -> bool:
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and _is_credential_url_name(node.value, extra_names)
    )


def _collect_url_refs(
    node: ast.AST,
    masked: bool,
    out: List[Tuple[int, str, bool]],
    extra_names: Set[str] = frozenset(),
) -> None:
    """收集日志参数里的凭据 URL 标识符，并记住它是否位于掩码器调用之内。

    ``extra_names``：本文件作用域里经赋值传播出来的 URL **别名**（``u = redis_url``
    之后 ``u`` 也是 URL）——名字判据看不见它，别名表负责补位。
    """
    if isinstance(node, ast.Call):
        called = _dotted(node.func).rsplit(".", 1)[-1]
        inner = masked or called in _MASKER_NAMES
        # r1 假阴 #2：``Call.func`` 必须一并走——否则 ``redis_url.strip()`` 的
        # ``redis_url``（藏在 func 链里）永远不被访问。注意 func 用**进入时**的
        # masked（``_mask_url(url).strip()`` 的内层仍应算已掩码）。
        _collect_url_refs(node.func, masked, out, extra_names)
        # r2 覆盖面：``getattr(cfg, "url")`` / ``cfg.get("url")`` —— 字符串键也是 URL 名。
        if called in _GETATTR_NAMES and len(node.args) >= 2 and _string_key_is_url_name(
            node.args[1], extra_names
        ):
            out.append((node.lineno, f'getattr(…, "{node.args[1].value}")', masked))
        if (
            called in _GET_NAMES
            and node.args
            and _string_key_is_url_name(node.args[0], extra_names)
        ):
            out.append((node.lineno, f'.get("{node.args[0].value}")', masked))
        for arg in node.args:
            _collect_url_refs(arg, inner, out, extra_names)
        for kw in node.keywords:
            _collect_url_refs(kw.value, inner, out, extra_names)
        return
    if isinstance(node, ast.Attribute):
        dotted = _dotted(node)
        if _is_credential_url_name(dotted, extra_names):
            out.append((node.lineno, dotted, masked))
            return
        for child in ast.iter_child_nodes(node):
            _collect_url_refs(child, masked, out, extra_names)
        return
    if isinstance(node, ast.Name):
        if _is_credential_url_name(node.id, extra_names):
            out.append((node.lineno, node.id, masked))
        return
    if isinstance(node, ast.Subscript):
        index = node.slice
        if _string_key_is_url_name(index, extra_names):
            out.append((node.lineno, f"[{index.value!r}]", masked))
            return
    for child in ast.iter_child_nodes(node):
        _collect_url_refs(child, masked, out, extra_names)


def _url_alias_names(tree: ast.AST) -> Set[str]:
    """本模块里「直接复制了 URL 名」的局部别名集合（``u = redis_url`` ⇒ ``{"u"}``）。

    **只认纯复制**（``u = redis_url`` / ``w = u`` / ``x = self.url``）——不追
    任意表达式里的 URL（``x = f(redis_url)`` 会让 taint 在整个模块里扩散，
    把无关名字全判红，定验实测过）。链式别名靠小闭包迭代覆盖。
    """
    aliases: Set[str] = set()
    for _ in range(4):
        changed = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                value, targets = node.value, list(node.targets)
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                value, targets = node.value, [node.target]
            else:
                continue
            if isinstance(value, ast.Name):
                is_url = _is_credential_url_name(value.id, aliases)
            elif isinstance(value, ast.Attribute):
                is_url = _is_credential_url_name(_dotted(value), aliases)
            else:
                is_url = False
            if not is_url:
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id not in aliases:
                    aliases.add(target.id)
                    changed = True
        if not changed:
            break
    return aliases


def credential_url_log_sites(source: str, filename: str = "<memory>") -> List[Tuple[int, str]]:
    """返回 ``[(lineno, identifier)]``：``logger.*`` 里**未经掩码**的凭据 URL 引用。

    消息本身的 f-string（``logger.info(f"{redis_url}")``）与 ``%`` 格式化
    （``logger.info("x %s" % redis_url)``）都扫——原始 URL 就落在第一个实参里。

    **判据能力边界**（定验实测 MISS，刻意不在本判据内）：
    ``adapter.info(...)``（LoggerAdapter 实例名不叫 log/logger，看不出它是日志面）、
    ``stream.write(str(url))`` / ``print(url)``（非 logger 输出面）、
    从参数/返回值流入的别名（只做**本文件内**赋值传播）。这些形态由「异常消息
    第二腿」与人工巡视覆盖，不在这里假装能判。
    """
    tree = ast.parse(source, filename=filename)
    aliases = _url_alias_names(tree)
    sites: List[Tuple[int, str]] = []
    for node in ast.walk(tree):
        if not _is_log_call(node):
            continue
        refs: List[Tuple[int, str, bool]] = []
        for arg in node.args:
            _collect_url_refs(arg, False, refs, aliases)
        for kw in node.keywords:
            _collect_url_refs(kw.value, False, refs, aliases)
        sites.extend((lineno, name) for lineno, name, masked in refs if not masked)
    return sites


def scan_plaita_for_credential_url_logs() -> List[str]:
    """全 ``plaita/`` 扫描，返回 ``["relpath:lineno: identifier", ...]``。"""
    offenders: List[str] = []
    for path in sorted(PLAITA_DIR.rglob("*.py")):
        rel = path.relative_to(REPO_ROOT).as_posix()
        for lineno, name in credential_url_log_sites(
            path.read_text(encoding="utf-8"), filename=str(path)
        ):
            offenders.append(f"{rel}:{lineno}: {name}")
    return offenders


# --------------------------------------------------------------------------- #
# 守卫第二腿：URL 藏进**异常对象**时，名字判据结构性失明（``#159`` r2）
# --------------------------------------------------------------------------- #
# 已知会把异常消息送进日志 sink 的家族（``URLPolicyError`` / ``NodeException`` /
# ``NodeExecutionError`` / ``NodeTimeoutError`` / ``FlowExecutionException`` /
# ``RedisUrlError`` / 通用 ``RuntimeError``·``ValueError``·``Exception``）：
# ``core/runner._handle_node_error`` 把 ``e`` 交给 ``logger.warning(…, e)`` 与
# ``logger.debug(…, exc_info=True)``；``server/flow_worker`` 再打
# ``NodeExecutionError``；还有 ``traceback.format_exception``。判据本身比这张
# 家族表更宽——**任何** ``raise`` 只要消息里插了 URL 名就判红。


def _raise_message_sites(node: ast.Raise, extra_names: Set[str]) -> List[Tuple[int, str]]:
    """``raise`` 消息里**未经掩码**的 URL 名（``raise URLPolicyError(f"…: {url}")``）。

    与 :func:`credential_url_log_sites` 的差别是判据位置：不是 ``logger.*`` 的
    实参名，而是**异常消息本身**。``URLPolicyError`` 的明文随后被
    ``HTTP.handle_http_node_err`` 用 ``str(err)`` 折进 ``NodeException``、再由
    ``runner._handle_node_error`` 折进 ``NodeExecutionError``，最后由
    ``logger.warning(…, e)`` 打印——只查 logger 实参名对这条链完全看不见
    （``#159`` r2 的 finding 正是如此隐身）。家族类之外的类只要消息里插了 URL
    名同样算数：判据是「URL 进了异常消息」这件事，不是异常叫什么。
    """
    if not isinstance(node.exc, ast.Call):
        return []
    refs: List[Tuple[int, str, bool]] = []
    for arg in node.exc.args:
        _collect_url_refs(arg, False, refs, extra_names)
    for kw in node.exc.keywords:
        _collect_url_refs(kw.value, False, refs, extra_names)
    return [(lineno, name) for lineno, name, masked in refs if not masked]


def _exception_note_sites(node: ast.AST, extra_names: Set[str]) -> List[Tuple[int, str]]:
    """``exc.add_note(url)``：异常 **notes** 会被 ``traceback`` 打印，同样是输出面。"""
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return []
    if node.func.attr != "add_note":
        return []
    refs: List[Tuple[int, str, bool]] = []
    for arg in node.args:
        _collect_url_refs(arg, False, refs, extra_names)
    return [(lineno, name) for lineno, name, masked in refs if not masked]


def exception_message_url_sites(source: str, filename: str = "<memory>") -> List[Tuple[int, str]]:
    """返回 ``[(lineno, identifier)]``：异常消息 / notes 里未掩码的凭据 URL 引用。"""
    tree = ast.parse(source, filename=filename)
    aliases = _url_alias_names(tree)
    sites: List[Tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Raise):
            sites.extend(_raise_message_sites(node, aliases))
        sites.extend(_exception_note_sites(node, aliases))
    return sites


def scan_plaita_for_exception_url_messages() -> List[str]:
    """全 ``plaita/`` 扫描，返回 ``["relpath:lineno: identifier", ...]``。"""
    offenders: List[str] = []
    for path in sorted(PLAITA_DIR.rglob("*.py")):
        rel = path.relative_to(REPO_ROOT).as_posix()
        for lineno, name in exception_message_url_sites(
            path.read_text(encoding="utf-8"), filename=str(path)
        ):
            offenders.append(f"{rel}:{lineno}: {name}")
    return offenders


# --------------------------------------------------------------------------- #
# 桩件：不连真 Redis / 不发真网络请求
# --------------------------------------------------------------------------- #
class PingFailingRedis:
    """``Redis.from_url`` 替身：``ping()`` 抛错，让服务启动器走 ``sys.exit(1)``。"""

    def __init__(self, url: Any = None, **kwargs: Any) -> None:
        self.url = url

    def ping(self) -> None:
        raise RuntimeError("stub: no redis in unit test")


class RecordingRedis:
    """够用的注册中心替身：记录每一次 ``SETEX`` 的 ``(key, ttl, value)``。"""

    def __init__(self) -> None:
        self.setex_calls: List[Tuple[str, int, Any]] = []

    def setex(self, key: str, ttl: int, value: Any) -> bool:
        self.setex_calls.append((key, ttl, value))
        return True

    def get(self, key: str) -> Any:
        for stored_key, _ttl, value in self.setex_calls:
            if stored_key == key:
                return value
        return None

    def delete(self, *keys: str) -> int:
        return len(keys)

    def expire(self, *args: Any, **kwargs: Any) -> bool:
        return True

    def keys(self, pattern: str = "*") -> List[str]:
        return [key for key, _ttl, _value in self.setex_calls]

    def scan_iter(self, match: Any = None, count: Any = None):
        return iter(self.keys())

    def config_get(self, *args: Any, **kwargs: Any) -> dict:
        return {}

    def pipeline(self, *args: Any, **kwargs: Any):  # pragma: no cover - 未走到
        raise AssertionError("RecordingRedis 不支持 pipeline")


def build_redis_flow_worker(
    redis_url: str,
    *,
    alert_webhook: Any = None,
    enable_registry: bool = True,
):
    """造一个不连真 Redis 的 ``RedisFlowWorker``（``RecordingRedis`` 桩）。

    返回 ``(worker, client)``。
    """
    from plaita.server.flow_worker import RedisFlowWorker
    from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

    client = RecordingRedis()
    worker = RedisFlowWorker(
        redis_url=redis_url,
        queue_name="plaita:flow:queue",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        enable_registry=enable_registry,
        redis_client=client,
        alert_webhook=alert_webhook,
    )
    return worker, client


def stored_values(client: RecordingRedis) -> str:
    return "\n".join(str(value) for _key, _ttl, value in client.setex_calls)


def log_record_fields(records) -> str:
    """把 ``caplog.records`` 的 ``msg`` / ``args`` / ``getMessage()`` / ``__dict__`` 拼成文本。

    ``#158`` r1 定验的第 4 条：只断言 ``caplog.text``（``Handler`` 格式化后的渲染）
    会漏掉「``record.args`` 里带着原文、但格式化后看不出」的形态——记录级字段必须
    一并钉住。

    ``#159`` r2 定验补的第 5 面：``logger.info("endpoint configured",
    extra={"hook": URL})`` 时 ``msg``/``args`` 全干净，密钥只落在
    ``record.hook``（即 ``record.__dict__``）里——结构化日志/JSON shipper 正是读
    这些自定义键。故除 ``LogRecord`` 标准键外，``__dict__`` 的每个键值都进断言。
    """
    chunks: List[str] = []
    for record in records:
        chunks.append(str(record.msg))
        chunks.append(repr(record.args))
        chunks.append(record.getMessage())
        for key, value in vars(record).items():
            if key in _LOGRECORD_STANDARD_ATTRS:
                continue
            chunks.append(f"{key}={value!r}")
    return "\n".join(chunks)
