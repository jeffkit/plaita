"""``#158`` / ``#159`` 测试的共用支撑：**凭据 URL 的日志守卫** + 少量桩件。

**不是测试模块**（``_`` 前缀，pytest 默认 ``python_files=test_*.py`` 不收集它）。

守卫的来由：``#158`` r1 的守卫只认 ``redis_url`` 这个名字，于是
``alerts.py`` 的 ``self.url`` 与 ``flow_worker`` 的 ``self._alert_webhook``
（``#159`` 的两个 sink）在它眼里完全隐身。本版把判据从「名字里有 ``redis_url``」
扩成「**任何形如 URL 的标识符**」：

* 名字（点分）里含 ``redis_url`` / ``webhook``（不分大小写）；或
* 点分名的**最后一段**恰是 ``url`` / ``uri``（``self.url``、``request.url``）；或
* 下标键是 ``"redis_url"`` / ``"url"`` 之类（``cfg["url"]``）。

同时修掉 r1 定验记录的两条**假阴**：

1. 白名单里的 ``_passwords`` 返回的是**原始口令**（``_passwords(url)`` 就是口令
   元组），把「返回原文」当成「已掩码」⇒ 已从 :data:`_MASKER_NAMES` 移除；
2. ``_collect_url_refs`` 从不访问 ``Call.func`` ⇒ ``redis_url.strip()`` /
   ``self.url.format(...)`` 这类现实写法漏判 ⇒ 现在递归 ``Call.func``。
"""
from __future__ import annotations

import ast
import pathlib
import re
from typing import Any, List, Set, Tuple

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
PLAITA_DIR = REPO_ROOT / "plaita"

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
        "_safe_render",
        "_scrub",
        "_safe_reason",
        "_sanitized_cause",
    }
)

#: 「凭据落在 URL 里」的名字特征：Redis 连接串、webhook（含 ``_alert_webhook``）。
_URL_NAME_RE = re.compile(r"redis_url|webhook", re.I)
#: 点分名的最后一段恰是 url/uri ⇒ 该标识符本身就是个 URL。
_URL_TAIL_RE = re.compile(r"^(?:url|uri)$", re.I)


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
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr not in _LOG_METHODS:
        return False
    dotted = _dotted(node.func)
    base = dotted.rsplit(".", 1)[0] if dotted else ""
    if not base:
        # ``logging.getLogger(__name__).info(...)``：被调用者是 Call，取它的链尾。
        inner = node.func.value
        if isinstance(inner, ast.Call):
            base = _dotted(inner.func)
    return base.lower().endswith(("log", "logger"))


def _is_credential_url_name(dotted: str) -> bool:
    if not dotted:
        return False
    tail = dotted.rsplit(".", 1)[-1]
    # 掩码器**自己的名字**不是 URL（``_mask_webhook_url_for_log`` 里就带
    # "webhook"）：否则修复后的每一个调用点都会被守卫判红。
    if tail in _MASKER_NAMES:
        return False
    if _URL_NAME_RE.search(dotted):
        return True
    return bool(_URL_TAIL_RE.match(tail))


def _collect_url_refs(node: ast.AST, masked: bool, out: List[Tuple[int, str, bool]]) -> None:
    """收集日志参数里的凭据 URL 标识符，并记住它是否位于掩码器调用之内。"""
    if isinstance(node, ast.Call):
        called = _dotted(node.func).rsplit(".", 1)[-1]
        inner = masked or called in _MASKER_NAMES
        # r1 假阴 #2：``Call.func`` 必须一并走——否则 ``redis_url.strip()`` 的
        # ``redis_url``（藏在 func 链里）永远不被访问。注意 func 用**进入时**的
        # masked（``_mask_url(url).strip()`` 的内层仍应算已掩码）。
        _collect_url_refs(node.func, masked, out)
        for arg in node.args:
            _collect_url_refs(arg, inner, out)
        for kw in node.keywords:
            _collect_url_refs(kw.value, inner, out)
        return
    if isinstance(node, ast.Attribute):
        dotted = _dotted(node)
        if _is_credential_url_name(dotted):
            out.append((node.lineno, dotted, masked))
            return
        for child in ast.iter_child_nodes(node):
            _collect_url_refs(child, masked, out)
        return
    if isinstance(node, ast.Name):
        if _is_credential_url_name(node.id):
            out.append((node.lineno, node.id, masked))
        return
    if isinstance(node, ast.Subscript):
        index = node.slice
        if (
            isinstance(index, ast.Constant)
            and isinstance(index.value, str)
            and _is_credential_url_name(index.value)
        ):
            out.append((node.lineno, f"[{index.value!r}]", masked))
            return
    for child in ast.iter_child_nodes(node):
        _collect_url_refs(child, masked, out)


def credential_url_log_sites(source: str, filename: str = "<memory>") -> List[Tuple[int, str]]:
    """返回 ``[(lineno, identifier)]``：``logger.*`` 里**未经掩码**的凭据 URL 引用。

    消息本身的 f-string（``logger.info(f"{redis_url}")``）与 ``%`` 格式化
    （``logger.info("x %s" % redis_url)``）都扫——原始 URL 就落在第一个实参里。
    """
    tree = ast.parse(source, filename=filename)
    sites: List[Tuple[int, str]] = []
    for node in ast.walk(tree):
        if not _is_log_call(node):
            continue
        refs: List[Tuple[int, str, bool]] = []
        for arg in node.args:
            _collect_url_refs(arg, False, refs)
        for kw in node.keywords:
            _collect_url_refs(kw.value, False, refs)
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
    """把 ``caplog.records`` 的 ``msg`` / ``args`` / ``getMessage()`` 拼成一段文本。

    ``#158`` r1 定验的第 4 条：只断言 ``caplog.text``（``Handler`` 格式化后的渲染）
    会漏掉「``record.args`` 里带着原文、但格式化后看不出」的形态——记录级字段必须
    一并钉住。
    """
    chunks: List[str] = []
    for record in records:
        chunks.append(str(record.msg))
        chunks.append(repr(record.args))
        chunks.append(record.getMessage())
    return "\n".join(chunks)
