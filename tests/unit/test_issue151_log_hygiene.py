"""plaita#151 —— 日志卫生：C 类（必须掩）站点的结构性守卫。

**判定表是边界**（``#168``）：``docs/log-site-taxonomy.json`` 把 806 个验收 grep
命中行判为 ``A 244 / B 85 / C 312 / D 0 / N-A 165``。本单（``#151``）只做**实施**：
把 C 类里会真泄漏的三族接到**既有**掩码设施上——不新起第二套掩码文法。

==============  =========================================================  ==============================
族              形态（判定表 C 类逐表达式判据）                               本单的渲染路径
==============  =========================================================  ==============================
① 异常文本      ``e`` / ``exc`` / ``error`` / ``exception`` 直接插值            ``_scrub_url_text``（原调用带
                                                                             ``exc_info`` 时改
                                                                            ``_scrub_exception_text``
                                                                            并摘掉 ``exc_info``）
①′ 异常文本     ``_exception_message(exc)``（有界取文本，仍是异常文本）        ``_scrub_url_text``
② 环境投影      ``raw`` ← ``os.environ.get`` / ``os.getenv`` 的**值**         ``_redact_env_value``
③ 已掩码        ``_mask_*`` / ``_scrub_*`` / ``_REDACTED``                   一字不动（必须保持掩码）
==============  =========================================================  ==============================

本文件钉四件事（``#151`` 验收第 4 条「结构性守卫 + 显式豁免表 + 新增未登记站点必须
转红（含正控）」）：

1. **腿 A**（全树）：``except … as NAME`` 体内把 ``NAME`` 裸交给 ``logger.*`` 的
   调用点一律判红——检测器**复用** ``#161`` 的 :func:`_bare_exception_log_sites`
   （同一个判据，不是第二份实现）；``exc_info=True`` 同判红，因为那一支的 traceback
   由 ``Formatter`` 在 logger 调用**之后**用原始异常重渲染，构造面清洗盖不住。
2. **腿 B**（全树）：赋值自 ``os.environ`` / ``os.getenv`` 的变量被**直接**插值进
   ``logger.*`` 即判红（必须经 ``_redact_env_value``）——与 ``#168`` 的 ``_env_probe``
   同一判据（"插值变量在所在函数内赋值自环境读取"），差别只在于这里检查**掩码是否
   落实**、判定表检查**分类**。
3. **渲染器自身**（``_redact_env_value``）：只给长度/形态、绝不回显值本体、total。
4. **正控 + 负向对照**：合成源与**真实源**改一行都必须让两条腿转红，否则守卫是空转。

**显式豁免表**（每条附理由；本文件同时是"剩余清单"的机器可读形态，只允许缩小）：

* :data:`_REMAINING_EXC_INFO_FILES` —— 表外 ``exc_info`` 面。``#168`` §1 明说
  「``exc_info=True`` 不是插值、不进本表，但 traceback 同样可能携带作者数据——
  ``#151`` 实施时按 §2 出口白名单第 4 行处理」。W12-A 按派单边界（"N-A 类不动"）
  留下 **62 站点 / 24 文件**的基线；**W19-A 已把这 62 处全部改成构造面清洗并摘掉
  ``exc_info``**，故本表现为**空**（终态）。空表不是豁免，而是**只允许保持为空**：
  任何新增 ``exc_info`` 站点都会让 :func:`test_exc_info_surface_is_registered`
  的双向对账转红。
* :data:`_DETECTOR_BLIND_SPOT_FILES` —— ``#161`` 检测器对 ``type(exc).__name__``
  这一类**类型名投影**失明（它只看"名字有没有落在掩码器调用之内"，于是把
  ``type(exc)`` 里的 ``exc`` 判成裸异常）。判定表 §4 明说该投影是 **B 类**（允许），
  故按文件登记基线计数，只允许缩小。
* :data:`_EXEMPT_ENV_NAMES` —— 判定表「env 投影」规则的**两处假阳性**：名字来源是
  env，语义却是**实体标识 / 文件系统路径**（判定表 A/B 面），插值它们不是"值回显"。
  按 ``#151`` 验收第 2 条的"显式豁免并登记理由"路径处理，不改代码。
"""
from __future__ import annotations

import ast
import pathlib
import re
from typing import Dict, List, Set, Tuple

from tests.unit._url_sink_support import PLAITA_DIR, _dotted, _is_log_call
from tests.unit.test_issue161_third_party_exception_text_sinks import (
    _MASKER_NAMES,
    _bare_exception_log_sites,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

#: ``_redact_env_value`` 之后任何"值已去值"的渲染器名都算掩码器。
_ENV_MASKER_NAMES = frozenset(_MASKER_NAMES) | {"_redact_env_value"}


def _rel(path: pathlib.Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def _iter_sources():
    for path in sorted(PLAITA_DIR.rglob("*.py")):
        yield _rel(path), path.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# 腿 A：裸异常对象 / exc_info 直达 logger
# --------------------------------------------------------------------------- #
_EXC_INFO_HISTORY = (
    "#168 §1：exc_info 不是插值、不进判定表，但 traceback 同样可能携带作者数据，"
    "#151 实施时须按 §2 出口白名单第 4 行处理（走构造面清洗并摘掉 exc_info）。"
    "W12-A 按派单边界「N-A 类不动」留下基线 62 站点 / 24 文件（plaita/__init__.py 1、"
    "core/async_utils.py 2、core/context.py 2、core/flow.py 1、core/http_session.py 2、"
    "core/parallel_executor.py 1、core/runner.py 1、dsl/ir_validate.py 1、event/redis.py 4、"
    "node/code.py 2、node/concurrent.py 1、obs.py 11、server/event_filter.py 5、"
    "server/flow_worker.py 9、server/metrics.py 1、server/node_timings.py 3、"
    "server/services/approval_service.py 2、base_service.py 1、delay_service.py 2、"
    "kafka_queue_service.py 2、redis_queue_service.py 2、schedule_service.py 2、"
    "server/tenant_status.py 3、storage/sqlalchemy.py 1）；W19-A 已全数处理（构造面渲染 + "
    "摘掉 exc_info），差量对账见 scripts/issue151_exc_info_leak_probe.py 与 RED-PROOF。"
)

#: 表外 ``exc_info`` 面 —— **终态：空**（W19-A 已处理完 62 站点 / 24 文件）。
#: 键 = 文件（相对仓根），值 = **(基线计数, 理由)**。本表**只允许保持为空 / 缩小**：
#: 任何新增 ``exc_info`` 站点都会让 :func:`test_exc_info_surface_is_registered`
#: 的双向对账转红；已掩码/已删除的行必须删掉（死行同样红）。
_REMAINING_EXC_INFO_FILES: Dict[str, Tuple[int, str]] = {}

#: ``#161`` 检测器的**已知盲区**命中（不是真泄漏）：``type(exc).__name__`` 是类型名
#: 投影（判定表 §4 判为 **B 类**、允许），但检测器把 ``type(exc)`` 里的 ``exc`` 当裸异常。
#: 键 = 文件，值 = **(基线计数, 理由)**；只允许缩小（同站的真异常值已被掩码）。
_DETECTOR_BLIND_SPOT_FILES: Dict[str, Tuple[int, str]] = {
    "plaita/core/expression_parser.py": (
        2,
        "#161 检测器盲区：type(exc).__name__ 里被当成裸异常的 exc（B 类类型名投影）。"
        "同站的异常**文本**已走 _scrub_url_text（见 plaita#151）。",
    ),
}


def _records_with_exc_info(source: str, filename: str = "<memory>") -> List[Tuple[int, str]]:
    """``logger.*`` 里 ``exc_info`` 为真（或非字面 False）的调用点。

    ``#168`` §1 的边界原话：「``exc_info=True`` 不是插值、不进本表，但 traceback
    同样可能携带作者数据——``#151`` 实施时按 §2 出口白名单第 4 行处理」。检测放在
    **所有** ``logger.*`` 调用上（不限 ``except`` 体内）——历史站点
    ``core/runner.py`` 的 ``logger.debug("handled node error traceback", exc_info=True)``
    就在 handler 之外（W19-A 已改成 ``_scrub_exception_text(e)``）。

    本判据在 W19-A 后于整棵树**零命中**（:data:`_REMAINING_EXC_INFO_FILES` 为空），
    故这里是"新增即红"的净空雷达：任何一处 ``exc_info`` 都会让双向对账出现差量。
    """
    tree = ast.parse(source, filename=filename)
    out: List[Tuple[int, str]] = []
    for node in ast.walk(tree):
        if not _is_log_call(node):
            continue
        for kw in node.keywords:
            if kw.arg != "exc_info":
                continue
            if isinstance(kw.value, ast.Constant) and kw.value.value is False:
                continue
            out.append((node.lineno, _dotted(node.func)))
    return out


def scan_bare_exception_sites() -> Dict[str, List[Tuple[int, str, str]]]:
    """``{relpath: [(lineno, identifier, reason)]}``——全 ``plaita/`` 的腿 A 命中。"""
    out: Dict[str, List[Tuple[int, str, str]]] = {}
    for rel, source in _iter_sources():
        hits = _bare_exception_log_sites(source, filename=rel)
        if hits:
            out[rel] = hits
    return out


def scan_exc_info_sites() -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {}
    for rel, source in _iter_sources():
        hits = [ln for ln, _ in _records_with_exc_info(source, filename=rel)]
        if hits:
            out[rel] = hits
    return out


class TestLegANoBareExceptionReachesLogger:
    """``except … as NAME`` 的 ``NAME``（或 ``exc_info``）不得原样进 ``logger.*``。"""

    def test_registries_are_coherent(self):
        """豁免表自身可读：计数为正、理由非空、文件存在。"""
        for registry in (_REMAINING_EXC_INFO_FILES, _DETECTOR_BLIND_SPOT_FILES):
            for rel, (count, why) in registry.items():
                assert count > 0, rel
                assert why and len(why) > 8, rel
                assert (REPO_ROOT / rel).is_file(), rel

    def test_no_unregistered_bare_exception_site(self):
        offenders = scan_bare_exception_sites()
        unregistered: List[str] = []
        for rel, hits in sorted(offenders.items()):
            budget = _DETECTOR_BLIND_SPOT_FILES.get(rel, (0, ""))[0]
            bare_hits = [h for h in hits if h[2] != "exc_info=True"]
            # 裸异常对象：本单已全数掩码，只放行登记在册的检测器盲区（类型名投影）。
            if len(bare_hits) > budget:
                unregistered.extend(
                    f"{rel}:{ln}: {name} ({why})" for ln, name, why in bare_hits
                )
            # exc_info 面：登记在另一个表里（见 test_exc_info_surface_is_registered）。
            exc_info_hits = [h for h in hits if h[2] == "exc_info=True"]
            for ln, name, why in exc_info_hits:
                if rel not in _REMAINING_EXC_INFO_FILES:
                    unregistered.append(f"{rel}:{ln}: {name} ({why})（文件未登记）")
        assert unregistered == [], (
            "下列 logger 调用点把异常对象原样交给日志（#151 C 类必须掩）：\n  "
            + "\n  ".join(unregistered)
            + "\n\n修法：异常文本走 plaita.core.url_redaction 的 _scrub_url_text（原调用带 "
            "exc_info 时走 _scrub_exception_text 并摘掉 exc_info）；确需留待后续批次时，"
            "必须在 _REMAINING_EXC_INFO_FILES / _DETECTOR_BLIND_SPOT_FILES 显式登记理由。"
        )

    def test_exc_info_surface_is_registered(self):
        """全树 ``exc_info`` 面与登记表**双向**一致（缺行/死行都红）。"""
        current = {rel: len(hits) for rel, hits in scan_exc_info_sites().items()}
        registered = {rel: count for rel, (count, _why) in _REMAINING_EXC_INFO_FILES.items()}
        diff = {
            rel: (registered.get(rel, 0), current.get(rel, 0))
            for rel in set(registered) | set(current)
            if registered.get(rel, 0) != current.get(rel, 0)
        }
        assert diff == {}, (
            "表外 exc_info 面与登记表不一致（#168 §1 指示 #151 处理该族）：\n"
            f"  文件: (登记, 实际) = {diff}\n"
            "  实际变多 ⇒ 先掩码；已掩码/已删除 ⇒ 从登记表删行（只允许缩小）。"
        )

    def test_exc_info_face_is_empty_terminal_state(self):
        """W19-A 终态：62 站点 / 24 文件的 ``exc_info`` 面已全数处理，整树零命中。

        与 :func:`test_exc_info_surface_is_registered` 的差别是**方向**：后者是双向
        对账（表 ↔ 树），本条件把"零命中"直接钉成不变量——若有人往空表里再登记一个
        基线（=把已修面重新变回豁免），这条先红。
        """
        assert _REMAINING_EXC_INFO_FILES == {}, "该表只允许保持为空（只允许缩小）"
        assert scan_exc_info_sites() == {}, f"exc_info 面复活：{scan_exc_info_sites()}"

    def test_negative_control_real_source_reddens_for_exc_info(self):
        """真实源（``server/tenant_status.py``）退回 ``exc_info=True`` ⇒ 两条判据都红。

        记录面（``_records_with_exc_info``）与裸异常面（``_bare_exception_log_sites``，
        本单给这些 ``except`` 加了 ``as exc``）必须同时抓住回退，否则 W19-A 的覆盖是空转。
        """
        rel = "plaita/server/tenant_status.py"
        source = (REPO_ROOT / rel).read_text(encoding="utf-8")
        fixed = '"租户 %s 状态同步到 Redis 失败: %s", tenant_id, _scrub_exception_text(exc)'
        assert fixed in source, f"前提失效：{rel} 里没有 {fixed}"
        probe = source.replace(
            fixed, '"租户 %s 状态同步到 Redis 失败", tenant_id, exc_info=True', 1
        )
        assert probe != source
        assert _records_with_exc_info(probe, filename=rel), "摘掉掩码后 exc_info 判据没红（假阴）"
        assert [h for h in _bare_exception_log_sites(probe, filename=rel) if h[2] == "exc_info=True"], (
            "摘掉掩码后裸异常判据没红（假阴）"
        )
        assert _records_with_exc_info(source, filename=rel) == []
        assert _bare_exception_log_sites(source, filename=rel) == []

    def test_positive_control_synthetic(self):
        red = _bare_exception_log_sites(
            "try:\n    x()\nexcept Exception as e:\n    logger.error('x: %s', e)\n"
        )
        assert red, "腿 A 的检测器对合成裸异常不红 —— 守卫空转"
        assert _bare_exception_log_sites(
            "try:\n    x()\nexcept Exception as e:\n"
            "    logger.error('x: %s', _scrub_url_text(e))\n"
        ) == []
        assert _records_with_exc_info(
            "try:\n    x()\nexcept Exception as e:\n    logger.error('x', exc_info=True)\n"
        ), "腿 A 对 exc_info=True 不红 —— 守卫空转"

    def test_negative_control_real_source_reddens(self, tmp_path):
        """真实源改一行（把 #151 的掩码摘掉）⇒ 腿 A 必须转红，还原后转绿。

        取 ``core/flow.py`` 的 ``except Exception as e`` 站点：检测器只看
        ``except … as NAME`` 体内的裸名（``core/runner.py:149`` 的 ``e`` 是**函数参数**，
        在 handler 之外——那类站点由 :func:`scan_bare_exception_sites` 之外的
        「C 类工作清单」逐条覆盖，见 RED-PROOF 的覆盖清单）。
        """
        rel = "plaita/core/flow.py"
        source = (REPO_ROOT / rel).read_text(encoding="utf-8")
        fixed = "_scrub_url_text(e)"
        assert fixed in source, f"前提失效：{rel} 里没有 {fixed}"
        probe = tmp_path / "probe_flow.py"
        probe.write_text(source.replace(fixed, "e", 1), encoding="utf-8")
        reddened = _bare_exception_log_sites(probe.read_text(encoding="utf-8"), filename=str(probe))
        assert reddened, "把掩码摘掉后腿 A 没红（假阴）"
        assert _bare_exception_log_sites(source, filename=rel) == []


# --------------------------------------------------------------------------- #
# 腿 B：环境变量**值**不得原样插值
# --------------------------------------------------------------------------- #
_ENV_READ_RE = re.compile(r"\bos\.environ\b|\bos\.getenv\b|\benviron\.get\b|\bgetenv\(")

#: 判定表「env 投影」规则的显式豁免（假阳性）：名字来源是 env，语义是实体标识 /
#: 文件系统路径（判定表 A/B 面）⇒ 插值它们不是"值回显"。
_EXEMPT_ENV_NAMES: Dict[Tuple[str, str], str] = {
    ("plaita/server/services/__main__.py", "instance_id"): (
        "服务**实例标识**（`PLAITA_INSTANCE_ID`，缺省 `{service_type}-{pid}`）——判定表 A 类"
        "「流程/运行实体标识（跨日志可回查）」，不是环境**值**回显"
    ),
    ("plaita/writefile_jail.py", "explicit"): (
        "writefile jail 的**工作根路径**（`PLAITA_NODES_WORKSPACE_ROOT`）——判定表 B 类"
        "「基础设施标量/文件系统路径」；掩掉它等于删掉这条日志的全部诊断价值"
    ),
    ("plaita/server/services/schedule_service.py", "console_queue"): (
        "调度**派发 Stream 键名**（`PLAITA_CONSOLE_TASK_QUEUE`）——判定表 A 类"
        "「流程/运行实体标识（跨日志可回查）」；A56-02 的分裂队列告警必须能回答"
        "「消息进了哪个队列」，掩掉键名这条 WARNING 就失去可操作性"
    ),
    ("plaita/server/services/schedule_service.py", "worker_queue"): (
        "worker **消费 Stream 键名**（`PLAITA_QUEUE_NAME`/`QUEUE_NAME`）——同上，"
        "实体标识不是凭据值；分裂队列告警需要同时给出生产/消费两侧键名"
    ),
}


def _reads_env(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Attribute) and child.attr in ("environ", "getenv"):
            return True
        if isinstance(child, ast.Name) and child.id == "getenv":
            return True
    return False


def _scope_map(tree: ast.AST) -> Dict[int, ast.AST]:
    parent: Dict[int, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parent[id(child)] = node
    return parent


def _scope_of(node: ast.AST, parent: Dict[int, ast.AST]):
    cur: ast.AST | None = node
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.Module)):
            return cur
        cur = parent.get(id(cur))
    return None


def _env_derived_names_by_scope(tree: ast.AST, parent: Dict[int, ast.AST]) -> Dict[int, Set[str]]:
    """**按作用域**收集赋值自 env 读取的变量名。

    与 ``#168`` 的 ``_env_probe`` 同一判据（"插值变量在**所在函数内**赋值自
    ``os.environ`` / ``os.getenv``"）——作用域必须限定，否则同一文件里另一个函数的
    `raw`（队列载荷，判定表 A 类）会被误判成 env 值（本单第一版实测的假阳性）。
    """
    names: Dict[int, Set[str]] = {}
    for node in ast.walk(tree):
        value = None
        targets: List[ast.AST] = []
        if isinstance(node, ast.Assign):
            value, targets = node.value, list(node.targets)
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            value, targets = node.value, [node.target]
        elif isinstance(node, ast.NamedExpr):
            value, targets = node.value, [node.target]
        if value is None or not _reads_env(value):
            continue
        scope = _scope_of(node, parent)
        bucket = names.setdefault(id(scope), set())
        for target in targets:
            if isinstance(target, ast.Name):
                bucket.add(target.id)
    return names


def _collect_env_refs(node: ast.AST, env_names: Set[str], masked: bool, out: list) -> None:
    if isinstance(node, ast.Call):
        called = _dotted(node.func).rsplit(".", 1)[-1]
        inner = masked or called in _ENV_MASKER_NAMES
        _collect_env_refs(node.func, env_names, masked, out)
        for arg in node.args:
            _collect_env_refs(arg, env_names, inner, out)
        for kw in node.keywords:
            _collect_env_refs(kw.value, env_names, inner, out)
        return
    if isinstance(node, ast.IfExp):
        # 三元表达式的 **test 位不是渲染值**：`ENV_NAME if raw else "默认值"` 渲染的是
        # env 变量**名**（判定表 B 类），`raw` 只作真值判定 ⇒ 只下潜 body/orelse。
        _collect_env_refs(node.body, env_names, masked, out)
        _collect_env_refs(node.orelse, env_names, masked, out)
        return
    if isinstance(node, ast.Name):
        if node.id in env_names:
            out.append((node.lineno, node.id, masked))
        return
    for child in ast.iter_child_nodes(node):
        _collect_env_refs(child, env_names, masked, out)


def env_value_log_sites(source: str, filename: str = "<memory>") -> List[Tuple[int, str]]:
    """``logger.*`` 里**未经掩码器**直接插值的 env 派生名（按所在作用域判定）。"""
    tree = ast.parse(source, filename=filename)
    parent = _scope_map(tree)
    by_scope = _env_derived_names_by_scope(tree, parent)
    if not by_scope:
        return []
    out: List[Tuple[int, str]] = []
    for node in ast.walk(tree):
        if not _is_log_call(node):
            continue
        env_names = by_scope.get(id(_scope_of(node, parent)), set())
        if not env_names:
            continue
        refs: list = []
        for arg in list(node.args)[1:]:
            _collect_env_refs(arg, env_names, False, refs)
        for kw in node.keywords:
            _collect_env_refs(kw.value, env_names, False, refs)
        out.extend((ln, name) for ln, name, masked in refs if not masked)
    return out


class TestLegBNoEnvValueReachesLogger:
    def test_no_unregistered_env_value_interpolation(self):
        offenders: List[str] = []
        files_hit = 0
        for rel, source in _iter_sources():
            sites = env_value_log_sites(source, filename=rel)
            if not sites:
                continue
            files_hit += 1
            for lineno, name in sites:
                if (rel, name) in _EXEMPT_ENV_NAMES:
                    continue
                offenders.append(f"{rel}:{lineno}: {name}")
        assert files_hit > 0, "腿 B 在整棵树上零命中 —— 检测器已失效（守卫空转）"
        assert offenders == [], (
            "下列 logger 调用点把**环境变量值**原样插进日志（#151 C 类「env 投影」必须掩）：\n  "
            + "\n  ".join(offenders)
            + "\n\n修法：值走 plaita.core.url_redaction._redact_env_value（只留长度/形态）；"
            "确需插值原值时必须在此显式登记理由。"
        )

    def test_the_seven_env_sites_are_actually_masked(self):
        """本轮覆盖的 7 个 env 站点逐个核对（都是判定表 C 类「env 投影」）。"""
        expected = {
            "plaita/core/expression.py": 2,
            "plaita/server/flow_worker.py": 3,
            "plaita/server/task_queue.py": 1,
            "plaita/storage/redis.py": 1,
        }
        for rel, count in expected.items():
            source = (REPO_ROOT / rel).read_text(encoding="utf-8")
            assert source.count("_redact_env_value(") >= count, rel

    def test_positive_control_synthetic(self):
        red = env_value_log_sites(
            "import os\n\n"
            "def f():\n"
            "    raw = (os.environ.get('X') or '').strip()\n"
            "    logger.warning('X=%r', raw)\n"
        )
        assert red == [(5, "raw")], red
        green = env_value_log_sites(
            "import os\n\n"
            "def f():\n"
            "    raw = (os.environ.get('X') or '').strip()\n"
            "    logger.warning('X=%s', _redact_env_value(raw))\n"
        )
        assert green == [], green
        # env 变量**名**始终允许（判定表 B 类）。
        assert env_value_log_sites(
            "import os\n\n"
            "def f(env_name):\n"
            "    raw = (os.environ.get(env_name) or '').strip()\n"
            "    logger.warning('%s=%s', env_name, _redact_env_value(raw))\n"
        ) == []
        # 作用域隔离：另一个函数里同名的 A 类载荷（`raw`）不许被误报。
        assert env_value_log_sites(
            "import os\n\n"
            "def env_reader():\n"
            "    raw = (os.environ.get('X') or '').strip()\n"
            "    return _redact_env_value(raw)\n\n"
            "def payload_reader(raw):\n"
            "    logger.info('payload=%s', raw)\n"
        ) == []

    def test_negative_control_real_source_reddens(self, tmp_path):
        """真实源（``server/task_queue.py``）改一行 ⇒ 腿 B 转红，还原后转绿。"""
        rel = "plaita/server/task_queue.py"
        source = (REPO_ROOT / rel).read_text(encoding="utf-8")
        fixed = "_redact_env_value(raw)"
        assert fixed in source, f"前提失效：{rel} 里没有 {fixed}"
        probe = tmp_path / "probe_task_queue.py"
        probe.write_text(source.replace(fixed, "raw", 1), encoding="utf-8")
        assert env_value_log_sites(
            probe.read_text(encoding="utf-8"), filename=str(probe)
        ), "把 _redact_env_value 摘掉后腿 B 没红（假阴）"
        assert env_value_log_sites(source, filename=rel) == []


# --------------------------------------------------------------------------- #
# 渲染器自身：只给形态、totality、绝不回显值本体
# --------------------------------------------------------------------------- #
class TestEnvValueRenderer:
    def test_never_echoes_the_value(self):
        from plaita.core.url_redaction import _redact_env_value

        secret = "Ks9Qm4Zt"
        for raw in (secret, f"access_token={secret}", f"Bearer {secret}"):
            rendered = _redact_env_value(raw)
            assert secret not in rendered, rendered
            assert raw != rendered  # 值本体一个字节都不留

    def test_shape_names(self):
        from plaita.core.url_redaction import _redact_env_value

        assert _redact_env_value("8080") == "<env:int len=4>"
        assert _redact_env_value("12.5") == "<env:number len=4>"
        assert _redact_env_value("abc") == "<env:text len=3>"
        assert _redact_env_value("") == "<env:empty>"
        assert _redact_env_value("   ") == "<env:empty>"
        assert _redact_env_value(None) == "<env:unset>"

    def test_total_on_hostile_inputs(self):
        from plaita.core.url_redaction import _redact_env_value

        for value in (None, object(), b"\xff\xfe", 123, ["x"], {"k": "v"}, "\ud800"):
            rendered = _redact_env_value(value)
            assert isinstance(rendered, str), value
            rendered.encode("utf-8")  # 写不进日志就等于是新的异常面
