"""P0-2 守卫：扫描 ``plaita/`` 源码, 拦截"静默吞咽异常"的 except 块。

一个 except 块如果既不 ``raise``、不记日志 (``logger.*`` / ``logging.*`` /
``self.log*``)、也不把异常对象显式存进返回值/哨兵, 就是在静默吞咽——这是
本项目历史重灾区 (历史上 161 处 ``except Exception`` + 5 处 bare ``except:``)。

本测试把当前已知的"可接受位点"列入白名单, 阻止新增静默吞咽。新代码请:
要么精确捕获并 ``raise``/记日志, 要么显式返回带 ``__error__`` 的哨兵。
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

PLAITA_DIR = Path(__file__).resolve().parents[2] / "plaita"

# 已审计的"可接受静默位点": 每条是 (相对 plaita/ 的文件路径, 行号)。
# 这些是预期的 fallback 分支 (如 JSON 解析失败回退到原始文本、版本号排序
# 失败回退到任意版本), 不记录异常是合理的。
ALLOWED_SILENT = {
    # 2026-10-02 行号随敏感头剥离 helper 插入下移（423→446, 485→511），位点未变；
    # 后随清理批次 C2-1（HttpRequestInfo 快照 + 错误帧摘要 helper 插入）再下移（446→510, 511→587），位点未变；
    # 后随 orjson 缺失分支补 ``orjson = None``（模块属性对齐 aiohttp/requests 写法）再下移（510→512, 587→589），位点未变
    ("node/http.py", 512),   # response.json() 失败回退到 response.text (sync path)
    ("node/http.py", 589),   # json.loads() 失败回退到原始文本 (async aiohttp path)
    ("storage/memory.py", 138),  # 版本号非纯数字排序失败, 回退到任意版本
    # 2026-10-02 行号随 TERMINAL_EXECUTION_STATUSES 常量插入下移（105→113），位点未变；
    # 后随波次④（订阅超时 checker 宿主）的 import 与 EventFilter 构造参数插入再下移（113→127），位点未变；
    # 后随事件回扫（EventReconciler 宿主）的 event_storage 参数与 _reconciler 字段插入再下移（127→135），位点未变；
    # 后随队列名 env 兜底（DEFAULT_QUEUE_NAME 常量 + _resolve_queue_name）插入再下移（135→139），位点未变
    ("server/event_filter.py", 139),  # 孤儿订阅巡检失败不影响主流程（有兜底 debug 日志）
    # 2026-09-30 killpg 孤儿修复：强杀路径是 best-effort——组消失/权限不足/mock pid
    # 时退化杀直接子进程；reap 的 communicate 再超时（组外进程握住管道）则放弃收尸
    # 直接抛错。异常细节不影响「进程组已被杀」这一主结果。
    # 2026-10-06 RLIMIT_AS 默认关闭（模块/函数 docstring 与配置注释插入）后整体下移 6：
    # 419→425, 450→456, 458→464；原 414 一条是与 425 同处的陈旧重复项，随更新删除。
    # 2026-10-07 env 白名单抽到 plaita.subprocess_env（顶层 import 插入）后再下移 2：
    # 425→427, 456→458, 464→466，位点未变。
    # plaita#29（js 接入档位体系 / 语言白名单：docstring 段、模块常量与 JS runner
    # 模板段插入）后再整体下移 81：427→508, 458→539, 466→547；随后 `run_js` 补
    # unsafe 档 docstring（+9）到 517/548/556，位点未变。
    ("node/code.py", 517),
    ("node/code.py", 548),
    ("node/code.py", 556),
    # 2026-10-07 沙箱生命周期回调条件装配：plaita-nodes 是可选依赖，缺装 / 无沙箱
    # 注册表 / 装配失败都返回 None（非沙箱部署零行为变化），异常细节不影响主流程。
    # plaita#22 沙箱白名单接线（`_code_allowed_backends_for_worker` 插入）后整体下移 12：
    # 781→793, 786→798, 792→804，位点未变。
    # 后续 shift：8cd9563/cbcdd38（暂停沙箱清扫 / 沙箱生命周期回调）与 #26 指标告警
    # 接线（顶层 import + env helper + metrics/alert 方法插入）累积下移，2026-10-08
    # 校准到 902/926/931/937——四个位点（可选依赖导入 / 生命周期装配 / 注册表加载）
    # 语义未变；另补 `collect_workspace_snapshots` 导入失败一条（此前漏登记）。
    # 再 shift：`_paused_sweeper` 的 e2b 预导入从「准备」try 里拆成独立 best-effort
    # 分支（缺 e2b 不再等于「不清扫」，见该函数 docstring）后整体下移 5：
    # 902→907, 926→931, 931→936, 937→942，位点未变。
    # plaita#29（worker 入口补 `_code_allowed_languages_for_worker` 接线）后再下移 16：
    # 907→923, 931→947, 936→952, 942→958，位点未变。
    # plaita#33（挂起幂等短路 + ResumeProtocolError）整体再下移 49：
    # 923→972, 947→996, 952→1001, 958→1007，位点未变。
    ("server/flow_worker.py", 1039),
    ("server/flow_worker.py", 1063),
    ("server/flow_worker.py", 1068),
    ("server/flow_worker.py", 1074),
}


def _handler_fingerprint(source: str, lineno: int) -> str:
    """静默 except 的**行号无关**指纹：该 handler 的 try 体首行 + handler + body。

    为什么需要它：ALLOWED_SILENT 历史上是**纯行号**清单，任何在该文件上方插入
    代码的改动都会让全部登记位点集体下移，门禁随即误报「新增静默吞咽」——
    2026-10-09/10 两天内因此反复校准（+12/+16/+49/+27/+66/+67… 全日志可见）。
    指纹让登记**跟随代码本身**，不再随行号漂移。

    取 try 体首行是因为「except Exception: return None」这类样板在文件里重复
    出现（仅 handler+body 会撞车），带上它守护的调用才唯一。
    """
    lines = source.split("\n")
    if lineno - 1 >= len(lines):
        return ""

    def _norm(idx: int) -> str:
        return lines[idx].split("#", 1)[0].strip() if 0 <= idx < len(lines) else ""

    head_indent = len(lines[lineno - 1]) - len(lines[lineno - 1].lstrip())
    # try 体首行：向上找最近的 "try:"，取其后第一条同缩进语句
    try_idx = None
    k = lineno - 2
    while k >= 0:
        st = _norm(k)
        if st == "try:" and (len(lines[k]) - len(lines[k].lstrip())) == head_indent:
            try_idx = k
            break
        if st and (len(lines[k]) - len(lines[k].lstrip())) < head_indent:
            break
        k -= 1
    guard = ""
    if try_idx is not None:
        j = try_idx + 1
        while j < lineno - 1:
            if _norm(j):
                guard = _norm(j)
                break
            j += 1
    parts = [guard, _norm(lineno - 1)]
    j = lineno
    while j < len(lines):
        if not _norm(j):
            j += 1
            continue
        if (len(lines[j]) - len(lines[j].lstrip())) <= head_indent:
            break
        parts.append(_norm(j))
        j += 1
    return " | ".join(p for p in parts if p)


def _allowed_fingerprints() -> set[str]:
    """ALLOWED_SILENT_FINGERPRINTS：显式登记的行号无关指纹。"""
    return set(globals().get("ALLOWED_SILENT_FINGERPRINTS") or ())


# ── 行号无关登记（新通道）────────────────────────────────────────────────
# 上面 ALLOWED_SILENT 是历史行号清单（保留兼容）；**新登记请用指纹**，
# 这样在文件上方插入代码时不会再集体漂移（见 _handler_fingerprint）。
ALLOWED_SILENT_FINGERPRINTS = {
    # flow_worker 沙箱装配三处 best-effort（plaita-nodes 为可选依赖：
    # 缺装 / 无注册表 / 装配失败都返回 None，非沙箱部署零行为变化）
    "from plaita_nodes.sandbox import collect_workspace_snapshots | except Exception: | return",
    "from plaita_nodes.lifecycle import SandboxLifecycleCallback | except Exception: | return None",
    "from plaita_nodes import sandbox as _sb | except Exception: | return None",
    "return SandboxLifecycleCallback(sandboxes=specs) | except Exception: | return None",
}


def _is_logging_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
        return False
    func = node.value.func
    # logger.xxx / logging.xxx / self.log.xxx / self.logger.xxx
    if isinstance(func, ast.Attribute):
        name = func.attr
        if name in {"debug", "info", "warning", "warn", "error", "critical",
                    "exception", "log"}:
            return True
    return False


def _body_is_silent(body: list[ast.stmt]) -> bool:
    """真"静默": body 里既无 ``raise``、无任何函数调用。有调用即视为非静默——
    调用可能是记日志、调 helper 重新抛、或把异常对象放进 queue/返回值传播。
    真静默形态: ``pass`` / ``return None`` / ``return {}`` / ``x = None``。"""
    for stmt in body:
        for child in ast.walk(stmt):
            if isinstance(child, ast.Raise):
                return False
            if isinstance(child, ast.Call):
                return False
    return True


def _is_blind_except(handler: ast.ExceptHandler) -> bool:
    """只盯"盲捕": ``except Exception``/``except BaseException``/bare ``except:``。
    窄捕获 (``except ImportError``/``except KeyError`` 等) 是显式选择, 不算静默吞咽文化。"""
    t = handler.type
    if t is None:
        return True  # bare except
    if isinstance(t, ast.Name) and t.id in {"Exception", "BaseException"}:
        return True
    # except (Exception, ...) 形式
    if isinstance(t, ast.Tuple):
        return any(isinstance(e, ast.Name) and e.id in {"Exception", "BaseException"}
                   for e in t.elts)
    return False


def _collect_silent_except(path: Path) -> list[tuple[int, str]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and _is_blind_except(node):
            if not _body_is_silent(node.body):
                continue
            offenders.append((node.lineno, path.relative_to(PLAITA_DIR).as_posix()))
    return offenders


def test_no_bare_except_in_plaita():
    """bare ``except:`` 连 KeyboardInterrupt/SystemExit 都吞, 必须为 0。"""
    bare = []
    for path in PLAITA_DIR.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler) and node.type is None:
                bare.append((path.relative_to(PLAITA_DIR).as_posix(), node.lineno))
    assert not bare, f"bare `except:` forbidden in plaita/: {bare}"


def test_silent_except_blocks_are_whitelisted():
    """所有"无日志无 raise"的 except 块必须在白名单里, 防止新增静默吞咽。"""
    all_silent = []
    for path in PLAITA_DIR.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        for lineno, rel in _collect_silent_except(path):
            all_silent.append((rel, lineno))

    # 双通道：行号（历史登记）**或**行号无关指纹（新增登记）命中即放行。
    # 指纹让门禁不再因上方插入代码而误报（见 _handler_fingerprint docstring）。
    fps = _allowed_fingerprints()
    fp_cache: dict[str, str] = {}
    unlisted = []
    for rel, lineno in all_silent:
        if (rel, lineno) in ALLOWED_SILENT:
            continue
        if fp_cache.get(rel) is None:
            try:
                fp_cache[rel] = (PLAITA_DIR / rel).read_text(encoding="utf-8")
            except OSError:
                fp_cache[rel] = ""
        fp = _handler_fingerprint(fp_cache[rel], lineno)
        if fp and fp in fps:
            continue
        unlisted.append((rel, lineno))
    if unlisted:
        pytest.fail(
            "Found silent except blocks (no raise / no logging) not in ALLOWED_SILENT:\n"
            + "\n".join(f"  {rel}:{lineno}" for rel, lineno in unlisted)
            + "\nEither log the exception, re-raise it, or add to ALLOWED_SILENT "
            "with a justification comment."
        )
