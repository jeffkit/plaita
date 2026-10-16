#!/usr/bin/env python3
"""日志站点分类判定表（plaita#168）——提取 + 机械分类 + 判定表发射。

覆盖口径（与 #168 验收 grep 一致）::

    grep -rn "logger\\.\\(info\\|warning\\|error\\|debug\\)" plaita/ | wc -l

即接收器字面为 ``logger`` 的 debug/info/warning/error 调用行；grep 误命中
（注释/文档串里的提及）逐条显式标注 N/A，不允许遗漏不标。

四类判定（site → class），判据全部机械可判定（规则即代码，本文件就是规则表）：

  A 数据面允许   插值能追到流程功能数据：求值结果/消息载荷、流程实体标识
                 （execution_id / node 名 / flow_id / message_id / 队列键…）。
  B 结构面允许   插值是引擎结构事实：类型名、错误类名、计数、耗时、状态枚举、
                 布尔判定、引擎常量（含 env 变量**名**）、基础设施标量、对象句柄。
  C 必须掩       插值是异常文本（可夹带作者数据/凭据形状 ⇒ #88 族构造面清洗）、
                 凭据/URL 形状、环境变量**值**（env 投影）、评估器内部投影。
  D 应删除       整条日志无意义或与相邻日志重复（HEAD 全量清点 0 站点命中，
                 见 docs/log-site-taxonomy.md §D）。

N/A 非插值     常量消息（无插值，无数据流出口）或 grep 误命中。

用法::

    python scripts/log_site_taxonomy.py --emit     # 重算并写 docs/log-site-taxonomy.json
    python scripts/log_site_taxonomy.py --check    # 与已提交 JSON 逐字节比对（默认）
"""
from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

METHODS = ("debug", "info", "warning", "error")
HIT = re.compile(r"\blogger\.(info|warning|error|debug)\b")

# ---------------------------------------------------------------------------
# 站点级覆盖（site 行 + 表达式子串 → 类）：bare 名在具体站点有更精确来源时使用，
# 每条附理由；这是显式枚举，不是"看情况"。
# ---------------------------------------------------------------------------
OVERRIDES: dict[tuple[str, str], tuple[str, str]] = {
    ("plaita/core/expression_parser.py:2679", "_expression._clip(path)"): (
        "A", "表达式数据路径（$NODE 段路径，_clip 限长输出），非文件系统路径"),
    ("plaita/server/services/http_callback_service.py:412", "path"): (
        "A", "回调请求路径标识（定位回调实体）"),
    ("plaita/event/redis.py:2869", "task"): (
        "B", "asyncio Task 句柄 repr（引擎对象句柄，非队列载荷）"),
    ("plaita/event/redis.py:2883", "task"): (
        "B", "asyncio Task 句柄 repr（引擎对象句柄，非队列载荷）"),
    ("plaita/event/redis.py:2943", "task"): (
        "B", "asyncio Task 句柄 repr（引擎对象句柄，非队列载荷）"),
    ("plaita/core/parallel_executor.py:104", "entry[0]"): (
        "B", "fork 池缓存的 pid（消息模板 'cached pid %s'）"),
}

# ---------------------------------------------------------------------------
# C：必须掩（按序匹配，命中即停）
# ---------------------------------------------------------------------------
C_RULES = [
    (r"(?<![\w\"'])(?:e|exc|ex|err|error|exception)(?![\w\"'])",
     "异常对象/异常文本：str(e) 可夹带作者数据与凭据形状 ⇒ 必须 #88 族构造面清洗后进日志"),
    (r"_exception_message|_scrub_\w*\(",
     "异常文本取出/清洗 helper 的产物：保持构造面已掩约束"),
    (r"_mask_\w*|_REDACTED",
     "凭据形状值（已按 #88 族掩码）：进日志必须保持掩码形态"),
    (r"\burl\b|webhook|\bdsn\b|conninfo|userinfo|authorization",
     "URL/凭据载体形状（#88 族掩码对象）"),
    (r"\bpassword\b|\bpasswd\b|\bsecret\b|\btoken\b|api_key|credential",
     "凭据名形状"),
]
# 环境投影（值面）：插值变量在所在函数内被赋值自 os.environ/os.getenv ⇒ C。
# 判定见 _env_probe()。

# ---------------------------------------------------------------------------
# B：结构面允许
# ---------------------------------------------------------------------------
B_RULES = [
    (r"\bmetric\.", "指标值（计数/计量面）"),
    (r"\bfull\b", "指标全名（结构标签）"),
    (r"^len\(", "计数"),
    (r"count\b|_count\b|\battempts?\b|\bconsecutive_errors\b|\bmax_consecutive_errors\b"
     r"|\btotal\b|(?<!\w)n(?!\w)|\bactive\b|\bfree\b|\bmin_free\b|dropped\b|pending\b"
     r"|replayed\b|killed\b|\bmatching_keys\b|\bunknown_options\b|sorted\(",
     "计数/集合规模"),
    (r"_MAX\b|max_\w+|budget\b|\blimit\b|\bttl\b|_ttl\b|\bTTL\b|concurrency",
     "上限/预算/租约"),
    (r"timeout|seconds|interval|duration|elapsed|latency|delay\b|delay_|\bbackoff\b|\bdrain\b"
     r"|window_(?:start|end)|\bcron\b|next_run_at|trigger_timestamp|\bpartition\b|\boffset\b",
     "耗时/调度与分区元数据"),
    (r"type\(|__name__|__class__|_type_name|_exception_name|service_class|handler_method"
     r"|func_name|mod_path|\blineno\b|node_type",
     "类型名/函数名/位置元数据（非作者数据）"),
    (r"\bstatus\b|state_status|strategy\.value|queue_type|service_type|storage_type"
     r"|event_bus_type|subscription_storage_type|\bbackend\b|\bkind\b|\bcomponent\b"
     r"|\bsubtype\b|\bsignum\b|\bsig\b|auto_offset_reset|result_type|resume_type"
     r"|event_type|event_type_desc|trigger_kind|\bphase\b|\bcommand\b|\bwhat\b"
     r"|env_name|\bnamespace\b|\breason\b|\bresolved\b",
     "状态/枚举/组件选择器/环境变量名/引擎具名原因/解析结果（名字非值）"),
    (r"\bgraceful\b|\bsuccess\b|\bvalid\w*|enabled|\bexplicit\b|\bshadowed\b|\bdeleted\b"
     r"|\bcleared\b|signature|enqueue|\bfeature\b|last_rejected|\bauth_error\b"
     r"|\bdefault\b|exhausted\b",
     "布尔判定/具名拒绝原因/特性名/回退默认值"),
    (r"\b_?[A-Z][A-Z0-9_]{2,}\b", "引擎常量（数值/键名/env 变量名/具名原因串）"),
    (r"version|\bhash\b|stored_algo|stored_engine|current_engine|_hash\b",
     "版本/哈希元数据"),
    (r"\bport\b|_port\b|\bpid\b|_host\b|\bhost\b|bootstrap_servers|container_name"
     r"|\bcheckout\b|\broot\b|\bparent\b|\bpath\b|\bfuture\b|\bhandler\b|\bowner\b"
     r"|event_bus\b",
     "基础设施标量/文件系统路径/对象句柄"),
]

# ---------------------------------------------------------------------------
# A：数据面允许
# ---------------------------------------------------------------------------
A_RULES = [
    (r"execution_id|flow_id|node_id|task_id|message_id|\bmid\b|group_id|tenant_id"
     r"|approval_id|schedule_id|\bsub_id\b|subscription_id|handler_id|event_id"
     r"|correlation_id|instance_id|_id\b|\bid\b",
     "流程/运行实体标识（跨日志可回查）"),
    (r"node_name|node\.name|\bbranch\b|pb\.name|_node_label|next_node|\btarget\b"
     r"|\bname\b|\blabel\b",
     "节点/分支/流程实体名"),
    (r"queue_name|stream_key|queue_key|dlq_key|dead_key|stream_name|control_channel"
     r"|cache_key|lease_key|lock_key|scheduled_key|consumer_name|group_name"
     r"|_consumer_group|\btopic\b|key_str|dedup_key|channel_name|attempts_key"
     r"|\bcallback_path\b",
     "队列/存储/回调实体键（定位数据实体）"),
    (r"root_key|field_path|\bfield\b|\bkey\b|\bcycle\b",
     "数据键/字段名/节点环"),
    (r"\braw\b|raw\[|\bvalue\b|\bresult\b|\bresults\b|\brs\b|event_data|\bbody\b"
     r"|body\.get|message_data\.get|task\.body|values\.get|schedule\.get|schedule\["
     r"|approval_record|resolved_config|task_config|\bcondition\b|\.branches"
     r"|ast\.unparse|\bdata\b|\bmember\b|final_decision|event_filter|service_config"
     r"|\binstance\b",
     "数据面内容（求值结果/消息载荷/流程配置内容/决议值）"),
]

CLASS_ORDER = ("C", "A", "B")  # 站点级取最严：任一 C ⇒ C；否则含 A ⇒ A；否则 B


def _interp_exprs(src: str, call: ast.Call) -> list[tuple[str, str]]:
    """返回 [(kind, 源文本)] 插值表达式。"""

    def seg(n):
        return (ast.get_source_segment(src, n) or "?").strip()

    out: list[tuple[str, str]] = []
    if not call.args:
        return out
    msg = call.args[0]
    if isinstance(msg, ast.JoinedStr):
        for v in msg.values:
            if isinstance(v, ast.FormattedValue):
                out.append(("f-string", seg(v.value)))
    elif isinstance(msg, ast.BinOp) and isinstance(msg.op, ast.Mod):
        if isinstance(msg.right, ast.Tuple):
            for e in msg.right.elts:
                out.append(("%-tuple", seg(e)))
        else:
            out.append(("%-single", seg(msg.right)))
    elif (
        isinstance(msg, ast.Call)
        and isinstance(msg.func, ast.Attribute)
        and msg.func.attr == "format"
        and (msg.args or msg.keywords)
    ):
        for e in msg.args:
            out.append((".format", seg(e)))
        for kw in msg.keywords:
            out.append((".format", seg(kw.value)))
    elif isinstance(msg, ast.Constant) and len(call.args) > 1:
        for e in call.args[1:]:
            out.append(("%-lazy", seg(e)))
    return out


def _function_spans(tree: ast.Module) -> list[tuple[int, int, str]]:
    spans = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            spans.append((node.lineno, node.end_lineno or node.lineno, node.name))
    return spans


def _enclosing_function(spans, lineno: int) -> str:
    best = ""
    for lo, hi, name in spans:
        if lo <= lineno <= hi and name:
            best = name
    return best


def _env_probe(expr: str, func_src: str) -> bool:
    """expr 的基础名是否在所在函数内被赋值自环境读取。"""
    if not func_src:
        return False
    for m in re.finditer(r"[A-Za-z_][A-Za-z0-9_]*", expr):
        name = m.group(0)
        if name in ("os", "getenv", "environ"):
            continue
        pat = re.compile(
            r"\b" + re.escape(name) + r"\s*=[^\n]*"
            r"(?:os\.environ|os\.getenv|\benviron\.get\b|\bgetenv\()"
        )
        if pat.search(func_src):
            return True
    return False


def classify_expr(site: str, expr: str, func_src: str) -> tuple[str, str]:
    key = (site, expr)
    if key in OVERRIDES:
        return OVERRIDES[key]
    for pat, why in C_RULES:
        if re.search(pat, expr):
            return "C", why
    if _env_probe(expr, func_src):
        return "C", "环境投影值：插值变量在所在函数内赋值自 os.environ/os.getenv"
    for pat, why in B_RULES:
        if re.search(pat, expr):
            return "B", why
    for pat, why in A_RULES:
        if re.search(pat, expr):
            return "A", why
    raise SystemExit(f"[log-site-taxonomy] 未匹配判据的插值表达式：{site} :: {expr!r}"
                     "——判据表必须扩展后才能重算（禁止'看情况'）")


def build_rows(root: Path) -> dict:
    pkg = root / "plaita"
    # pass 1: 验收 grep 同口径文本命中
    hits: dict[tuple[str, int, str], int] = {}
    for py in sorted(pkg.rglob("*.py")):
        rel = py.relative_to(root).as_posix()
        for i, ln in enumerate(py.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            m = HIT.search(ln)
            if m:
                hits[(rel, i, m.group(1))] = hits.get((rel, i, m.group(1)), 0) + 1
    # pass 2: 每文件一次解析，AST 关联调用
    parsed: dict[str, tuple[str, list[ast.Call], list[tuple[int, int, str]]]] = {}
    for py in sorted(pkg.rglob("*.py")):
        rel = py.relative_to(root).as_posix()
        src = py.read_text(encoding="utf-8", errors="replace")
        try:
            tree = ast.parse(src)
        except SyntaxError:
            continue
        calls = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in METHODS
                and isinstance(node.func.value, (ast.Name, ast.Attribute))
            ):
                recv = (ast.get_source_segment(src, node.func.value) or "").rstrip()
                if recv.endswith("logger"):
                    calls.append(node)
        if calls:
            parsed[rel] = (src, calls, _function_spans(tree))

    rows = []
    n_assoc = n_plain = n_interp = 0
    for (rel, lineno, method), _ in sorted(hits.items()):
        site = f"{rel}:{lineno}"
        if rel not in parsed:
            rows.append({"site": site, "method": method, "interp": False,
                         "class": "N/A",
                         "rationale": "grep 误命中：注释/文档串内提及，非调用点（文件含非 Python 模板语法）"})
            continue
        src, calls, spans = parsed[rel]
        found = None
        for node in calls:
            if node.func.attr != method:
                continue
            lo = min(node.lineno, node.end_lineno or node.lineno)
            hi = max(node.lineno, node.end_lineno or node.lineno)
            if lo <= lineno <= hi:
                found = node
                break
        if found is None:
            rows.append({"site": site, "method": method, "interp": False,
                         "class": "N/A",
                         "rationale": "grep 误命中：注释/文档串内提及，非调用点"})
            continue
        n_assoc += 1
        exprs = _interp_exprs(src, found)
        if not exprs:
            n_plain += 1
            rows.append({"site": site, "method": method, "interp": False,
                         "class": "N/A",
                         "rationale": "常量消息，无插值 ⇒ 无数据流出口"})
            continue
        n_interp += 1
        func_src = ""
        fname = _enclosing_function(spans, lineno)
        if fname:
            lo, hi, _ = next(s for s in spans if s[2] == fname and s[0] <= lineno <= s[1])
            func_src = "\n".join(src.splitlines()[lo - 1 : hi])
        judged = [ (e,) + classify_expr(site, e, func_src) for _k, e in exprs ]
        site_cls = next((c for c in CLASS_ORDER if any(j[1] == c for j in judged)), "B")
        rationale = "; ".join(f"{e} ⇒ {c}（{w}）" for e, c, w in judged)
        rows.append({"site": site, "method": method, "interp": True,
                     "class": site_cls, "exprs": [j[0] for j in judged],
                     "rationale": rationale})
    from collections import Counter
    dist = Counter(r["class"] for r in rows)
    meta = {
        "issue": "plaita#168",
        "coverage_grep": 'grep -rn "logger\\.(info|warning|error|debug)" plaita/',
        "counts": {
            "grep_hit_lines": sum(hits.values()),
            "ast_associated_calls": n_assoc,
            "interpolated_sites": n_interp,
            "constant_message_sites": n_plain,
            "grep_false_hits": sum(hits.values()) - n_assoc,
        },
        "class_distribution": dict(sorted(dist.items())),
        "note": "site = 文件:行（验收 grep 命中行）；class ∈ A/B/C/D/N/A；"
                "判据=docs/log-site-taxonomy.md §3 与 scripts/log_site_taxonomy.py 规则表（一一对应）",
    }
    return {"meta": meta, "sites": rows}


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    data = build_rows(root)
    out = root / "docs" / "log-site-taxonomy.json"
    if "--emit" in sys.argv:
        out.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        print(f"emitted {out} ({len(data['sites'])} rows)")
        return 0
    committed = json.loads(out.read_text(encoding="utf-8"))
    if committed == data:
        print(f"✓ 判定表与代码重算一致（{len(data['sites'])} 行）")
        return 0
    c_sites = {r["site"] for r in committed["sites"]}
    n_sites = {r["site"] for r in data["sites"]}
    for s in sorted(n_sites - c_sites)[:20]:
        print(f"  ✗ 代码新出现、判定表缺行: {s}")
    for s in sorted(c_sites - n_sites)[:20]:
        print(f"  ✗ 判定表有行、代码已不存在: {s}")
    print("  ✗ 判定表与重算结果不一致（逐字节比对失败）")
    return 1


if __name__ == "__main__":
    sys.exit(main())
