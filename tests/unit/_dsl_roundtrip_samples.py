"""plaita#238：DSL 往返 conformance 的**采样构建模块**（非测试模块，``_`` 前缀）。

被 :mod:`tests.unit.test_dsl_roundtrip_conformance` 消费，职责：

1. **harvest**：扫 ``tests/**/*.py`` 里现有的 IR 片段（``"type": "X"`` 的节点
   dict），按 type 去重合并字段（只收**节点模型声明过**的字段，camel/snake
   双拼写互斥），得到每类型一份「字段最全」底样；
2. **补齐**：harvest 覆盖不到的字段/类型，按节点 pydantic 模型声明（registry
   内省，即 ``plaita/node/<type>.py`` 与 plaita-nodes 各文件的权威声明）构造
   合法值；结构复杂的字段（childFlow / branches / condition / errorHandler /
   主机策略等）用本模块内的显式种子表；
3. **样本 flow**：包成 ``start → 样本节点 → 存根目标 → end`` 的完整可解析 IR；
4. **三表示出口**：
   - *sexpr 链*：``flow_to_sexpr``（IR→文本）→ ``compile_sexpr``（文本→IR）；
   - *模型链*：``plaita.core.flow.parse``（dict→Flow）；Flow 没有直接的 IR
     出口，其唯一序列化面是 ``model_dump``（snake_case、烘默认值、丢 ``type``
     判别键），此处按 #200/#207 立下的 camel/snake 双读口径反推 IR
     （``exclude_unset`` 只留显式声明的字段 + 节点 ``type`` 对象身份回填）；
   - *e2e 链*：IR→sexpr 文本→``parse_sexpr``→Flow→IR（写侧 + 读侧 + 模型层
     全程）。
5. **规范化整树 diff**（判据口径 = ``test_issue146_sexpr_mock_roundtrip`` 注释：
   「compile_sexpr 的往返比较是精确的整树相等」）：返回丢失面 path 列表，
   供测试侧做「登记式 xfail + 白名单计数」断言。diff 只覆盖**样本节点**与
   flow 级字段——包装用的 start/存根/end 不在比对面内。

本模块**不做任何断言**、不改产品代码；所有构造值都是合法 IR 值（构造失败
的字段会被定点丢弃并记录，见 :data:`drop_log`）。
"""

from __future__ import annotations

import ast
import re
import warnings
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ValidationError

from plaita.core.flow import Flow, parse
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr, parse_sexpr
from plaita.node import get_default_registry

# ---------------------------------------------------------------------------
# registry 快照
# ---------------------------------------------------------------------------

REG = get_default_registry()


def _is_product_node_module(module: str) -> bool:
    """节点类所属模块是否属于**产品面**（测试态与 examples 演示面之外）。

    ``examples.agent.demo`` 在 import 时（e2e 模块收集期）就向进程级
    default registry 注册 ``llm``/``tool``/``retrieve``。它们不是产品插件，
    且会让扫掠集随 pytest 收集顺序漂移：单跑 conformance 时不在集合里，
    全量会话（e2e 先于 unit）却多出 llm.model 10 条 + retrieve/tool 4 条
    丢失面——pre-fix 基线同样红（plaita#241 落地时定位）。产品插件（
    plaita-nodes 等，模块名 ``plaita*``）仍保留。
    """
    if module in ("tests", "examples"):
        return False
    return not module.startswith(("tests.", "test_", "examples."))


def _is_product_node_type(ntype: str) -> bool:
    """默认 registry 里属于**产品面**的节点类型。

    ``get_default_registry()`` 是进程级单例：同一次 pytest 会话里，别的测试
    模块会在 import / fixture 期注册临时节点（``codeflow_test_*`` /
    ``issue*_probe``），并可能把默认不注册的 ``code`` 临时注册进来。扫掠对象
    必须排除这些测试态类型与 examples 演示节点，否则测试顺序一变（分块 CI /
    全量单进程）就出现扫掠集漂移的假红。产品插件（plaita-nodes 等）的类定义
    在 ``plaita*`` 模块下，保留。
    """
    if ntype == "code":
        # CodeNode 在 plaita 里**刻意默认不注册**（执行任意代码需显式 opt-in），
        # 测试注册的 code 不是产品默认面。
        return False
    cls = REG.get(ntype)
    module = str(getattr(cls, "__module__", "")) if cls is not None else ""
    return _is_product_node_module(module)


#: 产品面节点类型（本单的扫掠对象；随已装插件集变化）。
NODE_TYPES: List[str] = sorted(
    t for t in REG.list_types() if _is_product_node_type(t))
CLS2TYPE = {REG.get(t): t for t in NODE_TYPES}

#: 伪样本类型：只测 flow 级字段（#147/#149 的面），不带业务节点。
FLOW_HEADER_TYPE = "flow:header"

# ---------------------------------------------------------------------------
# 规范化：snake → camel（#200/#207 的双读口径），再剔除「缺席 ≡ 默认值」
# ---------------------------------------------------------------------------

_SNAKE_TO_CAMEL = {
    # flow 级
    "flow_id": "flowId",
    "input_type": "inputType",
    "output_type": "outputType",
    "global_context": "globalContext",
    "expose_env": "exposeEnv",
    # 节点公共
    "source_line": "sourceLine",
    "error_handler": "errorHandler",
    "timeout_handler": "timeoutHandler",
    "result_type": "resultType",
    # Property（plaita/io.py）
    "data_type": "dataType",
    "is_required": "isRequired",
    "default_value": "defaultValue",
    "item_type": "itemType",
    # 分支 / 条件
    "is_default": "isDefault",
    "else_next": "elseNext",
    # 集合家族
    "child_flow": "childFlow",
    "max_concurrent": "maxConcurrent",
    "max_iterations": "maxIterations",
    # 事件家族
    "event_type": "eventType",
    "event_filter": "eventFilter",
    "subscription_timeout": "subscriptionTimeout",
    # assignment
    "upstream_output": "upstreamOutput",
    # parallel
    "is_conditional": "isConditional",
    "join_branches": "joinBranches",
    # reference
    "flow_version": "flowVersion",
    # http 访问策略（#200 双读家族）
    "allowed_hosts": "allowedHosts",
    "denied_hosts": "deniedHosts",
    "block_private_networks": "blockPrivateNetworks",
    "request_timeout": "requestTimeout",
    "max_redirects": "maxRedirects",
    # code 后端（#220-L3）
    "sandbox_backend": "sandboxBackend",
    # errorHandler 内层（sexpr IR 拼写 = errorCode/errorMessage）
    "retry_times": "retryTimes",
    "error_code": "errorCode",
    "error_message": "errorMessage",
}


def _norm_key(key: str) -> str:
    if key == "flowID":  # reference 节点 LEGACY 拼写（plaita/node/child.py）
        return "flowId"
    return _SNAKE_TO_CAMEL.get(key, key)


def norm(value: Any) -> Any:
    """规范化树：snake 键 → camel 键；剔除 None / 空容器（IR 约定「缺席 ≡ 默认」）。

    注意**不**剔除 ``False`` / ``0`` / ``""``——与 #146 的口径一致，它们是数据。
    """
    if isinstance(value, dict):
        out: Dict[Any, Any] = {}
        for key, val in value.items():
            if val is None:
                continue
            if isinstance(val, (list, tuple)) and not val:
                continue
            if isinstance(val, dict) and not val:
                continue
            if isinstance(val, Enum):
                val = val.value
            out[_norm_key(str(key))] = norm(val)
        return out
    if isinstance(value, (list, tuple)):
        return [norm(v) for v in value]
    if isinstance(value, Enum):
        return value.value
    return value


# ---------------------------------------------------------------------------
# Flow → IR 反推出口（``exclude_unset`` + ``type`` 回填 + snake→camel）
# ---------------------------------------------------------------------------


def _typed(value: Any, dump: Any) -> Any:
    """沿 (原始值, json dump) 平行树走，把 Node/Flow 节点的 ``type`` 判别键回填。

    ``model_dump`` 不输出 ``type``（它不是字段，是 ClassVar）；console 正典当初
    「从 model_dump 字段形状反推节点类型」出过真实事故（见
    ``plaita/dsl/codeflow/_canonical.py`` 模块注释），这里用**对象身份**回填，
    不做形状反推。
    """
    if isinstance(value, Flow):
        out = dict(dump or {})
        children = list(value.nodes or [])
        dumped = (dump or {}).get("nodes") or []
        out["nodes"] = [_typed(node, dn) for node, dn in zip(children, dumped)]
        return out
    if isinstance(value, BaseModel) and not isinstance(value, Flow):
        out = dict(dump or {})
        cls = type(value)
        if cls in CLS2TYPE:
            out["type"] = CLS2TYPE[cls]
        for name in cls.model_fields:
            if name in out:
                out[name] = _typed(getattr(value, name), out[name])
        return out
    if isinstance(value, dict):
        return {key: _typed(val, (dump or {}).get(key)) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        seq = dump if isinstance(dump, (list, tuple)) else []
        return [_typed(v, dv) for v, dv in zip(value, seq)]
    return dump


def flow_to_ir(flow: Flow) -> Dict[str, Any]:
    """Flow 的 IR 出口：``model_dump(exclude_unset, mode=json)`` + type 回填 + camel 化。

    ``exclude_unset`` 是关键——把「模型烘进来的默认值」（``timeout=''`` /
    ``timeout_handler=ErrorHandler()`` …）与「IR 里显式声明的字段」区分开；
    ``mode="json"`` 把 ``ErrorStrategy`` 等 enum 摊平成字符串。
    """
    dump = flow.model_dump(exclude_unset=True, mode="json")
    return norm(_typed(flow, dump))


# ---------------------------------------------------------------------------
# 整树 diff：src 有而 res 无（或值不同）⇒ 丢失面
# ---------------------------------------------------------------------------

_INDEX_RE = re.compile(r"\[\d+\]")


def _tree_faces(src: Any, res: Any, path: str, faces: List[str]) -> None:
    if isinstance(src, dict):
        if not isinstance(res, dict):
            faces.append(_INDEX_RE.sub("[]", path) if path else "<root>")
            return
        for key, sv in src.items():
            sub = f"{path}.{key}" if path else str(key)
            if key not in res:
                faces.append(_INDEX_RE.sub("[]", sub))
            else:
                _tree_faces(sv, res[key], sub, faces)
        return
    if isinstance(src, list):
        if not isinstance(res, list) or len(res) != len(src):
            faces.append(f"{_INDEX_RE.sub('[]', path)}<len>" if path else "<len>")
        seq = res if isinstance(res, list) else []
        for i, sv in enumerate(src):
            sub = f"{path}[{i}]"
            if i >= len(seq):
                faces.append(_INDEX_RE.sub("[]", sub))
                continue
            if isinstance(sv, dict) and "type" in sv:
                _tree_faces(sv, seq[i], f"{path}[{sv['type']}]", faces)
            else:
                _tree_faces(sv, seq[i], sub, faces)
        return
    if src != res:
        faces.append(_INDEX_RE.sub("[]", path) if path else "<value>")


def sample_losses(ir: Dict[str, Any], back: Dict[str, Any]) -> List[str]:
    """样本级 diff：**只比对样本节点**（id=``sample``）与 flow 级字段。

    - 节点面：``<type>.<field>…``（样本节点 type 冠首；childFlow 内部继续下钻，
      如 ``map.childFlow[end].output``）；
    - flow 级面：``flow.<field>``。

    包装用的 start / 存根 mock / end 是脚手架，不在比对面内。
    """
    faces: List[str] = []
    src_node = next((n for n in ir.get("nodes") or [] if n.get("id") == "sample"), {})
    back_node = next((n for n in back.get("nodes") or [] if n.get("id") == "sample"), None)
    label = src_node.get("type", "sample")
    if back_node is None:
        faces.append(f"{label}.<node-missing>")
    else:
        _tree_faces(src_node, back_node, label, faces)
    flow_src = {k: v for k, v in ir.items() if k != "nodes"}
    flow_back = {k: v for k, v in back.items() if k != "nodes"}
    _tree_faces(norm(flow_src), norm(flow_back), "flow", faces)
    return sorted(set(faces))


# ---------------------------------------------------------------------------
# harvest：tests/**/*.py 里的 IR 节点片段
# ---------------------------------------------------------------------------

_TYPE_RE = re.compile(r'["\']type["\']\s*:\s*["\']([a-z_]+)["\']')


def _enclosing_dict(text: str, pos: int) -> Optional[Any]:
    """``pos`` 处向前找最近的 ``{``，做括号平衡截取并 ``ast.literal_eval``。"""
    start = text.rfind("{", 0, pos)
    if start < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    quote = ""
    i = start
    end = None
    while i < len(text):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
        elif ch in "\"'":
            in_str = True
            quote = ch
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
        i += 1
    if end is None:
        return None
    try:
        return ast.literal_eval(text[start:end])
    except Exception:
        return None


def harvest_node_dicts(tests_dir: Optional[Path] = None) -> Dict[str, List[Dict[str, Any]]]:
    """``tests/**/*.py`` → ``{type: [node_dict, …]}``（只收注册表认识的 type）。"""
    root = tests_dir or Path(__file__).resolve().parents[1]
    found: Dict[str, List[Dict[str, Any]]] = {}
    for path in sorted(root.rglob("*.py")):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for m in _TYPE_RE.finditer(text):
            ntype = m.group(1)
            if ntype not in NODE_TYPES:
                continue
            d = _enclosing_dict(text, m.start())
            if isinstance(d, dict) and d.get("type") == ntype:
                found.setdefault(ntype, []).append(d)
    return found


# ---------------------------------------------------------------------------
# 显式结构种子（harvest 覆盖不到的字段；值取自节点声明的合法域）
# ---------------------------------------------------------------------------

_CHILD = {
    "runtime": "python",
    "flow_id": "child-1",
    "nodes": [
        {"type": "start", "id": "cs", "next": "ce"},
        {"type": "end", "id": "ce", "output": "$INPUT.item", "resultType": "success"},
    ],
}

_CHILD_HEADER = {
    "runtime": "python",
    "flow_id": "child-h",
    "inputType": {"dataType": "object"},
    "outputType": {"dataType": "string"},
    "desc": "child header",
    "version": "3",
    "nodes": [
        {"type": "start", "id": "hs", "next": "he"},
        {"type": "end", "id": "he", "output": "$INPUT.item", "resultType": "success"},
    ],
}

_COND = {"field": "$INPUT.n", "operator": "gt", "value": 1}

#: errorHandler 的 IR 全形状（``_compile_error_handler`` / ``_error_handler_to_src``
#: 的读写口径：strategy/retryTimes/defaultValue/errorCode/errorMessage）。
_FULL_ERROR_HANDLER = {
    "strategy": "continue-with",
    "retryTimes": 2,
    "defaultValue": "fb",
    "errorCode": 7,
    "errorMessage": "m",
}

_TIMEOUT_HANDLER = {"strategy": "continue", "errorCode": 9, "errorMessage": "slow"}

#: 公共节点字段（全部类型共用；sexpr 写侧只发 timeout/on-error，其余是预期
#: 会暴露的丢失面）。
_COMMON_NODE_SEED = {
    "name": "样本节点",
    "desc": "#238 conformance 样本",
    # "PT3S" 而非 "3000"：数字样字符串 timeout 会被 sexpr 写侧裸原子化成
    # int、再被模型链硬拒（真实丢失面，由专项 xfail 用例钉住，见测试文件），
    # 不放进全量样本以免 e2e 链整体塌成 <type-gap>。
    "timeout": "PT3S",
    "output": "$INPUT.pick",
    "errorHandler": _FULL_ERROR_HANDLER,
    "timeoutHandler": _TIMEOUT_HANDLER,
}

#: 每类型的自有字段种子。dict-IR 用 camel 拼写（console 正典 / 写侧口径）；
#: 业务节点（plaita-nodes / server）的字段声明即 snake，保持 snake。
_TYPE_SEEDS: Dict[str, Dict[str, Any]] = {
    "start": {},
    "end": {"resultType": "error", "error": {"code": 4001, "message": "bad"},
            "output": "$INPUT.pick", "next": "after"},
    "assignment": {
        "output": "$INPUT.a",
        "outputType": {"dataType": "string"},
        "upstreamOutput": [{"upstream": "s0", "value": "$INPUT.x"}],
    },
    "if": {"condition": _COND, "elseNext": "alt"},
    "switch": {
        "branches": [
            {"name": "big", "next": "after", "priority": 5,
             "condition": _COND, "label": "大值", "desc": "bd"},
            {"name": "small", "next": "alt", "priority": 1, "isDefault": True},
        ],
        "outputType": {"dataType": "string"},
    },
    "case": {
        "target": "$INPUT.k",
        "cases": [{"value": "a", "id": "after", "name": "A1"},
                  {"value": "b", "id": "alt"}],
        "default": "alt",
        "outputType": {"dataType": "string"},
    },
    "loop": {"collection": "$INPUT.items", "childFlow": _CHILD,
             "input": {"item": "$INPUT.item"},
             "itemType": {"dataType": "string"}, "condition": _COND},
    "map": {"collection": "$INPUT.items", "childFlow": _CHILD,
            "input": {"item": "$INPUT.item"},
            "itemType": {"dataType": "string"},
            "concurrent": True, "maxConcurrent": 4},
    "filter": {"collection": "$INPUT.items", "childFlow": _CHILD,
               "input": {"item": "$INPUT.item"},
               "itemType": {"dataType": "string"}},
    "find": {"collection": "$INPUT.items", "childFlow": _CHILD,
             "input": {"item": "$INPUT.item"},
             "itemType": {"dataType": "string"}},
    "reduce": {"collection": "$INPUT.items", "childFlow": _CHILD,
               "input": {"item": "$INPUT.item"},
               "itemType": {"dataType": "string"}, "initial": 0},
    "child": {"input": {"k": "$INPUT.k"}, "childFlow": _CHILD_HEADER},
    "reference": {"input": {"k": "$INPUT.k"}, "childFlow": _CHILD_HEADER,
                  "flowID": "ref-1", "flowVersion": "v2"},
    "parallel": {
        "branches": [
            {"name": "a", "flow": _CHILD, "input": "$INPUT.x",
             "condition": {"field": "$INPUT.flag", "operator": "truthy",
                           "value": True}},
            {"name": "b", "flow": _CHILD},
        ],
        "mode": "thread", "joinBranches": ["a"], "isConditional": True,
    },
    "http": {
        "method": "GET", "url": "https://example.com",
        "headers": {"X-Tag": "t"}, "body": {"a": 1},
        "input": {"h": "$INPUT.h"}, "output": "$INPUT.pick",
        "query": {"q": "1"}, "addressing": {"name": "domain"},
        "delegate": {"name": "proxy", "params": {"p": 1}},
        "allowedHosts": ["example.com"], "deniedHosts": ["*.internal"],
        "blockPrivateNetworks": True, "requestTimeout": 3.5, "maxRedirects": 2,
        "content_type": "application/json",
    },
    "mock": {"value": {"a": 1, "b": "$INPUT.x"}, "output": "$NODE.m"},
    "event": {"eventType": "approved", "eventFilter": {"kind": "x"},
              "subscriptionTimeout": 2.5},
    # ---- plaita-nodes / server 业务节点（本仓 tests 里几乎无 IR 语料，全构造）----
    "agentrun": {"agent": "coder", "prompt": "do it", "repo": "org/repo",
                 "workspace": "/tmp/ws", "session": "s1", "timeout_secs": 60,
                 "details": True, "dry_run": True},
    "api_request": {"credential": "cred-main", "method": "GET", "path": "/v1/x",
                    "query": {"q": "1"}, "body": {"b": 1}, "timeout_ms": 3000,
                    "raise_on_error": True, "dry_run": True},
    "approval": {"event_type": "approval.requested", "event_filter": {"kind": "x"},
                 "subscription_timeout": 30.0, "approval_title": "T",
                 "approval_content": "C", "approval_type": "manual",
                 "approvers": ["u1"], "approval_strategy": "any",
                 "auto_escalation": True, "escalation_timeout_hours": 24,
                 "escalation_approvers": ["u2"],
                 "form_fields": [{"name": "reason", "type": "string"}],
                 "allow_comments": True, "require_comments": False,
                 "notification_config": {"channel": "email"}},
    "capture": {"command": "echo hi", "cwd": "/tmp", "timeout_secs": 5,
                "env": {"E": "1"}, "stdin": "in", "dry_run": True},
    "decision": {"question": "q?", "choices": ["a", "b"],
                 "input": {"i": "$INPUT.i"}, "items": [1, 2],
                 "provider": "openai", "api_base": "https://api.example.com",
                 "api_key": "sk-x", "model": "m", "extra_body": {"t": 0.5},
                 "timeout_secs": 30, "min_confidence": 0.6,
                 "on_low_confidence": "default", "default_choice": "a",
                 "dry_run": True},
    "delay": {"event_type": "timer.done", "event_filter": {"kind": "t"},
              "subscription_timeout": 1.5, "delay_seconds": 3,
              "delay_unit": "seconds"},
    "dingtalk_webhook": {"credential": "cred-main", "text": "hello",
                         "dry_run": True},
    "email_send": {"credential": "cred-main", "to": "a@example.com",
                   "subject": "s", "body": "b", "body_is_html": True,
                   "dry_run": True},
    "feishu_webhook": {"credential": "cred-main", "text": "hello",
                       "dry_run": True},
    "gate": {"command": "make test", "gate_name": "ci", "cwd": "/tmp",
             "timeout_secs": 60, "max_retries": 2, "dry_run": True},
    "generic_webhook": {"credential": "cred-main", "payload": {"p": 1},
                        "timeout_ms": 2000, "dry_run": True},
    "git_publish": {"worktree_dir": "/tmp/wt", "branch_name": "b",
                    "commit_message": "m", "plan_file": "p.md",
                    "issue_number": 238, "merge_mode": "merge",
                    "main_clone": "/tmp/clone", "base_branch": "main",
                    "dry_run": True, "timeout_secs": 60},
    "github_comment": {"repo": "org/repo", "issue_number": 1, "text": "hi",
                       "redact": True, "artifact_dir": "/tmp/a",
                       "dedup_marker": "dm", "footer": "f", "dry_run": True,
                       "timeout_secs": 30},
    "hitl": {"message": "go?", "images": ["https://x.example.com/y.png"],
             "base_url": "https://hitl.example.com", "timeout_secs": 30,
             "poll_interval": 1.5, "wait_reply": True, "dry_run": True},
    "hitl_await": {"message": "go?", "images": [],
                   "base_url": "https://hitl.example.com", "timeout_secs": 30,
                   "event_type": "hitl.reply", "event_filter": {"kind": "h"},
                   "dry_run": True},
    "http_callback": {"event_type": "cb", "event_filter": {"kind": "c"},
                      "subscription_timeout": 60.0, "callback_path": "/cb",
                      "callback_method": "POST", "callback_timeout_minutes": 10,
                      "require_auth": True, "auth_type": "token",
                      "auth_token": "tok", "validate_headers": {"h": "v"},
                      "validate_params": {"p": "v"},
                      "success_response": {"s": 1}, "error_response": {"e": 1}},
    "kafka_queue": {"event_type": "kmsg", "event_filter": {"kind": "k"},
                    "subscription_timeout": 5.0,
                    "bootstrap_servers": "b.example.com:9092",
                    "security_protocol": "PLAINTEXT", "sasl_mechanism": "PLAIN",
                    "sasl_username": "u", "sasl_password": "p", "topic": "t",
                    "partition": 0, "group_id": "g",
                    "auto_offset_reset": "earliest", "enable_auto_commit": True,
                    "max_poll_records": 10, "message_format": "json",
                    "timeout_ms": 5000},
    "llm": {"messages": [{"role": "user", "content": "hi"}], "prompt": "hi",
            "system": "sys", "provider": "openai",
            "api_base": "https://api.example.com", "api_key": "sk-x",
            "model": "m", "temperature": 0.7, "max_tokens": 128,
            "json_mode": True, "extra_body": {"t": 1}, "timeout_secs": 30,
            "dry_run": True},
    "notify": {"message": "m", "channel": "email", "credential": "cred-main",
               "dry_run": True},
    "parse_json": {"text": "$INPUT.raw", "choices": ["data"],
                   "default": {"d": 1}, "join_fields": ["a", "b"]},
    "rate_limit": {"action": "allow", "key": "k", "daily": 100, "weekly": 500,
                   "store_dir": "/tmp/rl"},
    "redis_queue": {"event_type": "rq", "event_filter": {"kind": "r"},
                    "subscription_timeout": 5.0, "redis_host": "localhost",
                    "redis_port": 6379, "redis_db": 0, "redis_password": "p",
                    "queue_name": "q", "queue_type": "list",
                    "timeout_seconds": 5, "batch_size": 10,
                    "message_format": "json"},
    "report": {"action": "append", "entry": {"e": 1}, "repo": "org/repo",
               "base_dir": "/tmp", "token": "t"},
    "slack_webhook": {"credential": "cred-main", "text": "hello",
                      "dry_run": True},
    "sql_query": {"credential": "cred-main", "query": "SELECT 1",
                  "params": {"p": 1}, "dry_run": True, "max_rows": 100,
                  "max_bytes": 10000},
    "wecom_webhook": {"credential": "cred-main", "text": "hello",
                      "dry_run": True},
    "while": {"condition": _COND, "childFlow": _CHILD,
              "input": {"i": "$INPUT.i"}, "maxIterations": 5},
    "writefile": {"path": "/tmp/out.txt", "content": "data", "json_mode": True,
                  "indent": 2, "workspace_root": "/tmp/ws"},
}

#: 模型会**派生/改写**的结构字段——喂给它反而制造伪丢失面（语义变形，不是
#: 字段丢失），故这些类型不收 harvest / 泛填的该字段。
_TYPE_EXCLUDE = {
    "if": {"branches"},   # If 把 condition/else_next 重写成 branches[0]
    "case": {"branches"},  # Case 的分支面是 cases/default
}

#: flow 级字段种子（#147/#149 的面；单独作为 FLOW_HEADER_TYPE 样本）。
_FLOW_HEADER_SEED = {
    "inputType": {"dataType": "object",
                  "children": {"name": {"dataType": "string"}}},
    "outputType": {"dataType": "string"},
    "desc": "#238 flow 级样本",
    "version": "2",
    "author": "agent",
    "timeout": "PT10S",
    "globalContext": {"g": 1},
    "metadata": {"m": 1},
}

#: 不参与字段填充的声明字段（id/next 由包装器管理；source_line 是 @flow 前端
#: 专属；timeout_handler/error_handler 由显式种子管理）。
_SKIP_FILL_FIELDS = {
    "id", "next", "source_line", "timeout_handler", "error_handler",
    "timeout", "name", "desc", "output",
}

_STR_HINTS = {
    "method": "GET", "credential": "cred-main", "url": "https://example.com",
    "path": "/v1/x", "command": "echo hi", "cwd": "/tmp",
}


def _fill_str(name: str) -> str:
    return _STR_HINTS.get(name, f"v-{name}")


def _fill_value(annotation: Any, name: str, depth: int = 0) -> Any:
    """按类型注解构造一个合法、非退化的值（避免 None / 空容器）。"""
    import typing as _t

    if depth > 4:
        return "$INPUT.v"
    if annotation is None or annotation is type(None):
        return "$INPUT.v"
    origin = _t.get_origin(annotation)
    args = _t.get_args(annotation)
    if origin is _t.Union:
        for a in args:
            if a is type(None):
                continue
            v = _fill_value(a, name, depth + 1)
            if v is not None:
                return v
        return "$INPUT.v"
    if origin is _t.Literal:
        return args[0] if args else "$INPUT.v"
    if origin in (list, tuple):
        item_args = args or (Any,)
        item = _fill_value(item_args[0], name, depth + 1)
        return [item if item is not None else {"k": "$INPUT.v"}]
    if origin in (dict, dict):
        return {"k": "$INPUT.v"}
    if annotation is bool:
        return True
    if annotation is int:
        return 3
    if annotation is float:
        return 1.5
    if annotation is str:
        return _fill_str(name)
    if isinstance(annotation, type):
        if issubclass(annotation, Enum):
            members = list(annotation)
            return members[0].value if members else "$INPUT.v"
        if issubclass(annotation, BaseModel):
            # 嵌套模型（Condition / ErrorHandler / Property…）一律走显式种子，
            # 泛填不猜内部形状。
            return "$INPUT.v"
    return "$INPUT.v"


# ---------------------------------------------------------------------------
# 字段面过滤：只收节点模型声明过的字段（含 LEGACY 拼写），双拼写互斥
# ---------------------------------------------------------------------------


def _declared_surface(cls: Optional[type]) -> Tuple[set, set]:
    """返回 (声明的规范字段名集合, LEGACY/camel 接受名集合)。"""
    if cls is None:
        return set(), set()
    declared = set(cls.model_fields)
    legacy = set(getattr(cls, "LEGACY_KEYS", frozenset()) or ())
    return declared, legacy


def _key_acceptable(key: str, declared: set, legacy: set) -> bool:
    """harvest 键是否落在节点声明面上（snake 本名 / camel 双读拼写 / LEGACY）。"""
    if key in declared or key in legacy:
        return True
    return _norm_key(key) in declared


def _twin_present(key: str, node: Dict[str, Any]) -> bool:
    """同字段的双拼写（snake/camel/LEGACY）是否已有键在场。"""
    target = _norm_key(key)
    return any(_norm_key(k) == target for k in node)


def _drop_twins(key: str, node: Dict[str, Any]) -> None:
    target = _norm_key(key)
    for k in [k for k in node if _norm_key(k) == target]:
        del node[k]


# ---------------------------------------------------------------------------
# 样本 flow 组装（含失败字段的定点收敛）
# ---------------------------------------------------------------------------

_WRAP_START = {"type": "start", "id": "s0", "next": "sample"}
_WRAP_END = {"type": "end", "id": "the_end"}


def _referenced_targets(node: Dict[str, Any]) -> List[str]:
    """样本节点引用到的后继 id（包装器要为它们准备可到达的存根节点）。"""
    refs: List[str] = []
    for key in ("next", "elseNext", "else_next", "default"):
        if isinstance(node.get(key), str):
            refs.append(node[key])
    for branch in node.get("branches") or []:
        if isinstance(branch, dict):
            for key in ("next", "name"):
                if isinstance(branch.get(key), str):
                    refs.append(branch[key])
    for c in node.get("cases") or []:
        if isinstance(c, dict) and isinstance(c.get("id"), str):
            refs.append(c["id"])
    return sorted({r for r in refs if r and r not in ("s0", "the_end", "sample")})


def _wrapper_ir(ntype: str, node: Dict[str, Any],
                header: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """start → 样本节点 → 存根链 → end；存根串成链，保证从 start 可达
    （``parse_sexpr`` 的静态校验会拒不可达节点）。"""
    targets = _referenced_targets(node)
    chain = [dict(node)]
    for i, t in enumerate(targets):
        nxt = targets[i + 1] if i + 1 < len(targets) else "the_end"
        chain.append({"type": "mock", "id": t, "next": nxt})
    ir: Dict[str, Any] = {
        "runtime": "python",
        "flow_id": f"t238-{ntype.replace(':', '-')}",
        "nodes": [dict(_WRAP_START)] + chain + [dict(_WRAP_END)],
    }
    if header:
        ir.update(header)
    return ir


def _flow_header_ir() -> Dict[str, Any]:
    return _wrapper_ir(FLOW_HEADER_TYPE, {"type": "mock", "id": "sample"},
                       header=dict(_FLOW_HEADER_SEED))


def _try_build(ir: Dict[str, Any]) -> bool:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            parse(ir)
        return True
    except Exception:
        return False


def _failed_fields(exc: Exception) -> List[str]:
    """从构建异常里提取失败字段名（pydantic loc 优先）。"""
    names: List[str] = []
    if isinstance(exc, ValidationError):
        for err in exc.errors():
            for part in err.get("loc") or ():
                if isinstance(part, str) and part != "nodes":
                    names.append(part)
    return names


drop_log: Dict[str, List[str]] = {}


def _sample_node_for(ntype: str,
                     harvested: Dict[str, List[Dict[str, Any]]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """构造 ntype 的样本节点 dict；返回 (node, 元信息)。

    合并顺序（全部以「能构建」为门）：
    1. harvest 里该类型**字段最全**的片段打底（键过滤到声明面）；
    2. 显式种子覆盖（先摘掉同字段双拼写，避免 snake/camel 双键并存）；
    3. harvest 互补字段（union 里还没有的声明字段，逐个合并）；
    4. 模型声明面泛填（仍缺失的声明字段）；
    5. 构建失败 → 按 pydantic loc 定点丢弃（记录到 ``drop_log``）。
    """
    meta: Dict[str, Any] = {"harvest_fields": set(), "seed_fields": set(),
                            "n_harvest": 0, "harvest_source": None}
    candidates = harvested.get(ntype) or []
    meta["n_harvest"] = len(candidates)
    cls = REG.get(ntype)
    declared, legacy = _declared_surface(cls)
    excluded = _TYPE_EXCLUDE.get(ntype, set())

    node: Dict[str, Any] = {"type": ntype, "id": "sample", "next": "after"}
    if candidates:
        base = max(candidates, key=len)
        for key, val in base.items():
            if key in ("type", "id") or key in excluded:
                continue
            if not _key_acceptable(key, declared, legacy) or not isinstance(val, (str, int, float, bool, list, dict)):
                continue
            if _twin_present(key, node):
                continue
            node[key] = val
            meta["harvest_fields"].add(key)
        if meta["harvest_fields"]:
            meta["harvest_source"] = "tests/**/*.py"

    def buildable(candidate: Dict[str, Any]) -> bool:
        return _try_build(_wrapper_ir(ntype, candidate))

    # 2. 显式种子覆盖（同字段双拼写先摘除）
    seed = dict(_COMMON_NODE_SEED)
    seed.update(_TYPE_SEEDS.get(ntype, {}))
    for key, val in seed.items():
        if key in excluded:
            continue
        _drop_twins(key, node)
        node[key] = val
        meta["seed_fields"].add(key)
    if ntype == "http":  # http.output 的 IR 值必须能被 _data_value_to_src 表达
        node["output"] = "$INPUT.pick"

    # 3. harvest 互补字段
    union: Dict[str, Any] = {}
    for d in candidates:
        for key, val in d.items():
            union.setdefault(key, val)
    for key, val in sorted(union.items()):
        if key in ("type", "id", "node_id") or key in excluded or key in node:
            continue
        if not _key_acceptable(key, declared, legacy) or _twin_present(key, node):
            continue
        trial = dict(node)
        trial[key] = val
        if buildable(trial):
            node = trial
            meta["harvest_fields"].add(key)

    # 4. 模型声明面泛填
    if cls is not None:
        for name, field in cls.model_fields.items():
            if name in _SKIP_FILL_FIELDS or name in excluded or name in node:
                continue
            if _twin_present(name, node):
                continue
            trial = dict(node)
            trial[name] = _fill_value(field.annotation, name)
            if buildable(trial):
                node = trial
                meta["seed_fields"].add(name)
            else:
                drop_log.setdefault(ntype, []).append(name)

    # 5. 终检：仍不可构建就按 pydantic loc 定点丢弃。
    wrapper = _wrapper_ir(ntype, node)
    if not buildable(node):
        pending = dict(node)
        for _ in range(12):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    parse(_wrapper_ir(ntype, pending))
                node = pending
                break
            except Exception as exc:
                bad = _failed_fields(exc)
                if not bad:
                    break
                progressed = False
                for field in bad:
                    for key in [k for k in pending if _norm_key(k) == _norm_key(field)] or [field]:
                        if key in pending and key not in ("type", "id"):
                            pending = {k: v for k, v in pending.items() if k != key}
                            drop_log.setdefault(ntype, []).append(key)
                            progressed = True
                if not progressed:
                    break
        node = pending if buildable(pending) else node

    meta["harvest_fields"] = sorted(meta["harvest_fields"])
    meta["seed_fields"] = sorted(meta["seed_fields"])
    return node, meta


# ---------------------------------------------------------------------------
# 汇总：样本库 + 三链扫掠
# ---------------------------------------------------------------------------

_CACHE: Dict[str, Any] = {}


def _build_all() -> Dict[str, Any]:
    if _CACHE:
        return _CACHE
    harvested = harvest_node_dicts()
    samples: Dict[str, Dict[str, Any]] = {}
    metas: Dict[str, Dict[str, Any]] = {}
    for ntype in NODE_TYPES:
        node, meta = _sample_node_for(ntype, harvested)
        samples[ntype] = _wrapper_ir(ntype, node)
        metas[ntype] = meta
    samples[FLOW_HEADER_TYPE] = _flow_header_ir()
    metas[FLOW_HEADER_TYPE] = {"harvest_fields": [], "n_harvest": 0,
                               "seed_fields": sorted(_FLOW_HEADER_SEED),
                               "harvest_source": None}

    sweeps: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for ntype, ir in samples.items():
        entry: Dict[str, Dict[str, Any]] = {}
        expected = norm(ir)

        # ① 模型链：dict → parse → Flow → IR″
        try:
            model_back = flow_to_ir(parse(ir))
            entry["model"] = {"faces": sample_losses(expected, norm(model_back)),
                              "error": None}
        except Exception as exc:
            entry["model"] = {"faces": ["<build-failed>"], "error": repr(exc)}

        # ② sexpr 链：IR → 文本 → compile_sexpr → IR′（#150/#146 口径）
        try:
            text = flow_to_sexpr(ir)
        except Exception as exc:
            text = None
            entry["sexpr"] = {"faces": ["<emit-failed>"], "error": repr(exc), "text": None}
        if text is not None:
            try:
                back = norm(compile_sexpr(text))
                entry["sexpr"] = {"faces": sample_losses(expected, back),
                                  "error": None, "text": text}
            except Exception as exc:
                entry["sexpr"] = {"faces": ["<type-gap>"], "error": repr(exc),
                                  "text": text}

        # ③ e2e 链：IR → sexpr 文本 → parse_sexpr → Flow → IR
        text = entry["sexpr"].get("text")
        if text is None:
            entry["e2e"] = {"faces": ["<emit-failed>"],
                            "error": entry["sexpr"].get("error")}
        else:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    flow = parse_sexpr(text)
                entry["e2e"] = {"faces": sample_losses(expected, flow_to_ir(flow)),
                                "error": None}
            except Exception as exc:
                entry["e2e"] = {"faces": ["<type-gap>"], "error": repr(exc)}
        sweeps[ntype] = entry

    _CACHE.update({"harvested": harvested, "samples": samples, "metas": metas,
                   "sweeps": sweeps})
    return _CACHE


def sample_ir(ntype: str) -> Dict[str, Any]:
    return _build_all()["samples"][ntype]


def sample_types() -> List[str]:
    return NODE_TYPES + [FLOW_HEADER_TYPE]


def axis_faces(ntype: str, axis: str) -> List[str]:
    return _build_all()["sweeps"][ntype][axis]["faces"]


def axis_error(ntype: str, axis: str) -> Optional[str]:
    return _build_all()["sweeps"][ntype][axis].get("error")


def axis_text(ntype: str) -> Optional[str]:
    return _build_all()["sweeps"][ntype]["sexpr"].get("text")


def harvest_report() -> Dict[str, Any]:
    data = _build_all()
    return {
        "types_total": len(NODE_TYPES),
        "types_with_sample": len(NODE_TYPES),
        "types_with_harvest": sorted(
            t for t in NODE_TYPES if data["metas"][t]["n_harvest"] > 0),
        "types_harvest_missed": sorted(
            t for t in NODE_TYPES if data["metas"][t]["n_harvest"] == 0),
        "drop_log": drop_log,
    }
