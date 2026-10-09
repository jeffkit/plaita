"""Console-canonical（正典）序列化：codeflow IR → 稳定、diff 友好的 JSON 文档。

背景（大仓 recursive #84 拍板，2026-10 收敛为 plaita 官方实现）：``@flow`` 源码
是审查主体，JSON 只是编译产物。产物格式必须满足：

1. **与 console 发布的 definition 同构**——``type`` 首键、嵌套子图统一
   ``childFlow``（含 while 节点的 ``child_flow`` 改名）、节点顶层 None 剔除
   （pydantic 对 Optional 字段「不写该键」与显式 null 不等价：显式 None 会让
   list/dict 字段校验直接失败，而「不写」是安全的引擎默认回填）。
2. **字节稳定**——键序 = IR 插入序（``compile_source`` 恒定 ``type``/``id`` 在
   首、``next``/``source_line`` 收尾，纯函数无时间戳），固定缩进序列化，连续
   两次重建 diff 必为空；配合 CLI ``--check`` 可在 CI 里钉住「产物落后源码」。

与历史上仓外脚手架（recursive ``compile_v2.py`` 等）的实现差异：

- **以 ``compile_source`` IR 为输入**，不再走 ``Flow.model_dump``。IR 每个节点
  自带 ``type`` 判别键，无需从 model_dump 的字段形状**反推**节点类型——那张
  反推表出过真实事故（gate 判别键排到 sandbox 规则之后，门禁节点在产物里
  静默变成 sandbox_agent，退化成宿主执行）。
- **不把节点模型的默认值烘进产物**。model_dump 会把 ``sandbox="ags"`` 之类的
  语义默认值一并展开，导致 plaita-nodes 改默认值就引起产物漂移；本实现只
  记录源码显式声明的字段，默认值由运行期 parse 回填（语义等价）。
- **source_line 直接是源文件绝对行号**。CLI 走 ``compile_source(文件全文)``
  的源码模式，AST 行号天然相对文件首行；装饰器模式（import 后编译）才存在
  「相对 getsource 截取段」的偏移问题，仓外脚本曾为此维护一整套 offset 机器。
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

__all__ = ["to_canonical", "canonical_node", "serialize_canonical"]


def canonical_node(node: Dict[str, Any]) -> Dict[str, Any]:
    """单个节点 IR → 正典形态：``type``/``id`` 保持首位、顶层 None 剔除、
    ``child_flow`` → ``childFlow`` 改名并递归、``branches`` 里的并行子图递归。

    条件 dict（``condition`` 字段的 ``{"field", "operator", "value"}`` 树）**原样
    保留**——其中 ``value: null`` 是合法比较（``INPUT.x == null``），不属于
    「Optional 字段显式 null」的剔除范围。
    """
    out: Dict[str, Any] = {}
    for key, value in node.items():
        if value is None:
            continue
        if key == "child_flow":
            out["childFlow"] = _canonical_scope(value)
        elif key == "childFlow":
            out[key] = _canonical_scope(value)
        elif key == "branches" and isinstance(value, list):
            out[key] = [_canonical_branch(b) for b in value]
        else:
            out[key] = value
    return out


def to_canonical(ir: Dict[str, Any]) -> Dict[str, Any]:
    """``compile_source`` 产出的 Flow IR dict → 正典文档 dict（不序列化）。

    根层键序沿用 IR 插入序（``runtime``/``flow_id``/``inputType``/``desc``/
    …/``nodes``）；``nodes`` 逐个经 :func:`canonical_node`。
    """
    return _canonical_scope(ir)


def _canonical_scope(scope: Dict[str, Any]) -> Dict[str, Any]:
    """一个流程作用域（主流程 / childFlow / parallel 分支子图）的递归转换。

    只特判 ``nodes`` 列表；其余键（``runtime``/``inputType``/``desc``/…）原样
    保留键序与值。
    """
    out: Dict[str, Any] = {}
    for key, value in scope.items():
        if key == "nodes" and isinstance(value, list):
            out["nodes"] = [canonical_node(n) for n in value]
        else:
            out[key] = value
    return out


def _canonical_branch(branch: Any) -> Any:
    """PARALLEL/switch 的分支项：``flow`` 字段是内嵌子图时递归转换。"""
    if isinstance(branch, dict) and isinstance(branch.get("flow"), dict):
        branch = dict(branch)
        branch["flow"] = _canonical_scope(branch["flow"])
    return branch


def serialize_canonical(doc: Dict[str, Any]) -> str:
    """正典文档 → 落盘文本。字节稳定约定：``ensure_ascii=False``、
    ``indent=2``、结尾单个换行（POSIX 文本惯例）。"""
    return json.dumps(doc, ensure_ascii=False, indent=2) + "\n"


def embed_source(doc: Dict[str, Any], source: str) -> Dict[str, Any]:
    """把 @flow 源码原文写进 definition 的 ``metadata``（原地修改并返回）。

    这是 console 源码页签 / 「查看权威源码」跳转的数据源（前端读
    ``definition.metadata.source``）。与 mediaflow ``publish_console.py``
    的发布时注入等效——统一入口提供此能力后，各仓编译落盘即自带，无需
    在发布链路里各写一遍。已声明的 metadata 字段保留，``source`` /
    ``source_format`` 覆盖；源码原文即文件内容，天然字节稳定。
    """
    meta = doc.get("metadata")
    meta = dict(meta) if isinstance(meta, dict) else {}
    meta["source"] = source
    meta["source_format"] = "plaita@flow"
    doc["metadata"] = meta
    return doc


def count_nodes(doc: Dict[str, Any]) -> int:
    """统计文档里的节点总数（含 childFlow / parallel 分支子图，供 CLI 摘要）。"""
    total = 0
    stack: List[Dict[str, Any]] = [doc]
    while stack:
        scope = stack.pop()
        for node in scope.get("nodes", []):
            total += 1
            child = node.get("childFlow")
            if isinstance(child, dict):
                stack.append(child)
            for branch in node.get("branches", []) or []:
                if isinstance(branch, dict) and isinstance(branch.get("flow"), dict):
                    stack.append(branch["flow"])
    return total
