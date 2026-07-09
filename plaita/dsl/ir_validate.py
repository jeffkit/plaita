"""plaita.dsl.ir_validate — 共享 Flow IR 拓扑校验。

三条作者前端（builder / codeflow / sexpr）最终都产出同一形态的 dict IR，
再经 ``Flow.model_validate``。历史上拓扑校验在 builder / sexpr 各写一份，
且 codeflow / AI 路径完全跳过——本模块是唯一真相源。

校验内容：
- 节点 id 唯一
- next / else_next / switch·case 分支目标存在
- if 必须有真/假分支
- switch 必须有 isDefault
- parallel 分支 next（若有）目标存在
- ``recursive=True``（默认）时递归 ``childFlow`` 与 ``parallel.branches[].flow``
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


class FlowIRValidationError(ValueError):
    """Flow IR 拓扑校验失败。

    ``path`` 指向出错子图（如 ``nodes[map1].childFlow``），供 AI/工具定位。
    """

    def __init__(self, message: str, *, path: str = "") -> None:
        self.path = path
        self.message = message
        prefix = f"[{path}] " if path else ""
        super().__init__(f"{prefix}{message}")


def validate_flow_ir(
    data: Dict[str, Any],
    *,
    recursive: bool = True,
    path: str = "",
) -> None:
    """对 Flow IR dict 做构建期拓扑校验。失败抛 ``FlowIRValidationError``。"""
    if not isinstance(data, dict):
        raise FlowIRValidationError(
            f"Flow IR 必须是 dict，得到 {type(data).__name__}",
            path=path or "<root>",
        )

    nodes = data.get("nodes") or []
    if not isinstance(nodes, list):
        raise FlowIRValidationError("nodes 必须是 list", path=path or "nodes")

    _validate_nodes(nodes, path=path or "nodes")

    if recursive:
        for n in nodes:
            if not isinstance(n, dict):
                continue
            nid = n.get("id") or "?"
            ntype = n.get("type")
            child = n.get("childFlow") or n.get("child_flow")
            if isinstance(child, dict):
                child_path = f"{path + '.' if path else ''}nodes[{nid}].childFlow"
                validate_flow_ir(child, recursive=True, path=child_path)
            if ntype == "parallel":
                for i, branch in enumerate(n.get("branches") or []):
                    if not isinstance(branch, dict):
                        continue
                    flow = branch.get("flow")
                    if isinstance(flow, dict):
                        bpath = (
                            f"{path + '.' if path else ''}nodes[{nid}].branches[{i}].flow"
                        )
                        validate_flow_ir(flow, recursive=True, path=bpath)


def _validate_nodes(nodes: List[Any], *, path: str) -> None:
    ids = [n.get("id") for n in nodes if isinstance(n, dict) and n.get("id")]
    seen: Dict[str, int] = {}
    dupes: List[str] = []
    for nid in ids:
        seen[nid] = seen.get(nid, 0) + 1
        if seen[nid] == 2:
            dupes.append(nid)
    if dupes:
        raise FlowIRValidationError(f"节点 id 重复: {dupes}", path=path)

    id_set = set(ids)

    def _check_target(target: Optional[str], owner: str, field: str) -> None:
        if target is None:
            return
        if target not in id_set:
            raise FlowIRValidationError(
                f"节点 {owner!r} 的 {field} 指向不存在的节点 id {target!r}",
                path=path,
            )

    for n in nodes:
        if not isinstance(n, dict):
            continue
        nid = n.get("id")
        ntype = n.get("type")
        if ntype == "end":
            continue
        _check_target(n.get("next"), nid, "next")

        if ntype == "if":
            if n.get("next") is None:
                raise FlowIRValidationError(
                    f"if 节点 {nid!r} 缺少真分支目标（next/then）",
                    path=path,
                )
            if n.get("else_next") is None:
                raise FlowIRValidationError(
                    f"if 节点 {nid!r} 缺少假分支目标（else_next/else_）",
                    path=path,
                )
            _check_target(n.get("next"), nid, "next")
            _check_target(n.get("else_next"), nid, "else_next")
        elif ntype == "switch":
            has_default = False
            for b in n.get("branches") or []:
                if not isinstance(b, dict):
                    continue
                _check_target(b.get("next"), nid, "branches[].next")
                if b.get("isDefault"):
                    has_default = True
            if not has_default:
                raise FlowIRValidationError(
                    f"switch 节点 {nid!r} 缺少 isDefault 分支，"
                    "全部条件不命中时行为未定义",
                    path=path,
                )
        elif ntype == "case":
            for c in n.get("cases") or []:
                if not isinstance(c, dict):
                    continue
                _check_target(c.get("id") or c.get("next"), nid, "cases[].target")
            _check_target(n.get("default"), nid, "default")
        elif ntype == "parallel":
            for b in n.get("branches") or []:
                if not isinstance(b, dict):
                    continue
                _check_target(b.get("next"), nid, "branches[].next")


def build_flow(data: Dict[str, Any], *, recursive: bool = True) -> "Flow":
    """``validate_flow_ir`` → ``Flow.model_validate`` 单入口。"""
    from plaita.core.flow import Flow

    validate_flow_ir(data, recursive=recursive)
    return Flow.model_validate(data)
