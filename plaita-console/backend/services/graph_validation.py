"""graph validate 共享校验器（plaita#305 Phase 1）。

## GraphDTO —— 输入契约（权威定义）

GraphDTO 是 plaita-console 前端 ``components/flow`` 的 @xyflow/react 编辑器
序列化形状（``frontend/src/components/flow/flowConverter.ts`` 为前端权威实现，
本模块的 ``graph_to_definition`` 是其服务端镜像——两边改折叠规则必须同步）：

.. code-block:: json

    {
      "nodes": [                       // 画布节点（xyflow Node[]）
        {
          "id": "check",               // 画布节点 id = Flow 节点 id
          "type": "plaitaNode",        // xyflow 渲染类型，恒为 plaitaNode
          "position": {"x": 0, "y": 0},// 画布坐标，validate 不消费
          "parentId": "host",          // 容器展开态子节点标记（与
                                       //   data.isContainerChild 双标记同在时
                                       //   跳过：子节点已双写进 owner 的
                                       //   childFlow；半标/宿主缺失由
                                       //   shape_errors 报 graph.malformed_node
                                       //   ——#309 零报错剔除不可保留）
          "data": {
            "type": "if",              // plaita 节点类型（start/end/if/switch/...）
            "name": "检查",            // 展示名（可省）
            "fields": {"condition": "$INPUT.ok"}   // 类型特定配置字段
          }
        }
      ],
      "edges": [                       // 画布连线（xyflow Edge[]）
        {"id": "e1", "source": "check", "target": "end",
         "sourceHandle": "true"}       // true=if 真分支 / false=else /
                                       // switch|case=分支名 / 其余线性取首条
      ],
      "viewport": {"x": 0, "y": 0, "zoom": 1},   // 画布视口，validate 不消费
      "meta": {"flow_id": "...", "version": "1.0.0", "desc": "...",
               "author": "...", "inputType": {...}, "outputType": {...},
               "globalContext": {...}, "metadata": {...},
               "timeout": "30s"}         // timeout 为宿主可选扩展（环检测语义）
    }

折叠规则（graph_to_definition，与 flowToJson 逐条对齐）：

- 无 ``parentId`` 的节点为顶层节点（容器子节点已双写回 owner 的 childFlow）；
- ``data.fields`` 平铺进节点 dict，``None`` 字段剔除；assignment 缺 outputType
  时注入引擎默认 ``{"dataType": "object"}``；
- if：``sourceHandle=='false'`` 的边 → ``else_next``，其余 → ``next``；
- switch/case：``sourceHandle`` 匹配 ``fields.branches[i].name`` → 该分支 ``next``；
- 其余类型：第一条出边 → ``next``；
- ``meta`` 平铺进定义顶层（flow_id/version/desc/author/inputType/outputType/
  globalContext/metadata）。

## 与 publish 门共享实现（#305 硬要求）

图内容校验只有两个权威实现，本模块**直接调用它们**、不另写第二份：

- 字段 schema：:func:`services.validation.validate_definition`（保存/导入
  路径同一函数，内核 ``Flow.model_validate``）；
- 能力门：:func:`services.capability_gate.gate_flow_nodes`（#300 publish/
  start 同一函数）。

一致性契约（双向矩阵，tests/console/test_issue305_graph_validate.py）：validate
通过的 graph 经 save+publish 必成功；publish/save 拒绝的 graph validate 必报
同因。validate 额外做**编辑器图**层面的结构与连线校验（单入口/可达 end/无环/
孤儿/悬空连线/handle 语义），因此 validate 比 publish 更严——publish 通过但
validate 拒绝是设计内不对称（编辑器在保存前拦住「发布得了但运行期才炸」的图）。

## 错误项结构

``errors[]`` 每项 ``{type, code, node_id?, field?, message, severity}``：

- ``type``：``node``（节点级结构/字段）| ``field``（schema 校验）|
  ``edge``（连线语义）| ``capability``（能力门）；
- ``code``：机器可判子类（如 ``graph.no_start`` / ``edge.unknown_target`` /
  ``capability.unsupported_node_type``）；
- ``severity``：``error``（计入 invalid → 422）| ``warning``（不阻断，降级
  与死边类可观测提示）。
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

try:
    from . import validation
    from .capability_gate import UnsupportedNodeTypesError, gate_flow_nodes
except ImportError:  # 顶层 ``services`` 包（平铺运行/测试模式）
    import validation  # type: ignore
    from capability_gate import UnsupportedNodeTypesError, gate_flow_nodes  # type: ignore

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"

#: 与 flowToJson 一致的分支折叠类型集（if 单列：true/false 双出口语义）。
_BRANCHING_TYPES = frozenset({"switch", "case"})
_IF_TYPE = "if"

#: 必须携带 childFlow 的容器类节点（与 plaita.dsl.ir_validate 同集；reference
#: 由调度器注入除外）。
_CHILD_FLOW_REQUIRED = frozenset({
    "child", "loop", "while", "map", "filter", "find", "reduce",
})

#: 引用型节点：flow_id 指向同租户另一条流程（多租户隔离检查对象）。
_REFERENCE_TYPE = "reference"

_PATH_NODE_RE = re.compile(r"nodes\[([^\]]+)\]")


def _err(type_: str, code: str, message: str, *, node_id: Optional[str] = None,
         field: Optional[str] = None, severity: str = SEVERITY_ERROR,
         **extra: Any) -> Dict[str, Any]:
    item: Dict[str, Any] = {"type": type_, "code": code, "message": message,
                            "severity": severity}
    if node_id is not None:
        item["node_id"] = node_id
    if field is not None:
        item["field"] = field
    for k, v in extra.items():
        if v is not None:
            item[k] = v
    return item


# ---------------------------------------------------------------------------
# GraphDTO → Flow 定义（flowToJson 的服务端镜像）
# ---------------------------------------------------------------------------

def _top_level_nodes(graph: Any) -> List[Dict[str, Any]]:
    """画布顶层节点（跳过容器展开态子节点——已双写回 owner 的 childFlow）。

    对形状不合法的条目按「缺失」跳过（折叠不抛）；剔除是否合法由
    :func:`shape_errors` 具名裁决（#309：非法剔除 → error 级，保存被拦）。
    """
    nodes: List[Dict[str, Any]] = []
    for n in graph.get("nodes") or []:
        if not isinstance(n, dict):
            continue
        if n.get("parentId"):
            continue
        data = n.get("data")
        if not isinstance(data, dict):
            continue
        if data.get("isContainerChild"):
            continue
        nodes.append(n)
    return nodes


def _is_container_child(n: Dict[str, Any], data: Dict[str, Any]) -> bool:
    """容器子节点标记（宽容读两层：编辑器产 data 层，手造 DTO 可能放节点层）。"""
    return bool(data.get("isContainerChild") or n.get("isContainerChild"))


def _container_child_ids(graph: Dict[str, Any]) -> set:
    """合法展开态容器子节点 id 集（parentId + isContainerChild 双标记）。

    与 flowConverter ``jsonToGraph`` 的序列化对称：编辑器产出的子节点两者
    恒同在（data 层 isContainerChild + ownerId）；折叠不消费这类节点的
    连线（子流程拓扑已双写进 owner 的 childFlow）。
    """
    ids: set = set()
    for n in graph.get("nodes") or []:
        if not isinstance(n, dict):
            continue
        data = n.get("data")
        if not isinstance(data, dict):
            continue
        nid = n.get("id")
        if (isinstance(nid, str) and nid and n.get("parentId")
                and _is_container_child(n, data)):
            ids.add(nid)
    return ids


def shape_errors(graph: Dict[str, Any]) -> List[Dict[str, Any]]:
    """GraphDTO 节点形状校验——零报错剔除的正面（#309）。

    ``_top_level_nodes``/``graph_to_definition`` 对形状不合法的节点按
    「缺失」折叠（不抛）；历史上这份剔除零报错——孤立缺形节点 valid=true、
    照此保存即无声消失。这里逐个具名：凡不是「合法容器子节点」的剔除，
    一律 ``code=graph.malformed_node``、severity=error → 422，保存被拦。

    合法容器子节点（parentId + isContainerChild 双标记、宿主在图中）不报：
    已双写进 owner 的 childFlow，折叠跳过是设计语义（与 flowToJson L54 对齐）。
    """
    errors: List[Dict[str, Any]] = []
    nodes = graph.get("nodes") or []
    known_ids = {n.get("id") for n in nodes
                 if isinstance(n, dict) and isinstance(n.get("id"), str)}
    for idx, n in enumerate(nodes):
        if not isinstance(n, dict):
            errors.append(_err(
                "node", "graph.malformed_node",
                f"nodes[{idx}] 不是对象（xyflow 节点应为 {{id, type, position, "
                "data}} 形状）；折叠将剔除该条目，照此保存即丢失",
            ))
            continue
        nid = n.get("id") if isinstance(n.get("id"), str) else None
        where = f"节点 {nid!r}" if nid else f"nodes[{idx}]"
        data = n.get("data")
        if not isinstance(data, dict):
            errors.append(_err(
                "node", "graph.malformed_node",
                f"{where} 缺少 data 对象（无 type/fields 可折叠）；"
                "折叠将剔除该节点，照此保存即丢失",
                node_id=nid, field="data",
            ))
            continue
        parent = n.get("parentId")
        is_child = _is_container_child(n, data)
        if parent and is_child:
            if parent not in known_ids:
                errors.append(_err(
                    "node", "graph.malformed_node",
                    f"{where} 的容器宿主 {parent!r} 不在图中（陈旧 ownerId）；"
                    "折叠找不到双写目标，照此保存即丢失",
                    node_id=nid, field="parentId",
                ))
            continue
        if parent and not is_child:
            errors.append(_err(
                "node", "graph.malformed_node",
                f"{where} 有 parentId {parent!r} 但缺 isContainerChild 标记"
                "（编辑器产出的展开态子节点两者恒同在）；折叠按子节点剔除，"
                "照此保存即丢失",
                node_id=nid, field="isContainerChild",
            ))
            continue
        if is_child and not parent:
            errors.append(_err(
                "node", "graph.malformed_node",
                f"{where} 的 isContainerChild 为真但无 parentId"
                "（找不到折叠宿主）；折叠将剔除该节点，照此保存即丢失",
                node_id=nid, field="parentId",
            ))
    return errors


def graph_to_definition(graph: Dict[str, Any]) -> Dict[str, Any]:
    """GraphDTO → Flow 定义 dict（flowToJson 折叠规则的服务端镜像）。

    形状坏的节点/边按「缺失」处理（不抛）——是否合法由 :func:`shape_errors`
    裁决（#309：非法剔除 → error 级 422），连线语义由 :func:`edge_errors`
    报告，这里只负责尽力转换。
    """
    out_nodes: List[Dict[str, Any]] = []
    edges = [e for e in graph.get("edges") or [] if isinstance(e, dict)]

    for n in _top_level_nodes(graph):
        data = n.get("data") or {}
        node_type = data.get("type")
        if not isinstance(node_type, str) or not node_type:
            node_type = "unknown"
        node_id = n.get("id") if isinstance(n.get("id"), str) else ""
        node_obj: Dict[str, Any] = {"type": node_type}
        if node_id:
            node_obj["id"] = node_id
        name = data.get("name")
        if name:
            node_obj["name"] = name
        fields = data.get("fields")
        if isinstance(fields, dict):
            for k, v in fields.items():
                if v is None:
                    continue
                node_obj[k] = v
        # assignment 缺 outputType 时注入引擎默认（与 flowToJson 同款兜底）
        if (node_type == "assignment" and "outputType" not in node_obj
                and "output_type" not in node_obj):
            node_obj["outputType"] = {"dataType": "object"}

        out_edges = [e for e in edges if e.get("source") == node_id and node_id]
        if node_type == _IF_TYPE:
            for e in out_edges:
                if e.get("sourceHandle") == "false":
                    node_obj["else_next"] = e.get("target")
                else:
                    node_obj["next"] = e.get("target")
        elif node_type in _BRANCHING_TYPES:
            branches = fields.get("branches") if isinstance(fields, dict) else None
            resolved = [dict(b) for b in branches if isinstance(b, dict)] if isinstance(branches, list) else []
            for e in out_edges:
                handle = e.get("sourceHandle")
                idx = next(
                    (i for i, b in enumerate(resolved) if b.get("name") == handle),
                    -1,
                ) if handle is not None else -1
                if idx >= 0:
                    resolved[idx]["next"] = e.get("target")
            if resolved:
                node_obj["branches"] = resolved
        else:
            if out_edges:
                node_obj["next"] = out_edges[0].get("target")
        out_nodes.append(node_obj)

    definition: Dict[str, Any] = {"nodes": out_nodes}
    meta = graph.get("meta")
    if isinstance(meta, dict):
        # timeout 非 FlowMeta 既有键（引擎 Flow.timeout 字段，环检测语义相关），
        # 宿主显式给就透传
        for key in ("flow_id", "version", "desc", "author", "inputType",
                    "outputType", "globalContext", "metadata", "timeout"):
            if meta.get(key) is not None:
                definition[key] = meta[key]
    return definition


# ---------------------------------------------------------------------------
# 图结构校验（节点类错误）
# ---------------------------------------------------------------------------

def _definition_graphs(definition: Dict[str, Any]) -> List[Tuple[str, List[Any], Optional[Dict[str, Any]]]]:
    """定义里的全部（子）图：``(路径, nodes, 宿主节点)`` 列表（root + childFlow 递归）。"""
    graphs: List[Tuple[str, List[Any], Optional[Dict[str, Any]]]] = [
        ("nodes", definition.get("nodes") or [], None)
    ]

    def _walk(nodes: Any, path: str) -> None:
        if not isinstance(nodes, list):
            return
        for n in nodes:
            if not isinstance(n, dict):
                continue
            child = n.get("childFlow") or n.get("child_flow")
            if isinstance(child, (str, bytes)):
                try:
                    child = json.loads(child)
                except Exception:  # noqa: BLE001 — 非法 JSON 交字段校验报
                    continue
            if isinstance(child, dict):
                child_path = f"{path}.nodes[{n.get('id') or '?'}].childFlow"
                graphs.append((child_path, child.get("nodes") or [], n))
                _walk(child.get("nodes"), child_path)
            if n.get("type") == "parallel":
                for i, branch in enumerate(n.get("branches") or []):
                    if not isinstance(branch, dict):
                        continue
                    flow = branch.get("flow")
                    if isinstance(flow, dict):
                        bpath = f"{path}.nodes[{n.get('id') or '?'}].branches[{i}].flow"
                        graphs.append((bpath, flow.get("nodes") or [], n))
                        _walk(flow.get("nodes"), bpath)

    _walk(definition.get("nodes"), "nodes")
    return graphs


def _iter_typed_nodes(definition: Dict[str, Any]) -> List[Tuple[str, str]]:
    """全部节点的 ``(node_id, type)``（含子图，遍历序稳定）——能力门定位用。"""
    pairs: List[Tuple[str, str]] = []

    def _walk(nodes: Any) -> None:
        if not isinstance(nodes, list):
            return
        for n in nodes:
            if not isinstance(n, dict):
                continue
            nid = n.get("id")
            ntype = n.get("type")
            if isinstance(nid, str) and nid and isinstance(ntype, str) and ntype:
                pairs.append((nid, ntype))
            child = n.get("childFlow") or n.get("child_flow")
            if isinstance(child, (str, bytes)):
                try:
                    child = json.loads(child)
                except Exception:  # noqa: BLE001
                    continue
            if isinstance(child, dict):
                _walk(child.get("nodes"))
            if n.get("type") == "parallel":
                for branch in n.get("branches") or []:
                    if isinstance(branch, dict) and isinstance(branch.get("flow"), dict):
                        _walk(branch["flow"].get("nodes"))

    _walk(definition.get("nodes"))
    return pairs


def _successors(node: Dict[str, Any]) -> List[str]:
    """节点声明的后继 id（next/else_next/branches[].next；name-as-target 回退
    不算结构边，与引擎 ``Flow.resolve_nodes`` 的环检测邻接口径一致）。"""
    targets: List[str] = []
    for key in ("next", "else_next"):
        t = node.get(key)
        if isinstance(t, str):
            targets.append(t)
    for b in node.get("branches") or []:
        if isinstance(b, dict) and isinstance(b.get("next"), str):
            targets.append(b["next"])
    return targets


def structure_errors(definition: Dict[str, Any]) -> List[Dict[str, Any]]:
    """编辑器图结构校验（转出的定义上做）：单入口 / 可达 end / 无环 / 孤儿。

    根图与每个 childFlow / parallel 分支子图都过同一套检查（子流程缺 start
    / 尾部无 end 同样是运行期硬失败）。判据与引擎对齐：

    - 入口：引擎要求显式 ``type=start``（``Flow.start_node`` 缺失即
      ``FlowStartMissingError``）；多个 start 时首之外的全部死节点 → 逐个报。
    - 可达 end：从 start 可达的**终端**（无后继）节点必须都是 ``end``——
      引擎对非 end 终端抛 ``FlowExecutionException``（strategies 缺 next 硬失败）。
    - 无环：复用引擎 ``plaita.core.flow._resolve_cycle`` 的回边判据（#105 修正
      后：汇聚点不误报）；无 flow 级 timeout 的环 = Normal 模式永久挂起 →
      error，设了 timeout → warning（与引擎告警口径一致）。
    - 孤儿：start 不可达的节点运行期被静默跳过（结果无声丢失）→ 逐个报。
    """
    has_timeout = bool(definition.get("timeout"))
    errors: List[Dict[str, Any]] = []
    for path, nodes, host in _definition_graphs(definition):
        scope = "" if host is None else f"[子流程 {path}] "
        if host is None and not nodes:
            errors.append(_err("node", "graph.empty", "图没有任何节点"))
            continue
        errors.extend(_structure_errors_one(
            nodes, scope=scope, has_timeout=has_timeout,
        ))
    return errors


def _structure_errors_one(nodes: List[Any], *, scope: str,
                          has_timeout: bool) -> List[Dict[str, Any]]:
    errors: List[Dict[str, Any]] = []
    nodes = [n for n in nodes if isinstance(n, dict)]
    if not nodes:
        errors.append(_err(
            "node", "graph.no_start",
            f"{scope}子流程没有任何节点；运行期将抛 FlowStartMissingError",
        ))
        return errors

    by_id: Dict[str, Dict[str, Any]] = {}
    for n in nodes:
        nid = n.get("id")
        if isinstance(nid, str) and nid:
            by_id.setdefault(nid, n)
        else:
            errors.append(_err("node", "node.missing_id",
                               f"{scope}节点缺少字符串 id: {n!r}"[:200]))
    if not by_id:
        return errors

    # id 重复（引擎 last-wins 静默）
    seen: Dict[str, int] = {}
    for n in nodes:
        nid = n.get("id")
        if isinstance(nid, str) and nid:
            seen[nid] = seen.get(nid, 0) + 1
    for nid, count in sorted(seen.items()):
        if count > 1:
            errors.append(_err(
                "node", "node.duplicate_id",
                f"{scope}节点 id 重复 {nid!r}（{count} 处）：引擎按 last-wins "
                "静默取后者，先出现的同名节点会丢失",
                node_id=nid,
            ))

    # 节点类型缺失
    for nid, n in sorted(by_id.items()):
        if not isinstance(n.get("type"), str) or not n.get("type"):
            errors.append(_err("node", "node.missing_type",
                               f"{scope}节点缺少 type", node_id=nid))

    # 分支/容器完整性（与 plaita.dsl.ir_validate 硬编码集同判据）
    for nid, n in sorted(by_id.items()):
        ntype = n.get("type")
        if ntype == _IF_TYPE:
            if n.get("next") is None:
                errors.append(_err(
                    "edge", "edge.if_branch_missing",
                    f"{scope}if 节点 {nid!r} 缺少真分支出边（sourceHandle=true）",
                    node_id=nid, field="next",
                ))
            if n.get("else_next") is None:
                errors.append(_err(
                    "edge", "edge.if_branch_missing",
                    f"{scope}if 节点 {nid!r} 缺少假分支出边（sourceHandle=false）；"
                    "条件为假时运行期分支未命中硬失败",
                    node_id=nid, field="else_next",
                ))
        elif ntype == "switch":
            branches = [b for b in n.get("branches") or [] if isinstance(b, dict)]
            if not any(b.get("isDefault") for b in branches):
                errors.append(_err(
                    "node", "node.switch_no_default",
                    f"{scope}switch 节点 {nid!r} 缺少 isDefault 分支，"
                    "全部条件不命中时行为未定义",
                    node_id=nid, field="branches",
                ))
        elif ntype in _CHILD_FLOW_REQUIRED and not (
                n.get("childFlow") or n.get("child_flow")):
            errors.append(_err(
                "node", "node.child_flow_missing",
                f"{scope}节点 {nid!r}（{ntype}）缺少 childFlow 子流程",
                node_id=nid, field="childFlow",
            ))

    starts = sorted(nid for nid, n in by_id.items() if n.get("type") == "start")
    if not starts:
        errors.append(_err(
            "node", "graph.no_start",
            f"{scope}图缺少 type=start 入口节点（共 {len(by_id)} 个节点）；"
            "运行期将抛 FlowStartMissingError",
        ))
        return errors  # 无入口时可达性/环没有锚点，避免错误雪崩
    for extra in starts[1:]:
        errors.append(_err(
            "node", "graph.extra_start",
            f"{scope}多个 start 入口：只有 {starts[0]!r} 生效，{extra!r} 是死节点",
            node_id=extra,
        ))
    root = starts[0]

    # end 是运行期绝对终点（next_node 对 end 恒返回 None）：环检测/可达性
    # 都不沿 end 的出边前进——end 出边本身由 edge_errors 报死边 warning
    adjacency = {
        nid: ([] if n.get("type") == "end"
              else [t for t in _successors(n) if t in by_id])
        for nid, n in by_id.items()
    }
    reachable: set = set()
    stack = [root]
    while stack:
        cur = stack.pop()
        if cur in reachable:
            continue
        reachable.add(cur)
        stack.extend(adjacency.get(cur, ()))

    # 孤儿（不可达 → 运行期静默跳过）
    for nid in sorted(by_id.keys() - reachable):
        errors.append(_err(
            "node", "graph.orphan",
            f"{scope}节点 {nid!r} 从 start 不可达，运行期会被静默跳过"
            "（结果无声丢失）；请接入主流程或删除",
            node_id=nid,
        ))

    # 终端必须可达且为 end（有声明后继但全部悬空的节点不算终端——
    # 那是 edge.unknown_target 的失败面，这里不重复报）
    for nid in sorted(reachable):
        if _successors(by_id[nid]):
            continue
        if by_id[nid].get("type") != "end":
            errors.append(_err(
                "node", "graph.dead_end_non_end",
                f"{scope}节点 {nid!r} 无后继且不是 end：运行期将抛 "
                "FlowExecutionException（refuses to end with an ambiguous "
                "intermediate result）",
                node_id=nid,
            ))

    # 环（引擎同款回边判据）
    try:
        from plaita.core.flow import _resolve_cycle
        cycle = _resolve_cycle(adjacency, root)
    except Exception:  # noqa: BLE001 — 引擎内部符号不可用时退化为不查环
        cycle = []
    if cycle:
        errors.append(_err(
            "node", "graph.cycle",
            f"{scope}检测到环: " + " -> ".join(cycle) + (
                "；无 flow 级 timeout 时 Normal 模式将永久挂起"
                "（设 timeout 或改用 loop/while 等带迭代上限的节点）"
                if not has_timeout else
                "；已设 flow 级 timeout，运行期会按超时中止（请确认是否符合意图）"
            ),
            node_id=cycle[0] if cycle else None,
            severity=SEVERITY_ERROR if not has_timeout else SEVERITY_WARNING,
        ))
    return errors


# ---------------------------------------------------------------------------
# 连线语义校验（边类错误，在 GraphDTO 的 edges 上做）
# ---------------------------------------------------------------------------

def edge_errors(graph: Dict[str, Any]) -> List[Dict[str, Any]]:
    """画布连线语义：端点存在 / handle 与节点类型匹配 / 出边数约束。

    折叠语义以 ``flowToJson`` 为契约：if 节点任何非 ``false`` handle 都折进
    ``next``（last-wins）、线性节点只取第一条出边——同组多条边 = 后者静默
    覆盖/丢失，这里前置报 error；end 的出边被引擎整体忽略 → warning。

    容器展开态子节点之间的连线（#309）：子流程拓扑已双写进 owner 的
    childFlow，折叠只匹配顶层 source、不消费子边（flowConverter
    ``jsonToGraph`` 把子边推进同一 edges 数组——编辑器自身可产生的形态），
    不予拒绝。子节点与其余端点混连仍报：折叠要么丢边（child 出边）要么
    产出悬空 next（指入 child 的 target），都是运行期失败面。
    """
    errors: List[Dict[str, Any]] = []
    top = _top_level_nodes(graph)
    top_ids = {n.get("id") for n in top if isinstance(n.get("id"), str)}
    child_ids = _container_child_ids(graph)
    node_by_id = {n.get("id"): n for n in top if isinstance(n.get("id"), str)}
    edges = [e for e in graph.get("edges") or [] if isinstance(e, dict)]

    for e in edges:
        src, dst = e.get("source"), e.get("target")
        if src in child_ids and dst in child_ids:
            continue  # 容器内部连线：已双写进 owner.childFlow，折叠不消费
        if src not in top_ids:
            errors.append(_err("edge", "edge.unknown_source",
                               f"连线 {e.get('id') or ''} 的 source {src!r} 不是顶层节点 id"
                               .strip()))
            continue
        if dst not in top_ids:
            if dst in child_ids:
                errors.append(_err(
                    "edge", "edge.unknown_target",
                    f"连线 {e.get('id') or ''} 的 target {dst!r} 是容器展开态"
                    "子节点：折叠后将产生悬空 next（运行期将 NodeNotFoundError）；"
                    "请连接容器节点本身",
                    node_id=src, field="next"))
            else:
                errors.append(_err("edge", "edge.unknown_target",
                                   f"连线 {e.get('id') or ''} 的 target {dst!r} 不存在"
                                   "（悬空连线，运行期将 NodeNotFoundError）",
                                   node_id=src, field="next"))

    out_by_src: Dict[str, List[Dict[str, Any]]] = {}
    for e in edges:
        if e.get("source") in top_ids:
            out_by_src.setdefault(e["source"], []).append(e)

    for src in sorted(out_by_src):
        n = node_by_id.get(src) or {}
        ntype = (n.get("data") or {}).get("type")
        outs = out_by_src[src]
        if ntype == _IF_TYPE:
            groups: Dict[str, List[Dict[str, Any]]] = {"true": [], "false": []}
            for e in outs:
                handle = e.get("sourceHandle")
                if handle == "false":
                    groups["false"].append(e)
                elif handle in (None, "true"):
                    groups["true"].append(e)
                else:
                    errors.append(_err(
                        "edge", "edge.unknown_handle",
                        f"if 节点 {src!r} 的出边 handle {handle!r} 非法"
                        "（只认 true/false；其余会被当真分支静默折叠进 next）",
                        node_id=src, field="sourceHandle",
                    ))
            for branch, group in groups.items():
                if len(group) > 1:
                    errors.append(_err(
                        "edge", "edge.duplicate_branch",
                        f"if 节点 {src!r} 的 {branch} 分支有 {len(group)} 条出边，"
                        f"只有最后一条生效（{group[-1].get('target')!r}），其余被静默覆盖",
                        node_id=src,
                        field="next" if branch == "true" else "else_next",
                    ))
        elif ntype in _BRANCHING_TYPES:
            fields = (n.get("data") or {}).get("fields")
            branches = fields.get("branches") if isinstance(fields, dict) else None
            names = [b.get("name") for b in branches
                     if isinstance(b, dict) and isinstance(b.get("name"), str)] \
                if isinstance(branches, list) else []
            seen_handles: set = set()
            for e in outs:
                handle = e.get("sourceHandle")
                if handle is None:
                    errors.append(_err(
                        "edge", "edge.handle_missing",
                        f"{ntype} 节点 {src!r} 的出边缺 sourceHandle（应填分支名）",
                        node_id=src, field="sourceHandle",
                    ))
                elif handle not in names:
                    errors.append(_err(
                        "edge", "edge.handle_not_a_branch",
                        f"{ntype} 节点 {src!r} 的出边 handle {handle!r} 不在分支名 "
                        f"{names} 中（折叠不进任何分支，目标被静默丢弃）",
                        node_id=src, field="sourceHandle",
                    ))
                elif handle in seen_handles:
                    errors.append(_err(
                        "edge", "edge.duplicate_branch",
                        f"{ntype} 节点 {src!r} 的分支 {handle!r} 有多条出边，"
                        "只有最后一条生效，其余被静默覆盖",
                        node_id=src, field="branches",
                    ))
                seen_handles.add(handle)
        else:
            if len(outs) > 1:
                errors.append(_err(
                    "edge", "edge.ambiguous_multiple",
                    f"节点 {src!r}（{ntype}）有 {len(outs)} 条出边，只有第一条"
                    f"（→ {outs[0].get('target')!r}）生效，其余被静默丢弃",
                    node_id=src, field="next",
                ))
            if ntype == "end" and outs:
                errors.append(_err(
                    "edge", "edge.end_outgoing",
                    f"end 节点 {src!r} 的出边无运行期语义（引擎对 end 不再前进），"
                    "属死边，请删除",
                    node_id=src, severity=SEVERITY_WARNING,
                ))
    return errors


# ---------------------------------------------------------------------------
# 字段 schema 校验（与保存/发布同一权威实现）
# ---------------------------------------------------------------------------

def field_errors(definition: Dict[str, Any]) -> List[Dict[str, Any]]:
    """节点/流字段 schema 校验——**直接调用保存路径同一函数**。

    第一道（权威）：:func:`services.validation.validate_definition`，即保存/
    导入与发布面共用的 ``Flow.model_validate``。它失败时必然产生一个
    ``code=invalid_definition`` 的 error 项（保证「save/publish 拒绝 →
    validate 报同因」按构造成立），并尽力做逐节点定位：

    - 逐节点 ``registry.parse_node``（与 ``Flow.resolve_nodes`` 同一解析器），
      失败 → ``code=node_schema``；
    - 解析成功的节点跑 ``node.validate()``（引擎构建期检查，运行前最后一道
      语义闸——如 assignment 缺 output/upstream_output）→ ``code=node_validate``；
    - 引擎 ir 规则（$NODE/$F 引用、joinBranches、childflow 收尾、outputType
      预检）以 warning 透出（引擎侧行为即非致命告警，这里保持同severity）。
    """
    errors: List[Dict[str, Any]] = []
    try:
        validation.validate_definition(definition)
    except ValueError as e:
        errors.append(_err("field", "invalid_definition", str(e)))

    # 逐节点定位（尽力而为，权威结论已在上面的 catch-all 里）
    try:
        from plaita.node import get_default_registry
        from plaita.dsl.ir_validate import (
            FlowIRGraph,
            check_childflow_reachable_end,
            check_expression_functions,
            check_node_refs,
            check_output_type_literal,
            check_parallel_join_branches,
        )
    except Exception:  # noqa: BLE001 — 引擎不可导入时只留权威结论
        return errors

    registry = get_default_registry()
    granular: List[Dict[str, Any]] = []
    for path, nodes, host in _definition_graphs(definition):
        for n in nodes:
            if not isinstance(n, dict):
                continue
            nid = n.get("id") if isinstance(n.get("id"), str) else None
            try:
                instance = registry.parse_node(n)
            except Exception as exc:  # noqa: BLE001 — 解析失败细节即诊断
                granular.append(_err(
                    "field", "node_schema", _scrub(str(exc)),
                    node_id=nid,
                ))
                continue
            try:
                instance.validate()
            except Exception as exc:  # noqa: BLE001
                granular.append(_err(
                    "field", "node_validate", _scrub(str(exc)),
                    node_id=nid,
                ))

        # ir 规则（warning）：图级缓存对象按图构建，逐节点调用
        graph = FlowIRGraph(path=path or "nodes", nodes=nodes, host=host,
                            ancestors=(host,) if host is not None else ())
        ir_rules = (
            check_childflow_reachable_end,
            check_parallel_join_branches,
            check_node_refs,
            check_expression_functions,
            check_output_type_literal,
        )
        reported: set = set()
        for idx, n in enumerate(nodes):
            if not isinstance(n, dict):
                continue
            node_path = f"{path or 'nodes'}.nodes[{n.get('id') or idx}]"
            for rule in ir_rules:
                try:
                    verdict = rule(n, node_path, graph)
                except Exception as exc:  # noqa: BLE001 — 图级规则以 raise 报
                    verdict = _scrub(str(exc))
                if not verdict:
                    continue
                key = (path, verdict)
                if key in reported:
                    continue
                reported.add(key)
                match = _PATH_NODE_RE.search(node_path)
                granular.append(_err(
                    "node", "ir_rule", _scrub(str(verdict)),
                    node_id=match.group(1) if match else None,
                    severity=SEVERITY_WARNING,
                ))
    errors.extend(granular)
    return errors


_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _scrub(text: str) -> str:
    """异常/校验文本压成单行安全串（长度有界）。"""
    flat = _CONTROL_CHARS_RE.sub(" ", str(text)).strip()
    return flat if len(flat) <= 500 else flat[:499] + "…"


# ---------------------------------------------------------------------------
# 能力门（与 #300 publish/start 同一实现）
# ---------------------------------------------------------------------------

def capability_errors(definition: Dict[str, Any], runtime: str,
                      redis: Any) -> List[Dict[str, Any]]:
    """节点类型 vs 目标 runtime 池能力清单——调 :func:`gate_flow_nodes` 本体。

    - 不支持 → 每个 unsupported 类型一条 ``capability`` error（附首个命中
      节点 id 与池支持面，host AI 可直接改图）；
    - 池内无清单 → publish 门兜底放行（fail-open + WARNING），这里同样放行
      并附 ``capability.pool_manifest_missing`` warning（响应内可观测降级）；
    - ``redis=None``（本地单机档无 registry）→ 与 publish 门一致跳过。
    """
    if redis is None:
        return []
    try:
        gate_flow_nodes(redis, runtime, definition)
    except UnsupportedNodeTypesError as e:
        detail = dict(e.detail or {})
        unsupported = [str(t) for t in detail.get("unsupported") or []]
        supported = [str(t) for t in detail.get("supported") or []]
        first_hit: Dict[str, str] = {}
        for nid, ntype in _iter_typed_nodes(definition):
            if ntype in unsupported and ntype not in first_hit:
                first_hit[ntype] = nid
        return [
            _err(
                "capability", "capability.unsupported_node_type",
                f"节点类型 {t!r} 不被目标 runtime {detail.get('runtime') or runtime} "
                f"池支持（该类型首个节点: {first_hit.get(t, '?')!r}）；"
                f"池支持面: {supported}",
                node_id=first_hit.get(t),
                field="type",
                # 与 #300 publish 拒绝 detail 同键（双向矩阵按此对账）
                runtime=detail.get("runtime") or runtime,
                unsupported=unsupported,
                supported=supported,
            )
            for t in unsupported
        ]
    if _pool_has_no_manifest(runtime, redis):
        return [_err(
            "capability", "capability.pool_manifest_missing",
            f"runtime={runtime} 池内没有任何带 capability_manifest 的 worker，"
            "能力门按兜底放行（fail-open）——旧 build worker 部署请尽快升级",
            severity=SEVERITY_WARNING,
        )]
    return []


def _pool_has_no_manifest(runtime: str, redis: Any) -> bool:
    """gate_flow_nodes 放行的两种可能里，区分「全部支持」与「池无清单」。"""
    try:
        from .capability_gate import collect_pool_manifests
    except ImportError:
        from capability_gate import collect_pool_manifests  # type: ignore
    return not collect_pool_manifests(redis, runtime)


# ---------------------------------------------------------------------------
# 引用目标的多租户存在性与形状×池一致性（隔离负控 + #309/A2R51-01）
# ---------------------------------------------------------------------------

#: reference 节点当前**零可执行形状**的 runtime 池（A2R51-01）：python 引擎
#: 全仓无按 node.flow_id 注入 child_flow 的调度器（named-ref 运行期
#: plaita/node/child.py arun assert 必死），ReferenceFlow.setup_child_flow
#: （child.py）又覆写父类丢失 inline childFlow 解析——manifest 的类型级
#: 条目对 python 是死条目；go 池已转正 reference（plaita-go ir.go:99）。
#: 引擎补注入器或 manifest 摘除死条目（台账方向②③）落地后同步摘除本表。
_REFERENCE_DEAD_RUNTIMES = frozenset({"python"})


def reference_errors(definition: Dict[str, Any], store: Any,
                     tenant_id: Optional[str],
                     runtime: Optional[str] = None) -> List[Dict[str, Any]]:
    """reference 节点的目标可解析性（本租户）与形状×池一致性。

    - 既无 flow_id 也无 childFlow → ``reference_target_unspecified``（error）：
      任何 runtime 都无法解析引用目标（schema 对 reference 豁免 childFlow
      检查、flow_id Optional——声称的字段校验兜底层不存在，validate 必须自报）；
    - 目标池对 reference 零可执行形状 → ``reference_unexecutable_on_runtime``
      （warning，可观测不阻断——台账方向①验收格；validate 通过 ≠ 可在该池
      运行，runtime 请指定 go）；
    - 不区分「不存在」与「存在但属其他租户」（避免跨租户存在性探测）；引用
      版本不做校验（版本可在保存/发布时点再落定）。跨定义追引用是 Phase 1
      的诚实边界：只查存在性，不递归被引用流程的内容。
    """
    if store is None:
        return []
    errors: List[Dict[str, Any]] = []
    for nid, ntype in _iter_typed_nodes(definition):
        if ntype != _REFERENCE_TYPE:
            continue
        node = _find_node(definition, nid) or {}
        flow_id = node.get("flow_id")
        if flow_id is None:
            flow_id = node.get("flowID")
        if not isinstance(flow_id, str) or not flow_id:
            if not (node.get("childFlow") or node.get("child_flow")):
                errors.append(_err(
                    "node", "reference_target_unspecified",
                    f"reference 节点 {nid!r} 既无 flow_id 也无 childFlow："
                    "任何 runtime 都无法解析引用目标（运行期必死）",
                    node_id=nid, field="flow_id",
                ))
            continue
        try:
            exists = store.get_flow_record(flow_id, tenant_id=tenant_id) is not None
        except Exception:  # noqa: BLE001 — 存储不可达时不误报引用错误
            exists = True
        if not exists:
            errors.append(_err(
                "node", "reference_target_missing",
                f"reference 节点 {nid!r} 引用的流程 {flow_id!r} 在当前租户不存在"
                "或不可见（不跨租户解析引用）",
                node_id=nid, field="flow_id",
            ))
        if runtime in _REFERENCE_DEAD_RUNTIMES:
            errors.append(_err(
                "node", "reference_unexecutable_on_runtime",
                f"reference 节点 {nid!r}：runtime {runtime!r} 池对 reference 零"
                "可执行形状（named-ref 无 flow_id 注入器、inline childFlow 被 "
                "setup_child_flow 覆写丢弃）；请将 runtime 指定为 go 或改用显式"
                "子流程节点——通过 validate 不代表可在该池运行",
                node_id=nid, severity=SEVERITY_WARNING,
            ))
    return errors


def _find_node(definition: Dict[str, Any], node_id: str) -> Optional[Dict[str, Any]]:
    for _path, nodes, _host in _definition_graphs(definition):
        for n in nodes:
            if isinstance(n, dict) and n.get("id") == node_id:
                return n
    return None


# ---------------------------------------------------------------------------
# 汇总入口
# ---------------------------------------------------------------------------

def validate_graph(graph: Any, *, runtime: str, redis: Any,
                   store: Any = None, tenant_id: Optional[str] = None) -> Dict[str, Any]:
    """GraphDTO 全量校验。返回 ``{valid, errors, definition}``。

    ``valid`` = 无 error 级条目；warning（降级提示/死边/ir 规则）不阻断。
    各校验阶段全量执行、错误累积（非首错即停）。
    """
    if not isinstance(graph, dict):
        return {"valid": False, "definition": {},
                "errors": [_err("node", "graph_dto.invalid",
                                "graph 必须是对象（{nodes, edges, viewport?, meta?}）")]}
    nodes = graph.get("nodes")
    if not isinstance(nodes, list):
        return {"valid": False, "definition": {},
                "errors": [_err("node", "graph_dto.invalid",
                                "graph.nodes 必须是列表（编辑器节点数组）")]}

    definition = graph_to_definition(graph)
    errors: List[Dict[str, Any]] = []
    errors.extend(shape_errors(graph))
    errors.extend(structure_errors(definition))
    errors.extend(edge_errors(graph))
    errors.extend(field_errors(definition))
    errors.extend(capability_errors(definition, runtime, redis))
    errors.extend(reference_errors(definition, store, tenant_id, runtime=runtime))
    valid = not any(e.get("severity") == SEVERITY_ERROR for e in errors)
    return {"valid": valid, "errors": errors, "definition": definition}


__all__ = [
    "SEVERITY_ERROR",
    "SEVERITY_WARNING",
    "capability_errors",
    "edge_errors",
    "field_errors",
    "graph_to_definition",
    "reference_errors",
    "shape_errors",
    "structure_errors",
    "validate_graph",
]
