"""
dry-run 服务：实例化 Flow 并同步执行，收集节点级结果。

使用 ``FlowExecution.run(flow, params, callback_handlers=[...])`` 注入采集回调，
按节点聚合 input(配置)/output(执行结果)/status。

安全闸门：拒绝含 code / 可执行脚本类危险节点的 flow，避免 console backend 进程 RCE。
"""
import logging
from typing import Any, Dict, List, Optional, Set

from plaita.core.callback import FlowCallback
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow

try:
    from .flow_store import get_flow_store
except ImportError:
    from flow_store import get_flow_store

logger = logging.getLogger(__name__)

# dry-run 禁止的节点类型（大小写不敏感）。code 可在 backend 进程执行任意 Python。
_BLOCKED_NODE_TYPES: Set[str] = {
    "code",
    "python",
    "javascript",
    "js",
}


class _CollectingCallback(FlowCallback):
    def __init__(self) -> None:
        self.nodes: List[Dict[str, Any]] = []
        self._started: Dict[str, Dict[str, Any]] = {}

    def on_node_start(self, flow, node, **kwargs) -> None:
        entry = {
            "id": getattr(node, "id", None),
            "type": getattr(node, "node_type", None),
            "name": getattr(node, "name", None) or getattr(node, "id", None),
            "input": getattr(node, "input", None),
            "output": None,
            "status": "running",
            "error": None,
        }
        self._started[entry["id"]] = entry
        self.nodes.append(entry)

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        nid = getattr(node, "id", None)
        entry = self._started.get(nid)
        if entry is None:
            entry = {
                "id": nid,
                "type": getattr(node, "node_type", None),
                "name": getattr(node, "name", None) or nid,
                "input": getattr(node, "input", None),
                "output": None,
                "status": "success",
                "error": None,
            }
            self.nodes.append(entry)
        entry["output"] = result
        if error is not None or exception is not None:
            entry["status"] = "error"
            entry["error"] = str(error or exception)
        else:
            entry["status"] = "success"


def apply_debug_transform(
    data: Dict[str, Any],
    pinned: Optional[Dict[str, Any]] = None,
    only_node: Optional[str] = None,
) -> Dict[str, Any]:
    """调试变换（仅顶层节点，子流程不动）：

    - pinned: 把指定节点替换为 mock（value=固定输出），后续试跑跳过真实执行
    - only_node: 除 start 与目标节点外全部替换为 mock——目标节点上游取
      pinned 值（未 pin 的上游为 None），下游不产生真实副作用
    """
    if not pinned and not only_node:
        return data
    nodes = data.get("nodes")
    if not isinstance(nodes, list):
        return data
    out: List[Any] = []
    for node in nodes:
        if not isinstance(node, dict):
            out.append(node)
            continue
        nid = node.get("id")
        if pinned and nid in pinned:
            out.append({**node, "type": "mock", "value": pinned[nid]})
        elif only_node and node.get("type") != "start" and nid != only_node:
            out.append({**node, "type": "mock", "value": None})
        else:
            out.append(node)
    return {**data, "nodes": out}


def _collect_blocked_nodes(data: Any, path: str = "nodes") -> List[str]:
    """递归扫描 flow dict / childFlow，收集被禁节点描述。"""
    blocked: List[str] = []
    if not isinstance(data, dict):
        return blocked
    nodes = data.get("nodes") or []
    if not isinstance(nodes, list):
        return blocked
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        ntype = str(node.get("type") or node.get("node_type") or "").strip().lower()
        nid = node.get("id") or f"[{i}]"
        if ntype in _BLOCKED_NODE_TYPES:
            blocked.append(f"{path}[{nid}].type={ntype}")
        child = node.get("childFlow") or node.get("child_flow")
        if isinstance(child, dict):
            blocked.extend(_collect_blocked_nodes(child, path=f"{path}[{nid}].childFlow"))
        for key in ("flows", "branches", "children"):
            nested = node.get(key)
            if isinstance(nested, list):
                for j, item in enumerate(nested):
                    if isinstance(item, dict):
                        blocked.extend(
                            _collect_blocked_nodes(item, path=f"{path}[{nid}].{key}[{j}]")
                        )
    return blocked


def dry_run(
    flow_json: str,
    input_data: Optional[Dict[str, Any]] = None,
    pinned: Optional[Dict[str, Any]] = None,
    only_node: Optional[str] = None,
) -> Dict[str, Any]:
    """对给定 Flow JSON 字符串做同步试跑，返回 {result, nodes, error}。

    - pinned: 节点输出固定（该节点替换为 mock，跳过真实执行）
    - only_node: 仅真实执行该节点（其余除 start 外替换为 mock）
    """
    import json

    try:
        data = json.loads(flow_json)
    except json.JSONDecodeError as e:
        return {"result": None, "nodes": [], "error": f"flowJson 非合法 JSON: {e}"}

    data = apply_debug_transform(data, pinned=pinned, only_node=only_node)

    blocked = _collect_blocked_nodes(data)
    if blocked:
        return {
            "result": None,
            "nodes": [],
            "error": (
                "dry-run 拒绝含危险节点的流程（防止 console 进程 RCE）: "
                + "; ".join(blocked)
            ),
        }

    try:
        flow = Flow.model_validate(data)
    except Exception as e:
        return {"result": None, "nodes": [], "error": f"Flow 校验失败: {e}"}

    collector = _CollectingCallback()
    try:
        result = FlowExecution().run(
            flow,
            params=input_data or {},
            callback_handlers=[collector],
        )
    except Exception as e:
        return {
            "result": None,
            "nodes": collector.nodes,
            "error": f"执行失败: {e}",
        }

    return {"result": result, "nodes": collector.nodes, "error": None}
