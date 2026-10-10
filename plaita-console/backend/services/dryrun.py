"""
dry-run 服务：实例化 Flow 并同步执行，收集节点级结果。

使用 ``FlowExecution.run(flow, params, callback_handlers=[...])`` 注入采集回调，
按节点聚合 input(配置)/output(执行结果)/status，并维护主/子流程层级
（``depth`` / ``flow_path`` / ``flow_id``，供试跑面板做子图缩进）。

安全闸门（两层）：

1. 名黑名单：拒绝含 code / 可执行脚本类危险节点的 flow——这些节点无 dry_run
   契约，只能整体拒跑。
2. dry-run 全局旗标：注入 ``globalContext.dry_run=True``，并在执行器读点
   ``get_global_variable`` 上钉死（``_DryRunExecution``）。业务节点（经
   ``PLAITA_CONSOLE_NODE_MODULES`` 注册的 gate / capture / agentrun / llm /
   email / webhooks / database / git_publish 等）按契约读
   ``execution.get_global_variable("dry_run", False)``，为真即跳过真实副作用
   （子进程 / 出网 / agent CLI）。名黑名单只能枚举内置类型，拦不住动态注册的
   业务节点——这一层才是它们不真正执行的关键。
   仅注入 globalContext 不够：子流程自带 ``globalContext.dry_run=false`` 时，
   子级 context 优先读自己的 $GLOBAL（缺失才回退父级），副作用会被重新打开；
   读点钉死随 ``get_child_execution`` 下发到 inline child / parallel 分支 /
   loop 每轮，流程声明层无从绕过。
"""
import logging
import threading
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

# dry-run 全局旗标键。业务节点的 dry_run 契约：``execution.get_global_variable(
# "dry_run", False)`` 为真即跳过真实副作用（plaita-nodes 的 gate / capture /
# agentrun 均如此）。置位后它们不再在 console 进程内 spawn 子进程 / 出网。
_DRY_RUN_FLAG = "dry_run"


class _DryRunExecution(FlowExecution):
    """试跑专用执行器：dry_run 旗标在唯一读点上恒真，流程无从自解。

    ``get_global_variable`` 是业务节点 dry-run 契约的读点（plaita-nodes 全部
    带副作用节点均以 ``execution.get_global_variable("dry_run", False)`` 判定）。
    只在 ``globalContext`` 里写值是不够的——子流程自带 ``dry_run=false`` 会覆盖
    父级注入；这里把读点钉死，任何嵌套层级的声明都改不回去。

    ``get_child_execution`` 必须同构覆写（基类实现硬编码 ``FlowExecution``）：
    inline child / parallel 分支 / loop 每轮的子执行都经它产出，子级于是同样是
    本类实例。
    """

    def get_global_variable(self, key: str, default: Any = None) -> Any:
        if key == _DRY_RUN_FLAG:
            return True
        return super().get_global_variable(key, default)

    def get_child_execution(self):
        child = _DryRunExecution(self, callback_manager=self.callback_manager.child())
        child.mode = self.mode
        return child


class _FlowCtx:
    """flow 执行上下文（层级归因用）。

    ``ref`` 持有 flow 对象本身：注册期间对象不可回收，id() 不会复用。
    ``spawned_by`` 记录启动本 flow 的节点 id()，供跨线程反查时向上爬链。
    """

    __slots__ = ("depth", "path", "flow_id", "spawned_by", "ref")

    def __init__(self, depth: int, path: List[str], flow_id: Optional[str], spawned_by: Optional[int], ref: Any):
        self.depth = depth
        self.path = path
        self.flow_id = flow_id
        self.spawned_by = spawned_by
        self.ref = ref


class _CollectingCallback(FlowCallback):
    """采集节点级结果 + 主/子流程层级。

    层级归因（2026-09 试跑面板子图缩进）：
    - ``on_node_start`` 的 ``flow`` 参数即节点所属 flow，ctx 直接查表，精确；
    - ``on_flow_start`` 只带 flow 不带来源，需反查启动节点：
      * 同线程（inline child 与父同线程执行）取线程本地节点栈顶；
      * 跨线程（parallel 默认 thread 池，分支 flow 在 worker 里启动）取最近
        启动的未结束节点，再沿 ``ctx.spawned_by`` 链爬到仍 open 的启动节点——
        使同一 parallel 的各兄弟分支归到共同父，而非互相嵌套。
    - 已知边界：跨线程事件到达顺序由调度决定，极端交错下个别 flow 的
      depth/path 仍可能偏差；节点自身的 input/output 数据不受影响。
    """

    def __init__(self) -> None:
        self.nodes: List[Dict[str, Any]] = []
        self._started: Dict[Any, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        # id(flow) -> _FlowCtx（含 ref，注册期内 id 不复用）
        self._ctx_of: Dict[int, _FlowCtx] = {}
        # id(node) -> 启动节点记录（节点结束即清除）
        self._open_nodes: Dict[int, Dict[str, Any]] = {}
        self._seq = 0
        self._tls = threading.local()

    # ── flow 生命周期 ────────────────────────────────────────────────

    def on_flow_start(self, flow, **kwargs) -> None:
        with self._lock:
            spawn = self._find_spawn_node_record()
            fid = getattr(flow, "flow_id", None)
            if spawn is None:
                ctx = _FlowCtx(0, [fid or "root"], fid, None, flow)
            else:
                label = fid or spawn["name"] or "子流程"
                ctx = _FlowCtx(
                    spawn["ctx"].depth + 1,
                    spawn["ctx"].path + [label],
                    fid,
                    spawn["node_id"],
                    flow,
                )
            self._ctx_of[id(flow)] = ctx

    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs) -> None:
        with self._lock:
            self._ctx_of.pop(id(flow), None)

    def _find_spawn_node_record(self) -> Optional[Dict[str, Any]]:
        """定位正在启动的 flow 的启动节点记录（调用方持有锁）。"""
        stack = getattr(self._tls, "node_stack", None)
        if stack:
            rec = self._open_nodes.get(stack[-1])
            if rec is not None:
                return rec
        if not self._open_nodes:
            return None
        # 跨线程：最近启动的未结束节点，沿 spawned_by 爬到仍 open 的启动节点
        rec = max(self._open_nodes.values(), key=lambda r: r["seq"])
        seen: Set[int] = set()
        while rec is not None:
            rid = rec["ctx"].spawned_by
            if rid is None or rid in seen:
                break
            seen.add(rid)
            candidate = self._open_nodes.get(rid)
            if candidate is None:
                break
            rec = candidate
        return rec

    # ── 节点生命周期 ────────────────────────────────────────────────

    def on_node_start(self, flow, node, **kwargs) -> None:
        with self._lock:
            ctx = self._ctx_of.get(id(flow))
            if ctx is None:
                # 防御：flow_start 未经过本采集器（外部构造的执行）——按根层处理
                fid = getattr(flow, "flow_id", None)
                ctx = _FlowCtx(0, [fid or "root"], fid, None, flow)
                self._ctx_of[id(flow)] = ctx
            entry = {
                "id": getattr(node, "id", None),
                "type": getattr(node, "node_type", None),
                "name": getattr(node, "name", None) or getattr(node, "id", None),
                "input": getattr(node, "input", None),
                "output": None,
                "status": "running",
                "error": None,
                "depth": ctx.depth,
                "flow_path": list(ctx.path),
                "flow_id": ctx.flow_id,
            }
            self._started[entry["id"]] = entry
            self.nodes.append(entry)
            self._seq += 1
            self._open_nodes[id(node)] = {
                "ref": node,
                "node_id": id(node),
                "name": entry["name"],
                "ctx": ctx,
                "seq": self._seq,
            }
            stack = getattr(self._tls, "node_stack", None)
            if stack is None:
                stack = []
                self._tls.node_stack = stack
            stack.append(id(node))

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        nid = getattr(node, "id", None)
        with self._lock:
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
                    "depth": 0,
                    "flow_path": [],
                    "flow_id": getattr(flow, "flow_id", None),
                }
                self.nodes.append(entry)
            entry["output"] = result
            if error is not None or exception is not None:
                entry["status"] = "error"
                entry["error"] = str(error or exception)
            else:
                entry["status"] = "success"
            self._open_nodes.pop(id(node), None)
            stack = getattr(self._tls, "node_stack", None)
            if stack and id(node) in stack:
                # 正常路径同线程 LIFO；防御性按值移除
                stack.remove(id(node))


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
    synthetic_ends: List[Any] = []
    for node in nodes:
        if not isinstance(node, dict):
            out.append(node)
            continue
        nid = node.get("id")
        mock_value = None
        needs_mock = False
        if pinned and nid in pinned:
            mock_value = pinned[nid]
            needs_mock = True
        elif only_node and node.get("type") != "start" and nid != only_node:
            mock_value = None
            needs_mock = True
        if needs_mock:
            replacement = {**node, "type": "mock", "value": mock_value}
            # 引擎对「非 End 节点缺 next」响亮报错（静默收尾已移除）。被替换的
            # 节点若是 end / 无后继，mock 会悬空——接一个合成 End，输出保持
            # ``{node_id: pinned 值}``（与历史静默收尾的 result 形状一致，见
            # e2e dryrun pinned 用例）。
            if not node.get("next") or node.get("type") == "end":
                sid = f"{nid}__pinned_end"
                replacement["next"] = sid
                synthetic_ends.append({
                    "id": sid, "type": "end",
                    "output": {nid: "$NODE." + nid},
                    "result_type": "success",
                })
            out.append(replacement)
        else:
            out.append(node)
    if synthetic_ends:
        out.extend(synthetic_ends)
    return {**data, "nodes": out}


def _collect_blocked_nodes(data: Any, path: str = "nodes", _depth: int = 0) -> List[str]:
    """递归扫描 flow dict / 嵌套节点结构，收集被禁节点描述。

    2026-09 安全评审 P0 修复：原实现只沿固定键（childFlow/flows/branches/children）
    下钻，且把 branches[i] 当 flow dict 找 nodes——而 Parallel 的嵌套 flow 挂在
    ``branches[i].flow``，藏进分支的危险节点（code 等）永远不会被扫描，dry-run
    闸门被整个绕过。改为泛型遍历：含 ``type``/``node_type`` 的 dict 一律视为节点
    （检查并入其所有值下钻），含 ``nodes`` 的 dict 视为子流程，其余容器继续下钻。
    """
    blocked: List[str] = []
    if _depth > 32:  # 防御性深度上限；JSON 结构本无环
        return blocked
    if isinstance(data, dict):
        ntype = str(data.get("type") or data.get("node_type") or "").strip().lower()
        if ntype:
            nid = data.get("id") or "?"
            if ntype in _BLOCKED_NODE_TYPES:
                blocked.append(f"{path}[{nid}].type={ntype}")
            for key, value in data.items():
                blocked.extend(
                    _collect_blocked_nodes(value, path=f"{path}[{nid}].{key}", _depth=_depth + 1)
                )
            return blocked
        if "nodes" in data:
            for i, node in enumerate(data.get("nodes") or []):
                blocked.extend(
                    _collect_blocked_nodes(node, path=path, _depth=_depth + 1)
                )
            return blocked
        for key, value in data.items():
            blocked.extend(
                _collect_blocked_nodes(value, path=f"{path}.{key}", _depth=_depth + 1)
            )
        return blocked
    if isinstance(data, list):
        for i, item in enumerate(data):
            blocked.extend(
                _collect_blocked_nodes(item, path=f"{path}[{i}]", _depth=_depth + 1)
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

    # 安全闸门第 2 层（2026-10 评审 P1）：名黑名单只能拦内置 code/python/js，
    # 拦不住经 PLAITA_CONSOLE_NODE_MODULES 注册的业务节点。统一把
    # globalContext.dry_run 置真，让按契约自守的业务节点（gate / capture /
    # agentrun）跳过真实执行；流程显式声明 dry_run=false 也强制覆盖——试跑
    # 语义下不得有副作用。注入覆盖读 $GLOBAL 状态的一侧（表达式 / 状态直读），
    # 业务节点读点一侧由 _DryRunExecution 钉死（子流程声明无从绕过）。
    flow.global_context = {**(flow.global_context or {}), _DRY_RUN_FLAG: True}

    collector = _CollectingCallback()
    try:
        result = _DryRunExecution.run(
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
