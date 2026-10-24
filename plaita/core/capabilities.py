"""plaita.core.capabilities — 节点执行能力契约的消费面（plaita#331 后半）。

词表（5 token，封闭集；owner 裁决 2026-10-24：不加第六 token，服务可达性是运行期事实）
与 profile 表住在 core：执行入口（``run_compatible`` / ``arun_compatible`` /
``run_distributed``）与 NodeRunner 兜底都在 core 消费，core 不反向 import node 层，
节点属性一律鸭子读取（``execution_requirements`` / ``is_suspending`` / ``child_flow`` /
``branches[].flow``）。``plaita.node.capabilities`` 再导出词表并承担注册期不变量
（#330 前半，公开 import 路径守恒）。

**三件事，一个判定函数**（A2 r61 评审 §3：两层勿写两套判定）：

- :func:`required_capabilities` / :func:`capability_closure` —— 递归闭包：child / loop
  族 / parallel 分支 + reference 引用的 flow（``resolve_reference`` 钩子；未注入的
  reference 记 ``unresolved``，不当能力缺口）。每个声明站点带帧路径（根→叶）。
- :func:`validate_for_profile` —— **拒绝面**：独立于 ``model_validate``（读面不碰），
  只在执行入口与 console publish/start 调用。闭包 token ⊄ profile 能力集即在**任何
  节点执行前**具名拒绝：缺 park/resume 结构族（``suspend:*`` / ``event_bus`` / ``queue``）
  → :class:`SuspendNotExecutableError`（``stage="entry"``，#317 机器判据
  ``suspend_not_executable`` 不变）；只缺 ``network`` / ``sandbox`` →
  :class:`ExecutionProfileError`。
- :func:`assert_node_executable` —— **兜底面**：NodeRunner 在每个 suspending 节点
  ``execute`` 前断言宿主契约（宿主身份取执行链的根），覆盖绕过 strategies 的旁路；
  正常流不应触达，触达即入口漏了——具名错误 + WARNING，不静默。

**profile 判据 = ADR-001 runtime 组合**（docs/governance/adr-001-runtime-composition.md）：

| profile | 组合 | 满足的 token |
|---|---|---|
| ``normal`` / ``generator`` | Python reference 进程内（``flow.run`` / 单步调试） | network, sandbox |
| ``distributed`` | Python 唯一生产运行时的 worker 池（Redis 队列 + 事件总线 + delay_service） | 全集 |
| ``local`` | console local = Python 引擎进程内后端（§3.4：InMemoryEventBus + SQLite checkpoint，**无**持久队列/延迟排程） | 全集 − queue |

自定义 profile 以 token 集合（iterable）直接给出。``distributed`` 入口**只判 token 不判帧
类型**：loop/parallel 帧内挂起由运行期作用域闸（NS-M3 ``frame boundary``）照常具名拒，
入口不预判（传播面守恒）；normal 入口的 ``fix`` 则按帧类型分 ``run_distributed`` /
``restructure``（与根出口呈现同口径，单源 :data:`SUSPEND_ALLOWED_FRAME_NODE_TYPES`）。

回滚旋钮 ``PLAITA_SUSPEND_GUARD=warn``（#330 §7，2026-11-30 到期删除）同样降级本模块的
入口拒绝与 runner 兜底（WARNING + 放行，交回运行期闸/呈现）；只降挂起族，
``ExecutionProfileError`` 不受旋钮影响。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from plaita.core.errors import (
    ExecutionProfileError,
    SuspendNotExecutableError,
    suspend_guard_mode,
)

logger = logging.getLogger("plaita.core.capabilities")

#: 合法能力 token 全集（词表封闭：新 token = 运行时能力面变更，须改这里 + 治理文档 + 账本）。
EXECUTION_CAPABILITY_TOKENS: FrozenSet[str] = frozenset(
    {
        "suspend:root",
        "suspend:nested",
        "event_bus",
        "queue",
        "sandbox",
        "network",
    }
)

#: suspend 家族（is_suspending 节点的最低声明要求）。
SUSPEND_CAPABILITY_TOKENS: FrozenSet[str] = frozenset({"suspend:root", "suspend:nested"})

#: park/resume 结构族：缺任一即「挂起节点在此宿主不可执行」（SuspendNotExecutableError）；
#: 其余（network / sandbox）缺口走 ExecutionProfileError。
PARK_CAPABILITY_TOKENS: FrozenSet[str] = frozenset({"suspend:root", "suspend:nested", "event_bus", "queue"})

#: 挂起作用域闸放行的帧类型（v1：只有 child/reference 调用链内的挂起可穿透到分布式
#: 恢复边界）。单一事实源——运行期闸（strategies ``_scope_gate_blocker``）与入口静态
#: 闭包的 ``fix=restructure`` 判定共用。
SUSPEND_ALLOWED_FRAME_NODE_TYPES: FrozenSet[str] = frozenset({"child", "reference"})

#: 内置 profile → 满足的 token 集（ADR-001 runtime 组合，见模块 docstring 表）。
EXECUTION_PROFILES: Dict[str, FrozenSet[str]] = {
    "normal": frozenset({"network", "sandbox"}),
    "generator": frozenset({"network", "sandbox"}),
    "distributed": EXECUTION_CAPABILITY_TOKENS,
    "local": EXECUTION_CAPABILITY_TOKENS - {"queue"},
}

#: profile 形参：内置名，或自定义 token 集合。
ProfileSpec = Union[str, Iterable[str]]

_MODE_TO_PROFILE = {
    None: "normal",
    "normal": "normal",
    "generator": "generator",
    "distributed": "distributed",
}


def profile_name(profile: ProfileSpec) -> str:
    """profile 的呈现名（错误体 ``profile`` 键）：内置名原样，自定义集合为 ``custom:{...}``。"""
    if isinstance(profile, str):
        return profile
    return "custom:{" + ",".join(sorted(str(t) for t in profile)) + "}"


def profile_tokens(profile: ProfileSpec) -> FrozenSet[str]:
    """解析 profile 为 token 集；未知内置名 / 自定义集合含未知 token 具名拒绝。"""
    if isinstance(profile, str):
        try:
            return EXECUTION_PROFILES[profile]
        except KeyError:
            raise ValueError(
                f"未知执行 profile {profile!r}（内置: {sorted(EXECUTION_PROFILES)}；"
                "自定义 profile 请直接给出 token 集合）"
            ) from None
    tokens = frozenset(profile)
    unknown = sorted(t for t in tokens if not (isinstance(t, str) and t in EXECUTION_CAPABILITY_TOKENS))
    if unknown:
        raise ValueError(
            f"自定义 profile 含未知 token {unknown}（合法词表: {sorted(EXECUTION_CAPABILITY_TOKENS)}）"
        )
    return tokens


def profile_for_mode(mode: Any) -> str:
    """执行模式 → 内置 profile 名（``ExecutionMode`` 枚举 / 字符串 / None 均可，鸭子读 ``.value``）。"""
    value = getattr(mode, "value", mode)
    try:
        return _MODE_TO_PROFILE[value]
    except KeyError:
        raise ValueError(f"无法为执行模式 {mode!r} 解析 profile") from None


# ---------------------------------------------------------------------------
# 递归闭包
# ---------------------------------------------------------------------------


@dataclass
class RequirementSite:
    """flow 闭包里一个带执行要求的节点站点。

    ``frame_path`` 根→叶、含站点自身，元素形如 ``"call(child)"``（与 #330 根出口呈现的
    ``frame_path`` 同形）；``frames`` 为帧节点对象（根→叶，不含站点），供 ``fix``
    判定。``cause``：``declared``（有声明）/ ``undeclared``（is_suspending 却无
    suspend token——过渡期旋钮放行的注册，闭包按 fail-closed 记 implicit
    ``suspend:root``）。
    """

    node: Any
    node_id: str
    node_type: str
    flow_id: Optional[str]
    frame_path: List[str]
    frames: List[Any]
    tokens: FrozenSet[str]
    cause: str

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "flow_id": self.flow_id,
            "frame_path": list(self.frame_path),
            "tokens": sorted(self.tokens),
            "cause": self.cause,
        }


@dataclass
class CapabilityClosure:
    required: FrozenSet[str]
    sites: List[RequirementSite]
    unresolved: List[dict]


def _label(node: Any) -> str:
    return f"{getattr(node, 'id', '?')}({getattr(node, 'node_type', '?')})"


def _node_tokens(node: Any) -> Tuple[FrozenSet[str], str]:
    """节点声明 → (token 集, cause)。未知 token 在注册期已被拒，此处只滤不报。"""
    declared = frozenset(
        t
        for t in (getattr(node, "execution_requirements", ()) or ())
        if isinstance(t, str) and t in EXECUTION_CAPABILITY_TOKENS
    )
    if getattr(node, "is_suspending", False) is True and not (declared & SUSPEND_CAPABILITY_TOKENS):
        return declared | {"suspend:root"}, "undeclared"
    return declared, "declared"


def _walk(
    flow: Any,
    *,
    recursive: bool,
    resolver: Optional[Callable[[Any], Any]],
    frames: Tuple[Any, ...],
    sites: List[RequirementSite],
    unresolved: List[dict],
    seen: set,
) -> None:
    if flow is None or id(flow) in seen:
        return
    seen.add(id(flow))
    flow_id = getattr(flow, "flow_id", None)
    for node in getattr(flow, "nodes", None) or []:
        if isinstance(node, dict):
            # 未解析节点看不见声明——入口在 _ensure_flow_resolved 之后调用，此处只会在
            # 绕过解析的裸 Flow(...) 上出现；覆盖缩水必须可观测。
            logger.warning(
                "capability closure: flow %r node %r is unresolved (dict); its declaration is invisible",
                flow_id, node.get("id") if isinstance(node, dict) else node,
            )
            continue
        tokens, cause = _node_tokens(node)
        if tokens:
            sites.append(
                RequirementSite(
                    node=node,
                    node_id=str(getattr(node, "id", "?")),
                    node_type=str(getattr(node, "node_type", "?")),
                    flow_id=flow_id,
                    frame_path=[_label(f) for f in frames] + [_label(node)],
                    frames=list(frames),
                    tokens=tokens,
                    cause=cause,
                )
            )
        if not recursive:
            continue
        child = getattr(node, "child_flow", None)
        if child is not None:
            _walk(child, recursive=True, resolver=resolver, frames=frames + (node,),
                  sites=sites, unresolved=unresolved, seen=seen)
        elif getattr(node, "node_type", None) == "reference":
            resolved = resolver(node) if resolver is not None else None
            if resolved is not None:
                _walk(resolved, recursive=True, resolver=resolver, frames=frames + (node,),
                      sites=sites, unresolved=unresolved, seen=seen)
            else:
                unresolved.append({"node_id": str(getattr(node, "id", "?")),
                                   "flow_id": getattr(node, "flow_id", None)})
        for pb in getattr(node, "branches", None) or []:
            bflow = getattr(pb, "flow", None)
            if bflow is not None:
                _walk(bflow, recursive=True, resolver=resolver, frames=frames + (node,),
                      sites=sites, unresolved=unresolved, seen=seen)


def capability_closure(
    flow: Any,
    *,
    recursive: bool = True,
    resolve_reference: Optional[Callable[[Any], Any]] = None,
) -> CapabilityClosure:
    """递归闭包：返回 flow（含 child/loop 族/parallel 分支/已解析 reference）的全部声明站点。"""
    sites: List[RequirementSite] = []
    unresolved: List[dict] = []
    _walk(flow, recursive=recursive, resolver=resolve_reference, frames=(),
          sites=sites, unresolved=unresolved, seen=set())
    required = frozenset().union(*(s.tokens for s in sites)) if sites else frozenset()
    return CapabilityClosure(required=required, sites=sites, unresolved=unresolved)


def required_capabilities(
    flow: Any,
    recursive: bool = True,
    *,
    resolve_reference: Optional[Callable[[Any], Any]] = None,
) -> FrozenSet[str]:
    """flow 的执行要求 token 并集（``recursive=False`` 只看根层节点）。"""
    return capability_closure(flow, recursive=recursive, resolve_reference=resolve_reference).required


# ---------------------------------------------------------------------------
# profile 校验（拒绝面）
# ---------------------------------------------------------------------------


def _evaluate(flow, profile, *, recursive, resolve_reference):
    tokens = profile_tokens(profile)
    closure = capability_closure(flow, recursive=recursive, resolve_reference=resolve_reference)
    missing = closure.required - tokens
    report = {
        "profile": profile_name(profile),
        "required": sorted(closure.required),
        "missing": sorted(missing),
        "sites": [s.to_dict() for s in closure.sites],
        "unresolved": list(closure.unresolved),
    }
    return closure, missing, report


def check_execution_requirements(
    flow: Any,
    profile: ProfileSpec,
    *,
    recursive: bool = True,
    resolve_reference: Optional[Callable[[Any], Any]] = None,
) -> dict:
    """非致命报告 ``{profile, required, missing, sites, unresolved}``（flow_compile 作者期信号用）。"""
    return _evaluate(flow, profile, recursive=recursive, resolve_reference=resolve_reference)[2]


def _suspend_error(
    site: RequirementSite,
    profile: ProfileSpec,
    missing: FrozenSet[str],
    *,
    required: Sequence[str],
    stage: str,
) -> SuspendNotExecutableError:
    node = site.node
    pname = profile_name(profile)
    site_missing = sorted(site.tokens & missing)
    blocker = next(
        (f for f in site.frames if getattr(f, "node_type", None) not in SUSPEND_ALLOWED_FRAME_NODE_TYPES),
        None,
    )
    if blocker is not None:
        fix = {
            "action": "restructure",
            "call": None,
            "message": (
                f"挂起节点位于 {getattr(blocker, 'node_type', '?')!r} 帧 {getattr(blocker, 'id', '?')!r} 内："
                "v1 仅支持 child/reference 调用链内挂起，分布式运行时同样具名拒绝；"
                "请把挂起节点移出迭代/并行帧"
            ),
        }
    else:
        fix = {
            "action": "run_distributed",
            "call": None,
            "message": (
                f"目标 profile {pname!r} 不具备 {site_missing}：事件/审批/延时等待需要分布式运行时"
                "（事件总线 + 持久队列/延迟排程）。请到 console 将流程发布到分布式池验证，或使用 "
                "FlowExecution.run_distributed(...) 逐步驱动；本地 normal/generator 无法执行"
            ),
        }
    loc = f"，source line {node.source_line}" if getattr(node, "source_line", None) is not None else ""
    message = (
        "这不是源码缺陷，不要删除/替换该节点：\n"
        f"挂起节点 {site.node_id!r} ({site.node_type}{loc}) 在 flow {site.flow_id!r} 需要宿主能力 "
        f"{sorted(site.tokens)}，目标 profile {pname!r} 缺少 {site_missing}；"
        f"流程已在节点执行前具名拒绝（stage={stage}）。"
    )
    return SuspendNotExecutableError(
        message,
        node=node,
        source_line=getattr(node, "source_line", None),
        flow_id=site.flow_id,
        cause="environment" if site.cause == "declared" else "undeclared",
        fix=fix,
        frame_path=list(site.frame_path),
        stage=stage,
        profile=pname,
        required=list(required),
        missing=sorted(missing),
    )


def _build_error(flow, closure: CapabilityClosure, profile, missing: FrozenSet[str], report: dict, stage: str):
    offending = [s for s in closure.sites if s.tokens & missing]
    park_sites = [s for s in offending if s.tokens & missing & PARK_CAPABILITY_TOKENS]
    if park_sites:
        return _suspend_error(park_sites[0], profile, missing, required=report["required"], stage=stage)
    pname = profile_name(profile)
    message = (
        f"flow {getattr(flow, 'flow_id', None)!r} 的执行要求 {report['required']} 不被目标 profile "
        f"{pname!r} 满足，缺少 {report['missing']}（站点: "
        + ", ".join("/".join(s.frame_path) for s in offending)
        + "）；流程已在节点执行前具名拒绝。"
    )
    return ExecutionProfileError(
        message,
        profile=pname,
        required=report["required"],
        missing=report["missing"],
        flow_id=getattr(flow, "flow_id", None),
        sites=[s.to_dict() for s in offending],
        fix={
            "action": "choose_profile",
            "call": None,
            "message": f"选择具备 {report['missing']} 的执行 profile，或移除/替换需要这些宿主能力的节点",
        },
    )


def validate_for_profile(
    flow: Any,
    profile: ProfileSpec,
    *,
    recursive: bool = True,
    resolve_reference: Optional[Callable[[Any], Any]] = None,
    stage: str = "entry",
) -> dict:
    """执行入口 / publish / start 的拒绝面：闭包 token ⊄ profile 即具名拒绝。

    满足时返回与 :func:`check_execution_requirements` 同形的报告。**独立于
    ``model_validate``**：读面（解析/展示/编辑）永不调用本函数。
    """
    closure, missing, report = _evaluate(flow, profile, recursive=recursive, resolve_reference=resolve_reference)
    if not missing:
        return report
    err = _build_error(flow, closure, profile, missing, report, stage)
    if isinstance(err, SuspendNotExecutableError) and suspend_guard_mode() == "warn":
        logger.warning(
            "[suspend-guard=warn] %s validation degraded to WARNING (profile %r missing %s); "
            "falling back to the runtime suspend gate: %s",
            stage, report["profile"], report["missing"], err.to_dict(),
        )
        return report
    raise err


# ---------------------------------------------------------------------------
# 执行入口 / NodeRunner 接线助手
# ---------------------------------------------------------------------------


def host_profile(execution: Any) -> ProfileSpec:
    """执行链**根**的宿主 profile：显式 ``execution_profile`` 优先，否则由根的 mode 推导。

    子执行继承根的宿主身份（distributed 根下的 child 子执行虽经 run_compatible 驱动，
    宿主仍是分布式池）。
    """
    root = execution
    while getattr(root, "parent", None) is not None:
        root = root.parent
    explicit = getattr(root, "execution_profile", None)
    if explicit is not None:
        return explicit
    return profile_for_mode(getattr(root, "mode", None))


def gate_execution_entry(execution: Any, flow: Any) -> Optional[dict]:
    """执行入口闸：仅模式根（``parent is None``）校验；子执行不闸（递归闭包已在根算过，
    NS-M3 传播面不动）。返回报告或 ``None``（子执行）。"""
    if getattr(execution, "parent", None) is not None:
        return None
    return validate_for_profile(flow, host_profile(execution), stage="entry")


def assert_node_executable(node: Any, execution: Any, *, flow: Any = None) -> None:
    """NodeRunner 兜底：suspending 节点 ``execute`` 前断言宿主契约（含子流程，宿主取执行链根）。

    正常流不应触达——入口闭包已覆盖；触达即入口漏了（静态闭包看不见的复合节点、绕过
    strategies 直接驱动 runner 的旁路）：具名错误 + WARNING，不静默。无宿主身份的裸
    runner（``execution is None``）无法判定，交回运行期 carrier 闸。
    """
    if getattr(node, "is_suspending", False) is not True:
        return
    if execution is None:
        logger.debug("runner guard skipped: bare NodeRunner without host execution (node %r)", getattr(node, "id", "?"))
        return
    profile = host_profile(execution)
    tokens = profile_tokens(profile)
    declared, cause = _node_tokens(node)
    missing = declared - tokens
    if not missing:
        return
    site = RequirementSite(
        node=node,
        node_id=str(getattr(node, "id", "?")),
        node_type=str(getattr(node, "node_type", "?")),
        flow_id=getattr(flow, "flow_id", None),
        frame_path=[_label(node)],
        frames=[],
        tokens=declared,
        cause=cause,
    )
    err = _suspend_error(site, profile, missing, required=sorted(declared), stage="runner")
    if suspend_guard_mode() == "warn":
        logger.warning(
            "[suspend-guard=warn] runner guard degraded to WARNING for suspending node %r (profile %r missing %s): %s",
            site.node_id, profile_name(profile), sorted(missing), err.to_dict(),
        )
        return
    logger.warning(
        "[execution-capabilities] runner guard tripped before execute of suspending node %r — "
        "the entry gate missed it (profile %r missing %s): %s",
        site.node_id, profile_name(profile), sorted(missing), err.to_dict(),
    )
    raise err


# ---------------------------------------------------------------------------
# 矩阵差分（治理账本断言）
# ---------------------------------------------------------------------------


def diff_capability_matrix(ledger: Mapping[str, Sequence[str]], matrix: Mapping[str, Sequence[str]]) -> dict:
    """账本 vs 注册表矩阵的差分：``{added, removed, changed}`` 只含非空键；空 dict = 一致。

    承重墙（A2 r61 §1.3）：注册表新增 node_type 而账本未变 → ``added`` 非空 → 红。
    """
    added = {k: list(v) for k, v in matrix.items() if k not in ledger}
    removed = {k: list(v) for k, v in ledger.items() if k not in matrix}
    changed = {
        k: {"ledger": list(ledger[k]), "registry": list(matrix[k])}
        for k in ledger
        if k in matrix and list(ledger[k]) != list(matrix[k])
    }
    out: dict = {}
    if added:
        out["added"] = added
    if removed:
        out["removed"] = removed
    if changed:
        out["changed"] = changed
    return out
