"""
plaita.core.flow — Flow model definition and entry-point helpers.

Contains the canonical Flow class, parse(), and parse_and_run().
"""

from __future__ import annotations

import json
import warnings
from typing import Any, ClassVar, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from plaita.core import types
from plaita.core.errors import FlowStartMissingError, NodeNotFoundError
# FlowExecution 在 flow.run/arun/debug 便捷方法里被实例化。executor 顶部并不反引
# flow (仅 TYPE_CHECKING 引 EventBus), 故这是一条单向依赖, 可在模块顶部坦诚声明,
# 不必藏在方法里装作没有 (历史遗留的伪 band-aid)。
from plaita.core.executor import FlowExecution
from plaita.io import Property
from plaita.logger import logger
from plaita.node import End, Node, Start


class Flow(BaseModel):
    """流程定义

    ``flow_id`` is the canonical identifier.  The legacy ``id`` accessor
    still works but emits a DeprecationWarning.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    flow_id: Optional[str] = None
    version: Optional[str] = None
    runtime: str = "python"
    input_type: Optional[Union[Property, str]] = None
    output_type: Optional[Union[Property, str]] = None
    # ``nodes`` 接受 dict (JSON/YAML 原始结构) 或已解析的 ``Node`` 实例。节点
    # 解析**不再**在 ``parse_flow`` validator 里发生——那会隐式耦合到模块级
    # 默认 registry 的状态。改由 ``resolve_nodes()`` 显式解析，``model_validate``
    # / ``model_validate_json`` 默认用 ``get_default_registry()`` 自动调一次，
    # 保持 ``Flow.from_string`` / ``parse`` 等常规入口开箱即用。
    nodes: Optional[List[Any]] = Field(default_factory=list)
    author: Optional[str] = ""
    desc: Optional[str] = ""
    metadata: Optional[Dict] = Field(default_factory=dict)
    timeout: Optional[str] = ""
    global_context: Optional[Dict] = Field(default_factory=dict)
    # ``$ENV`` 暴露给流程表达式的环境变量 allowlist。默认空——避免黑名单遗漏
    # 的 secret（如 STRIPE_KEY / OPENAI_API_KEY / PG_CONN）通过 ``$ENV.XXX``
    # 流入流程表达式或被 ``to_dict()`` 序列化进分布式 checkpoint。
    # 用户需要环境变量时显式列出：``Flow(..., expose_env=["HOME", "API_BASE"])``。
    expose_env: Optional[List[str]] = Field(default_factory=list, alias="exposeEnv")

    # 节点 id -> Node 的索引, 惰性构建, 让 find_node_by_id/start_node 不必每次线性扫描
    _node_index: Dict[str, Node] = PrivateAttr(default_factory=dict)
    # 缓存指纹: ``(id(self.nodes), tuple(node.id for node in nodes))``。访问索引前
    # 比对一次, 不一致就重建。消除旧实现 "len==len 即认为有效" 的 staleness——
    # 节点被替换、id 被原地修改、列表被换引用都会被抓到。指纹比对本身 O(n),
    # 但只在 ``_ensure_index`` 实际需要重建的同一刻才会跑; 命中缓存时直接返回。
    _node_index_sig: tuple = PrivateAttr(default_factory=tuple)

    @property
    def id(self) -> Optional[str]:
        """Deprecated — use ``flow_id`` instead."""
        warnings.warn(
            "Flow.id is deprecated, use Flow.flow_id instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.flow_id

    @model_validator(mode="before")
    @classmethod
    def parse_flow(cls, data: Dict) -> Dict:
        """Parse flow definition and handle field name compatibility."""
        if not data:
            raise ValueError("Flow content cannot be empty")

        normalized = data.copy()

        # Normalize id / flowId → flow_id (flow_id takes priority)
        if "flow_id" not in normalized or normalized.get("flow_id") is None:
            for legacy_key in ("flowId", "id"):
                if legacy_key in normalized and normalized[legacy_key] is not None:
                    normalized["flow_id"] = normalized.pop(legacy_key)
                    break
        # Remove legacy keys so Pydantic doesn't choke on unknown fields
        normalized.pop("flowId", None)
        normalized.pop("id", None)

        camel_mappings = {
            "inputType": "input_type",
            "outputType": "output_type",
            "globalContext": "global_context",
        }
        for old_key, new_key in camel_mappings.items():
            if old_key in normalized:
                value = normalized.pop(old_key)
                if value is not None:
                    normalized[new_key] = value

        for key in ["input_type", "output_type"]:
            if key in normalized:
                normalized[key] = Property.from_json(normalized[key])

        # 节点解析已移除——历史上此处调 ``get_default_registry().parse_node``
        # 把每个 node dict 解析成对应 ``Node`` 子类。这让 Pydantic validator
        # 产生副作用并隐式耦合到模块级默认 registry 的当前状态 (import 期注册了
        # 坏节点会让全进程解析任何 flow 都坏)。现在节点保持为原始 dict，由
        # ``Flow.resolve_nodes`` 显式解析，``model_validate`` 默认自动调一次。
        return normalized

    # parse_flow validator 消费的遗留键（input_type 等字段无 alias）
    LEGACY_KEYS: ClassVar[frozenset] = frozenset(
        {"flowId", "id", "inputType", "outputType", "globalContext"}
    )

    @model_validator(mode="before")
    @classmethod
    def _schema_hygiene(cls, data):
        from plaita.node.basic import warn_unknown_keys

        return warn_unknown_keys(cls, data)

    @classmethod
    def model_validate(cls, data, *, registry=None, **kwargs):
        """Parse + validate a Flow, then resolve node dicts into ``Node`` 子类。

        默认用 ``get_default_registry()`` 解析节点，保持常规用法开箱即用；传
        ``registry=`` 可注入自定义注册表，避免隐式依赖进程级单例。
        """
        flow = super().model_validate(data, **kwargs)
        flow.resolve_nodes(registry)
        flow._warn_uncovered_env_refs()
        return flow

    @classmethod
    def model_validate_json(cls, json_data, *, registry=None, **kwargs):
        """JSON 解析 + 校验 + 节点解析（见 ``model_validate`` 说明）。"""
        flow = super().model_validate_json(json_data, **kwargs)
        flow.resolve_nodes(registry)
        flow._warn_uncovered_env_refs()
        return flow

    def resolve_nodes(self, registry=None) -> None:
        """把 ``self.nodes`` 里 dict 形态的节点解析成对应 ``Node`` 子类实例。

        幂等：已经是 ``Node`` 实例的节点原样保留，重复调用安全。这是节点解析
        的**显式**入口——不再藏在 Pydantic validator 里，调用方可随时用自定义
        ``registry`` 重新解析。``model_validate`` / ``model_validate_json`` /
        ``from_string`` / ``from_file`` / ``parse`` 默认会用
        ``get_default_registry()`` 自动调一次。
        """
        from plaita.node import Node, get_default_registry

        if not self.nodes:
            return
        reg = registry if registry is not None else get_default_registry()
        resolved: List[Any] = []
        changed = False
        for n in self.nodes:
            if isinstance(n, Node):
                resolved.append(n)
            elif isinstance(n, dict):
                resolved.append(reg.parse_node(n))
                changed = True
            else:
                resolved.append(n)
        if changed:
            self.nodes = resolved
            # 节点集合变了, 失效索引指纹
            self._node_index_sig = ()
        # 构建期校验：JSON 直载路径历史上从不执行 node.validate()，builder 路径
        # 却会（如 Switch 无 branches 直接抛错）——同一类配置错误一条路响一条路哑。
        # 这里以 warning 降级执行（存量 JSON 流程可能带有 builder 才会拦的配置），
        # 致命的运行期问题仍由调度层兜底（如分支未命中硬失败）。
        for node in resolved:
            if isinstance(node, Node):
                try:
                    node.validate()
                except Exception as e:
                    logger.warning(
                        "flow %s node %s (%s) failed construction-time validation "
                        "(non-fatal, the flow still runs): %s",
                        self.flow_id, getattr(node, "id", "?"), getattr(node, "node_type", "?"), e,
                    )

    def _warn_uncovered_env_refs(self) -> None:
        """扫描节点表达式里 ``$ENV.<key>`` 引用, 若 ``expose_env`` 为空则 warning。

        0.5.0 起 ``$ENV`` 默认不暴露任何环境变量 (allowlist 模型)。旧 flow 升级
        后, 任何 ``$ENV.HOME`` 之类引用会**静默**解析为空字符串, 下游节点拿到空
        值继续跑——典型"沉默地坏掉"。本方法在 ``model_validate`` 后扫一次, 命中即
        ``logger.warning`` 列出引用的 key 名与修复指引 (加到 ``expose_env=[...]``)。
        不报错, 保持兼容; 只是把沉默变可见。

        仅扫默认 ``$ENV`` 前缀; 自定义 ``express_environment_variable`` 的流程不
        覆盖 (罕见场景, 不值得复杂化)。
        """
        if self.expose_env:
            return  # 显式声明过, 不告警
        refs: set = set()
        for node in self.nodes or []:
            if isinstance(node, dict):
                _collect_env_refs(node, refs)
                continue
            try:
                dump = node.model_dump()
            except Exception:
                logger.debug("node model_dump failed during $ENV ref scan", exc_info=True)
                continue
            _collect_env_refs(dump, refs)
        if refs:
            logger.warning(
                "flow %r 引用了 $ENV.%s 但 expose_env 为空——0.5.0 起 $ENV 默认不"
                "暴露任何环境变量, 这些引用会静默解析为空。请在 Flow 上显式声明: "
                "Flow(..., expose_env=%s)。详见 MIGRATION.md。",
                self.flow_id, sorted(refs), sorted(refs),
            )

    @classmethod
    def from_string(cls, content: str, *, registry: Optional["NodeRegistry"] = None) -> "Flow":
        """从 JSON 或 YAML 字符串解析 Flow。

        JSON 走 ``model_validate_json``；YAML（及无法按 JSON 解析的内容）
        走 ``plaita.io_format.loads`` 再 ``model_validate``。
        JSON 解析失败时把原始异常作为 cause 保留，避免真正的报错被
        YAML fallback 的次级报错淹没。

        ``registry=`` 可注入自定义 ``NodeRegistry`` 解析节点（与
        ``model_validate``/``resolve_nodes`` 对齐）；不传时用
        ``get_default_registry()``。
        """
        from plaita.io_format import loads

        if not content or not content.strip():
            raise ValueError("Flow content cannot be empty")
        # 优先 JSON，保持历史行为与错误信息
        if content.lstrip()[:1] in ("{", "["):
            try:
                flow = cls.model_validate_json(content, registry=registry)
            except ValueError as json_err:
                # 不要静默吞掉 JSON 报错——若 YAML fallback 也失败，
                # 把原始 JSON 异常作为 cause 一并抛出，方便定位真凶。
                try:
                    data = loads(content)
                    flow = cls.model_validate(data, registry=registry)
                except Exception:
                    raise json_err
            return flow
        data = loads(content)
        return cls.model_validate(data, registry=registry)

    @classmethod
    def from_file(cls, path: str, *, registry: Optional["NodeRegistry"] = None) -> "Flow":
        """从文件加载 Flow，按后缀（.json/.yaml/.yml）选择解析器。

        ``registry=`` 语义同 :meth:`from_string`。
        """
        from plaita.io_format import load_file

        data = load_file(path)
        flow = cls.model_validate(data, registry=registry)
        return flow

    def _ensure_index(self) -> Dict[str, Node]:
        """构建/刷新节点 id 索引, O(n) 摊销。

        失效判断用指纹 ``(id(self.nodes), tuple(n.id for n in nodes))``, 而不是
        旧实现的 ``len == len``——后者在节点 id 被原地修改、节点被替换为同长度
        不同 id、列表引用被换时全部静默失效。
        """
        nodes = self.nodes or []
        sig = (id(nodes), tuple(n.id for n in nodes))
        if self._node_index and self._node_index_sig == sig:
            return self._node_index
        self._node_index = {n.id: n for n in nodes}
        self._node_index_sig = sig
        return self._node_index

    def rebuild_node_index(self) -> None:
        """显式重建节点索引。直接修改 ``flow.nodes`` 内节点 id 后调用一次即可,
        不必新建 Flow。"""
        self._node_index_sig = ()
        self._ensure_index()

    @property
    def start_node(self):
        """流程入口节点。

        2026-07 行为变更: 入度 0 "启发式推断" 已删除——历史上无显式 Start 时
        本属性会扫描 nodes 数组找"未被任何 next/branch 引用的节点"作为入口,
        但当存在多个孤儿节点时返回结果依赖数组顺序, 让可视化编排工具导出
        顺序的细微差异就能改变流程行为。现在统一要求显式 Start 节点:
        没有 Start 就抛 ``FlowStartMissingError``, 让问题在解析期暴露。
        """
        for node in self.nodes or []:
            if node.node_type == Start.node_type:
                return node
        if not self.nodes:
            return None
        raise FlowStartMissingError(
            "无法确定流程入口: 没有显式 Start 节点。"
            "请添加一个 type=start 的节点, 或通过 FlowBuilder.start_with(...) 指定入口。"
        )

    def find_node_by_id(self, node_id) -> Optional[Node]:
        if node_id is None:
            return None
        index = self._ensure_index()
        node = index.get(node_id)
        if node is None:
            raise NodeNotFoundError(node_id)
        return node

    def is_end_node(self, node) -> bool:
        """True if *node* is an End node. Lives on Flow so callers (strategies,
        helpers) need not import ``plaita.node.End`` and re-trigger the
        core → node circular import."""
        return node is not None and node.node_type == End.node_type

    def next_node(self, current: Node, branch=None) -> Optional[Node]:
        if self.is_end_node(current):
            return None
        target = self._get_target_node(current, branch)
        ret = self.find_node_by_id(target)
        logger.debug("finding next node %s for %s, result: %s", target, current.id, ret.id if ret else None)
        return ret

    def _get_target_node(self, current: Node, branch=None) -> str:
        if (not current.branching) and current.next:
            return current.next
        return self._get_branch_target(current, branch)

    def _get_branch_target(self, current: Node, branch: str) -> str:
        if not hasattr(current, "branches"):
            logger.warning("Node %s has no branches", current.id)
            return None
        if not branch:
            # branch 为空 = 分支未命中（或无分支条件）。不再打 "branch None not
            # found" 的误导性 warning——该情况的错误语义由调度层统一处理
            # （硬失败或 errorHandler continue 逃生口）。
            return None
        logger.debug("current node %s has branches: %s", current.id, current.branches)
        from plaita.node.decide import resolve_branch_target

        for b in current.branches:
            # ``resolve_branch_target`` 把 ``b.next or b.name`` 兜底契约显式化:
            # 仅当节点声明 ``branch_name_as_target`` (Switch/Logic) 时才用 name
            # 回退。其他 branching 节点未显式 ``next`` 时返回 None, 不再静默跳到
            # 以 branch.name 命名的节点 (任务 #6)。
            target = resolve_branch_target(current, b)
            if target == branch:
                return target
        logger.warning("branch %s not found for node %s", branch, current.id)
        return None

    def run(self, *args, **params):
        _enforce_dict_input(args)
        return FlowExecution().run_compatible(self, False, *args, **params)

    async def arun(self, *args, **params):
        _enforce_dict_input(args)
        return await FlowExecution().arun_compatible(self, False, *args, **params)

    def debug(self, *args, **params):
        _enforce_dict_input(args)
        return FlowExecution().run_compatible(self, True, *args, **params)

    @property
    def input_property(self):
        """历史别名（第 3/4 个名字）：等价 ``input_type``。勿在新代码使用——
        同一概念现有 inputType（JSON）/ input_type（Python 字段）两个规范名。"""
        return self.input_type

    @property
    def output_property(self):
        """历史别名：等价 ``output_type``。见 :attr:`input_property` 说明。"""
        return self.output_type


_ENV_REF_PREFIX = "$ENV."


def _collect_env_refs(obj, refs: set) -> None:
    """递归收集 ``$ENV.<key>`` 引用到的 key 名到 ``refs``。

    扫 dict 的 value / list 的元素 / 字符串字面量。匹配 ``$ENV.<key>`` 起始
    的子串, key 取到下一个非标识符字符为止 (``$ENV.HOME.foo`` 取 ``HOME``)。
    """
    if isinstance(obj, str):
        start = 0
        while True:
            idx = obj.find(_ENV_REF_PREFIX, start)
            if idx < 0:
                break
            key_start = idx + len(_ENV_REF_PREFIX)
            j = key_start
            while j < len(obj) and (obj[j].isalnum() or obj[j] == "_"):
                j += 1
            if j > key_start:
                refs.add(obj[key_start:j])
            start = j + 1
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_env_refs(v, refs)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _collect_env_refs(v, refs)


def _enforce_dict_input(args: tuple) -> None:
    """Public ``Flow.run``/``arun``/``debug`` accept only dict/kwargs input.

    Internal callers (InlineFlow, parallel branches, loops) bypass this and
    may pass a single non-dict value as a child flow's ``$INPUT``.
    """
    if args and not (len(args) == 1 and isinstance(args[0], dict)):
        raise TypeError(
            "flow.run() accepts a single dict and/or keyword arguments; "
            f"got positional args={args!r}. Wrap scalar/array input in a dict, "
            "e.g. flow.run({'value': '...'}) and reference $INPUT.value."
        )


def parse(content: Union[str, dict]) -> Optional[Flow]:
    """Parse flow definition from JSON/YAML string or dict.

    字符串内容自动识别 JSON 或 YAML（YAML 需安装 ``plaita[yaml]``）；
    dict 直接走模型校验。所有解析最终都经过 ``Flow.parse_flow`` 这个
    model_validator，行为与历史完全一致。
    """
    if not content:
        return None
    if isinstance(content, str):
        from plaita.io_format import loads

        data = loads(content)
    else:
        data = content
    runtime = data.get("runtime", "python")
    if runtime != "python":
        raise ValueError(
            f"Unsupported runtime: {runtime!r}. plaita only executes 'python' flows; "
            "use a matching engine for other runtimes."
        )
    return Flow.model_validate(data)


def parse_and_run(content: str, *args, **kwargs):
    """Parse and execute a flow definition.

    支持 JSON 与 YAML 两种文本格式。
    """
    from plaita.io_format import loads

    _enforce_dict_input(args)
    data = loads(content)
    flow = Flow.model_validate(data)
    return FlowExecution().run_compatible(flow, False, *args, **kwargs)
