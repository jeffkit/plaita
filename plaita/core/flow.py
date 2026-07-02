"""
plaita.core.flow — Flow model definition and entry-point helpers.

Contains the canonical Flow class, parse(), and parse_and_run().
"""

from __future__ import annotations

import json
import warnings
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from plaita.core import types
from plaita.core.errors import FlowStartMissingError, NodeNotFoundError
# FlowExecution 在 flow.run/arun/debug 便捷方法里被实例化。executor 顶部并不反引
# flow (仅 TYPE_CHECKING 引 EventBus), 故这是一条单向依赖, 可在模块顶部坦诚声明,
# 不必藏在方法里装作没有 (历史遗留的伪 band-aid)。
from plaita.core.executor import FlowExecution
from plaita.io import Property
from plaita.logger import logger
from plaita.node import End, Node, Start, get_default_registry


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
    nodes: Optional[List[Node]] = Field(default_factory=list)
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

        if "nodes" in normalized:
            _registry = get_default_registry()
            normalized["nodes"] = [_registry.parse_node(node) for node in normalized["nodes"]]

        return normalized

    @staticmethod
    def from_string(content: str) -> Flow:
        """从 JSON 或 YAML 字符串解析 Flow。

        JSON 走 ``model_validate_json``；YAML（及无法按 JSON 解析的内容）
        走 ``plaita.io_format.loads`` 再 ``model_validate``。
        JSON 解析失败时把原始异常作为 cause 保留，避免真正的报错被
        YAML fallback 的次级报错淹没。
        """
        from plaita.io_format import loads

        if not content or not content.strip():
            raise ValueError("Flow content cannot be empty")
        # 优先 JSON，保持历史行为与错误信息
        if content.lstrip()[:1] in ("{", "["):
            try:
                return Flow.model_validate_json(content)
            except ValueError as json_err:
                # 不要静默吞掉 JSON 报错——若 YAML fallback 也失败，
                # 把原始 JSON 异常作为 cause 一并抛出，方便定位真凶。
                try:
                    data = loads(content)
                    return Flow.model_validate(data)
                except Exception:
                    raise json_err
        data = loads(content)
        return Flow.model_validate(data)

    @classmethod
    def from_file(cls, path: str) -> "Flow":
        """从文件加载 Flow，按后缀（.json/.yaml/.yml）选择解析器。"""
        from plaita.io_format import load_file

        data = load_file(path)
        return cls.model_validate(data)

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
        logger.debug("current node %s has branches: %s", current.id, current.branches)
        for b in current.branches:
            target = b.next or b.name
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
        return self.input_type

    @property
    def output_property(self):
        return self.output_type


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
        raise RuntimeError(f"UnSupport runtime：{runtime}")
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
