"""
节点注册服务

合并内置节点（来自 ``plaita.node`` 默认注册表）与控制台持久化的自定义节点描述，
供编排前端生成节点面板与配置表单。

设计说明：
- 内置节点的 schema 由 ``cls.model_json_schema()`` 生成（Pydantic JSON Schema），
  ``node_type``/``node_name``/``branching``/``async_node`` 是 ClassVar，需手动读取。
- 自定义节点描述仅用于编排表单（schema 元数据），**不**在控制台侧注册可执行类，
  因此自定义节点的 dry-run 会在运行时报"未注册节点类型"——这是预期行为。
- 内置节点不可通过 API 删除或覆盖。
"""
import json
import logging
from typing import Dict, List, Optional

from plaita.node import Node, get_default_registry

try:
    from .flow_store import FlowStore, NodeDescriptorOut, get_flow_store
except ImportError:
    from flow_store import FlowStore, NodeDescriptorOut, get_flow_store

logger = logging.getLogger(__name__)


# 内置节点分类（按 node_type 前缀/语义分组）
_CATEGORY_MAP = {
    "start": "控制",
    "end": "控制",
    "if": "控制",
    "switch": "控制",
    "case": "控制",
    "assignment": "数据",
    "code": "数据",
    "calculate": "数据",
    "http": "调用",
    "redis": "调用",
    "loop": "循环",
    "map": "循环",
    "filter": "循环",
    "find": "循环",
    "reduce": "循环",
    "child": "子流程",
    "reference": "子流程",
    "parallel": "子流程",
    "event": "事件",
    "approval": "事件",
    "delay": "事件",
    "http_callback": "事件",
    "kafka_queue": "事件",
    "redis_queue": "事件",
}


def _builtin_descriptor(cls: type) -> NodeDescriptorOut:
    node_type = getattr(cls, "node_type", "")
    node_name = getattr(cls, "node_name", "") or node_type
    try:
        schema = cls.model_json_schema()
        schema_json = json.dumps(schema, ensure_ascii=False)
    except Exception as e:  # 个别节点 schema 生成可能失败
        logger.warning("生成节点 %s schema 失败: %s", node_type, e)
        schema_json = "{}"
    return NodeDescriptorOut(
        node_type=node_type,
        node_name=node_name,
        category=_CATEGORY_MAP.get(node_type, ""),
        schema_json=schema_json,
        is_builtin=True,
    )


def _builtin_map() -> Dict[str, NodeDescriptorOut]:
    registry = get_default_registry()
    out: Dict[str, NodeDescriptorOut] = {}
    for node_type in registry.list_types():
        cls = registry.get(node_type)
        if cls is None:
            continue
        try:
            out[node_type] = _builtin_descriptor(cls)
        except Exception as e:
            logger.warning("跳过内置节点 %s: %s", node_type, e)
    return out


def list_descriptors(store: Optional[FlowStore] = None) -> List[NodeDescriptorOut]:
    """返回内置 + 自定义节点描述，按 node_type 排序。自定义覆盖同 type 的内置项。"""
    if store is None:
        store = get_flow_store()
    builtins = _builtin_map()
    customs = {d.node_type: d for d in store.list_node_descriptors()}
    # 自定义不覆盖内置（内置优先展示，自定义单独标记）；二者并列
    merged: Dict[str, NodeDescriptorOut] = dict(builtins)
    for k, v in customs.items():
        if k not in merged:
            merged[k] = v
    return sorted(merged.values(), key=lambda d: d.node_type)


def builtin_types() -> set:
    """内置节点 type 集合，用于注册时冲突校验。"""
    return set(_builtin_map().keys())


def register_custom(
    store: Optional[FlowStore],
    node_type: str,
    node_name: str = "",
    category: str = "",
    schema_json: str = "{}",
) -> NodeDescriptorOut:
    if store is None:
        store = get_flow_store()
    if not node_type:
        raise ValueError("node_type 不能为空")
    if node_type in builtin_types():
        raise ValueError(f"node_type {node_type} 与内置节点冲突")
    # schema_json 必须是合法 JSON
    try:
        json.loads(schema_json)
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"schema_json 不是合法 JSON: {e}")
    return store.upsert_node_descriptor(
        node_type=node_type,
        node_name=node_name,
        category=category,
        schema_json=schema_json,
        is_builtin=False,
    )


def delete_custom(store: Optional[FlowStore], node_type: str) -> bool:
    if store is None:
        store = get_flow_store()
    if node_type in builtin_types():
        raise ValueError(f"内置节点 {node_type} 不可删除")
    return store.delete_node_descriptor(node_type)
