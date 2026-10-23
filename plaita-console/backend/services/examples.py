"""首次启动示例流程种子。

flows 表为空时写入 3 个已发布的示例，让新用户在画布里立即有可跑的东西：
1. hello-plaita  最小链路：赋值 → 输出
2. list-map      集合映射：内嵌子流程对列表逐项加工
3. http-echo     HTTP 调用：请求公共 API 并透传响应（需外网）

定义刻意只用最基础节点，保证在本地单机模式（无 Redis）也能直接启动。
"""
import json
import logging

from .flow_store import FlowStore, get_flow_store

logger = logging.getLogger(__name__)

HELLO = {
    "desc": "最小示例：赋值节点产生一条问候语，end 节点把它作为流程输出。",
    "definition": {
        "nodes": [
            {"type": "start", "id": "start", "next": "greet"},
            {
                "type": "assignment",
                "id": "greet",
                "outputType": {"dataType": "object"},
                "output": {"message": "你好，Plaita！", "from": "$FLOW_ID"},
                "next": "end",
            },
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.greet"},
        ]
    },
}

LIST_MAP = {
    "desc": "集合映射示例：赋值产生一个列表，map 节点对每项执行内嵌子流程（$INPUT.item / $INPUT.index）。",
    "definition": {
        "nodes": [
            {"type": "start", "id": "start", "next": "make-list"},
            {
                "type": "assignment",
                "id": "make-list",
                "output": {"nums": [1, 2, 3]},
                "next": "double-all",
            },
            {
                "type": "map",
                "id": "double-all",
                "collection": "$NODE.make-list.nums",
                "concurrent": False,
                "child_flow": {
                    "nodes": [
                        {"type": "start", "id": "c-start", "next": "c-echo"},
                        {
                            "type": "assignment",
                            "id": "c-echo",
                            "output": {"item": "$INPUT.item", "index": "$INPUT.index"},
                            "next": "c-end",
                        },
                        {"type": "end", "id": "c-end", "resultType": "success", "output": "$NODE.c-echo"},
                    ]
                },
                "next": "end",
            },
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.double-all"},
        ]
    },
}

HTTP_ECHO = {
    "desc": "HTTP 调用示例：请求公共 API 并把响应体透传为流程输出（需外网）。",
    "definition": {
        "nodes": [
            {"type": "start", "id": "start", "next": "fetch"},
            {
                "type": "http",
                "id": "fetch",
                "method": "GET",
                "url": "https://httpbin.org/json",
                "next": "end",
            },
            # plaita#129：`$NODE.<http_id>` 现在是**该节点的结果结构**
            # （{data, status, statusText, headers}），不再是裸响应体。
            # 只要响应体本身请取 `.data`；整份响应取 `$NODE.fetch`。
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.fetch.data"},
        ]
    },
}

_EXAMPLES = [
    ("hello-plaita", "快速开始", HELLO),
    ("list-map", "循环与映射", LIST_MAP),
    ("http-echo", "HTTP 调用", HTTP_ECHO),
]


def _default_seed_gate(row) -> None:
    """种子发布的默认门（A134-04：门=发布唯一执法点，种子面不破例）。

    与 API 发布（api/flows.py ``_publish_gates``）同一组闸：

    * **编码闸**（卡 A-1）：判据是定义自身、不依赖 redis——本地档照样执法；
    * **能力门**（#300）：数据源是 worker 注册清单，传 ``redis=None`` 时由
      ``_gate_runtime_capability`` 自跳过。种子只发生在本地单机档启动期
      （main.py 仅 ``local_mode`` 调用本函数），与本地档 HTTP 发布完全同判；
      若未来种子扩到集群启动，调用方应经 ``gate_fn`` 传真实 redis 句柄的
      门，而不是改这里。

    门拒（HTTPException）被 per-example except 兜住：该例跳过 + WARNING，
    启动不受影响。惰性导入 api 层防环（api.flows 不反向依赖本模块）。
    """
    try:
        from ..api.flows import _publish_gates
    except ImportError:
        from api.flows import _publish_gates  # type: ignore
    _publish_gates(None, row, where="examples.seed")


def seed_example_flows(store: FlowStore | None = None, tenant_id: str = "default",
                       gate_fn=None) -> int:
    """某租户 flows 为空时写入示例流程（1.0.0 已发布）。返回写入数量。

    ``gate_fn``：发布事务内的门（A134-04 补第三点注入），缺省用
    :func:`_default_seed_gate`（编码闸 + 本地档语义的能力门）。
    """
    store = store or get_flow_store()
    if gate_fn is None:
        gate_fn = _default_seed_gate
    if store.list_flows(tenant_id):
        return 0
    created = 0
    for flow_id, desc, spec in _EXAMPLES:
        try:
            store.ensure_flow(flow_id, author="plaita", desc=spec["desc"] or desc,
                              tenant_id=tenant_id)
            # 注入 flow_id：定义里的 $FLOW_ID 才能解析（否则恒为 null）
            definition = {"flow_id": flow_id, **spec["definition"]}
            store.save_flow_definition(
                flow_id,
                "1.0.0",
                json.dumps(definition, ensure_ascii=False),
                layout="",
                status="draft",
                created_by="plaita",
                tenant_id=tenant_id,
            )
            store.publish_version(flow_id, "1.0.0", tenant_id=tenant_id,
                                  gate_fn=gate_fn)
            created += 1
        except Exception as e:  # noqa: BLE001 — 单个示例失败不影响启动
            logger.warning("写入示例流程 %s 失败: %s", flow_id, e)
    if created:
        logger.info("已为租户 %s 写入 %d 个示例流程（hello-plaita / list-map / http-echo）",
                    tenant_id, created)
    return created
