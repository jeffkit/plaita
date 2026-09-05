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
    "desc": "HTTP 调用示例：请求公共 API 并把响应透传为流程输出（需外网）。",
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
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.fetch"},
        ]
    },
}

_EXAMPLES = [
    ("hello-plaita", "快速开始", HELLO),
    ("list-map", "循环与映射", LIST_MAP),
    ("http-echo", "HTTP 调用", HTTP_ECHO),
]


def seed_example_flows(store: FlowStore | None = None) -> int:
    """flows 表为空时写入示例流程（1.0.0 已发布）。返回写入数量。"""
    store = store or get_flow_store()
    if store.list_flows():
        return 0
    created = 0
    for flow_id, desc, spec in _EXAMPLES:
        try:
            store.ensure_flow(flow_id, author="plaita", desc=spec["desc"] or desc)
            store.save_flow_definition(
                flow_id,
                "1.0.0",
                json.dumps(spec["definition"], ensure_ascii=False),
                layout="",
                status="draft",
                created_by="plaita",
            )
            store.publish_version(flow_id, "1.0.0")
            created += 1
        except Exception as e:  # noqa: BLE001 — 单个示例失败不影响启动
            logger.warning("写入示例流程 %s 失败: %s", flow_id, e)
    if created:
        logger.info("已写入 %d 个示例流程（hello-plaita / list-map / http-echo）", created)
    return created
