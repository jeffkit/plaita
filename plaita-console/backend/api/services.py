"""
服务发现 API
提供服务列表、详情、拓扑等接口
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from redis import Redis

from plaita.redis_client import (
    HEALTH_CONNECT_TIMEOUT_SECONDS,
    HEALTH_SOCKET_TIMEOUT_SECONDS,
    make_redis_client,
)

try:
    from ..auth import ensure_platform_admin, require_auth
except ImportError:
    from auth import ensure_platform_admin, require_auth  # type: ignore

try:
    from ..services.read_redaction import RedactedResponseModel
except ImportError:
    from services.read_redaction import RedactedResponseModel  # type: ignore

try:
    from .redis_scan import scan_keys
except ImportError:  # 平铺布局（cwd=backend）运行时
    from redis_scan import scan_keys  # type: ignore

router = APIRouter()


# ============ 数据模型 ============

class ServiceInfo(RedactedResponseModel):
    """服务信息（序列化层脱敏：#265 基础设施/组件 host 不回显口令）"""
    instance_id: str = Field(..., description="实例唯一标识")
    service_type: str = Field(..., description="服务类型")
    host: str = Field(..., description="主机地址")
    status: str = Field(
        ...,
        description=(
            "状态：starting / running / draining（优雅下线中，不再领新任务）/ "
            "stopping / stopped"
        ),
    )
    start_time: Optional[str] = Field(None, description="启动时间")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="配置信息")
    active_tasks: int = Field(default=0, description="当前处理的任务数")
    last_heartbeat: Optional[str] = Field(None, description="最后心跳时间")


class ServiceListResponse(RedactedResponseModel):
    """服务列表响应"""
    services: List[ServiceInfo]
    total: int
    # A2R20-02：注册表扫描触界时结果不完整（scan_keys 已记 WARNING），
    # 显式告诉调用方「这只是部分结果」。
    truncated: bool = False


class ServiceNode(RedactedResponseModel):
    """服务拓扑节点"""
    instance_id: str
    service_type: str
    name: str
    host: str
    status: str
    start_time: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ServiceEdge(BaseModel):
    """服务拓扑边"""
    source_id: str
    target_id: str
    edge_type: str  # uses_queue, uses_storage, publishes_event, subscribes_event
    label: str


class ServiceTopology(RedactedResponseModel):
    """服务拓扑"""
    nodes: List[ServiceNode]
    edges: List[ServiceEdge]
    timestamp: str
    truncated: bool = False


class StopServiceRequest(BaseModel):
    """停止服务请求"""
    graceful: bool = Field(default=True, description="是否优雅停止")


class ServiceStatusResponse(BaseModel):
    """服务状态响应"""
    instance_id: str
    status: str
    active_tasks: int
    last_heartbeat: Optional[str]


# ============ 工具函数 ============

def get_redis(request: Request) -> Redis:
    """获取 Redis 客户端"""
    return request.app.state.redis


def parse_service_data(data: str) -> Optional[ServiceInfo]:
    """解析服务数据"""
    try:
        info = json.loads(data)
        return ServiceInfo(**info)
    except Exception:
        return None


# ---- A2R20-02：注册表读面有界化 ----
# 历史：``redis.scan_iter(match=pattern)`` 不传 count（redis-py 默认 COUNT=10），
# 15 万键的 keyspace 单次列表请求要发 1.4 万条 SCAN；65 万键 9.2s/6.3 万条，
# 且端点内是同步 Redis + 逐键 GET（N+1），一次 viewer GET 即冻结 console
# 事件循环。这里统一走 #202 的有界扫描工具：
# - COUNT 显式给 1000（PAGE_SIZE_MAX）：65 万键约 650 轮，落在 1500 轮预算内；
# - 触界返回 ``truncated`` + scan_keys 的 WARNING，不静默给部分结果；
# - 取值为 MGET 分块，不再逐键往返。
SERVICE_SCAN_COUNT = 1000
SERVICE_SCAN_MAX_ROUNDS = 1500
SERVICE_SCAN_MAX_KEYS = 5000
SERVICE_SCAN_TIME_BUDGET_S = 1.0
SERVICE_MGET_CHUNK = 500


def _scan_registry_keys(redis: Redis, pattern: str) -> Tuple[List[str], bool]:
    """有界扫描注册表键（显式 COUNT，keys/rounds/墙钟三重预算）。"""
    result = scan_keys(
        redis,
        pattern,
        count=SERVICE_SCAN_COUNT,
        max_keys=SERVICE_SCAN_MAX_KEYS,
        max_rounds=SERVICE_SCAN_MAX_ROUNDS,
        time_budget_s=SERVICE_SCAN_TIME_BUDGET_S,
    )
    return result.keys, result.truncated


def _mget_registry_values(redis: Redis, keys: List[str]) -> Dict[str, Any]:
    """分块 MGET 取代逐键 GET（N+1 → 批量往返）。"""
    values: Dict[str, Any] = {}
    for start in range(0, len(keys), SERVICE_MGET_CHUNK):
        chunk = keys[start : start + SERVICE_MGET_CHUNK]
        try:
            raws = redis.mget(chunk)
        except Exception:  # noqa: BLE001 - 批量不可用时逐键回退，行为不劣化
            # 批量不可用时逐键回退（行为不劣化），失败键跳过。
            raws = [redis.get(key) for key in chunk]
        for key, raw in zip(chunk, raws):
            if raw:
                values[key] = raw
    return values


def _escape_glob(value: str) -> str:
    """转义 SCAN MATCH 通配字符（instance_id 视为字面量）。"""
    return "".join("\\" + ch if ch in "\\*?[]" else ch for ch in value)


# ============ API 端点 ============

@router.get("/services", response_model=ServiceListResponse)
async def list_services(
    service_type: Optional[str] = None,
    request: Request = None,
):
    """
    获取所有服务列表（包含 Redis 注册的服务、托管实例和基础设施服务）
    
    - **service_type**: 可选，按服务类型筛选
    """
    redis = get_redis(request) if request is not None else None
    if redis is None:
        # 本地单机档无 Redis：与 queues/events 一致返回 503（此前 NoneType.keys 500）
        raise HTTPException(
            status_code=503,
            detail="服务注册表依赖 Redis，本地单机档不可用",
        )
    services = []
    added_instance_ids = set()
    truncated = False
    
    # 1. 从 Redis 注册表获取服务
    if service_type:
        pattern = f"plaita:registry:{service_type}:*"
    else:
        pattern = "plaita:registry:*"
    
    keys, truncated = _scan_registry_keys(redis, pattern)
    registry_keys = []
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        parts = key_str.split(":")
        
        if len(parts) < 4:
            continue
        
        if parts[1] != "registry":
            continue
        registry_keys.append(key_str)

    # N+1 GET → 分块 MGET（A2R20-02）
    for key_str, data in _mget_registry_values(redis, registry_keys).items():
        service = parse_service_data(data)
        if service:
            services.append(service)
            added_instance_ids.add(service.instance_id)
    
    # 2. 添加 ServiceManager 托管的实例（未在 Redis 中注册的）
    try:
        try:
            from ..services.service_manager import get_service_manager
        except ImportError:
            import sys
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from services.service_manager import get_service_manager
        
        manager = get_service_manager()
        for inst in manager.list_instances():
            if inst.instance_id not in added_instance_ids:
                # 只在没有类型筛选或类型匹配时添加
                if service_type is None or inst.service_type == service_type:
                    services.append(ServiceInfo(
                        instance_id=inst.instance_id,
                        service_type=inst.service_type,
                        host="localhost",
                        status=inst.status,
                        start_time=inst.start_time,
                        metadata={"managed_by": "console", "pid": inst.pid},
                        active_tasks=0,
                        last_heartbeat=None
                    ))
                    added_instance_ids.add(inst.instance_id)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"获取托管实例失败: {e}")
    
    # 3. 添加基础设施服务
    if service_type is None or service_type == "infrastructure":
        try:
            try:
                from ..services.service_manager import get_service_manager
            except ImportError:
                pass  # 已在上面导入
            
            manager = get_service_manager()
            config = manager.config
            
            # 从配置中获取已启用的基础设施
            infrastructure = getattr(config, 'infrastructure', {}) or {}
            if isinstance(infrastructure, dict):
                for infra_name, infra_config in infrastructure.items():
                    # 支持字典和 Pydantic 对象两种格式
                    if hasattr(infra_config, 'enabled'):
                        enabled = infra_config.enabled
                        url = getattr(infra_config, 'url', None) or 'localhost'
                        infra_type = getattr(infra_config, 'type', infra_name)
                        display_name = getattr(infra_config, 'display_name', infra_name)
                    elif isinstance(infra_config, dict):
                        enabled = infra_config.get('enabled', False)
                        url = infra_config.get('url', 'localhost')
                        infra_type = infra_config.get('type', infra_name)
                        display_name = infra_config.get('display_name', infra_name)
                    else:
                        continue
                    
                    if enabled:
                        infra_id = f"infra:{infra_name}"
                        if infra_id not in added_instance_ids:
                            services.append(ServiceInfo(
                                instance_id=infra_id,
                                service_type="infrastructure",
                                host=url,
                                status="running",  # 假设已启用的基础设施是运行中的
                                start_time=None,
                                metadata={
                                    "infra_type": infra_type,
                                    "display_name": display_name,
                                    "managed_by": "external"
                                },
                                active_tasks=0,
                                last_heartbeat=None
                            ))
                            added_instance_ids.add(infra_id)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"获取基础设施服务失败: {e}")
    
    # 4. 添加内部组件服务（事件总线、消息队列、存储）
    if service_type is None or service_type in ("component", "infrastructure"):
        try:
            try:
                from ..services.service_manager import get_service_manager
            except ImportError:
                pass  # 已在上面导入
            
            manager = get_service_manager()
            config = manager.config
            
            # 内部组件配置
            internal_components = [
                ("eventbus", "事件总线", getattr(config, 'eventbus', None)),
                ("queue", "消息队列", getattr(config, 'queue', None)),
                ("storage", "数据存储", getattr(config, 'storage', None)),
            ]
            
            for comp_name, display_name, comp_config in internal_components:
                if comp_config is None:
                    continue
                
                # 获取类型和 URL
                if hasattr(comp_config, 'type'):
                    comp_type = comp_config.type
                    comp_url = getattr(comp_config, 'url', None)
                elif isinstance(comp_config, dict):
                    comp_type = comp_config.get('type', 'memory')
                    comp_url = comp_config.get('url')
                else:
                    continue
                
                comp_id = f"component:{comp_name}"
                if comp_id not in added_instance_ids:
                    # 检测组件可用性
                    status = "running"  # 内存类型始终可用
                    host = "内存"
                    
                    if comp_type == "redis":
                        # 检测 Redis 连接
                        try:
                            redis_url = comp_url or getattr(config.redis, 'url', None) or config.redis.get('url', 'redis://localhost:6379/0')
                            test_conn = make_redis_client(
                                redis_url,
                                connect_timeout=HEALTH_CONNECT_TIMEOUT_SECONDS,
                                socket_timeout=HEALTH_SOCKET_TIMEOUT_SECONDS,
                            )
                            test_conn.ping()
                            status = "running"
                            host = redis_url
                        except Exception:
                            status = "error"
                            host = comp_url or "redis://localhost:6379/0"
                    elif comp_type == "db":
                        host = comp_url or "database"
                        # 可以添加数据库连接检测
                    elif comp_type == "kafka":
                        host = comp_url or "kafka://localhost:9092"
                        # 可以添加 Kafka 连接检测
                    
                    services.append(ServiceInfo(
                        instance_id=comp_id,
                        service_type="component",
                        host=host,
                        status=status,
                        start_time=None,
                        metadata={
                            "component_type": comp_type,
                            "display_name": f"{display_name} ({comp_type})",
                            "managed_by": "internal"
                        },
                        active_tasks=0,
                        last_heartbeat=None
                    ))
                    added_instance_ids.add(comp_id)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"获取内部组件服务失败: {e}")
    
    return ServiceListResponse(
        services=services,
        total=len(services),
        truncated=truncated,
    )


@router.get("/services/topology", response_model=ServiceTopology)
async def get_topology(
    request: Request = None,
):
    """
    获取服务拓扑结构（含关联关系）
    """
    redis = get_redis(request) if request is not None else None
    if redis is None:
        # 本地单机档无 Redis：与 queues/events 一致返回 503（此前 NoneType.keys 500）
        raise HTTPException(
            status_code=503,
            detail="服务注册表依赖 Redis，本地单机档不可用",
        )
    # 获取所有服务（有界 SCAN 替代无 COUNT 的 scan_iter：A2R20-02）
    pattern = "plaita:registry:*"
    keys, truncated = _scan_registry_keys(redis, pattern)
    
    nodes: List[ServiceNode] = []
    edges: List[ServiceEdge] = []
    
    # 虚拟资源节点 ID
    redis_node_id = "resource:redis"
    eventbus_node_id = "resource:eventbus"
    
    # 添加共享资源节点
    has_redis_resource = False
    has_eventbus_resource = False
    
    # 跟踪已添加的实例 ID，避免重复
    added_instance_ids = set()
    
    registry_keys = []
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        parts = key_str.split(":")
        
        if len(parts) < 4 or parts[1] != "registry":
            continue
        registry_keys.append(key_str)

    for key_str, data in _mget_registry_values(redis, registry_keys).items():
        try:
            info = json.loads(data)
        except Exception:
            continue
        
        service_type = info.get("service_type", "unknown")
        instance_id = info.get("instance_id", "")
        
        # 创建服务节点
        node = ServiceNode(
            instance_id=instance_id,
            service_type=service_type,
            name=f"{service_type}:{instance_id[:8]}",
            host=info.get("host", ""),
            status=info.get("status", "unknown"),
            start_time=info.get("start_time"),
            metadata=info.get("metadata", {})
        )
        nodes.append(node)
        added_instance_ids.add(instance_id)
        
        # 根据服务类型推断关联关系
        metadata = info.get("metadata", {})
        
        # FlowWorker 使用 Redis 队列
        if service_type == "flow_worker":
            has_redis_resource = True
            queue_name = metadata.get("queue_name", "plaita:flow:queue")
            edges.append(ServiceEdge(
                source_id=instance_id,
                target_id=redis_node_id,
                edge_type="uses_queue",
                label=f"Queue: {queue_name}"
            ))
        
        # 扩展服务使用事件总线
        if service_type in ("delay_service", "redis_queue_service", 
                            "kafka_queue_service", "http_callback_service",
                            "approval_service"):
            has_eventbus_resource = True
            edges.append(ServiceEdge(
                source_id=instance_id,
                target_id=eventbus_node_id,
                edge_type="subscribes_event",
                label="Subscribe"
            ))
            edges.append(ServiceEdge(
                source_id=instance_id,
                target_id=eventbus_node_id,
                edge_type="publishes_event",
                label="Publish"
            ))
    
    # 添加 ServiceManager 管理的运行中实例（未在 Redis 中注册的）
    try:
        try:
            from ..services.service_manager import get_service_manager
        except ImportError:
            import sys
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from services.service_manager import get_service_manager
        
        manager = get_service_manager()
        for inst in manager.list_instances():
            if inst.status == "running" and inst.instance_id not in added_instance_ids:
                node = ServiceNode(
                    instance_id=inst.instance_id,
                    service_type=inst.service_type,
                    name=f"{inst.service_type}:{inst.instance_id[:8]}",
                    host="localhost",
                    status=inst.status,
                    start_time=inst.start_time,
                    metadata={"managed_by": "console"}
                )
                nodes.append(node)
                added_instance_ids.add(inst.instance_id)
                
                # 为扩展服务添加关联关系
                if inst.service_type in ("delay_service", "redis_queue_service", 
                                         "kafka_queue_service", "http_callback_service",
                                         "approval_service"):
                    has_eventbus_resource = True
                    edges.append(ServiceEdge(
                        source_id=inst.instance_id,
                        target_id=eventbus_node_id,
                        edge_type="subscribes_event",
                        label="Subscribe"
                    ))
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"获取托管实例失败: {e}")
    
    # 添加共享资源节点
    if has_redis_resource:
        nodes.append(ServiceNode(
            instance_id=redis_node_id,
            service_type="resource",
            name="Redis",
            host="",
            status="running",
            metadata={"resource_type": "redis"}
        ))
    
    if has_eventbus_resource:
        nodes.append(ServiceNode(
            instance_id=eventbus_node_id,
            service_type="resource",
            name="EventBus",
            host="",
            status="running",
            metadata={"resource_type": "eventbus"}
        ))
    
    return ServiceTopology(
        nodes=nodes,
        edges=edges,
        timestamp=datetime.now().isoformat(),
        truncated=truncated,
    )


@router.get("/services/types/list")
async def list_service_types(
    redis: Redis = Depends(get_redis)
):
    """
    获取所有服务类型
    """
    pattern = "plaita:registry:*"
    keys, truncated = _scan_registry_keys(redis, pattern)
    
    service_types = set()
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        parts = key_str.split(":")
        if len(parts) >= 3 and parts[1] == "registry":
            service_types.add(parts[2])
    
    return {
        "types": list(service_types),
        "total": len(service_types),
        "truncated": truncated,
    }


@router.get("/services/{instance_id}", response_model=ServiceInfo)
async def get_service(
    instance_id: str,
    redis: Redis = Depends(get_redis)
):
    """
    获取指定服务详情
    
    - **instance_id**: 服务实例 ID
    """
    # 搜索所有服务类型中的该实例（有界 SCAN，A2R20-02）
    pattern = f"plaita:registry:*:{_escape_glob(instance_id)}"
    keys, _truncated = _scan_registry_keys(redis, pattern)
    
    if not keys:
        raise HTTPException(status_code=404, detail=f"服务不存在: {instance_id}")
    
    data = redis.get(keys[0])
    if not data:
        raise HTTPException(status_code=404, detail=f"服务不存在: {instance_id}")
    
    service = parse_service_data(data)
    if not service:
        raise HTTPException(status_code=500, detail="服务数据解析失败")
    
    return service


@router.post("/services/{instance_id}/stop")
async def stop_service(
    instance_id: str,
    req: StopServiceRequest,
    request: Request,
    identity: Dict = Depends(require_auth),
    redis: Redis = Depends(get_redis)
):
    """
    发送停止指令给指定服务

    - **instance_id**: 服务实例 ID
    - **graceful**: 是否优雅停止

    #260 / A42-02：服务实例是全局（跨租户）生命周期对象，租户 editor/admin
    都不是平台身份。该操作要求 platform_admin；拒绝进审计。
    """
    ensure_platform_admin(
        request, identity, action="service.stop", resource="service",
        target=instance_id,
    )
    # 验证服务存在（有界 SCAN，A2R20-02）
    pattern = f"plaita:registry:*:{_escape_glob(instance_id)}"
    keys, _truncated = _scan_registry_keys(redis, pattern)
    
    if not keys:
        raise HTTPException(status_code=404, detail=f"服务不存在: {instance_id}")
    
    # 发送控制指令
    control_channel = f"plaita:control:{instance_id}"
    command = {
        "command": "stop",
        "graceful": req.graceful,
        "timestamp": datetime.now().isoformat()
    }
    
    redis.publish(control_channel, json.dumps(command))

    # A55-S1：成功路径也必须留痕（此前只有 ensure_platform_admin 的 denied）。
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action="service.stop",
        resource="service",
        resource_id=instance_id,
        detail={"instance_id": instance_id, "graceful": req.graceful},
        tenant_id="",  # 全局生命周期对象（跨租户）
    )
    return {
        "status": "sent",
        "instance_id": instance_id,
        "command": "stop",
        "graceful": req.graceful
    }


@router.get("/services/{instance_id}/status", response_model=ServiceStatusResponse)
async def get_service_status(
    instance_id: str,
    redis: Redis = Depends(get_redis)
):
    """
    获取服务实时状态
    
    - **instance_id**: 服务实例 ID
    """
    # 搜索服务（有界 SCAN，A2R20-02）
    pattern = f"plaita:registry:*:{_escape_glob(instance_id)}"
    keys, _truncated = _scan_registry_keys(redis, pattern)
    
    if not keys:
        raise HTTPException(status_code=404, detail=f"服务不存在: {instance_id}")
    
    data = redis.get(keys[0])
    if not data:
        raise HTTPException(status_code=404, detail=f"服务不存在: {instance_id}")
    
    try:
        info = json.loads(data)
    except Exception:
        raise HTTPException(status_code=500, detail="服务数据解析失败")
    
    return ServiceStatusResponse(
        instance_id=instance_id,
        status=info.get("status", "unknown"),
        active_tasks=info.get("active_tasks", 0),
        last_heartbeat=info.get("last_heartbeat")
    )
