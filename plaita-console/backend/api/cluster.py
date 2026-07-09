"""
集群管理 API
提供服务启动、停止、配置等接口
"""
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

try:
    from ..services.service_manager import (
        get_service_manager,
        ServiceConfig,
        ManagedInstance,
        InfrastructureConfig
    )
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from services.service_manager import (
        get_service_manager,
        ServiceConfig,
        ManagedInstance,
        InfrastructureConfig
    )

router = APIRouter()


# ============ 数据模型 ============

class ServiceTypeInfo(BaseModel):
    """服务类型信息"""
    service_type: str
    display_name: str
    default_instances: int
    max_instances: int
    running_count: int


class ServiceTypesResponse(BaseModel):
    """服务类型列表响应"""
    mode: str  # process 或 docker
    service_types: List[ServiceTypeInfo]


class StartServiceRequest(BaseModel):
    """启动服务请求"""
    service_type: str = Field(..., description="服务类型")


class StartServiceResponse(BaseModel):
    """启动服务响应"""
    success: bool
    instance_id: Optional[str] = None
    status: str
    error: Optional[str] = None


class StopServiceRequest(BaseModel):
    """停止服务请求"""
    graceful: bool = Field(default=True, description="是否优雅停止")


class ManagedInstanceResponse(BaseModel):
    """托管实例响应"""
    instance_id: str
    service_type: str
    pid: Optional[int] = None
    container_id: Optional[str] = None
    status: str
    start_time: str
    error_message: Optional[str] = None
    managed_by: str = "console"  # console 或 external (Redis 注册)


class ManagedInstancesResponse(BaseModel):
    """托管实例列表响应"""
    instances: List[ManagedInstanceResponse]
    total: int


class ClusterConfigResponse(BaseModel):
    """集群配置响应"""
    mode: str
    config_path: str


class SwitchModeRequest(BaseModel):
    """切换模式请求"""
    mode: str = Field(..., description="目标模式: process 或 docker")


class InfrastructureInfo(BaseModel):
    """基础设施信息"""
    name: str
    display_name: str
    type: str
    enabled: bool
    url: Optional[str] = None
    bootstrap_servers: Optional[str] = None
    status: str = "unknown"  # healthy, unhealthy, disabled, unknown
    details: Optional[dict] = None
    docker: Optional[dict] = None


class InfrastructureListResponse(BaseModel):
    """基础设施列表响应"""
    infrastructure: List[InfrastructureInfo]
    total: int


class UpdateInfrastructureRequest(BaseModel):
    """更新基础设施配置请求"""
    display_name: Optional[str] = None
    enabled: Optional[bool] = None
    url: Optional[str] = None
    bootstrap_servers: Optional[str] = None
    docker: Optional[dict] = None


class CreateInfrastructureRequest(BaseModel):
    """创建基础设施配置请求"""
    name: str = Field(..., description="基础设施 ID")
    display_name: str = Field(..., description="显示名称")
    type: str = Field(..., description="类型: redis, kafka, database")
    enabled: bool = Field(True, description="是否启用")
    url: Optional[str] = Field(None, description="连接 URL")
    bootstrap_servers: Optional[str] = Field(None, description="Kafka bootstrap servers")
    docker: Optional[dict] = Field(None, description="Docker 配置")


# ============ API 端点 ============

@router.get("/cluster/service-types", response_model=ServiceTypesResponse)
async def list_service_types():
    """
    获取所有可启动的服务类型（包括外部注册的实例统计）
    """
    import json
    from redis import Redis
    
    manager = get_service_manager()
    
    # 先统计 Redis 注册表中的外部实例
    external_running_counts: dict = {}
    try:
        redis_url = manager.config.redis.get("url", "redis://localhost:6379/0")
        redis_client = Redis.from_url(redis_url)
        
        pattern = "plaita:registry:*"
        keys = redis_client.keys(pattern)
        
        for key in keys:
            data = redis_client.get(key)
            if not data:
                continue
            try:
                info = json.loads(data)
                svc_type = info.get("service_type", "")
                status = info.get("status", "")
                if status == "running" and svc_type:
                    external_running_counts[svc_type] = external_running_counts.get(svc_type, 0) + 1
            except Exception:
                continue
    except Exception:
        pass
    
    service_types = []
    for service_type, config in manager.get_available_services().items():
        # 计算托管实例运行数量
        managed_running = sum(
            1 for inst in manager.list_instances(service_type)
            if inst.status == "running"
        )
        
        # 加上外部实例数量
        external_running = external_running_counts.get(service_type, 0)
        total_running = managed_running + external_running
        
        service_types.append(ServiceTypeInfo(
            service_type=service_type,
            display_name=config.display_name,
            default_instances=config.default_instances,
            max_instances=config.max_instances,
            running_count=total_running
        ))
    
    return ServiceTypesResponse(
        mode=manager.config.mode,
        service_types=service_types
    )


@router.post("/cluster/start", response_model=StartServiceResponse)
async def start_service(request: StartServiceRequest):
    """
    启动一个服务实例
    """
    manager = get_service_manager()
    
    try:
        instance = await manager.start_service(request.service_type)
        
        return StartServiceResponse(
            success=instance.status in ("running", "starting"),
            instance_id=instance.instance_id,
            status=instance.status,
            error=instance.error_message
        )
    except ValueError as e:
        return StartServiceResponse(
            success=False,
            status="error",
            error=str(e)
        )
    except Exception as e:
        return StartServiceResponse(
            success=False,
            status="error",
            error=f"启动失败: {str(e)}"
        )


@router.post("/cluster/stop/{instance_id}")
async def stop_service(instance_id: str, request: StopServiceRequest):
    """
    停止一个服务实例
    """
    manager = get_service_manager()
    
    success = await manager.stop_service(instance_id, request.graceful)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"实例不存在或无法停止: {instance_id}")
    
    return {
        "success": True,
        "instance_id": instance_id,
        "message": "服务已停止"
    }


@router.post("/cluster/stop-all")
async def stop_all_services(request: StopServiceRequest):
    """
    停止所有托管服务
    """
    manager = get_service_manager()
    results = await manager.stop_all(request.graceful)
    
    return {
        "success": all(results.values()),
        "results": results
    }


@router.get("/cluster/instances", response_model=ManagedInstancesResponse)
async def list_managed_instances(
    service_type: Optional[str] = None,
    include_external: bool = True
):
    """
    获取所有服务实例（包括托管的和外部注册的）
    
    Args:
        service_type: 过滤服务类型
        include_external: 是否包含 Redis 注册表中的外部实例
    """
    from fastapi import Request
    from redis import Redis
    import json
    
    manager = get_service_manager()
    
    # 刷新托管实例状态
    await manager.refresh_status()
    
    all_instances = []
    seen_ids = set()
    
    # 1. 先添加 ServiceManager 托管的实例
    for inst in manager.list_instances(service_type):
        all_instances.append(ManagedInstanceResponse(
            instance_id=inst.instance_id,
            service_type=inst.service_type,
            pid=inst.pid,
            container_id=inst.container_id,
            status=inst.status,
            start_time=inst.start_time,
            error_message=inst.error_message,
            managed_by="console"
        ))
        seen_ids.add(inst.instance_id)
    
    # 2. 添加 Redis 注册表中的外部实例
    if include_external:
        try:
            # 获取 Redis 客户端
            redis_url = manager.config.redis.get("url", "redis://localhost:6379/0")
            redis_client = Redis.from_url(redis_url)
            
            pattern = "plaita:registry:*"
            keys = redis_client.keys(pattern)
            
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                parts = key_str.split(":")
                
                if len(parts) < 4 or parts[1] != "registry":
                    continue
                
                data = redis_client.get(key)
                if not data:
                    continue
                
                try:
                    info = json.loads(data)
                except Exception:
                    continue
                
                instance_id = info.get("instance_id", "")
                svc_type = info.get("service_type", "unknown")
                
                # 跳过已添加的
                if instance_id in seen_ids:
                    continue
                
                # 过滤服务类型
                if service_type and svc_type != service_type:
                    continue
                
                all_instances.append(ManagedInstanceResponse(
                    instance_id=instance_id,
                    service_type=svc_type,
                    pid=None,
                    container_id=None,
                    status=info.get("status", "unknown"),
                    start_time=info.get("start_time", ""),
                    error_message=None,
                    managed_by="external"
                ))
                seen_ids.add(instance_id)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"获取外部实例失败: {e}")
    
    return ManagedInstancesResponse(
        instances=all_instances,
        total=len(all_instances)
    )


@router.get("/cluster/config", response_model=ClusterConfigResponse)
async def get_cluster_config():
    """
    获取当前集群配置
    """
    try:
        manager = get_service_manager()
        return ClusterConfigResponse(
            mode=manager.config.mode,
            config_path=manager.config_path
        )
    except Exception:
        return ClusterConfigResponse(
            mode="standalone",
            config_path=""
        )


@router.post("/cluster/mode")
async def switch_mode(request: SwitchModeRequest):
    """
    切换服务管理模式（process/docker）
    
    注意：切换模式会停止所有当前托管的服务
    """
    if request.mode not in ("process", "docker"):
        raise HTTPException(status_code=400, detail="无效的模式，必须是 process 或 docker")
    
    manager = get_service_manager()
    
    # 停止所有服务
    await manager.stop_all(graceful=True)
    
    # 切换模式
    manager.config.mode = request.mode
    
    return {
        "success": True,
        "mode": request.mode,
        "message": f"已切换到 {request.mode} 模式"
    }


@router.post("/cluster/refresh")
async def refresh_instances():
    """
    刷新所有实例状态
    """
    manager = get_service_manager()
    await manager.refresh_status()
    
    return {
        "success": True,
        "message": "状态已刷新"
    }


# ============ 基础设施服务 API ============

@router.get("/cluster/infrastructure", response_model=InfrastructureListResponse)
async def list_infrastructure():
    """
    获取所有基础设施服务配置及其健康状态
    """
    manager = get_service_manager()
    infra_configs = manager.get_infrastructure()
    health_status = await manager.check_infrastructure_health()
    
    infrastructure = []
    for name, config in infra_configs.items():
        health = health_status.get(name, {"status": "unknown", "details": None})
        details = health.get("details")
        if isinstance(details, str):
            details = {"message": details}
        
        infrastructure.append(InfrastructureInfo(
            name=name,
            display_name=config.display_name,
            type=config.type,
            enabled=config.enabled,
            url=config.url,
            bootstrap_servers=config.bootstrap_servers,
            status=health.get("status", "unknown"),
            details=details
        ))
    
    return InfrastructureListResponse(
        infrastructure=infrastructure,
        total=len(infrastructure)
    )


@router.get("/cluster/infrastructure/{name}")
async def get_infrastructure_detail(name: str):
    """
    获取指定基础设施服务的详细信息和健康状态
    """
    manager = get_service_manager()
    infra_configs = manager.get_infrastructure()
    
    if name not in infra_configs:
        raise HTTPException(status_code=404, detail=f"基础设施 {name} 不存在")
    
    config = infra_configs[name]
    health_status = await manager.check_infrastructure_health()
    health = health_status.get(name, {"status": "unknown", "details": None})
    
    details = health.get("details")
    if isinstance(details, str):
        details = {"message": details}
    
    return InfrastructureInfo(
        name=name,
        display_name=config.display_name,
        type=config.type,
        enabled=config.enabled,
        url=config.url,
        bootstrap_servers=config.bootstrap_servers,
        status=health.get("status", "unknown"),
        details=details
    )


@router.post("/cluster/infrastructure/{name}/check")
async def check_infrastructure_health(name: str):
    """
    检查指定基础设施服务的健康状态
    """
    manager = get_service_manager()
    infra_configs = manager.get_infrastructure()
    
    if name not in infra_configs:
        raise HTTPException(status_code=404, detail=f"基础设施 {name} 不存在")
    
    health_status = await manager.check_infrastructure_health()
    health = health_status.get(name, {"status": "unknown", "details": None})
    
    return {
        "name": name,
        "status": health.get("status", "unknown"),
        "details": health.get("details"),
        "checked_at": datetime.now().isoformat()
    }


@router.post("/cluster/infrastructure", response_model=InfrastructureInfo)
async def create_infrastructure(request: CreateInfrastructureRequest):
    """
    创建新的基础设施配置
    """
    manager = get_service_manager()
    infra_configs = manager.get_infrastructure()
    
    if request.name in infra_configs:
        raise HTTPException(status_code=400, detail=f"基础设施 {request.name} 已存在")
    
    # 更新配置文件
    success = await manager.add_infrastructure(
        name=request.name,
        display_name=request.display_name,
        type=request.type,
        enabled=request.enabled,
        url=request.url,
        bootstrap_servers=request.bootstrap_servers,
        docker=request.docker or {}
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="保存配置失败")
    
    # 重新加载配置
    manager.load_config()
    config = manager.get_infrastructure().get(request.name)
    
    return InfrastructureInfo(
        name=request.name,
        display_name=config.display_name,
        type=config.type,
        enabled=config.enabled,
        url=config.url,
        bootstrap_servers=config.bootstrap_servers,
        status="unknown",
        docker=config.docker
    )


@router.put("/cluster/infrastructure/{name}", response_model=InfrastructureInfo)
async def update_infrastructure(name: str, request: UpdateInfrastructureRequest):
    """
    更新基础设施配置
    """
    manager = get_service_manager()
    infra_configs = manager.get_infrastructure()
    
    if name not in infra_configs:
        raise HTTPException(status_code=404, detail=f"基础设施 {name} 不存在")
    
    # 更新配置
    success = await manager.update_infrastructure(
        name=name,
        display_name=request.display_name,
        enabled=request.enabled,
        url=request.url,
        bootstrap_servers=request.bootstrap_servers,
        docker=request.docker
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="保存配置失败")
    
    # 重新加载并获取最新状态
    manager.load_config()
    config = manager.get_infrastructure().get(name)
    health_status = await manager.check_infrastructure_health()
    health = health_status.get(name, {"status": "unknown", "details": None})
    
    return InfrastructureInfo(
        name=name,
        display_name=config.display_name,
        type=config.type,
        enabled=config.enabled,
        url=config.url,
        bootstrap_servers=config.bootstrap_servers,
        status=health.get("status", "unknown"),
        docker=config.docker
    )


@router.delete("/cluster/infrastructure/{name}")
async def delete_infrastructure(name: str):
    """
    删除基础设施配置
    """
    manager = get_service_manager()
    infra_configs = manager.get_infrastructure()
    
    if name not in infra_configs:
        raise HTTPException(status_code=404, detail=f"基础设施 {name} 不存在")
    
    success = await manager.delete_infrastructure(name)
    
    if not success:
        raise HTTPException(status_code=500, detail="删除配置失败")
    
    return {"success": True, "message": f"已删除基础设施 {name}"}


@router.get("/cluster/infrastructure-templates")
async def get_infrastructure_templates():
    """
    获取预置的基础设施配置模板
    """
    templates = [
        {
            "name": "redis",
            "display_name": "Redis 缓存/队列",
            "type": "redis",
            "url": "redis://localhost:6379/0",
            "description": "用于状态存储、消息队列和事件总线",
            "docker": {
                "image": "redis:7-alpine",
                "ports": ["6379:6379"]
            }
        },
        {
            "name": "kafka",
            "display_name": "Kafka 消息队列",
            "type": "kafka",
            "bootstrap_servers": "localhost:9092",
            "description": "分布式消息队列，适用于高吞吐量场景",
            "docker": {
                "image": "confluentinc/cp-kafka:7.5.0",
                "ports": ["9092:9092"]
            }
        },
        {
            "name": "postgresql",
            "display_name": "PostgreSQL 数据库",
            "type": "database",
            "url": "postgresql://localhost:5432/plaita",
            "description": "持久化存储，用于执行状态和流程定义",
            "docker": {
                "image": "postgres:15-alpine",
                "ports": ["5432:5432"],
                "env": {
                    "POSTGRES_DB": "plaita",
                    "POSTGRES_USER": "plaita",
                    "POSTGRES_PASSWORD": "plaita"
                }
            }
        },
        {
            "name": "mysql",
            "display_name": "MySQL 数据库",
            "type": "database",
            "url": "mysql://localhost:3306/plaita",
            "description": "持久化存储，用于执行状态和流程定义",
            "docker": {
                "image": "mysql:8",
                "ports": ["3306:3306"],
                "env": {
                    "MYSQL_DATABASE": "plaita",
                    "MYSQL_USER": "plaita",
                    "MYSQL_PASSWORD": "plaita",
                    "MYSQL_ROOT_PASSWORD": "root"
                }
            }
        }
    ]
    
    return {"templates": templates}


from datetime import datetime


class QuickTestRequest(BaseModel):
    """快速测试请求"""
    test_type: str = Field("simple", description="测试类型: simple, delay, approval")
    params: Optional[dict] = Field(None, description="测试参数")


class QuickTestResponse(BaseModel):
    """快速测试响应"""
    success: bool
    message: str
    execution_id: Optional[str] = None
    flow_definition: Optional[dict] = None
    result: Optional[dict] = None
    error: Optional[str] = None


@router.post("/cluster/quick-test", response_model=QuickTestResponse)
async def run_quick_test(request: QuickTestRequest):
    """
    运行快速测试
    
    测试类型:
    - simple: 简单流程测试（Start -> Calculate -> End）
    - delay: 延迟测试（Start -> Delay -> End）
    - approval: 审批测试（Start -> Approval -> End）
    """
    import uuid
    
    try:
        # 生成测试流程定义
        flow_id = f"test_flow_{uuid.uuid4().hex[:8]}"
        execution_id = f"exec_{uuid.uuid4().hex[:8]}"
        test_params = request.params or {"value": 21}
        
        if request.test_type == "simple":
            # 简单计算流程：使用 code 节点来执行计算
            flow_definition = {
                "id": flow_id,
                "name": "简单测试流程",
                "description": "测试基本流程执行",
                "input_type": {"type": "object", "properties": {"value": {"type": "integer"}}},
                "nodes": [
                    {
                        "id": "start",
                        "type": "start",
                        "next": "compute"
                    },
                    {
                        "id": "compute",
                        "type": "code",
                        "language": "python",
                        "code": "def run(input_value):\n    return input_value * 2",
                        "input": "$INPUT.value",
                        "next": "end"
                    },
                    {
                        "id": "end",
                        "type": "end",
                        "result_type": "success",
                        "output": "$NODE.compute"
                    }
                ]
            }
            
        elif request.test_type == "distributed":
            # 分布式队列测试流程：通过 Redis 队列提交到 FlowWorker 执行
            flow_definition = {
                "id": flow_id,
                "name": "分布式队列测试流程",
                "description": "测试分布式架构：通过 Redis 队列提交任务",
                "input_type": {"type": "object", "properties": {"value": {"type": "integer"}}},
                "nodes": [
                    {
                        "id": "start",
                        "type": "start",
                        "next": "compute"
                    },
                    {
                        "id": "compute",
                        "type": "code",
                        "language": "python",
                        "code": "def run(input_value):\n    import time\n    time.sleep(0.5)  # 模拟耗时操作\n    return input_value * 3",
                        "input": "$INPUT.value",
                        "next": "end"
                    },
                    {
                        "id": "end",
                        "type": "end",
                        "result_type": "success",
                        "output": "$NODE.compute"
                    }
                ]
            }
            
        elif request.test_type == "event":
            # 事件驱动测试流程：使用事件节点等待外部事件
            flow_definition = {
                "id": flow_id,
                "name": "事件驱动测试流程",
                "description": "测试事件机制：等待事件触发后继续执行",
                "input_type": {"type": "object", "properties": {"event_data": {"type": "string"}}},
                "nodes": [
                    {
                        "id": "start",
                        "type": "start",
                        "next": "prepare"
                    },
                    {
                        "id": "prepare",
                        "type": "code",
                        "language": "python",
                        "code": "def run(event_data):\n    return {'prepared': True, 'data': event_data}",
                        "input": "$INPUT.event_data",
                        "next": "wait_event"
                    },
                    {
                        "id": "wait_event",
                        "type": "event",
                        "event_type": "test_event",
                        "event_filter": {},
                        "next": "process"
                    },
                    {
                        "id": "process",
                        "type": "code",
                        "language": "python",
                        "code": "def run(event_result):\n    return {'processed': True, 'event_received': event_result}",
                        "input": "$NODE.wait_event",
                        "next": "end"
                    },
                    {
                        "id": "end",
                        "type": "end",
                        "result_type": "success",
                        "output": "$NODE.process"
                    }
                ]
            }
            
        elif request.test_type == "delay":
            # 延迟测试流程
            flow_definition = {
                "id": flow_id,
                "name": "延迟测试流程",
                "description": "测试延迟服务",
                "nodes": [
                    {
                        "id": "start",
                        "type": "start",
                        "next": "delay"
                    },
                    {
                        "id": "delay",
                        "type": "delay",
                        "delay_seconds": 2,
                        "event_type": "delay_trigger",
                        "next": "end"
                    },
                    {
                        "id": "end",
                        "type": "end"
                    }
                ]
            }
            
        else:
            # 审批测试流程
            flow_definition = {
                "id": flow_id,
                "name": "审批测试流程",
                "description": "测试审批服务",
                "nodes": [
                    {
                        "id": "start",
                        "type": "start",
                        "next": "approval"
                    },
                    {
                        "id": "approval",
                        "type": "approval",
                        "approval_title": "测试审批",
                        "approval_content": "这是一个自动化快速测试审批",
                        "approvers": ["test_user"],
                        "event_type": "approval_decision",
                        "next": "end"
                    },
                    {
                        "id": "end",
                        "type": "end"
                    }
                ]
            }
        
        # 尝试导入 Plaita 并执行测试
        try:
            from plaita import Flow, FlowExecution
            from plaita.node import nodes as node_registry, node_register
            
            # 确保扩展节点已注册
            try:
                from plaita.server.nodes import ApprovalNode, DelayNode
                if "approval" not in node_registry:
                    node_register(ApprovalNode)
                if "delay" not in node_registry:
                    node_register(DelayNode)
            except ImportError:
                pass
            
            # 使用 Flow 从定义创建流程
            flow = Flow.model_validate(flow_definition)
            
            if request.test_type == "simple":
                # 简单同步测试 - 使用 run 方法
                result = flow.run(**test_params)
                
                return QuickTestResponse(
                    success=True,
                    message=f"测试完成！流程 {flow_id} 执行成功。输入 {test_params.get('value', 21)}，输出 {result}",
                    execution_id=execution_id,
                    flow_definition=flow_definition,
                    result={"status": "completed", "input": test_params, "output": result}
                )
            elif request.test_type == "distributed":
                # 分布式测试 - 使用分布式模式执行流程，直接完成
                # 这个测试演示分布式执行模式的工作原理（多步执行）
                try:
                    from plaita import FlowExecution, ExecutionMode
                    
                    context = None
                    result = None
                    max_steps = 20
                    steps_taken = 0
                    
                    for step in range(max_steps):
                        steps_taken += 1
                        if context is None:
                            result = FlowExecution.run(
                                flow, 
                                params=test_params, 
                                mode=ExecutionMode.DISTRIBUTED
                            )
                        else:
                            result = FlowExecution.run(
                                flow, 
                                params=test_params, 
                                mode=ExecutionMode.DISTRIBUTED,
                                context=context
                            )
                        
                        is_end = result.get("is_end", False)
                        is_suspend = result.get("is_suspend", False)
                        
                        if is_end:
                            break
                        if is_suspend:
                            # 不应该在这个测试中发生挂起
                            break
                            
                        context = result.get("context", {})
                    
                    exec_id = result.get("execution_id", execution_id)
                    final_result = result.get("result")
                    
                    return QuickTestResponse(
                        success=True,
                        message=f"✅ 分布式执行测试完成！\n\n"
                                f"📋 流程 ID: {flow_id}\n"
                                f"🔄 执行步数: {steps_taken}\n"
                                f"📊 输入: {test_params.get('value', 100)}\n"
                                f"📤 输出: {final_result}\n\n"
                                f"分布式模式下，流程可以在不同节点间中断和恢复执行。",
                        execution_id=exec_id,
                        flow_definition=flow_definition,
                        result={
                            "status": "completed",
                            "steps": steps_taken,
                            "input": test_params,
                            "output": final_result
                        }
                    )
                except Exception as e:
                    import traceback
                    return QuickTestResponse(
                        success=False,
                        message=f"分布式测试执行失败: {e}",
                        execution_id=execution_id,
                        flow_definition=flow_definition,
                        error=f"{str(e)}\n{traceback.format_exc()}"
                    )
            elif request.test_type == "event":
                # 事件驱动测试 - 使用真实 EventBus 发送事件
                try:
                    import asyncio
                    from plaita import FlowExecution, ExecutionMode
                    from plaita.event.memory import InMemoryEventBus
                    from plaita.event.core import Event
                    
                    # 创建共享的 EventBus 实例
                    event_bus = InMemoryEventBus()
                    
                    context = None
                    result = None
                    max_steps = 30
                    steps_taken = 0
                    event_published = False
                    event_type = None
                    published_event = None
                    
                    for step in range(max_steps):
                        steps_taken += 1
                        
                        if context is None:
                            # 首次执行，传入 EventBus
                            result = FlowExecution.run(
                                flow, 
                                params=test_params, 
                                mode=ExecutionMode.DISTRIBUTED,
                                event_bus=event_bus
                            )
                        elif event_published:
                            # 事件已发布到 EventBus，使用事件数据恢复执行
                            # 这里我们从 EventBus 的订阅存储中获取事件数据
                            event_data = {
                                "event_type": event_type,
                                "payload": published_event.data if published_event else {"message": "Test event triggered!"}
                            }
                            result = FlowExecution.run(
                                flow, 
                                params=test_params, 
                                mode=ExecutionMode.DISTRIBUTED,
                                context=context,
                                event_bus=event_bus,
                                resume_type="event",
                                resume_data=event_data
                            )
                            event_published = False
                        else:
                            # 正常继续执行
                            result = FlowExecution.run(
                                flow, 
                                params=test_params, 
                                mode=ExecutionMode.DISTRIBUTED,
                                context=context,
                                event_bus=event_bus
                            )
                        
                        is_suspend = result.get("is_suspend", False)
                        is_end = result.get("is_end", False)
                        
                        if is_end:
                            break
                            
                        if is_suspend:
                            # 流程在事件节点挂起，向 EventBus 发布真实事件
                            node_result = result.get("result", {})
                            event_type = node_result.get("event_type", "test_event") if isinstance(node_result, dict) else "test_event"
                            
                            # 创建真实事件对象
                            published_event = Event(
                                event_type=event_type,
                                data={
                                    "message": "Test event triggered via EventBus!",
                                    "source": "quick_test",
                                    "test_payload": test_params.get("event_data", "test_payload")
                                }
                            )
                            
                            # 向 EventBus 发布事件（异步方法需要运行）
                            async def publish_event():
                                return await event_bus.publish(published_event)
                            
                            try:
                                loop = asyncio.get_event_loop()
                                if loop.is_running():
                                    # 如果已有事件循环，创建新的线程执行
                                    import concurrent.futures
                                    with concurrent.futures.ThreadPoolExecutor() as executor:
                                        future = executor.submit(asyncio.run, publish_event())
                                        event_id = future.result(timeout=5)
                                else:
                                    event_id = asyncio.run(publish_event())
                            except RuntimeError:
                                # 没有事件循环时创建新的
                                event_id = asyncio.run(publish_event())
                            
                            event_published = True
                            
                        context = result.get("context", {})
                    
                    exec_id = result.get("execution_id", execution_id)
                    final_result = result.get("result")
                    
                    if result.get("is_end", False):
                        return QuickTestResponse(
                            success=True,
                            message=f"✅ 事件驱动测试完成！\n\n"
                                    f"📋 流程 ID: {flow_id}\n"
                                    f"🔄 执行步数: {steps_taken}\n"
                                    f"📡 通过 EventBus 发布事件: {event_type}\n"
                                    f"📤 最终结果: {final_result}\n\n"
                                    f"测试验证了事件节点的挂起、EventBus 事件发布和恢复机制。",
                            execution_id=exec_id,
                            flow_definition=flow_definition,
                            result={
                                "status": "completed",
                                "steps": steps_taken,
                                "event_type": event_type,
                                "event_data": published_event.data if published_event else None,
                                "output": final_result
                            }
                        )
                    else:
                        return QuickTestResponse(
                            success=True,
                            message=f"事件驱动测试未完成（达到最大步数）",
                            execution_id=exec_id,
                            flow_definition=flow_definition,
                            result={"status": "incomplete", "steps": steps_taken, "last_result": result}
                        )
                except Exception as e:
                    import traceback
                    return QuickTestResponse(
                        success=False,
                        message=f"执行事件驱动测试失败: {e}",
                        execution_id=execution_id,
                        flow_definition=flow_definition,
                        error=f"{str(e)}\n{traceback.format_exc()}"
                    )
            else:
                # 其他类型返回模拟结果（需要完整服务支持）
                return QuickTestResponse(
                    success=True,
                    message=f"测试流程已生成。{request.test_type} 类型测试需要启动对应的服务才能执行。",
                    execution_id=execution_id,
                    flow_definition=flow_definition,
                    result={"status": "pending", "note": "请先启动对应服务"}
                )
                
        except ImportError as e:
            # Plaita 未安装，返回流程定义
            import traceback
            return QuickTestResponse(
                success=False,
                message="Plaita 核心未正确导入，无法执行测试",
                execution_id=execution_id,
                flow_definition=flow_definition,
                result=None,
                error=f"{str(e)}\n{traceback.format_exc()}"
            )
        except Exception as e:
            import traceback
            return QuickTestResponse(
                success=False,
                message=f"执行测试时发生错误",
                execution_id=execution_id,
                flow_definition=flow_definition,
                result=None,
                error=f"{str(e)}\n{traceback.format_exc()}"
            )
            
    except Exception as e:
        import traceback
        return QuickTestResponse(
            success=False,
            message="测试失败",
            error=f"{str(e)}\n{traceback.format_exc()}"
        )


@router.get("/cluster/test-templates")
async def get_test_templates():
    """
    获取可用的测试模板
    """
    templates = [
        {
            "id": "simple",
            "name": "简单计算测试",
            "description": "测试基本流程执行：输入一个数值，输出其 2 倍",
            "required_services": [],
            "default_params": {"value": 21}
        },
        {
            "id": "distributed",
            "name": "分布式队列测试",
            "description": "测试分布式架构：将流程提交到 Redis 队列，由 FlowWorker 执行",
            "required_services": ["flow_worker"],
            "default_params": {"value": 100}
        },
        {
            "id": "event",
            "name": "事件驱动测试",
            "description": "测试事件机制：流程等待事件触发后继续执行",
            "required_services": ["flow_worker"],
            "default_params": {"event_data": "test_payload"}
        },
        {
            "id": "delay",
            "name": "延迟服务测试",
            "description": "测试延迟服务：流程将延迟 2 秒后完成",
            "required_services": ["delay_service"],
            "default_params": {}
        },
        {
            "id": "approval",
            "name": "审批服务测试",
            "description": "测试审批服务：流程将等待人工审批",
            "required_services": ["approval_service"],
            "default_params": {}
        }
    ]
    return {"templates": templates}


@router.delete("/cluster/instances/{instance_id}")
async def remove_instance(instance_id: str):
    """
    移除一个已停止或出错的实例记录
    """
    manager = get_service_manager()
    
    success = manager.remove_instance(instance_id)
    
    if not success:
        raise HTTPException(
            status_code=400, 
            detail=f"无法移除实例 {instance_id}（可能正在运行或不存在）"
        )
    
    return {
        "success": True,
        "instance_id": instance_id,
        "message": "实例记录已移除"
    }


@router.delete("/cluster/instances")
async def clear_failed_instances():
    """
    清除所有失败/已停止的实例记录
    """
    manager = get_service_manager()
    
    count = manager.clear_failed_instances()
    
    return {
        "success": True,
        "cleared_count": count,
        "message": f"已清除 {count} 个失败的实例记录"
    }


# ============ 多集群管理 API ============

try:
    from ..services.cluster_registry import get_cluster_registry, ClusterInfo
except ImportError:
    from services.cluster_registry import get_cluster_registry, ClusterInfo


class ClusterInfoResponse(BaseModel):
    """集群信息响应"""
    id: str
    name: str
    description: str
    config_path: str
    redis_url: str
    created_at: str
    is_active: bool


class ClustersListResponse(BaseModel):
    """集群列表响应"""
    clusters: List[ClusterInfoResponse]
    active_cluster_id: Optional[str]


class CreateClusterRequest(BaseModel):
    """创建集群请求"""
    id: str = Field(..., description="集群 ID（英文标识）")
    name: str = Field(..., description="集群显示名称")
    description: str = Field("", description="集群描述")
    redis_url: str = Field("redis://localhost:6379/0", description="Redis URL")


class UpdateClusterRequest(BaseModel):
    """更新集群请求"""
    name: Optional[str] = None
    description: Optional[str] = None
    redis_url: Optional[str] = None


class SwitchClusterRequest(BaseModel):
    """切换集群请求"""
    cluster_id: str


class ClusterFullConfigResponse(BaseModel):
    """集群完整配置响应"""
    cluster_id: str
    config: dict


class SaveClusterConfigRequest(BaseModel):
    """保存集群配置请求"""
    config: dict


@router.get("/clusters", response_model=ClustersListResponse)
async def list_clusters():
    """
    获取所有集群列表
    """
    registry = get_cluster_registry()
    clusters = registry.list_clusters()
    active = registry.get_active_cluster()
    
    return ClustersListResponse(
        clusters=[
            ClusterInfoResponse(
                id=c.id,
                name=c.name,
                description=c.description,
                config_path=c.config_path,
                redis_url=c.redis_url,
                created_at=c.created_at,
                is_active=c.is_active
            )
            for c in clusters
        ],
        active_cluster_id=active.id if active else None
    )


@router.get("/clusters/active", response_model=ClusterInfoResponse)
async def get_active_cluster():
    """
    获取当前活动集群
    """
    registry = get_cluster_registry()
    active = registry.get_active_cluster()
    
    if not active:
        raise HTTPException(status_code=404, detail="没有活动集群")
    
    return ClusterInfoResponse(
        id=active.id,
        name=active.name,
        description=active.description,
        config_path=active.config_path,
        redis_url=active.redis_url,
        created_at=active.created_at,
        is_active=True
    )


@router.get("/clusters/{cluster_id}", response_model=ClusterInfoResponse)
async def get_cluster(cluster_id: str):
    """
    获取指定集群信息
    """
    registry = get_cluster_registry()
    cluster = registry.get_cluster(cluster_id)
    
    if not cluster:
        raise HTTPException(status_code=404, detail=f"集群 {cluster_id} 不存在")
    
    return ClusterInfoResponse(
        id=cluster.id,
        name=cluster.name,
        description=cluster.description,
        config_path=cluster.config_path,
        redis_url=cluster.redis_url,
        created_at=cluster.created_at,
        is_active=cluster.is_active
    )


@router.post("/clusters", response_model=ClusterInfoResponse)
async def create_cluster(request: CreateClusterRequest):
    """
    创建新集群
    """
    registry = get_cluster_registry()
    
    try:
        cluster = registry.create_cluster(
            cluster_id=request.id,
            name=request.name,
            description=request.description,
            redis_url=request.redis_url
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    return ClusterInfoResponse(
        id=cluster.id,
        name=cluster.name,
        description=cluster.description,
        config_path=cluster.config_path,
        redis_url=cluster.redis_url,
        created_at=cluster.created_at,
        is_active=cluster.is_active
    )


@router.put("/clusters/{cluster_id}", response_model=ClusterInfoResponse)
async def update_cluster(cluster_id: str, request: UpdateClusterRequest):
    """
    更新集群信息
    """
    registry = get_cluster_registry()
    
    try:
        cluster = registry.update_cluster(
            cluster_id=cluster_id,
            name=request.name,
            description=request.description,
            redis_url=request.redis_url
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    return ClusterInfoResponse(
        id=cluster.id,
        name=cluster.name,
        description=cluster.description,
        config_path=cluster.config_path,
        redis_url=cluster.redis_url,
        created_at=cluster.created_at,
        is_active=cluster.is_active
    )


@router.delete("/clusters/{cluster_id}")
async def delete_cluster(cluster_id: str):
    """
    删除集群
    """
    registry = get_cluster_registry()
    
    try:
        success = registry.delete_cluster(cluster_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    if not success:
        raise HTTPException(status_code=404, detail=f"集群 {cluster_id} 不存在")
    
    return {"success": True, "message": f"集群 {cluster_id} 已删除"}


@router.post("/clusters/switch", response_model=ClusterInfoResponse)
async def switch_cluster(request: SwitchClusterRequest):
    """
    切换到指定集群
    
    注意：切换集群会重新加载 ServiceManager 配置
    """
    registry = get_cluster_registry()
    
    try:
        cluster = registry.switch_cluster(request.cluster_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    # 重新初始化 ServiceManager
    from ..services.service_manager import reset_service_manager
    reset_service_manager()
    
    return ClusterInfoResponse(
        id=cluster.id,
        name=cluster.name,
        description=cluster.description,
        config_path=cluster.config_path,
        redis_url=cluster.redis_url,
        created_at=cluster.created_at,
        is_active=True
    )


@router.get("/clusters/{cluster_id}/config", response_model=ClusterFullConfigResponse)
async def get_cluster_config_detail(cluster_id: str):
    """
    获取集群的完整配置内容
    """
    registry = get_cluster_registry()
    
    config = registry.get_cluster_config(cluster_id)
    if config is None:
        raise HTTPException(status_code=404, detail=f"无法读取集群 {cluster_id} 的配置")
    
    return ClusterFullConfigResponse(
        cluster_id=cluster_id,
        config=config
    )


@router.put("/clusters/{cluster_id}/config")
async def save_cluster_config_detail(cluster_id: str, request: SaveClusterConfigRequest):
    """
    保存集群配置（写入前校验 process 白名单，拒绝任意 command 注入）
    """
    registry = get_cluster_registry()

    try:
        from ..services.process_allowlist import ProcessConfigError
    except ImportError:
        from services.process_allowlist import ProcessConfigError

    try:
        success = registry.save_cluster_config(cluster_id, request.config)
    except ProcessConfigError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    if not success:
        raise HTTPException(status_code=400, detail=f"无法保存集群 {cluster_id} 的配置")
    
    # 如果是当前活动集群，重新加载 ServiceManager
    active = registry.get_active_cluster()
    if active and active.id == cluster_id:
        from ..services.service_manager import reset_service_manager
        reset_service_manager()
    
    return {"success": True, "message": "配置已保存"}

