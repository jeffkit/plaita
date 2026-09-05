"""
执行实例管理 API
提供执行列表、详情、启动、停止等接口
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from redis import Redis
from sse_starlette.sse import EventSourceResponse

router = APIRouter()


# ============ 数据模型 ============

class ExecutionInfo(BaseModel):
    """执行实例信息"""
    execution_id: str = Field(..., description="执行 ID")
    flow_id: str = Field(..., description="流程 ID")
    flow_version: Optional[str] = Field(None, description="流程版本")
    status: str = Field(..., description="状态")
    start_time: Optional[str] = Field(None, description="开始时间")
    end_time: Optional[str] = Field(None, description="结束时间")
    last_update_time: Optional[str] = Field(None, description="最后更新时间")
    context: Optional[Dict[str, Any]] = Field(None, description="执行上下文")
    error: Optional[Dict[str, Any]] = Field(None, description="错误信息")
    invoker: Optional[str] = Field(None, description="调用者")
    # 本地单机模式专有：节点级 trace 与最终输出（集群模式为 None）
    nodes: Optional[List[Dict[str, Any]]] = Field(None, description="节点级执行 trace（本地模式）")
    output: Optional[Any] = Field(None, description="流程输出（本地模式）")


class ExecutionListResponse(BaseModel):
    """执行列表响应"""
    executions: List[ExecutionInfo]
    total: int
    page: int
    size: int


class StartFlowRequest(BaseModel):
    """启动流程请求"""
    flow_id: str = Field(..., description="流程 ID")
    version: Optional[str] = Field(None, description="流程版本")
    params: Dict[str, Any] = Field(default_factory=dict, description="输入参数")


class ResumeFlowRequest(BaseModel):
    """恢复流程请求"""
    resume_type: str = Field(..., description="恢复类型: continue, cancel, timeout, event")
    data: Optional[Dict[str, Any]] = Field(None, description="恢复数据")


# ============ 工具函数 ============

# 任务队列 Stream 名（与 FlowWorker 的 PLAITA_QUEUE_NAME 默认值一致）
TASK_QUEUE_NAME = "plaita:flow:queue"

try:
    from plaita.server.task_queue import enqueue_task
except ImportError:  # 平铺布局（cwd=backend）运行时
    import sys as _sys
    from pathlib import Path as _Path
    _plaita_root = str(_Path(__file__).resolve().parents[3])
    if _plaita_root not in _sys.path:
        _sys.path.insert(0, _plaita_root)
    from plaita.server.task_queue import enqueue_task

try:
    from services import flow_store
except ImportError:  # 包内布局（pip 安装）
    from .services import flow_store  # type: ignore


def get_redis(request: Request) -> Redis:
    """获取 Redis 客户端（本地单机模式下拒绝并给出明确提示）"""
    redis = request.app.state.redis
    if redis is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "当前为本地单机模式（未连接 Redis），该功能不可用。"
                "启动 Redis 并重启 console 可恢复完整集群能力。"
            ),
        )
    return redis


def get_redis_or_none(request: Request) -> Optional[Redis]:
    """本地模式返回 None 而不报错——executions 端点据此走本地执行分支。"""
    return request.app.state.redis


def get_local_executor(request: Request):
    """本地单机模式分支：返回 local_executor 模块；集群模式返回 None。"""
    if getattr(request.app.state, "local_mode", False):
        try:
            from .services import local_executor
        except ImportError:
            from services import local_executor  # type: ignore
        return local_executor
    return None


def _enqueue(message: Dict[str, Any], redis: Redis) -> str:
    """把 start/resume 消息写入任务队列 Stream。

    FlowWorker 以 Redis Stream 消费组（XREADGROUP）消费；
    历史上这里误用 rpush（list 类型），与 Stream 同 key 类型冲突，
    消息永远到不了 worker。统一走 XADD。
    """
    return enqueue_task(redis, TASK_QUEUE_NAME, message)


# ============ API 端点 ============

@router.get("/executions", response_model=ExecutionListResponse)
async def list_executions(
    request: Request,
    page: int = 1,
    size: int = 20,
    status: Optional[str] = None,
    flow_id: Optional[str] = None,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    获取执行实例列表
    
    - **page**: 页码（从 1 开始）
    - **size**: 每页数量
    - **status**: 按状态筛选
    - **flow_id**: 按流程 ID 筛选
    """
    if (local := get_local_executor(request)) is not None:
        executions = [
            ExecutionInfo(**info) for info in local.list_local_executions(status=status, flow_id=flow_id)
        ]
    else:
        # 获取所有执行状态
        pattern = "plaita:execution:*"
        keys = redis.keys(pattern)
        
        executions = []
        for key in keys:
            data = redis.get(key)
            if data:
                try:
                    info = json.loads(data)
                    
                    # 筛选
                    if status and info.get("status") != status:
                        continue
                    if flow_id and info.get("flow_id") != flow_id:
                        continue
                    
                    executions.append(ExecutionInfo(**info))
                except Exception:
                    continue
    
    # 按开始时间排序（最新的在前）
    executions.sort(
        key=lambda x: x.start_time or "",
        reverse=True
    )
    
    # 分页
    total = len(executions)
    start = (page - 1) * size
    end = start + size
    paginated = executions[start:end]
    
    return ExecutionListResponse(
        executions=paginated,
        total=total,
        page=page,
        size=size
    )


@router.get("/executions/{execution_id}", response_model=ExecutionInfo)
async def get_execution(
    execution_id: str,
    request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    获取执行详情
    
    - **execution_id**: 执行 ID
    """
    if (local := get_local_executor(request)) is not None:
        info = local.get_local_execution(execution_id)
        if info is None:
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        return ExecutionInfo(**info)

    key = f"plaita:execution:{execution_id}"
    data = redis.get(key)
    
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
    
    try:
        info = json.loads(data)
        return ExecutionInfo(**info)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据解析失败: {e}")


@router.post("/executions")
async def start_execution(
    request: StartFlowRequest,
    http_request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    启动新的流程执行
    
    - **flow_id**: 流程 ID
    - **version**: 流程版本
    - **params**: 输入参数
    """
    # 本地单机模式：console 进程内直接执行
    if (local := get_local_executor(http_request)) is not None:
        try:
            info = local.start_local_execution(
                flow_store.get_flow_store(), request.flow_id, request.version, request.params
            )
        except LookupError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {
            "status": "running",
            "flow_id": request.flow_id,
            "execution_id": info["execution_id"],
            "message": "本地模式：流程已在 console 进程内启动",
        }

    # 构建任务消息
    message = {
        "type": "start",
        "flow_id": request.flow_id,
        "version": request.version,
        "params": request.params,
        "timestamp": datetime.now().isoformat()
    }
    
    # 写入任务队列 Stream（FlowWorker 消费组消费）
    _enqueue(message, redis)
    
    return {
        "status": "queued",
        "flow_id": request.flow_id,
        "message": "流程启动请求已加入队列"
    }


@router.post("/executions/{execution_id}/cancel")
async def cancel_execution(
    execution_id: str,
    request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    取消/终止执行（发送取消命令到队列）
    
    - **execution_id**: 执行 ID
    """
    # 本地单机模式：标记状态（尽力而为，不中断线程）
    if (local := get_local_executor(request)) is not None:
        if not local.cancel_local_execution(execution_id):
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        return {
            "success": True,
            "status": "cancelled",
            "execution_id": execution_id,
            "message": "执行已取消（本地模式）",
        }

    # 验证执行存在
    key = f"plaita:execution:{execution_id}"
    data = redis.get(key)
    
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
    
    try:
        info = json.loads(data)
        flow_id = info.get("flow_id")
        status = info.get("status")
    except Exception:
        raise HTTPException(status_code=500, detail="数据解析失败")
    
    # 发送取消消息到队列（如果有 FlowWorker 在监听）
    message = {
        "type": "resume",
        "flow_id": flow_id,
        "execution_id": execution_id,
        "resume_type": "cancel",
        "data": None,
        "timestamp": datetime.now().isoformat()
    }
    
    _enqueue(message, redis)

    # 同时直接更新状态（以防 FlowWorker 不在线）
    info["status"] = "cancelled"
    info["end_time"] = datetime.now().isoformat()
    redis.set(key, json.dumps(info))
    
    return {
        "success": True,
        "status": "cancelled",
        "execution_id": execution_id,
        "message": "执行已取消"
    }


@router.delete("/executions/{execution_id}")
async def delete_execution(
    execution_id: str,
    request: Request,
    redis: Optional[Redis] = Depends(get_redis_or_none),
):
    """
    删除执行记录（从 Redis 中永久删除）
    
    - **execution_id**: 执行 ID
    """
    # 本地单机模式：从 SQLite 删除
    if (local := get_local_executor(request)) is not None:
        if not local.delete_local_execution(execution_id):
            raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
        return {
            "success": True,
            "execution_id": execution_id,
            "message": "执行记录已删除",
        }

    key = f"plaita:execution:{execution_id}"
    
    # 检查是否存在
    if not redis.exists(key):
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
    
    # 删除记录
    redis.delete(key)
    
    # 同时删除相关的事件通道（如果存在）
    event_key = f"plaita:execution:events:{execution_id}"
    redis.delete(event_key)
    
    return {
        "success": True,
        "execution_id": execution_id,
        "message": "执行记录已删除"
    }


@router.post("/executions/{execution_id}/resume")
async def resume_execution(
    execution_id: str,
    request: ResumeFlowRequest,
    redis: Redis = Depends(get_redis)
):
    """
    恢复暂停的执行
    
    - **execution_id**: 执行 ID
    - **resume_type**: 恢复类型
    - **data**: 恢复数据
    """
    # 验证执行存在
    key = f"plaita:execution:{execution_id}"
    data = redis.get(key)
    
    if not data:
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
    
    try:
        info = json.loads(data)
        flow_id = info.get("flow_id")
    except Exception:
        raise HTTPException(status_code=500, detail="数据解析失败")
    
    # 发送恢复消息
    message = {
        "type": "resume",
        "flow_id": flow_id,
        "execution_id": execution_id,
        "resume_type": request.resume_type,
        "data": request.data,
        "timestamp": datetime.now().isoformat()
    }
    
    _enqueue(message, redis)

    return {
        "status": "resuming",
        "execution_id": execution_id,
        "resume_type": request.resume_type,
        "message": "恢复请求已加入队列"
    }


@router.get("/executions/{execution_id}/stream")
async def stream_execution(
    execution_id: str,
    request: Request,
    redis: Redis = Depends(get_redis)
):
    """
    SSE 端点：实时推送执行状态变化
    
    事件类型：status_changed, context_updated, completed, error
    """
    # 验证执行存在
    key = f"plaita:execution:{execution_id}"
    if not redis.exists(key):
        raise HTTPException(status_code=404, detail=f"执行不存在: {execution_id}")
    
    async def event_generator():
        """事件生成器"""
        pubsub = redis.pubsub()
        channel = f"plaita:execution:events:{execution_id}"
        pubsub.subscribe(channel)
        
        try:
            # 发送初始状态
            data = redis.get(key)
            if data:
                yield {
                    "event": "initial_state",
                    "data": data
                }
            
            # 持续监听事件
            while True:
                message = pubsub.get_message(timeout=1.0)
                if message and message["type"] == "message":
                    yield {
                        "event": "update",
                        "data": message["data"]
                    }
                
                # 检查客户端是否断开
                if await request.is_disconnected():
                    break
        finally:
            pubsub.unsubscribe(channel)
            pubsub.close()
    
    return EventSourceResponse(event_generator())

