import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Union

from pydantic import BaseModel, Field

from ..logger import logger


class ExecutionState(BaseModel):
    """
    流程执行状态模型，用于表示流程执行的完整状态信息
    
    属性:
        execution_id: 执行ID，唯一标识一次流程执行
        flow_id: 流程ID，标识所属流程
        flow_name: 流程名称，标识流程名称
        flow_version: 流程版本号，标识流程定义版本
        flow_hash: 启动时实际加载执行的 Flow 定义指纹（sha256，可选）。
            resume 时与当前定义指纹比对，不一致则拒绝续跑（运行中发布新
            版本会导致挂起执行走错分支）；None（老状态/老 worker 写入）
            跳过校验，零回归。
        context: 执行上下文，存储流程执行过程中的所有状态数据
        status: 当前状态，包括 running(运行中)、suspended(挂起)、completed(完成)、error(错误)
        start_time: 流程开始时间，ISO格式字符串
        last_update_time: 最后更新时间，ISO格式字符串
        end_time: 流程结束时间，ISO格式字符串
        error: 错误信息，当status为error时存储错误详情
        invoker: 调用者，标识发起流程的实体
    """
    execution_id: str
    flow_id: Optional[str] = None
    flow_name: Optional[str] = None
    flow_version: Optional[str] = None
    flow_hash: Optional[str] = None
    tenant_id: Optional[str] = None
    context: Dict[str, Any]
    status: str = Field(default="running")
    start_time: Optional[str] = None
    last_update_time: Optional[str] = None
    end_time: Optional[str] = None
    error: Optional[Dict[str, Any]] = None
    invoker: Optional[str] = None
    node_timings: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "节点级耗时（node_id → {started_at, ended_at, started_ms, ended_ms, "
            "duration_ms, total_duration_ms, attempts, failed}）。None = 老状态或宿主"
            "未挂 NodeTimingCallback——读取方必须按缺省处理，不得假定存在。"
        ),
    )
    queued_at: Optional[str] = Field(
        None,
        description=(
            "入队时间（ISO 字符串，取任务消息里的 timestamp）。None = 老消息或非"
            "队列路径（本地档 / 直接调用 start_flow）。"
        ),
    )
    queue_wait_ms: Optional[int] = Field(
        None,
        description=(
            "排队时长（毫秒）：queued_at → worker 认账开始执行的间隔。None = 无"
            "queued_at 或时间戳不可解析；跨机时钟回拨时为 0（时间戳带偏移时按"
            "同一时轴差分，TZ 差异不构成假等待；真实时钟偏斜不补偿）。"
        ),
    )

class ExecutionStorage(ABC):
    """
    状态存储基类，定义了所有状态存储必须实现的接口
    """

    @abstractmethod
    def save_execution_state(self, execution_id: str, state: ExecutionState) -> bool:
        """
        保存流程执行状态
        
        Args:
            execution_id: 执行ID
            state: 执行状态数据
            
        Returns:
            bool: 是否保存成功
        """
        pass
    
    @abstractmethod
    def load_execution_state(self, execution_id: str) -> Optional[ExecutionState]:
        """
        加载流程执行状态
        
        Args:
            execution_id: 执行ID
            
        Returns:
            ExecutionState or None: 执行状态数据，如果不存在则返回None
        """
        pass
    
    @abstractmethod
    def delete_execution_state(self, execution_id: str) -> bool:
        """
        删除流程执行状态
        
        Args:
            execution_id: 执行ID
            
        Returns:
            bool: 是否删除成功
        """
        pass
    
    @abstractmethod
    def list_executions(self, query: Optional[Any] = None, order_by: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[ExecutionState]:
        """
        列出执行ID
        
        Args:
            query: 查询条件
            order_by: 排序字段
            limit: 最大返回数量
            offset: 偏移量
            
        Returns:
            List[ExecutionState]: 执行状态列表
        """
        pass


    def serialize_state(self, state: Dict[str, Any]) -> str:
        """
        序列化状态数据
        
        Args:
            state: 状态数据
            
        Returns:
            str: 序列化后的字符串
        """
        try:
            return json.dumps(state)
        except Exception as e:
            logger.error("Failed to serialize state: %s", e)
            raise
    
    def deserialize_state(self, data: str) -> Dict[str, Any]:
        """
        反序列化状态数据
        
        Args:
            data: 序列化的状态数据
            
        Returns:
            Dict: 反序列化后的状态数据
        """
        try:
            return json.loads(data)
        except Exception as e:
            logger.error("Failed to deserialize state: %s", e)
            raise 
    

class FlowStorage(ABC):

    @abstractmethod
    def get_flow(self, flow_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        获取流程定义
        
        Args:
            flow_id: 流程ID
            version: 流程版本号
            
        Returns:
            Dict or None: 流程定义数据，如果不存在则返回None
        """
        pass
    
    @abstractmethod
    def save_flow(self, flow: Dict[str, Any]) -> bool:
        """
        保存流程定义
        """
        pass
    
    