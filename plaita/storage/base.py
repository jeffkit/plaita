import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Union

from pydantic import BaseModel, Field

from ..logger import logger
from plaita.core.url_redaction import _scrub_url_text


class StateSerializationError(RuntimeError):
    """执行状态**不可序列化**（写侧永久失败）——与「Redis 瞬断」严格区分。

    为什么必须有这个类型（plaita#80）：`RedisExecutionStorage.save_execution_state`
    原先用**一个** `except Exception -> return False` 把两类性质完全不同的故障
    混成同一返回值：

    * **不可序列化**：`state.context` 里出现了非 JSON 值。重试一万次也不会变好；
    * **Redis 瞬断**：该退避后重投。

    调用方（FlowWorker）把 `False` 一律当**瞬态**处理（不 ack、等重投），于是前者
    表现为「每次落盘都失败 → 反复从 checkpoint 重跑同一节点 → 副作用重放 → 执行
    永远拿不到终态，直到投递耗尽进 DLQ」。同一个 state 在 memory 后端恒 `True`
    （它不做 JSON 序列化），所以这个缺陷只在 Redis 后端显形、且会被任何
    memory 后端写的单测掩盖。

    与 `ExecutionStateLoadError` 对称（读侧/写侧各一支）。**刻意不继承
    `ValueError`**：FlowWorker 把 `ValueError` 当「畸形消息」poison ack，
    继承它会让本应终态化的永久失败被当成消息格式错误。
    """

    def __init__(self, execution_id: str, cause: BaseException):
        super().__init__(
            f"执行状态不可序列化（永久失败，重试无用）：execution_id={execution_id}；"
            f"底层 {type(cause).__name__}: {cause}"
        )
        self.execution_id = execution_id
        self.cause = cause


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
    flow_hash_algo: Optional[str] = Field(
        None,
        description=(
            "flow_hash 所用算法的标记（如 flow-dump-json-sortkeys-v1）。resume 时据此"
            "分级判定：算法相同→严格比对；算法不同但哈希相同→定义未变（升级导致标签"
            "变化）直接续跑；算法不同且哈希不同→需显式 override 才续跑。None = 老状态，"
            "按当前算法保守处理。"
        ),
    )
    engine_version: Optional[str] = Field(
        None,
        description=(
            "创建该执行的引擎版本（plaita.__version__）。resume 时仅用于观测：跨 minor "
            "续跑会打 WARNING，不作为硬门（硬门由 flow_hash 承担）。不随 resume 覆写，"
            "保留「创建者版本」语义。"
        ),
    )
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
    timeline: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "run 级**有序事件日志**（plaita#25）："
            "{events: [{seq, type, at, at_ms, node_id?, duration_ms?, ok?, error?}], "
            "dropped, max_events, enqueued_at, queue_wait_ms}；"
            "type ∈ enqueued|run_start|node_start|node_end|run_end。"
            "queue_wait_ms/enqueued_at 为 None = **未采集**（与「排队 0ms」严格"
            "区分，读取方不得折算成 0）。None = 老状态或宿主未挂时间线采集器"
            "——读取方必须按缺省处理，不得假定存在（零回归）。"
        ),
    )
    usage: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "run 级 token 归集（plaita#37）："
            "{prompt_tokens, completion_tokens, total_tokens, llm_calls, models[], "
            "source: observed|estimated|mixed, precision, count?, tokens_per_count?}。"
            "真源是每节点结果 ``context['$NODE'][node_id]['usage']``（本字段是"
            "聚合视图，不新增第二真相源）；``count``/``tokens_per_count`` 只由"
            "``patch_count`` 回填，不在聚合时编造。"
            "None = **未采集**（老状态 / 无任何节点报 usage）——与「采集到 0」"
            "严格区分，读取方不得折算成 0；resume 不覆写既有值（零回归）。"
        ),
    )
    dispatch_pending: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "挂起派发凭据（plaita#233）：``status=suspended`` 落盘时**与状态同一"
            "行原子写入**，记录尚未确认投递的外延服务任务 "
            "``{pending: true, service_type, service_config, node_id, recorded_at}``；"
            "``_dispatch_service_task`` 成功后再清空。派发瞬断/进程在"
            "「落盘→派发」窗口被杀 ⇒ 重投载体（resume/start）据此**幂等补派一次**。"
            "None = 无待派发任务（含：老状态、正常派发成功、非服务节点挂起）——"
            "读取方不得把 None 当成「已派发」，它只表示**没有可补偿的凭据**。"
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
            logger.error("Failed to serialize state: %s", _scrub_url_text(e))
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
            logger.error("Failed to deserialize state: %s", _scrub_url_text(e))
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
    
    