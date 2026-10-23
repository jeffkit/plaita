import json
import os
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


#: ``pending.kind`` 合法值（plaita#320）。五个 kind 全部进 schema；本轮写路径
#: 只产出 ``event`` / ``service`` 两种，``timer``/``start``/``cancel`` 为
#: schema 预留（生产者与消费契约见 ``docs/WORKER_PROTOCOL.md``，随各自
#: 里程碑——#321 reconciler / 调度占位行 / 取消意图落行——启用）。
PENDING_KINDS = frozenset({"event", "timer", "service", "start", "cancel"})

#: ``kind=event`` 的 pending 缺省 deadline（秒，plaita#320 ④窗补强：deadline
#: 必填）。节点配置了 ``subscription_timeout`` 时用节点值；未配置时用本缺省
#: ——默认取宽（30 天）：deadline 的消费方（#321 reconciler / 后续巡检）以
#: 「超期可检可终局」为准绳，缺省值本身不应改变存量挂起的存活语义。
#: 可经环境变量 ``PLAITA_PENDING_EVENT_DEADLINE_SECONDS`` 覆盖（须为正数；
#: 非法值忽略并 WARNING，回落本缺省——配置错误静默生效比缺省更危险）。
PENDING_EVENT_DEADLINE_DEFAULT_SECONDS = 30 * 86400


def pending_event_deadline_default_seconds() -> float:
    """读 ``PLAITA_PENDING_EVENT_DEADLINE_SECONDS`` 环境覆盖；非法值回落缺省。"""
    raw = os.environ.get("PLAITA_PENDING_EVENT_DEADLINE_SECONDS", "")
    if not raw:
        return float(PENDING_EVENT_DEADLINE_DEFAULT_SECONDS)
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "PLAITA_PENDING_EVENT_DEADLINE_SECONDS=%r 不是正数，回落缺省 %ss",
            raw, PENDING_EVENT_DEADLINE_DEFAULT_SECONDS,
        )
        return float(PENDING_EVENT_DEADLINE_DEFAULT_SECONDS)
    if value <= 0:
        logger.warning(
            "PLAITA_PENDING_EVENT_DEADLINE_SECONDS=%r 不是正数，回落缺省 %ss",
            raw, PENDING_EVENT_DEADLINE_DEFAULT_SECONDS,
        )
        return float(PENDING_EVENT_DEADLINE_DEFAULT_SECONDS)
    return value


def validate_pending_ref(pending: Any) -> Optional[Dict[str, Any]]:
    """写侧 pending 引用校验（plaita#320，fail-closed）：合法返回归一化 dict。

    只在**构造 pending 时**调用（worker 挂起落盘前 / 测试构造）——不在
    ``ExecutionState`` 加载路径上做硬校验：畸形 pending 若让整行加载失败，
    等于把「一个坏字段」放大成「毒丸行」（resume/巡检全被挡），违背
    「读取方按缺省处理」的加字段纪律。加载容错 + 写侧 fail-closed 两面
    合起来才是完整契约。

    规则（按 kind 分叉；``migrated=True`` 为读面惰性推导产物，豁免写侧
    必填——老行推不出 deadline/event_type 不编造）：

    - ``kind`` 必属 :data:`PENDING_KINDS`；
    - ``kind=event``：``deadline`` 必填（④窗补强——「不失联（可检可终局）」
      的数据前提）；``event_type`` 必填；``ref_id``（subscription_id）可为
      None——简化 bus 替身可能不回订阅 id，与 #221 绑定校验「缺字段放行」
      同款容忍，生产 bus 恒有值；
    - ``kind=service``：``payload.service_config`` 必填（补偿派发的凭据本体）；
    - ``kind=timer``：``due_at`` 必填；
    - ``kind=start``/``kind=cancel``：本轮 schema 预留，只校验 kind 合法。
    """
    if pending is None:
        return None
    if not isinstance(pending, dict):
        raise ValueError(
            f"pending 必须是 dict（引用对象），得到 {type(pending).__name__}"
        )
    kind = pending.get("kind")
    if kind not in PENDING_KINDS:
        raise ValueError(
            f"pending.kind 非法：{kind!r}（合法值：{sorted(PENDING_KINDS)}）"
        )
    migrated = bool(pending.get("migrated"))
    if kind == "event":
        if not migrated and not pending.get("deadline"):
            raise ValueError(
                "pending(kind=event) 缺 deadline：事件先于订阅窗（#320 ④）的"
                "补强契约要求事件等待必带终局时限（节点 subscription_timeout "
                "或 PLAITA_PENDING_EVENT_DEADLINE_SECONDS 缺省）"
            )
        if not pending.get("event_type"):
            raise ValueError("pending(kind=event) 缺 event_type（等待的事件类型）")
    elif kind == "service":
        payload = pending.get("payload")
        if not migrated:
            if not isinstance(payload, dict) or not payload.get("service_config"):
                raise ValueError(
                    "pending(kind=service) 缺 payload.service_config（补偿派发凭据）"
                )
    elif kind == "timer":
        if not migrated and not pending.get("due_at"):
            raise ValueError("pending(kind=timer) 缺 due_at（定时触发时刻）")
    return dict(pending)


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
        trigger: 触发归因（可选）：调度触发的执行带 {kind, schedule_id,
            schedule_name}；None = 手动启动/老执行
        pending: 挂起/等待引用（plaita#320，见字段描述）
        generation: fenced 写世代（plaita#320，见字段描述）
        progress_at: 步提交前进时刻（plaita#320，见字段描述）
        next_step: 预分配的下一步引用（plaita#320，见字段描述）
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
    trigger: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "触发归因（A2R44-02）：调度触发（cron / console「立即触发」manual）"
            "的执行带 {kind, schedule_id, schedule_name}——触发侧写入 queued 占位"
            "行、worker 接管落行时随 start 消息透传，终态写沿用同一 state 对象。"
            "None = 手动启动（console POST/SDK）或老执行，读取方按缺省处理。"
        ),
    )
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
            "**已退役（plaita#320），勿新写**：挂起派发凭据（plaita#233）——"
            "``status=suspended`` 落盘时与状态同一行原子写入的外延服务任务凭据。"
            "#320 起 worker **停写**本字段（停写即真相唯一：服务派发意图改由 "
            "``pending``（kind=service）承载，双字段并存=双真相）；本字段降级为"
            "**只读迁移别名**——升级前已落盘的存量凭据经 "
            ":meth:`ExecutionState.effective_pending` 惰性推导进 pending 语义"
            "（推导发生在读面、不回写），补偿派发后的清除写将 pending 与本字段"
            "**同一次 CAS 双清**。非空残余（不含被清除的窗口）只应来自升级前"
            "写入的行。字段保留至存量行自然 TTL 耗尽（90 天观察窗）。"
        ),
    )
    pending: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "执行意图引用（plaita#320，write-ahead state 的「先写状态」半边）："
            "记录「为什么停下、在等什么」的**可再生决定之外**的等待凭据——"
            "``{kind: event|timer|service|start|cancel, ref_id, event_type, "
            "due_at, deadline, node_id, payload(仅 service), recorded_at}``。"
            "**引用，不复制**：订阅/timer 内容仍以 context（$NODE.<id> 的 "
            "subscription_id 等）与外部存储（订阅键、服务队列条目）为准，"
            "pending 只持重建工作所需的最小引用。写序纪律（见 "
            "``docs/WORKER_PROTOCOL.md`` 不变量两条）：pending 的任何增/改/清"
            "只发生在带 generation CAS 的状态行写内；终态迁移与 pending 清除"
            "必须同一次 CAS（该条已在 worker 落盘咽喉点机械化，终态写自动清）。"
            "``node_id`` 与 NS 帧栈对齐 = **最内层挂起节点**（嵌套挂起取 "
            "$SUSPEND_FRAMES 首帧，根流程取 $LAST_NODE）。"
            "``kind=event`` 的 ``deadline`` 必填（④窗「事件先于订阅」补强："
            "事件不保证不丢，但等待必可检、超期必可终局）；"
            "None = 非挂起/老行（老行经 :meth:`effective_pending` 惰性推导）。"
        ),
    )
    generation: int = Field(
        0,
        description=(
            "执行世代号（plaita#320）：**复用 fence 世代**（storage/fenced.py 的"
            " ContextVar 世代，acquire 租约时产生）——fenced CAS 落盘时由存储层"
            "盖章为当前世代，老行/未过 fenced 路径的写保持 0。0→N 的首次 fenced"
            " 写天然与存量行互斥（A1 评审 §四.3）。语义为**最后一次 fenced 写"
            "的世代**（回滚模式 PLAITA_DISABLE_FENCING=1 下的写不盖章、保留"
            "盘上旧值，读者按 advisory 对待）；#321 reconciler 的幂等键"
            "（``execution_id + generation``）以此为取值源。"
        ),
    )
    progress_at: Optional[str] = Field(
        None,
        description=(
            "步提交前进时刻（plaita#320，ISO 串）：**只在步提交（step_persist）"
            "时刷新**、行首次落 running 时置 now（A1 评审 §四.1/§四.2 钉死——"
            "非步迁移 queued→running〔首落〕之外一律不刷，suspended→running、"
            "retry 唤醒都不刷，单调停滞语义才成立）。与 ``last_update_time`` "
            "严格正交：后者被看门狗心跳每 TTL/3 刷新（活性信号），本字段才是"
            "「步骤在前进」的证据（前进信号）——两者构成 #316 评审实锤的"
            " mode 5（长节点卡死）双信号检测面；停滞阈值消费在 #321 "
            "reconciler（可配+默认宽）。None = 老行（读者以 ``last_update_time``"
            " 保守替代，同 #320 迁移条款）。终态行冻结在最后一步提交时刻。"
        ),
    )
    next_step: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "预分配的下一步引用（plaita#320）：``{node_id, step_id, "
            "parent_step_id}``——上一步提交时预声明的「接下来跑哪个节点」。"
            "``step_id``/``parent_step_id`` 本轮恒 None（step 分配是 #322 "
            "B1 所有权 v1 的 step token 面，键位预留、schema 先行稳定）。"
            "与「不存派生动作」不矛盾：next node 的**解析**仍由引擎在 advance"
            " 时现算（可再生的图遍历），本字段存的是**分配决定**（#322 起带"
            " token 后不可再生）；#321 reconciler 的「running 无租约 → "
            "continue」重建读它核对行声明的工作。写点：start 首落（=start 节点）、"
            "step_persist（=图遍历后继；解析失败置 None 并 WARNING——aid 字段"
            "不得阻断落盘）；挂起行置 None（等待意图由 pending 承载）；终态行"
            "同 CAS 清除（终态不得声明未完成的工作）。None = 老行/挂起/终态。"
        ),
    )

    def effective_pending(self) -> Optional[Dict[str, Any]]:
        """读面 pending 归一（plaita#320 迁移条款）：显式 pending → 老行惰性推导。

        优先级（#320 迁移：``dispatch_pending → service，订阅存在 → event，
        否则 orphan``）：

        1. 显式 ``pending``（新 worker 写入）——原样返回；
        2. 存量 ``dispatch_pending`` 凭据 → 推导 ``kind=service``（带
           ``migrated: True`` 标记；**读面推导、不回写**——回写要过同款 CAS
           且会造成迁移写风暴+与在途写竞争，A1 评审 §四.3）；
        3. ``status=suspended`` 且根上下文 $NODE.<$LAST_NODE> 带
           ``subscription_id`` → 推导 ``kind=event``（``deadline``/``migrated``
           缺省——老行推不出节点订阅超时配置，不编造；嵌套帧的订阅推导不在此
           做，conservative：查不到就 None，#321 侧按 orphan 判据走）；
        4. 否则 None（无 armed 等待 / 非挂起老行）。

        产物经 :func:`validate_pending_ref` 的 migrated 豁免面归一；消费者
        （#233 补偿派发、#321 reconciler）据此统一读法，不感知新老行差异。
        """
        if isinstance(self.pending, dict) and self.pending.get("kind"):
            return self.pending
        marker = self.dispatch_pending
        if isinstance(marker, dict) and marker.get("pending"):
            return validate_pending_ref(
                {
                    "kind": "service",
                    "ref_id": None,
                    "node_id": marker.get("node_id"),
                    "payload": {
                        "service_type": marker.get("service_type"),
                        "service_config": marker.get("service_config"),
                    },
                    "recorded_at": marker.get("recorded_at"),
                    "migrated": True,
                }
            )
        if (self.status or "") == "suspended":
            context = self.context if isinstance(self.context, dict) else {}
            node_id = context.get("$LAST_NODE")
            node_state = (context.get("$NODE") or {}).get(node_id) if node_id else None
            if isinstance(node_state, dict):
                subscription_id = node_state.get("subscription_id")
                event_type = node_state.get("event_type")
                if subscription_id or event_type:
                    return validate_pending_ref(
                        {
                            "kind": "event",
                            "ref_id": str(subscription_id) if subscription_id else None,
                            "event_type": event_type,
                            "node_id": node_id,
                            "deadline": None,
                            "recorded_at": None,
                            "migrated": True,
                        }
                    )
        return None

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

    def load_execution_tombstone(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """读取执行删除墓碑（A51-01）；无墓碑/后端不支持时返回 None。

        删除是持久语义：状态行被删掉之后，任何仍在队列/PEL 里的 ``start``
        载体都会让 ``start_flow`` 走「盘上无状态 = 全新执行」分支，把已删
        执行重建并重放旧输入。墓碑在删除时与状态删除**同原子**落盘，worker
        在启动/重投/守卫克隆前复核；键位在平台根（不随租户命名空间清理），
        值至少含 ``tenant``，调用方按消息体租户匹配，避免跨租户同 id 误伤。

        返回 ``{"tenant": str, "deleted_at": str, "reason": str, ...}``；
        读取/反序列化失败应抛 ``ExecutionStateLoadError``（瞬态不得当
        「无墓碑」放行重建）。默认实现返回 None，保持第三方/内存后零回归。
        """
        return None


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
    
    