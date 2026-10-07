import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Tuple

from ..logger import logger
from .base import ExecutionStorage, ExecutionState, FlowStorage

try:
    import redis
except ImportError:
    redis = None


def _require_redis():
    """Raise ImportError with actionable message if redis is not installed."""
    if redis is None:
        raise ImportError(
            "redis package is required for Redis storage. "
            "Install it with: pip install plaita[redis]"
        )


# ── 执行列表索引 ─────────────────────────────────────────────────────
# ``{ns}:execution:index`` ZSET：score=start_time（epoch 秒）、member=execution_id。
# 历史 list_executions 走 scan_iter 全命名空间扫键 + 逐键 GET + 完整
# model_validate（含整个 context），排序分页在 Python 内存做——执行量上万后
# 每次翻页/巡检都是全扫描 + 大反序列化。索引把「谁在列表里、什么顺序」交给
# Redis，分页下推 ZRANGE/ZREVRANGE，只反序列化本页。
def execution_index_key(namespace: str) -> str:
    """执行列表索引 ZSET 键：``{namespace}:execution:index``。"""
    return f"{namespace}:execution:index"


def execution_index_ready_key(namespace: str) -> str:
    """索引回填就绪标记键（存量库懒回填只做一次）。"""
    return f"{namespace}:execution:index:ready"


def execution_start_time_score(value: Any) -> float:
    """start_time（ISO 字符串）→ ZSET score（epoch 秒）。

    缺失/不可解析时落 0.0——成员仍留在索引里（升序最前 / 降序最后），
    不因缺字段从列表静默消失。
    """
    raw = value.get("start_time") if isinstance(value, dict) else getattr(value, "start_time", None)
    if not raw:
        return 0.0
    try:
        return datetime.fromisoformat(str(raw)).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _parse_order_by(order_by: Optional[str]) -> Tuple[Optional[str], bool]:
    """``order_by`` → (字段名, 是否降序)；空值 → (None, False)。"""
    if not order_by:
        return None, False
    if order_by.startswith('-'):
        return order_by[1:], True
    return order_by, False


def _matches_query(state: ExecutionState, query: Any) -> bool:
    if not isinstance(query, dict):
        return True
    for key, value in query.items():
        if hasattr(state, key) and getattr(state, key) != value:
            return False
    return True


def _sort_states(
    states: List[ExecutionState], field: str, reverse: bool
) -> List[ExecutionState]:
    try:
        return sorted(states, key=lambda s: getattr(s, field), reverse=reverse)
    except (AttributeError, TypeError):
        # 字段不存在或取值不可比较 → 保持索引顺序（与旧实现同语义）。
        return states


# C4-3：终态执行状态键 TTL。非终态（running/suspended）必须可恢复，不设；
# 终态（completed/error/cancelled）只服务于查询/审计，默认 30 天自清理。
# env ``PLAITA_EXECUTION_STATE_TTL_DAYS`` 可调（<=0 关闭 TTL）。
DEFAULT_EXECUTION_STATE_TTL_DAYS = 30
TERMINAL_EXECUTION_STATUSES = frozenset({"completed", "error", "cancelled"})


def execution_state_ttl_seconds() -> int:
    """终态执行状态键的 TTL（秒）；<=0 表示禁用 TTL。"""
    raw = (os.environ.get("PLAITA_EXECUTION_STATE_TTL_DAYS") or "").strip()
    if not raw:
        days = DEFAULT_EXECUTION_STATE_TTL_DAYS
    else:
        try:
            days = int(raw)
        except ValueError:
            logger.warning(
                "PLAITA_EXECUTION_STATE_TTL_DAYS=%r 非法，回退默认 %d 天",
                raw,
                DEFAULT_EXECUTION_STATE_TTL_DAYS,
            )
            days = DEFAULT_EXECUTION_STATE_TTL_DAYS
    if days <= 0:
        return 0
    return days * 86400


class ExecutionStateLoadError(RuntimeError):
    """执行状态**读取失败**（Redis 瞬断/超时或数据损坏无法反序列化）。

    与「键不存在 → load_execution_state 返回 None」严格区分：调用方
    （FlowWorker.run）对前者不 ack、走重投递/DLQ 路径，对后者才做
    poison ack。历史上两类情况都被吞成 None，Redis 抖动一次 = 挂起
    执行的 resume 消息被当毒丸丢弃，永久失去恢复机会（ReviewFix D1）。

    注：语义上属于存储层公共契约，理想位置是 plaita/storage/base.py；
    本次修复受改动文件白名单约束先定义在 Redis 后端模块，后续可上移。
    （刻意不继承 ValueError——FlowWorker.run 把 ValueError 当畸形消息
    poison ack，继承它会让瞬态错误仍被丢弃。）
    """


class RedisExecutionStorage(ExecutionStorage):
    """
    基于Redis的状态存储实现
    """
    
    def __init__(
        self, 
        host: str = 'localhost', 
        port: int = 6379, 
        db: int = 0, 
        password: Optional[str] = None,
        client: Optional['redis.Redis'] = None,
        namespace: str = 'plaita',
        **kwargs
    ):
        """
        初始化Redis存储
        
        Args:
            host: Redis服务器地址
            port: Redis服务器端口
            db: Redis数据库编号
            password: Redis密码
            client: 已有的Redis客户端实例
            namespace: 键命名空间前缀
            **kwargs: 其他传递给Redis客户端的参数
        """
        _require_redis()
        
        self.namespace = namespace
        
        if client:
            self.client = client
        else:
            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,  # 自动将字节解码为字符串
                **kwargs
            )
    
    def get_namespace_key(self, key_type: str, *args) -> str:
        """生成带命名空间的键"""
        if args:
            return f"{self.namespace}:{key_type}:{':'.join(args)}"
        else:
            return f"{self.namespace}:{key_type}"

    def _execution_index_key(self) -> str:
        return execution_index_key(self.namespace)

    def _index_member_id(self, key: str) -> Optional[str]:
        """状态键 ``{ns}:execution:{id}`` → execution_id；机制键/索引键 → None。

        execution_id 不含冒号；机制键（lease/fence/cancel/noderetry/…）后缀必含
        冒号，索引键（``index``）与就绪键（``index:ready``）为保留名——据此分开。

        索引键本身也在 ``{ns}:execution:*`` 扫描范围内，一旦被当成成员就会
        对 ZSET 键发 ``GET``（WRONGTYPE）——必须显式排除，否则回填整批作废。
        """
        prefix = f"{self.namespace}:execution:"
        if not key.startswith(prefix):
            return None
        suffix = key[len(prefix):]
        if not suffix or ":" in suffix or suffix == "index":
            return None
        return suffix

    def _ensure_execution_index(self) -> None:
        """存量库懒回填：就绪标记缺失时把已有状态键补进索引一次。

        升级前写入的执行没有索引成员——不回填它们会从列表静默消失。
        回填是一次性 O(N)（与旧 list 路径同价），完成后置就绪标记，
        此后列表全程走索引。单个键读不到或反序列化失败只跳过该键（本次
        退化为「部分索引」）；扫描/管道整体失败则整批丢弃、就绪标记不置位，
        下次 list 重试回填。
        """
        ready_key = execution_index_ready_key(self.namespace)
        try:
            if self.client.exists(ready_key):
                return
            index_key = self._execution_index_key()
            pattern = self.get_namespace_key('execution', '*')
            pipe = self.client.pipeline()
            for key in self.client.scan_iter(match=pattern):
                key_str = key if isinstance(key, str) else key.decode()
                execution_id = self._index_member_id(key_str)
                if execution_id is None:
                    continue
                try:
                    data = self.client.get(key_str)
                except Exception as e:
                    # 单键不可读（类型不符/瞬断）不得作废整批回填。
                    logger.error("索引回填跳过不可读的状态键 %s: %s", key_str, e)
                    continue
                if not data:
                    continue
                try:
                    state_dict = self.deserialize_state(data)
                except Exception as e:
                    logger.error(
                        "索引回填跳过反序列化失败的状态 %s: %s", key_str, e
                    )
                    continue
                pipe.zadd(index_key, {execution_id: execution_start_time_score(state_dict)})
            pipe.set(ready_key, "1")
            pipe.execute()
        except Exception as e:
            logger.error("索引回填失败（本次列表退化为零更新，待下次重试）: %s", e)

    def _index_slice(self, reverse: bool, offset: int, limit: Optional[int]) -> List[str]:
        """按 start_time 从索引取一段 execution_id（offset/limit 下推到 ZSET）。"""
        start = max(0, offset)
        if limit is None:
            stop: int = -1
        else:
            if limit <= 0:
                return []
            stop = start + limit - 1
        fetch = self.client.zrevrange if reverse else self.client.zrange
        members = fetch(self._execution_index_key(), start, stop)
        return [m if isinstance(m, str) else m.decode() for m in members]

    def _load_indexed_states(self, execution_ids: List[str]) -> List[ExecutionState]:
        """批量反序列化索引成员对应的状态；悬空成员顺带 ZREM 剪枝。

        悬空 = 状态键已被 TTL/外部删除——剪掉它（分页不再出现空格）。
        数据**读得到但反序列化失败**（跨版本 schema 变更等）只跳过、不剪：
        那行仍在库里，剪掉索引成员等于让它从列表永久消失，降级回旧版本也
        读不到；下次 list 会再跳过，代价只是本页少一条。
        """
        if not execution_ids:
            return []
        keys = [self.get_namespace_key('execution', eid) for eid in execution_ids]
        raws = self.client.mget(keys)
        states: List[ExecutionState] = []
        stale: List[str] = []
        for execution_id, data in zip(execution_ids, raws):
            if data:
                try:
                    states.append(ExecutionState.model_validate(self.deserialize_state(data)))
                    continue
                except Exception as e:
                    logger.error("Failed to process execution state: %s", e)
                    continue
            stale.append(execution_id)
        if stale:
            try:
                self.client.zrem(self._execution_index_key(), *stale)
            except Exception as e:
                logger.error("Failed to prune stale execution index members: %s", e)
        return states

    def save_execution_state(self, execution_id: str, state: ExecutionState) -> bool:
        """保存流程执行状态（同步维护 ``{ns}:execution:index`` 索引成员）。

        C4-3：仅终态（completed/error/cancelled）写入带 TTL（默认 30 天，
        env ``PLAITA_EXECUTION_STATE_TTL_DAYS`` 可调，<=0 关闭）；非终态
        （running/suspended）是可恢复执行的活动状态，必须不过期。

        已核实（C4-3 报告注）：``FencedExecutionStorage`` 持 fence 世代时走
        单段 Lua 直接 SET 状态键。历史上该 Lua 不带 EX，经 worker resume
        路径落盘的终态键拿不到 TTL（靠 console 读时补偿）；2026-10-02 起
        Lua 已按状态带终态 TTL（fenced.py 复用本模块的
        ``execution_state_ttl_seconds``），读时补偿退化为无害的 EXPIRE 刷新。
        同理，fenced 路径的索引成员（ZADD）也在该 Lua 内维护（fenced.py）。
        """
        key = self.get_namespace_key('execution', execution_id)
        try:
            serialized = self.serialize_state(state.model_dump())
            ttl = (
                execution_state_ttl_seconds()
                if state.status in TERMINAL_EXECUTION_STATUSES
                else 0
            )
            pipe = self.client.pipeline()
            if ttl > 0:
                pipe.set(key, serialized, ex=ttl)
            else:
                pipe.set(key, serialized)
            pipe.zadd(
                self._execution_index_key(),
                {execution_id: execution_start_time_score(state)},
            )
            pipe.execute()
            return True
        except Exception as e:
            logger.error("Failed to save execution state %s: %s", execution_id, e)
            return False
    
    def load_execution_state(self, execution_id: str) -> Optional[ExecutionState]:
        """加载流程执行状态。

        键不存在 → 返回 None；读取/反序列化失败 → 抛 ``ExecutionStateLoadError``
        （ReviewFix D1：瞬态错误不得吞成 None——那会让 worker 把 resume 消息
        当毒丸 ack，挂起执行永久失去恢复机会）。
        """
        key = self.get_namespace_key('execution', execution_id)
        try:
            data = self.client.get(key)
        except Exception as e:
            raise ExecutionStateLoadError(
                f"读取执行状态失败（可能是 Redis 瞬断）: {execution_id}: {e}"
            ) from e
        if not data:
            return None
        try:
            state_dict = self.deserialize_state(data)
            return ExecutionState.model_validate(state_dict)
        except Exception as e:
            raise ExecutionStateLoadError(
                f"执行状态反序列化失败（数据损坏）: {execution_id}: {e}"
            ) from e
    
    def delete_execution_state(self, execution_id: str) -> bool:
        """删除流程执行状态（同时移除列表索引成员）"""
        execution_key = self.get_namespace_key('execution', execution_id)
        try:
            pipe = self.client.pipeline()
            pipe.delete(execution_key)
            pipe.zrem(self._execution_index_key(), execution_id)
            pipe.execute()
            return True
        except Exception as e:
            logger.error("Failed to delete execution state: %s", e)
            return False

    def list_executions(self, query: Optional[Any] = None, order_by: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[ExecutionState]:
        """列出执行状态列表（走 ``{ns}:execution:index`` ZSET，不再全命名空间扫键）。

        索引 score=start_time（epoch 秒）、member=execution_id：

        - 无过滤且按 start_time 排序（含默认）：分页直接下推 ZRANGE/ZREVRANGE，
          只 GET 本页对应的状态键——列表开销与执行总数解耦；
        - 带过滤或按其他字段排序：按索引顺序取成员再在内存过滤/排序/分页
          （仍免去 SCAN 全键 + 逐键反序列化的旧路径）。

        存量库的成员由 ``_ensure_execution_index`` 懒回填；索引成员对应的
        状态键已过期时（终态 TTL 自清理）读时 ZREM 剪枝，并继续向后取页直到
        本页填满——头部悬空积压超过多页时也不会返回空列表/短页。
        """
        try:
            self._ensure_execution_index()
            field, reverse = _parse_order_by(order_by)

            if not query and field in (None, 'start_time'):
                # 分页可下推：只反序列化本页。
                ids = self._index_slice(reverse, offset, limit)
                while True:
                    states = self._load_indexed_states(ids)
                    if len(states) == len(ids):
                        # 本页填满（或索引到此为止）。
                        return states
                    # 本页含悬空成员（终态 TTL 过期、外部删键）已被剪枝——窗口
                    # 前移后重取补齐。每次重取会跳过 ≤ limit 个新成员，而头部
                    # 积压可达多页（剪枝只发生在本页，默认/offset=0 的读又总是
                    # 从头部开始），所以**一次**重取不够：循环到本页填满为止。
                    # 每轮要么剪掉 ≥1 个成员（索引严格变小）要么返回，必然终止
                    # ——不必设轮数上限（上限反而会重新引入静默短页）。
                    refetched = self._index_slice(reverse, offset, limit)
                    if refetched == ids:
                        # 本页一个成员都没被剪 → 已收敛：剩下的缺员是「读得到但
                        # 反序列化失败」的坏行（刻意不剪），返回短页。
                        return states
                    ids = refetched

            states = self._load_indexed_states(self._index_slice(reverse, 0, None))
            if query:
                states = [s for s in states if _matches_query(s, query)]
            if field not in (None, 'start_time'):
                states = _sort_states(states, field, reverse)

            # 应用分页
            return states[offset:offset + limit]

        except Exception as e:
            logger.error("Failed to list executions: %s", e)
            return []


class RedisFlowStorage(FlowStorage):
    """
    基于Redis的流程定义存储实现
    """
    
    def __init__(
        self, 
        host: str = 'localhost', 
        port: int = 6379, 
        db: int = 0, 
        password: Optional[str] = None,
        client: Optional['redis.Redis'] = None,
        namespace: str = 'plaita',
        **kwargs
    ):
        """
        初始化Redis存储
        
        Args:
            host: Redis服务器地址
            port: Redis服务器端口
            db: Redis数据库编号
            password: Redis密码
            client: 已有的Redis客户端实例
            namespace: 键命名空间前缀
            **kwargs: 其他传递给Redis客户端的参数
        """
        _require_redis()
        
        self.namespace = namespace
        
        if client:
            self.client = client
        else:
            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,  # 自动将字节解码为字符串
                **kwargs
            )
    
    def get_namespace_key(self, key_type: str, *args) -> str:
        """生成带命名空间的键"""
        if args:
            return f"{self.namespace}:{key_type}:{':'.join(args)}"
        else:
            return f"{self.namespace}:{key_type}"
    
    def save_flow(self, flow: Dict[str, Any]) -> bool:
        """
        保存流程定义
        
        Args:
            flow: 流程定义数据，必须包含flow_id字段，可选包含version字段
            
        Returns:
            bool: 是否保存成功
        """
        flow_id = flow.get("flow_id") or flow.get("id")
        version = flow.get("version", "latest")
        
        if not flow_id:
            return False
        
        try:
            # 保存流程定义
            key = self.get_namespace_key('flow', flow_id, version)
            self.client.set(key, json.dumps(flow))
            
            # 维护流程ID列表
            flow_list_key = self.get_namespace_key('flow_list')
            self.client.sadd(flow_list_key, flow_id)
            
            # 维护每个流程的版本列表
            flow_versions_key = self.get_namespace_key('flow_versions', flow_id)
            self.client.sadd(flow_versions_key, version)
            
            return True
        except Exception as e:
            logger.error("Failed to save flow definition: %s", e)
            return False
    
    def get_flow(self, flow_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        获取流程定义
        
        Args:
            flow_id: 流程ID
            version: 流程版本号，如果为None则返回最新版本
            
        Returns:
            Dict or None: 流程定义数据，如果不存在则返回None
        """
        try:
            # 检查流程ID是否存在
            flow_list_key = self.get_namespace_key('flow_list')
            if not self.client.sismember(flow_list_key, flow_id):
                return None
            
            # 获取版本信息
            flow_versions_key = self.get_namespace_key('flow_versions', flow_id)
            all_versions = {
                v.decode("utf-8") if isinstance(v, bytes) else v
                for v in (self.client.smembers(flow_versions_key) or set())
            }
            
            if not all_versions:
                return None
            
            # 确定要获取的版本
            target_version = version
            if not target_version:
                # 如果存在latest版本，优先使用
                if "latest" in all_versions:
                    target_version = "latest"
                else:
                    # 尝试按版本号排序
                    try:
                        numeric_versions = sorted(
                            [v for v in all_versions if v.replace(".", "").isdigit()],
                            key=lambda x: [int(p) for p in x.split(".")]
                        )
                        if numeric_versions:
                            target_version = numeric_versions[-1]
                        else:
                            # 如果没有数字版本，使用任意版本
                            target_version = next(iter(all_versions))
                    except Exception:
                        target_version = next(iter(all_versions))
            elif target_version not in all_versions:
                # 请求的版本不存在
                return None
                
            # 获取流程定义
            key = self.get_namespace_key('flow', flow_id, target_version)
            data = self.client.get(key)
            
            if not data:
                return None
                
            return json.loads(data)
        except Exception as e:
            logger.error("Failed to get flow definition: %s", e)
            return None
    
    def list_flows(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        列出所有流程定义的ID
        
        Args:
            limit: 限制返回的数量
            offset: 偏移量
            
        Returns:
            List[Dict]: 流程ID列表
        """
        try:
            flow_list_key = self.get_namespace_key('flow_list')
            all_flow_ids = self.client.smembers(flow_list_key)
            
            # 应用分页
            paginated_ids = list(all_flow_ids)[offset:offset+limit]
            
            result = []
            for flow_id in paginated_ids:
                # 获取每个流程的最新版本
                flow = self.get_flow(flow_id)
                if flow:
                    result.append(flow)
            
            return result
        except Exception as e:
            logger.error("Failed to list flows: %s", e)
            return []
    
    def get_flow_versions(self, flow_id: str) -> List[str]:
        """
        获取流程的所有版本
        
        Args:
            flow_id: 流程ID
            
        Returns:
            List[str]: 版本列表
        """
        try:
            flow_versions_key = self.get_namespace_key('flow_versions', flow_id)
            versions = self.client.smembers(flow_versions_key)
            return list(versions)
        except Exception as e:
            logger.error("Failed to get flow versions: %s", e)
            return []
    
    def delete_flow(self, flow_id: str, version: Optional[str] = None) -> bool:
        """
        删除流程定义
        
        Args:
            flow_id: 流程ID
            version: 流程版本号，如果为None则删除所有版本
            
        Returns:
            bool: 是否删除成功
        """
        try:
            # 检查流程ID是否存在
            flow_list_key = self.get_namespace_key('flow_list')
            if not self.client.sismember(flow_list_key, flow_id):
                return False
            
            flow_versions_key = self.get_namespace_key('flow_versions', flow_id)
            
            if version:
                # 删除指定版本
                if not self.client.sismember(flow_versions_key, version):
                    return False
                
                key = self.get_namespace_key('flow', flow_id, version)
                self.client.delete(key)
                self.client.srem(flow_versions_key, version)
                
                # 如果删除后没有版本了，也删除流程ID
                if self.client.scard(flow_versions_key) == 0:
                    self.client.delete(flow_versions_key)
                    self.client.srem(flow_list_key, flow_id)
            else:
                # 删除所有版本
                all_versions = {
                v.decode("utf-8") if isinstance(v, bytes) else v
                for v in (self.client.smembers(flow_versions_key) or set())
            }
                pipe = self.client.pipeline()
                for ver in all_versions:
                    key = self.get_namespace_key('flow', flow_id, ver)
                    pipe.delete(key)
                
                pipe.delete(flow_versions_key)
                pipe.srem(flow_list_key, flow_id)
                pipe.execute()
            
            return True
        except Exception as e:
            logger.error("Failed to delete flow: %s", e)

    def diagnose_missing_flow(self, flow_id: str) -> None:
        """Log Redis-specific diagnostics when a flow definition cannot be found.

        This method is called by FlowWorker via an optional duck-type protocol;
        keeping the Redis introspection here ensures FlowWorker stays agnostic
        of the underlying storage implementation.
        """
        try:
            key_pattern = self.get_namespace_key("flow", flow_id, "*")
            # C4-3：诊断路径同样避免 O(N) 阻塞的 keys()，改 scan_iter。
            matching_keys = list(self.client.scan_iter(match=key_pattern))
            flow_list_key = self.get_namespace_key("flow_list")
            flow_ids = self.client.smembers(flow_list_key)
            logger.error("Redis diagnostics — flow_id: %s", flow_id)
            logger.error("Redis diagnostics — matching keys: %s", matching_keys)
            logger.error("Redis diagnostics — known flow IDs: %s", flow_ids)
            logger.error("Redis diagnostics — namespace: %s", self.namespace)
        except Exception as e:
            logger.error("Redis diagnostics failed: %s", e)
            return False 