import json
import time
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
    
    def save_execution_state(self, execution_id: str, state: ExecutionState) -> bool:
        """保存流程执行状态"""
        key = self.get_namespace_key('execution', execution_id)
        try:
            serialized = self.serialize_state(state.model_dump())
            self.client.set(key, serialized)
            return True
        except Exception as e:
            logger.error("Failed to save execution state: %s", e)
            return False
    
    def load_execution_state(self, execution_id: str) -> Optional[ExecutionState]:
        """加载流程执行状态"""
        key = self.get_namespace_key('execution', execution_id)
        try:
            data = self.client.get(key)
            if not data:
                return None
            state_dict = self.deserialize_state(data)
            return ExecutionState.model_validate(state_dict)
        except Exception as e:
            logger.error("Failed to load execution state: %s", e)
            return None
    
    def delete_execution_state(self, execution_id: str) -> bool:
        """删除流程执行状态"""
        execution_key = self.get_namespace_key('execution', execution_id)
        try:
            self.client.delete(execution_key)
            return True
        except Exception as e:
            logger.error("Failed to delete execution state: %s", e)
            return False
    
    def list_executions(self, query: Optional[Any] = None, order_by: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[ExecutionState]:
        """列出执行状态列表"""
        try:
            # 构建查询模式
            pattern = self.get_namespace_key('execution', '*')
            
            # 获取所有匹配的键
            all_keys = self.client.keys(pattern)
            
            # 获取执行状态列表
            execution_states = []
            for key in all_keys:
                try:
                    data = self.client.get(key)
                    if not data:
                        continue
                    
                    state_dict = self.deserialize_state(data)
                    state = ExecutionState.model_validate(state_dict)
                    
                    # 如果有查询条件，进行过滤 (简单实现)
                    if query:
                        # 这里仅做简单示例，实际可能需要更复杂的查询逻辑
                        if isinstance(query, dict):
                            match = True
                            for query_key, query_value in query.items():
                                if hasattr(state, query_key) and getattr(state, query_key) != query_value:
                                    match = False
                                    break
                            if not match:
                                continue
                    
                    execution_states.append(state)
                except Exception as e:
                    logger.error("Failed to process execution state: %s", e)
                    continue
            
            # 排序
            if order_by:
                reverse = False
                if order_by.startswith('-'):
                    reverse = True
                    order_by = order_by[1:]
                
                # 尝试按指定字段排序
                try:
                    execution_states.sort(key=lambda x: getattr(x, order_by), reverse=reverse)
                except (AttributeError, TypeError):
                    # 如果排序失败，保持原始顺序
                    pass
            
            # 应用分页
            return execution_states[offset:offset + limit]
            
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
            all_versions = self.client.smembers(flow_versions_key)
            
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
                all_versions = self.client.smembers(flow_versions_key)
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
            matching_keys = self.client.keys(key_pattern)
            flow_list_key = self.get_namespace_key("flow_list")
            flow_ids = self.client.smembers(flow_list_key)
            logger.error("Redis diagnostics — flow_id: %s", flow_id)
            logger.error("Redis diagnostics — matching keys: %s", matching_keys)
            logger.error("Redis diagnostics — known flow IDs: %s", flow_ids)
            logger.error("Redis diagnostics — namespace: %s", self.namespace)
        except Exception as e:
            logger.error("Redis diagnostics failed: %s", e)
            return False 