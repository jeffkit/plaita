import time
from typing import Any, Dict, List, Optional

from .base import ExecutionStorage, ExecutionState, FlowStorage


class MemoryExecutionStorage(ExecutionStorage):
    """
    基于内存的状态存储实现，用于开发和测试环境
    """
    
    def __init__(self):
        self.execution_states = {}  # 存储执行状态
    
    def save_execution_state(self, execution_id: str, state: ExecutionState) -> bool:
        """保存流程执行状态"""
        self.execution_states[execution_id] = state.model_dump()
        return True
    
    def load_execution_state(self, execution_id: str) -> Optional[ExecutionState]:
        """加载流程执行状态"""
        if execution_id not in self.execution_states:
            return None
        return ExecutionState.model_validate(self.execution_states[execution_id])
    
    def delete_execution_state(self, execution_id: str) -> bool:
        """删除流程执行状态"""
        if execution_id in self.execution_states:
            del self.execution_states[execution_id]
            return True
        return False
    
    def list_executions(self, query: Optional[Any] = None, order_by: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[ExecutionState]:
        """列出执行状态列表"""
        execution_states = []
        
        # 获取所有执行状态
        for execution_id, state_data in self.execution_states.items():
            state = ExecutionState.model_validate(state_data)
            
            # 如果有查询条件，进行过滤 (简单实现)
            if query:
                # 这里仅做简单示例，实际可能需要更复杂的查询逻辑
                if isinstance(query, dict):
                    match = True
                    for key, value in query.items():
                        if key in state_data and state_data[key] != value:
                            match = False
                            break
                    if not match:
                        continue
            
            execution_states.append(state)
        
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


class MemoryFlowStorage(FlowStorage):
    """
    基于内存的流程定义存储实现，用于开发和测试环境
    """
    
    def __init__(self):
        self.flows = {}  # 格式: {flow_id: {version: flow_dict}}
    
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
        
        # 如果flow_id不存在，创建一个新的字典
        if flow_id not in self.flows:
            self.flows[flow_id] = {}
        
        # 保存流程定义
        self.flows[flow_id][version] = flow
        return True
    
    def get_flow(self, flow_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        获取流程定义
        
        Args:
            flow_id: 流程ID
            version: 流程版本号，如果为None则返回最新版本
            
        Returns:
            Dict or None: 流程定义数据，如果不存在则返回None
        """
        if flow_id not in self.flows:
            return None
        
        versions = self.flows[flow_id]
        if not versions:
            return None
        
        # 如果指定了版本，直接返回
        if version and version in versions:
            return versions[version]
        
        # 如果没有指定版本，返回"latest"或最后添加的版本
        if "latest" in versions:
            return versions["latest"]
        
        # 如果没有"latest"版本，尝试找出版本号最大的那个
        try:
            # 尝试按数字版本排序
            numeric_versions = sorted([v for v in versions.keys() if v.replace(".", "").isdigit()], 
                                     key=lambda x: [int(p) for p in x.split(".")])
            if numeric_versions:
                return versions[numeric_versions[-1]]
        except Exception:
            # 版本号非纯数字或不可比较——回退到下方"任意版本", 不影响功能。
            pass
        
        # 如果无法按版本号排序，返回任意一个版本
        return next(iter(versions.values())) 