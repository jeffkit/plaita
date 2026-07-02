import json
import time
from typing import Any, Dict, List, Optional, Union
import uuid

from sqlalchemy import Column, String, Text, Integer, DateTime, JSON, Index, ForeignKey, select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import text
from datetime import datetime

from ..logger import logger
from .base import ExecutionStorage, ExecutionState, FlowStorage

Base = declarative_base()

class FlowDefinition(Base):
    """流程定义表"""
    __tablename__ = 'flow_definitions'
    
    id = Column(String(50), primary_key=True)
    flow_id = Column(String(100), nullable=False, index=True)
    version = Column(String(50), nullable=False)
    definition = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # 联合唯一索引
    __table_args__ = (
        Index('ix_flow_id_version', 'flow_id', 'version', unique=True),
    )

class ExecutionStateModel(Base):
    """执行状态表"""
    __tablename__ = 'execution_states'
    
    execution_id = Column(String(50), primary_key=True)
    flow_id = Column(String(100), nullable=True, index=True)
    flow_name = Column(String(100), nullable=True)
    flow_version = Column(String(50), nullable=True)
    context = Column(JSON, nullable=False)
    status = Column(String(50), nullable=False, index=True)
    start_time = Column(String(50), nullable=True)
    last_update_time = Column(String(50), nullable=True)
    end_time = Column(String(50), nullable=True)
    error = Column(JSON, nullable=True)
    invoker = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class SqlalchemyExecutionStorage(ExecutionStorage):
    """
    基于SQLAlchemy的执行状态存储实现
    """
    
    def __init__(self, database_url=None, engine=None, create_tables=True):
        """
        初始化SQLAlchemy存储
        
        Args:
            database_url: 数据库连接URL
            engine: 已有的SQLAlchemy引擎实例
            create_tables: 是否自动创建表结构
        """
        if engine:
            self.engine = engine
        elif database_url:
            self.engine = create_async_engine(database_url)
        else:
            raise ValueError("必须提供database_url或engine参数")
        
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        
        if create_tables:
            self._create_tables()
    
    def _create_tables(self):
        """创建表结构"""
        import asyncio
        
        async def create_all():
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        
        try:
            asyncio.run(create_all())
            logger.info("SQLAlchemy存储: 表结构创建成功")
        except Exception as e:
            logger.error("SQLAlchemy存储: 创建表结构失败: %s", e)
            raise
    
    async def save_execution_state(self, execution_id: str, state: ExecutionState) -> bool:
        """保存流程执行状态"""
        try:
            async with self.async_session() as session:
                # 检查是否存在
                query = select(ExecutionStateModel).where(
                    ExecutionStateModel.execution_id == execution_id
                )
                result = await session.execute(query)
                existing = result.scalar_one_or_none()
                
                # 准备数据
                state_dict = state.model_dump() if hasattr(state, 'model_dump') else state
                
                if existing:
                    # 更新现有记录
                    for key, value in state_dict.items():
                        setattr(existing, key, value)
                    existing.updated_at = datetime.now()
                else:
                    # 创建新记录
                    model = ExecutionStateModel(
                        execution_id=execution_id,
                        **state_dict
                    )
                    session.add(model)
                
                await session.commit()
                return True
                
        except Exception as e:
            logger.error("保存执行状态失败: %s", e)
            return False
    
    async def load_execution_state(self, execution_id: str) -> Optional[ExecutionState]:
        """加载流程执行状态"""
        try:
            async with self.async_session() as session:
                query = select(ExecutionStateModel).where(
                    ExecutionStateModel.execution_id == execution_id
                )
                result = await session.execute(query)
                model = result.scalar_one_or_none()
                
                if not model:
                    return None
                
                # 构建ExecutionState对象
                state_dict = {
                    'execution_id': model.execution_id,
                    'flow_id': model.flow_id,
                    'flow_name': model.flow_name,
                    'flow_version': model.flow_version,
                    'context': model.context,
                    'status': model.status,
                    'start_time': model.start_time,
                    'last_update_time': model.last_update_time,
                    'end_time': model.end_time,
                    'error': model.error,
                    'invoker': model.invoker
                }
                
                return ExecutionState(**state_dict)
                
        except Exception as e:
            logger.error("加载执行状态失败: %s", e)
            return None
    
    async def delete_execution_state(self, execution_id: str) -> bool:
        """删除流程执行状态"""
        try:
            async with self.async_session() as session:
                query = select(ExecutionStateModel).where(
                    ExecutionStateModel.execution_id == execution_id
                )
                result = await session.execute(query)
                model = result.scalar_one_or_none()
                
                if not model:
                    return False
                
                await session.delete(model)
                await session.commit()
                return True
                
        except Exception as e:
            logger.error("删除执行状态失败: %s", e)
            return False
    
    async def list_executions(self, query: Optional[Any] = None, order_by: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[ExecutionState]:
        """列出执行状态"""
        try:
            async with self.async_session() as session:
                # 构建查询
                stmt = select(ExecutionStateModel)
                
                # 应用过滤条件
                if query and isinstance(query, dict):
                    for key, value in query.items():
                        if hasattr(ExecutionStateModel, key):
                            stmt = stmt.where(getattr(ExecutionStateModel, key) == value)
                
                # 应用排序
                if order_by:
                    if order_by.startswith('-'):
                        # 降序
                        column_name = order_by[1:]
                        if hasattr(ExecutionStateModel, column_name):
                            stmt = stmt.order_by(getattr(ExecutionStateModel, column_name).desc())
                    else:
                        # 升序
                        if hasattr(ExecutionStateModel, order_by):
                            stmt = stmt.order_by(getattr(ExecutionStateModel, order_by))
                else:
                    # 默认按更新时间降序
                    stmt = stmt.order_by(ExecutionStateModel.updated_at.desc())
                
                # 应用分页
                stmt = stmt.limit(limit).offset(offset)
                
                # 执行查询
                result = await session.execute(stmt)
                models = result.scalars().all()
                
                # 转换为ExecutionState对象
                states = []
                for model in models:
                    state_dict = {
                        'execution_id': model.execution_id,
                        'flow_id': model.flow_id,
                        'flow_name': model.flow_name,
                        'flow_version': model.flow_version,
                        'context': model.context,
                        'status': model.status,
                        'start_time': model.start_time,
                        'last_update_time': model.last_update_time,
                        'end_time': model.end_time,
                        'error': model.error,
                        'invoker': model.invoker
                    }
                    states.append(ExecutionState(**state_dict))
                
                return states
                
        except Exception as e:
            logger.error("列出执行状态失败: %s", e)
            return []


class SqlalchemyFlowStorage(FlowStorage):
    """
    基于SQLAlchemy的流程定义存储实现
    """
    
    def __init__(self, database_url=None, engine=None, create_tables=True):
        """
        初始化SQLAlchemy存储
        
        Args:
            database_url: 数据库连接URL
            engine: 已有的SQLAlchemy引擎实例
            create_tables: 是否自动创建表结构
        """
        if engine:
            self.engine = engine
        elif database_url:
            self.engine = create_async_engine(database_url)
        else:
            raise ValueError("必须提供database_url或engine参数")
        
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        
        if create_tables:
            self._create_tables()
    
    def _create_tables(self):
        """创建表结构"""
        import asyncio
        
        async def create_all():
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        
        try:
            asyncio.run(create_all())
            logger.info("SQLAlchemy存储: 表结构创建成功")
        except Exception as e:
            logger.error("SQLAlchemy存储: 创建表结构失败: %s", e)
            raise
    
    async def save_flow(self, flow: Dict[str, Any]) -> bool:
        """保存流程定义"""
        flow_id = flow.get("flow_id") or flow.get("id")
        version = flow.get("version", "latest")
        
        if not flow_id:
            return False
            
        try:
            async with self.async_session() as session:
                # 检查是否存在
                query = select(FlowDefinition).where(
                    FlowDefinition.flow_id == flow_id,
                    FlowDefinition.version == version
                )
                result = await session.execute(query)
                existing = result.scalar_one_or_none()
                
                if existing:
                    # 更新现有记录
                    existing.definition = flow
                    existing.updated_at = datetime.now()
                else:
                    # 创建新记录
                    model = FlowDefinition(
                        id=str(uuid.uuid4()),
                        flow_id=flow_id,
                        version=version,
                        definition=flow
                    )
                    session.add(model)
                
                await session.commit()
                return True
                
        except Exception as e:
            logger.error("保存流程定义失败: %s", e)
            return False
    
    async def get_flow(self, flow_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """获取流程定义"""
        try:
            async with self.async_session() as session:
                if version:
                    # 获取指定版本
                    query = select(FlowDefinition).where(
                        FlowDefinition.flow_id == flow_id,
                        FlowDefinition.version == version
                    )
                else:
                    # 获取所有版本，优先返回latest
                    query = select(FlowDefinition).where(
                        FlowDefinition.flow_id == flow_id
                    )
                
                result = await session.execute(query)
                models = result.scalars().all()
                
                if not models:
                    return None
                
                # 如果没有指定版本，按优先级选择
                if not version:
                    # 优先使用latest版本
                    for model in models:
                        if model.version == "latest":
                            return model.definition
                    
                    # 尝试按版本号排序选择最新的
                    try:
                        numeric_versions = sorted(
                            [m for m in models if m.version.replace(".", "").isdigit()],
                            key=lambda x: [int(p) for p in x.version.split(".")]
                        )
                        if numeric_versions:
                            return numeric_versions[-1].definition
                    except Exception:
                        pass
                    
                    # 如果都不满足，返回任意一个
                    return models[0].definition
                else:
                    # 指定了版本，直接返回
                    for model in models:
                        if model.version == version:
                            return model.definition
                    
                    return None
                
        except Exception as e:
            logger.error("获取流程定义失败: %s", e)
            return None
    
    async def list_flows(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """列出所有流程"""
        try:
            async with self.async_session() as session:
                # 获取唯一的flow_id列表
                query = select(FlowDefinition.flow_id).distinct().limit(limit).offset(offset)
                result = await session.execute(query)
                flow_ids = result.scalars().all()
                
                # 对每个flow_id获取最新版本
                flows = []
                for flow_id in flow_ids:
                    flow = await self.get_flow(flow_id)
                    if flow:
                        flows.append(flow)
                
                return flows
                
        except Exception as e:
            logger.error("列出流程定义失败: %s", e)
            return []
    
    async def get_flow_versions(self, flow_id: str) -> List[str]:
        """获取流程的所有版本"""
        try:
            async with self.async_session() as session:
                query = select(FlowDefinition.version).where(
                    FlowDefinition.flow_id == flow_id
                )
                result = await session.execute(query)
                versions = result.scalars().all()
                return list(versions)
                
        except Exception as e:
            logger.error("获取流程版本失败: %s", e)
            return []
    
    async def delete_flow(self, flow_id: str, version: Optional[str] = None) -> bool:
        """删除流程定义"""
        try:
            async with self.async_session() as session:
                if version:
                    # 删除指定版本
                    stmt = select(FlowDefinition).where(
                        FlowDefinition.flow_id == flow_id,
                        FlowDefinition.version == version
                    )
                    result = await session.execute(stmt)
                    model = result.scalar_one_or_none()
                    
                    if not model:
                        return False
                    
                    await session.delete(model)
                else:
                    # 删除所有版本
                    stmt = select(FlowDefinition).where(
                        FlowDefinition.flow_id == flow_id
                    )
                    result = await session.execute(stmt)
                    models = result.scalars().all()
                    
                    if not models:
                        return False
                    
                    for model in models:
                        await session.delete(model)
                
                await session.commit()
                return True
                
        except Exception as e:
            logger.error("删除流程定义失败: %s", e)
            return False 