"""
集群注册表 - 管理多个集群配置

支持功能:
- 多集群配置管理
- 集群切换
- 集群配置 CRUD
"""

import os
import yaml
import shutil
from pathlib import Path
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class ClusterInfo(BaseModel):
    """集群信息"""
    id: str = Field(..., description="集群 ID")
    name: str = Field(..., description="集群显示名称")
    description: str = Field("", description="集群描述")
    config_path: str = Field(..., description="集群配置文件路径")
    redis_url: str = Field("redis://localhost:6379/0", description="Redis URL")
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    is_active: bool = Field(False, description="是否为当前活动集群")


class ClusterRegistry:
    """集群注册表"""
    
    def __init__(self, base_dir: Optional[str] = None):
        """
        初始化集群注册表
        
        Args:
            base_dir: 集群配置存储目录，默认为 ~/.plaita-console/clusters/
        """
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            self.base_dir = Path.home() / ".plaita-console" / "clusters"
        
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.registry_file = self.base_dir / "registry.yaml"
        self._clusters: Dict[str, ClusterInfo] = {}
        self._active_cluster_id: Optional[str] = None
        
        self._load_registry()
        self._ensure_default_cluster()
    
    def _load_registry(self):
        """加载注册表"""
        if self.registry_file.exists():
            try:
                with open(self.registry_file, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f) or {}
                
                self._active_cluster_id = data.get("active_cluster")
                clusters_data = data.get("clusters", {})
                
                for cluster_id, cluster_info in clusters_data.items():
                    self._clusters[cluster_id] = ClusterInfo(
                        id=cluster_id,
                        **cluster_info
                    )
            except Exception as e:
                print(f"加载集群注册表失败: {e}")
    
    def _save_registry(self):
        """保存注册表"""
        data = {
            "active_cluster": self._active_cluster_id,
            "clusters": {
                cluster_id: {
                    "name": info.name,
                    "description": info.description,
                    "config_path": info.config_path,
                    "redis_url": info.redis_url,
                    "created_at": info.created_at,
                }
                for cluster_id, info in self._clusters.items()
            }
        }
        
        with open(self.registry_file, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False)
    
    def _ensure_default_cluster(self):
        """确保存在默认集群"""
        if not self._clusters:
            # 查找项目中的默认配置
            project_config = Path(__file__).parents[2] / "cluster_config.yaml"
            
            if project_config.exists():
                default_config_path = str(project_config)
            else:
                # 创建默认配置
                default_config_path = str(self.base_dir / "default" / "cluster_config.yaml")
                self._create_default_config(default_config_path)
            
            self.create_cluster(
                cluster_id="default",
                name="本地开发集群",
                description="默认的本地开发集群",
                config_path=default_config_path,
                redis_url="redis://localhost:6379/0"
            )
            self._active_cluster_id = "default"
            self._save_registry()
    
    def _create_default_config(self, config_path: str):
        """创建默认集群配置"""
        Path(config_path).parent.mkdir(parents=True, exist_ok=True)
        
        default_config = {
            "mode": "process",
            "redis": {
                "url": "redis://localhost:6379/0"
            },
            "services": {
                "flow_worker": {
                    "display_name": "流程执行器",
                    "process": {
                        "module": "plaita.server.flow_worker"
                    },
                    "default_instances": 1,
                    "max_instances": 10,
                    "env": {
                        "REDIS_URL": "redis://localhost:6379/0",
                        "QUEUE_NAME": "plaita:flow:queue"
                    }
                },
                "delay_service": {
                    "display_name": "延迟定时服务",
                    "process": {
                        "command": "python -m plaita.server.services delay_service"
                    },
                    "default_instances": 1,
                    "max_instances": 3,
                    "env": {
                        "REDIS_URL": "redis://localhost:6379/0"
                    }
                }
            }
        }
        
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, allow_unicode=True, default_flow_style=False)
    
    def list_clusters(self) -> List[ClusterInfo]:
        """获取所有集群"""
        clusters = []
        for cluster_id, info in self._clusters.items():
            info.is_active = (cluster_id == self._active_cluster_id)
            clusters.append(info)
        return clusters
    
    def get_cluster(self, cluster_id: str) -> Optional[ClusterInfo]:
        """获取指定集群信息"""
        info = self._clusters.get(cluster_id)
        if info:
            info.is_active = (cluster_id == self._active_cluster_id)
        return info
    
    def get_active_cluster(self) -> Optional[ClusterInfo]:
        """获取当前活动集群"""
        if self._active_cluster_id:
            return self.get_cluster(self._active_cluster_id)
        return None
    
    def create_cluster(
        self,
        cluster_id: str,
        name: str,
        description: str = "",
        config_path: Optional[str] = None,
        redis_url: str = "redis://localhost:6379/0"
    ) -> ClusterInfo:
        """创建新集群"""
        if cluster_id in self._clusters:
            raise ValueError(f"集群 {cluster_id} 已存在")
        
        # 如果没有指定配置路径，创建新的
        if not config_path:
            cluster_dir = self.base_dir / cluster_id
            cluster_dir.mkdir(parents=True, exist_ok=True)
            config_path = str(cluster_dir / "cluster_config.yaml")
            self._create_default_config(config_path)
        
        info = ClusterInfo(
            id=cluster_id,
            name=name,
            description=description,
            config_path=config_path,
            redis_url=redis_url
        )
        
        self._clusters[cluster_id] = info
        self._save_registry()
        
        return info
    
    def update_cluster(
        self,
        cluster_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        redis_url: Optional[str] = None
    ) -> ClusterInfo:
        """更新集群信息"""
        if cluster_id not in self._clusters:
            raise ValueError(f"集群 {cluster_id} 不存在")
        
        info = self._clusters[cluster_id]
        
        if name is not None:
            info.name = name
        if description is not None:
            info.description = description
        if redis_url is not None:
            info.redis_url = redis_url
        
        self._save_registry()
        return info
    
    def delete_cluster(self, cluster_id: str) -> bool:
        """删除集群"""
        if cluster_id not in self._clusters:
            return False
        
        if cluster_id == self._active_cluster_id:
            raise ValueError("不能删除当前活动集群")
        
        # 删除集群目录（如果在 base_dir 下）
        cluster_dir = self.base_dir / cluster_id
        if cluster_dir.exists():
            shutil.rmtree(cluster_dir)
        
        del self._clusters[cluster_id]
        self._save_registry()
        
        return True
    
    def switch_cluster(self, cluster_id: str) -> ClusterInfo:
        """切换到指定集群"""
        if cluster_id not in self._clusters:
            raise ValueError(f"集群 {cluster_id} 不存在")
        
        self._active_cluster_id = cluster_id
        self._save_registry()
        
        return self.get_cluster(cluster_id)
    
    def get_cluster_config(self, cluster_id: str) -> Optional[dict]:
        """获取集群的完整配置内容"""
        info = self._clusters.get(cluster_id)
        if not info:
            return None
        
        config_path = Path(info.config_path)
        if not config_path.exists():
            return None
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception:
            return None
    
    def save_cluster_config(self, cluster_id: str, config: dict) -> bool:
        """保存集群配置（写入前校验 process 白名单）。"""
        info = self._clusters.get(cluster_id)
        if not info:
            return False

        try:
            from .process_allowlist import ProcessConfigError, validate_cluster_config
        except ImportError:
            from process_allowlist import ProcessConfigError, validate_cluster_config

        try:
            validate_cluster_config(config)
        except ProcessConfigError:
            raise

        try:
            with open(info.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
            return True
        except Exception:
            return False


# 全局单例
_cluster_registry: Optional[ClusterRegistry] = None


def get_cluster_registry() -> ClusterRegistry:
    """获取集群注册表单例"""
    global _cluster_registry
    if _cluster_registry is None:
        _cluster_registry = ClusterRegistry()
    return _cluster_registry


def reset_cluster_registry():
    """重置集群注册表（用于测试）"""
    global _cluster_registry
    _cluster_registry = None

