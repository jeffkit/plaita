"""
集群注册表 - 管理多个集群配置

支持功能:
- 多集群配置管理
- 集群切换
- 集群配置 CRUD
"""

import logging
import os
import stat
import tempfile
import yaml
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

logger = logging.getLogger(__name__)


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
        self._load_failed = False
        
        self._load_registry()
        self._ensure_default_cluster()
    
    def _load_registry(self):
        """加载注册表。

        解析失败时把损坏文件隔离为 ``registry.yaml.corrupt.<ts>`` 并记具名
        WARNING，同时置 ``_load_failed`` 阻止本次启动重建 default——旧实现只
        print，随后 ``_ensure_default_cluster`` 见空即覆盖原文件，一次
        ENOSPC 留下的半截 YAML 会抹掉全部集群登记（plaita#249）。
        """
        if not self.registry_file.exists():
            return
        try:
            with open(self.registry_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                raise ValueError(f"注册表顶层必须是映射，实际为 {type(data).__name__}")
            clusters_data = data.get("clusters", {})
            if not isinstance(clusters_data, dict):
                raise ValueError(f"clusters 必须是映射，实际为 {type(clusters_data).__name__}")

            self._active_cluster_id = data.get("active_cluster")
            for cluster_id, cluster_info in clusters_data.items():
                self._clusters[cluster_id] = ClusterInfo(
                    id=cluster_id,
                    **cluster_info
                )
        except Exception as e:
            # 逐条反序列化可能已填了半张表；载入失败即整表作废，避免把
            # 部分登记当成真实状态对外服务。
            self._clusters.clear()
            self._active_cluster_id = None
            self._load_failed = True
            backup = self._quarantine_registry_file()
            logger.warning(
                "集群注册表载入失败（%s）：%s；损坏文件备份为 %s，"
                "本次启动保留空表、不重建默认集群",
                self.registry_file, e, backup,
            )

    def _quarantine_registry_file(self) -> Optional[Path]:
        """把损坏的注册表改名为 ``registry.yaml.corrupt.<ts>`` 保留证据。"""
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
        backup = self.registry_file.with_name(f"{self.registry_file.name}.corrupt.{stamp}")
        try:
            os.replace(self.registry_file, backup)
        except OSError:
            logger.warning(
                "集群注册表损坏文件备份失败（原文件保留）: %s",
                self.registry_file, exc_info=True,
            )
            return None
        return backup

    def _save_registry(self):
        """原子保存注册表：同目录临时文件写满 + fsync 后 ``os.replace``。

        就地截断写下写到一半失败（磁盘满/进程中断）会留下半截 YAML，重启时
        只能按损坏文件隔离，全部集群登记丢失（plaita#249）。与 #223 凭据导出
        同一模板：失败路径只影响临时文件，旧文件逐字节不变。
        """
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
        # 先序列化再碰文件：yaml.dump 自身失败时连临时文件都不会创建。
        text = yaml.dump(data, allow_unicode=True, default_flow_style=False)

        fd, tmp_name = tempfile.mkstemp(
            dir=str(self.base_dir), prefix=f".{self.registry_file.name}.", suffix=".tmp"
        )
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
            # 沿用旧文件权限；新建时保留 mkstemp 的 0600（registry 含 redis_url）。
            if self.registry_file.exists():
                os.chmod(tmp, stat.S_IMODE(self.registry_file.stat().st_mode))
            os.replace(tmp, self.registry_file)
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _ensure_default_cluster(self):
        """确保存在默认集群。

        载入失败时不重建：损坏文件已隔离备份，原 ``registry.yaml`` 不得被
        default 重新覆盖（plaita#249）。
        """
        if self._load_failed:
            return
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
    
    # ---- 架构配套预设（快速上手 / 开发 / 生产），见 docs/architecture-profiles.md ----

    _SERVICE_TEMPLATES = {
        "flow_worker": {
            "display_name": "流程执行器",
            "process": {"module": "plaita.server.flow_worker"},
            "max_instances": 10,
            "env": {"PLAITA_QUEUE_NAME": "plaita:flow:queue"},
        },
        "delay_service": {
            "display_name": "延迟服务",
            "process": {"command": "python -m plaita.server.services delay_service"},
            "max_instances": 3,
        },
        "event_filter": {
            "display_name": "事件恢复器",
            "process": {"module": "plaita.server.event_filter"},
            "max_instances": 1,
        },
        "schedule_service": {
            "display_name": "调度服务",
            "process": {"command": "python -m plaita.server.services schedule_service"},
            "max_instances": 1,
        },
        "http_callback_service": {
            "display_name": "HTTP 回调服务",
            "process": {"command": "python -m plaita.server.services http_callback_service"},
            "max_instances": 3,
        },
        "redis_queue_service": {
            "display_name": "Redis 队列服务",
            "process": {"command": "python -m plaita.server.services redis_queue_service"},
            "max_instances": 5,
        },
        "approval_service": {
            "display_name": "审批服务",
            "process": {"command": "python -m plaita.server.services approval_service"},
            "max_instances": 3,
        },
        "kafka_queue_service": {
            "display_name": "Kafka 队列服务",
            "process": {"command": "python -m plaita.server.services kafka_queue_service"},
            "max_instances": 3,
        },
    }

    # 各档预置的服务集合：quickstart 最小可跑（同步流程+挂起恢复），
    # dev 全功能（+触发器/回调/队列），prod 全家桶 + 实例上限上调
    _PRESET_SERVICES = {
        "quickstart": ["flow_worker", "delay_service", "event_filter"],
        "dev": [
            "flow_worker", "delay_service", "event_filter", "schedule_service",
            "http_callback_service", "redis_queue_service",
        ],
        "prod": [
            "flow_worker", "delay_service", "event_filter", "schedule_service",
            "http_callback_service", "redis_queue_service", "approval_service",
            "kafka_queue_service",
        ],
    }

    def _build_preset_config(self, preset: str, redis_url: str) -> Dict[str, Any]:
        """按架构配套预设生成集群配置。真实后端一律 Redis（见
        docs/architecture-profiles.md），不生成 eventbus/queue/storage 死键。"""
        if preset not in self._PRESET_SERVICES:
            preset = "dev"
        services: Dict[str, Any] = {}
        for name in self._PRESET_SERVICES[preset]:
            tpl = self._SERVICE_TEMPLATES[name]
            svc: Dict[str, Any] = {
                "display_name": tpl["display_name"],
                "default_instances": 1,
                "max_instances": tpl["max_instances"] if preset != "prod" else tpl["max_instances"],
                "env": {"PLAITA_REDIS_URL": redis_url},
            }
            svc["process"] = dict(tpl["process"])
            if "env" in tpl:
                svc["env"].update(tpl["env"])
            services[name] = svc
        config: Dict[str, Any] = {
            "profile": preset,
            "mode": "process",
            "redis": {"url": redis_url},
            "services": services,
        }
        if preset == "prod":
            config["notes"] = (
                "生产级：Redis 开 AOF 持久化；流程定义库建议切 PostgreSQL；"
                "console 设置 PLAITA_CONSOLE_ADMIN_API_KEY"
            )
        return config

    def _create_default_config(self, config_path: str, preset: str = "dev", redis_url: str = "redis://localhost:6379/0"):
        """创建集群配置（按架构配套预设）"""
        Path(config_path).parent.mkdir(parents=True, exist_ok=True)
        config = self._build_preset_config(preset, redis_url)
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
    
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
    
    def _cluster_dir(self, cluster_id: str) -> Path:
        """由 id 推导的集群目录；解析后必须仍归属 ``base_dir``（plaita#244）。

        ``cluster_id`` 来自 API 输入，直接 ``base_dir / cluster_id`` 会让
        ``..`` / 绝对路径（以及指向外部的符号链接）把配置写到任意可写位置，
        删除侧则 ``rmtree`` 掉任意目录。这里按**解析后的路径归属**判定，作为
        schema 白名单之外的第二道闸（registry 直调同样生效）。
        """
        base = self.base_dir.resolve()
        candidate = (base / cluster_id).resolve()
        if not candidate.is_relative_to(base):
            raise ValueError(f"集群 ID 非法: {cluster_id!r}")
        return candidate

    def create_cluster(
        self,
        cluster_id: str,
        name: str,
        description: str = "",
        config_path: Optional[str] = None,
        redis_url: str = "redis://localhost:6379/0",
        preset: str = "dev"
    ) -> ClusterInfo:
        """创建新集群（preset：架构配套 quickstart / dev / prod）"""
        if cluster_id in self._clusters:
            raise ValueError(f"集群 {cluster_id} 已存在")
        
        # 如果没有指定配置路径，按预设创建
        if not config_path:
            cluster_dir = self._cluster_dir(cluster_id)
            cluster_dir.mkdir(parents=True, exist_ok=True)
            config_path = str(cluster_dir / "cluster_config.yaml")
            self._create_default_config(config_path, preset=preset, redis_url=redis_url)
        
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
        redis_url: Optional[str] = None,
        config_path: Optional[str] = None
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
        # config_path 此前不可更新：仓库整体迁移后旧路径永久失效，
        # registry 无法自我修复，只能让 /cluster/* 一直 500（10-06 事故）。
        if config_path is not None:
            info.config_path = config_path
        
        self._save_registry()
        return info
    
    def delete_cluster(self, cluster_id: str) -> bool:
        """删除集群"""
        if cluster_id not in self._clusters:
            return False
        
        if cluster_id == self._active_cluster_id:
            raise ValueError("不能删除当前活动集群")
        
        # 删除集群目录（与创建同闸：解析后必须仍在 base_dir 内，plaita#244）
        cluster_dir = self._cluster_dir(cluster_id)
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
    
    def data_plane_redis_url(self, cluster_id: Optional[str] = None) -> Optional[str]:
        """集群（缺省为活动集群）数据面该连的 Redis URL。

        与 ``ServiceManager.load_config`` 同一约定（plaita#65）：集群配置文件的
        ``redis.url`` 是权威，localhost / 127.0.0.1 占位让位
        ``PLAITA_CONSOLE_REDIS_URL``，配置文件没给 URL 时退到注册表元数据。
        """
        info = self.get_cluster(cluster_id) if cluster_id else self.get_active_cluster()
        if info is None:
            return None
        try:
            from .service_manager import config_redis_url, effective_redis_url
        except ImportError:
            from service_manager import config_redis_url, effective_redis_url  # type: ignore
        return effective_redis_url(
            config_redis_url(self.get_cluster_config(info.id)), info.redis_url
        )

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

