"""
服务管理器
负责启动、停止、管理 Plaita 服务实例

支持两种模式：
- process: 本地 Python 进程
- docker: Docker 容器
"""
import asyncio
import os
import signal
import subprocess
import sys
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field

# 项目根目录（支持通过环境变量覆盖，用于 Docker 环境）
PROJECT_ROOT = Path(os.environ.get("PLAITA_PROJECT_ROOT", str(Path(__file__).parent.parent.parent.parent)))


class ServiceConfig(BaseModel):
    """服务配置"""
    service_type: str
    display_name: str
    process: Dict[str, Any] = Field(default_factory=dict)
    docker: Dict[str, Any] = Field(default_factory=dict)
    env: Dict[str, str] = Field(default_factory=dict)
    default_instances: int = 1
    max_instances: int = 10


class ManagedInstance(BaseModel):
    """被管理的服务实例"""
    instance_id: str
    service_type: str
    pid: Optional[int] = None
    container_id: Optional[str] = None
    status: str = "starting"  # starting, running, stopping, stopped, error
    start_time: str
    error_message: Optional[str] = None


class InfrastructureConfig(BaseModel):
    """基础设施服务配置"""
    name: str  # 配置 key (如 redis, kafka, postgresql)
    display_name: str
    type: str  # redis, kafka, database
    enabled: bool = True
    url: Optional[str] = None
    bootstrap_servers: Optional[str] = None  # Kafka 专用
    docker: Dict[str, Any] = Field(default_factory=dict)


class InternalComponentConfig(BaseModel):
    """内部组件配置（事件总线、队列、存储）"""
    type: str = "memory"  # memory, redis, db, kafka 等
    url: Optional[str] = None
    
    model_config = {"extra": "allow"}  # 允许额外字段


class ClusterConfig(BaseModel):
    """集群配置"""
    mode: str = "process"  # process 或 docker
    redis: Dict[str, str] = Field(default_factory=dict)
    services: Dict[str, ServiceConfig] = Field(default_factory=dict)
    infrastructure: Dict[str, InfrastructureConfig] = Field(default_factory=dict)
    # 内部组件配置
    eventbus: Optional[InternalComponentConfig] = None
    queue: Optional[InternalComponentConfig] = None
    storage: Optional[InternalComponentConfig] = None


class ServiceLauncher(ABC):
    """服务启动器抽象类"""
    
    @abstractmethod
    async def start(self, service_type: str, config: ServiceConfig, env: Dict[str, str]) -> ManagedInstance:
        """启动服务实例"""
        pass
    
    @abstractmethod
    async def stop(self, instance: ManagedInstance, graceful: bool = True) -> bool:
        """停止服务实例"""
        pass
    
    @abstractmethod
    async def check_status(self, instance: ManagedInstance) -> str:
        """检查实例状态"""
        pass


class ProcessLauncher(ServiceLauncher):
    """本地进程启动器"""
    
    def __init__(self):
        self._processes: Dict[str, subprocess.Popen] = {}
    
    async def start(self, service_type: str, config: ServiceConfig, env: Dict[str, str]) -> ManagedInstance:
        """启动 Python 进程"""
        instance_id = f"{service_type}-{uuid.uuid4().hex[:8]}"
        
        # 构建环境变量
        process_env = os.environ.copy()
        process_env.update(env)
        process_env["PLAITA_INSTANCE_ID"] = instance_id
        process_env["PYTHONPATH"] = str(PROJECT_ROOT)
        
        # 白名单校验（纵深防御：即使配置绕过写入，启动时仍拦截）
        try:
            from .process_allowlist import ProcessConfigError, validate_process_spec
        except ImportError:
            from process_allowlist import ProcessConfigError, validate_process_spec
        try:
            validate_process_spec(config.process, service_key=service_type)
        except ProcessConfigError as e:
            raise ValueError(str(e)) from e

        # 获取模块路径（用 shlex 解析 command，避免简单 split 的注入面）
        import shlex

        module = config.process.get("module")
        command = config.process.get("command")
        
        if module:
            cmd = [sys.executable, "-m", module]
        elif command:
            cmd_parts = shlex.split(command)
            if cmd_parts[0] in ("python", "python3") or cmd_parts[0].endswith(
                ("/python", "/python3", "python.exe")
            ):
                cmd_parts[0] = sys.executable
            cmd = cmd_parts
        else:
            raise ValueError(f"服务 {service_type} 缺少 module 或 command 配置")
        
        try:
            # 启动进程
            process = subprocess.Popen(
                cmd,
                env=process_env,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True  # 创建新会话组
            )
            
            self._processes[instance_id] = process
            
            # 等待一小段时间检查进程是否正常启动
            await asyncio.sleep(0.5)
            
            if process.poll() is not None:
                # 进程已退出
                stdout, _ = process.communicate(timeout=1)
                error_msg = stdout.decode() if stdout else "进程启动后立即退出"
                return ManagedInstance(
                    instance_id=instance_id,
                    service_type=service_type,
                    pid=None,
                    status="error",
                    start_time=datetime.now().isoformat(),
                    error_message=error_msg
                )
            
            return ManagedInstance(
                instance_id=instance_id,
                service_type=service_type,
                pid=process.pid,
                status="running",
                start_time=datetime.now().isoformat()
            )
            
        except Exception as e:
            return ManagedInstance(
                instance_id=instance_id,
                service_type=service_type,
                status="error",
                start_time=datetime.now().isoformat(),
                error_message=str(e)
            )
    
    async def stop(self, instance: ManagedInstance, graceful: bool = True) -> bool:
        """停止进程"""
        process = self._processes.get(instance.instance_id)
        if not process:
            return False
        
        try:
            if graceful:
                # 发送 SIGTERM
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                # 等待进程退出
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    # 超时后强制杀死
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            else:
                # 直接 SIGKILL
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            
            del self._processes[instance.instance_id]
            return True
            
        except ProcessLookupError:
            # 进程已不存在
            if instance.instance_id in self._processes:
                del self._processes[instance.instance_id]
            return True
        except Exception:
            return False
    
    async def check_status(self, instance: ManagedInstance) -> str:
        """检查进程状态"""
        process = self._processes.get(instance.instance_id)
        if not process:
            return "stopped"
        
        poll_result = process.poll()
        if poll_result is None:
            return "running"
        elif poll_result == 0:
            return "stopped"
        else:
            return "error"


class DockerLauncher(ServiceLauncher):
    """Docker 容器启动器"""
    
    def __init__(self):
        self._containers: Dict[str, str] = {}  # instance_id -> container_id
        self._docker_available = self._check_docker()
    
    def _check_docker(self) -> bool:
        """检查 Docker 是否可用"""
        try:
            result = subprocess.run(
                ["docker", "version"],
                capture_output=True,
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False
    
    async def start(self, service_type: str, config: ServiceConfig, env: Dict[str, str]) -> ManagedInstance:
        """启动 Docker 容器"""
        if not self._docker_available:
            return ManagedInstance(
                instance_id=f"{service_type}-{uuid.uuid4().hex[:8]}",
                service_type=service_type,
                status="error",
                start_time=datetime.now().isoformat(),
                error_message="Docker 不可用"
            )
        
        instance_id = f"{service_type}-{uuid.uuid4().hex[:8]}"
        image = config.docker.get("image", f"plaita/{service_type}:latest")
        
        # 构建 docker run 命令
        cmd = ["docker", "run", "-d", "--name", instance_id]
        
        # 添加环境变量
        for key, value in env.items():
            cmd.extend(["-e", f"{key}={value}"])
        cmd.extend(["-e", f"PLAITA_INSTANCE_ID={instance_id}"])
        
        # 添加端口映射
        for port in config.docker.get("ports", []):
            cmd.extend(["-p", port])
        
        # 添加镜像
        cmd.append(image)
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                return ManagedInstance(
                    instance_id=instance_id,
                    service_type=service_type,
                    status="error",
                    start_time=datetime.now().isoformat(),
                    error_message=result.stderr or "Docker 启动失败"
                )
            
            container_id = result.stdout.strip()[:12]
            self._containers[instance_id] = container_id
            
            return ManagedInstance(
                instance_id=instance_id,
                service_type=service_type,
                container_id=container_id,
                status="running",
                start_time=datetime.now().isoformat()
            )
            
        except Exception as e:
            return ManagedInstance(
                instance_id=instance_id,
                service_type=service_type,
                status="error",
                start_time=datetime.now().isoformat(),
                error_message=str(e)
            )
    
    async def stop(self, instance: ManagedInstance, graceful: bool = True) -> bool:
        """停止 Docker 容器"""
        container_id = self._containers.get(instance.instance_id) or instance.container_id
        if not container_id:
            return False
        
        try:
            if graceful:
                cmd = ["docker", "stop", "-t", "10", container_id]
            else:
                cmd = ["docker", "kill", container_id]
            
            subprocess.run(cmd, capture_output=True, timeout=30)
            
            # 删除容器
            subprocess.run(["docker", "rm", container_id], capture_output=True, timeout=10)
            
            if instance.instance_id in self._containers:
                del self._containers[instance.instance_id]
            
            return True
        except Exception:
            return False
    
    async def check_status(self, instance: ManagedInstance) -> str:
        """检查容器状态"""
        container_id = self._containers.get(instance.instance_id) or instance.container_id
        if not container_id:
            return "stopped"
        
        try:
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Status}}", container_id],
                capture_output=True,
                text=True,
                timeout=5
            )
            status = result.stdout.strip()
            if status == "running":
                return "running"
            elif status in ("exited", "dead"):
                return "stopped"
            else:
                return "unknown"
        except Exception:
            return "unknown"


class ServiceManager:
    """
    服务管理器主类
    
    负责加载配置、管理服务实例
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or str(PROJECT_ROOT / "plaita-console" / "cluster_config.yaml")
        self.config: Optional[ClusterConfig] = None
        self.instances: Dict[str, ManagedInstance] = {}
        self._process_launcher = ProcessLauncher()
        self._docker_launcher = DockerLauncher()
    
    def load_config(self, config_path: Optional[str] = None) -> ClusterConfig:
        """加载配置文件"""
        if config_path:
            self.config_path = config_path
            
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)
        
        # 解析服务配置
        services = {}
        for service_type, service_data in raw_config.get("services", {}).items():
            service_data["service_type"] = service_type
            services[service_type] = ServiceConfig(**service_data)
        
        # 解析基础设施配置
        infrastructure = {}
        for infra_name, infra_data in raw_config.get("infrastructure", {}).items():
            infra_data["name"] = infra_name
            infrastructure[infra_name] = InfrastructureConfig(**infra_data)
        
        # 解析内部组件配置
        eventbus_config = None
        if "eventbus" in raw_config:
            eventbus_config = InternalComponentConfig(**raw_config["eventbus"])
        
        queue_config = None
        if "queue" in raw_config:
            queue_config = InternalComponentConfig(**raw_config["queue"])
        
        storage_config = None
        if "storage" in raw_config:
            storage_config = InternalComponentConfig(**raw_config["storage"])
        
        redis_config = raw_config.get("redis", {})
        # 允许环境变量覆盖 Redis URL（Docker 环境中 redis 服务地址不同）
        env_redis_url = os.environ.get("PLAITA_CONSOLE_REDIS_URL")
        if env_redis_url:
            redis_config["url"] = env_redis_url
        
        self.config = ClusterConfig(
            mode=raw_config.get("mode", "process"),
            redis=redis_config,
            services=services,
            infrastructure=infrastructure,
            eventbus=eventbus_config,
            queue=queue_config,
            storage=storage_config
        )
        
        return self.config
    
    def get_launcher(self) -> ServiceLauncher:
        """获取当前模式的启动器"""
        if not self.config:
            self.load_config()
        
        if self.config.mode == "docker":
            return self._docker_launcher
        return self._process_launcher
    
    def _resolve_env(self, env: Dict[str, str]) -> Dict[str, str]:
        """解析环境变量中的引用，并确保 Redis URL 与实际配置一致"""
        if not self.config:
            return env
        
        actual_redis_url = self.config.redis.get("url", "redis://localhost:6379/0")
        resolved = {}
        for key, value in env.items():
            if "${redis.url}" in value:
                value = value.replace("${redis.url}", actual_redis_url)
            elif key.upper() in ("REDIS_URL", "PLAITA_REDIS_URL") and "localhost" in value:
                value = actual_redis_url
            resolved[key] = value
        return resolved
    
    async def start_service(self, service_type: str) -> ManagedInstance:
        """启动一个服务实例"""
        if not self.config:
            self.load_config()
        
        if service_type not in self.config.services:
            raise ValueError(f"未知服务类型: {service_type}")
        
        service_config = self.config.services[service_type]
        
        # 检查实例数量限制
        current_count = sum(
            1 for inst in self.instances.values()
            if inst.service_type == service_type and inst.status == "running"
        )
        if current_count >= service_config.max_instances:
            raise ValueError(f"已达到最大实例数: {service_config.max_instances}")
        
        # 解析环境变量
        env = self._resolve_env(service_config.env)
        
        # 启动服务
        launcher = self.get_launcher()
        instance = await launcher.start(service_type, service_config, env)
        
        self.instances[instance.instance_id] = instance
        return instance
    
    async def stop_service(self, instance_id: str, graceful: bool = True) -> bool:
        """停止一个服务实例"""
        instance = self.instances.get(instance_id)
        if not instance:
            return False
        
        launcher = self.get_launcher()
        success = await launcher.stop(instance, graceful)
        
        if success:
            instance.status = "stopped"
        
        return success
    
    async def stop_all(self, graceful: bool = True) -> Dict[str, bool]:
        """停止所有服务"""
        results = {}
        for instance_id in list(self.instances.keys()):
            results[instance_id] = await self.stop_service(instance_id, graceful)
        return results
    
    async def refresh_status(self) -> None:
        """刷新所有实例状态"""
        launcher = self.get_launcher()
        for instance in self.instances.values():
            if instance.status in ("running", "starting"):
                instance.status = await launcher.check_status(instance)
    
    def list_instances(self, service_type: Optional[str] = None) -> List[ManagedInstance]:
        """列出所有实例"""
        instances = list(self.instances.values())
        if service_type:
            instances = [i for i in instances if i.service_type == service_type]
        return instances
    
    def remove_instance(self, instance_id: str) -> bool:
        """
        移除一个实例记录（仅用于已停止或出错的实例）
        
        Args:
            instance_id: 实例ID
            
        Returns:
            是否移除成功
        """
        instance = self.instances.get(instance_id)
        if not instance:
            return False
        
        # 只允许删除已停止或出错的实例
        if instance.status == "running":
            return False
        
        del self.instances[instance_id]
        return True
    
    def clear_failed_instances(self) -> int:
        """
        清除所有失败的实例记录
        
        Returns:
            清除的实例数量
        """
        failed_ids = [
            inst_id for inst_id, inst in self.instances.items()
            if inst.status in ("error", "stopped")
        ]
        for inst_id in failed_ids:
            del self.instances[inst_id]
        return len(failed_ids)
    
    def get_available_services(self) -> Dict[str, ServiceConfig]:
        """获取所有可用服务类型"""
        if not self.config:
            self.load_config()
        return self.config.services
    
    def get_infrastructure(self) -> Dict[str, InfrastructureConfig]:
        """获取所有基础设施配置"""
        if not self.config:
            self.load_config()
        return self.config.infrastructure
    
    async def check_infrastructure_health(self) -> Dict[str, Dict[str, Any]]:
        """
        检查所有基础设施服务的健康状态
        
        Returns:
            字典，key 为基础设施名称，value 包含 status 和 details
        """
        if not self.config:
            self.load_config()
        
        results = {}
        for name, infra in self.config.infrastructure.items():
            if not infra.enabled:
                results[name] = {
                    "status": "disabled",
                    "details": "已禁用"
                }
                continue
            
            if infra.type == "redis":
                results[name] = await self._check_redis_health(infra)
            elif infra.type == "kafka":
                results[name] = await self._check_kafka_health(infra)
            elif infra.type == "database":
                results[name] = await self._check_database_health(infra)
            else:
                results[name] = {
                    "status": "unknown",
                    "details": f"未知类型: {infra.type}"
                }
        
        return results
    
    async def _check_redis_health(self, infra: InfrastructureConfig) -> Dict[str, Any]:
        """检查 Redis 健康状态"""
        try:
            from redis import Redis
            url = infra.url or self.config.redis.get("url", "redis://localhost:6379/0")
            client = Redis.from_url(url, socket_connect_timeout=3)
            ping_result = client.ping()
            info = client.info("server")
            client.close()
            
            return {
                "status": "healthy" if ping_result else "unhealthy",
                "details": {
                    "url": url,
                    "version": info.get("redis_version", "unknown"),
                    "uptime_seconds": info.get("uptime_in_seconds", 0)
                }
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "details": f"连接失败: {str(e)}"
            }
    
    async def _check_kafka_health(self, infra: InfrastructureConfig) -> Dict[str, Any]:
        """检查 Kafka 健康状态"""
        try:
            import socket
            bootstrap = infra.bootstrap_servers or "localhost:9092"
            host, port = bootstrap.split(":")[0], int(bootstrap.split(":")[1])
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                return {
                    "status": "healthy",
                    "details": {
                        "bootstrap_servers": bootstrap,
                        "port_accessible": True
                    }
                }
            else:
                return {
                    "status": "unhealthy",
                    "details": f"端口 {port} 无法访问"
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "details": f"检查失败: {str(e)}"
            }
    
    async def _check_database_health(self, infra: InfrastructureConfig) -> Dict[str, Any]:
        """检查数据库健康状态"""
        try:
            from urllib.parse import urlparse
            import socket
            
            if not infra.url:
                return {
                    "status": "unhealthy",
                    "details": "未配置数据库 URL"
                }
            
            parsed = urlparse(infra.url)
            host = parsed.hostname or "localhost"
            port = parsed.port or (5432 if "postgresql" in infra.url else 3306)
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                return {
                    "status": "healthy",
                    "details": {
                        "url": infra.url,
                        "host": host,
                        "port": port,
                        "port_accessible": True
                    }
                }
            else:
                return {
                    "status": "unhealthy",
                    "details": f"端口 {port} 无法访问"
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "details": f"检查失败: {str(e)}"
            }
    
    def _save_config(self) -> bool:
        """保存当前配置到文件"""
        try:
            config_data = {
                "mode": self.config.mode,
                "redis": self.config.redis,
                "services": {},
                "infrastructure": {}
            }
            
            # 保存服务配置
            for name, svc in self.config.services.items():
                config_data["services"][name] = {
                    "display_name": svc.display_name,
                    "process": svc.process,
                    "docker": svc.docker,
                    "env": svc.env,
                    "default_instances": svc.default_instances,
                    "max_instances": svc.max_instances
                }
            
            # 保存基础设施配置
            for name, infra in self.config.infrastructure.items():
                infra_data: Dict[str, Any] = {
                    "display_name": infra.display_name,
                    "type": infra.type,
                    "enabled": infra.enabled,
                }
                if infra.url:
                    infra_data["url"] = infra.url
                if infra.bootstrap_servers:
                    infra_data["bootstrap_servers"] = infra.bootstrap_servers
                if infra.docker:
                    infra_data["docker"] = infra.docker
                config_data["infrastructure"][name] = infra_data
            
            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, allow_unicode=True, default_flow_style=False)
            
            return True
        except Exception as e:
            print(f"保存配置失败: {e}")
            return False
    
    async def add_infrastructure(
        self,
        name: str,
        display_name: str,
        type: str,
        enabled: bool = True,
        url: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        docker: Optional[Dict[str, Any]] = None
    ) -> bool:
        """添加新的基础设施配置"""
        if not self.config:
            self.load_config()
        
        new_infra = InfrastructureConfig(
            name=name,
            display_name=display_name,
            type=type,
            enabled=enabled,
            url=url,
            bootstrap_servers=bootstrap_servers,
            docker=docker or {}
        )
        
        self.config.infrastructure[name] = new_infra
        return self._save_config()
    
    async def update_infrastructure(
        self,
        name: str,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        url: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        docker: Optional[Dict[str, Any]] = None
    ) -> bool:
        """更新基础设施配置"""
        if not self.config:
            self.load_config()
        
        if name not in self.config.infrastructure:
            return False
        
        infra = self.config.infrastructure[name]
        
        if display_name is not None:
            infra.display_name = display_name
        if enabled is not None:
            infra.enabled = enabled
        if url is not None:
            infra.url = url
        if bootstrap_servers is not None:
            infra.bootstrap_servers = bootstrap_servers
        if docker is not None:
            infra.docker = docker
        
        return self._save_config()
    
    async def delete_infrastructure(self, name: str) -> bool:
        """删除基础设施配置"""
        if not self.config:
            self.load_config()
        
        if name not in self.config.infrastructure:
            return False
        
        del self.config.infrastructure[name]
        return self._save_config()


# 全局服务管理器实例
_manager: Optional[ServiceManager] = None


def get_service_manager() -> ServiceManager:
    """获取全局服务管理器"""
    global _manager
    if _manager is None:
        _manager = ServiceManager()
        
        # 尝试从集群注册表加载配置
        try:
            from .cluster_registry import get_cluster_registry
            registry = get_cluster_registry()
            active_cluster = registry.get_active_cluster()
            if active_cluster:
                _manager.load_config(active_cluster.config_path)
            else:
                _manager.load_config()
        except Exception:
            _manager.load_config()
    
    return _manager


def reset_service_manager():
    """重置服务管理器（用于集群切换时重新加载配置）"""
    global _manager
    _manager = None

