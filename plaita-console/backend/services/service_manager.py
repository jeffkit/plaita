"""
服务管理器
负责启动、停止、管理 Plaita 服务实例

支持两种模式：
- process: 本地 Python 进程
- docker: Docker 容器
"""
import asyncio
import logging
import os
import re
import signal
import subprocess
import sys
import uuid
import weakref
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field

from plaita.redis_client import (
    HEALTH_CONNECT_TIMEOUT_SECONDS,
    HEALTH_SOCKET_TIMEOUT_SECONDS,
    make_redis_client,
)

try:
    from .atomic_io import atomic_write_text
except ImportError:  # 平铺布局（cwd=backend）运行时
    from atomic_io import atomic_write_text  # type: ignore

# B59-01：基础设施 docker.env 里的 ${VAR} 引用在启动时从 console 进程环境
# 展开；模板/仓库不再内置固定口令。缺失变量时 start_infrastructure 返回
# 可操作错误（fail-closed），不会把字面量 "${VAR}" 传给容器。
_INFRA_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
#: 各数据库镜像启动必需的根口令键（缺失即拒绝启动）
_DB_REQUIRED_PASSWORD_KEYS = {
    "postgres": ("POSTGRES_PASSWORD",),
    "postgresql": ("POSTGRES_PASSWORD",),
    "mysql": ("MYSQL_ROOT_PASSWORD",),
    "mariadb": ("MYSQL_ROOT_PASSWORD",),
}

# 项目根目录（支持通过环境变量覆盖，用于 Docker 环境）
PROJECT_ROOT = Path(os.environ.get("PLAITA_PROJECT_ROOT", str(Path(__file__).parent.parent.parent.parent)))

_log = logging.getLogger(__name__)


class ClusterConfigError(RuntimeError):
    """集群配置文件不可用：缺失且无回退 / YAML 损坏 / 顶层结构非法。

    ``config_path`` 指向出问题的文件，供 API 层原样回给调用方。旧实现里
    半截 YAML 会在 ``raw_config.get(...)`` 处抛 ``AttributeError``，再被
    ``GET /api/cluster/config`` 折叠成 ``mode=standalone`` 200——故障被伪装
    成健康（plaita#253）。
    """

    def __init__(self, config_path: Any, reason: Any):
        self.config_path = str(config_path)
        self.reason = str(reason)
        super().__init__(f"集群配置载入失败（{self.config_path}）：{self.reason}")


def _is_concrete_redis_url(url: Any) -> bool:
    """集群配置是否给出了「非占位」的 Redis URL。

    localhost / 127.0.0.1 是模板留的占位（Docker 里 console 容器与 redis 容器
    不同主机），不算具体地址——只有具体地址才是集群的权威配置。
    """
    if not url:
        return False
    text = str(url)
    return "localhost" not in text and "127.0.0.1" not in text


def config_redis_url(config: Optional[Dict[str, Any]]) -> Optional[str]:
    """取集群配置字典里的 ``redis.url``（无则 None）。"""
    if not isinstance(config, dict):
        return None
    redis_config = config.get("redis")
    if not isinstance(redis_config, dict):
        return None
    url = redis_config.get("url")
    return str(url) if url else None


def effective_redis_url(
    config_url: Optional[str], registry_url: Optional[str] = None
) -> Optional[str]:
    """集群数据面该连的 Redis URL——与 ``load_config`` 同一约定（plaita#65）。

    集群配置文件的 ``redis.url`` 是权威；localhost / 127.0.0.1 只是模板占位
    （Docker 里 console 容器与 redis 容器不同主机），占位时让位给
    ``PLAITA_CONSOLE_REDIS_URL``；配置文件没给 URL 时才退到注册表元数据。
    """
    if _is_concrete_redis_url(config_url):
        return str(config_url)
    env_redis_url = os.environ.get("PLAITA_CONSOLE_REDIS_URL")
    if env_redis_url:
        return env_redis_url
    for url in (config_url, registry_url):
        if url:
            return str(url)
    return None


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

        # 凭据文件/密钥透传：worker 节点运行时经 plaita.credentials 解密读取。
        # 注入的是 default 租户基础文件；非 default 租户的 flow 由 worker 在
        # 消费任务时按消息体 tenant_id（plaita.tenant_context）原生路由到旁
        # 文件 .plaita-credentials.{tenant}.json——凭据路由在引擎侧，不在
        # 拉起环境里逐租户重复注入。
        try:
            from . import credentials_svc
        except ImportError:
            import credentials_svc  # type: ignore
        process_env.setdefault(
            "PLAITA_CREDENTIALS_FILE", str(credentials_svc.credentials_file())
        )
        key_env = os.environ.get("PLAITA_CREDENTIALS_KEY")
        if key_env:
            process_env["PLAITA_CREDENTIALS_KEY"] = key_env
        elif credentials_svc.credentials_key_file().is_file():
            process_env.setdefault(
                "PLAITA_CREDENTIALS_KEY_FILE", str(credentials_svc.credentials_key_file())
            )
        
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

        # 服务可用 PLAITA_PYTHON 指定业务 venv 解释器（其节点依赖装在 venv 里，
        # 如 mediaflow/.runtime/venv 的 plaita/agentproc/plaita-nodes）
        if process_env.get("PLAITA_PYTHON"):
            cmd[0] = process_env["PLAITA_PYTHON"]
        
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

        # B59-12：不再按服务名拼 `plaita/<type>` 的 latest 标签兜底。仓库没有
        # 发布这些镜像，隐式 pull 只会得到通用 404；更糟的是一旦命名空间被
        # 第三方注册，运维点「启动」就会运行攻击者镜像。image 必须显式配置，
        # 且必须已在本地（先在目标机构建/拉取），这里不做任何隐式 pull。
        image = (config.docker or {}).get("image")
        if not image or not str(image).strip():
            return ManagedInstance(
                instance_id=instance_id,
                service_type=service_type,
                status="error",
                start_time=datetime.now().isoformat(),
                error_message=(
                    "镜像未配置：cluster_config.yaml 的 service.docker.image 为空。"
                    "请先在目标机构建/拉取镜像并显式填写 image（见 docs-site/docs/guide/deployment.md）"
                ),
            )
        image = str(image).strip()
        if image.startswith("-") or any(ch.isspace() for ch in image):
            return ManagedInstance(
                instance_id=instance_id,
                service_type=service_type,
                status="error",
                start_time=datetime.now().isoformat(),
                error_message=f"非法镜像引用: {image!r}（不允许空白或前导 -）",
            )
        inspect = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True, text=True, timeout=10,
        )
        if inspect.returncode != 0:
            return ManagedInstance(
                instance_id=instance_id,
                service_type=service_type,
                status="error",
                start_time=datetime.now().isoformat(),
                error_message=(
                    f"镜像 {image} 本地不存在；已阻止隐式 pull（避免从不可信命名空间拉取）。"
                    f"请先 docker pull {image} 或构建镜像后重试"
                ),
            )
        
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
    
    def _fallback_config_path(self) -> Optional[Path]:
        """找一个可用的等价集群配置。

        仓库或家目录整体迁移后（10-06 tcloud_gz 迁移即此例），cluster registry
        里落着的旧 config_path 会失效；此时回退到当前 checkout 里的同名配置，
        而不是让 /cluster/* 端点全量 500。
        """
        candidates: List[Path] = [
            Path(PROJECT_ROOT) / "plaita-console" / "cluster_config.yaml",
            Path.home() / ".plaita-console" / "clusters" / "default" / "cluster_config.yaml",
        ]
        # 旧路径的 basename 在新 checkout 里通常还在，按文件名再兜一层
        if PROJECT_ROOT.is_dir():
            candidates.extend(PROJECT_ROOT.glob(f"**/{Path(self.config_path).name}"))
        for candidate in candidates:
            try:
                if candidate.is_file():
                    return candidate
            except OSError:
                continue
        return None

    def _persist_config_path(self, new_path: str) -> None:
        """把自愈后的路径写回 cluster registry，避免每次请求都走一遍回退。"""
        try:
            from .cluster_registry import get_cluster_registry
        except ImportError:
            return
        try:
            registry = get_cluster_registry()
            active = registry.get_active_cluster()
            if active and Path(active.config_path) == Path(new_path):
                return
            if active:
                registry.update_cluster(active.id, config_path=new_path)
                _log.info("已把集群 %s 的 config_path 更新为 %s", active.id, new_path)
        except Exception as e:
            _log.warning("回写集群 config_path 失败（不影响本次加载）: %s", e)

    def load_config(self, config_path: Optional[str] = None) -> ClusterConfig:
        """加载配置文件。

        损坏/截断（半截 YAML、空文件、顶层或 services 不是映射）时抛
        :class:`ClusterConfigError`，消息带配置路径；不静默回退默认配置——
        旧实现会在 ``raw_config.get(...)`` 抛 ``AttributeError``，再被
        ``GET /api/cluster/config`` 折叠成 ``standalone`` 200，把损坏文件
        伪装成健康（plaita#253）。
        """
        if config_path:
            self.config_path = config_path
            
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            fallback = self._fallback_config_path()
            if fallback is None:
                raise ClusterConfigError(
                    self.config_path, "配置文件不存在且无可回退配置")
            _log.warning("集群配置路径失效 %s，回退到 %s", self.config_path, fallback)
            self.config_path = str(fallback)
            config_file = fallback
            self._persist_config_path(str(fallback))
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)

            if not isinstance(raw_config, dict):
                raise ValueError(
                    f"顶层必须是映射，实际为 {type(raw_config).__name__}"
                    "（空文件或写一半被截断）"
                )
            for key in ("services", "infrastructure", "redis",
                        "eventbus", "queue", "storage"):
                value = raw_config.get(key)
                if value is not None and not isinstance(value, dict):
                    raise ValueError(
                        f"{key} 必须是映射，实际为 {type(value).__name__}"
                        "（配置被截断？）"
                    )

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
            # 集群配置的 redis.url 是权威（plaita#65）：切到别的集群后管理面读数
            # （/cluster/service-types 等）必须跟着集群走，不能再被 console 数据面
            # 的 env 无条件拉回旧集群。env 只在集群未给出 url、或给的是 localhost
            # 占位时兜底（Docker 环境集群配置留的 localhost 须让位）。
            env_redis_url = os.environ.get("PLAITA_CONSOLE_REDIS_URL")
            if env_redis_url and not _is_concrete_redis_url(redis_config.get("url")):
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
        except Exception as e:
            # 不保留上一份内存配置：否则损坏文件仍以旧值对外服务。原文件
            # 原地保留（改走 .corrupt 备份会让下一次加载把它当「路径失效」
            # 回退到别的 checkout，故障再次被伪装健康）。
            self.config = None
            _log.warning(
                "集群配置载入失败（%s），原文件保留未覆盖: %s",
                config_file, e, exc_info=True,
            )
            raise ClusterConfigError(config_file, e) from e
        
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
    
    # ---- 启动门的原子预留（plaita#211）----
    # 「计数 → 启动」之间原有 await（launcher.start）：两个并发请求都会在
    # 对方写入 instances 之前通过计数检查，max_instances=1 的 schedule_service
    # 因此被拉起两份（cron 同一时点双触发）。修法：把「计数（running +
    # starting/reserved）→ 预留」放进同一段**无 await** 的临界区（每
    # service_type 一把 asyncio.Lock），预留只占名额、不污染 instances
    # （list_instances / refresh_status 看不到幽灵实例）；启动失败在 except
    # 里释放预留，避免占位泄漏永久占满 max_instances。

    def _start_lock(self, service_type: str) -> asyncio.Lock:
        """每 service_type 一把锁。

        锁只包住「计数 → 预留」几步同步代码（不含 await），持锁时间可忽略；
        不跨 ``launcher.start`` 的 await 持锁，max_instances>1 的并发启动仍可
        并行。``asyncio.Lock`` 在 3.10+ 首次使用时绑定事件循环，跨 loop 复用会
        抛 RuntimeError——按事件循环分桶（``WeakKeyDictionary``：loop 被回收后
        条目自动消失，不会因 loop id 复用拿到绑定到死循环的旧锁），测试里对同一
        manager 多次 ``asyncio.run`` 也不会炸。
        """
        locks_by_loop = self.__dict__.setdefault(
            "_start_locks", weakref.WeakKeyDictionary()
        )
        locks = locks_by_loop.setdefault(asyncio.get_running_loop(), {})
        lock = locks.get(service_type)
        if lock is None:
            lock = locks[service_type] = asyncio.Lock()
        return lock

    def _reserved_starts(self, service_type: str) -> int:
        """正在启动中（已预留名额、launcher.start 尚未返回）的实例数。"""
        return self.__dict__.setdefault("_start_reservations", {}).get(service_type, 0)

    async def _reserve_start_slot(self, service_type: str, max_instances: int) -> None:
        """计数 + 预留原子完成；超限抛 ValueError（第二个并发请求在此被拒）。"""
        async with self._start_lock(service_type):
            reserved = self._reserved_starts(service_type)
            current_count = reserved + sum(
                1 for inst in self.instances.values()
                if inst.service_type == service_type
                and inst.status in ("running", "starting")
            )
            if current_count >= max_instances:
                if reserved:
                    raise ValueError(
                        f"服务已在启动中: {service_type}"
                        f"（{reserved} 个实例启动中，max_instances={max_instances}）"
                    )
                raise ValueError(f"已达到最大实例数: {max_instances}")
            reservations = self.__dict__.setdefault("_start_reservations", {})
            reservations[service_type] = reserved + 1

    def _release_start_slot(self, service_type: str) -> None:
        """释放预留名额（成功或失败都要释放，失败不得留下永久占位）。"""
        reservations = self.__dict__.setdefault("_start_reservations", {})
        reserved = reservations.get(service_type, 0)
        if reserved <= 1:
            reservations.pop(service_type, None)
        else:
            reservations[service_type] = reserved - 1

    async def start_service(self, service_type: str) -> ManagedInstance:
        """启动一个服务实例"""
        if not self.config:
            self.load_config()
        
        if service_type not in self.config.services:
            raise ValueError(f"未知服务类型: {service_type}")
        
        service_config = self.config.services[service_type]

        # 原子预留（#211）：并发第二个请求在这里拿到「已在启动」而不是双实例
        await self._reserve_start_slot(service_type, service_config.max_instances)
        try:
            # 解析环境变量
            env = self._resolve_env(service_config.env)

            # 启动服务
            launcher = self.get_launcher()
            instance = await launcher.start(service_type, service_config, env)
        except BaseException:
            self._release_start_slot(service_type)
            raise

        # 释放与写入之间没有 await：并发请求要么看到预留、要么看到 running 实例
        self._release_start_slot(service_type)
        self.instances[instance.instance_id] = instance
        return instance
    
    async def stop_service(self, instance_id: str, graceful: bool = True) -> bool:
        """停止一个服务实例"""
        instance = self.instances.get(instance_id)
        if not instance:
            return False

        # 迁移实例（见 reset_service_manager，#100）可能来自另一 mode 的旧
        # 集群：按实例自身句柄类型选 launcher，避免新集群 mode 翻转后旧
        # 实例无法收尸。
        if instance.container_id:
            launcher = self._docker_launcher
        elif instance.pid:
            launcher = self._process_launcher
        else:
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
    
    # ---- 基础设施容器化拉起（docker）----
    # 历史上 infrastructure 只有健康检查与配置 CRUD，docker: 段是死配置；
    # 这里补上真实的 start/stop：容器名约定 plaita-infra-{name}，
    # 启动后轮询健康检查直至 healthy（或超时返回当前状态）。

    INFRA_CONTAINER_PREFIX = "plaita-infra-"

    def _docker(self, *args, timeout: int = 60):
        return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)

    def _docker_available_now(self) -> bool:
        try:
            return self._docker("version", timeout=5).returncode == 0
        except Exception:
            return False

    def _infra_container_state(self, name: str) -> Optional[str]:
        """返回容器状态（running/exited/created…）；不存在返回 None。"""
        cname = f"{self.INFRA_CONTAINER_PREFIX}{name}"
        result = self._docker(
            "ps", "-a", "--filter", f"name=^{cname}$", "--format", "{{.State}}", timeout=10
        )
        return result.stdout.strip() or None

    def _resolve_infra_env(self, docker_cfg: Dict[str, Any]):
        """展开 infrastructure docker.env 中的 ``${VAR}``。

        返回 ``(resolved, missing)``；``missing`` 非空时调用方必须 fail-closed，
        不能把字面量 ``${VAR}`` 传给容器（否则数据库会用字面量当口令）。
        """
        resolved: Dict[str, str] = {}
        missing: List[str] = []
        for key, value in (docker_cfg.get("env") or {}).items():
            if isinstance(value, str):
                def _replace(match: "re.Match[str]") -> str:
                    var = match.group(1)
                    env_value = os.environ.get(var)
                    if not env_value:
                        missing.append(var)
                        return ""
                    return env_value
                value = _INFRA_ENV_REF.sub(_replace, value)
            resolved[str(key)] = str(value)
        return resolved, sorted(set(missing))

    def _missing_db_password(self, name: str, docker_cfg: Dict[str, Any],
                             docker_env: Dict[str, str]) -> Optional[str]:
        """数据库镜像缺少必需口令键时返回该键名（B59-01：不内置弱口令）。"""
        keys = _DB_REQUIRED_PASSWORD_KEYS.get(str(name).lower())
        if not keys:
            image = str(docker_cfg.get("image", "")).lower()
            if "postgres" in image:
                keys = ("POSTGRES_PASSWORD",)
            elif "mysql" in image or "mariadb" in image:
                keys = ("MYSQL_ROOT_PASSWORD",)
        if not keys:
            return None
        for key in keys:
            if not docker_env.get(key):
                return key
        return None

    async def start_infrastructure(self, name: str) -> Dict[str, Any]:
        """容器化拉起基础设施：已存在则 start，否则按 docker 配置 run；
        启动后轮询健康检查（redis ping / kafka 探测 / 数据库连接）。"""
        if not self.config:
            self.load_config()
        infra = self.config.infrastructure.get(name)
        if infra is None:
            return {"success": False, "message": f"基础设施未定义: {name}"}
        docker_cfg = infra.docker or {}
        if not docker_cfg.get("image"):
            return {"success": False, "message": "该基础设施未配置 docker 镜像，无法容器化拉起"}
        if not self._docker_available_now():
            return {"success": False, "message": "Docker 不可用"}

        docker_env, missing_env = self._resolve_infra_env(docker_cfg)
        if missing_env:
            return {
                "success": False,
                "message": (
                    f"基础设施 {name} 的 docker.env 引用了未设置的环境变量: "
                    f"{', '.join(missing_env)}——请在 console 进程环境提供后重试"
                    "（见 docs-site/docs/guide/deployment.md）"
                ),
            }
        missing_password = self._missing_db_password(name, docker_cfg, docker_env)
        if missing_password:
            return {
                "success": False,
                "message": (
                    f"基础设施 {name} 缺少 {missing_password}：数据库模板不再内置固定弱口令。"
                    f"请在 docker.env 写 ${{{missing_password}}} 并在 console 进程环境提供"
                    "（见 docs-site/docs/guide/deployment.md）"
                ),
            }

        cname = f"{self.INFRA_CONTAINER_PREFIX}{name}"
        state = self._infra_container_state(name)
        if state == "running":
            pass  # 容器已运行，仍继续走健康等待
        elif state:
            run_result = self._docker("start", cname, timeout=60)
            if run_result.returncode != 0:
                return {"success": False, "message": (run_result.stderr or "docker start 失败").strip()[-300:]}
        else:
            # 端口占用预检：宿主端口已被监听时，发布端口会造成连接混叠
            # （看起来启动成功，客户端连的却是别的实例）——明确拒绝。
            # 支持 "5432:5432" 与 "127.0.0.1:5432:5432" 两种写法；
            # 只写容器端口（随机宿主端口）时跳过预检。
            import socket as _socket
            for port in docker_cfg.get("ports", []):
                parts = str(port).rsplit(":", 2)
                if len(parts) < 2 or not parts[-2].isdigit():
                    continue
                host_port = int(parts[-2])
                probe = _socket.socket()
                probe.settimeout(1)
                occupied = probe.connect_ex(("127.0.0.1", host_port)) == 0
                probe.close()
                if occupied:
                    return {
                        "success": False,
                        "message": f"宿主机端口 {host_port} 已被占用——该端口上可能已有同类服务在运行，无需重复拉起",
                    }
            cmd = ["docker", "run", "-d", "--name", cname, "--restart", "unless-stopped"]
            for port in docker_cfg.get("ports", []):
                cmd += ["-p", str(port)]
            for vol in docker_cfg.get("volumes", []):
                cmd += ["-v", str(vol)]
            for env_key, env_value in docker_env.items():
                cmd += ["-e", f"{env_key}={env_value}"]
            cmd.append(docker_cfg["image"])
            run_result = self._docker(*cmd[1:], timeout=180)  # 首次可能拉取镜像；cmd[0] 是 "docker"，_docker 会补
            if run_result.returncode != 0:
                return {"success": False, "message": (run_result.stderr or "docker run 失败").strip()[-300:]}

        # 健康等待是尽力而为：容器动作成功即 success，健康状态单独回报
        # （数据库首启初始化 / WAL 恢复可能超过等待窗口）
        deadline = asyncio.get_event_loop().time() + 45
        health = "unknown"
        while asyncio.get_event_loop().time() < deadline:
            results = await self.check_infrastructure_health()
            health = results.get(name, {}).get("status", "unknown")
            if health in ("healthy", "unhealthy"):
                break
            await asyncio.sleep(1.5)

        return {
            "success": True,
            "status": health,
            "container": cname,
            "message": (
                "容器已启动，健康检查通过"
                if health == "healthy"
                else f"容器已启动，健康检查为 {health}——可稍后在卡片上再次「检测」"
            ),
        }

    async def stop_infrastructure(self, name: str) -> Dict[str, Any]:
        """停止基础设施容器（保留容器，重启机器/手动 docker start 可恢复）。"""
        if not self.config:
            self.load_config()
        infra = self.config.infrastructure.get(name)
        if infra is None:
            return {"success": False, "message": f"基础设施未定义: {name}"}
        cname = f"{self.INFRA_CONTAINER_PREFIX}{name}"
        state = self._infra_container_state(name)
        if state is None:
            return {"success": False, "message": "容器不存在（该基础设施从未由 console 拉起）"}
        if state != "running":
            return {"success": True, "message": f"容器已是 {state} 状态"}
        result = self._docker("stop", "-t", "10", cname, timeout=60)
        if result.returncode != 0:
            return {"success": False, "message": (result.stderr or "docker stop 失败").strip()[-300:]}
        return {"success": True, "message": "容器已停止（保留，可再次启动）"}

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
            url = infra.url or self.config.redis.get("url", "redis://localhost:6379/0")
            client = make_redis_client(
                url,
                connect_timeout=HEALTH_CONNECT_TIMEOUT_SECONDS,
                socket_timeout=HEALTH_SOCKET_TIMEOUT_SECONDS,
            )
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
        """保存当前配置到文件（原子写）。

        旧实现 ``open(self.config_path, 'w')`` 先截断再写：ENOSPC / 配额 /
        进程中断留下半截 YAML，随后 ``load_config`` 失败、``/cluster/*``
        管理面全量 500（plaita#253）。现在先序列化到字符串，再同目录
        mkstemp + fsync + ``os.replace``；失败只清理临时文件并记 WARNING。
        """
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
            
            text = yaml.dump(config_data, allow_unicode=True, default_flow_style=False)
            atomic_write_text(self.config_path, text, default_mode=0o644)
            
            return True
        except Exception as e:
            _log.warning(
                "保存配置失败（原文件保留）: %s: %s",
                self.config_path, e, exc_info=True,
            )
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
    """获取全局服务管理器。

    只有配置成功载入才登记单例：旧实现在 ``load_config`` 抛异常后仍把
    ``_manager`` 留成半初始化实例，后续请求拿到 ``config is None`` 的坏
    单例，错误也从具名原因退化成 ``AttributeError``（plaita#253）。集群配置
    损坏时异常原样上抛，由 API 层转成带路径的 503；只有注册表自身不可用时
    才回退默认路径加载，并记 WARNING（降级必须可观测）。
    """
    global _manager
    if _manager is not None:
        return _manager

    manager = ServiceManager()

    # 尝试从集群注册表加载配置
    try:
        from .cluster_registry import get_cluster_registry
        registry = get_cluster_registry()
        active_cluster = registry.get_active_cluster()
    except Exception as e:
        _log.warning("集群注册表不可用，回退到默认配置路径加载: %s", e, exc_info=True)
        manager.load_config()
        _manager = manager
        return _manager

    try:
        if active_cluster:
            manager.load_config(active_cluster.config_path)
        else:
            manager.load_config()
    except Exception:
        # 不登记半初始化单例：下一次请求必须重新尝试并再次拿到同一具名
        # 错误，而不是复用一个 config=None 的实例。
        _log.warning(
            "服务管理器初始化失败（活动集群: %s）",
            active_cluster.id if active_cluster else None,
        )
        raise

    _manager = manager
    return _manager


def reset_service_manager():
    """重置服务管理器（用于集群切换时重新加载配置）。

    旧单例不能简单置空：已拉起的实例是 start_new_session 的独立进程（或
    docker 容器），reset 本身并不会停掉它们；若托管实例表与 launcher 句柄
    随旧单例一起丢弃，/cluster/instances 显示为空、/cluster/stop-all 遍历
    空表被 all({}) 翻成 success，旧集群 worker 从此失联（stop-then-start
    链路断裂，只能手工 ps/docker 收尸）。因此把实例表与两个 launcher 的
    句柄迁移给新单例，并返回迁移统计供日志与测试观测（#100）。
    """
    global _manager
    old = _manager
    _manager = None
    if old is None:
        return {"instances": 0, "processes": 0, "containers": 0}
    new = get_service_manager()
    new.instances.update(old.instances)
    new._process_launcher._processes.update(old._process_launcher._processes)
    new._docker_launcher._containers.update(old._docker_launcher._containers)
    counts = {
        "instances": len(old.instances),
        "processes": len(old._process_launcher._processes),
        "containers": len(old._docker_launcher._containers),
    }
    if any(counts.values()):
        _log.info("reset_service_manager: 迁移旧管理面托管实例到新单例 %s", counts)
    return counts

