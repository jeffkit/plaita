"""
服务控制模块
提供通过 Redis Pub/Sub 接收控制指令的功能
"""
import json
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from redis import Redis

from ..logger import logger


class ControlCommand:
    """控制指令"""
    
    # 指令类型
    STOP = "stop"
    STATUS = "status"
    RELOAD_CONFIG = "reload_config"
    
    def __init__(
        self,
        command: str,
        graceful: bool = True,
        timestamp: Optional[str] = None,
        **kwargs
    ):
        self.command = command
        self.graceful = graceful
        self.timestamp = timestamp or datetime.now().isoformat()
        self.extra = kwargs
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ControlCommand":
        """从字典创建"""
        return cls(
            command=data.get("command", ""),
            graceful=data.get("graceful", True),
            timestamp=data.get("timestamp"),
            **{k: v for k, v in data.items() if k not in ("command", "graceful", "timestamp")}
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> "ControlCommand":
        """从 JSON 字符串创建"""
        data = json.loads(json_str)
        return cls.from_dict(data)


class ControlListener:
    """
    控制指令监听器
    监听 Redis Pub/Sub 通道接收控制指令
    """
    
    # 控制通道前缀
    CONTROL_CHANNEL_PREFIX = "plaita:control"
    
    def __init__(
        self,
        redis_client: Redis,
        instance_id: str,
        on_stop: Optional[Callable[[bool], None]] = None,
        on_status: Optional[Callable[[], Dict[str, Any]]] = None,
        on_reload_config: Optional[Callable[[], bool]] = None
    ):
        """
        初始化控制监听器
        
        Args:
            redis_client: Redis 客户端
            instance_id: 实例 ID
            on_stop: 停止回调，参数为是否优雅停止
            on_status: 状态回调，返回状态信息
            on_reload_config: 重新加载配置回调
        """
        self.redis_client = redis_client
        self.instance_id = instance_id
        self.control_channel = f"{self.CONTROL_CHANNEL_PREFIX}:{instance_id}"
        
        # 回调函数
        self.on_stop = on_stop
        self.on_status = on_status
        self.on_reload_config = on_reload_config
        
        # 自定义命令处理器
        self._handlers: Dict[str, Callable[[ControlCommand], None]] = {}
        
        # 监听线程
        self._listener_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pubsub = None
    
    def register_handler(self, command: str, handler: Callable[[ControlCommand], None]):
        """
        注册自定义命令处理器
        
        Args:
            command: 命令类型
            handler: 处理函数
        """
        self._handlers[command] = handler
    
    def start(self):
        """启动监听"""
        if self._listener_thread and self._listener_thread.is_alive():
            logger.warning("控制监听器已在运行")
            return
        
        self._stop_event.clear()
        self._listener_thread = threading.Thread(
            target=self._listen_loop,
            daemon=True,
            name=f"control-listener-{self.instance_id}"
        )
        self._listener_thread.start()
        
        logger.info(f"控制监听器已启动: {self.control_channel}")
    
    def stop(self):
        """停止监听"""
        self._stop_event.set()
        
        if self._pubsub:
            try:
                self._pubsub.unsubscribe(self.control_channel)
                self._pubsub.close()
            except Exception as e:
                logger.warning(f"关闭 Pub/Sub 时出错: {e}")
        
        if self._listener_thread:
            self._listener_thread.join(timeout=5)
            self._listener_thread = None
        
        logger.info("控制监听器已停止")
    
    def _listen_loop(self):
        """监听循环"""
        try:
            self._pubsub = self.redis_client.pubsub()
            self._pubsub.subscribe(self.control_channel)
            
            logger.info(f"开始监听控制通道: {self.control_channel}")
            
            while not self._stop_event.is_set():
                message = self._pubsub.get_message(timeout=1.0)
                if message and message["type"] == "message":
                    self._handle_message(message["data"])
                    
        except Exception as e:
            logger.error("控制监听器出错: %s", e, exc_info=True)
    
    def _handle_message(self, data):
        """
        处理接收到的消息
        
        Args:
            data: 消息数据
        """
        try:
            # 解析命令
            if isinstance(data, bytes):
                data = data.decode()
            
            command = ControlCommand.from_json(data)
            logger.info(f"收到控制指令: {command.command}")
            
            # 处理命令
            if command.command == ControlCommand.STOP:
                self._handle_stop(command)
            elif command.command == ControlCommand.STATUS:
                self._handle_status(command)
            elif command.command == ControlCommand.RELOAD_CONFIG:
                self._handle_reload_config(command)
            elif command.command in self._handlers:
                self._handlers[command.command](command)
            else:
                logger.warning(f"未知的控制指令: {command.command}")
                
        except Exception as e:
            logger.error("处理控制指令失败: %s", e, exc_info=True)
    
    def _handle_stop(self, command: ControlCommand):
        """处理停止指令"""
        logger.info(f"收到停止指令，优雅停止: {command.graceful}")
        
        if self.on_stop:
            self.on_stop(command.graceful)
        else:
            logger.warning("未配置停止回调")
    
    def _handle_status(self, command: ControlCommand):
        """处理状态查询指令"""
        logger.info("收到状态查询指令")
        
        if self.on_status:
            status = self.on_status()
            # 可以通过另一个通道返回状态
            logger.info(f"当前状态: {status}")
        else:
            logger.warning("未配置状态回调")
    
    def _handle_reload_config(self, command: ControlCommand):
        """处理重新加载配置指令"""
        logger.info("收到重新加载配置指令")
        
        if self.on_reload_config:
            success = self.on_reload_config()
            logger.info(f"重新加载配置: {'成功' if success else '失败'}")
        else:
            logger.warning("未配置重新加载配置回调")


class ControlMixin:
    """
    控制功能混入类
    为服务类提供控制指令监听功能
    """
    
    _control_listener: Optional[ControlListener] = None
    
    def init_control(
        self,
        redis_client: Redis,
        instance_id: str
    ):
        """
        初始化控制监听
        
        Args:
            redis_client: Redis 客户端
            instance_id: 实例 ID
        """
        self._control_listener = ControlListener(
            redis_client=redis_client,
            instance_id=instance_id,
            on_stop=self._on_stop_command,
            on_status=self._on_status_command
        )
    
    def start_control_listener(self):
        """启动控制监听"""
        if self._control_listener:
            self._control_listener.start()
    
    def stop_control_listener(self):
        """停止控制监听"""
        if self._control_listener:
            self._control_listener.stop()
    
    def _on_stop_command(self, graceful: bool):
        """
        停止命令回调
        子类应该重写此方法
        """
        logger.info(f"收到停止命令，优雅停止: {graceful}")
        # 子类实现具体的停止逻辑
    
    def _on_status_command(self) -> Dict[str, Any]:
        """
        状态查询命令回调
        子类应该重写此方法
        """
        return {"status": "unknown"}

