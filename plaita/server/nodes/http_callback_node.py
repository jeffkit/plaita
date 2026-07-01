"""
HTTP回调节点实现
支持等待HTTP回调请求并触发流程继续执行
"""
import uuid
from typing import Any, ClassVar, Dict, List, Optional, Union
from pydantic import Field

from .base_extended_node import BaseExtendedNode
from ...logger import logger


class HttpCallbackNode(BaseExtendedNode):
    """
    HTTP回调节点
    等待HTTP回调请求，接收到回调时触发事件继续流程
    """
    
    node_type: ClassVar[str] = "http_callback"
    node_name: ClassVar[str] = "HTTP回调节点"
    
    # 回调URL配置
    callback_path: Optional[str] = Field(default=None, description="回调路径，为空时自动生成")
    callback_method: str = Field(default="POST", description="回调HTTP方法")
    callback_timeout_minutes: int = Field(default=60, description="回调超时时间（分钟）")
    
    # 安全配置
    require_auth: bool = Field(default=True, description="是否需要认证")
    auth_type: str = Field(default="token", description="认证类型: token, basic, signature")
    auth_token: Optional[str] = Field(default=None, description="认证令牌")
    
    # 请求验证配置
    validate_headers: Dict[str, str] = Field(default_factory=dict, description="需要验证的请求头")
    validate_params: Dict[str, str] = Field(default_factory=dict, description="需要验证的请求参数")
    
    # 响应配置
    success_response: Dict[str, Any] = Field(default_factory=lambda: {"status": "success"}, description="成功响应内容")
    error_response: Dict[str, Any] = Field(default_factory=lambda: {"status": "error"}, description="错误响应内容")
    
    def __init__(self, **data):
        super().__init__(**data)
        # HTTP回调节点默认监听http_callback事件
        self.event_type = "http_callback"
        
    def generate_service_config(self, execution) -> Dict[str, Any]:
        """
        生成HTTP回调服务配置
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: HTTP回调服务配置
        """
        # 生成或解析回调路径
        callback_path = self._generate_callback_path(execution)
        
        # 生成认证信息
        auth_config = self._generate_auth_config(execution)
        
        # 解析验证配置
        validation_config = self._resolve_validation_config(execution)
        
        config = {
            "type": "http_callback",
            "node_id": self.id,
            "execution_id": execution._get_execution_id() if hasattr(execution, '_get_execution_id') else None,
            "flow_id": execution._get_state(f"{execution.express_prefix}FLOW_ID", None) if hasattr(execution, '_get_state') else None,
            "event_type": self.event_type,
            "event_filter": self.event_filter,
            "callback_config": {
                "path": callback_path,
                "method": self.callback_method.upper(),
                "timeout_minutes": self.callback_timeout_minutes,
                "full_url": self._generate_full_callback_url(callback_path)
            },
            "auth_config": auth_config,
            "validation_config": validation_config,
            "response_config": {
                "success_response": self.success_response,
                "error_response": self.error_response
            },
            "retry_config": self.get_default_retry_config()
        }
        
        logger.info(f"HTTP回调节点 [{self.id}] 配置: 回调URL={config['callback_config']['full_url']}")
        
        return config
    
    def _generate_callback_path(self, execution) -> str:
        """
        生成回调路径
        
        Args:
            execution: 执行上下文
            
        Returns:
            str: 回调路径
        """
        if self.callback_path:
            # 使用指定的回调路径，支持变量引用
            return self._resolve_value(execution, self.callback_path)
        else:
            # 自动生成回调路径
            execution_id = execution._get_execution_id() if hasattr(execution, '_get_execution_id') else uuid.uuid4().hex
            flow_id = execution._get_state(f"{execution.express_prefix}FLOW_ID", "unknown") if hasattr(execution, '_get_state') else "unknown"
            return f"/callback/{flow_id}/{self.id}/{execution_id}"
    
    def _generate_auth_config(self, execution) -> Dict[str, Any]:
        """
        生成认证配置
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: 认证配置
        """
        if not self.require_auth:
            return {"enabled": False}
        
        auth_config = {
            "enabled": True,
            "type": self.auth_type
        }
        
        if self.auth_type == "token":
            # 生成或使用指定的令牌
            token = self._resolve_value(execution, self.auth_token) if self.auth_token else self._generate_auth_token()
            auth_config["token"] = token
        elif self.auth_type == "basic":
            # 基础认证配置
            auth_config["username"] = self._resolve_value(execution, self.auth_token)
            auth_config["password"] = self._generate_auth_token()
        elif self.auth_type == "signature":
            # 签名认证配置
            secret = self._resolve_value(execution, self.auth_token) if self.auth_token else self._generate_auth_token()
            auth_config["secret"] = secret
            auth_config["algorithm"] = "HMAC-SHA256"
        
        return auth_config
    
    def _generate_auth_token(self) -> str:
        """
        生成认证令牌
        
        Returns:
            str: 认证令牌
        """
        return uuid.uuid4().hex
    
    def _resolve_validation_config(self, execution) -> Dict[str, Any]:
        """
        解析验证配置
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: 验证配置
        """
        resolved_headers = {}
        for key, value in self.validate_headers.items():
            resolved_headers[key] = self._resolve_value(execution, value)
        
        resolved_params = {}
        for key, value in self.validate_params.items():
            resolved_params[key] = self._resolve_value(execution, value)
        
        return {
            "headers": resolved_headers,
            "params": resolved_params
        }
    
    def _generate_full_callback_url(self, callback_path: str) -> str:
        """
        生成完整的回调URL
        
        Args:
            callback_path: 回调路径
            
        Returns:
            str: 完整的回调URL
        """
        # 使用默认的基础URL，实际使用中应该从环境变量或配置文件获取
        base_url = "http://localhost:8080"
        return f"{base_url.rstrip('/')}{callback_path}"
    
    def _resolve_value(self, execution, value):
        """
        解析单个值，支持变量引用
        
        Args:
            execution: 执行上下文
            value: 要解析的值
            
        Returns:
            Any: 解析后的值
        """
        if isinstance(value, str) and value.startswith('$'):
            try:
                resolved = execution.evaluate(value)
                return resolved if resolved is not None else value
            except Exception as e:
                logger.warning(f"解析变量引用失败 {value}: {e}")
                return value
        
        return value
    
    def validate_service_config(self, config: Dict[str, Any]) -> bool:
        """
        验证HTTP回调服务配置
        
        Args:
            config: 服务配置
            
        Returns:
            bool: 配置是否有效
        """
        required_fields = ["callback_config", "auth_config", "validation_config", "node_id", "event_type"]
        for field in required_fields:
            if field not in config:
                logger.error(f"HTTP回调节点配置缺少必要字段: {field}")
                return False
        
        # 验证回调配置
        callback_config = config["callback_config"]
        if not callback_config.get("path") or not callback_config.get("method"):
            logger.error("回调路径和方法不能为空")
            return False
        
        if callback_config.get("method") not in ["GET", "POST", "PUT", "DELETE"]:
            logger.error("不支持的HTTP方法")
            return False
            
        return True
    
    def get_callback_url(self, execution) -> str:
        """
        获取回调URL（供外部使用）
        
        Args:
            execution: 执行上下文
            
        Returns:
            str: 回调URL
        """
        config = self.generate_service_config(execution)
        return config["callback_config"]["full_url"] 