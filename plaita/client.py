import hashlib
import hmac
import json
import logging
import uuid
from threading import Lock
from time import time
from typing import Any, Dict, Optional, Union
from urllib.parse import urlencode

import requests

from plaita.core.flow import Flow

# 获取logger
logger = logging.getLogger("plaita.client")

# Constants
DEFAULT_SIGNATURE_EXPIRATION = 3  # 3 seconds
DEFAULT_REDIS_TTL = 3600  # 1 hour cache TTL in Redis
# 默认指向本仓库 plaita-console 控制台提供的流程定义契约接口
# （POST /api/flowVersion/semver/detail，HMAC 鉴权）。可通过 url 参数覆盖。
DEFAULT_CONSOLE_URL = "http://localhost:8080/api/flowVersion/semver/detail"


def _get_config_key(flow_id, version):
    return f"flow:{flow_id}:{version}"


class RedisConfig:
    """Redis 配置类"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
        decode_responses: bool = True,
        **kwargs
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.decode_responses = decode_responses
        self.extra_options = kwargs
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        config = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "socket_timeout": self.socket_timeout,
            "socket_connect_timeout": self.socket_connect_timeout,
            "decode_responses": self.decode_responses,
        }
        if self.password:
            config["password"] = self.password
        config.update(self.extra_options)
        return config


class PlaitaClient:
    """
    Plaita 客户端，用于从远程服务获取流程定义并执行
    
    支持多级缓存：
    1. 内存缓存（最快）
    2. Redis缓存（可选，跨进程共享）
    3. 远程服务（最新数据）
    
    使用示例：
        # 基础用法
        client = PlaitaClient(secret_id='xxx', secret_key='yyy', url='https://your-plaita-server/api/flowVersion/semver/detail')
        result = client.run_flow('flow_id', '1.0.0', {"param": "value"})
        
        # 使用 Redis 缓存
        client = PlaitaClient(
            secret_id='xxx',
            secret_key='yyy',
            url='https://your-plaita-server/api/flowVersion/semver/detail',
            redis_config=RedisConfig(host='localhost', port=6379)
        )
        
        # 使用已有的 Redis 客户端
        import redis
        redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        client = PlaitaClient(
            secret_id='xxx',
            secret_key='yyy',
            url='https://your-plaita-server/api/flowVersion/semver/detail',
            redis_client=redis_client
        )
    """
    
    def __init__(
        self,
        secret_id: str,
        secret_key: str,
        url: str = DEFAULT_CONSOLE_URL,
        signature_validity: int = DEFAULT_SIGNATURE_EXPIRATION,
        headers: Optional[Dict[str, str]] = None,
        redis_client: Optional[Any] = None,
        redis_config: Optional[Union[RedisConfig, Dict[str, Any]]] = None,
        redis_ttl: int = DEFAULT_REDIS_TTL,
        replay_protected: bool = False,
    ):
        """
        初始化 PlaitaClient

        Args:
            secret_id: API 密钥 ID
            secret_key: API 密钥
            url: API 服务地址，默认指向本仓库 plaita-console 控制台的
                ``/api/flowVersion/semver/detail`` 契约接口（本地部署）。
                生产环境请通过该参数指向你部署的控制台地址。
            signature_validity: 签名有效期（秒）
            headers: 额外的请求头
            redis_client: 已有的 Redis 客户端实例（优先使用）
            redis_config: Redis 配置（当 redis_client 为 None 时使用）
            redis_ttl: Redis 缓存过期时间（秒）
            replay_protected: 是否启用重放保护 (2026-07 新增)。True 时每次请求
                生成 uuid nonce 并纳入签名, 阻止签名在 ``signature_validity`` 秒
                窗口内被重放。**需要服务端配套支持** (plaita-console >= 2026.07)。
                未升级的服务端不识别 nonce 字段, 会按旧算法验签失败。默认 False
                保持向后兼容。
        """
        self.secret_id = secret_id
        self.secret_key = secret_key
        self.signature_validity = signature_validity
        self.url = url
        self.headers = headers if headers is not None else {}
        self.redis_ttl = redis_ttl
        self.replay_protected = replay_protected
        self.memory_cache = {}
        self.memory_cache_lock = Lock()

        # 初始化 Redis 客户端
        self._redis_client = None
        self._redis_available = False

        if redis_client is not None:
            # 使用传入的 Redis 客户端
            self._redis_client = redis_client
            self._validate_redis_connection()
        elif redis_config is not None:
            # 根据配置创建 Redis 客户端
            self._init_redis_from_config(redis_config)

    def __repr__(self) -> str:
        # secret_key 是高敏凭证, 默认 repr 会把它打印到日志/调试器/traceback。
        # 历史上 PlaitaClient 实例的 repr 会泄漏完整 secret_key; 这里只暴露
        # secret_id 的前几位用于辨识, key 一律打码。
        masked_key = f"{self.secret_key[:2]}***{self.secret_key[-2:]}" if self.secret_key else "***"
        return (
            f"PlaitaClient(secret_id={self.secret_id!r}, "
            f"secret_key={masked_key!r}, url={self.url!r})"
        )

    __str__ = __repr__

    def _init_redis_from_config(self, config: Union[RedisConfig, Dict[str, Any]]) -> None:
        """
        根据配置初始化 Redis 客户端
        
        Args:
            config: Redis 配置对象或字典
        """
        try:
            import redis
        except ImportError:
            logger.warning("Redis 库未安装，Redis 缓存将不可用。请运行: pip install redis")
            return
        
        try:
            if isinstance(config, dict):
                config = RedisConfig(**config)
            
            self._redis_client = redis.Redis(**config.to_dict())
            self._validate_redis_connection()
        except Exception as e:
            logger.warning("初始化 Redis 客户端失败: %s", e)
            self._redis_client = None
            self._redis_available = False
    
    def _validate_redis_connection(self) -> bool:
        """
        验证 Redis 连接是否可用
        
        Returns:
            bool: 连接是否可用
        """
        if self._redis_client is None:
            self._redis_available = False
            return False
        
        try:
            self._redis_client.ping()
            self._redis_available = True
            logger.info("Redis 连接验证成功")
            return True
        except Exception as e:
            logger.warning("Redis 连接验证失败: %s", e)
            self._redis_available = False
            return False
    
    @property
    def redis_client(self) -> Optional[Any]:
        """获取 Redis 客户端（兼容旧代码）"""
        return self._redis_client if self._redis_available else None

    def run_flow(self, flow_id: str, version: str, input_data: Optional[Dict] = None):
        """
        获取并执行流程
        
        Args:
            flow_id: 流程 ID
            version: 流程版本
            input_data: 输入参数
            
        Returns:
            流程执行结果
        """
        flow = self.get_flow(flow_id, version)
        return flow.run(input_data)

    def get_flow(self, flow_id: str, version: str) -> Flow:
        """
        获取流程定义
        
        优先级：内存缓存 > Redis缓存 > 远程服务
        
        Args:
            flow_id: 流程 ID
            version: 流程版本
            
        Returns:
            Flow: 流程对象
        """
        cache_key = _get_config_key(flow_id, version)
        
        # 1. 优先从内存缓存获取
        with self.memory_cache_lock:
            cached_flow = self.memory_cache.get(cache_key)
            if cached_flow:
                logger.debug("从内存缓存获取流程: %s", cache_key)
                return cached_flow

        # 2. 尝试从 Redis 缓存获取
        if self._redis_available and self._redis_client:
            try:
                config_data = self._redis_client.get(cache_key)
                if config_data:
                    logger.debug("从 Redis 缓存获取流程: %s", cache_key)
                    flow_obj = Flow.model_validate_json(json.loads(config_data))
                    # 更新内存缓存
                    with self.memory_cache_lock:
                        self.memory_cache[cache_key] = flow_obj
                    return flow_obj
            except Exception as e:
                logger.warning("从 Redis 获取缓存失败: %s", e)

        # 3. 从远程服务获取
        logger.debug("从远程服务获取流程: %s", cache_key)
        flow_data = self._fetch_flow(flow_id, version)
        flow_obj = Flow.model_validate_json(flow_data)
        
        # 更新缓存
        self._update_cache(cache_key, flow_obj, flow_data)
        
        return flow_obj
    
    def _update_cache(self, cache_key: str, flow_obj: Flow, flow_data: Any) -> None:
        """
        更新缓存
        
        Args:
            cache_key: 缓存键
            flow_obj: 流程对象
            flow_data: 原始流程数据
        """
        # 更新内存缓存
        with self.memory_cache_lock:
            self.memory_cache[cache_key] = flow_obj
        
        # 更新 Redis 缓存
        if self._redis_available and self._redis_client:
            try:
                self._redis_client.set(
                    cache_key, 
                    json.dumps(flow_data),
                    ex=self.redis_ttl  # 设置过期时间
                )
                logger.debug("已更新 Redis 缓存: %s, TTL: %ss", cache_key, self.redis_ttl)
            except Exception as e:
                logger.warning("更新 Redis 缓存失败: %s", e)
    
    def clear_cache(self, flow_id: Optional[str] = None, version: Optional[str] = None) -> int:
        """
        清除缓存
        
        Args:
            flow_id: 流程 ID（可选，不指定则清除所有）
            version: 流程版本（可选）
            
        Returns:
            int: 清除的缓存项数量
        """
        cleared_count = 0
        
        if flow_id and version:
            # 清除特定版本的缓存
            cache_key = _get_config_key(flow_id, version)
            with self.memory_cache_lock:
                if cache_key in self.memory_cache:
                    del self.memory_cache[cache_key]
                    cleared_count += 1
            
            if self._redis_available and self._redis_client:
                try:
                    self._redis_client.delete(cache_key)
                except Exception as e:
                    logger.warning("清除 Redis 缓存失败: %s", e)
        else:
            # 清除所有缓存
            with self.memory_cache_lock:
                cleared_count = len(self.memory_cache)
                self.memory_cache.clear()
            
            logger.info("已清除 %s 个内存缓存项", cleared_count)
        
        return cleared_count

    def _fetch_flow(self, flow_id: str, version: str) -> Any:
        """
        从远程服务获取流程定义
        
        Args:
            flow_id: 流程 ID
            version: 流程版本
            
        Returns:
            解析后的流程定义数据
            
        Raises:
            Exception: 获取流程失败时抛出异常
        """
        data = {"flowId": flow_id, "version": version}
        nonce = str(uuid.uuid4()) if self.replay_protected else None
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": generate_signature(
                self.secret_key, self.secret_id, self.signature_validity, int(time()),
                nonce=nonce,
            ),
        }
        headers.update(self.headers)  # 合并额外的请求头
        
        try:
            response = requests.post(self.url, headers=headers, data=data, timeout=30)
        except requests.exceptions.Timeout:
            raise Exception(f"请求超时: flow_id={flow_id}, version={version}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"请求失败: {e}")
        
        if response.status_code != 200:
            raise Exception(f"获取流程配置失败, HTTP状态码: {response.status_code}")
        
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            raise Exception("响应数据不是有效的 JSON 格式")
        
        if response_data.get("code"):
            raise Exception(f"获取流程配置失败: {response_data.get('message', '未知错误')}")
        
        if response_data.get("data") is None:
            raise Exception("获取流程配置失败: 响应数据为空")

        flow_str = response_data["data"].get("flow")
        if not flow_str:
            raise Exception("获取流程配置失败: flow 字段为空")
        
        logger.debug("成功获取流程定义: flow_id=%s, version=%s", flow_id, version)
        return json.loads(flow_str)


def generate_signature(
    secret_key: str,
    secret_id: str,
    signature_validity: int,
    sign_time: int,
    nonce: Optional[str] = None,
) -> str:
    """
    生成 API 请求签名

    Args:
        secret_key: API 密钥
        secret_id: API 密钥 ID
        signature_validity: 签名有效期（秒）
        sign_time: 签名时间戳
        nonce: 可选, 重放保护 nonce。非 None 时会同时纳入签名计算并作为
            ``nonce`` 字段加进 Authorization。服务端检测到该字段后启用重放
            校验 (plaita-console >= 2026.07)。

    Returns:
        str: URL 编码的签名字符串
    """
    signature_validity = max(DEFAULT_SIGNATURE_EXPIRATION, signature_validity)

    sign_expire = sign_time + signature_validity
    key_time = f"{sign_time};{sign_expire}"

    key = hmac.new(secret_key.encode(), key_time.encode(), hashlib.sha256)
    key_string = key.hexdigest()

    if nonce is None:
        string_to_sign = f"{sign_time}\n"
    else:
        # nonce 进入签名材料: 即使 nonce 不被服务端校验, 它也已经把"同一个签名
        # 用在别的请求上"变得不可能 (改 nonce 即改 signature)。
        string_to_sign = f"{sign_time}\n{nonce}\n"
    signature_key = hmac.new(key_string.encode(), string_to_sign.encode(), hashlib.sha256)
    sign = signature_key.hexdigest()

    fields = {
        "secret-id": secret_id,
        "sign-time": str(sign_time),
        "key-time": key_time,
        "signature": sign,
    }
    if nonce is not None:
        fields["nonce"] = nonce
    return urlencode(fields)


# Example Usage
#
# client = PlaitaClient(
#  'your secret id',
#  'your secret key',
#  url='https://your-plaita-server/api/flowVersion/semver/detail',
# )
# result = client.run_flow('259', '0.0.2', {"age": 14})
# print(result)
