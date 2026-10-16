import re
import urllib.parse
from collections import OrderedDict
from typing import Any, ClassVar, Dict, Optional

try:
    import redis
except ImportError as e:  # pragma: no cover - 取决于安装的 extras
    raise ImportError(
        "plaita.node.redis (RedisNode) requires the 'redis' package. "
        "Install it with: pip install plaita[redis]"
    ) from e
from pydantic import ConfigDict, Field, PrivateAttr

from .basic import Node


class RedisNode(Node):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    node_type: ClassVar[str] = "redis"
    node_name: ClassVar[str] = "redis缓存"

    target: str = Field(..., description="Redis connection URL")
    command: str = Field(..., description="Redis command to execute")
    arguments: str = Field(..., description="Redis command arguments")
    # #81：**不再是 pydantic 字段**。原实现把 redis.Redis 实例做成字段 ⇒
    #   ① `model_dump(mode="json")` 直接抛 PydanticSerializationError（`_compute_flow_hash`
    #      用的就是它）⇒ 同一 Flow 对象第二次 start/resume 硬失败；
    #   ② 字段级缓存让 execute() 里**每次重新解析的 target 全被丢弃**（静默写错实例）。
    # 改为 **PrivateAttr + 按 target 的有界 LRU**：target 变化立即生效、同 target 复用
    # （per-execute 重建会把循环打回每轮建连——正是缓存想省的）。
    _clients: "OrderedDict[str, Any]" = PrivateAttr(default_factory=OrderedDict)
    #: 显式注入的 client（`RedisNode(redis_client=...)`）—— **注入点保留**，
    #: 它优先于按 target 建连；同样不进 pydantic 字段，故 model_dump 仍不可见。
    _injected: Any = PrivateAttr(default=None)

    #: LRU 上限：超出则淘汰最旧并关闭其连接
    CLIENT_CACHE_MAX: ClassVar[int] = 8

    def __init__(self, **data: Any):
        # 把 `redis_client=` 从 pydantic 入参里**摘出来**再交给 super()：
        # 既不成为字段（model_dump 干净），又保住既有的注入契约。
        injected = data.pop("redis_client", None)
        super().__init__(**data)
        if injected is not None:
            self._injected = injected

    def _client_for(self, url: str):
        """按 **target 字符串**取连接（有界 LRU）；有显式注入则优先用注入的那个。"""
        if self._injected is not None:
            return self._injected
        cached = self._clients.get(url)
        if cached is not None:
            self._clients.move_to_end(url)
            return cached
        u = urllib.parse.urlparse(url)
        username, password = u.username, u.password
        hostname, port = u.hostname, u.port
        # #81 附带修正：原 `db = u.path[1:]` 得到**字符串**（'1'）而非 int
        db = int(u.path[1:]) if len(u.path) > 1 else 0
        assert username is not None, "redis node target must have username"
        assert password is not None, "redis node target must have password"
        assert hostname is not None, "redis node target must have hostname"
        assert port is not None, "redis node target must have port"
        client = redis.Redis(
            host=hostname, port=port, db=db, username=username, password=password,
            socket_connect_timeout=5, socket_timeout=30, retry_on_timeout=True,
        )
        self._clients[url] = client
        while len(self._clients) > self.CLIENT_CACHE_MAX:
            _, evicted = self._clients.popitem(last=False)
            try:
                evicted.close()
            except Exception:  # noqa: BLE001
                pass
        return client

    def execute(self, execution):
        client = self._client_for(execution.evaluate(self.target))
        argument_list = re.split(r"\s+", execution.evaluate(self.arguments).strip())
        return client.execute_command(execution.evaluate(self.command), *argument_list)

    def validate(self):
        pass

