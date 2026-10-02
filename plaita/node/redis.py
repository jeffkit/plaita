import re
import urllib.parse
from typing import ClassVar, Optional

try:
    import redis
except ImportError as e:  # pragma: no cover - 取决于安装的 extras
    raise ImportError(
        "plaita.node.redis (RedisNode) requires the 'redis' package. "
        "Install it with: pip install plaita[redis]"
    ) from e
from pydantic import ConfigDict, Field

from .basic import Node


class RedisNode(Node):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    node_type: ClassVar[str] = "redis"
    node_name: ClassVar[str] = "redis缓存"

    target: str = Field(..., description="Redis connection URL")
    command: str = Field(..., description="Redis command to execute")
    arguments: str = Field(..., description="Redis command arguments")
    redis_client: Optional[redis.Redis] = None

    def validate(self):
        pass

    def execute(self, execution):
        redis_url = urllib.parse.urlparse(execution.evaluate(self.target))
        username = redis_url.username
        password = redis_url.password
        hostname = redis_url.hostname
        port = redis_url.port
        db = 0
        if len(redis_url.path) > 1:
            db = redis_url.path[1:]
        assert username is not None, "redis node target must have username"
        assert password is not None, "redis node target must have password"
        assert hostname is not None, "redis node target must have hostname"
        assert port is not None, "redis node target must have port"
        if not self.redis_client:
            # 连接/读写超时（2026-10 评审 C4）：不传时 redis-py 的
            # connect/read 均为无限阻塞——Redis 抖动会永久挂起整个 flow。
            # 节点模型（target/command/arguments）无超时透出点，按评审
            # 建议取安全默认值，不新增配置字段。
            self.redis_client = redis.Redis(
                host=hostname, port=port, db=db, username=username, password=password,
                socket_connect_timeout=5, socket_timeout=30, retry_on_timeout=True,
            )
        argument_list = re.split(r"\s+", execution.evaluate(self.arguments).strip())
        return self.redis_client.execute_command(execution.evaluate(self.command), *argument_list)
