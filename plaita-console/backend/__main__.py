"""`python -m plaita_console`：pip 安装后的编排台启动入口。"""
import logging

import uvicorn

from .config import warn_if_public_bind
from .main import app, get_settings


def main() -> None:
    settings = get_settings()
    # #208：默认 127.0.0.1；显式对外监听时留一条启动 warning
    warn_if_public_bind(settings.host, logging.getLogger(__name__))
    # 传 app 对象而非 "plaita_console.main:app" 字符串：
    # 字符串模式要求模块可被顶层 import（reload 场景），包内运行不可靠
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
