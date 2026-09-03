"""`python -m plaita_console`：pip 安装后的编排台启动入口。"""
import uvicorn

from .main import app, get_settings


def main() -> None:
    settings = get_settings()
    # 传 app 对象而非 "plaita_console.main:app" 字符串：
    # 字符串模式要求模块可被顶层 import（reload 场景），包内运行不可靠
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
