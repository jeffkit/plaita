#!/usr/bin/env python
"""
启动脚本 - 支持直接运行
用法: python run.py
"""
import sys
import os

# 添加 Plaita 模块到 Python 路径
plaita_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if plaita_root not in sys.path:
    sys.path.insert(0, plaita_root)

import uvicorn
from main import app, get_settings

def main():
    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )

if __name__ == "__main__":
    main()

