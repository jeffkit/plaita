#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Plaita Flow Server - 基于FastAPI的事件驱动流程服务器
"""


def _require_server():
    """Raise ImportError with actionable message if server dependencies are missing."""
    missing = []
    try:
        import fastapi  # noqa: F401
    except ImportError:
        missing.append("fastapi")
    try:
        import uvicorn  # noqa: F401
    except ImportError:
        missing.append("uvicorn")
    try:
        import sqlalchemy  # noqa: F401
    except ImportError:
        missing.append("sqlalchemy")

    if missing:
        raise ImportError(
            f"Server dependencies not installed: {', '.join(missing)}. "
            "Install them with: pip install plaita[server]"
        )
