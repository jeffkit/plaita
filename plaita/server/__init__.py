#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Plaita Flow Server - 基于FastAPI的事件驱动流程服务器

server extra 的「缺依赖就给可操作报错」只有一份探针清单：
``plaita._EXTRAS_GUIDE["server"]``（``plaita/__init__.py``），由
``_check_extra_available("server")`` 在 ``from plaita import FlowWorker`` 等
懒导出入口消费。

#99：本文件里曾有一份零调用点的 ``_require_server()``，只探
fastapi/uvicorn/sqlalchemy——既没有任何调用点，清单也不对（``plaita/server/``
下模块级真正 import 的是 cachetools/redis）。它已删除；要加探针请改
``_EXTRAS_GUIDE``，不要在这里并列维护第二份清单。
"""
