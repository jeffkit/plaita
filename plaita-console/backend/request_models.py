"""控制面请求体模型基类（A2R24-10）。

请求模型的未知字段一律 422，而不是 Pydantic 默认的 ``extra="ignore"``：
控制面请求里拼错 ``params`` / ``base_updated_at`` 时，静默丢参或绕过乐观锁
比显式拒绝更危险——调用方会以为参数/并发检查生效了。

响应模型不受影响（保持 ignore，以容忍老客户端/新增字段）。

例外：``DispatchOneRequest`` 显式 ``extra="ignore"``——``shadow`` 等服务端
权威字段不信任请求体（#262），传了也不生效；该行为是设计选择，见
``api/capacity.py`` 的模型注释。

不套用本基类的非模型请求面（不是遗漏，按原协议/契约保持）：
- ``POST /api/copilot`` 是 AG-UI 协议透传（``await request.json()``，未知
  字段随协议交给 recursive / AGUIAdapter，不属于控制面请求模型）；
- ``POST /api/flowVersion/semver/detail`` 是
  ``application/x-www-form-urlencoded`` 契约面，缺 ``flowId``/``version``
  时按业务码返回（``{code,message,data}``），不做模型级字段校验。
"""
from pydantic import BaseModel, ConfigDict


class StrictRequestModel(BaseModel):
    """请求体基类：未知字段 422（``additionalProperties: false``）。"""

    model_config = ConfigDict(extra="forbid")
