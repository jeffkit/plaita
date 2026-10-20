"""控制面请求体模型基类（A2R24-10）。

请求模型的未知字段一律 422，而不是 Pydantic 默认的 ``extra="ignore"``：
控制面请求里拼错 ``params`` / ``base_updated_at`` 时，静默丢参或绕过乐观锁
比显式拒绝更危险——调用方会以为参数/并发检查生效了。

响应模型不受影响（保持 ignore，以容忍老客户端/新增字段）。

例外：``DispatchOneRequest`` 显式 ``extra="ignore"``——``shadow`` 等服务端
权威字段不信任请求体（#262），传了也不生效；该行为是设计选择，见
``api/capacity.py`` 的模型注释。
"""
from pydantic import BaseModel, ConfigDict


class StrictRequestModel(BaseModel):
    """请求体基类：未知字段 422（``additionalProperties: false``）。"""

    model_config = ConfigDict(extra="forbid")
