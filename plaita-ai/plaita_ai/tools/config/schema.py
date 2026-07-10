"""扁平配置模型 — 无 Component/Instance，仅 ToolBundle + Resources。"""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class DatasourceResource(BaseModel):
    driver: str = "postgresql"
    url: str


class VectorStoreResource(BaseModel):
    provider: str
    collection: str
    embedding: Optional[str] = None
    url: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)


class McpServerResource(BaseModel):
    url: str
    transport: str = "sse"
    headers: Dict[str, str] = Field(default_factory=dict)


class Resources(BaseModel):
    """命名资源池（连接信息），不是动态类型系统。"""

    datasources: Dict[str, DatasourceResource] = Field(default_factory=dict)
    vectorstores: Dict[str, VectorStoreResource] = Field(default_factory=dict)
    mcp_servers: Dict[str, McpServerResource] = Field(default_factory=dict)


class ParamDefConfig(BaseModel):
    type: str = "string"
    required: bool = True
    default: Any = None
    description: str = ""


class _ToolConfigBase(BaseModel):
    name: str
    description: str = ""
    tags: List[str] = Field(default_factory=list)
    success_condition: Optional[str] = None
    error_message: str = "工具调用失败"
    params: Dict[str, ParamDefConfig] = Field(default_factory=dict)


class HttpToolConfig(_ToolConfigBase):
    type: Literal["http"]
    url: str
    method: str = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    timeout: float = 10.0
    response_path: Optional[str] = None
    content_type: str = "application/json"
    addressing: Optional[str] = None


class NativeToolConfig(_ToolConfigBase):
    type: Literal["native"]
    module: str
    function: str


class SqlToolConfig(_ToolConfigBase):
    type: Literal["sql"]
    sql: str
    datasource: Optional[str] = None
    url: Optional[str] = None
    row_limit: int = 100


class VectorToolConfig(_ToolConfigBase):
    type: Literal["vector"]
    store: Optional[str] = None
    search_type: str = "similarity"
    k: int = 4
    filter: Optional[Dict[str, Any]] = None


ToolConfig = Annotated[
    Union[HttpToolConfig, NativeToolConfig, SqlToolConfig, VectorToolConfig],
    Field(discriminator="type"),
]


class ToolBundle(BaseModel):
    version: str = "1"
    tools: List[ToolConfig]  # type: ignore[valid-type]
