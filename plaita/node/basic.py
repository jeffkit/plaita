from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Dict, FrozenSet, Optional

from pydantic import BaseModel, Field, model_validator

from plaita.core.errors import ErrorHandler, RecoverableErrorHandler

if TYPE_CHECKING:
    # 节点应依赖窄接口 NodeExecutionContext，而非完整 FlowExecution facade。
    # FlowExecution 已实现该 Protocol；运行时仍传入 facade 实例。
    from plaita.core.node_context import NodeExecutionContext

_logger = logging.getLogger("plaita.node.schema")

# _known_input_keys 的按类缓存（model_fields 是反射结果，解析期反复调用）
_KNOWN_KEYS_CACHE: Dict[type, FrozenSet[str]] = {}


def warn_unknown_keys(cls, values):
    """Schema 卫生：原始 dict 里的未知键告警（不报错）。

    pydantic 对未声明字段默认 ``extra="ignore"``——JSON 流程里拼错的字段名
    （如 ``"conditon"``）会被静默吞掉，流程带着错误配置"成功"运行。0.5.0 的
    一贯策略是把沉默变可见：逐键对照 声明字段+alias+LEGACY_KEYS，未知即
    ``logger.warning``（带节点 id 与合法键提示），不改变解析结果。
    """
    if not isinstance(values, dict):
        return values
    known = _known_input_keys(cls)
    unknown = sorted(k for k in values if isinstance(k, str) and k not in known)
    if unknown:
        _logger.warning(
            "%s %r: unknown config keys %s will be IGNORED (possible typo?). "
            "Known keys: %s",
            cls.__name__, values.get("id", "?"), unknown, sorted(known),
        )
    return values


def _known_input_keys(cls) -> FrozenSet[str]:
    cached = _KNOWN_KEYS_CACHE.get(cls)
    if cached is None:
        keys = {"type"}
        try:
            for name, field in cls.model_fields.items():
                keys.add(name)
                if field.alias:
                    keys.add(field.alias)
        except Exception:  # pragma: no cover - 反射失败的极端场景退化为无告警
            return frozenset()
        keys |= set(getattr(cls, "LEGACY_KEYS", ()) or ())
        cached = frozenset(keys)
        _KNOWN_KEYS_CACHE[cls] = cached
    return cached


# 表达式语义字段：运行时接受任意值（$INPUT.x / $NODE.x 表达式或字面量，经
# execution.evaluate 求值），仅在 JSON Schema 层标注为 string，供编排控制台
# 渲染为带变量插入的表达式输入框。直接收紧类型注解会拒收存量 flow 里的
# 字面量 dict/list，故运行时保持 Any。
Expression = Annotated[Any, Field(json_schema_extra={"type": "string"})]


class NodeConfigException(RuntimeError):
    pass


class Node(BaseModel):
    # Class attributes (not included in the model's schema)
    node_type: ClassVar[str] = "ignore"
    node_name: ClassVar[str] = "ignore"
    branching: ClassVar[bool] = False
    async_node: ClassVar[bool] = False
    # 是否为"挂起型"节点: 执行后暂停流程, 等待外部 resume (如 EventNode)。
    # 内核 DistributedStrategy 据此走 suspend 分支并调 resume(), 不再 isinstance
    # 具体节点类型, 从而切断 core -> plaita.node.event_node 的反向依赖。
    is_suspending: ClassVar[bool] = False

    # validator 消费但非声明字段的遗留键（camelCase 别名等），unknown-key 告警的
    # 白名单由「声明字段 ∪ alias ∪ LEGACY_KEYS」构成；子类按需扩展。
    LEGACY_KEYS: ClassVar[FrozenSet[str]] = frozenset(
        {"timeoutHandler", "timeout_handler", "errorHandler", "error_handler"}
    )

    # Instance fields
    id: str = Field(..., description="Node identifier")
    name: Optional[str] = Field(None, description="Node name")
    desc: Optional[str] = Field(None, description="Node description")
    output: Optional[Any] = None
    next: Optional[Any] = None
    timeout: str = Field("", description="Timeout configuration")
    # 源码行号回标：仅 @flow 前端在编译期写入 IR，运行期错误可据此定位到
    # 用户书写的 Python 源码行。JSON/S-expr/Builder 前端不产生此字段，保持 None。
    source_line: Optional[int] = Field(None, description="@flow 编译期回标的源码行号")
    # input_type: Optional[Union[Property, List[Property]]] = None
    # output_type: Optional[Union[Property, List[Property]]] = None
    timeout_handler: ErrorHandler = Field(default_factory=lambda: ErrorHandler())
    error_handler: RecoverableErrorHandler = Field(default_factory=lambda: RecoverableErrorHandler())

    @model_validator(mode="before")
    @classmethod
    def _schema_hygiene(cls, values: Dict) -> Dict:
        return warn_unknown_keys(cls, values)

    @model_validator(mode="before")
    @classmethod
    def setup_error_handler(cls, values: Dict) -> Dict:
        timeout_handler_config = values.get("timeoutHandler") or values.get("timeout_handler")
        error_handler_config = values.get("errorHandler") or values.get("error_handler")

        if timeout_handler_config:
            values["timeout_handler"] = ErrorHandler(**timeout_handler_config)
        if error_handler_config:
            values["error_handler"] = RecoverableErrorHandler(**error_handler_config)
        return values

    def validate(self) -> None:
        """构造期校验节点配置。由 ``FlowBuilder.validate`` 调用，子类可覆写
        （如 ``Switch`` / ``HTTP``）以在流程构建阶段就拦下非法配置。"""
        return None

    def _validate_output(self, result: Any) -> None:
        """运行期校验节点输出。``run`` 会在 ``execute`` 后调用本方法，子类可
        覆写以校验产出格式。默认无操作。"""
        return None

    def run(self, execution: "NodeExecutionContext") -> Any:
        result = self.execute(execution)
        self._validate_output(result)
        return result

    def execute(self, execution: "NodeExecutionContext") -> Any:
        """执行节点并返回输出。

        ``execution`` 满足 ``NodeExecutionContext``（evaluate / state / child）。
        运行时传入的是 ``FlowExecution`` facade，它实现该 Protocol。
        """
        raise NotImplementedError()

    def resume(self, execution: "NodeExecutionContext", resume_type, resume_data=None) -> Any:
        """恢复一个此前挂起的节点 (仅 ``is_suspending`` 节点需要实现)。

        内核 ``DistributedStrategy._handle_resume`` 通过本方法多态分发 resume,
        避免直接 ``isinstance`` 具体挂起节点类型 (如 EventNode), 让 core 层
        不反向依赖 node 插件层。基类默认抛错, 挂起型节点覆写之。
        """
        raise NotImplementedError(
            f"Node {self.id} ({type(self).__name__}) is not a suspending node and "
            f"cannot be resumed; is_suspending={self.is_suspending}"
        )
