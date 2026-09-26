"""plaita.obs — 执行观测适配器（可选 extra：``pip install plaita[langfuse]``）。

当前提供 :class:`LangfuseCallback`：把 :class:`~plaita.core.callback.FlowCallback`
的 8 个生命周期钩子映射到 Langfuse（Python SDK **v4**，OTel 内核）的
trace / span / generation 模型：

- 流程 → trace（v4 中 trace 由根 span 隐式创建；trace 级 name/tags/metadata/
  session/user 经根 span 的 OTel 属性写入）
- ``on_node_start`` / ``on_node_end`` → 流程根 span 下的子 span；节点输出形如
  ``{"model", "usage", ...}``（llm 节点输出形状）时额外记一条 generation，
  token 用量自动归并到 run 级
- ``on_node_end`` 带 error/exception → span 标记 ERROR 后收口
- ``on_node_end`` 结果含 ``observations`` 列表（agentrun ``details=true`` 输出）
  → 逐条建 agent span 的子 observation（generation/span），打开 agent 内部
  循环的可见性
- ``on_flow_suspend`` / ``on_node_suspend`` → 立即 flush（挂起进程随时可能消失，
  观测数据不能丢）
- ``on_flow_end`` → 根 span 收口（output / level）+ flush

**trace id 解析链（Distributed）**：解析出"语义 id"后经
``Langfuse.create_trace_id(seed=语义id)`` **确定性派生**出 OTel 要求的 32 位
hex trace id——同一种子在任意进程派生同一 trace id。语义 id 依次取：

1. ``flow.global_context[trace_id_key]``（显式指定，最高优先）；
2. 绑定的 ``FlowExecution`` 状态里的运行时 ``$EXECUTION_ID``（经
   :meth:`bind_execution` 注入）。``$EXECUTION_ID`` 由运行时 fresh start 生成、
   随 checkpoint 持久化，resume 后另一进程读到同一值——**宿主只需在每次
   新建 FlowExecution 后调用 bind_execution**，同一条分布式流程的所有步骤
   天然落在同一条 trace 上（console ``_LocalTraceCallback`` 覆写
   ``$EXECUTION_ID`` 时排在它前面即可让派生 trace 与控制台执行实例对应）；
3. 都没有时按 ``flow_id-随机`` 现场生成（各进程各自成 trace，不做隐式关联）。

适配器自身按"观测旁路"约束构建：任何内部异常都吞掉并记 warning，
绝不影响流程执行（CallbackManager 本身也有一层兜底）。

**错误语义与内核对齐（重要）**：内核只在节点**成功**路径发
``on_node_end``；abort 策略的节点失败以 ``NodeExecutionError`` 直接穿透、
**不触发** ``on_flow_end``——此时 trace/span 保持 open，由宿主收尾
（Langfuse 侧 TTL 兜底）。span/trace 级 ERROR 标记经
``on_node_end(error=...)`` / ``on_flow_end(error=...)`` 契约保留，供
未来内核补发错误回调或自定义节点手动触发。
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from typing import Any, Dict, List, Optional

from plaita.core.callback import FlowCallback

logger = logging.getLogger("plaita.obs")

# Langfuse v4 trace 级 OTel 属性名（wire 契约，与 SDK
# LangfuseOtelSpanAttributes 的取值一致；SDK 侧也以这些字符串读回）
_TRACE_NAME = "langfuse.trace.name"
_TRACE_TAGS = "langfuse.trace.tags"
_TRACE_METADATA = "langfuse.trace.metadata"
_TRACE_SESSION_ID = "session.id"
_TRACE_USER_ID = "user.id"

__all__ = ["LangfuseCallback", "map_openai_usage"]


def map_openai_usage(usage: Any) -> Optional[Dict[str, int]]:
    """OpenAI / Anthropic / Agent CLI 用量形状 → Langfuse ``usage_details``。

    已知键原位翻译；``total`` 缺失时由 input+output 兜底；非 int 值丢弃；
    已是目标形状的原样保留；非 dict 返回 None。
    """
    if not isinstance(usage, dict):
        return None
    rename = {
        "prompt_tokens": "input",
        "completion_tokens": "output",
        "total_tokens": "total",
        "input_tokens": "input",
        "output_tokens": "output",
        "cache_read_input_tokens": "input_cached",
        "cache_creation_input_tokens": "input_cache_creation",
    }
    mapped: Dict[str, int] = {}
    for key, value in usage.items():
        canonical = rename.get(key, key)
        if canonical in ("input", "output", "total", "input_cached",
                         "input_cache_creation") and isinstance(value, int):
            mapped[canonical] = value
    if "total" not in mapped and "input" in mapped and "output" in mapped:
        mapped["total"] = mapped["input"] + mapped["output"]
    return mapped or None


class LangfuseCallback(FlowCallback):
    """把流程执行上报为 Langfuse trace/span/generation（SDK v4，OTel 内核）。

    Args:
        public_key / secret_key / host: Langfuse 凭据；缺省时交由 SDK 从
            ``LANGFUSE_PUBLIC_KEY`` / ``LANGFUSE_SECRET_KEY`` / ``LANGFUSE_HOST``
            环境变量解析。
        client: 预构造的 Langfuse client（测试 / 多流程共享 client 时注入）。
            注入后本类不再 import langfuse，也不做 extra 检查。
        client_kwargs: 透传给 ``langfuse.Langfuse(**kwargs)`` 的额外参数
            （release / environment / flush_interval 等）。
        trace_id_key / session_id_key / user_id_key: 读取运行身份的
            ``flow.global_context`` 键名。语义 trace id 缺失时依次回退到绑定
            execution 的 ``$EXECUTION_ID``、随机生成（跨进程契约见模块
            docstring）；session/user 缺失即不上报对应字段。
        tags: 追加到每条 trace 的标签（自动附带 ``flow:<flow_id>``）。
        max_content_chars: input/output/metadata 里字符串的截断长度，
            防止大 payload 拖垮采集侧。
    """

    def __init__(
        self,
        *,
        public_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        host: Optional[str] = None,
        client: Optional[Any] = None,
        client_kwargs: Optional[Dict[str, Any]] = None,
        trace_id_key: str = "langfuse_trace_id",
        session_id_key: str = "langfuse_session_id",
        user_id_key: str = "langfuse_user_id",
        tags: Optional[List[str]] = None,
        max_content_chars: int = 10_000,
    ) -> None:
        if client is None:
            client = self._build_client(public_key, secret_key, host, client_kwargs or {})
        self._client = client
        self._trace_id_key = trace_id_key
        self._session_id_key = session_id_key
        self._user_id_key = user_id_key
        self._tags = list(tags or [])
        self._max_chars = max_content_chars
        self._lock = threading.Lock()
        self._execution: Optional[Any] = None
        # 每次运行的观测状态（on_flow_start / bind_execution 重置）
        self._root: Optional[Any] = None
        # 节点 id → 未收口 span 栈（loop/子流程会重入同一 node.id，LIFO 配对）
        self._open_spans: Dict[str, List[Any]] = {}

    # ------------------------------------------------------------------ 构造

    def bind_execution(self, execution: Any) -> None:
        """绑定当前 ``FlowExecution``（语义 trace id 解析链第 2 级的数据来源）。

        常驻宿主（FlowWorker / console）为每次处理循环新建 execution 但复用
        同一回调实例——绑定新 execution 时重置 run 状态，防止上一个执行的
        trace/span 串到下一个。
        """
        self._execution = execution
        with self._lock:
            self._root = None
            self._open_spans = {}

    @staticmethod
    def _build_client(public_key, secret_key, host, client_kwargs):
        try:
            import langfuse
        except ImportError as exc:  # 可操作的 extra 报错，与 plaita 全仓口径一致
            raise ImportError(
                "The 'langfuse' extra is required for LangfuseCallback but is not "
                "installed. Install it with: pip install plaita[langfuse]\n"
                "Or install all extras: pip install plaita[all]"
            ) from exc
        kwargs: Dict[str, Any] = dict(client_kwargs)
        if public_key is not None:
            kwargs.setdefault("public_key", public_key)
        if secret_key is not None:
            kwargs.setdefault("secret_key", secret_key)
        if host is not None:
            kwargs.setdefault("host", host)
        return langfuse.Langfuse(**kwargs)

    # ---------------------------------------------------------- 内部工具

    def _clip(self, value: Any) -> Any:
        """递归截断字符串，防大 payload；其余类型原样透传（SDK 负责 JSON 化）。"""
        limit = self._max_chars
        if isinstance(value, str):
            return value if len(value) <= limit else value[:limit] + f"…[{len(value)} chars]"
        if isinstance(value, dict):
            return {k: self._clip(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._clip(v) for v in value]
        return value

    def _flow_meta(self, flow) -> Dict[str, Any]:
        meta: Dict[str, Any] = {"flow_id": flow.flow_id}
        expose = getattr(flow, "expose_env", None)
        if expose:
            meta["expose_env"] = list(expose)
        return meta

    def _global_of(self, flow, key: str) -> Optional[Any]:
        gc = getattr(flow, "global_context", None)
        if isinstance(gc, dict) and key in gc:
            return gc[key]
        return None

    def _execution_trace_id(self) -> Optional[str]:
        """从绑定的 execution 状态读运行时 ``$EXECUTION_ID``（解析链第 2 级）。"""
        execution = self._execution
        getter = getattr(execution, "get_state", None) if execution is not None else None
        if not callable(getter):
            return None
        try:
            prefix = getattr(execution, "express_prefix", "$")
            value = getter(f"{prefix}EXECUTION_ID")
        except Exception:  # noqa: BLE001 — 观测旁路
            logger.warning("读取 $EXECUTION_ID 失败", exc_info=True)
            return None
        return str(value) if value else None

    def _resolve_seed(self, flow) -> str:
        """语义 trace id（再经 create_trace_id(seed=) 确定性派生 OTel id）。"""
        return str(
            self._global_of(flow, self._trace_id_key)
            or self._execution_trace_id()
            or f"{flow.flow_id}-{uuid.uuid4().hex[:12]}"
        )

    def _trace_attrs(self, flow) -> Dict[str, Any]:
        """trace 级身份属性（name/tags/metadata/session/user）。"""
        attrs: Dict[str, Any] = {
            _TRACE_NAME: str(flow.flow_id),
            _TRACE_TAGS: self._tags + [f"flow:{flow.flow_id}"],
            _TRACE_METADATA: json.dumps(self._clip(self._flow_meta(flow)),
                                        ensure_ascii=False),
        }
        session_id = self._global_of(flow, self._session_id_key)
        user_id = self._global_of(flow, self._user_id_key)
        if session_id is not None:
            attrs[_TRACE_SESSION_ID] = str(session_id)
        if user_id is not None:
            attrs[_TRACE_USER_ID] = str(user_id)
        return attrs

    def _ensure_root(self, flow):
        """创建（或返回）流程虚拟根 span。

        v4 中以显式 ``trace_context`` 创建的 span 是**虚拟挂载点**（提供
        trace/span id 供子 span 挂靠，本身不导出）——因此 trace 级身份属性
        不能只写在它身上，须随每个真实导出的子 span 重复写入（见
        :meth:`on_node_start`），ingestion 侧任一 span 携带即可生效。
        """
        if self._root is not None:
            return self._root
        seed = self._resolve_seed(flow)
        otel_trace_id = self._client.create_trace_id(seed=seed)
        root = self._client.start_observation(
            trace_context={"trace_id": otel_trace_id},
            name=str(flow.flow_id),
            as_type="span",
            metadata=self._clip(self._flow_meta(flow)),
        )
        with self._lock:
            self._root = root
        return root

    def _pop_span(self, node_id: str):
        with self._lock:
            stack = self._open_spans.get(node_id)
            return stack.pop() if stack else None

    def _mark_error(self, observation, error, exception):
        message = str(exception) if exception is not None else str(error or "error")
        updater = getattr(observation, "update", None)
        if callable(updater):
            try:
                updater(level="ERROR", status_message=self._clip(message))
            except Exception:  # noqa: BLE001
                logger.warning("langfuse update(level=ERROR) failed", exc_info=True)

    def _flush(self):
        flusher = getattr(self._client, "flush", None)
        if callable(flusher):
            try:
                flusher()
            except Exception:  # noqa: BLE001
                logger.warning("langfuse flush failed", exc_info=True)

    # -------------------------------------------------------- 生命周期钩子

    def on_flow_start(self, flow, **kwargs) -> None:
        with self._lock:
            self._root = None
            self._open_spans = {}
        self._ensure_root(flow)

    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs) -> None:
        root = self._ensure_root(flow)
        try:
            fields: Dict[str, Any] = {
                "output": self._clip(result),
                "metadata": {"status": "error" if (error or exception) else "success"},
            }
            if error or exception:
                fields["level"] = "ERROR"
                fields["status_message"] = self._clip(
                    str(exception) if exception is not None else str(error))
            updater = getattr(root, "update", None)
            if callable(updater):
                updater(**fields)
        except Exception:  # noqa: BLE001
            logger.warning("langfuse root update failed", exc_info=True)
        # v4 的流程根是真 OTel span，必须 end 才会导出（v3 的 trace 是隐式的）
        ender = getattr(root, "end", None)
        if callable(ender):
            try:
                ender()
            except Exception:  # noqa: BLE001
                logger.warning("langfuse root end failed", exc_info=True)
        self._flush()
        with self._lock:
            self._root = None
            self._open_spans = {}

    def on_flow_suspend(self, flow, **kwargs) -> None:
        self._flush()

    def on_flow_resume(self, flow, **kwargs) -> None:
        # trace 惰性重建：跨进程 resume 后本实例是空状态，下一个节点事件
        # 会按解析链重新派生同一 trace id 并挂回。
        pass

    def on_node_start(self, flow, node, **kwargs) -> None:
        try:
            root = self._ensure_root(flow)
            span = root.start_observation(
                name=str(node.id),
                as_type="span",
                metadata=self._clip({"node_type": getattr(node, "node_type", None)}),
            )
            # trace 级身份随每个真实导出的子 span 重复写入（虚拟根不导出，
            # ingestion 从任一携带属性的 span 还原 trace 身份）。
            raw = getattr(span, "_otel_span", None)
            if raw is not None:
                try:
                    raw.set_attributes(self._trace_attrs(flow))
                except Exception:  # noqa: BLE001
                    logger.warning("写入 trace 级 OTel 属性失败", exc_info=True)
            with self._lock:
                self._open_spans.setdefault(str(node.id), []).append(span)
        except Exception:  # noqa: BLE001
            logger.warning("langfuse span start failed", exc_info=True)

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        span = self._pop_span(str(node.id))
        if span is None:
            return
        try:
            if error or exception:
                self._mark_error(span, error, exception)
            elif isinstance(result, dict):
                # agent 节点的内部事件（agentrun details 输出）：建子
                # observation——工具调用为 span、文本轮为 generation（无逐轮
                # usage，用量由节点聚合 generation 承载，不重复计数）
                for item in result.get("observations") or []:
                    if not isinstance(item, dict):
                        continue
                    try:
                        self._render_child_observation(span, item)
                    except Exception:  # noqa: BLE001
                        logger.warning("langfuse 子 observation 失败", exc_info=True)
                if "usage" in result and "model" in result:
                    # LLM 形状输出（llm/agentrun 节点契约）→ 聚合 generation
                    usage = map_openai_usage(result.get("usage"))
                    gen_kwargs: Dict[str, Any] = {
                        "name": f"{node.id}.generation",
                        "as_type": "generation",
                        "model": result.get("model"),
                        "metadata": self._clip(
                            {k: v for k, v in result.items()
                             if k not in ("text", "usage", "observations")}),
                    }
                    if usage is not None:
                        gen_kwargs["usage_details"] = usage
                    completion = result.get("text")
                    if completion is not None:
                        gen_kwargs["output"] = self._clip(completion)
                    generation = span.start_observation(**gen_kwargs)
                    ender = getattr(generation, "end", None)
                    if callable(ender):
                        ender()
                    output_setter = getattr(span, "update", None)
                    if callable(output_setter):
                        output_setter(output=self._clip(result))
                else:
                    updater = getattr(span, "update", None)
                    if callable(updater):
                        updater(output=self._clip(result))
            else:
                updater = getattr(span, "update", None)
                if callable(updater):
                    updater(output=self._clip(result))
            ender = getattr(span, "end", None)
            if callable(ender):
                ender()
        except Exception:  # noqa: BLE001
            logger.warning("langfuse span end failed", exc_info=True)

    def _render_child_observation(self, span, item: Dict[str, Any]) -> None:
        """把 agent 内部事件渲染为 agent span 的子 observation（立即收口）。"""
        name = str(item.get("name") or item.get("type") or "detail")[:200]
        kwargs: Dict[str, Any] = {
            "name": name,
            "as_type": "generation" if item.get("type") == "generation" else "span",
        }
        if item.get("type") == "generation" and item.get("model"):
            kwargs["model"] = item["model"]
        for src, dst in (("input", "input"), ("output", "output")):
            if item.get(src) is not None:
                kwargs[dst] = self._clip(item[src])
        metadata = {k: v for k, v in item.items()
                    if k not in ("type", "name", "model", "input", "output", "usage")}
        if metadata:
            kwargs["metadata"] = self._clip(metadata)
        child_usage = map_openai_usage(item.get("usage"))
        if child_usage is not None:
            kwargs["usage_details"] = child_usage
        child = span.start_observation(**kwargs)
        ender = getattr(child, "end", None)
        if callable(ender):
            ender()

    def on_node_suspend(self, flow, node, **kwargs) -> None:
        self._flush()

    def on_node_resume(self, flow, node, **kwargs) -> None:
        pass

    # -------------------------------------------------------------- 对外

    def finalize(self) -> None:
        """run 终态收尾：收口未结束的流程根 span + flush。

        distributed 模式内核不发 ``on_flow_end``（is_end 由宿主循环判定），
        流程根 span 无人收口——未结束的 span 不会被导出，其携带的 trace 级
        身份属性随之丢失。宿主在判定 run 终结（completed / failed）时必须
        调用本方法；挂起场景用 :meth:`flush`（root 保持 open，resume 续写）。
        幂等：root 已收口（normal 模式走了 on_flow_end）时只 flush。
        """
        with self._lock:
            root = self._root
            self._root = None
        if root is not None:
            ender = getattr(root, "end", None)
            if callable(ender):
                try:
                    ender()
                except Exception:  # noqa: BLE001
                    logger.warning("langfuse root end failed", exc_info=True)
        self._flush()

    def flush(self) -> None:
        """冲刷（挂起场景用；root 保持 open 供 resume 续写）。终态请用
        :meth:`finalize`。"""
        self._flush()
