"""plaita.obs — 执行观测适配器（可选 extra：``pip install plaita[langfuse]``）。

当前提供 :class:`LangfuseCallback`：把 :class:`~plaita.core.callback.FlowCallback`
的 8 个生命周期钩子映射到 Langfuse 的 trace / span / generation 模型：

- ``on_flow_start``  → 开 trace（含 input / metadata / tags）
- ``on_node_start`` / ``on_node_end`` → 开/收 span；节点输出形如
  ``{"model", "usage", ...}``（llm 节点输出形状）时额外记一条 generation，
  token 用量随 span 层级自动归并到 run 级
- ``on_node_end`` 带 error/exception → span 标记 ERROR 后收口
- ``on_flow_suspend`` / ``on_node_suspend`` → 立即 flush（挂起进程随时可能消失，
  观测数据不能丢）
- ``on_flow_end`` → trace 收口（output / level）+ flush

**跨进程 trace 续写契约（Distributed）**：trace id 只读自
``flow.global_context[trace_id_key]``；未设置时现场随机生成。global_context
在执行启动时拷贝进执行状态，回调读的是 **Flow 对象上的那份**——因此分布式
流程的每个进程在重建 Flow 时都必须把同一个 key 注入 global_context（与
dry_run 等全局变量同一通道，FlowWorker/console 从执行实例记录带出）。
没有这个 key 时每个进程各自成 trace，不做隐式关联。

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

import logging
import threading
import uuid
from typing import Any, Dict, List, Optional

from plaita.core.callback import FlowCallback

logger = logging.getLogger("plaita.obs")

__all__ = ["LangfuseCallback", "map_openai_usage"]


def map_openai_usage(usage: Any) -> Optional[Dict[str, Any]]:
    """OpenAI 用量形状 → Langfuse v3 用量形状（原位翻译已知键，未知键原样保留）。

    ``{"prompt_tokens", "completion_tokens", "total_tokens"}`` →
    ``{"input", "output", "total", "unit": "TOKENS"}``。
    非 dict 输入返回 None；已经是目标形状（含 input/output/total）的原样返回。
    """
    if not isinstance(usage, dict):
        return None
    rename = {
        "prompt_tokens": "input",
        "completion_tokens": "output",
        "total_tokens": "total",
    }
    mapped = {rename.get(k, k): v for k, v in usage.items()}
    if any(k in mapped for k in ("input", "output", "total")):
        mapped.setdefault("unit", "TOKENS")
        return mapped
    return usage


class LangfuseCallback(FlowCallback):
    """把流程执行上报为 Langfuse trace/span/generation。

    Args:
        public_key / secret_key / host: Langfuse 凭据；缺省时交由 SDK 从
            ``LANGFUSE_PUBLIC_KEY`` / ``LANGFUSE_SECRET_KEY`` / ``LANGFUSE_HOST``
            环境变量解析。
        client: 预构造的 Langfuse client（测试 / 多流程共享 client 时注入）。
            注入后本类不再 import langfuse，也不做 extra 检查。
        client_kwargs: 透传给 ``langfuse.Langfuse(**kwargs)`` 的额外参数
            （release / environment / timeout 等）。
        trace_id_key / session_id_key / user_id_key: 读取运行身份的
            ``flow.global_context`` 键名。trace_id 缺失时现场生成（跨进程
            契约见模块 docstring）；session/user 缺失即不上报对应字段。
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
        # 每次运行的观测状态（on_flow_start 重置）
        self._trace: Optional[Any] = None
        # 节点 id → 未收口 span 栈（loop/子流程会重入同一 node.id，LIFO 配对）
        self._open_spans: Dict[str, List[Any]] = {}

    # ------------------------------------------------------------------ 构造

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

    def _ensure_trace(self, flow):
        if self._trace is not None:
            return self._trace
        trace_id = self._global_of(flow, self._trace_id_key) or (
            f"{flow.flow_id}-{uuid.uuid4().hex[:12]}"
        )
        tags = self._tags + [f"flow:{flow.flow_id}"]
        kwargs: Dict[str, Any] = {
            "id": str(trace_id),
            "name": str(flow.flow_id),
            "tags": tags,
            "metadata": self._clip(self._flow_meta(flow)),
        }
        session_id = self._global_of(flow, self._session_id_key)
        user_id = self._global_of(flow, self._user_id_key)
        if session_id is not None:
            kwargs["session_id"] = str(session_id)
        if user_id is not None:
            kwargs["user_id"] = str(user_id)
        self._trace = self._client.trace(**kwargs)
        return self._trace

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
            self._trace = None
            self._open_spans = {}
        trace = self._ensure_trace(flow)
        try:
            trace.update(metadata={"status": "running"})
        except Exception:  # noqa: BLE001
            logger.warning("langfuse trace.update failed", exc_info=True)

    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs) -> None:
        trace = self._ensure_trace(flow)
        try:
            fields: Dict[str, Any] = {
                "output": self._clip(result),
                "metadata": {"status": "error" if (error or exception) else "success"},
            }
            if error or exception:
                fields["level"] = "ERROR"
                fields["status_message"] = self._clip(
                    str(exception) if exception is not None else str(error))
            trace.update(**fields)
        except Exception:  # noqa: BLE001
            logger.warning("langfuse trace.update failed", exc_info=True)
        self._flush()
        with self._lock:
            self._trace = None
            self._open_spans = {}

    def on_flow_suspend(self, flow, **kwargs) -> None:
        self._flush()

    def on_flow_resume(self, flow, **kwargs) -> None:
        # trace 惰性重建：跨进程 resume 后本实例是空状态，下一个节点事件
        # 会按 global_context 的 trace id 重新挂上同一条 trace。
        pass

    def on_node_start(self, flow, node, **kwargs) -> None:
        try:
            trace = self._ensure_trace(flow)
            span = trace.span(
                name=str(node.id),
                metadata=self._clip({"node_type": getattr(node, "node_type", None)}),
            )
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
            elif isinstance(result, dict) and "usage" in result and "model" in result:
                # LLM 形状输出（llm 节点契约 {text, model, usage, dry_run}）→ generation
                usage = map_openai_usage(result.get("usage"))
                gen_kwargs: Dict[str, Any] = {
                    "name": f"{node.id}.generation",
                    "model": result.get("model"),
                    "metadata": self._clip(
                        {k: v for k, v in result.items() if k not in ("text", "usage")}),
                }
                if usage is not None:
                    gen_kwargs["usage"] = usage
                completion = result.get("text")
                if completion is not None:
                    gen_kwargs["output"] = self._clip(completion)
                generation = span.generation(**gen_kwargs)
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
            ender = getattr(span, "end", None)
            if callable(ender):
                ender()
        except Exception:  # noqa: BLE001
            logger.warning("langfuse span end failed", exc_info=True)

    def on_node_suspend(self, flow, node, **kwargs) -> None:
        self._flush()

    def on_node_resume(self, flow, node, **kwargs) -> None:
        pass

    # -------------------------------------------------------------- 对外

    def flush(self) -> None:
        """手动冲刷（长驻进程周期性落盘可调用；suspend/end 已自动 flush）。"""
        self._flush()
