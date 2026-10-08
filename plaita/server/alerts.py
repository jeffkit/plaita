"""告警 webhook（#26）——死信/僵尸巡检等「必须叫醒值守」的事件出口。

只依赖标准库：事件进**有界**队列，由独立 daemon 线程串行 POST JSON——
发送绝不阻塞队列消费或巡检关键路径（webhook 端慢/挂不得反压 worker）。
队列满则丢弃并计数告警；发送失败只记 warning 并计数，绝不抛。

配置：``PLAITA_ALERT_WEBHOOK``（worker 启动时读取）；事件体示例::

    {"event": "dead_letter", "queue": "plaita:flow:queue:v2",
     "dlq_key": "...:dlq", "message_id": "1699-0", "reason": "max_deliveries=5",
     "delivery_count": 5, "ts": 1699999999.5}
"""
from __future__ import annotations

import json
import logging
import queue
import threading
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("plaita.server.alerts")

DEFAULT_WEBHOOK_TIMEOUT = 5.0
DEFAULT_WEBHOOK_QUEUE_SIZE = 100
_CLOSE = object()

# 注入点：默认用 urllib；测试传自己的 post 以免真发网络请求。
# 签名 (url, body: bytes, headers: dict, timeout: float) -> None，失败即抛。
PostFn = Callable[[str, bytes, Dict[str, str], float], None]


def _post_urllib(url: str, body: bytes, headers: Dict[str, str], timeout: float) -> None:
    from urllib.request import Request, urlopen

    request = Request(url, data=body, headers=headers, method="POST")
    with urlopen(request, timeout=timeout):  # noqa: S310 - URL 由运营者显式配置
        pass


class WebhookAlerter:
    """有界队列 + 后台线程的 best-effort JSON webhook 发送器。"""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = DEFAULT_WEBHOOK_TIMEOUT,
        queue_size: int = DEFAULT_WEBHOOK_QUEUE_SIZE,
        headers: Optional[Dict[str, str]] = None,
        post: Optional[PostFn] = None,
    ) -> None:
        self.url = url
        self._timeout = float(timeout)
        self._headers = {"Content-Type": "application/json", **(headers or {})}
        self._post = post or _post_urllib
        self._queue: "queue.Queue" = queue.Queue(maxsize=max(1, int(queue_size)))
        self._sent = 0
        self._failed = 0
        self._dropped = 0
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._pending = 0
        self._thread: Optional[threading.Thread] = None

    def _ensure_thread(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run, name="plaita-alert-webhook", daemon=True
            )
            self._thread.start()

    def send(self, event: Dict[str, Any]) -> bool:
        """入队一条告警（非阻塞）。队列满返回 False 并计数。"""
        self._ensure_thread()
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 100 == 0:
                logger.warning(
                    "告警 webhook 队列已满，事件丢弃 (total=%d, url=%s)",
                    self._dropped,
                    self.url,
                )
            return False
        with self._cond:
            self._pending += 1
        return True

    def flush(self, timeout: float = 5.0) -> bool:
        """等待队列清空（脚本退出前调用）；超时返回 False。"""
        with self._cond:
            if self._pending > 0:
                self._cond.wait_for(lambda: self._pending == 0, timeout=timeout)
            return self._pending == 0

    def close(self, timeout: float = 5.0) -> None:
        """冲刷在途事件并等待后台线程收尾（幂等）。"""
        self.flush(timeout)
        thread = self._thread
        if thread is None:
            return
        # 队列满（flush 超时）时退出信号可能塞不进去——不阻塞关闭路径，
        # daemon 线程随进程退出即可。
        try:
            self._queue.put(_CLOSE, timeout=timeout)
        except queue.Full:
            logger.debug("告警 webhook 关闭信号入队超时（线程为 daemon，随进程退出）")
            return
        thread.join(timeout=timeout)
        if not thread.is_alive():
            self._thread = None

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "url": self.url,
                "sent": self._sent,
                "failed": self._failed,
                "dropped": self._dropped,
                "pending": self._pending,
            }

    def _run(self) -> None:
        while True:
            event = self._queue.get()
            if event is _CLOSE:
                self._queue.task_done()
                return
            try:
                body = json.dumps(event, ensure_ascii=False).encode("utf-8")
                try:
                    self._post(self.url, body, self._headers, self._timeout)
                    self._sent += 1
                except Exception as exc:  # noqa: BLE001 - 告警失败不得影响主流程
                    self._failed += 1
                    logger.warning("告警 webhook 发送失败 url=%s: %s", self.url, exc)
            finally:
                with self._cond:
                    self._pending -= 1
                    if self._pending == 0:
                        self._cond.notify_all()
                self._queue.task_done()
