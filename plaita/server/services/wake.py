"""唤醒面共享消费骨架（plaita 队列消费服务合并，2026-10-25）。

**实况勘定**（本单调研结论，防过度合并）：五个外延服务里真正共享「list 队列
消费循环」的只有 **DelayService**（list→ZSET 排程 + 到期提交 + 重试/死信）与
**ApprovalService**（list 直消费 + 处理完出队）；http_callback 是进程内 HTTP
认领、redis_queue/kafka 是替单次执行长驻外部监听——它们与队列消费无关，共享
的只有 :class:`BaseExtendedService`（注册/控制/线程池/resume 发布），不在本
骨架范围。event_filter 是事件总线侧正确性守卫，同样不进。

**2026-10-26 修订（递送线补桥）**：上段勘定把「任务递送」与「任务体」混为
一谈——http_callback/redis_queue/kafka 的**任务递送**同样是
``plaita:{subtype}:queue`` LIST 轮询（worker 挂起时 RPUSH），此前面上全仓
无消费者＝挂起执行睡死。三家已接入本骨架承接递送（``_wake_queue_key`` →
LIST → ``handle_task``）；「机制差异」收窄到**任务体**（注册回调记录 / 长驻
外部监听——长监听占本服务线程池一坑，停机由监听循环的 shutdown 检查退出、
条目保留待下次启动重读）。kafka 仍不进默认白名单（外部 broker 依赖）。

本模块收敛 Delay/Approval 两套实现逐行互抄的六件共享原语（C4-2/#35 与 #56
两案的历史合流）：

1. 周期轮询壳：单轮失败隔离（不杀线程）+ 停机事件及时唤醒（不用裸 sleep）；
2. ``poll_interval`` 正数校验（0/负值会忙轮询打满核，r39 A7，loud fail）；
3. in-flight 去重（进程内：已提交未出队的条目不重复提交）；
4. 线程池任务壳：独立 event loop + active_tasks 记账 + 异常隔离；
5. **触发确认分流**（#35 语义，两服务共用）：``ResumePublishError`` = 触发未
   确认 → ``on_rejected``（条目保留重试）；其余完成 → ``on_confirmed``（出队）；
   停机中断 → 条目保留、仅清 in-flight（下次启动重读）；
6. 解析失败就地出队（坏条目不无限重读）。

**合并进程**：``python -m plaita.server.services wake --sources delay,approval``
——一个守护进程托管多个唤醒 source（各 source 独立消费线程/注册身份，互不
牵连）；旧服务名（``delay_service`` / ``approval_service``）保留为单 source
等价运行，存量部署零破坏。这是台阶契约 ``managed:queue`` 的执行实体：
「队列唤醒服务在不在场」从查多个心跳收敛为一个进程。

**刻意不做**：跨面合并（唤醒面 × 调度面）——delay 动「在跑的执行」、
schedule 动「没开始的执行」，数据模型（临时任务 vs 用户 cron 资源）与故障域
（在途执行失醒 vs 该跑没跑）都不同，合一个进程省一个 PID 赔两套语义。
"""
from __future__ import annotations

import asyncio
import json
import math
import threading
from typing import Any, Callable, Dict, List, Optional

from .base_service import ResumePublishError
from ...logger import logger
from ...core.url_redaction import _scrub_exception_text

#: 消费轮询周期（秒）；service_config.poll_interval 可覆盖。
WAKE_POLL_INTERVAL_SECONDS = 0.5
#: 每周期读取/提交的最大条数（防单周期阻塞过久）。
WAKE_CONSUME_BATCH = 50


def positive_poll_interval(value: Any, *, service: str = "wake") -> float:
    """``poll_interval`` 必须为有限正数；非正值具名拒绝（r39 A7）。

    0/负值传给 ``threading.Event.wait(timeout=...)`` 会立即返回：消费线程
    退化成忙轮询（吃满一个核并持续打 Redis）。不在构造期静默钳到某个下限
    ——配置写错应当 loud fail。
    """
    try:
        interval = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{service} poll_interval must be a positive number of seconds "
            f"(got {value!r})"
        ) from exc
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError(
            f"{service} poll_interval must be a positive number of seconds "
            f"(got {value!r}); 0/negative values would busy-poll"
        )
    return interval


def decode_wake_raw(raw: Any) -> Optional[str]:
    """队列条目 bytes→str；其余类型原样返回（调用方按 str 处理）。"""
    if isinstance(raw, bytes):
        return raw.decode()
    return raw


class WakeConsumeMixin:
    """list 队列消费骨架（混入 :class:`BaseExtendedService` 子类）。

    宿主须提供（BaseExtendedService 均有）：``thread_pool`` / ``active_tasks``
    / ``_shutdown_event`` / ``is_shutdown_requested()`` / ``_generate_task_id``
    / ``_handle_task_error`` / ``handle_task`` / ``_redis_client``（lrange/lrem）。
    子类钩子见各方法 docstring。
    """

    # ---- 初始化（子类 __init__ 调用一次） ----

    def _init_wake_consume(self, service_config: Optional[Dict[str, Any]] = None) -> None:
        cfg = service_config or {}
        self._poll_interval = positive_poll_interval(
            cfg.get("poll_interval", WAKE_POLL_INTERVAL_SECONDS),
            service=self.get_service_type(),
        )
        self._wake_batch = max(1, int(cfg.get("consume_batch", WAKE_CONSUME_BATCH)))
        self._consumer_thread: Optional[threading.Thread] = None
        # in-flight 去重（进程内）：同一队列条目在处理期间不重复提交。
        self._inflight: set = set()
        self._inflight_lock = threading.Lock()

    # ---- 钩子（子类按需覆写） ----

    def _wake_queue_key(self) -> str:
        """消费的 list 键。"""
        raise NotImplementedError

    def _parse_wake_task(self, raw: str) -> Optional[Dict[str, Any]]:
        """条目 → 任务配置；返回 None = 坏条目（调用方就地出队，不无限重读）。"""
        try:
            task_config = json.loads(raw)
        except json.JSONDecodeError:
            logger.error("%s 任务配置非法（丢弃）: %r", self.get_service_type(), raw[:200])
            return None
        if not isinstance(task_config, dict) or not self.validate_task_config(task_config):
            logger.error("%s 任务配置校验失败（丢弃）: %r", self.get_service_type(), raw[:200])
            return None
        return task_config

    def _pre_submit_check(self, task_config: Dict[str, Any]) -> bool:
        """提交前检查（如 approval 的重复投递存在性检查）；False = 已处理过，
        直接出队不重复执行。默认放行。"""
        return True

    def _ack_wake_task(self, raw: str) -> None:
        """确认完成：出队 + 清 in-flight。LREM 失败只导致下轮重复消费一次
        （at-least-once；delay 覆写为 ZREM finalize）。"""
        try:
            self._redis_client.lrem(self._wake_queue_key(), 1, raw)
        except Exception as e:  # noqa: BLE001 — 出队失败下轮重试
            logger.error("%s 任务出队失败: %s", self.get_service_type(), _scrub_exception_text(e))
        with self._inflight_lock:
            self._inflight.discard(raw)

    def _on_wake_rejected(self, raw: str) -> None:
        """触发未确认（ResumePublishError）：条目**保留**在队列（不出队），
        清 in-flight 等下轮重试——宁可重复触发（事件侧 EventFilter SET NX 去重
        收敛），不可静默丢失（#35）。"""
        logger.error(
            "%s 任务触发发布失败（条目保留待重试）: %.200s", self.get_service_type(), raw
        )
        with self._inflight_lock:
            self._inflight.discard(raw)

    # ---- 共享原语 ----

    def _wake_inflight_try_add(self, raw: str) -> bool:
        with self._inflight_lock:
            if raw in self._inflight:
                return False
            self._inflight.add(raw)
            return True

    def _wake_wait_tick(self) -> None:
        """轮询间隔等待：用停机事件而非裸 sleep，停机即刻唤醒消费线程。"""
        self._shutdown_event.wait(timeout=self._poll_interval)

    def _run_wake_task(
        self,
        raw: str,
        task_config: Dict[str, Any],
        *,
        on_confirmed: Callable[[str], None],
        on_rejected: Callable[[str], None],
    ) -> None:
        """线程池任务壳（共享）：独立 event loop 跑 ``handle_task`` + active_tasks
        记账 + 触发确认分流（原语 4/5）。

        - 正常完成（含业务 False 返回——已触发错误事件兜底）→ ``on_confirmed``；
        - :class:`ResumePublishError` = 触发未确认 → ``on_rejected``（#35：不出队，
          保留重试；不广播 service_error——任务未终态失败，重试成功还会 resume，
          每次抖动广播会把上游错误处理打成双跑）；
        - 其余异常 → ``_handle_task_error`` + ``on_confirmed``（既有语义：处理过
          但业务失败的任务按完成出队）；
        - 停机中断 → 条目保留，仅清 in-flight（下次启动由消费线程重读）。
        """
        task_id = self._generate_task_id(task_config)
        self.active_tasks.add(task_id)
        trigger_failed = False
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self.handle_task(task_config))
            finally:
                try:
                    loop.close()
                except Exception as exc:
                    logger.warning(
                        "event loop close failed during task cleanup: %s",
                        _scrub_exception_text(exc),
                    )
            logger.info("%s 任务 %s 执行完成: %s", self.get_service_type(), task_id, result)
        except ResumePublishError as e:
            logger.error(
                "%s 任务 %s 触发发布失败（保留条目待重试）: %s",
                self.get_service_type(), task_id, _scrub_exception_text(e),
            )
            trigger_failed = True
        except Exception as e:  # noqa: BLE001 — 单任务失败不杀消费线程
            logger.error(
                "%s 任务 %s 执行失败: %s",
                self.get_service_type(), task_id, _scrub_exception_text(e),
            )
            self._handle_task_error(task_config, e)
        finally:
            self.active_tasks.discard(task_id)
            if self.is_shutdown_requested():
                with self._inflight_lock:
                    self._inflight.discard(raw)
            elif trigger_failed:
                on_rejected(raw)
            else:
                on_confirmed(raw)

    def _consume_wake_batch(self) -> int:
        """单轮消费：LRANGE 批读 → 解析 → in-flight 去重 → 线程池提交。

        不用 BLPOP：出队即内存持有、崩溃即丢（#11/#56 实证）；「处理走完才
        出队」由 ``_run_wake_task`` 的确认分流保证。返回本轮提交数（测试/观测用）。
        """
        raw_items = self._redis_client.lrange(self._wake_queue_key(), 0, self._wake_batch - 1)
        submitted = 0
        for raw in raw_items or []:
            raw = decode_wake_raw(raw)
            task_config = self._parse_wake_task(raw)
            if task_config is None:
                self._ack_wake_task(raw)
                continue
            if not self._pre_submit_check(task_config):
                self._ack_wake_task(raw)
                continue
            if not self._wake_inflight_try_add(raw):
                continue
            self.thread_pool.submit(self._run_wake_task, raw, task_config,
                                    on_confirmed=self._ack_wake_task,
                                    on_rejected=self._on_wake_rejected)
            submitted += 1
        return submitted

    def _consume_wake_loop(self) -> None:
        """消费循环壳（原语 1）：周期轮询，单轮失败隔离，停机及时退出。

        退出条件双看：``is_shutdown_requested()``（base.shutdown 路径）与
        ``is_running``（stop_service 直停路径，approval 旧循环语义——只设
        shutdown 事件的停机等不到直停的调用方）。
        """
        while not self.is_shutdown_requested() and getattr(self, "is_running", True):
            try:
                self._consume_wake_batch()
            except Exception as e:  # noqa: BLE001 — 单轮失败不杀消费线程
                logger.error(
                    "%s 队列轮询失败: %s", self.get_service_type(), _scrub_exception_text(e)
                )
            self._wake_wait_tick()

    def _start_wake_consumer(self) -> bool:
        """启动消费线程（redis list 原语缺一不可；无客户端只告警不失败——
        与 Delay/Approval 既有启动语义一致）。"""
        if (
            self._redis_client is None
            or not hasattr(self._redis_client, "lrange")
            or not hasattr(self._redis_client, "lrem")
        ):
            logger.warning(
                "%s 服务无 redis 客户端，队列消费不启动", self.get_service_type()
            )
            return True
        self._consumer_thread = threading.Thread(
            target=self._consume_wake_loop,
            name=f"{self.get_service_type()}-service-consumer",
            daemon=True,
        )
        self._consumer_thread.start()
        logger.info(
            "%s 服务消费线程已启动（队列: %s）", self.get_service_type(), self._wake_queue_key()
        )
        return True

    def _stop_wake_consumer(self, timeout: Optional[float] = None) -> None:
        """等待消费线程退出（停机窗口 = 轮询周期 + 1s；不留残余线程）。"""
        thread = self._consumer_thread
        if thread is not None:
            thread.join(timeout=timeout if timeout is not None else self._poll_interval + 1.0)
            if thread.is_alive():
                logger.warning(
                    "%s 队列消费线程未在停机窗口内退出: %s",
                    self.get_service_type(), self._wake_queue_key(),
                )
            self._consumer_thread = None


def wake_source_classes(sources: List[str]) -> List[type]:
    """source 名 → 服务类（统一守护进程用）；未知 source 具名拒绝。

    2026-10-25 第二步扩容 + 10-26 修订：http_callback 与 redis_queue 进
    值班室，且两家**已挂 WakeConsumeMixin 承接任务递送**（消费各自的
    ``plaita:{subtype}:queue``——10-25 的「不挂骨架」判断混淆了任务递送与
    任务体，递送面三家同形，任务体才是机制差异：回调注册快任务 / 长驻外部
    监听占线程池一坑）。redis_queue 构造签名 (event_bus, retry_config,
    redis_client) 兼容——无 service_config 形参，基类默认面即其全部配置。
    kafka 默认 disabled（cluster_config 未启用、broker 是外部依赖），不进
    默认白名单；单跑/显式点名时同样消费自己的任务 LIST。
    """
    from .delay_service import DelayService
    from .approval_service import ApprovalService
    from .http_callback_service import HttpCallbackService
    from .redis_queue_service import RedisQueueService

    known = {
        "delay": DelayService,
        "approval": ApprovalService,
        "http_callback": HttpCallbackService,
        "redis_queue": RedisQueueService,
    }
    unknown = [s for s in sources if s not in known]
    if unknown:
        raise ValueError(f"未知唤醒 source {unknown}（合法: {sorted(known)}）")
    return [known[s] for s in sources]
