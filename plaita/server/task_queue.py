"""Redis Stream task queue for FlowWorker / EventFilter (at-least-once + DLQ)."""
from __future__ import annotations

import json
import logging
import os
import socket
import time
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Optional, Union

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from plaita.core.url_redaction import _scrub_url_text, _redact_env_value

# #48：Redis 的**瞬时**故障家族。redis-py 8.x 里 `TimeoutError` 与 `ConnectionError`
# 是 `RedisError` 下**互不继承**的兄弟（实测 `issubclass(TimeoutError, ConnectionError)
# == False`），socket 层超时（`socket.timeout`，py3.10+ 即内建 `TimeoutError`）也不在
# 这两支之下 —— 所以「只吞 ConnectionError」的写法对超时类**天然失效**，读超时会从
# `read()` 直穿到 `run()`（无 except）→ 进程 `sys.exit(1)`。
# 瞬时故障 = 记 warning + 退避重试；**schema/权限类**仍按错误上抛（见 ensure_group）。
_TRANSIENT_REDIS_ERRORS = (RedisConnectionError, RedisTimeoutError, socket.timeout)

logger = logging.getLogger("plaita.server.task_queue")

DEFAULT_CONSUMER_GROUP = "plaita-workers"
PAYLOAD_FIELD = "payload"
DEFAULT_CLAIM_MIN_IDLE_MS = 60_000
DEFAULT_MAX_DELIVERIES = 5
DEFAULT_DLQ_SUFFIX = ":dlq"

# 消息信封版本：**语义**版本的唯一入口。字段只增不改语义时不 bump；
# 一旦改了同名字段的含义/必需性，必须 bump，让仍按旧语义解析的消费者
# 明确拒收（进 DLQ）而不是「猜着跑」。缺该字段 = v1（历史消息）。
TASK_SCHEMA_VERSION = 1
# 信封字段引入（f34266e，2026-10-07）之前的历史消息没有该字段，语义恒为 v1。
# 它与 TASK_SCHEMA_VERSION（本 build **写入口**的版本）必须分开：bump 后缺
# 字段仍按 v1 解析与继承，否则历史消息会被当成本代语义执行（A44-02）。
LEGACY_TASK_SCHEMA_VERSION = 1
SCHEMA_FIELD = "schema_version"
# C4-1：DLQ 保留条数。DLQ 是纯记录流（无消费者），不裁剪会无限增长；
# 每次入队后 XTRIM 保留最近 N 条。env 可调（PLAITA_DLQ_MAX_LEN）。
DEFAULT_DLQ_MAX_LEN = 1000


def _dlq_max_len_from_env() -> int:
    raw = (os.environ.get("PLAITA_DLQ_MAX_LEN") or "").strip()
    if not raw:
        return DEFAULT_DLQ_MAX_LEN
    try:
        return max(1, int(raw))
    except ValueError:
        logger.warning("PLAITA_DLQ_MAX_LEN=%s 非法，回退默认 %d", _redact_env_value(raw), DEFAULT_DLQ_MAX_LEN)
        return DEFAULT_DLQ_MAX_LEN
# redis 瞬断（DNS 解析失败/连接拒绝）后的重试退避：退避完返回 None 让主循环
# 继续轮询，Redis 恢复后下一次 read 自动重连恢复消费。模块常量便于测试归零。
RECONNECT_BACKOFF_SECONDS = 1.0
# 队列残留回收（#43）：单轮 sweep 扫描/删除的条目数上限。批量有界 ⇒ 单次
# Redis 往返有界（sweep 跑在消费线程里）；剩余残留下轮继续回收。
DEFAULT_RESIDUE_SWEEP_BATCH = 256


@dataclass(frozen=True)
class StreamTask:
    """One task read from the stream (pending until acked)."""

    message_id: str
    body: Dict[str, Any]
    delivery_count: int = 1
    # 消息信封版本（缺失/非法 → v1）。消费者据此拒收「比自己新」的语义
    schema_version: int = TASK_SCHEMA_VERSION
    # 本次死信决策是否来自「信封版本超出本 build 理解范围」的拒收（A49-01）。
    # 守卫据此**不得**克隆恢复副本：副本继承同一超版本，本 build 仍无法消费，
    # 每轮「克隆 → 再拒收 → 再克隆」会形成自持热环（流内恒 1 条、DLQ 线性增长）。
    schema_rejected: bool = False


# #186：畸形消息（payload 非 JSON / JSON 非对象）在**解析点**处置，不再抛给
# 消费循环。DLQ 信封里的原始 payload 截断到该字符预算——坏消息可能是任意大块
# 垃圾，不能原样放大进 DLQ（DLQ 条数上限只管条数，不管单条体积）。
INVALID_PAYLOAD_MAX_CHARS = 500


def _decode(value: Union[str, bytes]) -> str:
    return value.decode() if isinstance(value, bytes) else value


class InvalidTaskPayload(ValueError):
    """队列消息 payload 不可解析（非 JSON / 非对象）。

    解析点（``_task_from_fields``）抛出，由 ``read()`` 与 ``_reclaim_one()``
    就地转成 DLQ 信封并 ack（#186）。**不得穿出队列层**：``read()`` 位于
    FlowWorker 消费循环的内层 try 之外，漏出去就是一条坏消息打死 worker。
    保留 ``ValueError`` 基类只为兼容既有调用方的异常预期；队列内部一律按
    本类型精确处置。
    """

    def __init__(self, message_id: str, fields: Dict[Any, Any], reason: str):
        super().__init__(reason)
        self.message_id = message_id
        self.fields = fields
        self.reason = reason


def _schema_version_from_fields(fields: Dict[Any, Any]) -> int:
    """信封版本解析：缺失（历史消息）→ v1；非法值 → v1 + 告警（不因信封坏掉吞消息）。

    注意「缺失 = LEGACY_TASK_SCHEMA_VERSION(1)」而不是「= 当前写版本」：
    升级后仍可能读到信封字段引入前入队的历史消息，它们只能按 v1 语义执行。
    """
    raw = fields.get(SCHEMA_FIELD, fields.get(SCHEMA_FIELD.encode()))
    if raw is None:
        return LEGACY_TASK_SCHEMA_VERSION
    try:
        return int(_decode(raw))
    except (TypeError, ValueError):
        logger.warning(
            "任务信封 schema_version=%r 无法解析，按 v%d 处理",
            raw,
            LEGACY_TASK_SCHEMA_VERSION,
        )
        return LEGACY_TASK_SCHEMA_VERSION


def _payload_from_fields(fields: Dict[Any, Any]) -> str:
    for key in (PAYLOAD_FIELD, b"payload"):
        if key in fields:
            return _decode(fields[key])
    raise ValueError("missing payload field")


# DLQ 信封的 payload 覆盖哨兵：``None`` 是合法 payload（畸形消息可能是空串），
# 不能用 None 表示「未提供」。
_UNSET = object()


def enqueue_task(
    redis_client,
    stream_key: str,
    task: Dict[str, Any],
    *,
    schema_version: Optional[int] = None,
) -> str:
    """Append a start/resume task. Creates the stream if needed.

    信封带 ``schema_version``（与 payload 分离：payload 是领域消息，信封是
    传输契约）。旧消费者忽略未知字段；新消费者对「比自己新」的版本拒收。

    ``schema_version`` 缺省 = 本 build 的写版本（新生产消息）。**重投/交接
    副本必须传源消息的版本**（A44-02）：副本承载的是源消息的语义，盖当前
    build 的版本会让混跑池里的旧消费者直接拒收唯一恢复载体。
    """
    version = TASK_SCHEMA_VERSION if schema_version is None else int(schema_version)
    msg_id = redis_client.xadd(
        stream_key,
        {
            PAYLOAD_FIELD: json.dumps(task, ensure_ascii=False),
            SCHEMA_FIELD: str(version),
        },
    )
    return _decode(msg_id)


def dlq_stream_key(stream_key: str, suffix: str = DEFAULT_DLQ_SUFFIX) -> str:
    return f"{stream_key}{suffix}"


def _task_tenant_id(fields: Dict[Any, Any]) -> Optional[str]:
    """从 Stream 字段里的 ``payload`` 取显式租户 id（A50-05）。

    与派发口径不同：worker 对缺失 ``tenant_id`` 按 default 执行（fail-open），
    但**删除是破坏性操作**，只清能明确归属的条目。缺字段/空值返回 None，
    调用方必须跳过——历史消息/平台消息不能被 default 删除顺带清掉。
    payload 无法解析同样返回 None（跳过并告警）。
    """
    raw = None
    for key in (PAYLOAD_FIELD, PAYLOAD_FIELD.encode()):
        if key in fields:
            raw = fields[key]
            break
    if raw is None:
        return None
    try:
        body = json.loads(_decode(raw))
    except (TypeError, ValueError):
        return None
    if not isinstance(body, dict):
        return None
    tenant_id = body.get("tenant_id")
    if not isinstance(tenant_id, str) or not tenant_id:
        return None
    return tenant_id


def purge_tenant_tasks(
    redis_client,
    stream_key: str,
    tenant_id: str,
    *,
    batch_size: int = 256,
) -> int:
    """删除共享任务队列里归属 ``tenant_id`` 的 start/resume 消息（A50-05）。

    队列是多租户共用 Stream，消息信封只带 ``tenant_id``。租户删除后同名
    重建会把墓碑覆盖回 active，未消费的旧消息会重新执行并写进新租户——
    删除时必须按消息体收窄清理。匹配消息先在各消费组 ``XACK`` 再 ``XDEL``
    （只 XDEL 会在 PEL 留下 pending 幽灵）；payload 损坏/缺显式 tenant_id
    的条目跳过并 WARNING，绝不做无法归属的删除（default 与平台键共用
    Stream，删错即误伤）。返回删除条数。
    """
    if not tenant_id:
        return 0
    batch_size = max(1, int(batch_size))
    matched_ids: List[str] = []
    unattributed = 0
    start = "-"
    while True:
        entries = redis_client.xrange(
            stream_key, min=start, max="+", count=batch_size
        )
        if not entries:
            break
        for message_id, fields in entries:
            mid = _decode(message_id)
            owner = _task_tenant_id(fields)
            if owner is None:
                unattributed += 1
                continue
            if owner == tenant_id:
                matched_ids.append(mid)
        if len(entries) < batch_size:
            break
        start = f"({_decode(entries[-1][0])}"
    if unattributed:
        logger.warning(
            "任务队列清理（tenant=%s）：%d 条消息缺显式 tenant_id 或 payload 无法解析，"
            "跳过（无法归属，需人工确认）",
            tenant_id,
            unattributed,
        )
    if not matched_ids:
        return 0
    try:
        groups = redis_client.xinfo_groups(stream_key)
    except Exception as exc:  # noqa: BLE001 — 无法读组时仍须删条目
        logger.warning(
            "任务队列清理：读取消费组失败（%s），仅 XDEL 不 XACK（PEL 可能留 pending）",
            _scrub_url_text(exc),
        )
        groups = []
    for group in groups:
        name = group.get("name", group.get("name".encode()))
        if name is None:
            continue
        for offset in range(0, len(matched_ids), batch_size):
            redis_client.xack(
                stream_key, name, *matched_ids[offset:offset + batch_size]
            )
    deleted = 0
    for offset in range(0, len(matched_ids), batch_size):
        deleted += int(
            redis_client.xdel(stream_key, *matched_ids[offset:offset + batch_size])
        )
    return deleted


class RedisStreamTaskQueue:
    """Consumer-group queue with reclaim + dead-letter after max deliveries."""

    def __init__(
        self,
        redis_client,
        stream_key: str,
        *,
        group_name: str = DEFAULT_CONSUMER_GROUP,
        consumer_name: str = "worker-1",
        claim_min_idle_ms: int = DEFAULT_CLAIM_MIN_IDLE_MS,
        reclaim_window: int = 8,
        max_deliveries: int = DEFAULT_MAX_DELIVERIES,
        dlq_key: Optional[str] = None,
        dlq_max_len: Optional[int] = None,
        dead_letter_guard: Optional[Callable[[StreamTask], bool]] = None,
        reclaim_lease_guard: Optional[Callable[[Dict[str, Any]], bool]] = None,
        max_schema_version: Optional[int] = TASK_SCHEMA_VERSION,
        on_dead_letter: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        # 拒收「比自己新」的消息（进 DLQ）是**默认**行为：语义不兼容时宁可显式
        # 死信+告警，也不要按旧语义猜着跑。None 关闭校验（仅供测试/特殊场景）。
        self.max_schema_version = max_schema_version
        self.redis = redis_client
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.claim_min_idle_ms = claim_min_idle_ms
        # #196：reclaim 扫描游标（PEL 中上一轮扫到的 message id）。固定只看头 8
        # 条时，前 8 条都是活跃租约就会把窗口占死，第 9 条起的真孤儿永不被看到。
        # 每轮从游标处继续、到 PEL 尾部回绕；不新增 Redis 键，纯进程内状态。
        self._reclaim_cursor: Optional[str] = None
        # #196：扫描窗口大小（默认 8，仍有界；轮转保证不会饿死窗口外孤儿）。
        self.reclaim_window = max(1, int(reclaim_window))
        # ``max_deliveries`` 是**三个不同机制共同的上界**（2026-10-18 复核时发现，
        # 原先没有任何一处声明这一点）——调这个参数会同时改三件事：
        #   1. **正常重投**：消费了但未 ack 的消息被 reclaim 回来后重新投递；
        #   2. **#186 DLQ 重复**：``_dead_letter_invalid`` 的顺序是
        #      ``xadd`` → ``xtrim`` → ``ack``，三者非原子 ⇒ ``ack`` 失败或进程死在该区间时，
        #      消息仍留 PEL ⇒ 下次再 ``xadd`` 一次 ⇒ **同一 ``source_id`` 在 DLQ 出现多次**。
        #      重复次数有界，上界就是本参数（每次重放 ``delivery_count`` +1）。
        #      ⇒ 调大它会让**毒消息的 DLQ 副本更多**，可能挤掉 ``dlq_max_len`` 里的真实死信。
        #   3. **#189 定义重试**：``FlowDefinitionLoadError`` 不是 ``ValueError`` ⇒
        #      不被当畸形消息毒化（这是 #189 的修复点），但 worker 侧不特判它 ⇒
        #      异常上抛 ⇒ 消息未 ack、留 PEL ⇒ 由本参数兜住重试次数。
        #      ⇒ 定义一直读不到/读坏时，一条消息会以 ``claim_min_idle_ms`` 为间隔重试到本上界。
        # **因此它是一个跨机制参数，不是"只是重投次数"**：改它之前请确认上面三条都仍可接受。
        # 计数口径 = **处理尝试次数**：首投（xreadgroup ">"，delivery_count=1）
        # 永远先获得一次尝试，`max_deliveries=1` 也不会在派发前死信；失败/未 ack
        # 之后才由 worker 兜底分支或 reclaim 路径处置（plaita#252 捆绑单）。
        self.max_deliveries = max(1, int(max_deliveries))
        self.dlq_key = dlq_key or dlq_stream_key(stream_key)
        # C4-1：DLQ 裁剪上限（构造参数优先，其次 env，最后默认值）
        if dlq_max_len is not None:
            self.dlq_max_len = max(1, int(dlq_max_len))
        else:
            self.dlq_max_len = _dlq_max_len_from_env()
        # Track B 任务1（2026-10-02 评审）：死信守卫。长步处理中的消息会被
        # 同伴按 idle 阈值反复抢走，delivery_count 随之虚增到 max_deliveries；
        # 若此时无条件死信（内部 XACK 原消息），存活持有者一崩，执行永久
        # 失去恢复机会。守卫（如「执行租约仍被持有」）在任何 dead_letter
        # 决策前调用：True=允许死信；False 或抛异常=跳过（消息留 pending）。
        # None（默认）恒放行——存量行为零变化。
        self.dead_letter_guard = dead_letter_guard
        # #47：reclaim 前的活跃租约闸门。``_reclaim_one`` 原本只按 idle 阈值
        # XCLAIM，不看执行是否仍活着——排队/处理中的消息被反复抢占，
        # delivery_count 虚增触顶后经死信守卫「非终态+租约空」分支重入队，
        # 形成「死信→重入队→再烧 5 次投递→再死信」循环（DLQ 噪声 + churn）。
        # 回调契约：入参 = 消息体 dict，返回 True = 执行仍有活跃租约 →
        # reclaim 跳过该条（不 XCLAIM、不计数，等其终态/租约过期）；False =
        # 存量 reclaim 接管（真孤儿恢复路径不受影响）。None（默认）恒跳过
        # 检查——存量行为零变化。回调抛异常按「无法确认无租约」保守跳过。
        self.reclaim_lease_guard = reclaim_lease_guard
        # #26 告警钩子：死信只 logger.error 时值守全靠人刷日志。钩子在**入队
        # 之后**调用（事件已持久化，钩子失败不回滚死信语义），且 best-effort
        # ——WebhookAlerter.send 是非阻塞入队，绝不反压消费主循环。
        self.on_dead_letter = on_dead_letter
        self._metrics = {
            "enqueued": 0,
            "acked": 0,
            "reclaimed": 0,
            "dead_lettered": 0,
            "invalid_payload": 0,
            "lease_conflicts": 0,
            "poison_acked": 0,
            "failed": 0,
            "dlq_guard_skipped": 0,
            "reclaim_lease_skipped": 0,
            "residue_swept": 0,
        }

    def ensure_group(self) -> None:
        try:
            self.redis.xgroup_create(
                self.stream_key,
                self.group_name,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                return
            if isinstance(exc, _TRANSIENT_REDIS_ERRORS):
                # #48：连接/超时类**不得上抛**。原实现只放过 BUSYGROUP，其余一律
                # raise —— 而 `_read_once()` 的第一行就是本方法，于是读超时绕开了
                # `read()` 的 ConnectionError 兜底，直穿到 run() 把 worker 打死。
                # 这里改为「记 warning + 返回」：本轮没建上组，下一轮轮询再试；
                # Redis 恢复后自然成功。
                logger.warning("建消费组失败（瞬时故障，留待下轮重试）: %s", _scrub_url_text(exc))
                return
            # 非瞬时（NOAUTH/权限/schema/参数错）仍上抛：这是配置问题，
            # 静默重试只会把真问题藏起来。
            raise

    def enqueue(self, task: Dict[str, Any], *, schema_version: Optional[int] = None) -> str:
        msg_id = enqueue_task(
            self.redis, self.stream_key, task, schema_version=schema_version
        )
        self._metrics["enqueued"] += 1
        return msg_id

    def read(self, block_ms: int = 10_000) -> Optional[StreamTask]:
        """Read one task: reclaim stale pending first, then new messages.

        Reclaimed messages that already exhausted their delivery attempts are
        moved to the DLQ and acked (not returned); fresh messages from
        ``XREADGROUP ">"`` always get their first processing attempt.
        """
        try:
            return self._read_once(block_ms)
        except _TRANSIENT_REDIS_ERRORS as exc:
            # #48：**瞬时故障家族**（连接错 / redis 超时 / socket 超时）一律不得
            # 炸穿 run() 主循环。原实现只写了 `except RedisConnectionError`：
            #   * 2026-09 的修复容忍了 BLOCK 到期的 TimeoutError（那支在
            #     `_read_once` 内部已就地返回 None），但**没有**容忍从
            #     `ensure_group()`/`xclaim` 冒出来的超时——容器网络抖动 /
            #     Redis 重启窗口里 worker 会直接退出（实测复现）。
            #   * 又因 redis-py 8.x 里 TimeoutError 与 ConnectionError 互不继承，
            #     即使写成 ConnectionError 也接不住超时类。
            # 退避后返回 None 继续轮询，Redis 恢复后自动重连恢复消费。
            logger.warning("读队列失败（瞬时故障，退避 %.1fs 后重试）: %s",
                           RECONNECT_BACKOFF_SECONDS, _scrub_url_text(exc))
            time.sleep(RECONNECT_BACKOFF_SECONDS)
            return None

    def _read_once(self, block_ms: int) -> Optional[StreamTask]:
        self.ensure_group()
        # Drain any over-delivered pending into DLQ before serving work.
        # 超限条目的处置（死信或守卫拒绝后的跳过）内联在 _reclaim_one 的
        # 单轮扫描里——扫描天然收敛，无需外层 while 重扫（重扫会在
        # claim_min_idle_ms 极小时对守卫拒绝的条目空转，见 _reclaim_one）。
        reclaimed = self._reclaim_one()
        if reclaimed is not None:
            self._metrics["reclaimed"] += 1
            return reclaimed

        try:
            resp = self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams={self.stream_key: ">"},
                count=1,
                block=block_ms,
            )
        except RedisTimeoutError:
            # redis-py 5+ 在 BLOCK 到期时抛 TimeoutError（旧版返回空列表）。
            # 空轮询是常态，必须返回 None 让上层继续循环——
            # 否则异常炸穿 run() 主循环，worker 启动数秒后即退出。
            return None
        try:
            task = self._parse_read_response(resp, delivery_count=1)
        except InvalidTaskPayload as exc:
            # #186：解析点在 read() 的兜底分支之外——一条坏消息（生产者 bug /
            # 手工 XADD / 升级期旧格式）必须在这里就进 DLQ+ack，绝不能上抛把
            # worker 打死（消息不 ack 还会留在 PEL 里制造崩溃/重启循环）。
            self._dead_letter_invalid(
                exc.message_id, exc.fields, delivery_count=1, reason=exc.reason
            )
            return None
        if task is not None and self._unsupported_schema(task):
            # 语义上无法理解的更新消息：拒收并留证据（DLQ 含 message_id/payload），
            # 绝不按旧语义硬跑——升级期「先升消费者再升生产者」的兜底防线。
            # A42-05：走队列层守卫单一入口（与超投递/兜底异常同判）。守卫拒绝
            #（典型：执行仍有活租约）时消息留 pending，等新 build 回收。
            self._reject_unsupported_schema(task)
            return None
        # xreadgroup(">") 只交付**本消费组从未见过**的新消息：`delivery_count`
        # 恒为 1，表示「本次是第 1 次投递、此前零处理尝试」。`max_deliveries`
        # 的契约是「N 次处理尝试后死信」（见 `_reclaim_one` 的 delivery_count
        # 注释与 `docs-site/docs/distributed/flow-worker.md`），所以这里不能
        # 套用「预算已耗尽」判据——`max_deliveries=1` 时 `1 >= 1` 会在任何
        # 处理发生前就把合法任务死信（plaita#252 捆绑单：A1 r37 a/P2，零处理
        # 尝试）。超限存量由 `_reclaim_one` 判定；失败尝试后的死信由
        # `FlowWorker.run` 兜底分支判定（两者都过 `dead_letter_guard`）。
        return task

    def ack(self, message_id: str) -> None:
        acked = self.redis.xack(self.stream_key, self.group_name, message_id)
        self._metrics["acked"] += 1
        if acked:
            # C4-1：ack 成功后从 Stream 精确删除该条目，否则已确认消息永驻
            # 内存（stream 只增不减，长跑 worker 的 Redis 占用线性膨胀）。
            # 安全性已核实：XACK 把条目移出本消费组 PEL 后，XDEL 仅清理条目
            # 本体；对已 ack 的条目 XDEL 不会影响任何 pending 语义。本仓
            # 每 Stream 只挂一个消费组（plaita-workers），不存在其他组 PEL
            # 引用被删条目的路径。best-effort：XDEL 失败只影响空间回收，
            # 不回滚 ack（消息语义已终结）。
            try:
                self.redis.xdel(self.stream_key, message_id)
            except Exception as exc:
                logger.debug(
                    "xdel(%s, %s) failed (ack already applied): %s",
                    self.stream_key,
                    message_id,
                    _scrub_url_text(exc),
                )

    def _group_last_delivered_id(self) -> Optional[str]:
        """本消费组的 ``last-delivered-id``（组不存在/读不到则 None）。"""
        for group in self.redis.xinfo_groups(self.stream_key):
            if not isinstance(group, dict):
                continue
            if _decode(group.get("name")) == self.group_name:
                return _decode(group.get("last-delivered-id"))
        return None

    def sweep_acked_residue(self, batch_size: int = DEFAULT_RESIDUE_SWEEP_BATCH) -> int:
        """兜底回收「已 ack 未 XDEL」的残留条目（#43），返回删除条数。

        ack() 的 XDEL 是 best-effort：XACK 与 XDEL 之间进程被杀就留下已确认
        却未删除的条目（XDEL 全仓库只出现在 ack() 与本方法，故残留只可能来自
        这个窗口——跳过一次 ack 的条目留在 PEL 里是 pending，本方法不碰）
        ——只增不减地计入 XLEN，把运维的「积压」读数读成假阳性（2026-10-07
        实测误判）。本方法把这些条目按 ack() 同样的语义幂等回收，判据两条
        同时成立才删：

        - id ≤ 消费组 ``last-delivered-id``：已交付；大于它的条目尚未投递，
          是**合法积压**，必须保留；
        - 不在本组 PEL 中：已确认；pending（已交付未 ack）条目语义不变，
          绝不删（读不到 PEL 时本轮整体跳过——宁留残留不误删）。

        与 ack() 同一假设：每 Stream 只挂一个消费组。best-effort：任何
        Redis 失败只记 debug 并返回 0，不得影响消费主循环。单轮只扫 id 最小的
        ``batch_size`` 条：窗口里全是 pending 时本轮删 0 条，等它们被 ack 后
        的下轮继续收敛（收敛速率 = batch_size / 扫描间隔）。
        """
        batch_size = max(1, int(batch_size))
        try:
            last_delivered = self._group_last_delivered_id()
            if not last_delivered:
                return 0
            entries = self.redis.xrange(
                self.stream_key, min="-", max=last_delivered, count=batch_size
            )
        except Exception as exc:
            logger.debug("残留回收扫描失败（best-effort，忽略）: %s", _scrub_url_text(exc))
            return 0
        if not entries:
            return 0
        ids = [_decode(entry[0]) for entry in entries]
        try:
            pending = self.redis.xpending_range(
                self.stream_key,
                self.group_name,
                min=ids[0],
                max=ids[-1],
                count=batch_size,
            )
        except Exception as exc:
            logger.debug("残留回收读 PEL 失败（本轮跳过）: %s", _scrub_url_text(exc))
            return 0
        pending_ids = {
            _decode(entry.get("message_id") if isinstance(entry, dict) else entry[0])
            for entry in pending or []
        }
        victims = [mid for mid in ids if mid not in pending_ids]
        if not victims:
            return 0
        try:
            deleted = int(self.redis.xdel(self.stream_key, *victims) or 0)
        except Exception as exc:
            logger.debug("残留回收 XDEL 失败（下轮重试）: %s", _scrub_url_text(exc))
            return 0
        self._metrics["residue_swept"] += deleted
        if deleted:
            logger.info(
                "队列残留回收：XDEL %d 条已 ack 未删条目（%s）",
                deleted,
                self.stream_key,
            )
        return deleted

    def _unsupported_schema(self, task: StreamTask) -> bool:
        """消息信封版本是否超出本 build 的理解范围（``None`` = 关闭校验）。"""
        return (
            self.max_schema_version is not None
            and task.schema_version > self.max_schema_version
        )

    def _reject_unsupported_schema(self, task: StreamTask) -> bool:
        """超版本信封的统一处置：计数 + 守卫后的死信单一入口。

        A42-05：首投（``_read_once``）与回收（``_reclaim_one``）**必须同判**
        ——原实现首投直接 ``dead_letter``、回收完全不校验，同一条消息仅仅因为
        发生了一次消费者崩溃就从「死信留证」变成「旧语义硬跑」。

        返回 True = 已死信；False = 守卫拒绝/异常（消息留 pending）。每次遇到
        都计 ``schema_rejected``：守卫拒绝时消息会留 pending 被下轮再遇到，
        该计数是「拒收尝试」而非「DLQ 行数」（DLQ 行数看 ``dead_lettered``）。

        A49-01：任务带 ``schema_rejected`` 标记交给守卫。FlowWorker 的守卫据此
        跳过「非终态 + 租约空 → 克隆恢复副本」分支——副本继承同一超版本，本
        build 仍无法消费，克隆只会把「拒收」变成自持热环（DLQ 每轮 +1）。守卫
        仍可拒绝死信（典型：执行有活租约）让消息留 pending 等新 build。
        """
        self._metrics["schema_rejected"] = self._metrics.get("schema_rejected", 0) + 1
        task = replace(task, schema_rejected=True)
        return self.dead_letter_if_allowed(
            task,
            reason=(
                f"unsupported schema_version={task.schema_version} "
                f"(this build understands <= {self.max_schema_version})"
            ),
        )

    def _guard_allows_dead_letter(self, task: StreamTask) -> bool:
        """dead_letter 决策前的守卫闸门（Track B 任务1）。

        无守卫（默认 None）恒 True——存量行为零变化。守卫返回 False 或
        抛异常一律按「拒绝死信」处理：warning 日志、消息留在 pending 等
        待下一轮处置、``dlq_guard_skipped`` 计数。典型守卫是「执行租约
        是否仍被存活 worker 持有」：持有中说明持有者正常处理长步，此时
        死信会让该执行永久失去 resume 机会。
        """
        guard = self.dead_letter_guard
        if guard is None:
            return True
        try:
            allowed = bool(guard(task))
        except Exception as exc:
            self._metrics["dlq_guard_skipped"] += 1
            logger.warning(
                "dead_letter_guard raised for %s (delivery_count=%s); "
                "skip dead-letter, message stays pending: %s",
                task.message_id,
                task.delivery_count,
                _scrub_url_text(exc),
            )
            return False
        if not allowed:
            self._metrics["dlq_guard_skipped"] += 1
            logger.warning(
                "dead_letter_guard refused %s (delivery_count=%s); "
                "skip dead-letter, message stays pending",
                task.message_id,
                task.delivery_count,
            )
        return allowed

    def dead_letter_if_allowed(
        self,
        task: StreamTask,
        *,
        reason: str,
        payload: Any = _UNSET,
        extra: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """守卫闸门后的死信单一入口（plaita#252）。

        任何调用方在决定死信前都必须走这里，判定与 ``_read_once`` /
        ``_reclaim_one`` 完全一致：

        - 返回 ``True``：已死信（payload 进 DLQ，原消息已 ack）；
        - 返回 ``False``：守卫拒绝或抛异常（``dlq_guard_skipped`` 已计数），
          消息**留在 pending**，调用方不得 ack，等下一轮处置；
        - 守卫为 ``None``（默认）：恒放行——存量行为零变化。

        覆盖 ``payload``/``extra`` 只给畸形消息信封用（原始串 + raw_fields）；
        其余调用方保持默认 = 消息体。

        worker 的兜底异常分支（``FlowWorker.run``）曾绕过守卫直调
        ``dead_letter``，在瞬态故障/活租约场景把唯一恢复载体 ack 出 PEL。
        信封版本拒收（A42-05）与畸形 payload（B44-05）是同一契约下的另两处
        历史旁路：现在都经由本方法。
        """
        if not self._guard_allows_dead_letter(task):
            return False
        # 只有畸形消息才覆盖 payload/extra：普通路径的调用形状保持
        # ``dead_letter(task, reason=...)``，既有子类/测试覆写零变化。
        overrides: Dict[str, Any] = {}
        if payload is not _UNSET:
            overrides["payload"] = payload
        if extra is not None:
            overrides["extra"] = extra
        self.dead_letter(task, reason=reason, **overrides)
        return True

    def dead_letter(
        self,
        task: StreamTask,
        *,
        reason: str,
        payload: Any = _UNSET,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Move task payload to DLQ stream and ack the original message.

        信封固定带 ``schema_version``（A44 b 类）：审计/重放时不依赖 payload
        猜测源消息的语义版本。
        """
        envelope = {
            "reason": reason,
            "source_stream": self.stream_key,
            "source_id": task.message_id,
            "delivery_count": task.delivery_count,
            "dead_lettered_at": time.time(),
            "schema_version": task.schema_version,
            "payload": task.body if payload is _UNSET else payload,
        }
        if extra:
            envelope.update(extra)
        dlq_id = self.redis.xadd(
            self.dlq_key,
            {PAYLOAD_FIELD: json.dumps(envelope, ensure_ascii=False)},
        )
        # C4-1：DLQ 无消费者，入队后裁剪只保留最近 N 条，防无限增长。
        try:
            self.redis.xtrim(self.dlq_key, maxlen=self.dlq_max_len, approximate=False)
        except Exception as exc:
            logger.debug("xtrim(%s, maxlen=%s) failed: %s", self.dlq_key, self.dlq_max_len, _scrub_url_text(exc))
        self.ack(task.message_id)
        self._metrics["dead_lettered"] += 1
        logger.error(
            "Dead-lettered task %s -> %s id=%s (%s)",
            task.message_id,
            self.dlq_key,
            _decode(dlq_id),
            reason,
        )
        # #26：死信事件外发（webhook 等）。best-effort——钩子抛错只记 warning，
        # 死信本身已落 DLQ，不得因告警通道故障影响 at-least-once 语义。
        if self.on_dead_letter is not None:
            try:
                self.on_dead_letter({
                    "event": "dead_letter",
                    "queue": self.stream_key,
                    "dlq_key": self.dlq_key,
                    "dlq_id": _decode(dlq_id),
                    "message_id": task.message_id,
                    "delivery_count": task.delivery_count,
                    "reason": reason,
                    "ts": time.time(),
                })
            except Exception as exc:  # noqa: BLE001 - 告警旁路
                logger.warning("dead_letter 告警钩子失败: %s", _scrub_url_text(exc))
        return _decode(dlq_id)

    def _dead_letter_invalid(
        self,
        message_id: str,
        fields: Dict[Any, Any],
        *,
        delivery_count: int,
        reason: str,
    ) -> bool:
        """#186：解析失败的畸形消息 → DLQ 信封 + ack（绝不抛回消费循环）。

        信封沿用 ``dead_letter`` 的既有字段；``payload`` 放**原始 payload 串**
        （截断到 ``INVALID_PAYLOAD_MAX_CHARS``），另带 ``raw_fields``（原始
        信封）与 ``truncated`` 标记，排障时能区分「生产者 bug」与「升级期
        旧格式」。

        B44-05（守卫第二旁路）：本路径同样经 ``dead_letter_if_allowed``。守卫
        看到的是不可解析体 ⇒ 传空 body 的 ``StreamTask``（FlowWorker 的守卫对
        无 ``execution_id`` 的消息恒放行，生产语义不变）；守卫拒绝/异常时消息
        留 pending，返回 False。
        """
        def _text(value: Any) -> str:
            return value.decode(errors="replace") if isinstance(value, bytes) else str(value)

        raw_payload = ""
        for key in (PAYLOAD_FIELD, b"payload"):
            if key in fields:
                raw_payload = _text(fields[key])
                break
        truncated = len(raw_payload) > INVALID_PAYLOAD_MAX_CHARS
        raw_fields = {
            _text(key): _text(value)[:INVALID_PAYLOAD_MAX_CHARS]
            for key, value in fields.items()
        }
        self._metrics["invalid_payload"] = self._metrics.get("invalid_payload", 0) + 1
        task = StreamTask(
            message_id=message_id,
            body={},
            delivery_count=delivery_count,
            schema_version=_schema_version_from_fields(fields),
        )
        return self.dead_letter_if_allowed(
            task,
            reason=reason,
            payload=raw_payload[:INVALID_PAYLOAD_MAX_CHARS],
            extra={"raw_fields": raw_fields, "truncated": truncated},
        )

    def note_lease_conflict(self) -> None:
        self._metrics["lease_conflicts"] += 1

    def note_poison(self) -> None:
        self._metrics["poison_acked"] += 1

    def note_failed(self) -> None:
        self._metrics["failed"] += 1

    def stats(self) -> Dict[str, Any]:
        """Operational snapshot (best-effort; fake/partial Redis ok)."""
        pending_count = 0
        stream_len = 0
        dlq_len = 0
        try:
            stream_len = int(self.redis.xlen(self.stream_key) or 0)
        except Exception as exc:
            logger.debug("xlen(%s) failed: %s", self.stream_key, _scrub_url_text(exc))
        try:
            dlq_len = int(self.redis.xlen(self.dlq_key) or 0)
        except Exception as exc:
            logger.debug("xlen(%s) failed: %s", self.dlq_key, _scrub_url_text(exc))
        try:
            summary = self.redis.xpending(self.stream_key, self.group_name)
            if isinstance(summary, dict):
                pending_count = int(summary.get("pending", 0) or 0)
            elif summary:
                pending_count = int(summary[0] or 0)
        except Exception as exc:
            logger.debug("xpending(%s, %s) failed: %s", self.stream_key, self.group_name, _scrub_url_text(exc))
        return {
            "stream_key": self.stream_key,
            "dlq_key": self.dlq_key,
            "group": self.group_name,
            "consumer": self.consumer_name,
            "stream_length": stream_len,
            "pending": pending_count,
            "dlq_length": dlq_len,
            "max_deliveries": self.max_deliveries,
            "claim_min_idle_ms": self.claim_min_idle_ms,
            **self._metrics,
        }

    def _parse_read_response(self, resp, delivery_count: int = 1) -> Optional[StreamTask]:
        if not resp:
            return None
        _stream, messages = resp[0]
        if not messages:
            return None
        msg_id, fields = messages[0]
        return self._task_from_fields(_decode(msg_id), fields, delivery_count)

    def _task_from_fields(
        self, message_id: str, fields: Dict[Any, Any], delivery_count: int
    ) -> StreamTask:
        try:
            body = json.loads(_payload_from_fields(fields))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.error("Invalid task payload id=%s: %s", message_id, _scrub_url_text(exc))
            raise InvalidTaskPayload(
                message_id, fields, f"invalid_payload: {_scrub_url_text(exc)}"
            ) from exc
        if not isinstance(body, dict):
            raise InvalidTaskPayload(
                message_id, fields, "invalid_payload: task payload must be a JSON object"
            )
        return StreamTask(
            message_id=message_id,
            body=body,
            delivery_count=delivery_count,
            schema_version=_schema_version_from_fields(fields),
        )

    def _pending_delivery_count(self, message_id: str) -> int:
        try:
            pending = self.redis.xpending_range(
                self.stream_key,
                self.group_name,
                min=message_id,
                max=message_id,
                count=1,
            )
        except Exception as exc:
            logger.debug("xpending_range delivery count failed for %s: %s", message_id, _scrub_url_text(exc))
            return 1
        if not pending:
            return 1
        entry = pending[0]
        if isinstance(entry, dict):
            return int(entry.get("times_delivered") or 1)
        # tuple: (id, consumer, idle, deliveries)
        if len(entry) >= 4:
            return int(entry[3] or 1)
        return 1

    def _peek_task_body(self, message_id: str) -> Optional[Dict[str, Any]]:
        """XCLAIM 前无副作用读消息体（#47：XRANGE 不动 PEL/delivery_count）。

        读不到（Redis 瞬断/信封损坏/条目已不存在）返回 None：调用方放行按
        存量 reclaim——探测缺位不得切断恢复路径（XCLAIM 侧的 Redis 异常
        自有既有兜底）。
        """
        try:
            entries = self.redis.xrange(
                self.stream_key, min=message_id, max=message_id, count=1
            )
        except Exception as exc:  # noqa: BLE001 — 保守放行，见 docstring
            logger.debug("reclaim 前读消息体失败（放行 reclaim）: id=%s: %s", message_id, _scrub_url_text(exc))
            return None
        if not entries:
            return None
        _mid, fields = entries[0]
        try:
            body = json.loads(_payload_from_fields(fields))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.debug("reclaim 前消息体解析失败（放行 reclaim）: id=%s: %s", message_id, _scrub_url_text(exc))
            return None
        return body if isinstance(body, dict) else None

    def _reclaim_allowed_by_lease(self, body: Dict[str, Any], message_id: str) -> bool:
        """#47：XCLAIM 前的活跃租约闸门。True=允许 reclaim；False=跳过该条。

        回调（``reclaim_lease_guard``）按消息体判「执行是否仍有活跃租约」：
        True（租约活跃）→ 跳过——不 XCLAIM、不计数，等其终态/租约过期；
        False → 存量 reclaim 接管（真孤儿恢复路径不受影响）。回调抛异常按
        「无法确认无租约」保守跳过（与 dead_letter_guard 异常语义同向：宁可
        少抢一轮，不可误伤活执行）；消息留 pending，下轮探测再判。
        """
        try:
            active = bool(self.reclaim_lease_guard(body))
        except Exception as exc:  # noqa: BLE001 — 探测故障保守跳过
            self._metrics["reclaim_lease_skipped"] += 1
            logger.warning(
                "reclaim_lease_guard 探测失败（保守跳过 reclaim）: id=%s: %s",
                message_id,
                _scrub_url_text(exc),
            )
            return False
        if active:
            self._metrics["reclaim_lease_skipped"] += 1
            logger.debug(
                "reclaim 跳过（执行租约活跃，等其终态/过期再接管）: id=%s",
                message_id,
            )
            return False
        return True

    def _reclaim_one(self) -> Optional[StreamTask]:
        """抢回一条 idle 超阈的 pending 消息（一条；无则 None）。

        扫描窗口（8 条）内逐条 XCLAIM；超 ``max_deliveries`` 的条目**就地
        处置**——守卫允许则 dead_letter，被拒（False/异常）则跳过——两种
        处置都继续扫下一条 pending。处置必须在同一轮扫描内完成而不能交给
        调用方重扫：XCLAIM 会把条目 idle 归零，``claim_min_idle_ms`` 极小时
        （如单测的 1ms）下一轮扫描会立刻再抢到同一条目，外层循环空转。

        A44-01：XCLAIM 后同样做信封版本校验——超版本消息走与首投相同的
        守卫死信路径（``_reject_unsupported_schema``），绝不返回给消费循环。
        否则「首投 → 死信」与「崩溃后 reclaim → 旧语义执行」对同一条消息
        给出两种语义，升级/回退窗口的崩溃就能绕过版本边界。

        #47：XCLAIM 前先过活跃租约闸门（``reclaim_lease_guard``）——租约
        活跃的条目直接跳过（不 XCLAIM、不计数），排队/处理中的消息不再被
        反复抢占刷 ``times_delivered`` 触顶死信。未配守卫时零变化。

        #196：扫描窗口按 ``_reclaim_cursor`` 轮转——每轮从上一轮扫到的 id
        处继续（PEL 按 id 升序返回），到尾部回绕到头部。固定只取头 8 条时，
        前 8 条长期活跃的租约会把窗口占死，第 9 条起的真孤儿永不进入扫描。
        游标只是进程内状态（不新增 Redis 键）；XCLAIM 会把 idle 归零，故
        游标自身在下一轮显式跳过，避免把自己的投递次数刷爆。
        """
        window = self.reclaim_window
        try:
            pending = self.redis.xpending_range(
                self.stream_key,
                self.group_name,
                min=self._reclaim_cursor or "-",
                max="+",
                count=window,
            )
        except Exception as exc:
            logger.debug("xpending_range reclaim scan failed: %s", _scrub_url_text(exc))
            return None
        if not pending:
            # 到尾部（或 PEL 空）：下轮回到头部，保证有限轮内覆盖全部 pending。
            self._reclaim_cursor = None
            return None

        scanned_from = self._reclaim_cursor
        last_scanned: Optional[str] = None
        for entry in pending:
            if isinstance(entry, dict):
                msg_id = entry.get("message_id")
                idle = entry.get("time_since_delivered", 0)
                deliveries = int(entry.get("times_delivered") or 1)
            else:
                msg_id, _consumer, idle, deliveries = entry[0], entry[1], entry[2], entry[3]
                deliveries = int(deliveries or 1)
            msg_id_str = _decode(msg_id)
            last_scanned = msg_id_str
            if msg_id_str == scanned_from:
                # 游标自身：上一轮已处置/已判定，跳过（否则 idle 归零后会被
                # 自己反复 XCLAIM，白白烧 delivery_count）。
                continue
            if idle < self.claim_min_idle_ms:
                continue
            # #47：租约闸门在 XCLAIM 之前——抢走一条「执行还活着」的消息
            # 只会白白虚增投递次数（死信循环的根因），先探测再决定。
            if self.reclaim_lease_guard is not None:
                body = self._peek_task_body(msg_id_str)
                if body is not None and not self._reclaim_allowed_by_lease(body, msg_id_str):
                    continue
                # body 读不到 → 放行按存量 reclaim（恢复路径优先于探测完整性）
            claimed = self.redis.xclaim(
                self.stream_key,
                self.group_name,
                self.consumer_name,
                min_idle_time=self.claim_min_idle_ms,
                message_ids=[msg_id_str],
            )
            if not claimed:
                continue
            mid, fields = claimed[0]
            claimed_id = _decode(mid)
            # delivery_count 语义（2026-10-03 澄清，勿「顺手修」成 +1）：
            # `deliveries` = XCLAIM 前的 times_delivered（已完成派发次数），
            # 本次 XCLAIM 是第 deliveries+1 次投递，故这里上报的是
            # 「即将进行的这次处理」的**前一次**序号——看似差一，但两侧
            # `>= max_deliveries` 死信检查（本函数与 FlowWorker.run 兜底）
            # 净值恰为「max_deliveries 次处理后死信」，与「最大投递次数」
            # 契约一致。改成 +1 会让死信提前一轮（5 次变 4 次）。该值仅作
            # 阈值判据与可观测透传（FlowWorker 节点重试预算另用 Redis 计数
            # 键，不依赖它），报告展示时理解为 attempt-1 即可。
            try:
                task = self._task_from_fields(
                    claimed_id, fields, delivery_count=max(deliveries, 1)
                )
            except InvalidTaskPayload as exc:
                # #186：reclaim 路径同款处置——XCLAIM 之后才发现坏消息，
                # 也必须 DLQ+ack，不能把解析异常抛回消费循环。游标推到本条
                # （#196）：坏消息已被 ack 出 PEL，但推进游标仍必要——否则
                # 下一轮从同一 min 重扫，窗口被本条占位、真孤儿后移。
                self._dead_letter_invalid(
                    exc.message_id,
                    exc.fields,
                    delivery_count=max(deliveries, 1),
                    reason=exc.reason,
                )
                self._reclaim_cursor = claimed_id
                continue
            # A44-01：回收路径与首投**同判**。XCLAIM 后拿到的 task 同样可能是
            # 新 build 写出的超版本载体；原实现只首投拒收，回收则原样返回执行
            #（同一条消息：崩溃前死信、崩溃后旧语义硬跑）。检查放在超投递判据
            # 之前——版本不兼容比「投递次数」更根本，也避免以误导性 reason 死信。
            if self._unsupported_schema(task):
                self._reject_unsupported_schema(task)
                self._reclaim_cursor = claimed_id
                continue
            if task.delivery_count >= self.max_deliveries:
                if self._guard_allows_dead_letter(task):
                    self.dead_letter(
                        task,
                        reason=f"max_deliveries={self.max_deliveries}",
                    )
                self._reclaim_cursor = claimed_id
                continue
            self._reclaim_cursor = claimed_id
            return task
        # 整窗扫完：短窗说明已到 PEL 尾部，下轮回绕到头部；整窗则从窗口尾
        # 继续（窗口尾条目下轮作为游标被跳过，实际推进 7 条新条目）。
        self._reclaim_cursor = None if len(pending) < window else last_scanned
        return None
