"""Fenced execution storage：世代号 CAS 写（设计稿 §4.2，波次②）。

同一执行双 worker 并发推进时，执行状态写是无条件 SET（last-writer-wins）。
本包装器把 ``save_execution_state`` 变成**单段 Lua 的 compare-and-set**：
仅当 ContextVar 携带的 fence 世代与 Redis ``{ns}:execution:fence:{id}``
一致才落盘，世代不符（租约已被新世代接管）抛 ``ExecutionLeaseError``，
让 worker 现有异常链（``except ExecutionLeaseError: raise`` → 消息不 ack）
接管，旧世代 worker 不会污染新世代的执行状态。

- 世代号由 ``RedisExecutionLease.try_acquire_fenced`` 在 acquire 时产生；
  worker 侧经 ContextVar 传给本包装器（save_execution_state 调用面不改
  签名——contextvar 方案，设计稿 §4.2 预留的两个选项中最小侵入者）。
- 未设置世代（None：start 路径无租约、内存存储、单测）退化为普通写——
  NullLease 同款 no-op，存量行为零变化（设计稿 §3.5 兼容红线）。
- Redis 原语选型 Lua 而非 WATCH/Multi：compare+SET 是「读-判-写」原子序，
  WATCH 需客户端重试循环 + 两次 RTT；仓内已有同构 Lua 先例
  （execution_lease.py 的 release/renew）。
- 回滚开关 ``PLAITA_DISABLE_FENCING=1``：tenant_context 注入点直接退回裸
  RedisExecutionStorage（设计稿 §6 波次②回滚门）。
"""
from __future__ import annotations

import json
import os
from contextvars import ContextVar, Token
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..core.errors import ExecutionLeaseError
from ..logger import logger
from .base import ExecutionState, ExecutionStorage
from .base import StateSerializationError
from .redis import (
    TERMINAL_EXECUTION_STATUSES,
    RedisExecutionStorage,
    execution_fence_key,
    execution_index_key,
    execution_lease_key,
    execution_start_time_score,
    execution_state_ttl_seconds,
    execution_tombstone_key,
    execution_tombstone_tenant,
    execution_tombstone_ttl_seconds,
)
from plaita.core.url_redaction import _scrub_url_text


# 单段 Lua：fence 世代匹配才允许写执行状态。
# KEYS[1]=状态键, KEYS[2]=fence 键, KEYS[3]=执行列表索引 ZSET,
# ARGV[1]=序列化状态, ARGV[2]=期望世代, ARGV[3]=TTL 秒（0=不带 EX），
# ARGV[4]=索引 score（start_time epoch），ARGV[5]=索引 member（execution_id）。
# 返回 1=写入成功；0=世代不符（键缺失也算不符）。
# 索引必须在脚本内同步维护：本路径绕过 RedisExecutionStorage.save_execution_state
# 直接 SET 状态键，不在此 ZADD 则索引成员会滞后（列表长期显示过期 status，
# 这正是当初否决「save 时维护索引」的顾虑）。
_FENCED_SAVE_LUA = """
local ttl = tonumber(ARGV[3]) or 0
if redis.call('get', KEYS[2]) == ARGV[2] then
  if ttl > 0 then
    redis.call('set', KEYS[1], ARGV[1], 'EX', ttl)
  else
    redis.call('set', KEYS[1], ARGV[1])
  end
  redis.call('zadd', KEYS[3], ARGV[4], ARGV[5])
  return 1
else
  return 0
end
"""


# 单段 Lua：只有 fence 世代仍归本持有者时才允许删除执行状态（A43-07）。
# 与 save 的 CAS 对称——delete 也是状态改写，旧世代不得删除新世代已推进
# 的状态。删除范围与 RedisExecutionStorage.delete_execution_state 同口径：
# 状态键 + 索引成员 + fence 键 + lease 键 + 执行级计数键（A51-03）+
# 执行墓碑（A51-01，平台根键）。
# KEYS[1]=状态键, KEYS[2]=fence 键, KEYS[3]=索引 ZSET, KEYS[4]=lease 键,
# KEYS[5..N-1]=执行级计数键（nofail/g1wakeups/noderefetch/noderetry[:node]/
# affinityhandover），KEYS[N]=执行墓碑键（平台根，最后一个）。
# ARGV[1]=期望世代, ARGV[2]=execution_id, ARGV[3]=墓碑 TTL 秒（0=不过期）,
# ARGV[4]=墓碑 payload
# 返回 1=删除成功；0=世代不符（键缺失也算不符）。墓碑与四删同段 Lua 原子
# 落盘：脚本中途失败时 Redis 不会只执行一半（Lua 单命令原子）。
# 世代计数器 fencegen 不在删除范围（A2R13）：它是删除后 ABA 防护的单调锚。
_FENCED_DELETE_LUA = """
if redis.call('get', KEYS[2]) == ARGV[1] then
  redis.call('del', KEYS[1])
  redis.call('zrem', KEYS[3], ARGV[2])
  redis.call('del', KEYS[2])
  redis.call('del', KEYS[4])
  for i = 5, #KEYS - 1 do
    redis.call('del', KEYS[i])
  end
  local ttl = tonumber(ARGV[3]) or 0
  if ttl > 0 then
    redis.call('set', KEYS[#KEYS], ARGV[4], 'EX', ttl)
  else
    redis.call('set', KEYS[#KEYS], ARGV[4])
  end
  return 1
else
  return 0
end
"""


_fence_token_var: ContextVar[Optional[int]] = ContextVar(
    "plaita_fence_token", default=None
)


def current_fence_token() -> Optional[int]:
    """当前执行线程的 fence 世代号（None = 未持 fenced 租约，写不设防）。"""
    return _fence_token_var.get()


def set_current_fence_token(generation: int) -> Token:
    """worker 取得 fenced 租约后设置世代号；返回 token 供 finally 复位。"""
    return _fence_token_var.set(int(generation))


def reset_current_fence_token(token: Token) -> None:
    _fence_token_var.reset(token)


def fencing_disabled() -> bool:
    """波次②回滚开关：``PLAITA_DISABLE_FENCING=1`` 时整体退回无 fencing 行为。"""
    return os.environ.get("PLAITA_DISABLE_FENCING", "").strip() == "1"


class FencedExecutionStorage(ExecutionStorage):
    """ExecutionStorage fencing 包装器（套在 RedisExecutionStorage 外）。

    除 ``save_execution_state`` 变世代 CAS 写外全部方法透传内层存储。
    """

    def __init__(self, inner: ExecutionStorage) -> None:
        self._inner = inner

    # ---- fenced 写 ----

    def save_execution_state(self, execution_id: str, state: ExecutionState) -> bool:
        generation = current_fence_token()
        if generation is None:
            # 未持 fenced 租约（start 路径 / 回滚模式 / 内存存储 / 单测）：
            # 保持普通写，存量行为零变化（§3.5 兼容红线）。
            return self._inner.save_execution_state(execution_id, state)

        client = getattr(self._inner, "client", None)
        key_builder = getattr(self._inner, "get_namespace_key", None)
        if client is None or not callable(key_builder) or not hasattr(client, "eval"):
            # 内层非 Redis 后端（注入点约束下不应发生）→ 防御性退化普通写
            return self._inner.save_execution_state(execution_id, state)

        state_key = key_builder("execution", execution_id)
        namespace = getattr(self._inner, "namespace", "plaita")
        fence_key = execution_fence_key(namespace, execution_id)
        try:
            serialized = self._inner.serialize_state(state.model_dump())
        except Exception as e:
            # #80：与普通写路径同判据 —— 不可序列化是**永久**失败，不得吞成
            # `False`（那会被上层当瞬态重投）。
            logger.error(
                "fenced 执行状态不可序列化 %s（永久失败，不重试）: %s", execution_id, _scrub_url_text(e)
            )
            raise StateSerializationError(execution_id, e) from e
        try:
            # C4-3 对齐（Track B 任务2）：fenced 落盘的终态键此前只 SET 不带
            # EX，永不过期、只能靠 console 读时补偿。现按状态取与普通路径
            # （RedisExecutionStorage.save_execution_state）同源的 TTL——常量
            # 与 env 语义（PLAITA_EXECUTION_STATE_TTL_DAYS，<=0 关）经 import
            # 复用自动继承；非终态 ttl=0 保持无过期（可恢复执行的活动状态）。
            ttl = (
                execution_state_ttl_seconds()
                if state.status in TERMINAL_EXECUTION_STATUSES
                else 0
            )
            result = client.eval(
                _FENCED_SAVE_LUA,
                3,
                state_key,
                fence_key,
                execution_index_key(namespace),
                serialized,
                str(int(generation)),
                str(int(ttl)),
                str(execution_start_time_score(state)),
                execution_id,
            )
        except Exception as e:
            logger.error("Fenced save execution state %s failed: %s", execution_id, _scrub_url_text(e))
            return False
        if not result:
            # 世代不符：租约已被新世代接管（或 fence 键丢失）。拒绝写并让
            # 上层（FlowWorker 的 except ExecutionLeaseError 链）以失租语义
            # 退出——消息不 ack，等待重投。
            raise ExecutionLeaseError(
                f"fencing token mismatch for execution {execution_id} "
                f"(gen={generation}); state write refused"
            )
        return True

    # ---- 透传 ----

    def load_execution_state(self, execution_id: str) -> Optional[ExecutionState]:
        return self._inner.load_execution_state(execution_id)

    def load_execution_tombstone(self, execution_id: str) -> Optional[Dict[str, Any]]:
        return self._inner.load_execution_tombstone(execution_id)

    def delete_execution_state(self, execution_id: str) -> bool:
        """删除执行状态；持世代号时只允许删除**自己的世代**（A43-07）。

        无世代（控制面删除 / 未持 fenced 租约 / 单测）→ 透传内层，内层会
        连同 fence、lease、执行级计数键一并清掉（删除即吊销，A43-01 /
        A51-03）。持世代 → 单段 Lua 先比较 fence 键，匹配才删状态 + 索引
        成员 + fence + lease + 计数键，并写平台根执行墓碑（A51-01）；
        不匹配抛 ``ExecutionLeaseError``
        （与 save 的 CAS 语义对称——旧世代不得删除新世代已推进的状态，也
        不得借删除把新世代的状态清掉）。

        两条路径都**不**删除 ``fencegen`` 世代计数器（A2R13）：删除后同 id
        的新执行必须拿到严格更大的世代，旧 worker 的迟到写/删才会被拒。
        """
        generation = current_fence_token()
        if generation is None:
            return self._inner.delete_execution_state(execution_id)

        client = getattr(self._inner, "client", None)
        key_builder = getattr(self._inner, "get_namespace_key", None)
        if client is None or not callable(key_builder) or not hasattr(client, "eval"):
            # 内层非 Redis 后端（注入点约束下不应发生）→ 防御性透传
            return self._inner.delete_execution_state(execution_id)

        state_key = key_builder("execution", execution_id)
        namespace = getattr(self._inner, "namespace", "plaita")
        counter_key_builder = getattr(
            self._inner, "counter_keys_for_execution", None
        )
        counter_keys = (
            counter_key_builder(execution_id)
            if callable(counter_key_builder)
            else []
        )
        # A51-01/A52-01：墓碑与四删同一段 Lua（CAS 成功才写），平台根键位
        # 保证租户 namespace purge 不会把它擦掉。墓碑键排在计数键之后。
        tombstone_payload = json.dumps(
            {
                "tenant": execution_tombstone_tenant(namespace),
                "deleted_at": datetime.now().isoformat(),
                "reason": "execution_deleted",
            },
            ensure_ascii=False,
        )
        tombstone_ttl = execution_tombstone_ttl_seconds()
        try:
            result = client.eval(
                _FENCED_DELETE_LUA,
                4 + len(counter_keys) + 1,
                state_key,
                execution_fence_key(namespace, execution_id),
                execution_index_key(namespace),
                execution_lease_key(namespace, execution_id),
                *counter_keys,
                execution_tombstone_key(execution_id),
                str(int(generation)),
                execution_id,
                str(int(tombstone_ttl)),
                tombstone_payload,
            )
        except Exception as e:
            logger.error(
                "Fenced delete execution state %s failed: %s",
                execution_id,
                _scrub_url_text(e),
            )
            return False
        if not result:
            raise ExecutionLeaseError(
                f"fencing token mismatch for execution {execution_id} "
                f"(gen={generation}); state delete refused"
            )
        return True

    def list_executions(
        self,
        query: Optional[Any] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ExecutionState]:
        return self._inner.list_executions(
            query=query, order_by=order_by, limit=limit, offset=offset
        )

    def __getattr__(self, name: str) -> Any:
        # serialize_state / diagnose 等非 ABC 方法透传内层
        if name == "_inner":
            raise AttributeError(name)
        return getattr(self._inner, name)


def build_fenced_execution_storage(**kwargs: Any) -> ExecutionStorage:
    """``_storage_cls`` 同位注入工厂：RedisExecutionStorage（+ fencing）。

    PLAITA_DISABLE_FENCING=1 时返回裸 RedisExecutionStorage（§6 波次②回滚）。
    """
    inner = RedisExecutionStorage(**kwargs)
    if fencing_disabled():
        return inner
    return FencedExecutionStorage(inner)
