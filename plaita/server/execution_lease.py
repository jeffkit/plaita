"""Execution resume lease: at most one worker resumes a given execution_id.

波次②（设计稿 docs/DESIGN-cancellation-and-lease.md §4.2）在 SET NX 之上
增加 **fencing token（世代号）**：``try_acquire_fenced`` 以 Lua 原子地
「NX 抢锁 → INCR 世代键 → 租约值变为 ``{holder}:{gen}``」，renew/release
仍对**整个 value 串** compare（原样）；fenced storage 的写侧 CAS 据此拒绝
旧世代的落盘。
"""
from __future__ import annotations

import logging
import uuid
from typing import Optional, Protocol, Union

logger = logging.getLogger("plaita.server.execution_lease")

DEFAULT_LEASE_TTL_SECONDS = 120
DEFAULT_KEY_PREFIX = "plaita:execution:lease:"

# fence 世代键 TTL：远大于租约 TTL（7 天，与取消标志键同量级），残留无害、
# 到期自清理（设计稿 §6 波次②回滚说明「fence 键残留无害，带 TTL 清理」）。
FENCE_KEY_TTL_SECONDS = 7 * 86400

# Release only if we still own the key (compare-and-del).
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""

# Renew only if we still own the key.
_RENEW_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('expire', KEYS[1], ARGV[2])
else
  return 0
end
"""

# Fenced acquire（设计稿 §4.2）：世代号产生 + NX 互斥一体原子完成。
# GET 判空 + SET 在同一 Lua 脚本内执行，脚本原子性保证等价于 SET NX
# ——另一 worker 持有未过期租约时 acquire 失败（T1 契约）；租约真过期
# 后 acquire 成功且世代必然递增，旧持有者的写被 fenced storage CAS 拒绝。
# 设计稿 Lua 草图（无条件 INCR+SET）缺 NX 语义，会破坏「B 持租约时 A
# resume 抛 ExecutionLeaseError」，实现按 T1 修正为 NX-preserving。
# KEYS[1]=lease key, KEYS[2]=fence key, ARGV[1]=holder, ARGV[2]=ttl 秒,
# ARGV[3]=fence key TTL 秒。返回世代号（>0）或 0（已被持有）。
_FENCED_ACQUIRE_LUA = """
if redis.call('get', KEYS[1]) then
  return 0
end
local gen = redis.call('incr', KEYS[2])
redis.call('expire', KEYS[2], ARGV[3])
redis.call('set', KEYS[1], ARGV[1] .. ':' .. tostring(gen), 'EX', ARGV[2])
return gen
"""


class ExecutionLeaseError(RuntimeError):
    """Raised when resume cannot acquire or keep the execution lease."""


class ExecutionLease(Protocol):
    def try_acquire(self, execution_id: str, holder: str, ttl_seconds: int) -> bool: ...

    def release(self, execution_id: str, holder: str) -> bool: ...

    def renew(self, execution_id: str, holder: str, ttl_seconds: int) -> bool: ...


class FencedExecutionLease(Protocol):
    """支持 fencing 世代号的 lease 可选扩展（鸭子探测，不强制实现）。"""

    def try_acquire_fenced(
        self, execution_id: str, holder: str, ttl_seconds: int
    ) -> Optional[int]: ...


def _decode(value: Union[str, bytes, None]) -> Optional[str]:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else value


class RedisExecutionLease:
    """Redis SET NX EX lease keyed by execution_id.

    ``try_acquire``（普通档）值即 holder；``try_acquire_fenced``（fencing 档）
    值为 ``{holder}:{gen}``——renew/release 对整个 value 串 compare，调用方
    需以 acquire 返回的完整租约值串续期/释放。
    """

    def __init__(
        self,
        redis_client,
        key_prefix: str = DEFAULT_KEY_PREFIX,
        fence_key_prefix: Optional[str] = None,
    ):
        self.redis = redis_client
        self.key_prefix = key_prefix
        if fence_key_prefix is None:
            if key_prefix.endswith("lease:"):
                fence_key_prefix = key_prefix[: -len("lease:")] + "fence:"
            else:
                fence_key_prefix = key_prefix + ":fence:"
        self.fence_key_prefix = fence_key_prefix

    def _key(self, execution_id: str) -> str:
        return f"{self.key_prefix}{execution_id}"

    def _fence_key(self, execution_id: str) -> str:
        return f"{self.fence_key_prefix}{execution_id}"

    def try_acquire(self, execution_id: str, holder: str, ttl_seconds: int) -> bool:
        key = self._key(execution_id)
        ok = self.redis.set(key, holder, nx=True, ex=ttl_seconds)
        return bool(ok)

    def try_acquire_fenced(self, execution_id: str, holder: str, ttl_seconds: int) -> Optional[int]:
        """NX 抢锁 + 世代号递增（原子）。成功返回世代号，被持有返回 None。

        租约值变为 ``{holder}:{gen}``；fence 键 ``{fence_prefix}{id}`` 首次
        acquire 时由 INCR 创建（兼容旧数据，设计稿 §4.2），TTL 自清理。
        """
        key = self._key(execution_id)
        fence_key = self._fence_key(execution_id)
        result = self.redis.eval(
            _FENCED_ACQUIRE_LUA,
            2,
            key,
            fence_key,
            holder,
            str(ttl_seconds),
            str(FENCE_KEY_TTL_SECONDS),
        )
        gen = int(result)
        return gen if gen > 0 else None

    def release(self, execution_id: str, holder: str) -> bool:
        key = self._key(execution_id)
        result = self.redis.eval(_RELEASE_LUA, 1, key, holder)
        return bool(result)

    def renew(self, execution_id: str, holder: str, ttl_seconds: int) -> bool:
        key = self._key(execution_id)
        result = self.redis.eval(_RENEW_LUA, 1, key, holder, str(ttl_seconds))
        return bool(result)


class NullExecutionLease:
    """No-op lease for memory / single-process FlowWorker tests."""

    def try_acquire(self, execution_id: str, holder: str, ttl_seconds: int) -> bool:
        return True

    def release(self, execution_id: str, holder: str) -> bool:
        return True

    def renew(self, execution_id: str, holder: str, ttl_seconds: int) -> bool:
        return True


def new_holder_token(prefix: str = "worker") -> str:
    return f"{prefix}:{uuid.uuid4().hex[:16]}"
