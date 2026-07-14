"""Execution resume lease: at most one worker resumes a given execution_id."""
from __future__ import annotations

import logging
import uuid
from typing import Optional, Protocol, Union

logger = logging.getLogger("plaita.server.execution_lease")

DEFAULT_LEASE_TTL_SECONDS = 120
DEFAULT_KEY_PREFIX = "plaita:execution:lease:"

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


class ExecutionLeaseError(RuntimeError):
    """Raised when resume cannot acquire or keep the execution lease."""


class ExecutionLease(Protocol):
    def try_acquire(self, execution_id: str, holder: str, ttl_seconds: int) -> bool: ...

    def release(self, execution_id: str, holder: str) -> bool: ...

    def renew(self, execution_id: str, holder: str, ttl_seconds: int) -> bool: ...


def _decode(value: Union[str, bytes, None]) -> Optional[str]:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else value


class RedisExecutionLease:
    """Redis SET NX EX lease keyed by execution_id."""

    def __init__(self, redis_client, key_prefix: str = DEFAULT_KEY_PREFIX):
        self.redis = redis_client
        self.key_prefix = key_prefix

    def _key(self, execution_id: str) -> str:
        return f"{self.key_prefix}{execution_id}"

    def try_acquire(self, execution_id: str, holder: str, ttl_seconds: int) -> bool:
        key = self._key(execution_id)
        ok = self.redis.set(key, holder, nx=True, ex=ttl_seconds)
        return bool(ok)

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
