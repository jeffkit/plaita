"""start dedup 预认领状态机（r59 设计级收口）。

背景：``dedup_key`` 的受理窗口在 #274→#280→#285 三轮补丁后仍被打穿（#286
release TOCTOU、#287 owner 键迁移）。本模块把 claim/续期/占位行/入队
reservation/owner/release 收进一组**token 校验的 Lua CAS 原语**，console 与
schedule 共用；worker 侧仍只认 ``{ns}:start-dedup:{key} = execution_id``。

状态（owner 值；token 存独立键 ``{ns}:start-dedup-token:{key}``）::

    none ──claim──> claiming:<id> ──row 落盘──> claiming:<id>
                         │                          │
                         └──reserve_enqueue──> enqueueing:<id>
                                                    │
                                                enqueue XADD
                                                    │
                                              mark_enqueued
                                                    ↓
                                              enqueued:<id>
    claiming/enqueueing ──release(token=我)──> none（失败清理）
    none/claiming(+row 缺失/墓碑) ──release(token 无)──> none（孤儿回收）

每次转换都校验「map==我的 id、owner 状态、token、墓碑」；release 在 Lua 内
原子复核 row/墓碑，不再有 GET→DEL 窗口。旧格式 ``<map>:owner`` 作为**只读
迁移来源**：新键缺失时按旧值视为 legacy claiming/enqueued；必要时
``adopt_legacy`` 迁移到新键。写路径只写新键（滚动升级要求 console 先排空
旧实例，见 ops-runbook）。
"""
from __future__ import annotations

import logging
import re
import secrets
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional

from ..core.url_redaction import _scrub_url_text
from ..tenant_context import tenant_namespace
from .task_queue import START_DEDUP_TTL_SECONDS

logger = logging.getLogger(__name__)

CLAIM_TTL_SECONDS = 60
STATE_CLAIMING = "claiming"
STATE_ENQUEUEING = "enqueueing"
STATE_ENQUEUED = "enqueued"

_OWNER_RE = re.compile(
    r"^(claiming|enqueueing|enqueued):([^:]+)(?::([^:]+))?$"
)
_LEGACY_OWNER_RE = re.compile(r"^(claiming|enqueued):([^:]+)$")


def map_key(tenant: Optional[str], dedup_key: str) -> str:
    from .task_queue import start_dedup_map_key

    return start_dedup_map_key(tenant_namespace(tenant), dedup_key)


def owner_key(tenant: Optional[str], dedup_key: str) -> str:
    return f"{tenant_namespace(tenant)}:start-dedup-owner:{dedup_key}"


def token_key(tenant: Optional[str], dedup_key: str) -> str:
    return f"{tenant_namespace(tenant)}:start-dedup-token:{dedup_key}"


def legacy_owner_key(tenant: Optional[str], dedup_key: str) -> str:
    return f"{map_key(tenant, dedup_key)}:owner"


def execution_row_key(tenant: Optional[str], execution_id: str) -> str:
    return f"{tenant_namespace(tenant)}:execution:{execution_id}"


def tombstone_key(execution_id: str) -> str:
    return f"plaita:exec_tombstone:{execution_id}"


def new_token() -> str:
    return secrets.token_hex(16)


@dataclass(frozen=True)
class StartClaim:
    """claim 结果；``claimed/mapped`` 保持二元组解包兼容（历史调用方）。"""

    claimed: bool
    mapped: Optional[str]
    token: Optional[str] = None
    legacy: bool = False
    owner_state: Optional[str] = None
    error: Optional[str] = None

    def __iter__(self) -> Iterator[Any]:
        yield self.claimed
        yield self.mapped


@dataclass(frozen=True)
class OwnerView:
    state: Optional[str]
    execution_id: Optional[str]
    token: Optional[str]
    legacy: bool

    @property
    def live(self) -> bool:
        return self.state in (STATE_CLAIMING, STATE_ENQUEUEING, STATE_ENQUEUED)


_EMPTY_OWNER = OwnerView(None, None, None, False)


def _text(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    return raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)


def _parse_owner(raw: Any, *, legacy: bool) -> OwnerView:
    text = _text(raw)
    if not text:
        return _EMPTY_OWNER
    m = (_LEGACY_OWNER_RE if legacy else _OWNER_RE).match(text)
    if not m:
        return _EMPTY_OWNER
    state, execution_id = m.group(1), m.group(2)
    token = None if legacy else m.group(3)
    return OwnerView(state, execution_id, token, legacy)


def read_owner(redis, tenant: Optional[str], dedup_key: str) -> OwnerView:
    """新键优先；缺失时只读旧 ``<map>:owner``（迁移窗口）。"""
    try:
        view = _parse_owner(redis.get(owner_key(tenant, dedup_key)), legacy=False)
        if view.live:
            return view
        return _parse_owner(
            redis.get(legacy_owner_key(tenant, dedup_key)), legacy=True
        )
    except Exception as exc:  # noqa: BLE001 — 读不到按未知（调用方 fail-closed）
        logger.warning(
            "start-dedup owner 读取失败: %s: %s",
            map_key(tenant, dedup_key), _scrub_url_text(exc),
        )
        return _EMPTY_OWNER


def read_mapped(redis, tenant: Optional[str], dedup_key: str) -> Optional[str]:
    try:
        return _text(redis.get(map_key(tenant, dedup_key)))
    except Exception as exc:  # noqa: BLE001 — 读不到按无映射
        logger.warning(
            "start-dedup map 读取失败: %s: %s",
            map_key(tenant, dedup_key), _scrub_url_text(exc),
        )
        return None


_CLAIM_LUA = """
if not redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[4]) then
  return 0
end
redis.call('SET', KEYS[2], 'claiming:' .. ARGV[1], 'EX', ARGV[3])
redis.call('SET', KEYS[3], ARGV[1] .. ':' .. ARGV[2], 'EX', ARGV[3])
return 1
"""

_ADOPT_LEGACY_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
if redis.call('GET', KEYS[3]) then return 0 end
local legacy = redis.call('GET', KEYS[4])
if legacy ~= 'claiming:' .. ARGV[1] then return 0 end
redis.call('SET', KEYS[3], 'claiming:' .. ARGV[1], 'EX', ARGV[3])
redis.call('SET', KEYS[5], ARGV[1] .. ':' .. ARGV[2], 'EX', ARGV[3])
return 1
"""
_MIGRATE_COLLISION_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
local existing = redis.call('GET', KEYS[4])
if existing and existing ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[4], ARGV[1], 'EX', ARGV[4])
redis.call('SET', KEYS[5], ARGV[2] .. ':' .. ARGV[3], 'EX', ARGV[4])
redis.call('DEL', KEYS[1])
return 1
"""

_RESERVE_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
local want = 'claiming:' .. ARGV[1]
local tok = redis.call('GET', KEYS[3])
local owner = redis.call('GET', KEYS[4])
if tok ~= ARGV[1] .. ':' .. ARGV[2] then
  -- 慢写跨过 claiming TTL：map 仍属我且 owner/token 都缺失 ⇒ 用同一 token
  -- 原地续期并进入 enqueueing；被接管/墓碑仍拒绝。
  if tok or owner then return 0 end
elseif owner ~= want then
  return 0
end
redis.call('SET', KEYS[4], 'enqueueing:' .. ARGV[1], 'EX', ARGV[3])
redis.call('SET', KEYS[3], ARGV[1] .. ':' .. ARGV[2], 'EX', ARGV[3])
return 1
"""

_MARK_ENQUEUED_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
if redis.call('GET', KEYS[3]) ~= ARGV[1] .. ':' .. ARGV[2] then return 0 end
local owner = redis.call('GET', KEYS[4])
if owner ~= 'enqueueing:' .. ARGV[1] and owner ~= 'claiming:' .. ARGV[1] then
  return 0
end
redis.call('SET', KEYS[4], 'enqueued:' .. ARGV[1], 'EX', ARGV[3])
redis.call('SET', KEYS[3], ARGV[1] .. ':' .. ARGV[2], 'EX', ARGV[3])
return 1
"""

_GUARD_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
local want = ARGV[3] .. ':' .. ARGV[1]
local tok = redis.call('GET', KEYS[3])
local owner = redis.call('GET', KEYS[4])
if tok ~= ARGV[1] .. ':' .. ARGV[2] then
  -- 慢写跨过 claiming TTL：映射仍属我且无人接管 ⇒ 用同一 token 原地续期。
  if ARGV[5] ~= '1' or tok or owner then return 0 end
elseif owner ~= want then
  return 0
end
redis.call('SET', KEYS[4], want, 'EX', ARGV[4])
redis.call('SET', KEYS[3], ARGV[1] .. ':' .. ARGV[2], 'EX', ARGV[4])
return 1
"""

_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local token = redis.call('GET', KEYS[3])
local owner = redis.call('GET', KEYS[4])
local tomb = redis.call('EXISTS', KEYS[5])
if ARGV[2] ~= '' then
  -- 认领者自己的失败清理：token 必须匹配，owner 不得已 enqueued。
  if token ~= ARGV[1] .. ':' .. ARGV[2] then return 0 end
  if owner ~= 'claiming:' .. ARGV[1] and owner ~= 'enqueueing:' .. ARGV[1] then
    return 0
  end
  if ARGV[3] == '1' and redis.call('EXISTS', KEYS[6]) == 1 then return 0 end
  redis.call('DEL', KEYS[1], KEYS[3], KEYS[4])
  return 1
end
-- 孤儿回收（无 token）：墓碑在位 ⇒ 该 id 已死，允许清映射/新 owner/token。
if tomb == 1 then
  -- 墓碑 = 执行已死：即使 row 残留也允许清映射（worker 侧墓碑闸兜底）。
  redis.call('DEL', KEYS[1], KEYS[3], KEYS[4])
  return 1
end
-- 无墓碑：只有「无 token 且 owner 缺失/claiming」才算可回收孤儿；
-- enqueued/enqueueing 或新 token 在位 ⇒ 认领仍活，拒绝。
if token then return 0 end
if owner then
  if owner ~= 'claiming:' .. ARGV[1] then return 0 end
else
  local legacy = redis.call('GET', KEYS[7])
  if legacy and legacy ~= 'claiming:' .. ARGV[1] then return 0 end
end
if ARGV[3] == '1' and redis.call('EXISTS', KEYS[6]) == 1 then return 0 end
redis.call('DEL', KEYS[1], KEYS[3], KEYS[4])
return 1
"""

_RECOVER_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
if redis.call('GET', KEYS[3]) then return 0 end
if redis.call('EXISTS', KEYS[4]) ~= 1 then return 0 end
local owner = redis.call('GET', KEYS[5])
if owner then
  if owner ~= 'claiming:' .. ARGV[1] then return 0 end
else
  local legacy = redis.call('GET', KEYS[6])
  if legacy and legacy ~= 'claiming:' .. ARGV[1] then return 0 end
end
redis.call('SET', KEYS[5], 'enqueueing:' .. ARGV[1], 'EX', ARGV[3])
redis.call('SET', KEYS[3], ARGV[1] .. ':' .. ARGV[2], 'EX', ARGV[3])
return 1
"""


def _reverse_collision_base(dedup_key: str) -> Optional[str]:
    suffix = ":owner"
    if dedup_key.endswith(suffix) and len(dedup_key) > len(suffix):
        return dedup_key[: -len(suffix)]
    return None


def _migrate_reverse_collision(
    redis, tenant: Optional[str], dedup_key: str, marker: str
) -> bool:
    """旧 owner 键与另一个合法 map 键共键时，把 owner 迁到新键后释放该键。"""
    base = _reverse_collision_base(dedup_key)
    if base is None:
        return False
    legacy = _parse_owner(marker, legacy=True)
    if not legacy.live or not legacy.execution_id:
        return False
    ttl = (
        START_DEDUP_TTL_SECONDS
        if legacy.state == STATE_ENQUEUED
        else CLAIM_TTL_SECONDS
    )
    try:
        return bool(
            redis.eval(
                _MIGRATE_COLLISION_LUA,
                5,
                map_key(tenant, dedup_key),
                map_key(tenant, base),
                tombstone_key(legacy.execution_id),
                owner_key(tenant, base),
                token_key(tenant, base),
                marker,
                legacy.execution_id,
                new_token(),
                str(ttl),
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "start-dedup 旧 owner 反向碰撞迁移失败: %s: %s", dedup_key, _scrub_url_text(exc)
        )
        return False


def claim(
    redis,
    tenant: Optional[str],
    dedup_key: str,
    execution_id: str,
    *,
    token: Optional[str] = None,
    claim_ttl: int = CLAIM_TTL_SECONDS,
    map_ttl: int = START_DEDUP_TTL_SECONDS,
) -> StartClaim:
    """原子认领（map SET NX + owner claiming + token）。"""
    token = token or new_token()
    try:
        claimed = bool(
            redis.eval(
                _CLAIM_LUA,
                3,
                map_key(tenant, dedup_key),
                owner_key(tenant, dedup_key),
                token_key(tenant, dedup_key),
                execution_id,
                token,
                str(max(1, int(claim_ttl))),
                str(max(1, int(map_ttl))),
            )
        )
    except Exception as exc:  # noqa: BLE001 — 认领瞬断：fail-closed
        logger.warning(
            "start-dedup 认领失败（fail-closed）: %s: %s", dedup_key, _scrub_url_text(exc)
        )
        return StartClaim(False, None, None, error="claim_failed")
    if claimed:
        return StartClaim(True, None, token)

    raw_map = read_mapped(redis, tenant, dedup_key)
    # 旧 `<base>:owner` 与 key/base+":owner" 的 map 共键：先迁 owner 再重试。
    if raw_map and _LEGACY_OWNER_RE.match(raw_map) and _reverse_collision_base(
        dedup_key
    ):
        if _migrate_reverse_collision(redis, tenant, dedup_key, raw_map):
            return claim(
                redis,
                tenant,
                dedup_key,
                execution_id,
                token=token,
                claim_ttl=claim_ttl,
                map_ttl=map_ttl,
            )
        return StartClaim(False, None, None, error="legacy_owner_collision")
    mapped = raw_map
    owner = read_owner(redis, tenant, dedup_key)
    if mapped and owner.legacy and owner.state == STATE_CLAIMING:
        # 旧 claiming 无 token：若映射仍是它，迁移到新键（只读兼容→写新键）。
        try:
            adopted = bool(
                redis.eval(
                    _ADOPT_LEGACY_LUA,
                    5,
                    map_key(tenant, dedup_key),
                    tombstone_key(mapped),
                    owner_key(tenant, dedup_key),
                    legacy_owner_key(tenant, dedup_key),
                    token_key(tenant, dedup_key),
                    mapped,
                    token,
                    str(max(1, int(claim_ttl))),
                )
            )
        except Exception as exc:  # noqa: BLE001 — 迁移失败按未迁移（不阻断受理）
            logger.warning(
                "start-dedup 旧 owner 迁移失败: %s: %s",
                dedup_key, _scrub_url_text(exc),
            )
            adopted = False
        if adopted:
            return StartClaim(
                False, mapped, token, legacy=True, owner_state=STATE_CLAIMING
            )
    return StartClaim(
        False, mapped, owner.token, legacy=owner.legacy,
        owner_state=owner.state,
    )


def reserve_enqueue(
    redis,
    tenant: Optional[str],
    dedup_key: str,
    execution_id: str,
    token: str,
    *,
    ttl: int = CLAIM_TTL_SECONDS,
) -> bool:
    """claimed/row_written → enqueueing（入队前的原子 reservation）。"""
    try:
        return bool(
            redis.eval(
                _RESERVE_LUA,
                4,
                map_key(tenant, dedup_key),
                tombstone_key(execution_id),
                token_key(tenant, dedup_key),
                owner_key(tenant, dedup_key),
                execution_id,
                token,
                str(max(1, int(ttl))),
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("start-dedup reserve 失败: %s: %s", dedup_key, _scrub_url_text(exc))
        return False


def mark_enqueued(
    redis,
    tenant: Optional[str],
    dedup_key: str,
    execution_id: str,
    token: str,
    *,
    ttl: int = START_DEDUP_TTL_SECONDS,
) -> bool:
    """enqueueing/claiming → enqueued（owner CAS，绝不被旧认领者覆盖）。"""
    try:
        return bool(
            redis.eval(
                _MARK_ENQUEUED_LUA,
                4,
                map_key(tenant, dedup_key),
                tombstone_key(execution_id),
                token_key(tenant, dedup_key),
                owner_key(tenant, dedup_key),
                execution_id,
                token,
                str(max(1, int(ttl))),
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("start-dedup mark_enqueued 失败: %s: %s", dedup_key, _scrub_url_text(exc))
        return False


def adopt_for_recovery(
    redis,
    tenant: Optional[str],
    dedup_key: str,
    execution_id: str,
    *,
    claim_ttl: int = CLAIM_TTL_SECONDS,
) -> Optional[str]:
    """行已落、无 token/owner 的崩溃恢复：原子建立 enqueueing reservation。

    返回新 token；不满足（映射换主/墓碑/token 已在/行缺失/owner 已 enqueued）
    返回 None。调用方随后补入队并 ``mark_enqueued``。
    """
    token = new_token()
    try:
        ok = redis.eval(
            _RECOVER_LUA,
            6,
            map_key(tenant, dedup_key),
            tombstone_key(execution_id),
            token_key(tenant, dedup_key),
            execution_row_key(tenant, execution_id),
            owner_key(tenant, dedup_key),
            legacy_owner_key(tenant, dedup_key),
            execution_id,
            token,
            str(max(1, int(claim_ttl))),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("start-dedup 恢复 reservation 失败: %s: %s", dedup_key, _scrub_url_text(exc))
        return None
    return token if ok else None


def guard(
    redis,
    tenant: Optional[str],
    dedup_key: str,
    execution_id: str,
    token: str,
    *,
    state: str = STATE_CLAIMING,
    ttl: int = CLAIM_TTL_SECONDS,
    allow_missing_claim: bool = True,
) -> bool:
    """写前复核 + 续期：map/owner/token/墓碑全部匹配才放行。

    ``allow_missing_claim``：owner/token 都缺失但 map 仍属我（慢写跨过
    claiming TTL、无人接管）时用同一 token 原地续期；被接管/墓碑仍拒绝。
    """
    try:
        return bool(
            redis.eval(
                _GUARD_LUA,
                4,
                map_key(tenant, dedup_key),
                tombstone_key(execution_id),
                token_key(tenant, dedup_key),
                owner_key(tenant, dedup_key),
                execution_id,
                token,
                state,
                str(max(1, int(ttl))),
                "1" if allow_missing_claim else "0",
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("start-dedup guard 失败（fail-closed）: %s: %s", dedup_key, _scrub_url_text(exc))
        return False


def release(
    redis,
    tenant: Optional[str],
    dedup_key: str,
    execution_id: str,
    *,
    token: Optional[str] = None,
    require_row_absent: bool = False,
) -> bool:
    """条件释放（Lua 原子复核 map/owner/token/row/墓碑）。

    - ``token`` 给出：认领者自己的失败清理（owner 不得已 enqueued）；
    - ``token=None``：孤儿回收。墓碑在位可直接清；否则只允许无 token 且
      owner 缺失/claiming，row 存在则拒绝（#286 的 stale release 在此失败）。
    """
    try:
        return bool(
            redis.eval(
                _RELEASE_LUA,
                7,
                map_key(tenant, dedup_key),
                "",
                token_key(tenant, dedup_key),
                owner_key(tenant, dedup_key),
                tombstone_key(execution_id),
                execution_row_key(tenant, execution_id),
                legacy_owner_key(tenant, dedup_key),
                execution_id,
                token or "",
                "1" if require_row_absent else "0",
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("start-dedup release 失败: %s: %s", dedup_key, _scrub_url_text(exc))
        return False


def snapshot(redis, tenant: Optional[str], dedup_key: str) -> Dict[str, Any]:
    """诊断投影（只读；测试/审计用）。"""
    owner = read_owner(redis, tenant, dedup_key)
    mapped = read_mapped(redis, tenant, dedup_key)
    try:
        row = bool(mapped and redis.exists(execution_row_key(tenant, mapped)))
        tomb = bool(mapped and redis.exists(tombstone_key(mapped)))
    except Exception as exc:  # noqa: BLE001 — 诊断投影读不到按 false
        logger.debug(
            "start-dedup snapshot 读取失败: %s: %s", mapped, _scrub_url_text(exc)
        )
        row = tomb = False
    return {
        "mapped": mapped,
        "owner_state": owner.state,
        "owner_id": owner.execution_id,
        "token": owner.token,
        "legacy": owner.legacy,
        "row": row,
        "tombstoned": tomb,
    }
