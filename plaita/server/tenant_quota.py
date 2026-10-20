"""每租户 start 准入配额（跨进程共享视图，plaita#28）。

缺陷（issue #28）：任务队列单键单 stream FIFO + worker 串行消费，无任何按
租户的并发上限/准入控制——一个租户批量启动（或 schedule 风暴、map 大扇出）
即可占满全部 worker，其他租户无限期排队；没有拒绝/降级路径，也没有
「每租户并发 run 数」这一最基本的配额概念。

最小公平性原语（issue 建议的「最小可行」）：
- **名额**：每租户一个 ZSET ``plaita:tenant_running:{tenant_id}``（平台机制键，
  不按租户 namespace 分区，与 ``plaita:tenant_status`` 同款），成员=execution_id、
  score=过期时刻（``now + TTL``）。score 过期语义给出**崩溃自愈**：worker 在
  「认领 → 终态释放」之间崩溃时名额条目自行过期，租户不会被死名额永久卡住
  （TTL 默认 24h，``PLAITA_TENANT_RUNNING_TTL_SECONDS`` 可调）。
- **上限**：per-tenant HASH ``plaita:tenant_quota``（tenant_id → 最大并发 start）
  覆盖全局默认 env ``PLAITA_TENANT_MAX_RUNNING``；都缺省 / 值 ≤0 / 解析失败 =
  不限（fail-open，零行为变化）。「彻底停用一个租户」是 #27 tenant_status 的
  职责，本模块不做 0=全拒语义。
- **准入**（``try_admit_tenant_start``）：Lua 原子「清过期 → 已是成员则放行
  （幂等，消息重投安全）→ 未满则占位」；返回 False = 该租户 start 配额已满。
  调用方约定：**console 入口**超限 → HTTP 429（积压根本不形成——单 stream
  FIFO 下这是唯一有效的拦截位置）；**worker 侧**对无名额的外部生产方消息
  超限 → 延迟重投（排队），见 ``flow_worker.TenantQuotaDeferred``。
- **释放**（``release_tenant_start``）：执行终态（completed/error/cancelled）
  时 ZREM，幂等、尽力而为。worker 侧统一收口在 ``_persist_state_or_raise``；
  suspended 不释放（执行仍在被 admitted 的生命周期内，resume 复用名额）。

fail-open 矩阵（与 tenant_status 同哲学：闸门自身故障不放大成全局拒绝）：
无 redis 客户端 / 无 execution_id / 未配置上限 / Redis 读写失败 → 一律放行。
回滚开关 ``PLAITA_DISABLE_TENANT_QUOTA=1`` 由 worker 侧读取（本模块保持纯函数，
不读该开关）。

配额是**软上限**：admit 与 release 之间的进程崩溃窗口由 TTL 兜底，长于 TTL
的合法运行会提前让出名额（退化为现状行为，不会拒绝卡死）。
"""
from __future__ import annotations

import logging
import os
import time
from typing import Optional
from plaita.core.url_redaction import _scrub_exception_text, _redact_env_value

logger = logging.getLogger(__name__)

# per-tenant 上限覆盖（HASH: tenant_id -> 最大并发 start）；console/worker 共用。
TENANT_QUOTA_KEY = "plaita:tenant_quota"
# 全局默认上限 env（0/缺失/非法 = 不限）。
TENANT_RUNNING_ENV = "PLAITA_TENANT_MAX_RUNNING"
# 名额条目 TTL env（秒；≤0/非法 = 默认值）。
TENANT_RUNNING_TTL_ENV = "PLAITA_TENANT_RUNNING_TTL_SECONDS"
DEFAULT_RUNNING_TTL_SECONDS = 86400

# 名额键前缀（平台机制键，不分区）：plaita:tenant_running:{tenant_id}
_RUNNING_KEY_PREFIX = "plaita:tenant_running:"

# admit 的原子语义（KEYS[1]=名额 ZSET；ARGV: now, ttl, cap, member）：
# 1) 清过期成员（崩溃自愈）；2) 已是成员 → 幂等放行（重投安全）；
# 3) 未满 → ZADD 占位并续 key TTL；4) 已满 → 拒。
# cap 传超大数表示「不限」（调用方已在 Python 侧短路，双保险）。
_ADMIT_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZSCORE', KEYS[1], ARGV[4]) then
  redis.call('ZADD', KEYS[1], tonumber(ARGV[1]) + tonumber(ARGV[2]), ARGV[4])
  redis.call('EXPIRE', KEYS[1], math.ceil(tonumber(ARGV[2])) + 60)
  return 1
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
  return 0
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[1]) + tonumber(ARGV[2]), ARGV[4])
redis.call('EXPIRE', KEYS[1], math.ceil(tonumber(ARGV[2])) + 60)
return 1
"""

_UNLIMITED_CAP = 2 ** 62  # Lua 侧 double 精度内的「不限」哨兵


def tenant_running_key(tenant_id: Optional[str]) -> str:
    """名额 ZSET 键（平台机制键，不按租户 namespace 分区）。"""
    return f"{_RUNNING_KEY_PREFIX}{tenant_id or 'default'}"


def _running_entry_ttl_seconds() -> float:
    raw = os.environ.get(TENANT_RUNNING_TTL_ENV, "")
    if not raw:
        return float(DEFAULT_RUNNING_TTL_SECONDS)
    try:
        ttl = float(raw)
    except (TypeError, ValueError):
        logger.warning(
            "%s=%s 非法，回退默认 %s 秒（B2R31-03：不静默）",
            TENANT_RUNNING_TTL_ENV, _redact_env_value(raw), DEFAULT_RUNNING_TTL_SECONDS,
        )
        return float(DEFAULT_RUNNING_TTL_SECONDS)
    if ttl <= 0:
        logger.warning(
            "%s=%s ≤0，回退默认 %s 秒（不是永久/TTL 关闭）",
            TENANT_RUNNING_TTL_ENV, ttl, DEFAULT_RUNNING_TTL_SECONDS,
        )
        return float(DEFAULT_RUNNING_TTL_SECONDS)
    return ttl


def tenant_max_running(redis_client, tenant_id: Optional[str]) -> Optional[int]:
    """解析租户 start 并发上限；None = 不限（fail-open）。

    优先级：per-tenant HASH 覆盖 > env 全局默认。值 ≤0 / 非法 / 无记录均视为
    未配置（不限）——「0=全拒」语义归 #27 tenant_status（disabled）。
    """
    if redis_client is None:
        return None
    try:
        raw = redis_client.hget(TENANT_QUOTA_KEY, tenant_id or "default")
    except Exception as exc:  # noqa: BLE001 — Redis 抖动不误伤全租户
        logger.warning(
            "读取租户 %s 配额失败，按不限处理: %s", tenant_id, _scrub_exception_text(exc),
        )
        return None
    if raw is not None:
        try:
            value = int(raw.decode() if isinstance(raw, bytes) else raw)
        except (TypeError, ValueError):
            logger.warning(
                "租户 %s 的配额覆盖值 %r 非法，按不限处理（B2R31-03：不静默）",
                tenant_id or "default", _redact_env_value(raw),
            )
            return None
        if value <= 0:
            logger.warning(
                "租户 %s 的配额覆盖值 %s ≤0，按不限处理（0=不限；全拒请用 disabled）",
                tenant_id or "default", value,
            )
            return None
        return value
    raw = os.environ.get(TENANT_RUNNING_ENV, "")
    if not raw:
        return None
    from plaita.runtime_config import ConfigError, parse_int

    try:
        value = parse_int(raw, TENANT_RUNNING_ENV, minimum=0)
    except ConfigError as exc:
        # 启动预检会 fail-fast；库内/运行期按「不限」继续但必须 loud，绝不静默。
        logger.warning(
            "%s；按不限处理（启动预检会拒绝该配置，B2R31-03）",
            _scrub_exception_text(exc),
        )
        return None
    if value == 0:
        logger.warning(
            "%s=0：文档语义为「不限」（非「全拒」）；如需全拒请用租户 disabled",
            TENANT_RUNNING_ENV,
        )
        return None
    return value


def _purge_expired(redis_client, key: str, now: float) -> None:
    try:
        redis_client.zremrangebyscore(key, "-inf", now)
    except Exception as exc:  # noqa: BLE001 — 清扫失败交给 admit 的 Lua 内联清扫
        logger.debug("过期名额清扫失败（由 admit 的 Lua 内联清扫兜底）: %s",
                     _scrub_exception_text(exc))


def tenant_running_count(redis_client, tenant_id: Optional[str]) -> int:
    """当前占用的 start 名额数（先清过期再计数；故障/无客户端 = 0）。"""
    if redis_client is None:
        return 0
    key = tenant_running_key(tenant_id)
    try:
        _purge_expired(redis_client, key, time.time())
        return int(redis_client.zcard(key))
    except Exception as exc:  # noqa: BLE001 — 观测口径绝不抛
        logger.warning("读取租户 %s 名额计数失败: %s", tenant_id, _scrub_exception_text(exc))
        return 0


def try_admit_tenant_start(
    redis_client, tenant_id: Optional[str], execution_id: Optional[str]
) -> bool:
    """start 准入：占一个该租户的并发名额。

    返回 True = 放行（新占名额，或该 execution 已有名额——消息重投幂等）；
    False = 该租户配额已满，调用方按入口语义处置（console 429 / worker 延迟）。

    fail-open（True）：无 redis / 无 execution_id（旧生产方消息不可跟踪）/
    未配置上限 / Redis 故障。保持「未配额即现状」的零行为变化承诺。
    """
    if redis_client is None or not execution_id:
        return True
    cap = tenant_max_running(redis_client, tenant_id)
    if cap is None:
        return True
    key = tenant_running_key(tenant_id)
    now = time.time()
    ttl = _running_entry_ttl_seconds()
    try:
        allowed = redis_client.eval(
            _ADMIT_LUA,
            1,
            key,
            f"{now:.6f}",
            f"{max(1.0, ttl):.6f}",
            str(cap),
            execution_id,
        )
    except Exception as exc:  # noqa: BLE001 — 闸门故障不放大成全局拒绝
        logger.warning(
            "租户 %s start 准入检查失败（fail-open 放行）: execution=%s: %s",
            tenant_id or "default", execution_id, _scrub_exception_text(exc),
        )
        return True
    return bool(allowed)


def release_tenant_start(
    redis_client, tenant_id: Optional[str], execution_id: Optional[str]
) -> None:
    """释放一个 start 名额（执行终态时调用；幂等、尽力而为）。"""
    if redis_client is None or not execution_id:
        return
    key = tenant_running_key(tenant_id)
    try:
        redis_client.zrem(key, execution_id)
    except Exception as exc:  # noqa: BLE001 — 释放失败由条目 TTL 兜底自愈
        logger.warning(
            "释放租户 %s start 名额失败（TTL 兜底）: execution=%s: %s",
            tenant_id or "default", execution_id, _scrub_exception_text(exc),
        )


def publish_tenant_quota(
    redis_client, tenant_id: Optional[str], max_running: Optional[int]
) -> None:
    """发布/更新 per-tenant 上限（max_running=None 删除记录=不限）。

    console/运维写共享视图（与 publish_tenant_status 同款协作方式）；
    失败不抛——配额发布是尽力而为的控制面操作。
    """
    if redis_client is None or not tenant_id:
        return
    try:
        if max_running is None:
            redis_client.hdel(TENANT_QUOTA_KEY, tenant_id)
        else:
            redis_client.hset(TENANT_QUOTA_KEY, key=tenant_id, value=str(int(max_running)))
    except Exception as exc:  # noqa: BLE001
        logger.warning("租户 %s 配额发布失败: %s", tenant_id, _scrub_exception_text(exc))
