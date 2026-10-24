"""存量 checkpoint 的 $ENV 快照一次性清除（plaita#326 E1，owner 裁决③）。

背景：#326 落地后 checkpoint **不再落** ``$ENV`` 快照（``_persist_state_or_raise``
持久化咽喉点 strip，恢复路径按当前 worker env 重注）。但存量行——终态行
TTL 30 天内、非终态行不过期——context 里还带着旧快照（值与 allowlist 键名，
owner 裁决④：键名泄露技术栈，同源脱敏）。本脚本一次性清掉。

#338：清除面从顶层键扩到嵌套挂起帧——``$SUSPEND_FRAMES[*].context`` 及帧内
``$PARENT`` 链的 ``$ENV`` 与 worker 咽喉 strip 同源（``plaita.core.env_checkpoint.
strip_env_snapshot``）；顶层无 ``$ENV`` 但帧面有的存量行不再 ``kept`` 短路。

对账器顺带面定位（owner 裁决③「可挂对账器顺带面」）：与 #316
``carrier_audit`` / #321 ``reconciler_shadow`` 同族（scripts/ 对账家族），
单发运行（systemd oneshot / 手动），**不新增常驻进程**；验收按降级口径
（脚本可跑 + 新执行 secret env grep 不到），不进 #326 验收硬门槛。

范围语义：「TTL 30 天内旧 checkpoint」= 当时仍存活的执行状态键——
``{ns}:execution:<id>``（终态 30 天 TTL 自灭；非终态活动行不过期）。
超期行已自灭无需清理；新执行经咽喉点 strip 天然无 ``$ENV``，本脚本对它们
是 no-op（字节预滤 ``b'"$ENV"'`` 不命中即跳过）。

原子性（防与 worker 持久化竞态回滚）：读-改-写不是原子，worker 在脚本的
GET 与 SET 之间落盘会被本脚本的旧值覆盖（丢更新）。因此改写走 **CAS Lua**：
仅当键值仍等于脚本读到的字节时才写回——worker 已推进的行 CAS 不中即跳过
（其新值已无 ``$ENV``），绝不回滚。

TTL 保持：改写时按读取时的 TTL 重设（``EX``）；无 TTL 行（非终态）保持
不过期。TTL 读取与 CAS 写之间过期由 CAS 字节比对兜底（键已消失 → 不写）。

用法（与对账家族同形制）：
    export PLAITA_PURGE_ENV_REDIS_URL=redis://localhost:6379/0
    python scripts/purge_env_from_checkpoints.py              # 干跑（只报告）
    python scripts/purge_env_from_checkpoints.py --apply      # 实际清除
    python scripts/purge_env_from_checkpoints.py --namespace myns --apply

回滚：无需——清除的是 owner 裁决定为不落盘的数据；误清（活动执行在飞）
由恢复路径重注自愈（值漂移 owner 裁决①接受）。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger("purge_env_from_checkpoints")

# #338：strip 与 worker 咽喉/恢复重注同源（plaita.core.env_checkpoint）——
# 脚本以仓内路径运行（对账家族 carrier_audit 同款 import plaita 形制）。
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from plaita.core.env_checkpoint import strip_env_snapshot  # noqa: E402

#: Redis 连接 env（与对账家族同款：URL 显式，不做隐式默认连接）。
REDIS_URL_ENV = "PLAITA_PURGE_ENV_REDIS_URL"

#: 缺省命名空间（``plaita:execution:<id>`` 的 ``plaita`` 段）。
DEFAULT_NAMESPACE = "plaita"

#: 改写的 CAS Lua：字节级比对（worker 并发落盘即不中），TTL 按读取值重设。
_CAS_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  if tonumber(ARGV[3]) > 0 then
    redis.call('set', KEYS[1], ARGV[2], 'EX', ARGV[3])
  else
    redis.call('set', KEYS[1], ARGV[2])
  end
  return 1
end
return 0
"""


def execution_state_keys(client, namespace: str):
    """枚举 ``{ns}:execution:<id>`` 状态键（排除 index 等机制键）。

    成员判别与 ``RedisExecutionStorage._index_member_id`` 同款：后缀无冒号
    且不为 ``index``；索引键/机制键后缀必含冒号或为保留名。用 SCAN 不用
    KEYS（对账家族只读纪律的扫描面同源）。
    """
    prefix = f"{namespace}:execution:"
    pattern = f"{prefix}*"
    scan = getattr(client, "scan_iter", None)
    keys = scan(match=pattern) if callable(scan) else client.keys(pattern)
    for key in keys:
        key = key.decode() if isinstance(key, bytes) else key
        suffix = key[len(prefix):]
        if not suffix or ":" in suffix or suffix == "index":
            continue
        yield key


def purge_one(client, key: str, *, apply: bool) -> str:
    """清单个状态键的 ``context.$ENV``。返回动作：kept/purged/cas_skipped。

    dry-run（``apply=False``）只检测不写。序列化走 plain ``json.dumps``——
    与 ``RedisExecutionStorage.serialize_state`` 同形制（state 值已由
    ``model_dump`` 保证 JSON 安全）；CAS 不中返回 ``cas_skipped``（worker
    已推进，新值无 $ENV，无需处理）。
    """
    raw = client.get(key)
    if raw is None:
        return "kept"
    if isinstance(raw, bytes):
        raw_t = raw.decode("utf-8", "surrogatepass")
    else:
        raw_t = raw
    # 字节预滤：绝大多数行（新执行/已清）不命中，零解析开销跳过。
    # （#338 起命中面含嵌套帧——帧内 $ENV 同样带 `"$ENV"` 字节。）
    if '"$ENV"' not in raw_t:
        return "kept"
    try:
        state = json.loads(raw_t)
    except (TypeError, ValueError):
        logger.warning("键 %s 反序列化失败（跳过，不动损坏行）", key)
        return "kept"
    context = state.get("context") if isinstance(state, dict) else None
    if not isinstance(context, dict):
        return "kept"
    # #338：解析后递归嵌套帧面（$SUSPEND_FRAMES[*].context 及帧内 $PARENT
    # 链，与 worker 咽喉 strip 同源 helper）——顶层无 $ENV 但帧面有的行
    # 不再 kept 短路；strip 未命中（净行）原对象返回即 kept。
    stripped = strip_env_snapshot(context)
    if stripped is context:
        return "kept"
    state["context"] = stripped
    if not apply:
        return "purged"
    try:
        ttl = int(client.ttl(key) or 0)
    except Exception:  # noqa: BLE001 — TTL 读失败按不过期处理（CAS 兜底）
        ttl = 0
    if ttl and ttl < 0:
        ttl = 0
    new_raw = json.dumps(state)
    try:
        purged = client.eval(
            _CAS_LUA, 1, key, raw_t, new_raw, str(max(ttl, 0))
        )
    except Exception as exc:  # noqa: BLE001 — 单键失败隔离
        logger.warning("键 %s CAS 改写失败（跳过）: %s", key, exc)
        return "kept"
    return "purged" if int(purged or 0) == 1 else "cas_skipped"


def purge_all(client, namespace: str = DEFAULT_NAMESPACE, *, apply: bool = False,
              sleep_seconds: float = 0.0) -> dict:
    """全量扫描并清除。返回计数报告（scanned/with_env/purged/cas_skipped）。"""
    report = {
        "scanned": 0,
        "with_env": 0,
        "purged": 0,
        "cas_skipped": 0,
        "apply": bool(apply),
    }
    for key in execution_state_keys(client, namespace):
        report["scanned"] += 1
        outcome = purge_one(client, key, apply=apply)
        if outcome == "purged":
            report["with_env"] += 1
            report["purged"] += 1
        elif outcome == "cas_skipped":
            report["with_env"] += 1
            report["cas_skipped"] += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return report


def build_client(redis_url: str):
    import redis

    # 连接超时口径与对账家族（carrier_audit/reap）一致（A2R20 卫生门）。
    return redis.Redis.from_url(
        redis_url,
        decode_responses=False,
        socket_connect_timeout=5.0,
        socket_timeout=30.0,
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="存量 checkpoint 的 $ENV 快照一次性清除（plaita#326）"
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get(REDIS_URL_ENV, ""),
        help=f"Redis 连接（缺省读 env {REDIS_URL_ENV}）",
    )
    parser.add_argument(
        "--namespace", default=DEFAULT_NAMESPACE,
        help=f"执行状态键命名空间（缺省 {DEFAULT_NAMESPACE}）",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="实际写回（缺省干跑，只报告会清多少）",
    )
    parser.add_argument(
        "--sleep", type=float, default=0.0, dest="sleep_seconds",
        help="每键间隔秒（限流用，缺省 0）",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not args.redis_url:
        parser.error(f"--redis-url 或 env {REDIS_URL_ENV} 必填")
    client = build_client(args.redis_url)
    report = purge_all(client, args.namespace, apply=args.apply,
                       sleep_seconds=args.sleep_seconds)
    logger.info(
        "purge_env_from_checkpoints: scanned=%(scanned)d with_env=%(with_env)d "
        "purged=%(purged)d cas_skipped=%(cas_skipped)d apply=%(apply)s",
        report,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
