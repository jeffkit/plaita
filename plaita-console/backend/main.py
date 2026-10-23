"""
Plaita Console Backend - FastAPI 应用入口
"""
import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import Redis

from plaita.writefile_jail import apply_writefile_jail
from plaita.redis_client import (
    CONSOLE_CONNECT_TIMEOUT_SECONDS,
    CONSOLE_SOCKET_TIMEOUT_SECONDS,
)
from plaita.storage.redis import validate_tombstone_retention
from plaita.tenant_context import (
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    set_current_tenant,
)
from plaita.core.url_redaction import register_mask_url_impl
from plaita.server.factory import _mask_url as _console_mask_url_impl

# A2R58-03：读面清洗器（detail/timeline/SSE 经 plaita.core.url_redaction 抹 URL
# 凭据分量）依赖的掩码实现 _mask_url 住在 plaita.server.factory（#88，server→core
# 向下注册）。console 进程此前零 factory 导入 ⇒ 未注册分支把一切绝对 URL（含零
# 凭据的诊断 URL）坠成 <unprintable-url>、丢 host:port，且每次 scrub 一条 WARNING。
# 与 worker（flow_worker 导入 factory）对称：进程入口注册一次，注册实现仍以
# factory 为唯一家（不另起第二套掩码文法）。
register_mask_url_impl(_console_mask_url_impl)

# 支持多种运行方式的导入
try:
    from .config import get_settings, warn_if_public_bind
    from .auth import require_auth, tenant_generation_context
    from .api import services, executions, queues, logs, cluster, events
    from .api import nodes, flows, flow_version, dryrun, copilot, schedules, credentials, audit, property_types, credential_templates
    from .api import tenants
    from .api import graphs
    from .api import supervisor
    from .api import metrics as metrics_api
    from .api import capacity as capacity_api
    from .services import flow_store, signature, users_svc
    from .api import auth_users
    from .error_contract import install_error_handlers
except ImportError:  # 平铺布局（cwd=backend 直接跑）；包布局不该走到这里
    if __package__ == "plaita_console":
        raise
    sys.path.insert(0, str(Path(__file__).parent))
    from config import get_settings, warn_if_public_bind
    from auth import require_auth, tenant_generation_context
    from api import services, executions, queues, logs, cluster, events
    from api import nodes, flows, flow_version, dryrun, copilot, schedules, credentials, audit, property_types, credential_templates  # type: ignore
    from api import tenants  # type: ignore
    from api import graphs  # type: ignore
    from api import supervisor  # type: ignore
    from api import metrics as metrics_api  # type: ignore
    from api import capacity as capacity_api  # type: ignore
    from services import flow_store, signature, users_svc  # type: ignore
    from api import auth_users  # type: ignore
    from error_contract import install_error_handlers  # type: ignore

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def is_api_path(path: str) -> bool:
    """路径是否落在 ``/api`` 命名空间（供 SPA 兜底拒绝用）。"""
    clean = (path or "").split("?", 1)[0].strip("/")
    return clean == "api" or clean.startswith("api/")


def build_health_body(app) -> dict:
    """``/health`` 响应体（模块级便于单测，不依赖完整 lifespan）。

    ``engine_sync`` 只在集群档存在：A2R20-06 的回填预算触界/失败时启动
    仍 ready，但这里必须暴露 ``degraded`` 与计数（降级必须可观测）。
    """
    body = {
        "status": "healthy",
        "local_mode": bool(getattr(app.state, "local_mode", False)),
    }
    summary = getattr(app.state, "reconcile_summary", None)
    if summary:
        body["reconcile"] = summary
    engine_sync = getattr(app.state, "engine_sync", None)
    if engine_sync:
        body["engine_sync"] = engine_sync
    # #299：发布 outbox 投递链状态（backlog / last error / 最近送达）。
    engine_outbox = getattr(app.state, "engine_outbox", None)
    if engine_outbox:
        body["engine_outbox"] = engine_outbox
    return body


def make_spa_static_files(directory):
    """构造 BrowserRouter 兜底静态挂载（模块级工厂，便于独立测试）。

    未命中的非 ``/api`` 路径回 ``index.html``；``/api/*`` 不得被 SPA 兜成
    200 HTML（S17-03 3C：前端 ``response.json()`` 会得到 ``Unexpected token '<'``）。
    """
    from fastapi.responses import JSONResponse
    from fastapi.staticfiles import StaticFiles
    from starlette.exceptions import HTTPException as StarletteHTTPException

    class SPAStaticFiles(StaticFiles):
        """BrowserRouter 深链接回退：未命中的静态路径一律回 index.html。"""

        async def get_response(self, path: str, scope):
            try:
                return await super().get_response(path, scope)
            except StarletteHTTPException as exc:
                if exc.status_code == 404:
                    scope_path = (scope or {}).get("path", "") or path
                    if is_api_path(path) or is_api_path(scope_path):
                        return JSONResponse(
                            {"detail": f"接口不存在: /{path.lstrip('/')}"},
                            status_code=404,
                        )
                    return await super().get_response("index.html", scope)
                raise

    return SPAStaticFiles(directory=directory, html=True)

# 全局 Redis 客户端
redis_client: Redis = None


# 建连/读超时：connect 默认 None 时遇到黑洞主机要等 OS SYN 重试窗口（~2 分钟）；
# read 默认随 redis-py 版本变化（5.x=∞ / 8.x=5s）——半开 Redis 会永久阻塞启动
# 与集群切换（后者还是单 worker 的事件循环）。两者都必须显式声明（A2R20-05）。
REDIS_CONNECT_TIMEOUT_SECONDS = CONSOLE_CONNECT_TIMEOUT_SECONDS
REDIS_SOCKET_TIMEOUT_SECONDS = CONSOLE_SOCKET_TIMEOUT_SECONDS


def connect_redis(url: str) -> Redis:
    """按 URL 建 console 数据面同步客户端（decode_responses）并 ping 验证可达。

    不可达时抛异常——lifespan 据此进本地单机模式，集群切换
    （``api/cluster.py:switch_cluster``）据此拒绝把数据面绑到连不上的 Redis。
    """
    client = Redis.from_url(
        url,
        decode_responses=True,
        # A53-11：DLQ/队列里可能出现非法 UTF-8（生产者 bug/二进制垃圾）。
        # 严格解码会在 xrevrange 客户端层直接抛 UnicodeDecodeError，一条毒丸
        # 让整个读面 500；replace 保留可诊断的替换字符，交给解析层容错。
        encoding_errors="replace",
        socket_connect_timeout=REDIS_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
    )
    client.ping()
    return client


def _active_cluster_redis_url() -> Optional[str]:
    """注册表里活动集群数据面的 Redis URL（读不到注册表时 None）。

    plaita#65：切换集群改的是注册表活动指针，重启后数据面必须还跟着它走，
    否则「页面显示集群 B、实际读写的还是 A」的错位在重启后重现。
    """
    try:
        from .services.cluster_registry import get_cluster_registry
    except ImportError:  # 平铺布局（cwd=backend）运行时
        from services.cluster_registry import get_cluster_registry  # type: ignore
    try:
        return get_cluster_registry().data_plane_redis_url()
    except Exception as e:  # noqa: BLE001 — 注册表不可读不该阻断启动
        logger.warning("读取活动集群 Redis URL 失败（退回环境变量）: %s", e)
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global redis_client

    # plaita#239 phase 2：显式 default 租户声明（单租户部署迁移行；请求任务
    # 从事件循环上下文继承，写路径不再静默降级 default）。
    set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
    settings = get_settings()

    # B2R31-03：console 启动预检（身份/安全/资源类非法值 fail-fast；可选调优
    # WARNING+安全默认）。与 worker 同口径，杜绝「console rc=0 / worker rc=1」。
    try:
        from plaita.runtime_config import (
            ConfigError,
            load_runtime_config,
            log_config_effective,
            preflight_security_and_limits,
        )

        preflight_security_and_limits()
        runtime_config = load_runtime_config("console", strict=True)
    except ConfigError as exc:
        logger.error("配置预检失败，console 拒绝启动：%s", exc)
        raise
    log_config_effective(logger, runtime_config)
    app.state.runtime_config = runtime_config

    # A69-04：启动期保留期配置校验（每进程一次）。墓碑 TTL 现在独立于 state
    # TTL，``<=0`` 不再表示永久；可疑配置（年龄闸关闭/显式墓碑过短/非法回退）
    # 只 WARNING 不阻断启动，但必须可观测。
    validate_tombstone_retention()

    # 启动时连接 Redis；不可达则进入本地单机模式（不阻断启动）。
    # app.state.redis_url 是「数据面当前连的是哪套 Redis」的真相源，集群切换
    # 会把两者一起改（见 switch_cluster）。连接顺序：活动集群（注册表/集群配置
    # 文件，与切换同约定）→ env（settings.redis_url，旧行为兜底）。
    candidates = [u for u in (_active_cluster_redis_url(), settings.redis_url) if u]
    local_mode = True
    error: Optional[Exception] = None
    for url in dict.fromkeys(candidates):
        try:
            redis_client = connect_redis(url)
        except Exception as e:  # noqa: BLE001 — 逐个候选尝试，全失败才进本地档
            error = e
            logger.warning("Redis 不可达（%s）: %s", url, e)
            continue
        logger.info(f"连接 Redis: {url}")
        app.state.redis = redis_client
        app.state.redis_url = url
        local_mode = False
        break
    if local_mode:
        redis_client = None
        app.state.redis = None
        app.state.redis_url = None
        logger.warning(
            "Redis 不可达（%s）——进入本地单机模式：流程在本进程内执行，"
            "执行历史存 SQLite；集群/事件/队列功能不可用。"
            "启动 Redis 并重启可恢复完整能力。",
            error,
        )
    app.state.local_mode = local_mode

    # 初始化流程编排持久化（SQLAlchemy）—— 建表，不破坏 Redis 初始化
    try:
        flow_store.init_engine(settings.db_url)
        # 多租户引导（幂等）：default 租户、存量数据归租户、legacy admin 提平台管理员
        flow_store.ensure_tenant_bootstrap()
        logger.info(f"FlowStore 已初始化: {settings.db_url}")
        if local_mode:
            try:
                from .services import examples as examples_svc
            except ImportError:
                from services import examples as examples_svc  # type: ignore
            examples_svc.seed_example_flows(tenant_id="default")
            # 本地模式没有队列重投：重启前 running 的执行会失去执行线程，
            # 启动时对账成「可处置状态」（默认 suspended + 原因），否则永远卡 running
            try:
                from .services import reconcile as reconcile_svc
            except ImportError:
                from services import reconcile as reconcile_svc  # type: ignore
            try:
                app.state.reconcile_summary = reconcile_svc.reconcile_orphan_local_executions()
            except Exception as exc:  # noqa: BLE001 — 对账失败不得阻断启动
                logger.warning("本地执行对账失败（不阻断启动）: %s", exc)
        try:
            from .services import users_svc, local_scheduler
        except ImportError:
            from services import users_svc, local_scheduler  # type: ignore
        app.state.store = flow_store.get_flow_store()
        users_svc.ensure_bootstrap_user(app.state.store)
        if local_mode:
            # 本地档内置调度循环：替代集群档的 schedule_service 进程
            local_scheduler.start_scheduler_loop(app.state.store)
        else:
            # A48-01：集群档不启动本地调度循环。已落盘的 local_schedules 保留
            # 原样但不执行（切回本地档可继续），启动时显式告警其不可见语义。
            local_scheduler.warn_local_schedules_inactive(app.state.store)
    except Exception as e:
        logger.error(f"FlowStore 初始化失败: {e}")
        raise

    # A66-03/A66-04：凭据导出文件是 DB 的派生缓存，启动时以 DB 为权威对账。
    # 缺失重建、陈旧（含 #251 导出→commit 窗口被 SIGKILL 打断留下的鬼凭据）
    # 刷新、多余旁文件 WARNING。导出失败 ⇒ 拒绝启动：让引擎带着缺失/陈旧
    # 密钥继续跑，比启动失败更危险——控制台仍显示「凭据正常」，分叉不会自愈。
    try:
        from .services import credentials_svc
    except ImportError:
        from services import credentials_svc  # type: ignore
    try:
        app.state.credential_export_reconcile = credentials_svc.reconcile_exports()
    except Exception as e:
        logger.error(
            "凭据导出对账失败（拒绝启动：DB 与引擎导出文件已分叉）: %s", e
        )
        raise

    # 引擎存储漂移兜底（尽力而为，不阻断启动）：把 PG 里全部已发布定义
    # 回填/对齐到「本进程所连的 Redis」——补齐「历史版本从未重跑发布」的缺口
    # （console 从 SQLite 迁到 PG 后 Redis 为空，worker 报「找不到流程定义」）。
    #
    # 边界（务必知悉）：本钩子只解决 console 与 worker **同机同 Redis** 的场景。
    # 二者可能不在同一台机器、不共享 Redis——此时本钩子无效，须由运维在 worker
    # 所在机器上用同样的 REDIS_URL 跑
    # plaita-console/scripts/sync_published_to_engine.py 对齐。见 engine_sync 模块
    # docstring 的「边界」段。失败仅 warning。
    if not local_mode and redis_client is not None:
        # B2R31-02：跨进程配置漂移预检（queue/redis/dlq/instance id）。
        # fail-open：只 WARNING + app.state.config_drift，不阻断启动。
        try:
            try:
                from .services import config_drift
            except ImportError:
                from services import config_drift  # type: ignore
            drift = config_drift.preflight_config_drift(redis_client)
            app.state.config_drift = drift
            if drift.get("drift"):
                logger.warning(
                    "配置漂移预检：%s 项（workers=%s），详见 /health 与日志",
                    len(drift["drift"]), drift.get("workers"),
                )
        except Exception as exc:  # noqa: BLE001 — 预检不得阻断启动
            app.state.config_drift = {"status": "degraded", "error": str(exc)[:300]}
            logger.warning("配置漂移预检失败（不阻断启动）: %s", exc)
        app.state.engine_sync = {"status": "running"}
        try:
            try:
                from .services import engine_sync
            except ImportError:
                from services import engine_sync  # type: ignore
            stats = engine_sync.sync_all_published_to_engine(
                redis_client, app.state.store)
            # A2R20-06：回填有界（pipeline + deadline/版本预算）。超预算或
            # 有错误时启动继续，但 /health 必须能看出 degraded。
            degraded = bool(stats.get("truncated")) or stats.get("errors", 0) > 0
            app.state.engine_sync = dict(stats)
            app.state.engine_sync["status"] = "degraded" if degraded else "ready"
            logger.info(
                "启动同步：已发布流程回填引擎存储 → written=%s skipped=%s errors=%s "
                "truncated=%s(%s) elapsed_ms=%s status=%s",
                stats.get("written"), stats.get("skipped"), stats.get("errors"),
                stats.get("truncated"), stats.get("truncated_reason"),
                stats.get("elapsed_ms"), app.state.engine_sync["status"],
            )
        except Exception as e:  # noqa: BLE001 — 兜底同步不得阻断启动
            app.state.engine_sync = {
                "status": "degraded",
                "error": str(e)[:300],
            }
            logger.warning("启动同步已发布流程到引擎存储失败（忽略，不阻断启动）: %s", e)

        # #299：跨 Redis 部署的发布 outbox 投递链。未配目标 Redis（同 Redis
        # 部署，默认）时整体关闭——不建连、不起线程、零额外写入。
        try:
            try:
                from .services import engine_outbox
            except ImportError:
                from services import engine_outbox  # type: ignore
            if engine_outbox.is_enabled():
                try:
                    from .services.flow_store import session_factory as _outbox_sessions
                except ImportError:
                    from services.flow_store import session_factory as _outbox_sessions  # type: ignore
                app.state.engine_outbox = {"status": "starting"}
                outbox_target = None
                try:
                    outbox_target = engine_outbox.connect_target()
                except Exception as exc:  # noqa: BLE001 — 不可达不阻断启动，线程按轮重连
                    logger.warning(
                        "outbox 目标 Redis 不可达（线程将按轮重连）: %s", exc,
                    )
                # #303：启动恢复——进程内删除协议不跨进程存活，启动时仍处
                # deleting 的行 = 上次进程死在「已标记、未删行」之间，一律
                # 打回 pending 交还常规投递/对账自愈（源表仍在 → 重投收敛；
                # 源表已删 → 对账 source-missing 收尾）。
                try:
                    recovered = engine_outbox.recover_deleting_rows(
                        _outbox_sessions())
                    if recovered:
                        logger.warning(
                            "outbox 启动恢复：%s 行 deleting（上次删除未完成）"
                            "已打回 pending", recovered,
                        )
                except Exception as exc:  # noqa: BLE001 — 恢复失败不阻断启动
                    logger.warning(
                        "outbox 启动恢复失败（deleting 行将保持跳过，"
                        "重发布会产生新 hash 行）: %s", exc,
                    )
                # 启动首遍：对账（delivered 缺 key/hash 不符 → 重投）+ 一轮投递。
                # 有界由 batch 上限保证；目标不可达时 cycle 内部自愈为空转统计。
                startup_summary: dict = {}
                if outbox_target is not None:
                    sessions = _outbox_sessions()
                    startup_summary.update(engine_outbox.run_reconcile_cycle(
                        outbox_target, sessions))
                    startup_summary.update(engine_outbox.run_delivery_cycle(
                        outbox_target, sessions))
                    startup_summary.update(engine_outbox.stats_snapshot(sessions))
                app.state.engine_outbox_target = outbox_target
                app.state.engine_outbox = {
                    **startup_summary,
                    "status": "ready" if outbox_target is not None else "degraded",
                }
                if outbox_target is not None:
                    logger.info(
                        "发布 outbox 投递链已启用（目标 Redis 已连）：对账 "
                        "checked=%s requeued=%s，投递 delivered=%s errors=%s，"
                        "周期对账每 %.0fs",
                        startup_summary.get("checked"),
                        startup_summary.get("requeued"),
                        startup_summary.get("delivered"),
                        startup_summary.get("errors"),
                        engine_outbox.reconcile_seconds(),
                    )
                reconcile_interval = engine_outbox.reconcile_seconds()
                poll_interval = engine_outbox.poll_seconds()
                if outbox_target is not None and poll_interval > 0:
                    import threading

                    outbox_stop = threading.Event()
                    app.state.engine_outbox_stop = outbox_stop

                    def _outbox_loop():
                        next_reconcile_at = 0.0  # 启动首遍已对账，下一轮按间隔
                        target = app.state.engine_outbox_target
                        while not outbox_stop.wait(poll_interval):
                            try:
                                if target is None:
                                    target = engine_outbox.connect_target()
                                    app.state.engine_outbox_target = target
                                    logger.info("outbox 目标 Redis 已恢复连接")
                                now_mono = time.monotonic()
                                reconcile_stats: dict = {}
                                if reconcile_interval > 0 and now_mono >= next_reconcile_at:
                                    reconcile_stats = engine_outbox.run_reconcile_cycle(
                                        target, _outbox_sessions())
                                    next_reconcile_at = now_mono + reconcile_interval
                                deliver_stats = engine_outbox.run_delivery_cycle(
                                    target, _outbox_sessions())
                                app.state.engine_outbox = {
                                    **reconcile_stats, **deliver_stats,
                                    **engine_outbox.stats_snapshot(_outbox_sessions()),
                                    "status": "ready",
                                }
                            except Exception as exc:  # noqa: BLE001 — 下轮重连/重试
                                app.state.engine_outbox = {
                                    "status": "degraded",
                                    "error": str(exc)[:300],
                                }
                                target = None  # 连接可疑：下轮重建
                                logger.warning(
                                    "outbox 维护轮次失败（下轮重连重试）: %s", exc,
                                )

                    threading.Thread(
                        target=_outbox_loop,
                        name="engine-outbox-delivery",
                        daemon=True,
                    ).start()
            else:
                app.state.engine_outbox = {"status": "disabled"}
        except Exception as e:  # noqa: BLE001 — outbox 装配失败不阻断启动
            app.state.engine_outbox = {"status": "degraded", "error": str(e)[:300]}
            logger.warning("发布 outbox 投递链装配失败（不阻断启动）: %s", e)

        # 租户状态对账（#27/#255/A2R25-06）：console 停机期间改过状态 / Redis
        # 被清空 / Redis-only 恢复回滚墓碑时，worker 侧的租户状态闸只有这份
        # 短视图可读——启动时以权威库为准对齐，并把 DB 已不存在的租户回落
        # disabled（否则恢复的旧快照会让已删租户继续 fail-open active）。
        try:
            try:
                from .services import tenants_svc
            except ImportError:
                from services import tenants_svc  # type: ignore
            status_reconcile = tenants_svc.publish_all_tenant_status(
                redis_client, app.state.store
            )
            app.state.tenants_svc = tenants_svc
            logger.info(
                "租户状态对账：published=%s disabled_stale=%s failed=%s",
                status_reconcile.get("published"),
                status_reconcile.get("disabled_stale"),
                status_reconcile.get("failed"),
            )
            if status_reconcile.get("disabled_stale"):
                logger.warning(
                    "租户状态对账发现 Redis 恢复了已删除租户的旧状态：%s 个"
                    "租户已回落 disabled 墓碑（#255/A2R25-06）",
                    status_reconcile.get("disabled_stale"),
                )
        except Exception as e:  # noqa: BLE001 — 不得阻断启动
            logger.warning("租户状态对账失败（忽略，不阻断启动）: %s", e)

        # A2R31-02：运行中周期对账（默认 60s；0=关闭但启动 WARNING）。
        # 启动对账只覆盖「恢复发生在启动前」；Redis-only 恢复可能发生在运行中，
        # 没有周期对账就会把 disabled 墓碑回滚成 active 且永久无人纠正。
        tenants_svc = getattr(app.state, "tenants_svc", None)
        interval = float(
            getattr(runtime_config, "tenant_status_reconcile_seconds", 60.0)
        )
        if interval > 0 and tenants_svc is not None:
            import threading

            stop_event = threading.Event()
            app.state.tenant_status_reconcile_stop = stop_event

            def _reconcile_loop():
                while not stop_event.wait(interval):
                    try:
                        summary = tenants_svc.publish_all_tenant_status(
                            app.state.redis, app.state.store
                        )
                    except Exception as exc:  # noqa: BLE001 — 下轮重试，必须 loud
                        logger.warning(
                            "租户状态周期对账失败（下轮重试）: %s", exc
                        )
                        continue
                    if summary.get("disabled_stale") or summary.get("failed"):
                        logger.warning(
                            "租户状态周期对账：published=%s disabled_stale=%s failed=%s",
                            summary.get("published"),
                            summary.get("disabled_stale"),
                            summary.get("failed"),
                        )

            threading.Thread(
                target=_reconcile_loop,
                name="tenant-status-reconcile",
                daemon=True,
            ).start()
            logger.info("租户状态周期对账已启用：每 %.1fs（A2R31-02）", interval)
        elif interval <= 0:
            logger.warning(
                "PLAITA_TENANT_STATUS_RECONCILE_SECONDS=0：租户状态周期对账已关闭；"
                "运行中 Redis-only 恢复的墓碑回滚不会被自动纠正（A2R31-02）"
            )

    # HMAC 重放保护：本地档（Redis 不可达）直接配进程内存档；集群档配 Redis
    # nonce store（enable_replay_protection 内部会 ping 一次，连不上同样降级
    # 并 warning，避免启动日志虚报 "multi-worker safe"）。
    signature.enable_replay_protection(None if local_mode else settings.redis_url)
    if local_mode:
        logger.info("本地单机模式：HMAC replay protection 使用进程内 nonce store（仅单进程有效）")

    if not settings.admin_api_key and not settings.allow_insecure_admin:
        logger.warning(
            "未配置 PLAITA_CONSOLE_ADMIN_API_KEY：管理 API 将返回 503，"
            "直到配置密钥或显式设置 ALLOW_INSECURE_ADMIN=true"
        )
    if not settings.secret_id or not settings.secret_key:
        logger.warning(
            "未配置 PLAITA_CONSOLE_SECRET_ID/SECRET_KEY：契约接口仅接受租户契约密钥"
            "（在租户管理页创建/轮换），全局密钥路径不可用"
        )

    # B108-01：start-dedup park 周期清扫（轻量 janitor；仅集群档有 Redis）。
    if not local_mode and redis_client is not None:
        try:
            from plaita.server.start_dedup_sweeper import (
                ParkSweeper,
                park_sweep_disabled,
            )

            if park_sweep_disabled():
                logger.warning(
                    "PLAITA_DISABLE_START_DEDUP_PARK_SWEEP=1：park 超时不再自动"
                    "清收（队列/映射/配额依赖人工或同 key 重试；B108-01）"
                )
            else:
                sweeper = ParkSweeper(redis_client)
                sweeper.start()
                app.state.park_sweeper = sweeper
        except Exception as exc:  # noqa: BLE001 — 清扫不阻断启动，但必须 loud
            logger.warning("start-dedup park 清扫启动失败（不阻断启动）: %s", exc)

    yield
    
    # A48-01：先停本地调度线程，再断开数据面/存储——线程不得在收尾期间继续
    # 起执行（lifespan 已退出后仍触发是修复前实测现象）。
    try:
        from .services import local_scheduler
    except ImportError:
        from services import local_scheduler  # type: ignore
    local_scheduler.stop_scheduler_loop()

    # B108-01：先停 park 清扫线程，再断 Redis。
    park_sweeper = getattr(app.state, "park_sweeper", None)
    if park_sweeper is not None:
        try:
            park_sweeper.stop()
        except Exception as exc:  # noqa: BLE001 — 收尾失败只 WARNING
            logger.warning("start-dedup park 清扫停止失败: %s", exc)
        app.state.park_sweeper = None

    # A2R31-02：先停周期对账线程，再断 Redis（线程不得在收尾期间回写）。
    stop_event = getattr(app.state, "tenant_status_reconcile_stop", None)
    if stop_event is not None:
        stop_event.set()

    # #299：先停 outbox 投递线程，再断目标 Redis。
    outbox_stop = getattr(app.state, "engine_outbox_stop", None)
    if outbox_stop is not None:
        outbox_stop.set()
    outbox_target = getattr(app.state, "engine_outbox_target", None)
    if outbox_target is not None:
        try:
            outbox_target.close()
        except Exception as exc:  # noqa: BLE001 — 收尾失败只 WARNING
            logger.warning("outbox 目标 Redis 连接关闭失败: %s", exc)
        app.state.engine_outbox_target = None

    # 关闭时断开 Redis 连接（关当前绑定：集群切换后 redis_client 全局已过时）
    logger.info("关闭 Redis 连接")
    current = getattr(app.state, "redis", None)
    if current is not None:
        current.close()


def _load_external_node_modules() -> None:
    """加载业务侧节点模块（PLAITA_CONSOLE_NODE_MODULES=mod.path[:mod2,...]）。

    console 校验/试跑业务 flow 时，其自定义节点须注册进运行时 registry。
    模块需暴露 register_all()（或任意可调用 register）。
    """
    import importlib
    import logging
    import sys

    for extra in [p for p in os.environ.get("PLAITA_CONSOLE_NODE_PATH", "").split(os.pathsep) if p]:
        if extra not in sys.path:
            sys.path.insert(0, extra)
    raw = os.environ.get("PLAITA_CONSOLE_NODE_MODULES", "")
    for mod_path in [m.strip() for m in raw.split(",") if m.strip()]:
        try:
            mod = importlib.import_module(mod_path)
            register = getattr(mod, "register_all") or getattr(mod, "register")
            register()
            # CODE 节点 opt-in（与 plaita-nodes 的 agentrun/capture 等保持一致）。
            # 白名单（plaita#22）：未显式配置 PLAITA_SANDBOX_ALLOWED_BACKENDS 时默认
            # 只放行 docker ∪ 生效后端——console 进程内同样会跑流程（dry-run 试跑、
            # 本地单机模式的 local_executor），流程 JSON 不得逐节点降级到 "unsafe"。
            # 语言白名单（plaita#29）：默认只放行 python；放行 js 须显式配置
            # PLAITA_SANDBOX_ALLOWED_LANGUAGES（js 历史实现绕开整个档位体系）。
            try:
                from plaita.node import (register_code_node,
                                         resolve_sandbox_allowed_backends,
                                         resolve_sandbox_allowed_languages)
                register_code_node(
                    allowed_backends=list(
                        resolve_sandbox_allowed_backends(component="console")),
                    allowed_languages=list(
                        resolve_sandbox_allowed_languages(component="console")))
            except ImportError:
                pass
            logging.getLogger("backend.main").info("已加载外部节点模块: %s", mod_path)
        except Exception as exc:  # noqa: BLE001
            logging.getLogger("backend.main").warning("外部节点模块加载失败 %s: %s", mod_path, exc)


if os.environ.get("PLAITA_CONSOLE_NODE_MODULES"):
    _load_external_node_modules()

# writefile 写入 jail（plaita#39）：console 进程内也会跑流程（dry-run 试跑、
# 本地单机模式的 local_executor），与 worker 同属部署入口，须同样注入。
apply_writefile_jail("console")


def create_app() -> FastAPI:
    """创建 FastAPI 应用"""
    # plaita#239 phase 2（owner 2026-10-18 裁决）：单租户部署迁移——console
    # 进程入口**显式**声明 default 租户；写路径对隐式 default 已 fail-closed。
    set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
    settings = get_settings()
    
    app = FastAPI(
        title="Plaita Console API",
        description=(
            "Plaita 流程引擎管理控制台 API。\n\n"
            "**管理面 (admin)**：Console 前端 / 运维；鉴权 "
            "`X-Admin-API-Key` 或 `Authorization: Bearer`。\n\n"
            "**契约面 (contract)**：外部 `PlaitaClient`；鉴权 HMAC "
            "（`PLAITA_CONSOLE_SECRET_ID/SECRET_KEY`）；"
            "仅 `POST /api/flowVersion/semver/detail`。"
        ),
        version="1.0.0",
        lifespan=lifespan,
        openapi_tags=[
            {"name": "admin", "description": "管理面：需 Admin API Key"},
            {"name": "contract", "description": "契约面：需 HMAC 签名"},
            {"name": "health", "description": "健康检查（无鉴权）"},
        ],
    )

    # A2R24-01：所有错误响应统一为 {"detail": {code, message, ...}}；
    # 未捕获异常回 JSON 500（不再 text/plain），异常原文只进日志。
    install_error_handlers(app)
    
    # 添加 CORS 中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def request_id_middleware(request, call_next):
        """A55-S3：每个请求生成服务端 request_id，审计行与响应头同值对账。"""
        request_id = uuid.uuid4().hex
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    # A51-02：认证世代绑定到请求 ContextVar——所有管理面写路径在 flush 后
    # 同一事务内复核「请求世代 == 当前世代」，防止旧租户请求跨 delete+create
    # 写入重建租户。tenant_generation_context 依赖 require_auth（FastAPI 缓存，
    # 不会重复执行鉴权）。
    admin_deps = [Depends(require_auth), Depends(tenant_generation_context)]
    prefix = settings.api_prefix

    def _mount_admin(router, resource_tag: str) -> None:
        """挂载管理面路由：Admin API Key + OpenAPI tag=admin/<resource>。"""
        app.include_router(
            router,
            prefix=prefix,
            tags=["admin", resource_tag],
            dependencies=admin_deps,
        )

    def _mount_contract(router, resource_tag: str) -> None:
        """挂载契约面路由：独立 HMAC，不加 admin 依赖。"""
        app.include_router(
            router,
            prefix=prefix,
            tags=["contract", resource_tag],
        )

    # --- 管理面（需 Admin API Key）---
    _mount_admin(services.router, "services")
    _mount_admin(executions.router, "executions")
    _mount_admin(queues.router, "queues")
    _mount_admin(logs.router, "logs")
    _mount_admin(cluster.router, "cluster")
    _mount_admin(events.router, "events")
    _mount_admin(nodes.router, "nodes")
    _mount_admin(flows.router, "flows")
    _mount_admin(dryrun.router, "dryrun")
    _mount_admin(copilot.router, "copilot")
    _mount_admin(supervisor.router, "supervisor")
    _mount_admin(schedules.router, "schedules")
    _mount_admin(credentials.router, "credentials")
    _mount_admin(property_types.router, "property_types")
    _mount_admin(credential_templates.router, "credential_templates")
    _mount_admin(audit.router, "audit")
    _mount_admin(tenants.router, "tenants")
    # #305 Phase 1：编辑器 graph validate（宿主 AI / 嵌入编辑器保存前校验）
    _mount_admin(graphs.router, "graphs")
    _mount_admin(metrics_api.router, "metrics")
    # #42（W10-a）：worker 容量 / 派发决策只读面（影子模式：只算不发）
    _mount_admin(capacity_api.router, "capacity")

    # --- 契约面（独立 HMAC，不加 admin 依赖）---
    _mount_contract(flow_version.router, "flow_version")

    # --- 登录/用户管理（自带角色规则；users 前缀要求 admin）---
    app.include_router(auth_users.router, prefix=prefix)
    
    @app.get("/health", tags=["health"])
    async def health():
        """健康检查端点。

        本地模式附带启动对账摘要（``reconcile``）：重启后有僵尸执行被处置时，
        编排/运维不必翻日志就能看到。集群档附带 ``engine_sync``（ready/
        degraded + 回填计数/预算触界原因）。
        """
        return build_health_body(app)

    # --- 打包发布模式：后端直接托管前端构建产物（pip 安装后无需 Node 环境）---
    # webDist 由 scripts/build_package.sh 从 frontend/dist 填充；仓库开发模式
    # 下该目录不存在，走 vite dev server，此处静默跳过。挂载在 API 路由之后，
    # /api 与上方健康检查不受影响；"/" 让给前端首页，健康检查走 /health。
    web_dist = Path(__file__).parent / "webDist"
    if web_dist.is_dir():
        app.mount("/", make_spa_static_files(web_dist), name="web")
        logger.info("已托管前端静态资源: %s", web_dist)
    else:

        @app.get("/", tags=["health"])
        async def root():
            """健康检查"""
            return {"status": "ok", "service": "plaita-console"}

    return app


# 创建应用实例
app = create_app()


def run_server():
    """启动服务器（用于脚本入口）"""
    import uvicorn
    settings = get_settings()
    # #208：默认 127.0.0.1；显式对外监听时留一条启动 warning
    warn_if_public_bind(settings.host, logger)
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )


if __name__ == "__main__":
    run_server()
