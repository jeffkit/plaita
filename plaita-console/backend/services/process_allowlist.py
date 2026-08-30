"""
集群进程启动白名单。

防止 PUT /clusters/{id}/config 写入任意 command 后经 ProcessLauncher
执行 OS 级命令。仅允许已知的 plaita.server 模块与 services 子命令。
"""
from __future__ import annotations

import shlex
from typing import Any, Dict, List, Optional, Set

ALLOWED_MODULES: Set[str] = {
    "plaita.server.flow_worker",
    "plaita.server.event_filter",
    "plaita.server.services",
}

ALLOWED_SERVICE_SUBCOMMANDS: Set[str] = {
    "delay_service",
    "redis_queue_service",
    "http_callback_service",
    "approval_service",
    "kafka_queue_service",
    "schedule_service",
}


class ProcessConfigError(ValueError):
    """进程配置不在白名单内。"""


def validate_process_spec(process: Optional[Dict[str, Any]], *, service_key: str = "") -> None:
    """校验单个 service.process 块。失败抛 ``ProcessConfigError``。"""
    if not isinstance(process, dict):
        raise ProcessConfigError(
            f"服务 {service_key!r} 的 process 必须是对象，得到 {type(process).__name__}"
        )

    module = process.get("module")
    command = process.get("command")

    if module and command:
        raise ProcessConfigError(
            f"服务 {service_key!r} 不能同时指定 module 与 command"
        )
    if not module and not command:
        raise ProcessConfigError(
            f"服务 {service_key!r} 必须提供 process.module 或 process.command"
        )

    if module is not None:
        if not isinstance(module, str) or not module.strip():
            raise ProcessConfigError(f"服务 {service_key!r} 的 module 必须是非空字符串")
        mod = module.strip()
        if mod not in ALLOWED_MODULES:
            raise ProcessConfigError(
                f"服务 {service_key!r} 的 module {mod!r} 不在白名单内。"
                f"允许: {sorted(ALLOWED_MODULES)}"
            )
        return

    _validate_command(str(command), service_key=service_key)


def _validate_command(command: str, *, service_key: str) -> None:
    try:
        parts = shlex.split(command)
    except ValueError as e:
        raise ProcessConfigError(f"服务 {service_key!r} 的 command 无法解析: {e}") from e

    if len(parts) < 3:
        raise ProcessConfigError(
            f"服务 {service_key!r} 的 command 必须形如 "
            f"'python -m plaita.server.services <subcommand>'，得到: {command!r}"
        )

    # 允许 python / python3 / 绝对路径解释器
    exe = parts[0]
    if not (
        exe in ("python", "python3")
        or exe.endswith("/python")
        or exe.endswith("/python3")
        or exe.endswith("python.exe")
    ):
        raise ProcessConfigError(
            f"服务 {service_key!r} 的 command 必须以 python 解释器开头，得到 {exe!r}"
        )

    if parts[1] != "-m":
        raise ProcessConfigError(
            f"服务 {service_key!r} 的 command 第二段必须是 -m，得到 {parts[1]!r}"
        )

    module = parts[2]
    if module == "plaita.server.services":
        if len(parts) < 4:
            raise ProcessConfigError(
                f"服务 {service_key!r} 缺少 services 子命令"
            )
        sub = parts[3]
        if sub not in ALLOWED_SERVICE_SUBCOMMANDS:
            raise ProcessConfigError(
                f"服务 {service_key!r} 的子命令 {sub!r} 不在白名单内。"
                f"允许: {sorted(ALLOWED_SERVICE_SUBCOMMANDS)}"
            )
        if len(parts) > 4:
            # 允许额外 CLI 参数，但禁止 shell 元字符已由 shlex 拆分；
            # 仍禁止再嵌套 -c / 管道类危险段
            for extra in parts[4:]:
                if extra in ("-c", "--command") or ";" in extra or "|" in extra:
                    raise ProcessConfigError(
                        f"服务 {service_key!r} 的 command 含禁止参数: {extra!r}"
                    )
        return

    if module in ALLOWED_MODULES and module != "plaita.server.services":
        # python -m plaita.server.flow_worker ...
        for extra in parts[3:]:
            if extra in ("-c", "--command") or ";" in extra or "|" in extra:
                raise ProcessConfigError(
                    f"服务 {service_key!r} 的 command 含禁止参数: {extra!r}"
                )
        return

    raise ProcessConfigError(
        f"服务 {service_key!r} 的 -m 模块 {module!r} 不在白名单内。"
        f"允许: {sorted(ALLOWED_MODULES)}"
    )


def validate_cluster_config(config: Dict[str, Any]) -> None:
    """校验整份集群配置中所有 services.*.process。"""
    if not isinstance(config, dict):
        raise ProcessConfigError("集群配置必须是对象")

    services = config.get("services") or {}
    if not isinstance(services, dict):
        raise ProcessConfigError("services 必须是对象")

    for key, svc in services.items():
        if not isinstance(svc, dict):
            raise ProcessConfigError(f"服务 {key!r} 配置必须是对象")
        process = svc.get("process")
        if process is None:
            continue  # 无 process 的条目（如仅元数据）跳过
        validate_process_spec(process, service_key=str(key))
