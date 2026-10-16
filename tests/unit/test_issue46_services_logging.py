"""plaita#46 — 扩展服务启动器缺 logging 配置（五个扩展服务全域静默）。

库策略（``plaita/logger.py``）：只挂 ``NullHandler``、不加强制 level、输出交由
应用配置 ⇒ 经 ``plaita/server/services/__main__.py`` 启动的
delay / schedule / approval / http_callback / redis_queue 服务**零日志输出**
（2026-10-07 运维实证：systemd 起 schedule_service 后日志文件 0 字节）。

同一问题 flow_worker.main() 已修并有注释先例（2026-09 分布式评审 P2-6）：
``logging.basicConfig(level=os.environ.get("PLAITA_LOG_LEVEL", "INFO"), ...)``——
扩展服务启动器是漏修而非设计取舍。

守卫（零依赖、免在跑 Redis）：子进程实跑
``python -m plaita.server.services delay_service --redis-url <必拒连端口>``——
连接拒绝走 ``logger.error("Redis 连接失败") + sys.exit(1)``，据此断言：
1. 缺省 INFO 可见（「连接 Redis」「Redis 连接失败」出现在 stderr）；
2. ``PLAITA_LOG_LEVEL=WARNING`` 时 INFO 被级别门关掉、ERROR 仍可见
   （证明级别确从 env 缺省 INFO 接管）。
"""
import logging
import os
import socket
import subprocess
import sys

import pytest

pytest.importorskip("plaita")


def _repo_root() -> str:
    import plaita

    return os.path.dirname(os.path.dirname(os.path.abspath(plaita.__file__)))


def _dead_redis_port() -> int:
    """取一个当前无人监听的本地端口（bind 后立即释放 → connect 必被拒）。"""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _run_launcher(extra_env: dict) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("PLAITA_LOG_LEVEL", None)
    env["PYTHONPATH"] = _repo_root()
    env.update(extra_env)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "plaita.server.services",
            "delay_service",
            "--redis-url",
            f"redis://127.0.0.1:{_dead_redis_port()}/0",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
        cwd=_repo_root(),
    )


def test_launcher_boot_writes_info_logs_without_config():
    """启动器实跑必须自己产出可见日志（应用入口配 logging，库侧不配）。"""
    proc = _run_launcher({})
    assert proc.returncode == 1, (
        "预期走「Redis 连接失败」退出路径；实际："
        f"returncode={proc.returncode}\nstdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    out = proc.stdout + proc.stderr
    assert "连接 Redis" in out, (
        "INFO 启动日志不可见——启动器未配置 logging（plaita#46），"
        f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert "Redis 连接失败" in out, (
        f"ERROR 日志不可见，stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )


def test_plaita_log_level_env_gates_info_but_not_error():
    """PLAITA_LOG_LEVEL 生效：WARNING 关掉 INFO、ERROR 仍可见（缺省=INFO 的级别接管）。"""
    proc = _run_launcher({"PLAITA_LOG_LEVEL": "WARNING"})
    assert proc.returncode == 1
    out = proc.stdout + proc.stderr
    assert "连接 Redis" not in out, (
        f"PLAITA_LOG_LEVEL=WARNING 未生效（INFO 仍输出）：stdout={proc.stdout!r} "
        f"stderr={proc.stderr!r}"
    )
    assert "Redis 连接失败" in out, (
        f"ERROR 被误关：stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
