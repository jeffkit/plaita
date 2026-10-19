"""第五十二轮部署/CI 基建批的回归钉（A59-01 / B59-01 / B59-02 / B59-12）。

判据：
1. 镜像内容边界：根 .dockerignore 是递归模式（子目录 tests/build/db/webDist
   不进 context），Dockerfile.backend 不再整目录 COPY；
2. 基础设施模板：数据库口令改成 ``${VAR}`` 引用（缺失时 fail-closed），
   端口只绑回环，PostgreSQL/MySQL/Redis 带 named volume；
3. Redis Commander：默认 compose 不再包含它；opt-in 文件固定 digest、
   只绑回环、强制 HTTP 基本认证；
4. 服务镜像：cluster_config 不再引用未发布/可被抢占的 ``plaita/*`` 镜像，
   DockerLauncher 不再按服务名拼 latest 兜底，缺 image/本地无镜像时返回
   可操作错误而不是隐式 pull。
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import yaml

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

CONSOLE_DIR = Path(__file__).resolve().parents[3]
REPO_ROOT = CONSOLE_DIR.parent

from services import service_manager as sm  # noqa: E402
from services.service_manager import (  # noqa: E402
    ClusterConfig,
    DockerLauncher,
    InfrastructureConfig,
    ServiceConfig,
    ServiceManager,
)


# ---- A59-01 镜像内容边界 ---------------------------------------------------

def test_dockerignore_is_recursive_and_covers_runtime_artifacts():
    text = (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8")
    for pattern in (
        "**/tests",
        "**/build",
        "**/__pycache__",
        "**/*.db",
        "**/.plaita-credentials*",
        "**/webDist",
        "**/.env",
    ):
        assert pattern in text, f".dockerignore 缺少递归规则 {pattern}"


def test_backend_dockerfile_copies_narrow_runtime_paths():
    text = (CONSOLE_DIR / "docker" / "Dockerfile.backend").read_text(encoding="utf-8")
    assert "COPY plaita-console/backend/ ./backend/" not in text, (
        "Dockerfile.backend 不得整目录 COPY backend/（A59-01）"
    )
    for sub in ("api", "migrations", "models", "services"):
        assert f"COPY plaita-console/backend/{sub} " in text
    assert "COPY plaita-console/backend/*.py ./backend/" in text
    assert "COPY plaita-console/backend/webDist" not in text, (
        "backend 镜像不应 COPY 工作树 webDist"
    )


# ---- B59-01 基础设施模板 ---------------------------------------------------

def test_cluster_config_infra_is_loopback_volume_and_env_ref():
    raw = yaml.safe_load((CONSOLE_DIR / "cluster_config.yaml").read_text(encoding="utf-8"))
    pg = raw["infrastructure"]["postgresql"]["docker"]
    assert pg["ports"] == ["127.0.0.1:5432:5432"]
    assert "plaita_pg_data:/var/lib/postgresql/data" in pg["volumes"]
    assert pg["env"]["POSTGRES_PASSWORD"] == "${PLAITA_POSTGRES_PASSWORD}"

    redis = raw["infrastructure"]["redis"]["docker"]
    assert redis["ports"] == ["127.0.0.1:6379:6379"]
    assert "plaita_redis_data:/data" in redis["volumes"]

    for name, svc in raw["services"].items():
        assert "image" not in (svc.get("docker") or {}), (
            f"{name} 仍引用未发布/可被抢占的服务镜像（B59-12）"
        )


def test_infra_env_refs_expand_and_missing_fails_closed(monkeypatch):
    manager = object.__new__(ServiceManager)
    monkeypatch.setenv("PLAITA_POSTGRES_PASSWORD", "s3cret")
    monkeypatch.delenv("PLAITA_MISSING_PG_PW", raising=False)

    env, missing = manager._resolve_infra_env({
        "env": {
            "POSTGRES_PASSWORD": "${PLAITA_POSTGRES_PASSWORD}",
            "POSTGRES_USER": "plaita",
        }
    })
    assert env == {"POSTGRES_PASSWORD": "s3cret", "POSTGRES_USER": "plaita"}
    assert missing == []

    env, missing = manager._resolve_infra_env({
        "env": {"POSTGRES_PASSWORD": "${PLAITA_MISSING_PG_PW}"}
    })
    assert missing == ["PLAITA_MISSING_PG_PW"]
    assert env["POSTGRES_PASSWORD"] == ""


def test_start_infrastructure_missing_env_returns_actionable_error(monkeypatch):
    monkeypatch.delenv("PLAITA_MISSING_PG_PW", raising=False)
    manager = object.__new__(ServiceManager)
    manager.config = ClusterConfig(infrastructure={
        "postgresql": InfrastructureConfig(
            name="postgresql",
            display_name="PostgreSQL",
            type="database",
            docker={
                "image": "postgres:15-alpine",
                "env": {"POSTGRES_PASSWORD": "${PLAITA_MISSING_PG_PW}"},
            },
        )
    })
    manager._docker_available_now = lambda: True

    result = asyncio.run(manager.start_infrastructure("postgresql"))
    assert result["success"] is False
    assert "PLAITA_MISSING_PG_PW" in result["message"]
    assert "deployment.md" in result["message"]


def test_start_infrastructure_missing_db_password_fails_closed():
    manager = object.__new__(ServiceManager)
    manager.config = ClusterConfig(infrastructure={
        "mysql": InfrastructureConfig(
            name="mysql",
            display_name="MySQL",
            type="database",
            docker={"image": "mysql:8", "env": {"MYSQL_DATABASE": "plaita"}},
        )
    })
    manager._docker_available_now = lambda: True

    result = asyncio.run(manager.start_infrastructure("mysql"))
    assert result["success"] is False
    assert "MYSQL_ROOT_PASSWORD" in result["message"]


# ---- B59-02 Redis Commander ------------------------------------------------

def test_deps_compose_has_no_unauthenticated_commander():
    deps = yaml.safe_load(
        (CONSOLE_DIR / "docker" / "docker-compose.deps.yml").read_text(encoding="utf-8")
    )
    assert deps["services"]["redis"]["ports"] == ["127.0.0.1:6379:6379"]
    assert "redis-commander" not in deps["services"], (
        "无鉴权 Redis Commander 不得出现在默认依赖 compose（B59-02）"
    )


def test_redis_commander_optin_is_pinned_loopback_and_authenticated():
    raw = yaml.safe_load(
        (CONSOLE_DIR / "docker" / "docker-compose.redis-commander.yml")
        .read_text(encoding="utf-8")
    )
    svc = raw["services"]["redis-commander"]
    assert svc["ports"] == ["127.0.0.1:8081:8081"]
    assert "@sha256:" in svc["image"] and ":latest" not in svc["image"]
    env = svc["environment"]
    assert any(item.startswith("HTTP_USER=") and ":?" in item for item in env)
    assert any(item.startswith("HTTP_PASSWORD=") and ":?" in item for item in env)


# ---- B59-12 服务镜像 -------------------------------------------------------

def test_docker_launcher_refuses_missing_image_without_pull():
    launcher = object.__new__(DockerLauncher)
    launcher._containers = {}
    launcher._docker_available = True
    config = ServiceConfig(service_type="delay_service", display_name="延迟服务")

    instance = asyncio.run(launcher.start("delay_service", config, {}))
    assert instance.status == "error"
    assert "镜像未配置" in (instance.error_message or "")
    assert "deployment.md" in (instance.error_message or "")


def test_docker_launcher_refuses_image_not_present_locally(monkeypatch):
    launcher = object.__new__(DockerLauncher)
    launcher._containers = {}
    launcher._docker_available = True

    monkeypatch.setattr(
        sm.subprocess, "run",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="No such image"),
    )
    config = ServiceConfig(
        service_type="delay_service",
        display_name="延迟服务",
        docker={"image": "registry.example/plaita-delay:1.0"},
    )

    instance = asyncio.run(launcher.start("delay_service", config, {}))
    assert instance.status == "error"
    assert "本地不存在" in (instance.error_message or "")
    assert "docker pull" in (instance.error_message or "")


def test_no_service_type_latest_fallback_in_source():
    source = (BACKEND_DIR / "services" / "service_manager.py").read_text(encoding="utf-8")
    assert 'f"plaita/{service_type}:latest"' not in source
