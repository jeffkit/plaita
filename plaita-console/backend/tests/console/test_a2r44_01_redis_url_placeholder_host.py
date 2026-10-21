"""``_is_concrete_redis_url`` 占位识别按 host 精确判定（A2R44-01）。

#65 权威链的地基：``redis.url`` 是否「具体地址」原来用
``"localhost" not in text and "127.0.0.1" not in text`` 的**子串**判定，
两个方向误分类：

1. 大小写逃逸抢占权威：``redis://LOCALHOST:6379/0`` 判 concrete ⇒ 作为
   config_file 权威生效，**静默阻断** ``PLAITA_CONSOLE_REDIS_URL`` 覆盖与
   注册表 metadata 兜底（Docker 跨容器即 #276 同族连错实例）；
2. userinfo 子串反向失权：凭据段含 localhost/127.0.0.1 子串的具体 URL
   （``redis://admin:localhost-secret@10.0.0.5:6379``）被判占位，env/metadata
   反向抢占真权威。

修法：``urlsplit`` 后按 host 段精确比较（hostname 已小写化，天然大小写
不敏感），userinfo 不参与。派生/改写面（``_resolve_env``、
``service_restart_redis_urls``）的占位改写判定与 concrete 判定同口径，
LOCALHOST 变体占位被改写成生效 URL 而非原样透传。
"""
import sys
from pathlib import Path

import pytest
import yaml

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.service_manager import (  # noqa: E402
    ServiceManager,
    _is_concrete_redis_url,
    effective_redis_url,
    frozen_service_redis_urls,
    redis_url_source,
    service_restart_redis_urls,
)

URL_ENV = "redis://env-redis:6379/0"
URL_META = "redis://meta-redis:6379/0"
URL_CONCRETE = "redis://10.0.0.5:6379/0"
# 方向 1：大写 LOCALHOST（DNS 大小写不敏感，本机掩盖、Docker 跨容器连错）
URL_LOCALHOST_UPPER = "redis://LOCALHOST:6379/0"
URL_LOCALHOST_MIXED = "redis://LocalHost:6379/0"
# 方向 2：凭据段含占位子串的具体地址
URL_CREDS_LOCALHOST = "redis://admin:localhost-secret@10.0.0.5:6379/0"
URL_CREDS_LOOPBACK = "redis://admin:pw-127.0.0.1@redis-prod:6379/0"


@pytest.fixture()
def no_env(monkeypatch):
    monkeypatch.delenv("PLAITA_CONSOLE_REDIS_URL", raising=False)


# ---------------------------------------------------------------------------
# 方向 1：LOCALHOST 大小写变体是占位，不得抢占 config_file 权威
# ---------------------------------------------------------------------------

def test_uppercase_localhost_is_placeholder(no_env):
    assert _is_concrete_redis_url(URL_LOCALHOST_UPPER) is False
    assert _is_concrete_redis_url(URL_LOCALHOST_MIXED) is False


def test_uppercase_localhost_config_yields_to_console_env(monkeypatch, no_env):
    """LOCALHOST 配置不再抢占权威：env 覆盖生效、source=console_env。"""
    monkeypatch.setenv("PLAITA_CONSOLE_REDIS_URL", URL_ENV)
    assert effective_redis_url(URL_LOCALHOST_UPPER) == URL_ENV
    assert redis_url_source(URL_LOCALHOST_UPPER) == "console_env"


def test_localhost_variant_no_longer_blocks_authority_ladder(monkeypatch, no_env):
    """A2R44-01 验收：LOCALHOST 配置不再抢占权威。

    env 已设 → source=console_env（env 覆盖重新可达）；配置无 url →
    metadata 兜底可达；env 未设 + 占位配置 → 占位本身按 #65 序仍居
    metadata 之前（大小写两形态一致，非本单语义改动）。
    """
    monkeypatch.setenv("PLAITA_CONSOLE_REDIS_URL", URL_ENV)
    assert redis_url_source(URL_LOCALHOST_UPPER) == "console_env"
    monkeypatch.delenv("PLAITA_CONSOLE_REDIS_URL")
    assert effective_redis_url(None, URL_META) == URL_META
    assert redis_url_source(None) == "registry_metadata"
    assert effective_redis_url(URL_LOCALHOST_UPPER, URL_META) == URL_LOCALHOST_UPPER
    assert effective_redis_url(
        "redis://localhost:6379/0", URL_META) == "redis://localhost:6379/0"


def test_load_config_rescues_uppercase_localhost_placeholder(tmp_path, monkeypatch):
    """load_config 的 env 纠偏面（:751）：LOCALHOST 占位同样让位 env。"""
    cfg_dir = tmp_path / "upper"
    cfg_dir.mkdir()
    cfg_path = cfg_dir / "cluster_config.yaml"
    cfg_path.write_text(yaml.safe_dump({
        "profile": "dev",
        "mode": "process",
        "redis": {"url": URL_LOCALHOST_UPPER},
    }), encoding="utf-8")
    monkeypatch.setenv("PLAITA_CONSOLE_REDIS_URL", URL_ENV)
    manager = ServiceManager(config_path=str(cfg_path))
    config = manager.load_config()
    assert config.redis["url"] == URL_ENV


def test_derivation_rewrites_uppercase_localhost_service_env(monkeypatch):
    """派生面同口径：services env 的 LOCALHOST 占位字面量改写成生效 URL。

    修复前：``_resolve_env``/``service_restart_redis_urls`` 用大小写敏感的
    ``"localhost" in value``——LOCALHOST 变体原样透传，worker 静默连回环。
    """
    effective = URL_ENV
    envs = {"PLAITA_REDIS_URL": URL_LOCALHOST_UPPER}
    resolved = service_restart_redis_urls(
        {"services": {"flow_worker": {"env": envs}}}, effective)
    assert resolved["flow_worker"] == effective

    assert not frozen_service_redis_urls(
        {"services": {"flow_worker": {"env": envs}}}), \
        "LOCALHOST 占位不是冻结字面量（改写面会处理，无需冻结告警）"
    assert _is_concrete_redis_url(URL_LOCALHOST_UPPER) is False


# ---------------------------------------------------------------------------
# 方向 2：userinfo 凭据段含占位子串不反向失权
# ---------------------------------------------------------------------------

def test_credential_substring_keeps_concrete_authority(no_env):
    assert _is_concrete_redis_url(URL_CREDS_LOCALHOST) is True
    assert _is_concrete_redis_url(URL_CREDS_LOOPBACK) is True
    assert _is_concrete_redis_url(URL_CONCRETE) is True


def test_credential_substring_config_beats_env_and_metadata(monkeypatch):
    """具体 URL（凭据含占位子串）保持 config_file 权威：env 不得抢占。"""
    monkeypatch.setenv("PLAITA_CONSOLE_REDIS_URL", URL_ENV)
    assert effective_redis_url(URL_CREDS_LOCALHOST) == URL_CREDS_LOCALHOST
    assert effective_redis_url(URL_CREDS_LOCALHOST, URL_META) == URL_CREDS_LOCALHOST
    assert redis_url_source(URL_CREDS_LOCALHOST) == "config_file"


def test_credential_substring_is_frozen_literal():
    """具体 URL（凭据含占位子串）入冻结告警面（A2R40-03），不被改写吞掉。"""
    frozen = frozen_service_redis_urls(
        {"services": {"flow_worker": {"env": {"PLAITA_REDIS_URL": URL_CREDS_LOCALHOST}}}})
    assert frozen == {"flow_worker": URL_CREDS_LOCALHOST}
    resolved = service_restart_redis_urls(
        {"services": {"flow_worker": {"env": {"PLAITA_REDIS_URL": URL_CREDS_LOCALHOST}}}},
        URL_ENV)
    assert resolved["flow_worker"] == URL_CREDS_LOCALHOST


# ---------------------------------------------------------------------------
# 占位 host 集与守恒栅栏（既有行为不变）
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url", [
    "redis://localhost:6379/0",
    "redis://127.0.0.1:6379/0",
    "redis://[::1]:6379/0",
    "redis://0.0.0.0:6379/0",
    None,
    "",
])
def test_placeholder_and_empty_stay_non_concrete(no_env, url):
    assert _is_concrete_redis_url(url) is False


def test_bare_host_strings_keep_legacy_substring_view(no_env):
    """无 scheme/解析不出 host 的历史形态退回字面口径：行为守恒。"""
    assert _is_concrete_redis_url("localhost:6379") is False
    assert _is_concrete_redis_url("127.0.0.1:6379") is False
    assert _is_concrete_redis_url("127.0.0.2:6379") is True  # 非占位回环段仍具体
    # ${redis.url} 引用不在 concrete 判定的职责内（各消费面先行处理）
    assert _is_concrete_redis_url("${redis.url}") is True


def test_placeholder_precedence_order_unchanged(monkeypatch, no_env):
    """#65 序守恒：小写占位让位 env；env 未设时占位居 metadata 前不变。"""
    monkeypatch.setenv("PLAITA_CONSOLE_REDIS_URL", URL_ENV)
    assert effective_redis_url("redis://localhost:6379/0") == URL_ENV
    assert redis_url_source("redis://localhost:6379/0") == "console_env"
    # env 未设 + 占位配置：占位本身仍按 config_file 档回显（不改语义）
    monkeypatch.delenv("PLAITA_CONSOLE_REDIS_URL")
    assert effective_redis_url("redis://localhost:6379/0", URL_META) == "redis://localhost:6379/0"
    assert redis_url_source("redis://localhost:6379/0") == "config_file"


def test_malformed_url_is_not_authoritative(no_env):
    """解析失败的畸形 URL 不能当权威（让位 env/metadata 兜底）。"""
    assert _is_concrete_redis_url("redis://[::1") is False
