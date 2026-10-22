"""A2R53-01：create_tenant 的 active 状态发布与世代发布同标 fail-closed。

修前：同函数双标——世代发布 ``_publish_generation``（事务内）失败 →
TenantError 回滚建租户（#284），而状态发布 ``_publish_status``（commit 后）
经 ``publish_tenant_status`` 的 ``except Exception: warning`` 吞掉
（tenant_status.py:124-129）。删除路径写下的 disabled 墓碑（#255）在同名
重建时若这一次 hset 恰好故障（抖动/READONLY/池耗尽）→ create 200 + 契约
密钥签发 + DB 行 active，Redis 短视图仍是 disabled，worker 的
``tenant_is_active`` 拦该租户一切 start/resume，自愈只剩下次 console 重启
对账——「console active / 运行面 disabled」拦行窗。

修后：active 发布挪进 commit 前事务窗口、``publish_tenant_status`` 返回
是否成功（与 ``publish_tenant_generation`` 同形制）；create_tenant 判失败
raise TenantError 回滚建租户——不留半租户。

负控制（A2 r53 剧本复刻）：只对 ``plaita:tenant_status`` 的 hset 注入故障
（世代 meta 键健康，模拟毫秒级窗口内仅状态写失败）。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
import fakeredis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import Tenant  # noqa: E402
from services import flow_store, tenants_svc  # noqa: E402
from plaita.server.tenant_status import tenant_is_active  # noqa: E402

TENANT = "acme"
STATUS_KEY = "plaita:tenant_status"
META_KEY = "plaita:tenant_status:meta"


class FlakyStatusHset:
    """同一 Redis 视图的代理：只对 plaita:tenant_status 的 hset 可注入故障。"""

    def __init__(self, inner, fail: bool = True):
        self._inner = inner
        self.fail = fail

    def hset(self, name, key=None, value=None):
        if self.fail and name == STATUS_KEY:
            raise ConnectionError("simulated redis blip on tenant_status hash")
        return self._inner.hset(name, key=key, value=value)

    def __getattr__(self, item):
        return getattr(self._inner, item)


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "a2r53.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    return {"store": flow_store.get_flow_store(), "redis": fakeredis.FakeRedis(decode_responses=True)}


def test_status_publish_failure_aborts_create_no_half_tenant(env):
    """红格翻转：状态 hset 故障 → 具名失败 + 无半租户（修前 = 200 + 拦行窗）。"""
    store, real = env["store"], env["redis"]
    # ① 首世：建 → 删，删除写 disabled 墓碑（#255 前提）。
    tenants_svc.create_tenant(store, TENANT, "Acme", redis_client=real)
    assert tenants_svc.delete_tenant(store, TENANT, redis_client=real) is True
    assert real.hget(STATUS_KEY, TENANT) == "disabled"

    # ② 同名重建：状态 hset 注入故障（世代发布正常）。修后 = 具名 TenantError。
    with pytest.raises(tenants_svc.TenantError, match="状态发布失败"):
        tenants_svc.create_tenant(store, TENANT, "Acme II",
                                  redis_client=FlakyStatusHset(real, fail=True))

    # ③ 无半租户：DB 行回滚，契约密钥不入库。
    with store._session_local() as session:
        assert session.get(Tenant, TENANT) is None
    with pytest.raises(LookupError):
        tenants_svc.get_tenant(store, TENANT)

    # ④ 短视图不被误写：墓碑保持 disabled（对已删 slug 的拦行语义原样）。
    assert real.hget(STATUS_KEY, TENANT) == "disabled"

    # ⑤ 失败不烧掉重建资格：健康 redis 重试同名 create 即成功且运行面同态。
    result = tenants_svc.create_tenant(store, TENANT, "Acme II",
                                       redis_client=FlakyStatusHset(real, fail=False))
    assert result["status"] == "active"
    assert real.hget(STATUS_KEY, TENANT) == "active"
    assert tenant_is_active(real, TENANT) is True


def test_healthy_recreate_overrides_tombstone(env):
    """健康路径守恒：hset 正常时墓碑被 active 覆盖（#255 语义不变）。"""
    store, real = env["store"], env["redis"]
    tenants_svc.create_tenant(store, TENANT, "Acme", redis_client=real)
    assert tenants_svc.delete_tenant(store, TENANT, redis_client=real) is True
    result = tenants_svc.create_tenant(store, TENANT, "Acme II",
                                       redis_client=FlakyStatusHset(real, fail=False))
    assert result["status"] == "active"
    assert real.hget(STATUS_KEY, TENANT) == "active"
    assert tenant_is_active(real, TENANT) is True


def test_generation_publish_failure_still_aborts_before_status(env):
    """佐证格：#284 fail-closed 守恒——世代失败仍回滚，且状态发布不执行。"""
    store, real = env["store"], env["redis"]
    status_calls: list[str] = []
    real_status = tenants_svc._publish_status

    def spy_status(*a, **k):
        status_calls.append(a[1] if len(a) > 1 else k.get("tenant_id"))
        return real_status(*a, **k)

    class FlakyMetaHset(FlakyStatusHset):
        def hset(self, name, key=None, value=None):
            if name == META_KEY:
                raise ConnectionError("simulated redis blip on meta hash")
            return super().hset(name, key=key, value=value)

    monkey = pytest.MonkeyPatch()
    try:
        monkey.setattr(tenants_svc, "_publish_status", spy_status)
        with pytest.raises(tenants_svc.TenantError, match="世代发布失败"):
            tenants_svc.create_tenant(store, "genfail", "GenFail",
                                      redis_client=FlakyMetaHset(real, fail=False))
    finally:
        monkey.undo()
    assert status_calls == [], "世代发布失败时不得走到 active 状态发布"
    with store._session_local() as session:
        assert session.get(Tenant, "genfail") is None
    assert real.hget(STATUS_KEY, "genfail") is None
