"""卡 A-1（A129-01 Python 半边）：console 发布闸吞 hash 计算失败——孤立代理
转义定义照常发布成功落 Redis（混编撕裂修复对的 console 半边；Go 半边 =
plaita-go G-1 ``a20da36`` canonicaljson 拒孤立代理）。

机制（A1 r129 实弹）：含 ``\\ud800``/``\\udc00`` 孤立代理转义的定义是合法
JSON（ES2019 well-formed ``JSON.stringify`` 即产此形，前端可达），console
save（Flow schema，str 合法）接受；publish 链上 ``engine_payload`` →
canonical dumps（ensure_ascii=False）``.encode("utf-8")`` 抛
``UnicodeEncodeError``——它是 ``ValueError`` 子类，被 ``enqueue_publish``
的 ``except (TypeError, ValueError, json.JSONDecodeError)`` 吞为「定义不可
解析」ERROR、``return False``：outbox 无行、发布照常 200；save 面
``sync_flow_to_engine`` → ``save_flow`` 默认 ensure_ascii=True dumps 把孤立
代理回写为 ``\\udXXX`` 转义不炸 ⇒ **Redis 有键 / outbox 无行双通道不一致**。
Python worker 读回 hash 即炸（毒丸）；Go worker 修 G-1 前折 U+FFFD 静默
执行且指纹折叠，修后拒载——发布成功但两池都跑不了（运行期才炸）。

修法（hash 算法本身零改动）：
  ①发布端点在状态翻转前前置编码闸（与 #300 能力门同层）：canonical hash
    不可计算 → 422 ``definition_unencodable``，版本保持 draft、引擎存储
    零写入；发布事务内 gate_fn 对 fresh 行复核（并发保存竞窗，A2R54 同款）。
  ②``enqueue_publish`` 编码失败改具名上抛 ``DefinitionUnencodableError``
    （不再吞；unparseable 类「定义不可解析」skip 语义守旧）。
  ③save 面同闸：``sync_flow_to_engine`` 与启动回填
    ``sync_all_published_to_engine`` 拒写，零键落 Redis。

负控（修复前红/修复后绿）：
  格① 主格（E2E）：含 ``\\ud800`` 转义定义 publish → 修复前 200（红）/
      修复后 422 ``definition_unencodable`` 且版本保持 draft；
  格② 双通道（E2E）：422 且引擎存储零键 + outbox 零行；
  格③ save 面直格：``sync_flow_to_engine`` 毒定义 → False + 零键
      （修复前 True + 有键）；
  格④ 回填直格：已 published 毒定义（DB 直种，绕开 API 的写入是唯一
      存量来源）回填 → errors 计数 + 零键；合法 published 版本照常写出；
  格⑤ enqueue 直格：毒定义 → ``DefinitionUnencodableError``（修复前静默
      False）；unparseable 定义守旧 skip（返回 False，「定义不可解析」类
      非本闸管辖——该类在 save 面已被 ``_validate_definition`` 422 拦住，
      只有 DB 级写入能到此，故守恒只在 seam 层钉）；
  格⑥ 正控：成对代理（emoji 转义）定义发布链照旧——200/published/
      引擎键/outbox 行俱全；
  格⑦ 导入面守恒（基线即绿锁定）：手工毒包 ``import-version`` → 422
      ``promotion_invalid``——晋升包完整性哈希（canonical ensure_ascii=False）
      对毒定义 fail-loud、导入器收口为 PromotionValidationError（既有具名
      fail-closed），导入面不是毒定义可达面；``_publish_gates`` 在导入
      直发上是纵深。
"""
import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from sqlalchemy import text
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import engine_outbox, flow_store  # noqa: E402
from services.engine_sync import (  # noqa: E402
    sync_all_published_to_engine,
    sync_flow_to_engine,
)

ADMIN_KEY = "a12901-admin"
FLOW_ID = "a12901-flow"
VER = "1.0.0"


def _definition(extra_desc: str) -> str:
    return json.dumps(
        {
            "flow_id": FLOW_ID,
            "desc": extra_desc,
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name",
                 "resultType": "success"},
            ],
        }
    )


# 孤立代理（单独 high surrogate U+D800）：Python str 合法，ensure_ascii=True
# 转义形合法 JSON，ensure_ascii=False 形不可 UTF-8 编码
POISON_DEF = _definition("poison \ud800")
# 成对代理（emoji U+1F600 = \ud83d\ude00 代理对）：正控，两形皆合法
EMOJI_DEF = _definition("emoji \ud83d\ude00")
# 不可解析 JSON：非本闸管辖的守旧类
UNPARSEABLE_DEF = "{\"flow_id\": \"x\", oops"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def rd(tmp_path, monkeypatch) -> FakeRedis:
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'a12901.db'}")
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def rd_outbox(tmp_path, monkeypatch) -> FakeRedis:
    """同 ``rd`` + 启用 outbox（配目标 Redis URL，仅验登记面不跑投递线程）。"""
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'a12901-ob.db'}")
    monkeypatch.setenv(engine_outbox.TARGET_URL_ENV, "redis://outbox-target:6379/0")
    return FakeRedis(decode_responses=True)


def _client(r: FakeRedis) -> TestClient:
    app = FastAPI()
    app.state.redis = r
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(flows_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])
    return TestClient(app)


def _headers() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed(client: TestClient, definition: str) -> None:
    assert client.post("/api/flows", json={"flow_id": FLOW_ID},
                       headers=_headers()).status_code == 200
    r = client.put(f"/api/flows/{FLOW_ID}/versions/{VER}",
                   json={"definition": definition}, headers=_headers())
    assert r.status_code == 200, r.text


def _engine_keys(r: FakeRedis) -> list:
    return [k for k in r.keys("*") if ":flow:" in k]


def _outbox_rows() -> list:
    sessions = flow_store.session_factory()
    with sessions() as s:
        return s.execute(text("SELECT flow_id FROM flow_publish_outbox")).all()


def _version_status() -> str:
    store = flow_store.get_flow_store()
    row = store.get_version(FLOW_ID, VER)
    assert row is not None
    return row.status


def _detail_code(r) -> str:
    try:
        return str(((r.json() or {}).get("detail") or {}).get("code", ""))
    except Exception:  # noqa: BLE001
        return ""


# ============ 格①：主格（E2E·RED） ============

def test_publish_lone_surrogate_422_definition_unencodable(rd):
    """含 \\ud800 转义定义 publish → 422 definition_unencodable（修复前 200）
    且版本保持 draft（发布闸在状态翻转前拒绝）。"""
    client = _client(rd)
    _seed(client, POISON_DEF)
    r = client.post(f"/api/flows/{FLOW_ID}/publish",
                    json={"version": VER}, headers=_headers())
    assert r.status_code == 422, r.text
    assert _detail_code(r) == "definition_unencodable"
    assert "UTF-8" in r.json()["detail"]["message"]
    assert _version_status() == "draft"


# ============ 格②：双通道（E2E） ============

def test_publish_lone_surrogate_no_redis_key_no_outbox_row(rd_outbox):
    """422 且引擎存储零键 + outbox 零行（修复前：200 + 有键 + 无行）。"""
    client = _client(rd_outbox)
    _seed(client, POISON_DEF)
    r = client.post(f"/api/flows/{FLOW_ID}/publish",
                    json={"version": VER}, headers=_headers())
    assert r.status_code == 422
    assert _engine_keys(rd_outbox) == []
    assert _outbox_rows() == []


# ============ 格③：save 面直格 ============

def test_sync_flow_to_engine_refuses_lone_surrogate(rd):
    """sync_flow_to_engine 毒定义 → False + 零键（修复前 True + 有键）。"""
    assert sync_flow_to_engine(rd, FLOW_ID, VER, POISON_DEF,
                               tenant_id="default") is False
    assert _engine_keys(rd) == []
    # 正控：成对代理（emoji）定义照常写出
    assert sync_flow_to_engine(rd, FLOW_ID, VER, EMOJI_DEF,
                               tenant_id="default") is True
    assert len(_engine_keys(rd)) == 1


# ============ 格④：回填直格 ============

def test_backfill_skips_unencodable_published_row(rd, tmp_path):
    """已 published 毒定义（DB 直种）回填 → errors 计数 + 零键；
    合法 published 版本照常写出。"""
    client = _client(rd)
    _seed(client, EMOJI_DEF)
    assert client.post(f"/api/flows/{FLOW_ID}/publish",
                       json={"version": VER}, headers=_headers()).status_code == 200
    # 第二版毒定义经 save 面入库（save 接受），再 DB 直翻 published
    #（绕开发布闸——存量/DB 级修改是唯一来源）
    r = client.put(f"/api/flows/{FLOW_ID}/versions/1.0.1",
                   json={"definition": POISON_DEF}, headers=_headers())
    assert r.status_code == 200, r.text
    store = flow_store.get_flow_store()
    sessions = flow_store.session_factory()
    with sessions() as s:
        s.execute(
            text("UPDATE flow_versions SET status = 'published' "
                 "WHERE flow_id = :fid AND version = :ver"),
            {"fid": FLOW_ID, "ver": "1.0.1"},
        )
        s.commit()

    stats = sync_all_published_to_engine(rd, store)
    assert stats["errors"] == 1
    assert stats["written"] == 1
    keys = _engine_keys(rd)
    assert len(keys) == 1 and VER in keys[0]


# ============ 格⑤：enqueue 直格 ============

def test_enqueue_publish_raises_named_on_unencodable(rd_outbox):
    """enqueue_publish 毒定义 → DefinitionUnencodableError（修复前静默
    False）；unparseable 守旧 skip（返回 False）。"""
    sessions = flow_store.session_factory()
    with sessions() as s:
        with pytest.raises(engine_outbox.DefinitionUnencodableError):
            engine_outbox.enqueue_publish(
                s, tenant_id="", flow_id=FLOW_ID, version=VER,
                definition=POISON_DEF,
            )
        s.rollback()
        assert engine_outbox.enqueue_publish(
            s, tenant_id="", flow_id=FLOW_ID, version=VER,
            definition=UNPARSEABLE_DEF,
        ) is False


# ============ 格⑥：正控（成对代理发布链守恒） ============

def test_publish_paired_surrogate_chain_conserved(rd_outbox):
    """emoji（成对代理转义）定义：发布 200/published、引擎键、outbox 行
    俱全——编码闸不误伤合法 Unicode。"""
    client = _client(rd_outbox)
    _seed(client, EMOJI_DEF)
    r = client.post(f"/api/flows/{FLOW_ID}/publish",
                    json={"version": VER}, headers=_headers())
    assert r.status_code == 200, r.text
    assert _version_status() == "published"
    assert len(_engine_keys(rd_outbox)) == 1
    rows = _outbox_rows()
    assert len(rows) == 1 and rows[0][0] == FLOW_ID


# ============ 格⑦：导入面守恒（晋升包完整性哈希本就 fail-closed） ============

def test_import_poison_package_422_promotion_invalid_nothing_saved(rd):
    """手工毒包（definition 换成毒定义、哈希保持原值）导入 → 422
    `promotion_invalid`（包完整性校验 canonical_definition_hash 对不可编码
    定义抛 UnicodeEncodeError，导入器收口为 PromotionValidationError——
    既有具名 fail-closed）：导入面本就不是毒定义可达面；零版本落库、零键。"""
    from services import deployments as deployments_svc

    client = _client(rd)
    _seed(client, EMOJI_DEF)
    store = flow_store.get_flow_store()
    package = deployments_svc.build_promotion_package(store, FLOW_ID, VER)
    package["definition"] = json.loads(POISON_DEF)
    # 线上形态：ES2019 JSON.stringify 把孤立代理写成 \udXXX 转义（ASCII），
    # 预编码送出（httpx 自身 ensure_ascii=False 编不了裸孤立代理）。
    body = json.dumps({"package": package, "new_version": "2.0.0",
                       "publish": True})
    r = client.post("/api/flows/import-version", content=body.encode("utf-8"),
                    headers={**_headers(), "Content-Type": "application/json"})
    assert r.status_code == 422, r.text
    assert _detail_code(r) == "promotion_invalid"
    assert store.get_version(FLOW_ID, "2.0.0") is None
    assert _engine_keys(rd) == []
