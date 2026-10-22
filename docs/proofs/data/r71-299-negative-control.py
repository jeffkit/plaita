"""#299 负控：跨 Redis 部署下 publish 后目标 Redis 能否拿到定义。

场景：console 进程连 console Redis；worker/目标环境用另一个 Redis
（PLAITA_CONSOLE_ENGINE_OUTBOX_TARGET_REDIS_URL 指向之）。
修复前：该 env 不存在/被忽略，发布只写 console 自己的 Redis，
目标 Redis 永远缺 key —— 「console 发布成功、worker 找不到 flow」。
修复后：publish 事务内登记 outbox，投递 worker 把定义送进目标 Redis，
hash（flow-definition-json-sortkeys-v2 口径）与 outbox 登记一致。
"""
import json
import os
import sys
from pathlib import Path

import fakeredis
from fastapi import FastAPI
from starlette.testclient import TestClient

REPO = Path(__file__).resolve().parents[3]
BACKEND = REPO / "plaita-console" / "backend"
sys.path.insert(0, str(BACKEND))
sys.path.insert(0, str(REPO))

from api import flows as flows_api          # noqa: E402
from services import flow_store             # noqa: E402

try:
    from services.engine_outbox import run_delivery_cycle  # noqa: E402
except ImportError:  # 修复前：模块不存在，直接展示目标 Redis 缺 key 的缺口
    run_delivery_cycle = None

DEF = json.dumps({
    "flow_id": "demo",
    "desc": "outbox 负控",
    "inputType": {"dataType": "object"},
    "nodes": [
        {"type": "start", "id": "start", "next": "end"},
        {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
    ],
})

import tempfile
from plaita.tenant_context import DEFAULT_TENANT_ID, TENANT_SOURCE_EXPLICIT, set_current_tenant

# console 启动入口同款（lifespan 迁移行）：TestClient 不经 lifespan，需显式声明
set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)

tmp = tempfile.mkdtemp(prefix="outbox-negctl-")
flow_store.init_engine(f"sqlite:///{tmp}/neg.db")

console_redis = fakeredis.FakeRedis(decode_responses=True)   # console 进程所连 Redis
target_redis = fakeredis.FakeRedis(decode_responses=True)    # 目标环境 Redis（跨机）

os.environ["PLAITA_CONSOLE_ENGINE_OUTBOX_TARGET_REDIS_URL"] = "redis://target-env:6379/0"

app = FastAPI()
app.state.redis = console_redis
app.state.local_mode = False
app.state.store = flow_store.get_flow_store()
app.include_router(flows_api.router, prefix="/api")
client = TestClient(app)

r = client.post("/api/flows", json={"flow_id": "demo"})
assert r.status_code == 200, r.text
r = client.put("/api/flows/demo/versions/1.0.0",
               json={"definition": DEF, "layout": "{}"})
assert r.status_code == 200, r.text
r = client.post("/api/flows/demo/publish", json={"version": "1.0.0"})
assert r.status_code == 200, r.text
print("publish rc=200:", r.json()["status"])

print("console_redis key plaita:flow:demo:1.0.0 exists:",
      bool(console_redis.exists("plaita:flow:demo:1.0.0")))
print("target_redis  key plaita:flow:demo:1.0.0 exists:",
      bool(target_redis.exists("plaita:flow:demo:1.0.0")))

# 修复后路径：投递一轮，目标 Redis 应拿到定义（修复前 delivery 能力不存在）
if run_delivery_cycle is None:
    print("RESULT: RED —— 跨 Redis 部署下目标环境拿不到定义（worker 侧 ErrFlowNotFound）")
    sys.exit(1)

summary = run_delivery_cycle(target_redis, flow_store.get_flow_store()._session_local)
print("delivery_cycle:", summary)

import hashlib

def _artifact_hash(def_json: str, flow_id: str, version: str) -> str:
    """引擎落盘工件（定义 + 注入 flow_id/version）的 v2 指纹——worker 校验口径。"""
    defn = json.loads(def_json)
    defn["flow_id"] = flow_id
    defn["version"] = version
    canon = json.dumps(defn, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), default=str)
    return hashlib.sha256(canon.encode()).hexdigest()

target_key = target_redis.get("plaita:flow:demo:1.0.0")
if target_key is None:
    print("RESULT: RED —— 跨 Redis 部署下目标环境拿不到定义（worker 侧 ErrFlowNotFound）")
    sys.exit(1)

print("target hash matches engine artifact (v2):",
      _artifact_hash(DEF, "demo", "1.0.0") ==
      _artifact_hash(target_key, "demo", "1.0.0"))
print("RESULT: GREEN")
