"""A2R58-03：console 进程 ``_mask_url`` 注册与读面 host:port 保全——上游化负控。

缺陷（A2 r58 实弹）：r89 把 ``_scrub_url_text`` 引入 console 读面（detail
``error`` / ``timeline`` / SSE ``initial_state`` / ``update`` 四面）后，console
进程全链零 ``plaita.server.factory`` 导入 ⇒ ``plaita.core.url_redaction._mask_url``
恒未注册 ⇒ 一切绝对 URL（含零凭据的诊断 URL）坠 ``<unprintable-url>``、丢
host:port；且每次 scrub 一条无频控 WARNING 刷日志。

修法：①console 入口（``main.py``）注册 factory 掩码实现（与 worker 侧
``flow_worker`` 导入 factory 对称；实现仍以 factory 为唯一家，不另起第二套
掩码文法）；②未注册分支 WARNING 频控（首条 + 计数，后续降 DEBUG）。

**测试盲修正**：既有「凭据针不在」断言对「正确掩码（host:port 保留）」与
「过度掩码（unprintable 常量）」两形态同真。本文件的判据形是三元组：
``host 在场 + <unprintable-url> 不在场 + 凭据针不在场``（外加 path 明文
不在场）——能区分正确掩码与过度掩码。

**负控确定性**：全量测试会话里多个 console 测试文件 import ``flow_worker`` /
``factory`` 会把注册态带进宿主进程（历史盲源），因此：

* 注册断言走**子进程**（fresh 解释器只加载 console 导入链，A2 探针 04a 同款），
  不受宿主进程态污染，修前必红；
* 四读面断言在**单文件隔离**会话（``pytest tests/console/
  test_a2r58_03_mask_url_registration.py`` 单跑）为修前红相——本文件不 import
  factory，宿主进程零注册；全量会话里为绿（注册态在场）。
"""
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import fakeredis
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
REPO_ROOT = BACKEND_DIR.parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import plaita.core.url_redaction as UR  # noqa: E402

# 生产导入链（平铺布局，与 main.py 自身的 sys.path 兜底同式）：main.py 在修后
# 注册 _mask_url——本模块进程内的注册态即由这一行产生（修前为 None）。
import main  # noqa: E402,F401

# ---- 载体文本（A2 r58 探针同款三形）---------------------------------------
#: 无凭据纯诊断 URL（丢 host 的害处最直白：查不出哪个上游挂了）
RAW_NO_CRED = (
    "节点执行失败: HTTPError 502 Server Error for url: https://api.example.com/v1/items/42?x=1"
)
#: worker 注册态 scrub 的产物（timeline 落盘存量形态：保 host、抹 path/query）
WORKER_SCRUBBED = "HTTPError 502 Server Error for url: https://api.example.com/***/***/***?***"
#: 滚动部署窗旧版 worker 存量：未 scrub、含凭据（对照）
LEGACY_WITH_CRED = (
    "ConnectionError for url: https://user:SECRETPW@api.example.com/x?token=sk-live-r58"
)

HOST = "api.example.com"
UNPRINTABLE = UR.UNPRINTABLE_URL
CRED_NEEDLES = ("SECRETPW", "sk-live-r58")


def _assert_correctly_masked(text: str) -> None:
    """区分「正确掩码」与「过度掩码」的断言形（本单的测试盲修正本体）。

    旧形「凭据针不在」对 ``<unprintable-url>`` 与 ``https://host/***`` 同真；
    这里三元组缺一不可：host 保留（诊断价值）+ 常量不在场（不过度掩码）+
    凭据针与 path 明文不在场（真掩过）。
    """
    assert HOST in text, f"host:port 丢失（过度掩码）：{text!r}"
    assert UNPRINTABLE not in text, f"坠不可打印常量（过度掩码）：{text!r}"
    leaked = [n for n in CRED_NEEDLES if n in text]
    assert not leaked, f"凭据针泄漏：{leaked}\n{text[:600]}"
    assert "/v1/items/42" not in text and "token=" not in text, f"path/query 明文残留：{text!r}"


# ===========================================================================
# 注册态证明（修法①）
# ===========================================================================


def test_console_import_chain_registers_mask_url():
    """子进程证明（生产态，不受宿主进程态污染）：console 导入链加载完后
    ``url_redaction._mask_url`` 必须已注册。修前恒 UNREGISTERED。"""
    code = (
        "import sys; sys.path[:0]=[%r,%r]\n"
        "import plaita.core.url_redaction as UR\n"
        "import api.executions, api.flows, api.schedules, api.tenants\n"
        "import main\n"
        "print('REGISTERED' if UR._mask_url is not None else 'UNREGISTERED')\n"
    ) % (str(REPO_ROOT), str(BACKEND_DIR))
    env = dict(os.environ, PLAITA_CONSOLE_DB_URL="sqlite:////tmp/a2r58-03-registration-check.db")
    r = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True,
        env=env, cwd=str(BACKEND_DIR), timeout=180,
    )
    assert "UNREGISTERED" not in r.stdout and r.stdout.strip().endswith("REGISTERED"), (
        f"console 进程 _mask_url 未注册（stdout={r.stdout.strip()!r}）\n"
        f"stderr[-600:]={r.stderr[-600:]!r}"
    )


def test_console_process_registered_scrub_keeps_host_and_masks_credentials():
    """注册态（本进程经 main 导入链产生）scrub 语义：保 host、抹 path/query、
    抹凭据——与 worker（factory 注册态）同判据，四面读面共用这一条单一判据。"""
    assert UR._mask_url is not None, "宿主进程未注册（隔离会话里应由 main 导入链产生）"
    _assert_correctly_masked(UR._scrub_url_text(RAW_NO_CRED))
    _assert_correctly_masked(UR._scrub_url_text(LEGACY_WITH_CRED))


def test_console_rescrub_of_worker_scrubbed_text_keeps_host():
    """console 二次 scrub 不许把 worker 已保住的 host 抹掉（A2 探针 04b 的 S1→S2
    形态，修后语义）：已掩码形态是幂等不动点，host 必须存活。"""
    assert UR._mask_url is not None
    s2 = UR._scrub_url_text(WORKER_SCRUBBED)
    assert HOST in s2, f"二次 scrub 把 worker 保住的 host 抹掉：{s2!r}"
    assert UNPRINTABLE not in s2, s2


# ===========================================================================
# 四读面（detail error / timeline / SSE initial_state / SSE update）
# ===========================================================================


@pytest.fixture()
def console_client(tmp_path):
    from services import flow_store

    flow_store.init_engine(f"sqlite:///{tmp_path / 'a2r58-03.db'}")
    from api import executions as executions_api

    rd = fakeredis.FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = rd
    app.state.local_mode = False
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app), rd


def _seed(rd, eid="exec-a2r58-03"):
    state = {
        "execution_id": eid, "flow_id": "f", "flow_name": "f", "flow_version": "1.0.0",
        "status": "error", "tenant_id": "", "start_time": "2026-10-23T10:00:00",
        "end_time": "2026-10-23T10:05:00", "last_update_time": "2026-10-23T10:05:00",
        "context": {"$INPUT": {}},
        # worker 写 {"message": str(e)}：无凭据、只有诊断 URL（RAW）
        "error": {"code": -500, "error_type": "NodeExecutionError", "message": RAW_NO_CRED},
        "timeline": {"events": [
            {"seq": 1, "type": "node_start", "node_id": "call", "attempt": 1},
            # worker 源头已 scrub（注册态产物：保 host）
            {"seq": 2, "type": "node_end", "node_id": "call", "attempt": 1,
             "ok": False, "error": WORKER_SCRUBBED, "duration_ms": 120},
            # 滚动窗旧版 worker 存量：未 scrub、含凭据（对照）
            {"seq": 3, "type": "node_end", "node_id": "call2", "attempt": 1,
             "ok": False, "error": LEGACY_WITH_CRED, "duration_ms": 5},
            {"seq": 4, "type": "run_end", "status": "error", "final": True},
        ], "dropped": 0, "max_events": 500},
    }
    rd.set(f"plaita:execution:{eid}", json.dumps(state, ensure_ascii=False))
    return eid


def test_detail_error_message_keeps_host(console_client):
    """面 1：detail ``error.message``（前端取作人话标题）。"""
    client, rd = console_client
    eid = _seed(rd)
    r = client.get(f"/api/executions/{eid}")
    assert r.status_code == 200, r.text
    err = r.json()["error"]
    assert err["error_type"] == "NodeExecutionError"  # 机器判读键守恒（对照）
    _assert_correctly_masked(err["message"])


def test_timeline_worker_preserved_host_survives_rescrub(console_client):
    """面 2：timeline 端点——worker 已 scrub 保住 host 的存量文本（seq2）不许被
    console 二次 scrub 抹掉；旧版存量含凭据行（seq3）凭据针仍不在（对照）。"""
    client, rd = console_client
    eid = _seed(rd)
    r = client.get(f"/api/executions/{eid}/timeline")
    assert r.status_code == 200, r.text
    events = r.json()["events"]
    e2 = next(e for e in events if e.get("seq") == 2)["error"]
    _assert_correctly_masked(e2)
    body = r.text
    leaked = [n for n in CRED_NEEDLES if n in body]
    assert not leaked, f"timeline 响应体凭据针泄漏：{leaked}"


def test_sse_initial_state_view_keeps_host(console_client):
    """面 3：SSE ``initial_state`` 与 detail 共用 ``_redact_execution_view``。"""
    _client, rd = console_client
    eid = _seed(rd)
    info = json.loads(rd.get(f"plaita:execution:{eid}"))
    from api.executions import _redact_execution_view

    view = _redact_execution_view(dict(info))
    _assert_correctly_masked(view["error"]["message"])
    e2 = next(e for e in view["timeline"]["events"] if e.get("seq") == 2)["error"]
    _assert_correctly_masked(e2)


def test_sse_update_frame_keeps_host():
    """面 4：SSE update 帧消费侧 scrub（``_scrub_update_payload``）。"""
    from api.executions import _scrub_update_payload

    frame = json.dumps({
        "execution_id": "e", "type": "node_end", "node_id": "call", "ok": False,
        "error": WORKER_SCRUBBED, "duration_ms": 1,
    })
    out = json.loads(_scrub_update_payload(frame))
    _assert_correctly_masked(out["error"])
    legacy = json.loads(_scrub_update_payload(json.dumps({
        "execution_id": "e", "type": "node_end", "node_id": "call2", "ok": False,
        "error": LEGACY_WITH_CRED, "duration_ms": 1,
    })))
    _assert_correctly_masked(legacy["error"])


# ===========================================================================
# 未注册分支：fail-closed 契约守恒 + WARNING 频控（修法②）
# ===========================================================================


def test_unregistered_fallback_fail_closed_and_warning_throttled(monkeypatch, caplog):
    """``_mask_url`` 未注册（纯 node 进程态）：结果仍 fail-closed 常量（契约守恒），
    但 WARNING 只打**首条**，后续降 DEBUG——高频 scrub 面上不再逐条刷屏。"""
    monkeypatch.setattr(UR, "_mask_url", None)
    monkeypatch.setattr(UR, "_unregistered_count", 0)
    with caplog.at_level(logging.DEBUG, logger="plaita.core.url_redaction"):
        s1 = UR._scrub_url_text(RAW_NO_CRED)
        s2 = UR._scrub_url_text(RAW_NO_CRED)
    # 契约守恒：未注册态宁可过度掩码也绝不回原文（修前既有语义，不动）
    assert HOST not in s1 and UNPRINTABLE in s1, s1
    assert UR._unregistered_count == 2
    warnings = [r for r in caplog.records
                if r.levelno == logging.WARNING and "未注册" in r.getMessage()]
    assert len(warnings) == 1, (
        f"未注册 WARNING 未频控：{len(warnings)} 条\n"
        + "\n".join(r.getMessage() for r in warnings)
    )
    debugs = [r for r in caplog.records
              if r.levelno == logging.DEBUG and "未注册" in r.getMessage()]
    assert len(debugs) >= 1, "第二次起应降 DEBUG，而非彻底静默"


def test_registered_path_does_not_trip_unregistered_counter(monkeypatch):
    """对照：注册态 scrub 不走未注册分支（计数不动）。"""
    assert UR._mask_url is not None
    before = UR._unregistered_count
    UR._scrub_url_text(RAW_NO_CRED)
    assert UR._unregistered_count == before
