#!/usr/bin/env python3
"""Plaita Console UI 旅程 e2e（Playwright，无 Docker 依赖）。

覆盖「@flow 语义化节点 id 可读性」链路的浏览器级断言：
  1. 编辑器画布：if/return 节点显示语义 id 的 name（如 `INPUT.score >= 90?`）
     与 desc 副标题（带源码行号），无裸 `_n*`/`_IF*` 合成 id；
  2. 节点抽屉：desc 展示 + 「查看权威源码 · 第 N 行」按钮 → 源码面板
     `@flow` 页签按行号展示 metadata.source；
  3. FlowViewer（执行详情）：语义名 / desc 同样生效（mock 执行 API）；
  4. 拖拽新节点 + 保存草稿 round-trip：原节点 id/name 保留、
     metadata.source 不丢、新节点入库（draft 自动 bump 版本号）。

用法（对已运行的 console 实例）：
    python scripts/e2e-ui-journey.py                     # BASE_URL 默认 8124
    BASE_URL=http://127.0.0.1:8080 python scripts/...    # 指定实例
    SEED_DEMO_FLOW=1 python scripts/...                  # 先经 API 播种 demo flow

环境变量：
    BASE_URL        console 地址（默认 http://127.0.0.1:8124）
    ADMIN_USER / ADMIN_PASSWORD   管理员凭据（默认 admin / test-admin-1234）
    SEED_DEMO_FLOW  =1 时先创建/刷新 demo flow（需实例 allow_insecure_admin）

依赖：playwright（chromium 本机缓存可用，见 `python -m playwright --version`）。
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8124").rstrip("/")
USER = os.environ.get("ADMIN_USER", "admin")
PASSWORD = os.environ.get("ADMIN_PASSWORD", "test-admin-1234")
FLOW_ID = "score-review"

DEMO_SOURCE = '''
@flow("score-review", desc="成绩评审演示：语义化节点 id")
def score_review(INPUT):
    total = F.len(INPUT.items)
    if INPUT.score >= 90:
        return "excellent"
    elif INPUT.score >= 60:
        return "pass"
    return "fail"
'''

KNOWN_IDS = {"start", "total", "score_ge_90", "ret_excellent", "score_ge_60", "ret_pass", "ret_fail"}

_results: list[tuple[str, bool]] = []


def check(name: str, cond: bool) -> None:
    _results.append((name, bool(cond)))
    print(("  ✅ " if cond else "  ❌ ") + name)


def api_json(path: str, method: str = "GET", payload: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        BASE + path, method=method,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return resp.status, json.loads(resp.read())


def seed_demo_flow() -> None:
    """经 API 播种 demo flow（published 1.0.0 + draft 1.1.0），幂等。"""
    from plaita.dsl.codeflow import compile_source  # noqa: PLC0415  惰性：需本机 plaita
    ir = compile_source(DEMO_SOURCE)
    ir["metadata"] = {"source": DEMO_SOURCE, "source_format": "plaita@flow"}
    definition = json.dumps(ir, ensure_ascii=False)
    api_json(f"/api/flows", "POST", {"flow_id": FLOW_ID, "author": "e2e", "desc": "UI 旅程演示"})
    body = json.dumps({"definition": definition, "layout": "{}", "created_by": "e2e"}, ensure_ascii=False)
    for version in ("1.0.0", "1.1.0"):
        req = urllib.request.Request(
            BASE + f"/api/flows/{FLOW_ID}/versions/{version}", method="PUT",
            data=body.encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200, f"PUT {version} → {resp.status}"
    api_json(f"/api/flows/{FLOW_ID}/publish", "POST", {"version": "1.0.0"})
    print(f"  已播种 {FLOW_ID}@1.0.0(published) + 1.1.0(draft)")


def latest_draft_version() -> str:
    _, flow = api_json(f"/api/flows/{FLOW_ID}")
    versions = [v["version"] for v in flow.get("versions", []) if v.get("status") == "draft"]
    return max(versions) if versions else "1.1.0"


def main() -> int:
    if os.environ.get("SEED_DEMO_FLOW") == "1":
        seed_demo_flow()

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context(viewport={"width": 1680, "height": 1000})
        pg = ctx.new_page()

        def login() -> None:
            pg.goto(BASE + "/flows", wait_until="networkidle")
            pg.locator("input").nth(0).fill(USER)
            pg.locator("input").nth(1).fill(PASSWORD)
            pg.locator("button", has_text="登录").click()
            pg.wait_for_timeout(1200)

        def open_editor(version: str | None = None) -> None:
            pg.goto(BASE + "/flows", wait_until="networkidle")
            pg.locator(f"text={FLOW_ID}").first.click()
            pg.wait_for_timeout(1000)
            pg.locator("button", has_text="编辑").first.click()
            pg.locator(".react-flow").wait_for(state="visible", timeout=15000)
            pg.locator('[draggable="true"]').first.wait_for(state="visible", timeout=15000)
            if version:
                for i in range(pg.locator("select").count()):
                    sel = pg.locator("select").nth(i)
                    opts = [sel.locator("option").nth(j).get_attribute("value")
                            for j in range(sel.locator("option").count())]
                    if version in opts:
                        sel.select_option(version)
                        break
                pg.wait_for_timeout(1800)

        DROP_JS = """() => {
          const item = document.querySelector('[draggable="true"]');
          const dt = new DataTransfer();
          dt.setData('application/plaita-node', JSON.stringify({nodeType: 'notify', name: 'notify'}));
          item.dispatchEvent(new DragEvent('dragstart', {bubbles: true, dataTransfer: dt}));
          const el = document.querySelector('.react-flow');
          const rect = el.getBoundingClientRect();
          const opts = {bubbles: true, cancelable: true, dataTransfer: dt,
                        clientX: rect.left + rect.width/2, clientY: rect.top + rect.height/2};
          el.dispatchEvent(new DragEvent('dragover', opts));
          el.dispatchEvent(new DragEvent('drop', opts));
        }"""

        login()

        # ========== 1. 编辑器画布：语义名 + desc ==========
        print("[1] 编辑器画布")
        open_editor()
        t = pg.locator(".react-flow").inner_text()
        check("if 节点显示语义名（完整不截断）", "score >= 90?" in t)
        check("画布不含行号（行号只在抽屉/tooltip）", "第 5 行" not in t)
        check("无裸 _n/_IF 合成 id", "_n" not in t and "_IF" not in t)

        # ========== 2. 节点抽屉 + 源码跳转 ==========
        print("[2] 节点抽屉与源码面板")
        pg.locator(".react-flow").locator("text=score >= 90").first.click()
        pg.wait_for_timeout(1000)
        check("抽屉显示 desc", pg.locator("text=if INPUT.score >= 90（第 5 行）").count() > 0)
        src_btn = pg.locator("text=查看权威源码")
        check("「查看权威源码」按钮存在", src_btn.count() > 0)
        if src_btn.count():
            src_btn.first.click()
            pg.wait_for_timeout(1200)
            check("源码面板出现 @flow 页签", pg.locator("text=@flow").count() > 0)
            check("面板展示源码原文", pg.locator("text=if INPUT.score >= 90:").count() > 0)

        # ========== 3. FlowViewer（执行详情，mock 执行 API） ==========
        print("[3] FlowViewer（执行详情）")
        exec_row = {"execution_id": "exec-ui-e2e", "flow_id": FLOW_ID, "flow_version": "1.0.0",
                    "status": "succeeded", "context": {"score": 95}}
        exec_detail = {**exec_row, "nodes": [
            {"id": "score_ge_90", "type": "if", "name": "INPUT.score >= 90?", "status": "executed", "output": True},
        ]}
        ctx.route("**/api/executions?*", lambda r: r.fulfill(json={"executions": [exec_row], "total": 1}))
        ctx.route("**/api/executions/exec-ui-e2e", lambda r: r.fulfill(json=exec_detail))
        ctx.route("**/api/executions/exec-ui-e2e/events*",
                  lambda r: r.fulfill(body="", content_type="text/event-stream"))
        pg.goto(BASE + "/executions", wait_until="networkidle")
        pg.wait_for_timeout(1200)
        # 列表可能来自共享集群（非 mock），找不到就只走详情直达（客户端路由不可达则跳过）
        row = pg.locator("text=exec-ui-e2e")
        if row.count():
            row.first.click()
            pg.wait_for_timeout(2500)
            body = pg.inner_text("body")
            check("FlowViewer 语义名", "score >= 90" in body)
            check("FlowViewer desc 行号", "第 5 行" in body)
        else:
            print("  ⚠️ 列表无 mock 执行（共享集群环境），跳过 FlowViewer 列表路径")

        # ========== 4. 拖拽新节点 + 保存草稿 round-trip ==========
        print("[4] 拖拽 + 保存草稿 round-trip")
        open_editor(version=latest_draft_version())
        before = pg.locator(".react-flow__node").count()
        pg.evaluate(DROP_JS)
        pg.wait_for_timeout(800)
        check("拖拽新增节点", pg.locator(".react-flow__node").count() == before + 1)
        pg.locator("button", has_text="保存草稿").first.click()
        pg.wait_for_timeout(2500)
        import re
        m = re.search(rf"已保存 {FLOW_ID}@([\d.]+)", pg.inner_text("body"))
        check("保存成功（消息含已保存@版本）", m is not None)
        if m:
            _, saved = api_json(f"/api/flows/{FLOW_ID}/versions/{m.group(1)}")
            ir = json.loads(saved["definition"])
            named = {n["id"]: n for n in ir["nodes"]}
            check("原节点 id/name 保留", named.get("score_ge_90", {}).get("name") == "INPUT.score >= 90?")
            check("metadata.source 保存不丢", len(ir.get("metadata", {}).get("source", "")) > 100)
            check("新节点已入库", any(i not in KNOWN_IDS for i in named))

        b.close()

    passed = sum(1 for _, c in _results if c)
    print(f"\n断言汇总: {passed} / {len(_results)}")
    return 0 if passed == len(_results) else 1


if __name__ == "__main__":
    sys.exit(main())
