#!/usr/bin/env python3
"""ui-semantic-ids.yaml 的本地等价执行器（不依赖 argusai/mcp2cli）。

用途：在无法本地起 argusai-mcp daemon 的机器上，用 Playwright 按 suite YAML
逐 case 执行 browser 步骤 + expect.page 断言，验证 suite 内容本身的正确性
（选择器、等待式、evaluate 脚本、断言值）。argusai 引擎语义（变量模板、
重试、持久化）不在覆盖范围——那部分留给 CI 的 e2e-run.sh 链路。

用法：
    BASE_URL=http://127.0.0.1:8124 ADMIN_PASSWORD=test-admin-1234 \
        python scripts/e2e-ui-semantic-ids-local.py
"""
from __future__ import annotations

import os
import sys

import yaml
from playwright.sync_api import sync_playwright

HERE = os.path.dirname(os.path.abspath(__file__))
SUITE = os.path.join(HERE, "..", "tests", "e2e", "ui-semantic-ids.yaml")
BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8124").rstrip("/")
PASSWORD = os.environ.get("ADMIN_PASSWORD", "test-admin-1234")

# e2e 环境的 bootstrap 密码与本机实例不同：把 suite 里的 e2e 密码替换成本地凭据
SUITE_YAML = open(SUITE, encoding="utf-8").read().replace("e2e-admin-pass", PASSWORD)
suite = yaml.safe_load(SUITE_YAML)

results: list[tuple[str, bool]] = []


def check(name: str, cond: bool) -> None:
    results.append((name, bool(cond)))
    print(("  ✅ " if cond else "  ❌ ") + name)


def run_action(pg, act: dict):
    action = act["action"]
    selector = act.get("selector")
    timeout = 15000
    if action == "goto":
        pg.goto(act["url"].replace("http://localhost:18081", BASE), wait_until="domcontentloaded")
    elif action == "click":
        pg.locator(selector).first.click(timeout=timeout)
    elif action == "fill":
        pg.locator(selector).first.fill(act["value"], timeout=timeout)
    elif action == "waitForSelector":
        pg.locator(selector).first.wait_for(state="visible", timeout=timeout)
    elif action == "waitForLoadState":
        pg.wait_for_load_state(act.get("state", "load"))
    elif action == "screenshot":
        pg.screenshot(path=act["path"])
    elif action == "select":
        pg.locator(selector).first.select_option(act.get("option"), timeout=timeout)
    elif action == "evaluate":
        return pg.evaluate(act["script"])
    else:
        raise ValueError(f"mini-executor 未实现 action: {action}")


def assert_page(pg, expect: dict) -> None:
    page = expect.get("page") or {}
    for sel in page.get("visible", []) or []:
        check(f"visible {sel}", pg.locator(sel).first.is_visible())
    for sel in page.get("hidden", []) or []:
        check(f"hidden {sel}", pg.locator(sel).count() == 0 or not pg.locator(sel).first.is_visible())
    for sel, want in (page.get("text") or {}).items():
        actual = pg.locator(sel).first.inner_text()
        want = want if isinstance(want, str) else want.get("contains", "")
        check(f"text {sel} contains {want!r}", want in actual)
    if "result" in page:
        # result 断言针对「最近一次 evaluate」，执行器按步骤顺序天然满足
        pass


def main() -> int:
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        pg = b.new_context(viewport={"width": 1680, "height": 1000}).new_page()
        last_eval = None
        for case in suite["cases"]:
            print(f"· {case['name']}")
            try:
                if "browser" in case:
                    r = run_action(pg, case["browser"])
                    if r is not None:
                        last_eval = r
                expect = case.get("expect") or {}
                page_expect = dict(expect.get("page") or {})
                if "result" in page_expect:
                    want = page_expect.pop("result")
                    if last_eval != want:
                        print(f"    (实际 result: {last_eval!r})")
                    check(f"result == {want!r}", last_eval == want)
                if page_expect:
                    assert_page(pg, {"page": page_expect})
            except Exception as e:  # noqa: BLE001  任一步失败即标记并继续后续 case
                check(f"case 异常: {str(e)[:120]}", False)
        b.close()

    passed = sum(1 for _, c in results if c)
    print(f"\n断言汇总: {passed} / {len(results)}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
