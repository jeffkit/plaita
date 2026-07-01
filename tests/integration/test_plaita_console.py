#!/usr/bin/env python3
"""
Comprehensive test script for plaita-console management UI.
Tests all pages and interactive elements.

This is a manual UI smoke script, not an automated pytest test: it
requires a running plaita-console at http://localhost:15173. Run it
directly with `python -m tests.integration.test_plaita_console`. It is
skipped under normal `pytest` collection to avoid spurious failures.
"""

import pytest

# Skip the whole module during pytest collection — needs playwright and
# a live console. importorskip also avoids an ImportError at collection
# time when playwright is not installed.
pytest.importorskip("playwright")
pytestmark = pytest.mark.skip(
    reason="manual plaita-console UI smoke test; requires a running console "
    "at http://localhost:15173 (run via "
    "`python -m tests.integration.test_plaita_console`)"
)

from playwright.sync_api import sync_playwright
import time
import json
from pathlib import Path

# Create screenshots directory
screenshots_dir = Path("plaita_console_test_screenshots")
screenshots_dir.mkdir(exist_ok=True)

# Test results
test_results = {
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    "pages": {}
}

def test_page(page, name, url, checks):
    """Test a single page and record results."""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"URL: {url}")
    print(f"{'='*60}")
    
    result = {
        "url": url,
        "status": "passed",
        "errors": [],
        "warnings": [],
        "checks": {}
    }
    
    try:
        # Navigate to page
        page.goto(url, timeout=10000)
        page.wait_for_load_state('networkidle', timeout=10000)
        time.sleep(1)  # Extra wait for animations
        
        # Take screenshot
        screenshot_path = screenshots_dir / f"{name.lower().replace(' ', '_')}.png"
        page.screenshot(path=str(screenshot_path), full_page=True)
        print(f"✓ Screenshot saved: {screenshot_path}")
        
        # Get console messages
        console_errors = []
        
        # Check for UI errors in page content
        content = page.content()
        if "error" in content.lower() or "错误" in content:
            error_elements = page.locator('text=/error|错误/i').all()
            if error_elements:
                result["warnings"].append(f"Found {len(error_elements)} potential error messages in UI")
        
        # Run page-specific checks
        for check_name, check_fn in checks.items():
            try:
                check_result = check_fn(page)
                result["checks"][check_name] = check_result
                status = "✓" if check_result.get("passed", False) else "✗"
                print(f"{status} {check_name}: {check_result.get('message', 'OK')}")
            except Exception as e:
                result["checks"][check_name] = {"passed": False, "error": str(e)}
                result["errors"].append(f"{check_name}: {str(e)}")
                print(f"✗ {check_name}: {str(e)}")
        
        # Check if page is blank
        body_text = page.locator('body').inner_text()
        if len(body_text.strip()) < 10:
            result["errors"].append("Page appears to be blank or has very little content")
            result["status"] = "failed"
        
    except Exception as e:
        result["status"] = "failed"
        result["errors"].append(f"Page load error: {str(e)}")
        print(f"✗ Failed to load page: {str(e)}")
    
    if result["errors"]:
        result["status"] = "failed"
    
    test_results["pages"][name] = result
    return result

def check_stats_cards(p):
    selector = '[class*="stat"], [class*="card"]'
    count = p.locator(selector).count()
    return {"passed": count > 0, "message": f"Found {count} stat cards"}

def check_recent_executions(p):
    selector = 'table, [role="table"]'
    has_table = p.locator(selector).count() > 0
    has_text = "执行" in p.content()
    return {"passed": has_table or has_text, "message": "Recent executions section present"}

def check_service_status(p):
    has_service = "服务" in p.content() or "service" in p.content().lower()
    return {"passed": has_service, "message": "Service status grid present"}

def check_table_renders(p):
    selector = 'table, [role="table"]'
    count = p.locator(selector).count()
    return {"passed": count > 0, "message": f"Found {count} tables"}

def check_filter_by_status(p):
    selector = 'select, [role="combobox"], button:has-text("状态")'
    count = p.locator(selector).count()
    return {"passed": count > 0, "message": "Status filter found"}

def check_start_process_button(p):
    try:
        button = p.locator('button:has-text("启动流程")').first
        if button.is_visible():
            button.click(timeout=2000)
            time.sleep(0.5)
            dialog_present = p.locator('[role="dialog"], .dialog, .modal').count() > 0
            if dialog_present:
                p.keyboard.press('Escape')
            msg = "Button clicked, dialog opened" if dialog_present else "Button clicked, no dialog"
            return {"passed": dialog_present, "message": msg}
        return {"passed": False, "message": "Button not visible"}
    except Exception as e:
        count = p.locator('button:has-text("启动流程")').count()
        return {"passed": count > 0, "message": f"Button exists but interaction failed: {str(e)}"}

def check_reactflow_canvas(p):
    selector = '.react-flow, [class*="reactflow"]'
    count = p.locator(selector).count()
    return {"passed": count > 0, "message": "ReactFlow canvas found"}

def check_canvas_rendered(p):
    svg_count = p.locator('svg').count()
    return {"passed": svg_count > 0, "message": f"Found {svg_count} SVG elements"}

def check_tabs_present(p):
    selector = '[role="tab"], .tab, button:has-text("事件")'
    tab_count = p.locator(selector).count()
    return {"passed": tab_count >= 3, "message": f"Found {tab_count} tabs"}

def check_event_subscription_tab(p):
    has_tab = "事件订阅" in p.content()
    return {"passed": has_tab, "message": "事件订阅 tab present"}

def check_event_record_tab(p):
    has_tab = "事件记录" in p.content()
    return {"passed": has_tab, "message": "事件记录 tab present"}

def check_publish_event_tab(p):
    try:
        selector = 'button:has-text("发布事件"), [role="tab"]:has-text("发布事件")'
        pub_tab = p.locator(selector).first
        if pub_tab.is_visible():
            pub_tab.click(timeout=2000)
            time.sleep(0.5)
            inputs = p.locator('input, textarea').count()
            return {"passed": inputs > 0, "message": f"Publish tab opened, found {inputs} input fields"}
        return {"passed": False, "message": "Publish event tab not visible"}
    except Exception as e:
        has_tab = "发布事件" in p.content()
        return {"passed": has_tab, "message": f"Tab present but interaction failed: {str(e)}"}

def check_queue_cards(p):
    selector = '[class*="card"], [class*="queue"]'
    card_count = p.locator(selector).count()
    return {"passed": card_count > 0, "message": f"Found {card_count} queue cards"}

def check_expand_queue(p):
    try:
        selector = 'button[aria-expanded], [class*="expand"], [class*="collapse"]'
        expandable = p.locator(selector).first
        if expandable.count() > 0 and expandable.is_visible():
            expandable.click(timeout=2000)
            time.sleep(0.5)
            return {"passed": True, "message": "Expanded queue successfully"}
        return {"passed": False, "message": "No expandable elements found"}
    except Exception as e:
        return {"passed": False, "message": f"Expand failed: {str(e)}"}

def check_log_list(p):
    selector = '[class*="log"], ul, table'
    log_count = p.locator(selector).count()
    return {"passed": log_count > 0, "message": f"Found {log_count} log containers"}

def check_filter_present(p):
    selector = 'input[type="text"], input[placeholder*="filter"], input[placeholder*="搜索"]'
    filter_count = p.locator(selector).count()
    return {"passed": filter_count > 0, "message": f"Found {filter_count} filter inputs"}

def check_sse_toggle(p):
    selector = 'input[type="checkbox"], button:has-text("SSE"), [role="switch"]'
    toggle_count = p.locator(selector).count()
    has_sse = "SSE" in p.content()
    msg = "SSE toggle found" if toggle_count > 0 else "SSE mentioned in content"
    return {"passed": toggle_count > 0 or has_sse, "message": msg}

def check_cluster_tabs(p):
    service_tab = "服务管理" in p.content()
    infra_tab = "基础设施" in p.content()
    config_tab = "集群配置" in p.content()
    count = sum([service_tab, infra_tab, config_tab])
    return {"passed": count >= 3, "message": f"Found {count}/3 expected tabs"}

def check_quick_test_dialog(p):
    try:
        test_btn = p.locator('button:has-text("快速测试")').first
        if test_btn.count() > 0 and test_btn.is_visible():
            test_btn.click(timeout=2000)
            time.sleep(0.5)
            dialog = p.locator('[role="dialog"], .dialog, .modal').count() > 0
            if dialog:
                run_btn = p.locator('button:has-text("运行"), button:has-text("执行"), button:has-text("测试")').first
                if run_btn.count() > 0:
                    run_btn.click(timeout=2000)
                    time.sleep(1)
                    result = {"passed": True, "message": "Quick test dialog opened and test executed"}
                else:
                    result = {"passed": True, "message": "Quick test dialog opened"}
                p.keyboard.press('Escape')
                return result
            return {"passed": False, "message": "Button clicked but dialog didn't open"}
        return {"passed": False, "message": "Quick test button not found"}
    except Exception as e:
        return {"passed": False, "message": f"Quick test failed: {str(e)}"}

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Collect console messages
        console_messages = []
        page.on("console", lambda msg: console_messages.append({
            "type": msg.type,
            "text": msg.text
        }))
        
        base_url = "http://localhost:15173"
        
        # Test 1: Dashboard (/)
        test_page(page, "Dashboard", f"{base_url}/", {
            "stats_cards": check_stats_cards,
            "recent_executions": check_recent_executions,
            "service_status": check_service_status
        })
        
        # Test 2: Executions (/executions)
        test_page(page, "Executions", f"{base_url}/executions", {
            "table_renders": check_table_renders,
            "filter_by_status": check_filter_by_status,
            "start_process_button": check_start_process_button
        })
        
        # Test 3: Topology (/topology)
        test_page(page, "Topology", f"{base_url}/topology", {
            "reactflow_canvas": check_reactflow_canvas,
            "canvas_rendered": check_canvas_rendered
        })
        
        # Test 4: Events (/events)
        test_page(page, "Events", f"{base_url}/events", {
            "tabs_present": check_tabs_present,
            "event_subscription_tab": check_event_subscription_tab,
            "event_record_tab": check_event_record_tab,
            "publish_event_tab": check_publish_event_tab
        })
        
        # Test 5: Queues (/queues)
        test_page(page, "Queues", f"{base_url}/queues", {
            "queue_cards": check_queue_cards,
            "expand_queue": check_expand_queue
        })
        
        # Test 6: Logs (/logs)
        test_page(page, "Logs", f"{base_url}/logs", {
            "log_list": check_log_list,
            "filter_present": check_filter_present,
            "sse_toggle": check_sse_toggle
        })
        
        # Test 7: Cluster (/cluster)
        test_page(page, "Cluster", f"{base_url}/cluster", {
            "tabs_present": check_cluster_tabs,
            "quick_test_dialog": check_quick_test_dialog
        })
        
        # Get console errors
        error_logs = [msg for msg in console_messages if msg["type"] == "error"]
        test_results["console_errors"] = error_logs
        
        if error_logs:
            print(f"\n⚠ Console Errors Found: {len(error_logs)}")
            for err in error_logs[:5]:  # Show first 5
                print(f"  - {err['text']}")
        
        browser.close()
    
    # Save results
    results_file = "plaita_console_test_results.json"
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(test_results, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    
    passed = sum(1 for p in test_results["pages"].values() if p["status"] == "passed")
    failed = sum(1 for p in test_results["pages"].values() if p["status"] == "failed")
    total = len(test_results["pages"])
    
    print(f"Total Pages Tested: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"\nResults saved to: {results_file}")
    print(f"Screenshots saved to: {screenshots_dir}/")
    
    for page_name, result in test_results["pages"].items():
        status_icon = "✓" if result["status"] == "passed" else "✗"
        print(f"\n{status_icon} {page_name}:")
        if result["errors"]:
            for err in result["errors"]:
                print(f"  ERROR: {err}")
        if result["warnings"]:
            for warn in result["warnings"]:
                print(f"  WARNING: {warn}")

if __name__ == "__main__":
    main()
