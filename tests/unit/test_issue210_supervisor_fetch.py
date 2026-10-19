"""plaita#210：Supervisor 页三处请求必须走 API_BASE + 统一鉴权请求层。

仓库前端没有 Vitest/MSW（package.json 仅 dev/build）；本测试做**源码级**
回归，锁定三处调用不再回到裸 ``fetch('/api/...')``：

- 配置探测 ``${API_BASE}/supervisor/config``
- 数据集列表 ``${API_BASE}/flows/${flowId}/supervisor/datasets``
- 迭代 POST ``${API_BASE}/flows/${flowId}/supervisor/iterate``

S17 前端批（2026-10-19，e9075e0）把鉴权/超时/错误归一化收进
``services/api.ts`` 的 ``apiFetch``：Authorization / X-Admin-API-Key /
X-Tenant-ID 由该封装注入（``authHeaders()`` 不再由页面手工调用），长跑
iterate 另带 ``API_TIMEOUTS.supervisorIterate`` 分级超时。本测试因此同时
锁定「走 apiFetch + API_BASE」与「页面不再手拼鉴权头」两个契约。
前端构建（``npm run build``）另行验证 TS 编译与打包。
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SUPERVISOR_TSX = (
    REPO_ROOT / "plaita-console" / "frontend" / "src" / "pages" / "Supervisor.tsx"
)


@pytest.fixture(scope="module")
def source() -> str:
    if not SUPERVISOR_TSX.is_file():
        pytest.skip(f"前端源码不在当前树内：{SUPERVISOR_TSX}")
    return SUPERVISOR_TSX.read_text(encoding="utf-8")


def test_config_probe_uses_api_base_and_auth_fetch(source):
    assert "${API_BASE}/supervisor/config" in source
    assert "apiFetch(`${API_BASE}/supervisor/config`)" in source


def test_datasets_uses_api_base_and_auth_fetch(source):
    assert "${API_BASE}/flows/${flowId}/supervisor/datasets" in source
    assert "apiFetch(`${API_BASE}/flows/${flowId}/supervisor/datasets`)" in source


def test_iterate_uses_api_base_and_auth_fetch(source):
    assert "${API_BASE}/flows/${flowId}/supervisor/iterate" in source
    assert "apiFetch(`${API_BASE}/flows/${flowId}/supervisor/iterate`, {" in source
    assert "method: 'POST'" in source
    assert "timeoutMs: API_TIMEOUTS.supervisorIterate" in source


def test_auth_headers_are_injected_by_api_layer(source):
    """鉴权头统一由 apiFetch 注入；页面不得回退手拼 fetch + authHeaders。"""
    assert "apiFetch" in source
    assert "authHeaders()" not in source
    assert "fetch(`${API_BASE}" not in source


def test_no_bare_api_fetch_remains(source):
    """负控锚：恢复 ``fetch('/api/...')`` / ``fetch(`/api/...`)`` 即红。"""
    assert "fetch('/api" not in source
    assert "fetch(`/api" not in source


def test_error_branches_surface_status_and_detail(source):
    assert "async function readErrorText(" in source
    assert "if (!r.ok) throw new Error(await readErrorText(r))" in source
    assert "setError(await readErrorText(resp))" in source
    assert "errorUserMessage(e)" in source
