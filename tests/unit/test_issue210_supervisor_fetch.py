"""plaita#210：Supervisor 页三处请求必须走 API_BASE + authHeaders。

仓库前端没有 Vitest/MSW（package.json 仅 dev/build）；本测试做**源码级**
回归，锁定三处调用不再回到裸 ``fetch('/api/...')``：

- 配置探测 ``${API_BASE}/supervisor/config``
- 数据集列表 ``${API_BASE}/flows/${flowId}/supervisor/datasets``
- 迭代 POST ``${API_BASE}/flows/${flowId}/supervisor/iterate``

``API_BASE`` 已按 ``SUB_PATH`` 拼接（``services/api.ts``），``authHeaders()``
注入 Authorization / X-Admin-API-Key / X-Tenant-ID；错误分支用
``readErrorText`` / ``errorDetailText`` 展示 status/detail。
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


def test_config_probe_uses_api_base_and_auth_headers(source):
    assert "${API_BASE}/supervisor/config" in source
    assert "fetch(`${API_BASE}/supervisor/config`, { headers: authHeaders() })" in source


def test_datasets_uses_api_base_and_auth_headers(source):
    assert "${API_BASE}/flows/${flowId}/supervisor/datasets" in source
    assert (
        "fetch(`${API_BASE}/flows/${flowId}/supervisor/datasets`, "
        "{ headers: authHeaders() })" in source
    )


def test_iterate_uses_api_base_and_auth_headers(source):
    assert "${API_BASE}/flows/${flowId}/supervisor/iterate" in source
    assert "headers: { 'Content-Type': 'application/json', ...authHeaders() }" in source


def test_no_bare_api_fetch_remains(source):
    """负控锚：恢复 ``fetch('/api/...')`` / ``fetch(`/api/...`)`` 即红。"""
    assert "fetch('/api" not in source
    assert "fetch(`/api" not in source


def test_error_branches_surface_status_and_detail(source):
    assert "function errorDetailText(" in source
    assert "async function readErrorText(" in source
    assert "if (!r.ok) throw new Error(await readErrorText(r))" in source
    assert "setError(errorDetailText(body, resp))" in source
