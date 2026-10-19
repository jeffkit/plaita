"""plaita#210：Supervisor 页三处请求必须走 API_BASE + 统一鉴权请求层。

仓库前端没有 Vitest/MSW（package.json 仅 dev/build）；本测试做**源码级**
回归，锁定 #210 的保护性质，而不是某种历史写法：

- 配置探测 ``${API_BASE}/supervisor/config``
- 数据集列表 ``${API_BASE}/flows/${flowId}/supervisor/datasets``
- 迭代 POST ``${API_BASE}/flows/${flowId}/supervisor/iterate``

S17 前端批（2026-10-19，e9075e0）把鉴权/超时/错误归一化收进
``services/api.ts`` 的 ``apiFetch``：认证头由该封装在调用时合并
``authHeaders()``（会话 Bearer；平台管理员另带 X-Tenant-ID），子路径仍由
``resolveApiUrl`` 解析，长跑 iterate 另带 ``API_TIMEOUTS.supervisorIterate``
分级超时。保护性质不变：探针依旧携带部署前缀与认证头。因此断言分两层且
缺一不可——页面层必须用 ``apiFetch(`${API_BASE}/...`)``，传输层必须真的注入
``authHeaders()``；传输层若弱化（例如不再带认证头），本测试红。

非 2xx 仍经 ``readErrorText`` → ``buildHttpError(...).userMessage`` 展示
status/detail，不静默吞错。前端构建（``npm run build``）另行验证 TS 编译
与打包。
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SUPERVISOR_TSX = (
    REPO_ROOT / "plaita-console" / "frontend" / "src" / "pages" / "Supervisor.tsx"
)
API_TS = REPO_ROOT / "plaita-console" / "frontend" / "src" / "services" / "api.ts"


@pytest.fixture(scope="module")
def source() -> str:
    if not SUPERVISOR_TSX.is_file():
        pytest.skip(f"前端源码不在当前树内：{SUPERVISOR_TSX}")
    return SUPERVISOR_TSX.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def api_source() -> str:
    if not API_TS.is_file():
        pytest.skip(f"前端源码不在当前树内：{API_TS}")
    return API_TS.read_text(encoding="utf-8")


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
    # 认证头由 apiFetch 注入；调用点只保留业务所需的 Content-Type。
    assert "headers: { 'Content-Type': 'application/json' }" in source
    assert "timeoutMs: API_TIMEOUTS.supervisorIterate" in source


def test_auth_headers_are_injected_by_api_layer(source):
    """鉴权头统一由 apiFetch 注入；页面不得回退手拼 fetch + authHeaders。"""
    assert "apiFetch" in source
    assert "authHeaders()" not in source
    assert "fetch(`${API_BASE}" not in source


def test_transport_preserves_api_base_and_injects_auth(api_source):
    """负控锚：apiFetch 去掉 authHeaders 合并 / API_BASE 不再走子路径解析即红。"""
    assert "export async function apiFetch(" in api_source
    assert "const url = resolveApiUrl(input, SUB_PATH)" in api_source
    assert "export const API_BASE = `${SUB_PATH}/api`" in api_source
    assert "for (const [key, value] of Object.entries(authHeaders()))" in api_source
    assert "if (!headers.has(key)) headers.set(key, value)" in api_source
    assert "headers['Authorization'] = `Bearer ${token}`" in api_source


def test_no_bare_api_fetch_remains(source):
    """负控锚：回到裸 ``fetch('/api/...')`` / ``fetch(`${API_BASE}...`)`` 即红。"""
    assert "fetch('/api" not in source
    assert "fetch(`/api" not in source
    assert "fetch(`${API_BASE}" not in source
    assert "await fetch(" not in source


def test_error_branches_surface_status_and_detail(source):
    assert "async function readErrorText(" in source
    assert "buildHttpError(resp.status, text, resp.headers.get('content-type'), resp.url).userMessage" in source
    assert "if (!r.ok) throw new Error(await readErrorText(r))" in source
    assert "setError(await readErrorText(resp))" in source
    assert "errorUserMessage(e)" in source
