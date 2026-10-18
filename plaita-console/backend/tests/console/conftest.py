"""tests/console 的测试环境自愈（plaita#172 基线 16 条红的归因落地）。

#172 归因结论（双跑：`tests/console` 全量 ×2，失败集逐条相同 16/16）：
**16 条全部是环境问题，无一为代码回归**，分两类——

1) 11 条 `test_issue157_resume_params.py`：用例测的是**本 checkout 已修好的**
   #157 实现（`plaita-console/backend/api/executions.py` 的
   `ResumeFlowRequest.params`、`plaita/server/flow_worker.py` 的
   `_handle_start_dedup_hit(..., params=…)`）。但 venv 的 editable 安装
   （`__editable__.plaita-0.6.1.pth` → `MAPPING['plaita']`）指向**另一个检出**，
   worktree 场景下即落后的主检出，于是 `import plaita.*` 拿到旧代码 → 断言全红。
   本 conftest 把本 checkout 根前置到 sys.path：setuptools 的 editable finder 是
   `sys.meta_path.append(...)`，排在 `PathFinder` **之后**，所以 sys.path 前置即可
   压过它。conftest 的导入发生在同目录测试模块导入之前，因此整目录一次生效
   （`test_e2e_closure.py` 里同款 `parents[4]` 前置只对该文件首访有效——同会话里
   `plaita` 一旦被前面的用例按旧路径导入，`sys.modules` 缓存就让它失效）。

2) 5 条（`test_flow_version_api.py` ×3 / `test_tenants.py` ×1 /
   `test_e2e_closure.py` ×1）：venv 缺**已声明依赖** `python-multipart`
   （`plaita-console/backend/requirements.txt:16`）——flowVersion 契约端点是表单
   端点（`api/flow_version.py:79` `await request.form()`），缺库时 Starlette 直接
   抛断言，不是本仓代码问题。依赖装好后这些用例**真跑**（本仓验证环境即装了
   `python-multipart==0.0.32`，5 条全绿、无 skip）；`needs_form_parser` 标记只
   作为「装不了依赖的环境」的具名 skip 兜底，理由是给到位的、可执行的。
"""
from __future__ import annotations

import importlib.util
import sys
import warnings
from pathlib import Path

import pytest

# `<repo>/plaita-console/backend/tests/console/conftest.py` → parents[4] = 仓根
REPO_ROOT = Path(__file__).resolve().parents[4]

# ---- 1) 让 console 测试只测本 checkout 的 plaita，而非 editable 安装指向的检出 ----
if (REPO_ROOT / "plaita" / "__init__.py").is_file():
    _root = str(REPO_ROOT)
    if _root in sys.path:
        sys.path.remove(_root)
    sys.path.insert(0, _root)
else:  # 布局异常（非源码仓内运行）时不硬失败，退回既有行为
    warnings.warn(
        f"未找到 {REPO_ROOT / 'plaita' / '__init__.py'}：console 测试将按 "
        "PYTHONPATH/editable 安装解析 plaita.*，可能测到的不是本 checkout 的代码",
        stacklevel=1,
    )

_HAS_FORM_PARSER = importlib.util.find_spec("multipart") is not None

_FORM_PARSER_SKIP_REASON = (
    "环境缺 python-multipart（plaita-console/backend/requirements.txt:16 已声明）："
    "flowVersion 契约端点是表单端点（api/flow_version.py:79 `await request.form()`），"
    "缺库时 starlette 抛断言而非业务响应。装上即真跑："
    "uv pip install --python <venv>/bin/python 'python-multipart>=0.0.12'"
)


@pytest.fixture(autouse=True)
def _explicit_default_tenant():
    """plaita#239 phase 2：console 测试显式声明 default 租户。

    写路径对来源 ``default`` 已 fail-closed；此 fixture 等价 console 启动
    入口的迁移行（``set_current_tenant(DEFAULT, source='explicit')``）。
    需要「缺失上下文」形状的用例自行在 fresh ``Context()`` 中运行。
    """
    from plaita.tenant_context import (
        DEFAULT_TENANT_ID,
        TENANT_SOURCE_EXPLICIT,
        reset_current_tenant,
        set_current_tenant,
    )

    token = set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
    try:
        yield
    finally:
        reset_current_tenant(token)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "needs_form_parser: 该用例经由 flowVersion 表单端点，需要已声明依赖 "
        "python-multipart（plaita-console/backend/requirements.txt:16）",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """依赖缺失时把 needs_form_parser 用例转成**具名 skip**（带装法），
    而不是留一堆 starlette 断言红掩盖真回归信号（#172 影响面）。"""
    if _HAS_FORM_PARSER:
        return
    skip = pytest.mark.skip(reason=_FORM_PARSER_SKIP_REASON)
    for item in items:
        if "needs_form_parser" in item.keywords:
            item.add_marker(skip)
