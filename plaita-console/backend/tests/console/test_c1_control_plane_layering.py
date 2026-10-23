"""C1 控制面逻辑边界门（plaita#323）。

console 后端按**逻辑域**切分（不物理搬包；域 = 模块归属，物理布局另行处理）：

    shared（无域工具：config/error_contract/request_models/secure_fields/
            streaming、api.redis_scan、services.{atomic_io,read_redaction,
            runtime_pools}）
        ↘ 只许被 import，不许 import 任何域
    registry（SQL 为权威的定义模型：models、migrations、flow_store 及其
            同域 svc、deployments、schedule_store、graph/validation 族）
        ↛ gateway / observability
    observability（观测读面：api.{audit,logs,events,metrics,sse_redis}、
            services.{audit,obs_link}）
        ↛ gateway
    gateway（控制面入口 + 运行面：main/run/__main__/auth、其余 api/*、
            dispatcher/service_manager/local_*/reconcile/examples 等）
        顶层，不限

禁表按**源码级 runtime import** 执法（含函数体惰性导入——console 的惰性
导入恰恰是真实依赖边，见 examples→api.flows 历史）；``if TYPE_CHECKING:``
块豁免（类型依赖不产生运行时耦合，与 ``tests/integration/test_layering.py``
同口径）。console 存在包/平铺双布局（``from ..services import x`` 与
``from services import x``），解析器把两种写法归约到同一逻辑模块名再比对。

**未登记模块 = 门红**：新增文件必须显式归域（在 ``_DOMAINS`` 加一行），
防止新代码无意识落在错误域。唯一具名豁免：``api/{audit,logs,events} →
auth``（``require_auth`` / ``tenant_scope``——HTTP 鉴权是全路由框架横切，
不是 observability 对运行面的业务依赖）；豁免必须随真实导入共存（见
``test_exempt_edges_still_referenced``），防豁免表腐烂。
"""
from __future__ import annotations

import ast
import pathlib

import pytest

_BACKEND = pathlib.Path(__file__).resolve().parents[2]

# 模块逻辑名（相对 backend 根，点分）→ 域。前缀规则只给 migrations 族
# （alembic 版本文件会持续新增）；api/ 与 services/ 的新文件必须显式登记。
_DOMAINS: dict[str, str] = {
    # ---- shared：无域工具 ----
    "__init__": "shared",
    "config": "shared",
    "error_contract": "shared",
    "request_models": "shared",
    "secure_fields": "shared",
    "streaming": "shared",
    "api.redis_scan": "shared",
    "services.atomic_io": "shared",
    "services.read_redaction": "shared",
    "services.runtime_pools": "shared",
    # ---- registry：SQL 为权威的定义模型 ----
    "models": "registry",
    "models.flow": "registry",
    "services.flow_store": "registry",
    "services.schema_migrations": "registry",
    "services.tenant_write_guard": "registry",
    "services.tenants_svc": "registry",
    "services.users_svc": "registry",
    "services.credentials_svc": "registry",
    "services.node_registry_svc": "registry",
    "services.engine_outbox": "registry",
    "services.engine_sync": "registry",
    "services.validation": "registry",
    "services.capability_gate": "registry",
    "services.graph_validation": "registry",
    "services.deployments": "registry",
    "services.schedule_store": "registry",
    # ---- observability：观测读面 ----
    "api.audit": "observability",
    "api.logs": "observability",
    "api.events": "observability",
    "api.metrics": "observability",
    "api.sse_redis": "observability",
    "services.audit": "observability",
    "services.obs_link": "observability",
    # ---- gateway：控制面入口 + 运行面（顶层）----
    "api": "gateway",
    "services": "gateway",
    "auth": "gateway",
    "main": "gateway",
    "run": "gateway",
    "__main__": "gateway",
    "api.auth_users": "gateway",
    "api.capacity": "gateway",
    "api.cluster": "gateway",
    "api.copilot": "gateway",
    "api.credential_templates": "gateway",
    "api.credentials": "gateway",
    "api.dryrun": "gateway",
    "api.executions": "gateway",
    "api.flow_version": "gateway",
    "api.flows": "gateway",
    "api.graphs": "gateway",
    "api.nodes": "gateway",
    "api.property_types": "gateway",
    "api.queues": "gateway",
    "api.schedules": "gateway",
    "api.services": "gateway",
    "api.supervisor": "gateway",
    "api.tenants": "gateway",
    "services.ai_flow": "gateway",
    "services.claude_brain": "gateway",
    "services.cluster_registry": "gateway",
    "services.config_drift": "gateway",
    "services.dispatcher": "gateway",
    "services.dryrun": "gateway",
    "services.examples": "gateway",
    "services.flow_agent": "gateway",
    "services.graph_generation": "gateway",
    "services.local_executor": "gateway",
    "services.local_scheduler": "gateway",
    "services.login_guard": "gateway",
    "services.process_allowlist": "gateway",
    "services.reconcile": "gateway",
    "services.service_manager": "gateway",
    "services.signature": "gateway",
}

# 前缀归属（点分前缀，最长命中优先语义由遍历顺序保证：先查精确表）。
_DOMAIN_PREFIXES: tuple[tuple[str, str], ...] = (
    ("migrations", "registry"),
)

_FORBIDDEN: dict[str, tuple[str, ...]] = {
    "shared": ("registry", "observability", "gateway"),
    "registry": ("gateway", "observability"),
    "observability": ("gateway",),
    "gateway": (),
}

# 具名豁免（源模块, 目标模块）：HTTP 鉴权框架横切——observability 读面路由
# 挂同一套 admin 依赖（require_auth/tenant_scope）；auth 是会话/鉴权设施。
# 每条豁免必须仍有真实导入对应（test_exempt_edges_still_referenced）。
_EXEMPT: frozenset[tuple[str, str]] = frozenset(
    {
        ("api.audit", "auth"),
        ("api.logs", "auth"),
        ("api.events", "auth"),
    }
)

# console 双布局的仓内顶层名（平铺 ``from services import x`` 与包布局
# ``from ..services import x`` 必须归约到同一逻辑名）。
_INTERNAL_TOPS = frozenset(
    {
        "api",
        "services",
        "models",
        "migrations",
        "auth",
        "config",
        "error_contract",
        "main",
        "request_models",
        "run",
        "secure_fields",
        "streaming",
    }
)


def _domain_for(module: str):
    if module in _DOMAINS:
        return _DOMAINS[module]
    for prefix, domain in _DOMAIN_PREFIXES:
        if module == prefix or module.startswith(prefix + "."):
            return domain
    return None


def _module_name(pyfile: pathlib.Path, backend: pathlib.Path) -> str:
    rel = pyfile.relative_to(backend).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _runtime_files(backend: pathlib.Path):
    for pyfile in sorted(backend.rglob("*.py")):
        rel = pyfile.relative_to(backend).as_posix()
        if rel.startswith(("tests/", "build/")) or "__pycache__" in rel:
            continue
        yield pyfile


def _known_modules(backend: pathlib.Path) -> set[str]:
    return {m for p in _runtime_files(backend) if (m := _module_name(p, backend))}


def _resolve_import(node: ast.AST, pkg_parts: list[str], known: set[str]) -> list[str]:
    """一条 import 语句 → 本仓逻辑模块名列表（双布局归约到同一名字）。

    相对导入按所在包上溯（PEP 328）；平铺绝对导入取顶层名直认。名字候选
    优先于裸包：``from services import audit`` 记 ``services.audit``，不记
    裸 ``services``（包仅是导入载体，不是依赖目标）；候选不是模块时向上
    归约（``services.tenant_write_guard.assert_tenant_generation`` →
    ``services.tenant_write_guard``）。越出 backend 根的相对导入不产生仓内
    名字。
    """
    ordered: list[str] = []
    if isinstance(node, ast.Import):
        ordered = [alias.name for alias in node.names]
    elif isinstance(node, ast.ImportFrom):
        if node.level:
            # PEP 328：一层=当前包。``from .. import x`` 上溯到根后名字落在
            # 根层，合法；上溯步数**超过**包深度才是越出 backend。
            if node.level - 1 > len(pkg_parts):
                return []
            head = (
                pkg_parts[: len(pkg_parts) - (node.level - 1)]
                if node.level > 1
                else pkg_parts
            )
            base = ".".join(head)
            if node.module:
                mod_path = f"{base}.{node.module}" if base else node.module
                # 名字候选在前：``from .pkg import submod`` 记 pkg.submod，
                # 不记裸 pkg（包仅是导入载体）；名字不是模块时向上归约兜底。
                for alias in node.names:
                    if alias.name != "*":
                        ordered.append(f"{mod_path}.{alias.name}")
                ordered.append(mod_path)
            else:
                for alias in node.names:
                    if alias.name != "*":
                        ordered.append(f"{base}.{alias.name}" if base else alias.name)
        else:
            if node.module:
                for alias in node.names:
                    if alias.name != "*":
                        ordered.append(f"{node.module}.{alias.name}")
                ordered.append(node.module)

    resolved: list[str] = []
    for cand in ordered:
        if cand.startswith("plaita_console."):
            cand = cand[len("plaita_console."):]
        if cand.split(".")[0] not in _INTERNAL_TOPS:
            continue
        while cand and cand not in known:
            cand = cand.rsplit(".", 1)[0] if "." in cand else ""
        if cand and not any(r == cand or r.startswith(cand + ".") for r in resolved):
            resolved.append(cand)
    return resolved


def _is_type_checking_guard(test: ast.AST) -> bool:
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    return (
        isinstance(test, ast.Attribute)
        and isinstance(test.value, ast.Name)
        and test.value.id == "typing"
        and test.attr == "TYPE_CHECKING"
    )


def _runtime_import_nodes(tree: ast.AST):
    """yield 全部 runtime import 节点（TYPE_CHECKING 块豁免；函数体内算数）。"""

    class _Walker(ast.NodeVisitor):
        def __init__(self) -> None:
            self.nodes: list[ast.AST] = []
            self._in_type_checking = 0

        def visit_Import(self, node: ast.Import) -> None:
            if not self._in_type_checking:
                self.nodes.append(node)
            self.generic_visit(node)

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
            if not self._in_type_checking:
                self.nodes.append(node)
            self.generic_visit(node)

        def visit_If(self, node: ast.If) -> None:
            if self._is_guard(node.test):
                self._in_type_checking += 1
                for child in node.body:
                    self.visit(child)
                self._in_type_checking -= 1
                for child in node.orelse:
                    self.visit(child)
            else:
                self.generic_visit(node)

        @staticmethod
        def _is_guard(test: ast.AST) -> bool:
            return _is_type_checking_guard(test)

    walker = _Walker()
    walker.visit(tree)
    return walker.nodes


def collect_violations(backend: pathlib.Path = _BACKEND) -> list[str]:
    """扫全 backend，返回可读的禁表违规列表（含豁免剪枝）。"""
    known = _known_modules(backend)
    violations: list[str] = []
    for pyfile in _runtime_files(backend):
        src = _module_name(pyfile, backend)
        src_domain = _domain_for(src)
        if src_domain is None:
            continue  # 未登记由 test_every_backend_module_is_registered 单独执法
        pkg_parts = list(pyfile.relative_to(backend).parts[:-1])
        try:
            tree = ast.parse(pyfile.read_text(encoding="utf-8"), filename=str(pyfile))
        except SyntaxError:
            continue
        for node in _runtime_import_nodes(tree):
            for target in _resolve_import(node, pkg_parts, known):
                target_domain = _domain_for(target)
                if target_domain is None:
                    continue
                if target_domain in _FORBIDDEN.get(src_domain, ()) and (
                    src,
                    target,
                ) not in _EXEMPT:
                    violations.append(
                        f"{pyfile.relative_to(backend)}: {src}({src_domain}) "
                        f"-> {target}({target_domain})"
                    )
    return violations


class TestDomainRegistry:
    """归属登记执法：仓内每个 runtime 模块都必须显式归域。"""

    def test_every_backend_module_is_registered(self):
        unregistered = sorted(
            m for m in _known_modules(_BACKEND) if _domain_for(m) is None
        )
        assert unregistered == [], (
            "以下模块未在 _DOMAINS 登记归属域（新文件必须显式归域，"
            f"防无意识落错域）：{unregistered}"
        )

    def test_forbidden_table_covers_all_domains(self):
        assert set(_FORBIDDEN) == {"shared", "registry", "observability", "gateway"}


class TestControlPlaneLayering:
    """禁表执法：shared/registry/observability 不得向上依赖。"""

    @pytest.mark.parametrize(
        "domain",
        ["shared", "registry", "observability", "gateway"],
    )
    def test_domain_respects_forbidden_edges(self, domain: str):
        violations = [v for v in collect_violations() if f"({domain}) ->" in v]
        assert violations == [], (
            f"{domain} 域存在禁表违规（C1 逻辑边界，plaita#323）：\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    def test_no_violations_overall(self):
        violations = collect_violations()
        assert violations == [], (
            "console 后端三域禁表违规：\n" + "\n".join(f"  - {v}" for v in violations)
        )

    def test_exempt_edges_still_referenced(self):
        """豁免必须随真实导入共存：源模块不再 import 目标时豁免失效。"""
        stale: list[str] = []
        known = _known_modules(_BACKEND)
        for src, target in sorted(_EXEMPT):
            src_file = _BACKEND / src.replace(".", "/")
            if src_file.is_dir():
                src_file = src_file / "__init__.py"
            src_file = src_file.with_suffix(".py")
            assert src_file.exists(), f"豁免源模块文件不存在: {src_file}"
            pkg_parts = list(src_file.relative_to(_BACKEND).parts[:-1])
            tree = ast.parse(src_file.read_text(encoding="utf-8"))
            targets = []
            for node in _runtime_import_nodes(tree):
                targets.extend(_resolve_import(node, pkg_parts, known))
            if target not in targets:
                stale.append(f"{src} 已不再 import {target}——请同步删除豁免")
        assert stale == [], "；".join(stale)


class TestImportResolution:
    """双布局归约单测：包/平铺两种写法必须落到同一逻辑名。"""

    def test_flat_from_package_prefers_submodule_over_bare_package(self):
        known = {"services", "services.audit"}
        node = ast.parse("from services import audit").body[0]
        assert _resolve_import(node, [], known) == ["services.audit"]

    def test_flat_from_module_reduces_to_module(self):
        known = {"services", "services.tenant_write_guard"}
        node = ast.parse(
            "from services.tenant_write_guard import assert_tenant_generation"
        ).body[0]
        assert _resolve_import(node, [], known) == ["services.tenant_write_guard"]

    def test_relative_from_package_imports_submodule(self):
        known = {"services", "services.local_executor"}
        node = ast.parse("from . import local_executor").body[0]
        assert _resolve_import(node, ["services"], known) == ["services.local_executor"]

    def test_relative_two_levels_escapes_to_repo_root(self):
        known = {"secure_fields"}
        node = ast.parse("from .. import secure_fields").body[0]
        assert _resolve_import(node, ["services"], known) == ["secure_fields"]

    def test_relative_overshoot_yields_nothing(self):
        known = {"services"}
        node = ast.parse("from ...outside import thing").body[0]
        assert _resolve_import(node, ["services"], known) == []

    def test_external_packages_ignored(self):
        node = ast.parse("from plaita.server.factory import _mask_url").body[0]
        assert _resolve_import(node, [], {"services"}) == []


class TestGateCatchesViolations:
    """端到端负控：合成违规必须被门报出（修前恒绿 = 假绿）。"""

    def _scaffold(self, tmp_path: pathlib.Path) -> pathlib.Path:
        (tmp_path / "services").mkdir()
        (tmp_path / "services" / "__init__.py").write_text("", encoding="utf-8")
        (tmp_path / "services" / "local_executor.py").write_text("x = 1\n", encoding="utf-8")
        (tmp_path / "secure_fields.py").write_text("x = 1\n", encoding="utf-8")
        return tmp_path

    def test_registry_flat_import_of_gateway_is_flagged(self, tmp_path, monkeypatch):
        self._scaffold(tmp_path)
        (tmp_path / "svcviolator.py").write_text(
            "from services.local_executor import start_local_execution\n", encoding="utf-8"
        )
        monkeypatch.setitem(_DOMAINS, "services", "gateway")
        monkeypatch.setitem(_DOMAINS, "services.local_executor", "gateway")
        monkeypatch.setitem(_DOMAINS, "secure_fields", "shared")
        monkeypatch.setitem(_DOMAINS, "svcviolator", "registry")
        violations = collect_violations(tmp_path)
        assert any(
            "svcviolator.py" in v and "services.local_executor" in v for v in violations
        ), f"registry 平铺导入 gateway 必须被报出: {violations}"

    def test_registry_relative_import_of_gateway_is_flagged(self, tmp_path, monkeypatch):
        self._scaffold(tmp_path)
        (tmp_path / "services" / "relviolator.py").write_text(
            "from . import local_executor\n", encoding="utf-8"
        )
        monkeypatch.setitem(_DOMAINS, "services", "gateway")
        monkeypatch.setitem(_DOMAINS, "services.local_executor", "gateway")
        monkeypatch.setitem(_DOMAINS, "secure_fields", "shared")
        monkeypatch.setitem(_DOMAINS, "services.relviolator", "registry")
        violations = collect_violations(tmp_path)
        assert any(
            "relviolator.py" in v and "services.local_executor" in v for v in violations
        ), f"registry 相对导入 gateway 必须被报出: {violations}"

    def test_shared_import_of_registry_is_flagged(self, tmp_path, monkeypatch):
        self._scaffold(tmp_path)
        (tmp_path / "sharedviolator.py").write_text(
            "from secure_fields import mask\n", encoding="utf-8"
        )
        monkeypatch.setitem(_DOMAINS, "services", "gateway")
        monkeypatch.setitem(_DOMAINS, "services.local_executor", "gateway")
        monkeypatch.setitem(_DOMAINS, "secure_fields", "registry")
        monkeypatch.setitem(_DOMAINS, "sharedviolator", "shared")
        violations = collect_violations(tmp_path)
        assert any(
            "sharedviolator.py" in v and "secure_fields" in v for v in violations
        ), f"shared 导入 registry 必须被报出: {violations}"

    def test_exempt_pair_is_not_flagged(self, tmp_path, monkeypatch):
        self._scaffold(tmp_path)
        (tmp_path / "auth.py").write_text("x = 1\n", encoding="utf-8")
        (tmp_path / "api").mkdir()
        (tmp_path / "api" / "__init__.py").write_text("", encoding="utf-8")
        (tmp_path / "api" / "audit.py").write_text(
            "from ..auth import require_auth\n", encoding="utf-8"
        )
        monkeypatch.setitem(_DOMAINS, "services", "gateway")
        monkeypatch.setitem(_DOMAINS, "services.local_executor", "gateway")
        monkeypatch.setitem(_DOMAINS, "secure_fields", "shared")
        monkeypatch.setitem(_DOMAINS, "auth", "gateway")
        monkeypatch.setitem(_DOMAINS, "api", "gateway")
        monkeypatch.setitem(_DOMAINS, "api.audit", "observability")
        violations = collect_violations(tmp_path)
        assert not any("api/audit.py" in v for v in violations), (
            f"具名豁免对 (api.audit, auth) 不得报违规: {violations}"
        )

    def test_lazy_import_inside_function_is_flagged(self, tmp_path, monkeypatch):
        self._scaffold(tmp_path)
        (tmp_path / "lazyviolator.py").write_text(
            "def seed():\n    from services.local_executor import start\n",
            encoding="utf-8",
        )
        monkeypatch.setitem(_DOMAINS, "services", "gateway")
        monkeypatch.setitem(_DOMAINS, "services.local_executor", "gateway")
        monkeypatch.setitem(_DOMAINS, "secure_fields", "shared")
        monkeypatch.setitem(_DOMAINS, "lazyviolator", "registry")
        violations = collect_violations(tmp_path)
        assert any("lazyviolator.py" in v for v in violations), (
            f"函数体惰性导入是真实依赖边，必须被报出: {violations}"
        )

    def test_type_checking_import_is_exempt(self, tmp_path, monkeypatch):
        self._scaffold(tmp_path)
        (tmp_path / "tcviolator.py").write_text(
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n"
            "    from services.local_executor import start\n",
            encoding="utf-8",
        )
        monkeypatch.setitem(_DOMAINS, "services", "gateway")
        monkeypatch.setitem(_DOMAINS, "services.local_executor", "gateway")
        monkeypatch.setitem(_DOMAINS, "secure_fields", "shared")
        monkeypatch.setitem(_DOMAINS, "tcviolator", "registry")
        assert collect_violations(tmp_path) == []
