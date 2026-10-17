"""plaita#95 回归：配置参考页的 extras 表必须与 `pyproject.toml` 的真实 extras 对称。

`docs-site/docs/reference/configuration.md` 的 extras 表是对
`pyproject.toml` `[project.optional-dependencies]` 的**手工复述**，2026-10 审计发现三处失真：

1. `server` 行多列已裁撤的 `python-multipart` / `python-jose[cryptography]` /
   `passlib[bcrypt]`，却漏列 `redis>=4.0.0`——`plaita/server` 下多个模块**模块级
   import redis**，照该页装 `plaita[server]` 会在导入期 ModuleNotFoundError；
   而 `python-jose` 会传递引入无修复版本的 `ecdsa`（CVSS 7.4）。
2. `lint` 行还列着 `mypy` / `flake8` / `black`——2026-10 已收敛为 ruff 单工具，
   照着装会让本地口径与 CI 门禁（只跑 ruff）不一致。
3. `all` 行漏列 `credentials` / `langfuse` / `fast`。

本文件把「表 ↔ `pyproject.toml`」的对称性钉成**纯静态、不联网、不装包**的断言：
改 extras 而不同步本页，这里就红（这正是 issue 说的「漂移不可被发现」）。

对称口径：先按分发包名比对集合（`SQLAlchemy>=1.4` 与 `SQLAlchemy>=1.4.0` 同名），
再要求文档里的版本下限是 `pyproject.toml` 下限的**前缀**（`>=4.0` vs `>=4.0.0` 合法，
`>=0.6` vs `>=0.7` 不合法 ⇒ 升版必须同步）。
"""

import re
import tomllib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PYPROJECT = REPO / "pyproject.toml"
CONFIG_DOC = REPO / "docs-site/docs/reference/configuration.md"
DOC_CODE_MAP = REPO / "docs/DOC_CODE_MAP.md"

CONFIG_DOC_REL = "docs-site/docs/reference/configuration.md"
EXTRAS_HEADING = "## 可选依赖（extras）"

# issue#95 建议的三项裁撤（不再属于 plaita 源码依赖面，只在 plaita-console 里）
REVOKED_SERVER_DEPS = ("python-jose", "passlib", "python-multipart")
REVOKED_LINT_TOOLS = ("mypy", "flake8", "black")

# 页面尚未列行的 extras：**本单边界外**（#95 只覆盖三行失真 + DOC_CODE_MAP 缺行）。
# 这不是"允许漂移"的白名单——一旦为某个 extra 补了表行，就必须把它从本集合移除；
# 新增 extra 若不在本集合又无表行，下面的 coverage 测试会红。
UNDOCUMENTED_EXTRAS = {"fast", "uvloop", "langfuse"}


# ── 解析 ────────────────────────────────────────────────────────────────────
def _load_extras() -> dict[str, list[str]]:
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)["project"]["optional-dependencies"]


def _doc_rows() -> dict[str, str]:
    """extras 小节里 ``{extra: 依赖单元格原文}``（表头/分隔行/正文自动跳过）。"""
    text = CONFIG_DOC.read_text(encoding="utf-8")
    section = text[text.index(EXTRAS_HEADING) :]
    rows: dict[str, str] = {}
    for line in section.splitlines()[1:]:
        if line.startswith("## "):
            break
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3 or cells[0] == "extra":
            continue
        m = re.fullmatch(r"`([A-Za-z0-9_-]+)`", cells[0])
        if m:
            rows[m.group(1)] = cells[1]
    return rows


_NAME_RE = re.compile(r"^([A-Za-z0-9._-]+)")
_FLOOR_RE = re.compile(r">=\s*([0-9]+(?:\.[0-9]+)*)")


def _req_name(spec: str) -> str:
    """分发包名：切掉版本约束 / extras / 环境标记，按 PEP 503 归一。"""
    m = _NAME_RE.match(spec.strip())
    assert m, f"无法解析依赖声明：{spec!r}"
    return m.group(1).lower().replace("_", "-").replace(".", "-")


def _floor(spec: str) -> tuple[int, ...] | None:
    m = _FLOOR_RE.search(spec)
    return tuple(int(x) for x in m.group(1).split(".")) if m else None


def _dep_map(cell: str) -> dict[str, tuple[int, ...] | None]:
    """文档单元格 → ``{包名: 版本下限}``（单元格用「、」分隔）。"""
    out: dict[str, tuple[int, ...] | None] = {}
    for part in cell.split("、"):
        spec = part.strip().strip("`").strip()
        if spec:
            out[_req_name(spec)] = _floor(spec)
    return out


def _pyproject_map(specs: list[str]) -> dict[str, tuple[int, ...] | None]:
    return {_req_name(s): _floor(s) for s in specs}


@pytest.fixture(scope="module")
def extras() -> dict[str, list[str]]:
    return _load_extras()


@pytest.fixture(scope="module")
def rows() -> dict[str, str]:
    return _doc_rows()


# ── 表 ↔ pyproject 对称 ─────────────────────────────────────────────────────
class TestExtrasTable:
    def test_page_and_pyproject_both_parsed(self, extras, rows):
        """解析器自检：两边都真读到了东西，否则下面的断言会静默空转。"""
        assert "server" in extras and "server" in rows
        assert len(rows) >= 8, f"只解析到 {len(rows)} 行，解析器可能失效：{sorted(rows)}"

    def test_no_phantom_extras(self, extras, rows):
        """页面不得出现 `pyproject.toml` 里没有的 extra（含已裁撤的整行）。"""
        phantom = sorted(set(rows) - set(extras))
        assert not phantom, (
            f"这些 extra 在 {CONFIG_DOC_REL} 里有行、却不在 pyproject.toml 中：{phantom}——"
            "行已失效，照它安装会报错或装上不存在的依赖面"
        )

    def test_documented_extras_cover_pyproject(self, extras, rows):
        """新增 extra 必须在本页登记（已知缺口在 UNDOCUMENTED_EXTRAS 显式登记理由）。"""
        missing = sorted(set(extras) - set(rows) - UNDOCUMENTED_EXTRAS)
        assert not missing, (
            f"pyproject.toml 新增了 extras {missing}，但 {CONFIG_DOC_REL} 没有对应表行——"
            "这正是 #95 说的「改 extras 不会触发改文档」的漂移"
        )

    def test_documented_rows_match_pyproject(self, extras, rows):
        """每个有表行的 extra，其依赖名集合必须与 pyproject.toml 完全一致。

        唯一例外：`dev` 行的单元格自带「等」字，页面**显式声明**自己非穷尽——
        此时只要求文档是真实集合的子集，且不得出现 pyproject 里没有的包。
        """
        problems: list[str] = []
        for extra, cell in rows.items():
            if extra == "all":
                continue  # all 只列 extra 名，不是依赖清单，另测
            doc = _dep_map(cell)
            truth = _pyproject_map(extras[extra])
            extra_missing = sorted(set(doc) - set(truth))
            if extra_missing:
                problems.append(
                    f"`{extra}` 行列了 pyproject.toml 里没有的依赖：{extra_missing}"
                )
            if "等" in cell:
                continue
            omitted = sorted(set(truth) - set(doc))
            if omitted:
                problems.append(f"`{extra}` 行漏列：{omitted}")
        assert not problems, "\n".join(problems) + (
            f"\n→ 逐行对齐 {CONFIG_DOC_REL} 的 extras 表与 pyproject.toml"
            " `[project.optional-dependencies]`"
        )

    def test_version_floors_are_prefix_compatible(self, extras, rows):
        """文档写的下限必须是 pyproject 下限的前缀（`>=4.0` ⊂ `>=4.0.0` 合法）。"""
        problems: list[str] = []
        for extra, cell in rows.items():
            if extra == "all" or "等" in cell:
                continue
            doc = _dep_map(cell)
            truth = _pyproject_map(extras[extra])
            for name, floor in doc.items():
                if name not in truth or floor is None or truth[name] is None:
                    continue
                if truth[name][: len(floor)] != floor:
                    problems.append(
                        f"`{extra}` 行 {name} 写 {floor}，pyproject 是 {truth[name]}"
                        "（下限升版必须同步本页）"
                    )
        assert not problems, "\n".join(problems)


class TestRevokedDependenciesStayOut:
    """三项裁撤（#95 证据）——按该页安装不得再把它们写回安装指引。"""

    @pytest.mark.parametrize("pkg", REVOKED_SERVER_DEPS)
    def test_server_row_has_no_revoked_package(self, rows, pkg):
        assert pkg not in _dep_map(rows["server"]), (
            f"`server` 行又把 {pkg} 写回来了：plaita 源码从不 import 它"
            "（python-jose 还传递引入无修复版本的 ecdsa，CVSS 7.4）"
        )

    @pytest.mark.parametrize("pkg", REVOKED_SERVER_DEPS)
    def test_revoked_packages_absent_from_whole_page(self, pkg):
        text = CONFIG_DOC.read_text(encoding="utf-8")
        assert pkg not in text, f"{CONFIG_DOC_REL} 仍出现已裁撤的 {pkg}"

    def test_server_row_lists_redis(self, rows):
        """`redis` 是 server extra 自足的前提（plaita/server 下模块级 import redis）。"""
        deps = _dep_map(rows["server"])
        assert "redis" in deps, (
            "`server` 行漏列 redis——只按本页装 server 会在导入 plaita.server 时 "
            "ModuleNotFoundError"
        )
        assert "redis" in _pyproject_map(_load_extras()["server"])

    def test_lint_row_is_ruff_only(self, rows):
        assert set(_dep_map(rows["lint"])) == {"ruff"}, (
            f"`lint` 行应为 ruff 单工具（2026-10 决策），实际 {sorted(_dep_map(rows['lint']))}"
        )

    @pytest.mark.parametrize("tool", REVOKED_LINT_TOOLS)
    def test_lint_row_has_no_removed_tool(self, rows, tool):
        assert tool not in _dep_map(rows["lint"]), (
            f"`lint` 行仍列 {tool}：该工具已删除且 CI 门禁只跑 ruff，"
            "照着装会让本地口径与门禁不一致"
        )


class TestAllRow:
    def test_all_row_expands_to_declared_extras(self, extras, rows):
        """`all` 行必须逐个点名 pyproject 里 `all=plaita[...]` 声明的每个 extra。"""
        declared_specs = extras["all"]
        assert len(declared_specs) == 1, f"all 行结构变了：{declared_specs}"
        inner = re.search(r"plaita\[([^\]]+)\]", declared_specs[0])
        assert inner, f"all 不是 plaita[...] 形式：{declared_specs[0]!r}"
        declared = {x.strip() for x in inner.group(1).split(",") if x.strip()}
        assert declared <= set(extras), f"all 引用了不存在的 extra：{sorted(declared - set(extras))}"

        cell = rows["all"]
        mentioned = {
            e for e in extras if re.search(rf"(?<![\w-]){re.escape(e)}(?![\w-])", cell)
        }
        assert mentioned == declared, (
            f"`all` 行列的 extra 集合与 pyproject 不一致：\n"
            f"  文档：{sorted(mentioned)}\n  pyproject：{sorted(declared)}"
        )


@pytest.fixture(scope="module")
def doc_code_map() -> str:
    return DOC_CODE_MAP.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def config_map_row(doc_code_map: str) -> str:
    for line in doc_code_map.splitlines():
        if line.startswith("|") and CONFIG_DOC_REL in line:
            return line
    return ""


# ── DOC_CODE_MAP 登记（漂移可被发现的前提）──────────────────────────────────
class TestConfigPageIsMapped:
    def test_map_lists_config_page(self, config_map_row):
        assert config_map_row, (
            f"DOC_CODE_MAP.md 缺少 `{CONFIG_DOC_REL}` 的映射行——"
            "改 pyproject.toml extras 时没人会被提醒回看这一页（#95）"
        )

    def test_row_binds_optional_dependencies(self, config_map_row):
        assert "pyproject.toml" in config_map_row, "该行必须点名 pyproject.toml"
        assert "optional-dependencies" in config_map_row, (
            "代码路径模式必须点到 `[project.optional-dependencies]`，否则匹配不到 extras 变更"
        )

    def test_trigger_names_extras_churn(self, config_map_row):
        assert "extras" in config_map_row and "增删改" in config_map_row, (
            "同步触发条件必须写明「extras 增删改」，笼统写「配置变更」会漏掉新增/裁撤"
        )

    def test_trigger_names_the_regression_test(self, config_map_row):
        assert "test_docs_extras_sync" in config_map_row, (
            "该行应点名本回归测试，让「表↔pyproject 对称」有机检入口"
        )
