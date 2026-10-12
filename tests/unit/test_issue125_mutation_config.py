"""plaita#125 回归：变异门禁的**配置自洽性**与文档数值一致性。

两个缺陷都会让人**照着文档操作却得到错误结论**：

1. `pytest_add_cli_args_test_selection` 与 `only_mutate` 不匹配——前者**优先于**
   `pytest_add_cli_args`，mutmut 用它逐变异点跑测试。按 §7 收窄 `only_mutate` 到单文件
   时若不同步收窄本列表，跑 `decide.py` 实际执行的是 **event/codeflow 的测试**，
   与该模块无关 ⇒ 分数被配置压得**假低**。实测：
     保持 event/codeflow → 133 变异 / 98 killed / 35 survived = **73.7%**
     换成 test_decide.py → 147 变异 / 138 killed / 9 survived  = **93.9%**
   差额 **19.5 个百分点**，极易被误读成"该模块测试很弱"。
2. `docs/mutation-testing.md` §2.3 曾把 `decide.py` 记为 **107/107 = 100%**，
   而现口径实测 **93.9%（147 变异 / 9 true survived）**。

本文件钉住：配置可机器检测的自洽性、文档与实测口径不再互相矛盾、
以及 `--snippet` 助手能给出正确的测试集合。
"""

import subprocess
import tomllib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PYPROJECT = REPO / "pyproject.toml"
DOCS = REPO / "docs/mutation-testing.md"
HELPER = REPO / "scripts/mutation-module-tests.sh"

# 已知且经人工确认的例外（宽松匹配无法自动定位，且确实分散在多个文件里）
KNOWN_UNMAPPED = {
    "plaita/node/__init__.py",        # 注册表，相关测试分散在 test_node_registry*.py 等
    "plaita/dsl/codeflow/_common.py",  # 无同名测试，覆盖散在 test_codeflow_*.py
}


def _mutmut_cfg():
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)["tool"]["mutmut"]


class TestSelectionConfigIsCoherent:
    """缺陷 1：配置本身必须自洽、可机器检测。"""

    def test_both_keys_exist(self):
        cfg = _mutmut_cfg()
        assert "only_mutate" in cfg
        assert "pytest_add_cli_args_test_selection" in cfg

    def test_selection_entries_all_exist_on_disk(self):
        """列表里每个路径都必须真实存在（#124 的同类问题：失效路径 ⇒ 假 killed）。"""
        missing = [
            t for t in _mutmut_cfg()["pytest_add_cli_args_test_selection"]
            if not (REPO / t).exists()
        ]
        assert not missing, f"test_selection 里有不存在的路径：{missing}"

    def test_selection_is_not_wildly_out_of_scope(self):
        """选择列表不得只覆盖某两个模块族——那正是"跑别处测试"的成因。

        `only_mutate` 覆盖 30 个模块，而 `test_selection` 若只列 event/codeflow，
        等于对绝大多数模块都在跑无关测试。这里给一个**下界**断言：
        列表里至少要有与 `only_mutate` 模块数「量级相当」的覆盖意图，
        即：**每个模块族至少能映射到一个存在的测试文件**（见下一个用例）。
        """
        sel = _mutmut_cfg()["pytest_add_cli_args_test_selection"]
        assert len(sel) >= 3, f"test_selection 过短，几乎必然与 only_mutate 不匹配：{sel}"

    def test_no_selection_file_maps_to_nothing(self):
        """列表里不得有"看起来属于某模块但该模块并不在 only_mutate"的条目。

        反向也有意义：若某测试属于 codeflow，则 codeflow 必须在 only_mutate 里。
        """
        sel = _mutmut_cfg()["pytest_add_cli_args_test_selection"]
        only = " ".join(_mutmut_cfg()["only_mutate"])
        for t in sel:
            stem = Path(t).stem.replace("test_", "", 1)
            # 取最长可识别的模块族前缀
            family = stem.split("_")[0]
            if family in ("event", "codeflow"):
                assert family in only, (
                    f"test_selection 含 {t}，但 only_mutate 里没有 {family} 相关模块"
                )


class TestHelperScriptMapsModules:
    """配置助手：按模块给出正确的测试集合（§7 收窄时用它，避免重演 19.5 点偏差）。"""

    def test_helper_exists_and_is_runnable(self):
        assert HELPER.exists(), "缺少 scripts/mutation-module-tests.sh"
        r = subprocess.run(["bash", str(HELPER), "decide"],
                           capture_output=True, text=True, cwd=REPO, timeout=120)
        assert r.returncode == 0, r.stderr
        assert "test_decide.py" in r.stdout

    def test_snippet_mode_emits_valid_toml(self):
        r = subprocess.run(["bash", str(HELPER), "--snippet", "decide"],
                           capture_output=True, text=True, cwd=REPO, timeout=120)
        assert r.returncode == 0, r.stderr
        assert "pytest_add_cli_args_test_selection = [" in r.stdout
        assert "test_decide.py" in r.stdout

    def test_helper_covers_almost_all_modules(self):
        """未映射的模块必须收敛到**已知**的几个（否则会静默给出空测试集合）。"""
        r = subprocess.run(["bash", str(HELPER)],
                           capture_output=True, text=True, cwd=REPO, timeout=120)
        assert r.returncode == 0, r.stderr
        unmapped = set()
        for line in r.stdout.splitlines():
            if "⚠" in line:
                unmapped.add(line.strip().split()[0])
        unknown = unmapped - KNOWN_UNMAPPED
        assert not unknown, (
            f"出现新的未映射模块（会让 §7 收窄时跑空集合）：{sorted(unknown)}"
        )


class TestDocsMatchMeasuredDecideBaseline:
    """缺陷 2：文档数值不得与实测口径矛盾。"""

    def test_docs_no_longer_claim_decide_100_percent_as_current(self):
        """§2.3 曾记 107/107 = 100%。现口径是 93.9%——不能留作"当前值"而不加说明。"""
        text = DOCS.read_text(encoding="utf-8")
        # 允许历史记录存在，但必须同时出现现口径或"待复核/已过时"标记
        if "| `plaita/node/decide.py` | 92% | 107 | 107 | 0 | **100%** |" in text:
            pytest.fail(
                "docs §2.3 仍把 decide.py 记为 107/107=100% 的当前值；"
                "实测为 93.9%（147 变异 / 9 true survived），见 #125"
            )

    def test_docs_record_decide_measured_value(self):
        text = DOCS.read_text(encoding="utf-8")
        assert "93.9%" in text, "docs 应记录 decide.py 的实测口径 93.9%"

    def test_docs_warn_about_selection_scope(self):
        """必须写明"只改 only_mutate 不改 test_selection 会假低分"这个坑。"""
        text = DOCS.read_text(encoding="utf-8")
        assert "test_selection" in text, "docs 应说明 test_selection 的影响"
        assert ("19.5" in text) or ("假低" in text), (
            "docs 应量化或明确标注该配置导致的分数偏差"
        )
