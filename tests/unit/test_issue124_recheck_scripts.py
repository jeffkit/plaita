"""plaita#124 回归：变异复核脚本的三处缺陷。

三处缺陷都会让复核结论**不可信**，其中第三处最危险——它把「没能真正执行」
记成 ``killed``，于是复核安全网被静默关闭、分数被伪造成 100%，且没有任何报错。

本文件只做**静态 + 冒烟**断言（不依赖 mutmut / mutants/ 目录），
因为这些缺陷的根因在脚本文本与公共库的判定逻辑里：

1. ``TESTS`` 里的路径必须真实存在（原先 ``tests/test_calculate.py`` /
   ``tests/test_decide.py`` 不存在 ⇒ pytest collection error 被记成 killed）；
2. ``$VAR`` 不得紧贴全角标点（macOS bash 3.2 会把多字节字符吞进变量名 ⇒
   ``unbound variable``）；
3. 不得再出现裸 ``timeout``；退出码 ``124/126/127`` 必须归入 ``error`` 而非 ``killed``。
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"
COMMON = SCRIPTS / "lib" / "mutation_recheck_common.sh"

# 所有做"独立进程复核"的脚本
RECHECK_SCRIPTS = [
    SCRIPTS / "recheck_mutants.sh",
    SCRIPTS / "recheck_async_utils.sh",
    SCRIPTS / "recheck_codeflow_expr.sh",
    SCRIPTS / "recheck_codeflow_nodes.sh",
    SCRIPTS / "recheck_codeflow_stmt.sh",
    SCRIPTS / "recheck_sexpr.sh",
    SCRIPTS / "run_full_mutation_sweep.sh",
]

BASH = shutil.which("bash") or "/bin/bash"
CJK_PUNCT = "（），。：；、「」【】？！"


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


class TestTestPathsExist:
    """缺陷 1：复核用的测试路径必须真实存在。"""

    def test_recheck_mutants_tests_all_exist(self):
        src = _read(SCRIPTS / "recheck_mutants.sh")
        m = re.search(r'^TESTS="([^"]*)"', src, re.M)
        assert m, "recheck_mutants.sh 里没有 TESTS 常量"
        missing = [t for t in m.group(1).split() if not (REPO / t).exists()]
        assert not missing, f"TESTS 里存在不存在的路径（会以 collection error 被误判 killed）：{missing}"

    def test_no_stale_tests_root_paths(self):
        """原先写成 tests/xxx.py（少了 unit/）——全仓复核脚本都不该再出现。"""
        bad = re.compile(r"\btests/(?!unit/|integration/|e2e/)[a-z_]+\.py")
        for p in RECHECK_SCRIPTS:
            hits = bad.findall(_read(p))
            assert not hits, f"{p.name} 里仍有疑似错误路径：{hits}"


class TestBash32Multibyte:
    """缺陷 2：$VAR 紧跟全角标点在 bash 3.2 上会被吞进变量名。"""

    def test_no_var_immediately_followed_by_cjk_punct(self):
        pat = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*[" + re.escape(CJK_PUNCT) + r"]")
        offenders = []
        for p in list(SCRIPTS.rglob("*.sh")):
            for i, line in enumerate(_read(p).splitlines(), 1):
                # 跳过注释行：注释不影响 bash 展开
                if line.lstrip().startswith("#"):
                    continue
                if pat.search(line):
                    offenders.append(f"{p.name}:{i}")
        assert not offenders, (
            "以下位置 $VAR 紧跟全角标点，macOS bash 3.2 下会 unbound variable："
            f"{offenders}（请写成 ${{VAR}}）"
        )

    def test_no_match_branch_prints_message_on_bash32(self):
        """冒烟：无匹配变异点时必须打印消息并 exit 1，而不是 unbound variable。"""
        r = subprocess.run(
            [BASH, str(SCRIPTS / "recheck_mutants.sh"), "__no_such_filter__"],
            capture_output=True, text=True, cwd=REPO, timeout=120,
        )
        combined = r.stdout + r.stderr
        assert "unbound variable" not in combined, f"bash 3.2 变量吞噬复现：{combined[:300]}"
        assert "没有匹配的变异点" in combined, f"未打印预期消息：{combined[:300]}"
        assert r.returncode == 1, f"应 exit 1，实际 {r.returncode}"


class TestNoBareTimeout:
    """缺陷 3：裸 timeout 在 macOS 不存在（rc=127），会被判成 killed（伪造满分）。"""

    def test_no_bare_timeout_invocation(self):
        # 匹配 `timeout <n> ...` 这种直接调用（排除变量引用与注释）
        pat = re.compile(r"(?<![\w$\"'])\btimeout\s+\d+")
        offenders = []
        for p in RECHECK_SCRIPTS:
            for i, line in enumerate(_read(p).splitlines(), 1):
                if line.lstrip().startswith("#"):
                    continue
                if pat.search(line):
                    offenders.append(f"{p.name}:{i}")
        assert not offenders, f"仍有裸 timeout 调用（macOS 上 rc=127 ⇒ 伪造 killed）：{offenders}"

    def test_common_lib_exists_and_resolves_timeout(self):
        assert COMMON.exists(), "缺少 scripts/lib/mutation_recheck_common.sh"
        src = _read(COMMON)
        assert "command -v timeout" in src and "command -v gtimeout" in src
        assert "TimeoutExpired" in src, "回退路径应使用纯 Python subprocess 超时"

    def test_classify_never_maps_exec_failure_to_killed(self):
        """124/126/127 必须是 error；0 是 survived；其余才是 killed。"""
        script = f"""
set -u
source "{COMMON}"
mut_init_timeout >/dev/null 2>&1
for rc in 0 1 2 124 126 127; do
  printf '%s=%s\\n' "$rc" "$(mut_classify $rc)"
done
"""
        r = subprocess.run([BASH, "-c", script], capture_output=True, text=True,
                           cwd=REPO, timeout=60)
        assert r.returncode == 0, r.stderr
        got = dict(line.split("=") for line in r.stdout.strip().splitlines())
        assert got["0"] == "survived"
        assert got["1"] == "killed" and got["2"] == "killed"
        for rc in ("124", "126", "127"):
            assert got[rc] == "error", f"rc={rc} 必须归入 error（否则伪造 killed），实际 {got[rc]}"

    def test_mut_run_actually_runs_pytest(self):
        """回退路径必须真的能跑起来（rc 不应是 127）。"""
        script = f"""
set -u
source "{COMMON}"
mut_init_timeout >/dev/null 2>&1
rc=0
mut_run "plaita.__fake__mutmut_1" 120 tests/unit/test_decide.py >/dev/null 2>&1 || rc=$?
echo "rc=$rc"
"""
        r = subprocess.run([BASH, "-c", script], capture_output=True, text=True,
                           cwd=REPO, timeout=300)
        assert r.returncode == 0, r.stderr
        rc = int(r.stdout.strip().split("=")[1])
        assert rc != 127, "回退路径仍以 127 退出（等于伪造 killed 的老问题）"
        assert rc == 0, f"合法测试集合应通过（survived 判定可达），实际 rc={rc}"


class TestErrorAccounting:
    """有 error 时必须非零退出，且报告里单列计数——不能静默当满分。"""

    @pytest.mark.parametrize("name", [
        "recheck_mutants.sh", "recheck_async_utils.sh", "recheck_codeflow_expr.sh",
        "recheck_codeflow_nodes.sh", "recheck_codeflow_stmt.sh", "recheck_sexpr.sh",
    ])
    def test_reports_error_count_and_exits_nonzero(self, name):
        src = _read(SCRIPTS / name)
        assert "error=" in src, f"{name} 摘要未单列 error 计数"
        assert "errors=0" in src, f"{name} 未初始化 errors 计数器"
        assert re.search(r'if \[\[ \$errors -gt 0 \]\]; then.*?exit 1', src, re.S), \
            f"{name} 有 error 时未非零退出"
