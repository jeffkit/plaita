"""plaita#167：runner 辅助脚本必须用**可用的**解释器——不再裸信 PATH 上的 ``python3``。

现象（三台机器一致，含落地机）：``tests/unit`` 全量里稳定 4 条红，与变异无关——

    test_issue124_recheck_scripts.py::TestNoBareTimeout::test_mut_run_actually_runs_pytest
    test_issue125_mutation_config.py::TestHelperScriptMapsModules::test_helper_exists_and_is_runnable
    test_issue125_mutation_config.py::TestHelperScriptMapsModules::test_snippet_mode_emits_valid_toml
    test_issue125_mutation_config.py::TestHelperScriptMapsModules::test_helper_covers_almost_all_modules

根因：两条辅助脚本 shell 出到**裸 ``python3``**。macOS 系统 ``python3`` 是 3.9.6
——没有 ``tomllib``（3.11+ 才有）也没有 pytest。分离实验（r7 实现者 + #109 验收
各自独立做出）：

    base（无 PYTHON_BIN，PATH python3=3.9.6）          4 failed
    仅 PYTHON_BIN 指向 3.11                            3 failed（124 那条转绿）
    仅 PATH 前置 3.11 的 python3                       24 passed

后两行正是本文件的判据来源：``mutation-module-tests.sh`` **只认 PATH 上的
``python3``**，连 ``PYTHON_BIN`` 都不看。

本文件钉住修法（issue 的三条「必须做」）：

1. 解析顺序**两条脚本共用同一处**（``PYTHON_BIN`` → ``$VIRTUAL_ENV/bin/python``
   → 仓根 ``.venv/bin/python`` → PATH 上第一个 ≥3.11）——用**假 PATH + 会写
   标记文件的桩解释器**实测「用的是哪一个」，不是读源码断言；
2. 解释器 < 3.11 时 ``mutation-module-tests.sh`` 输出**具名 SKIP 理由**
   （说清缺什么、怎么修），不是 traceback、也不是静默的空模块表；
3. ``mut_run`` 真的用解析出来的那个解释器跑 pytest。

「已按人工豁免把 4 条红当环境问题」正是 #167 点名的**可伪造通道**——所以这里
的判据是「脚本实际调用了哪个可执行文件」，而不是「脚本里有没有某行代码」。
"""
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"
MODULE_HELPER = SCRIPTS / "mutation-module-tests.sh"
COMMON_LIB = SCRIPTS / "lib" / "mutation_recheck_common.sh"

BASH = shutil.which("bash") or "/bin/bash"


def _clean_env(**overrides):
    """基线 env：剔掉 PYTHON_BIN / VIRTUAL_ENV（含 conftest 注入的那个）。"""
    env = dict(os.environ)
    for key in ("PYTHON_BIN", "VIRTUAL_ENV"):
        env.pop(key, None)
    env.update({k: v for k, v in overrides.items() if v is not None})
    return env


def _make_stub_python(path: Path, *, version_ok: bool, marker: Path,
                      module_line: str = "plaita/node/decide.py",
                      pytest_ok: bool = True) -> Path:
    """造一个**桩解释器**：记录「谁被调用了」，并按需扮演新旧版本。

    * ``-c``（版本探测 / 表达式）⇒ ``version_ok`` 决定退出码；
      其中 ``import pytest`` 由 ``pytest_ok`` 单独决定（#167 的具名中止面）；
    * ``-``（stdin 脚本，即模块表那段）⇒ 打印一行模块名；
    * ``-m pytest …`` ⇒ 退出 0（``mut_run`` 的存活路径）。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s %s\\n" "$0" "$*" >> "{marker}"\n'
        'if [[ "${1:-}" == "-c" ]]; then\n'
        '    case "${2:-}" in\n'
        f'        *pytest*) exit {0 if pytest_ok else 1} ;;\n'
        f'        *)        exit {0 if version_ok else 1} ;;\n'
        "    esac\n"
        "fi\n"
        'if [[ "${1:-}" == "-" ]]; then\n'
        f'    printf "%s\\n" "{module_line}"\n'
        "    exit 0\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _fake_path(*dirs: Path, base: Path = Path("/usr/bin")) -> str:
    """最小 PATH：假目录 + 系统工具目录（脚本自己要用 grep/sed/ls/dirname）。

    刻意**不含** ``~/.local/bin`` / venv —— 否则真实机器上的 ``python3.13``
    会先于桩件命中，判据就不再确定。
    """
    return ":".join(str(d) for d in dirs) + f":{base}:/bin"


def _resolve_common_lib(env) -> str:
    """公共库解析出来的解释器（``mut_run`` 用的就是它）。"""
    r = subprocess.run(
        [BASH, "-c", f'source "{COMMON_LIB}"; printf "%s" "$MUT_PYTHON_BIN"'],
        capture_output=True, text=True, cwd=REPO, env=env, timeout=120,
    )
    assert r.returncode == 0, r.stderr
    return r.stdout


class TestBothScriptsShareOneResolution:
    """两条脚本必须用**同一个**解释器，且顺序是「显式 > venv > 仓根 .venv > PATH」。"""

    def test_python_bin_wins_in_both_scripts(self, tmp_path):
        """显式 ``PYTHON_BIN`` + 假 PATH（含诱饵 python3）：两条脚本都认它。"""
        fake = tmp_path / "bin"
        # 诱饵：PATH 上的 python3 与 python3.11 都不是我们要的
        _make_stub_python(fake / "python3", version_ok=False, marker=tmp_path / "marker")
        _make_stub_python(fake / "python3.11", version_ok=False, marker=tmp_path / "marker")
        chosen = _make_stub_python(
            tmp_path / "chosen" / "python", version_ok=True, marker=tmp_path / "marker"
        )
        env = _clean_env(PYTHON_BIN=str(chosen), PATH=_fake_path(fake))

        assert _resolve_common_lib(env) == str(chosen)

        r = subprocess.run([BASH, str(MODULE_HELPER), "decide"],
                           capture_output=True, text=True, cwd=REPO, env=env, timeout=120)
        assert r.returncode == 0, r.stderr
        assert "test_decide.py" in r.stdout
        marker = (tmp_path / "marker").read_text(encoding="utf-8")
        assert str(chosen) in marker, f"模块脚本没用 PYTHON_BIN：{marker}"
        assert str(fake / "python3") not in marker, f"模块脚本仍碰了裸 python3：{marker}"

    def test_activated_venv_wins_over_path_decoy(self, tmp_path):
        """``$VIRTUAL_ENV``（已激活的 venv = 正常工作流）优先于 PATH。"""
        fake = tmp_path / "bin"
        _make_stub_python(fake / "python3", version_ok=True, marker=tmp_path / "marker")
        venv_python = _make_stub_python(
            tmp_path / "venv" / "bin" / "python", version_ok=True, marker=tmp_path / "marker"
        )
        env = _clean_env(VIRTUAL_ENV=str(tmp_path / "venv"), PATH=_fake_path(fake))

        assert _resolve_common_lib(env) == str(venv_python)

        r = subprocess.run([BASH, str(MODULE_HELPER), "decide"],
                           capture_output=True, text=True, cwd=REPO, env=env, timeout=120)
        assert r.returncode == 0, r.stderr
        marker = (tmp_path / "marker").read_text(encoding="utf-8")
        assert str(venv_python) in marker, marker

    def test_path_fallback_skips_an_old_python3(self, tmp_path):
        """PATH 上 ``python3`` 太老 ⇒ 兜底跳过它；选中者版本 ≥3.11（**性质判据**）。

        兜底解析按**版本降序**挑候选 ⇒ 宿主装有更新解释器（3.12/3.13）时
        宿主胜出是**正确结果**——断言写死 ``== "python3.11"`` 会在 CI
        （/usr/bin 有 python3.13）上假红（run 756f058 实证）。改为性质：
        ① 太老的 ``python3`` 名字不得胜出；② 选中者（名字或解释器本体）
        实测版本 ≥3.11；③ 老 python3 至多被 ``-c`` 版本探测，不得干活。
        判据没有放宽：3.12/3.13 命中是正确结果，照样被②验到 ≥3.11。
        """
        fake = tmp_path / "bin"
        old = _make_stub_python(fake / "python3", version_ok=False, marker=tmp_path / "marker")
        new = _make_stub_python(fake / "python3.11", version_ok=True, marker=tmp_path / "marker")
        env = _clean_env(PATH=_fake_path(fake))

        # PATH 兜底解析出的是**名字**（按名字 exec，走 PATH）
        resolved = _resolve_common_lib(env)
        assert resolved != "python3", "兜底选中了太老的 python3——跳过判据失效"
        stub_won = resolved == "python3.11"
        if not stub_won:
            # 真实解释器（仓根 .venv / 宿主）胜出：实跑 -V 验版本 ≥3.11
            probe = resolved if str(resolved).startswith("/") else (shutil.which(resolved) or resolved)
            ver_run = subprocess.run([str(probe), "-V"],
                                     capture_output=True, text=True, timeout=60)
            ver = (ver_run.stdout or ver_run.stderr).split()[-1]
            major, minor = (int(p) for p in ver.split(".")[:2])
            assert (major, minor) >= (3, 11), f"兜底选中了 <3.11 的解释器:{resolved} = {ver}"

        r = subprocess.run([BASH, str(MODULE_HELPER), "decide"],
                           capture_output=True, text=True, cwd=REPO, env=env, timeout=120)
        assert r.returncode == 0, r.stderr
        if stub_won:
            # 桩件胜出 ⇒ 桩件会写 marker,按**第一个空格前的完整路径**归属
            # （`.../python3` 是 `.../python3.11` 的前缀,前缀匹配会混）:
            # 老 python3 至多被**版本探测**（``-c``）碰一下,不许真的读模块表。
            lines = (tmp_path / "marker").read_text(encoding="utf-8").splitlines()
            old_lines = [ln for ln in lines if ln.split(" ", 1)[0] == str(old)]
            new_lines = [ln for ln in lines if ln.split(" ", 1)[0] == str(new)]
            assert old_lines, "没探测到老的 python3？判据失效"
            assert all(" -c " in ln for ln in old_lines), f"太老的 python3 仍被用于实际工作：{old_lines}"
            assert any(ln.endswith(" -") for ln in new_lines), (
                f"模块表不是用 ≥3.11 的那个解释器读的：{new_lines}"
            )
            assert str(old) not in {ln.split(" ", 1)[0] for ln in new_lines}
        # 真实解释器（仓根 .venv / 宿主）胜出时桩件不写 marker;helper rc==0 +
        # 上方 -V 版本断言即「跳过老 python3、用了 ≥3.11」的证据。


class TestOldInterpreterGetsNamedSkip:
    """没有 ≥3.11 时的行为：**具名 SKIP 理由**，不是 traceback、不是空模块表。"""

    @staticmethod
    def _old_python():
        for cand in (shutil.which("python3"), "/usr/bin/python3", shutil.which("python")):
            if not cand:
                continue
            try:
                out = subprocess.run([cand, "-c", "import sys;print('%d.%d' % sys.version_info[:2])"],
                                     capture_output=True, text=True, timeout=30)
            except OSError:
                continue
            if out.returncode == 0 and tuple(
                int(x) for x in out.stdout.strip().split(".")
            ) < (3, 11):
                return cand
        return None

    def test_module_helper_names_the_skip(self, tmp_path):
        old = self._old_python()
        if old is None:
            pytest.skip("本机没有 <3.11 的解释器，无法验证具名跳过")

        env = _clean_env(PYTHON_BIN=old)
        r = subprocess.run([BASH, str(MODULE_HELPER), "decide"],
                           capture_output=True, text=True, cwd=REPO, env=env, timeout=120)
        combined = r.stdout + r.stderr

        assert r.returncode != 0, f"老解释器上必须非零退出（免得被当成空模块表）：{combined[:300]}"
        assert "SKIP(#167)" in combined, f"缺具名 SKIP 理由：{combined[:400]}"
        assert "tomllib" in combined and "3.11" in combined, combined[:400]
        assert "Traceback" not in combined, f"仍是 traceback：{combined[:400]}"
        assert "ModuleNotFoundError" not in combined, f"仍是裸导入错误：{combined[:400]}"


class TestMutRunUsesTheResolvedInterpreter:
    """``mut_run`` 的存活判定必须落在**同一个**解释器上（否则就是伪造 killed）。"""

    def test_mut_run_invokes_the_resolved_interpreter(self, tmp_path):
        """``mut_run`` 必须调用**解析结果**那个解释器（桩件 marker 实证）。

        用 ``PYTHON_BIN``（解析顺序第一档）把解析结果**钉**在桩件上：在装有
        仓根 ``.venv``/宿主新解释器的机器上，解析会落到真实解释器（不写
        桩件 marker），marker 归属断言就无法成立——钉第一档让验证在任何
        机器上确定性地进行。契约本身不变：「mut_run 用解析结果」。
        """
        fake = tmp_path / "bin"
        chosen = _make_stub_python(fake / "python3", version_ok=True, marker=tmp_path / "marker")
        env = _clean_env(PATH=_fake_path(fake), PYTHON_BIN=str(chosen))
        tests = "tests/unit/test_decide.py"
        assert (REPO / tests).exists()

        script = (
            f'source "{COMMON_LIB}"\n'
            "mut_init_timeout >/dev/null 2>&1\n"
            f'mut_run "plaita.__fake__mutmut_1" 60 {tests} >/dev/null 2>&1\n'
            'echo "rc=$?"\n'
        )
        r = subprocess.run([BASH, "-c", script], capture_output=True, text=True,
                           cwd=REPO, env=env, timeout=180)
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "rc=0", r.stdout

        marker = (tmp_path / "marker").read_text(encoding="utf-8")
        assert str(chosen) in marker, f"mut_run 没用解析出来的解释器：{marker}"
        # 有 GNU timeout 时是 `python -m pytest …`；macOS 无 timeout 时是
        # `python - <python> <tmo> <tests>`（纯 Python 回退）。两条都算「用了它」。
        assert re.search(r"-m pytest\b", marker) or re.search(
            r"- .* 60 tests/unit/test_decide\.py", marker
        ), f"没看到 pytest 调用（或其回退形态）：{marker}"


class TestMutInitTimeoutAbortsWithoutPytest:
    """#167 的「必须能 import pytest」不是注释——缺 pytest 必须在开跑前具名中止。"""

    def test_named_abort_when_resolved_interpreter_lacks_pytest(self, tmp_path):
        chosen = _make_stub_python(
            tmp_path / "bin" / "python", version_ok=True, pytest_ok=False,
            marker=tmp_path / "marker",
        )
        env = _clean_env(PYTHON_BIN=str(chosen))
        r = subprocess.run(
            [BASH, "-c", f'source "{COMMON_LIB}"; mut_init_timeout'],
            capture_output=True, text=True, cwd=REPO, env=env, timeout=120,
        )
        combined = r.stdout + r.stderr
        assert r.returncode != 0, f"缺 pytest 仍继续跑（会伪造 killed）：{combined[:400]}"
        assert "没有 pytest" in combined, f"缺具名中止理由：{combined[:400]}"
        assert "PYTHON_BIN" in combined, f"没告诉用户怎么修：{combined[:400]}"
        assert "Traceback" not in combined, f"仍是 traceback：{combined[:400]}"


class TestRepoRootVenvTier:
    """解析顺序第三档：没有 PYTHON_BIN / VIRTUAL_ENV 时用仓根 .venv。"""

    def test_dotvenv_wins_over_old_path_python_and_is_evaluated_once(self, tmp_path):
        repo = tmp_path / "repo"
        lib = repo / "scripts" / "lib" / "mutation_recheck_common.sh"
        lib.parent.mkdir(parents=True)
        shutil.copy(COMMON_LIB, lib)

        venv_python = _make_stub_python(
            repo / ".venv" / "bin" / "python", version_ok=True,
            marker=tmp_path / "marker",
        )
        fake = tmp_path / "pathbin"
        # PATH 上的 python3 太老：若解析顺序漏掉仓根 .venv，就会落到它（或失败）。
        _make_stub_python(fake / "python3", version_ok=False, marker=tmp_path / "marker")
        env = _clean_env(PATH=_fake_path(fake))

        script = (
            f'source "{lib}"\n'
            'printf "first=%s\\n" "$MUT_PYTHON_BIN"\n'
            # source 之后再改环境变量**不应**触发重新解析（source 期只求值一次）
            'PYTHON_BIN=/nonexistent/python\n'
            'printf "second=%s\\n" "$MUT_PYTHON_BIN"\n'
        )
        r = subprocess.run([BASH, "-c", script], capture_output=True, text=True,
                           cwd=repo, env=env, timeout=120)
        assert r.returncode == 0, r.stderr
        assert f"first={venv_python}" in r.stdout, r.stdout
        assert f"second={venv_python}" in r.stdout, r.stdout


class TestMutTimeoutPrimitive:
    """通用超时原语（#167 的共用面）：命令退出码透传，超时归 124。"""

    def test_passthrough_and_timeout(self):
        env = _clean_env(PYTHON_BIN=sys.executable)
        script = (
            f'source "{COMMON_LIB}"\n'
            'MUT_TIMEOUT_BIN=""\n'  # 强制纯 Python 回退，判据与宿主是否装 GNU timeout 无关
            'mut_timeout 10 /bin/sh -c "exit 7"; echo "rc1=$?"\n'
            'mut_timeout 1 /bin/sh -c "sleep 5"; echo "rc2=$?"\n'
        )
        r = subprocess.run([BASH, "-c", script], capture_output=True, text=True,
                           cwd=REPO, env=env, timeout=60)
        assert r.returncode == 0, r.stderr
        assert "rc1=7" in r.stdout, f"退出码未透传：{r.stdout}"
        assert "rc2=124" in r.stdout, f"超时未归 124：{r.stdout}"


# 变异测试工具链里**会真正执行 pytest/mutmut**的 runner 辅助脚本。
# 不含 run_parallel_mutation_sweep.sh：它是 pyenv `loki` 编排器（契约不同）。
MUTATION_RUNNER_HELPERS = [
    "recheck_mutants.sh",
    "recheck_async_utils.sh",
    "recheck_codeflow_expr.sh",
    "recheck_codeflow_nodes.sh",
    "recheck_codeflow_stmt.sh",
    "recheck_sexpr.sh",
    "run_full_mutation_sweep.sh",
    "run_mutation_baseline.sh",
    "run_mut_worker.sh",
    "mutation-module-tests.sh",
]

# 这些脚本直接跑 pytest（或通过 mutmut 跑 pytest），必须先做 pytest 可用性检查。
# run_mut_worker.sh 只编辑 TOML、mutmut 交给 pyenv loki；mutation-module-tests.sh
# 只读 TOML（<3.11 有自己的具名 SKIP），两者不需要 mut_init_timeout。
PYTEST_INIT_HELPERS = {
    "recheck_mutants.sh",
    "recheck_async_utils.sh",
    "recheck_codeflow_expr.sh",
    "recheck_codeflow_nodes.sh",
    "recheck_codeflow_stmt.sh",
    "recheck_sexpr.sh",
    "run_full_mutation_sweep.sh",
    "run_mutation_baseline.sh",
}

BARE_PYTHON = re.compile(r"""(?<![\w$./"'])(python3?)(?![\w.-])""")
BARE_TIMEOUT = re.compile(r"""(?<![\w$"'])\btimeout\s+\d""")


class TestRemainingRunnerHelpersShareResolver:
    """#167 的解析面不止两条脚本：其余 runner 辅助脚本也必须同源。"""

    @pytest.mark.parametrize("name", MUTATION_RUNNER_HELPERS)
    def test_helper_sources_the_shared_library(self, name):
        src = (SCRIPTS / name).read_text(encoding="utf-8")
        assert "mutation_recheck_common.sh" in src, (
            f"{name} 没 source 公共库——解释器/超时解析会再次分叉"
        )

    @pytest.mark.parametrize("name", sorted(PYTEST_INIT_HELPERS))
    def test_helper_calls_named_pytest_abort(self, name):
        src = (SCRIPTS / name).read_text(encoding="utf-8")
        assert re.search(r"^mut_init_timeout\b", src, re.M), (
            f"{name} 没调用 mut_init_timeout——缺 pytest 时会把「没能执行」记成 killed"
        )

    @pytest.mark.parametrize("name", MUTATION_RUNNER_HELPERS)
    def test_no_bare_python_or_timeout_in_executable_lines(self, name):
        offenders = []
        for i, line in enumerate((SCRIPTS / name).read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if BARE_PYTHON.search(line) or BARE_TIMEOUT.search(line):
                offenders.append(f"{name}:{i}: {line.strip()}")
        assert not offenders, (
            "以下位置仍裸调 python/timeout（绕开共享解析，可能伪造成 killed）：\n"
            + "\n".join(offenders)
        )

    def test_recheck_mutants_actually_uses_the_resolved_interpreter(self, tmp_path):
        """把 recheck_mutants.sh 放进沙箱，桩解释器实证它走共享解析。

        只做静态断言不够：#167 的教训正是「脚本里有没有某行代码」不等于
        「实际调用了哪个可执行文件」。这里用 fake ``mutmut`` + stub python 跑完
        整个脚本，断言 marker 归属是**已激活 venv** 的解释器，而不是 PATH 诱饵。
        """
        repo = tmp_path / "repo"
        (repo / "scripts" / "lib").mkdir(parents=True)
        (repo / "mutants").mkdir()
        shutil.copy(COMMON_LIB, repo / "scripts" / "lib" / "mutation_recheck_common.sh")
        shutil.copy(SCRIPTS / "recheck_mutants.sh", repo / "scripts" / "recheck_mutants.sh")

        src = (SCRIPTS / "recheck_mutants.sh").read_text(encoding="utf-8")
        m = re.search(r'^TESTS="([^"]*)"', src, re.M)
        assert m, "recheck_mutants.sh 里没有 TESTS 常量"
        for t in m.group(1).split():
            p = repo / t
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("", encoding="utf-8")

        fake_bin = tmp_path / "fakebin"
        fake_bin.mkdir()
        fake_mutmut = fake_bin / "mutmut"
        fake_mutmut.write_text(
            '#!/usr/bin/env bash\n'
            'printf "    plaita.__fake__mutmut_1: timeout\\n"\n',
            encoding="utf-8",
        )
        fake_mutmut.chmod(0o755)
        decoy = _make_stub_python(
            fake_bin / "python3", version_ok=False, marker=tmp_path / "marker"
        )
        venv_python = _make_stub_python(
            tmp_path / "venv" / "bin" / "python", version_ok=True,
            marker=tmp_path / "marker",
        )

        env = _clean_env(VIRTUAL_ENV=str(tmp_path / "venv"), PATH=_fake_path(fake_bin))
        r = subprocess.run(
            [BASH, str(repo / "scripts" / "recheck_mutants.sh"), "timeout"],
            capture_output=True, text=True, cwd=repo, env=env, timeout=180,
        )
        combined = r.stdout + r.stderr
        assert r.returncode == 0, f"{combined[:800]}"
        marker = (tmp_path / "marker").read_text(encoding="utf-8")
        assert str(venv_python) in marker, f"没用共享解析选中的 venv 解释器：{marker}"
        assert str(decoy) not in marker, f"仍碰了 PATH 上的老 python3：{marker}"
        record = (repo / "mutation-recheck.txt").read_text(encoding="utf-8")
        assert "survived" in record, record

    def test_baseline_calls_named_pytest_abort(self, tmp_path):
        """run_mutation_baseline.sh 也必须在开跑前拦截「缺 pytest」。"""
        chosen = _make_stub_python(
            tmp_path / "bin" / "python", version_ok=True, pytest_ok=False,
            marker=tmp_path / "marker",
        )
        env = _clean_env(PYTHON_BIN=str(chosen))
        r = subprocess.run(
            [BASH, str(SCRIPTS / "run_mutation_baseline.sh")],
            capture_output=True, text=True, cwd=REPO, env=env, timeout=120,
        )
        combined = r.stdout + r.stderr
        assert r.returncode != 0, f"缺 pytest 仍继续跑：{combined[:400]}"
        assert "没有 pytest" in combined, f"缺具名中止理由：{combined[:400]}"
