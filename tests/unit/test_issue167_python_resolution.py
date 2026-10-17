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
                      module_line: str = "plaita/node/decide.py") -> Path:
    """造一个**桩解释器**：记录「谁被调用了」，并按需扮演新旧版本。

    * ``-c``（版本探测 / 表达式）⇒ ``version_ok`` 决定退出码；
    * ``-``（stdin 脚本，即模块表那段）⇒ 打印一行模块名；
    * ``-m pytest …`` ⇒ 退出 0（``mut_run`` 的存活路径）。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s %s\\n" "$0" "$*" >> "{marker}"\n'
        'if [[ "${1:-}" == "-c" ]]; then\n'
        f"    exit {0 if version_ok else 1}\n"
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
        """PATH 上 ``python3`` 太老 ⇒ 顺序里的下一个 ≥3.11 候选（python3.11）胜出。"""
        fake = tmp_path / "bin"
        old = _make_stub_python(fake / "python3", version_ok=False, marker=tmp_path / "marker")
        new = _make_stub_python(fake / "python3.11", version_ok=True, marker=tmp_path / "marker")
        env = _clean_env(PATH=_fake_path(fake))

        # PATH 兜底解析出的是**名字**（按名字 exec，走 PATH）
        assert _resolve_common_lib(env) == "python3.11"

        r = subprocess.run([BASH, str(MODULE_HELPER), "decide"],
                           capture_output=True, text=True, cwd=REPO, env=env, timeout=120)
        assert r.returncode == 0, r.stderr
        lines = (tmp_path / "marker").read_text(encoding="utf-8").splitlines()
        # 按**第一个空格前的完整路径**归属（`.../python3` 是 `.../python3.11` 的前缀，
        # 前缀匹配会把两者混在一起）
        old_lines = [ln for ln in lines if ln.split(" ", 1)[0] == str(old)]
        new_lines = [ln for ln in lines if ln.split(" ", 1)[0] == str(new)]
        # 老 python3 只允许被**版本探测**（``-c``）碰一下，不许真的拿它读模块表
        assert old_lines, "没探测到老的 python3？判据失效"
        assert all(" -c " in ln for ln in old_lines), f"太老的 python3 仍被用于实际工作：{old_lines}"
        assert any(ln.endswith(" -") for ln in new_lines), (
            f"模块表不是用 ≥3.11 的那个解释器读的：{new_lines}"
        )


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
        fake = tmp_path / "bin"
        chosen = _make_stub_python(fake / "python3", version_ok=True, marker=tmp_path / "marker")
        env = _clean_env(PATH=_fake_path(fake))
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
