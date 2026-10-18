#!/usr/bin/env bash
# Usage: run_mut_worker.sh <worktree_dir> <target_module> <test_files...>
# Runs mutmut for one module in an isolated worktree and saves survived list.
set -e
WT=$1; shift
TARGET=$1; shift
TESTS=("$@")

# 解释器解析与其它 runner 辅助脚本同源（plaita#167）。本脚本只编辑 TOML，
# 下面的 mutmut 仍由 pyenv `loki` 环境执行，故不调用 mut_init_timeout
# （它要求解释器里必须有 pytest，那个契约属于 loki，不属于这里的 TOML 编辑）。
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/lib/mutation_recheck_common.sh
source "$SCRIPT_DIR/lib/mutation_recheck_common.sh"

cd "$WT"

# Build python snippet to edit pyproject.toml
"$MUT_PYTHON_BIN" - << PYEOF
import re

target = "$TARGET"
tests_raw = """$*"""
test_list = [t.strip() for t in tests_raw.split() if t.strip()]

with open("pyproject.toml", "r") as f:
    content = f.read()

# Update only_mutate
content = re.sub(
    r'only_mutate = \[.*?\]',
    'only_mutate = [\n  "' + target + '",\n]',
    content, flags=re.DOTALL
)

# Update test selection
test_lines = "\n".join(f'  "{t}",' for t in test_list)
content = re.sub(
    r'pytest_add_cli_args_test_selection = \[.*?\]',
    f'pytest_add_cli_args_test_selection = [\n{test_lines}\n]',
    content, flags=re.DOTALL
)

with open("pyproject.toml", "w") as f:
    f.write(content)

print(f"Configured: target={target}, tests={test_list}")
PYEOF

# Clean and run
rm -rf mutants/ .mutmut-cache
echo "=== Starting mutmut for $TARGET ==="
PYENV_VERSION=loki mutmut run 2>&1
echo "=== Results for $TARGET ==="
PYENV_VERSION=loki mutmut results 2>&1 | grep -E "survived|timeout" | awk '{print $1}' | tr -d ':' > /tmp/survived_$(basename $TARGET .py).txt
SURVIVED=$(wc -l < /tmp/survived_$(basename $TARGET .py).txt | tr -d ' ')
echo "Survived: $SURVIVED"
