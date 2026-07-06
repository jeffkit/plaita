#!/usr/bin/env bash
# Usage: run_mut_worker.sh <worktree_dir> <target_module> <test_files...>
# Runs mutmut for one module in an isolated worktree and saves survived list.
set -e
WT=$1; shift
TARGET=$1; shift
TESTS=("$@")

cd "$WT"
# Configure pyproject.toml
python3 - << PYEOF
import re, sys
target = "${TARGET}"
tests = ${TESTS[@]+"${TESTS[@]}"}
PYEOF

# Build python snippet to edit pyproject.toml
python3 - << PYEOF
import re

target = "$TARGET"
tests_raw = """$(IFS=,; echo "$*")"""
test_list = [t.strip() for t in tests_raw.split(",") if t.strip()]

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
