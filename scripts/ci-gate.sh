#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

COVERAGE_THRESHOLD=80
FAIL=0

echo "========================================"
echo " plaita CI Regression Gate"
echo "========================================"
echo ""

# 1. Run pytest with coverage
echo "[1/4] Running pytest with coverage..."
if python -m pytest tests/ \
    --cov=plaita \
    --cov-report=term-missing \
    --cov-fail-under="$COVERAGE_THRESHOLD" \
    -x -q; then
    echo "  ✓ All tests passed with coverage >= ${COVERAGE_THRESHOLD}%"
else
    echo "  ✗ Tests failed or coverage below ${COVERAGE_THRESHOLD}%"
    FAIL=1
fi
echo ""

# 2. Import layering check
echo "[2/4] Checking import layering (no plaita.core → plaita.server imports)..."
if python -m pytest tests/e2e/test_import_layering.py -x -q 2>/dev/null; then
    echo "  ✓ No reverse imports detected"
elif python -c "
import ast, pathlib, sys
core_dir = pathlib.Path('plaita/core')
violations = []
forbidden = ('plaita.server', 'plaita.storage.redis', 'plaita.storage.sqlalchemy')
for f in core_dir.glob('**/*.py'):
    tree = ast.parse(f.read_text(), filename=str(f))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                for pfx in forbidden:
                    if alias.name == pfx or alias.name.startswith(pfx + '.'):
                        violations.append(f'{f}: imports {alias.name}')
        elif isinstance(node, ast.ImportFrom) and node.module:
            for pfx in forbidden:
                if node.module == pfx or node.module.startswith(pfx + '.'):
                    violations.append(f'{f}: imports {node.module}')
if violations:
    print('  ✗ Reverse imports found:')
    for v in violations:
        print(f'    - {v}')
    sys.exit(1)
print('  ✓ No reverse imports detected')
"; then
    :
else
    echo "  ✗ Import layering check failed"
    FAIL=1
fi
echo ""

# 3. Wheel content check
echo "[3/4] Checking wheel contents (no test_*.py files)..."
WHEEL_DIR=$(mktemp -d)
trap "rm -rf $WHEEL_DIR" EXIT

if python -m pip wheel . --no-deps --wheel-dir="$WHEEL_DIR" -q 2>/dev/null; then
    WHEEL_FILE=$(ls "$WHEEL_DIR"/*.whl 2>/dev/null | head -1)
    if [ -n "$WHEEL_FILE" ]; then
        TEST_FILES=$(python -m zipfile -l "$WHEEL_FILE" 2>/dev/null | grep 'test_.*\.py' || true)
        if [ -z "$TEST_FILES" ]; then
            echo "  ✓ No test_*.py files in wheel"
        else
            echo "  ✗ Found test files in wheel:"
            echo "$TEST_FILES" | head -10 | sed 's/^/    /'
            FAIL=1
        fi
    else
        echo "  ⚠ No wheel file produced"
        FAIL=1
    fi
else
    echo "  ⚠ Wheel build failed (non-fatal)"
fi
echo ""

# 4. Module size check (SC-003: core execution classes < 200 LOC)
echo "[4/4] Checking module sizes (SC-003)..."
if python -c "
import ast, sys
from pathlib import Path

checks = [
    ('plaita/core/executor.py', 'FlowExecution'),
    ('plaita/core/context.py', 'ExecutionContext'),
    ('plaita/core/runner.py', 'NodeRunner'),
    ('plaita/core/strategies.py', 'NormalStrategy'),
    ('plaita/core/strategies.py', 'GeneratorStrategy'),
    ('plaita/core/strategies.py', 'DistributedStrategy'),
    ('plaita/core/callback.py', 'CallbackManager'),
]
fail = 0
for path, name in checks:
    tree = ast.parse(Path(path).read_text())
    loc = next(
        (n.end_lineno - n.lineno + 1 for n in ast.walk(tree)
         if isinstance(n, ast.ClassDef) and n.name == name),
        None,
    )
    if loc is None:
        print(f'  ✗ {name}: not found in {path}')
        fail = 1
        continue
    mark = '✓' if loc < 200 else '✗'
    print(f'  {mark} {name}: {loc} LOC')
    if loc >= 200:
        fail = 1
sys.exit(fail)
"; then
    echo "  ✓ All SC-003 classes within 200 LOC"
else
    echo "  ✗ SC-003 class size check failed"
    FAIL=1
fi
echo ""

# Summary
echo "========================================"
if [ "$FAIL" -eq 0 ]; then
    echo " ✓ CI gate PASSED"
else
    echo " ✗ CI gate FAILED"
fi
echo "========================================"

exit $FAIL
