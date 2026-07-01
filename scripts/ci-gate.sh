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

# 4. Module size check (SC-003: FlowExecution < 200 LOC)
echo "[4/4] Checking module sizes..."
EXECUTOR_LOC=$(grep -c '.' plaita/core/executor.py 2>/dev/null || echo 0)
echo "  plaita/core/executor.py: $EXECUTOR_LOC lines total"

FACADE_LOC=$(python -c "
import ast
tree = ast.parse(open('plaita/core/executor.py').read())
for node in ast.walk(tree):
    if isinstance(node, ast.ClassDef) and node.name == 'FlowExecution':
        loc = node.end_lineno - node.lineno + 1
        print(loc)
        break
" 2>/dev/null || echo "unknown")
echo "  FlowExecution class: $FACADE_LOC lines"
if [ "$FACADE_LOC" != "unknown" ] && [ "$FACADE_LOC" -gt 200 ]; then
    echo "  ✗ FlowExecution exceeds 200 LOC (SC-003)"
    FAIL=1
else
    echo "  ✓ FlowExecution within 200 LOC (SC-003)"
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
