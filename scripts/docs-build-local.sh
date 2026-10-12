#!/usr/bin/env bash
# 本地一键复现 docs-site 构建（与 .github/workflows/docs.yml 的 build job 等价）。
#
# 存在的理由（jeffkit/plaita#126）：`Deploy docs` 曾连续 19 次失败、约 46 小时全红，
# 而该失败**只能在推到 main 之后才被发现**——因为本地通常已有一套顺手的 docs 环境
# （甚至解释器版本都不同），不会复现 CI 的解析结果。
#
# ⚠ 关键点：**CI 用的是 Python 3.10**，而本机默认 python3 往往是 3.13。
# 两者的依赖解析结果**不同**：3.13 会装到 mkdocstrings 1.0.7（能与
# pymdown-extensions 12.2 共存、构建通过），3.10 只能装 1.0.6（与 12.2 冲突、必炸）。
# 所以本脚本默认改用与 CI 一致的 3.10，避免"本地能过、CI 全红"。
#
# 用法:
#   bash scripts/docs-build-local.sh              # 默认 python3.10
#   PYTHON=python3.13 bash scripts/docs-build-local.sh
#   KEEP=1 bash scripts/docs-build-local.sh       # 保留 venv 以便排查
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python3.10}"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "找不到 ${PYTHON}。CI 用 Python 3.10，请指定一个 3.10 解释器：" >&2
    echo "  PYTHON=/path/to/python3.10 bash scripts/docs-build-local.sh" >&2
    exit 127
fi

VER="$("$PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
if [[ "$VER" != "3.10" ]]; then
    echo "[warn] 当前用 Python ${VER}，而 CI 用 3.10。" >&2
    echo "[warn] 依赖解析结果可能不同（见本脚本头部说明），结论未必代表 CI。" >&2
fi

VENV="$(mktemp -d)/dv"
cleanup() { [[ "${KEEP:-0}" == "1" ]] || rm -rf "$(dirname "$VENV")"; }
trap cleanup EXIT

echo "==> 建 venv（${PYTHON}）并安装 docs-site/requirements.txt"
"$PYTHON" -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r docs-site/requirements.txt
echo "==> 安装 plaita（mkdocstrings 内省需要）"
"$VENV/bin/pip" install -q .

echo "==> 实际解析到的关键版本（出现冲突时先看这里）"
"$VENV/bin/pip" list 2>/dev/null | grep -iE "^(mkdocs|mkdocs-material|mkdocstrings|mkdocstrings-python|pymdown-extensions) " || true

echo "==> mkdocs build --clean --strict"
"$VENV/bin/mkdocs" build -f docs-site/mkdocs.yml --clean --strict
echo "==> ✅ 本地构建通过（与 CI build job 等价）"
