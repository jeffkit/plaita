#!/usr/bin/env bash
# 构建 plaita-console 发布包：前端构建产物填充 backend/webDist 后打 wheel。
# 产物: dist/plaita_console-*.whl
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> 构建前端 (pnpm build)"
cd "$ROOT/frontend"
pnpm install --frozen-lockfile
pnpm build

echo "==> 填充 backend/webDist"
rm -rf "$ROOT/backend/webDist"
cp -R "$ROOT/frontend/dist" "$ROOT/backend/webDist"

echo "==> 打 wheel"
cd "$ROOT"
# homebrew python 受 PEP 668 保护，build 工具装进独立 venv；系统已有 build 则直用
if python3 -m build --version >/dev/null 2>&1; then
  python3 -m build --wheel
else
  BUILD_VENV="$ROOT/.build-venv"
  [ -x "$BUILD_VENV/bin/python" ] || python3 -m venv "$BUILD_VENV"
  "$BUILD_VENV/bin/pip" install --quiet --upgrade build
  "$BUILD_VENV/bin/python" -m build --wheel
fi

echo "==> 完成: $ROOT/dist/"
ls -lh "$ROOT"/dist/plaita_console-*.whl
