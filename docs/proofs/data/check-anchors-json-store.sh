#!/usr/bin/env bash
# 复现「JSON store 锚点全绿」：replay.py 的 --store 同时加载 <store>/mutations.py，
# 本脚本只挂 mutations/ 子目录（不加载 #133/#33 的 mutations.py 存量红）。
set -euo pipefail
TREE="${1:-$(cd "$(dirname "$0")/../../.." && pwd)}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
ln -s "$TREE/scripts/mutation_replay/mutations" "$TMP/mutations"
PYTHONPATH="$TREE" "${PYTHON:-python3}" "$TREE/scripts/mutation_replay/replay.py" \
  --check-anchors --store "$TMP" --tree "$TREE"
