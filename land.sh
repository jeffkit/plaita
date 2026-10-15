#!/usr/bin/env bash
# land.sh（由 gist-patch-delivery skill 的 gen-land.py 生成）
# 用法: ./land.sh --list | --check <label>… | <label>… | --all | --push <label>… | --dry-run | --gist <gist-id> | --base <ref>
set -euo pipefail
BASE_DEFAULT=fe1590e
TABLE='creds-v3|5866df51f4af1db2a955562c28990cc9|fix/160-161-v3|f75c97f6e8cf863f2f5e62fe9ed9dc09101d9ff5|8fdb06fa8cb9dcdcc00aaeb943de7de5f5953f51|17|'
ALL_KEYS="creds-v3"
MODE=apply; PUSH=0; DRY=0; BASE=$BASE_DEFAULT; TARGETS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --list) MODE=list ;;
    --check) MODE=check ;;
    --push) PUSH=1 ;;
    --dry-run) DRY=1 ;;
    --all) TARGETS="$TARGETS $ALL_KEYS" ;;
    --base) BASE="$2"; shift ;;
    --gist) TARGETS="$TARGETS $(echo "$TABLE" | awk -F'|' -v g="$2" '$2==g{print $1}')"; shift ;;
    -h|--help) sed -n '2,4p' "$0"; exit 0 ;;
    *) TARGETS="$TARGETS $1" ;;
  esac
  shift
done
gist_get() {  # $1=gist  $2=filename  $3=out；最多 5 次（容忍偶发 API 失败）；失败时区分"文件不存在"与"抓取失败"
  local i
  for i in 1 2 3 4 5; do
    if gh gist view "$1" --filename "$2" > "$3" 2>/dev/null && [ -s "$3" ]; then return 0; fi
    sleep 2
  done
  if gh gist view "$1" --files 2>/dev/null | grep -qxF "$2"; then
    echo "  (diag: $2 存在于 gist，但多次抓取失败 —— 疑似 API/网络一过性故障)" >&2
  else
    echo "  (diag: gist 中不存在 $2)" >&2
  fi
  return 1
}
row_of() {
  # 标签兼容：清单标签为 issue168 时，168 / issue168 两种写法都接受
  echo "$TABLE" | awk -F'|' -v t="$1" '
    $1==t {print; f=1; exit}
    $1=="issue" t {print; f=1; exit}
    t=="issue" $1 {print; f=1; exit}
    END{if(!f) exit 1}'
}
if [ "$MODE" = list ]; then
  printf '%-12s %-30s %-10s %-5s %-9s %s\n' LABEL BRANCH HEAD COMMITS BASE GIST
  echo "$TABLE" | while IFS='|' read -r l g b h t c ib; do printf '%-12s %-30s %-10s %-5s %-9s %s\n' "$l" "$b" "$h" "$c" "${ib:-$BASE_DEFAULT}" "$g"; done
  exit 0
fi
[ -n "$TARGETS" ] || { echo "usage: $0 [--list|--check|--push|--dry-run|--all] <label|gist>..." >&2; exit 64; }
# 入口处环境自检：所有模式（含 apply）都要能第一时间发现"临时原语不可用"
if ! { _t=$(mktemp) && rm -f "$_t" && _d=$(mktemp -d) && rmdir "$_d"; } 2>/dev/null; then
  echo "FAIL: temp primitives unusable in this environment (mktemp)" >&2; exit 3
fi
if [ "$MODE" = check ]; then
  rc=0
  for n in $TARGETS; do
    row=$(row_of "$n") || { echo "FAIL: $n not in manifest"; rc=1; continue; }
    IFS='|' read -r l g b h t c ib <<<"$row"
    ib="${ib:-$BASE}"
    _tmp=$(mktemp)
    if gist_get "$g" "${l}-replay.sh" "$_tmp"; then
      if grep -qE "BASE=$ib" "$_tmp"; then
        printf 'OK  %s  gist=%s  base=%s  expect HEAD=%s tree=%s (%s commits)\n' "$l" "$g" "$ib" "$h" "${t:0:8}" "$c"
      else
        printf 'FAIL %s  清单 base=%s 与 %s-replay.sh 的 BASE 不一致\n' "$l" "$ib" "$l"; rc=1
      fi
    else printf 'FAIL %s  gist %s missing %s-replay.sh\n' "$l" "$g" "$l"; rc=1; fi
    rm -f "$_tmp"
  done
  exit $rc
fi
git rev-parse --git-dir >/dev/null 2>&1 || { echo "FAIL: run inside a git clone of the target repo" >&2; exit 2; }
ROOT=$(git rev-parse --show-toplevel)
[ -z "$(git status --porcelain --untracked-files=no)" ] || { echo "FAIL: tracked files modified; commit or stash first" >&2; exit 2; }
git rev-parse -q --verify "${BASE}^{commit}" >/dev/null || { echo "FAIL: base $BASE not present" >&2; exit 2; }
fail=0
for n in $TARGETS; do
  row=$(row_of "$n") || { echo "FAIL: $n not in manifest"; fail=1; continue; }
  IFS='|' read -r l g b h t c ib <<<"$row"
  ib="${ib:-$BASE}"
  git rev-parse -q --verify "${ib}^{commit}" >/dev/null || { echo "  FAIL: 该项基线 $ib 不在本地仓库（先 fetch）"; fail=1; continue; }
  echo "=== $l -> $b (base $ib, expect HEAD $h) ==="
  tmp=$(mktemp)
  if ! gist_get "$g" "${l}-replay.sh" "$tmp"; then
    echo "  FAIL: cannot fetch ${l}-replay.sh from $g"; fail=1; rm -f "$tmp"; continue; fi
  # replay 脚本同时兼容两种写法：EXPECT=<短SHA>（旧）与 EXPECT_FULL=<完整SHA>（新）
  grep -qE "EXPECT(_FULL)?=$h" "$tmp" || { echo "  FAIL: script EXPECT != manifest (refusing)"; fail=1; rm -f "$tmp"; continue; }
  wt=$(mktemp -d); rmdir "$wt"
  if ! git worktree add -q --detach "$wt" "$ib" 2>/dev/null; then
    echo "  WARN: cannot create temp worktree; falling back to in-place"
    [ -z "$(git status --porcelain)" ] || { echo "  FAIL: in-place replay needs a fully clean tree"; fail=1; rm -f "$tmp"; continue; }
    wt="$ROOT"
  fi
  if ! ( cd "$wt" && bash "$tmp" ); then echo "  FAIL: replay failed"; fail=1; rm -f "$tmp"; [ "$wt" != "$ROOT" ] && git worktree remove --force "$wt" >/dev/null 2>&1; continue; fi
  got_h=$(git rev-parse "$b"); got_t=$(git rev-parse "$b^{tree}")   # 必须用完整 SHA 比对：短 SHA 长度随仓库规模变化，拿它比全 hash 会必然假失败
  [ "$wt" != "$ROOT" ] && git worktree remove --force "$wt" >/dev/null 2>&1
  if [ "${got_h:0:${#h}}" != "$h" ] || [ "${got_t:0:${#t}}" != "$t" ]; then
    echo "  FAIL: HEAD=${got_h:0:8} (want ${h:0:8}) tree=${got_t:0:8} (want ${t:0:8}) -- DO NOT PUSH"; fail=1
  else
    echo "  OK: HEAD and tree match the delivery ($c commits)"
    if [ "$PUSH" = 1 ]; then
      if [ "$DRY" = 1 ]; then echo "  [dry-run] git push -u origin $b"; else git push -u origin "$b" && echo "  OK: pushed $b"; fi
    else echo "  (not pushed; use --push or: git push -u origin $b)"; fi
  fi
  rm -f "$tmp"
done
[ "$fail" = 0 ] && echo "ALL DONE." || { echo "SOME ITEMS FAILED (see above)." >&2; exit 1; }
