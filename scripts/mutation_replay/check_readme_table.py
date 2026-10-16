#!/usr/bin/env python
"""校验「README 的变异表」与「store 里的变异定义」**一一对应**（机器可验证）。

来源：``fix/79-85`` 的 ``check_readme_table.py``。统一化适配：新增 ``--store`` /
``--readme``，使同一个脚本既能校验本仓默认 store，也能校验**任一单位**的
store+README（例如复现验证时指向 ``--store /tmp/store-src/<rev>/scripts/mutation_replay``）。

**默认行为（G5/N1 诚实化）**：

- 默认位置**没有** ``mutations/*.json``（补丁未落地时）⇒ ``SKIP`` rc=0；**显式** ``--store``
  仍为 0 个 JSON ⇒ ``FAIL`` rc=1；
- 默认位置**有** ``mutations/*.json`` 且 README 是**统一登记册** ⇒ ``N/A`` rc=0：统一
  README 的 §4 是**按单位/revision** 的表，不是「一变异一行」的单位表，本检查器对它不适用
  （否则会把 4~9 行单位表与 115 个定义比出假失败）。**显式** ``--readme`` 指向登记册 ⇒
  ``FAIL`` rc=1（拿错 README 是用法错，不当成通过）。
- **「登记册」不再是自报字符串（r5 N1 补洞）**：r4 只检查 README 里是否出现
  ``<!-- mutation-replay:unified-registry -->``，于是往**任一单位 README** 追加这一行就能把
  ``--store <unit>`` 从 ``OK``/``FAIL`` 静默变成 ``N/A rc=0``。现在必须**同时**满足三条，
  缺一即按**单位 README 严格校验**：
  1. 标记行带 ``store-version=<64 位十六进制>``：``<!-- mutation-replay:unified-registry store-version=<sha256> -->``；
  2. 该 sha256 **等于本脚本同目录 ``replay.py`` 的 sha256**（由检查器现算，不由 README 自报）；
  3. 该 README **就是本脚本旁边的那份** ``README.md``（路径同一）。
  残留边界（诚实说明）：这是防「顺手自报」，不是防「蓄意复制」——能读到本目录 ``replay.py``
  的人当然能算出哈希并贴到别处，但那样做是**蓄意冒充**，与变异体伪造 pytest 产物同级，
  不在本工具威胁模型内。
- 单位 store + 单位 README ⇒ 照旧严格校验（``--store <unit> --readme <unit README>``）。

用法::

    python scripts/mutation_replay/check_readme_table.py                     # 默认：SKIP/N-A
    python scripts/mutation_replay/check_readme_table.py --store <dir>
    python scripts/mutation_replay/check_readme_table.py --store <dir> --readme <file>
"""
import argparse
import hashlib
import json
import pathlib
import re

SCRIPT = pathlib.Path(__file__).resolve()
ROOT = SCRIPT.parents[2]

# 统一 README 的登记册身份（HTML 注释行）。**光有标记字符串不算登记册**（r5 N1）：
# 必须带 store-version=<sha256(replay.py)>，且该 README 就是本脚本旁边那份。
# r4 只查标记字符串，于是往任一单位 README 追加一行标记即可把 --store <unit> 变成 N/A rc=0。
UNIFIED_MARKER = "mutation-replay:unified-registry"
UNIFIED_MARKER_RE = re.compile(
    r"<!--\s*mutation-replay:unified-registry\s+store-version=([0-9a-f]{64})\s*-->")
UNIFIED_README = SCRIPT.parent / "README.md"
UNIFIED_REPLAY = SCRIPT.parent / "replay.py"


def replay_py_sha256():
    """本脚本同目录 ``replay.py`` 的 sha256（登记册标记里 store-version 的权威值）。"""
    try:
        return hashlib.sha256(UNIFIED_REPLAY.read_bytes()).hexdigest()
    except OSError:
        return None


def registry_identity(table, readme):
    """判断 ``readme`` 是否是**统一登记册**：返回 ``(is_registry, reason)``。"""
    match = UNIFIED_MARKER_RE.search(table)
    if not match:
        return False, (f"标记行缺少 store-version（需 "
                       f"`<!-- {UNIFIED_MARKER} store-version=<sha256(replay.py)> -->`）")
    expected = replay_py_sha256()
    if expected is None:
        return False, f"找不到可校验的 {UNIFIED_REPLAY}，无法确认 store-version"
    if match.group(1) != expected:
        return False, (f"store-version 不匹配（README {match.group(1)[:12]}… != "
                       f"replay.py {expected[:12]}…）")
    if readme.resolve() != UNIFIED_README.resolve():
        return False, f"该 README 不是检查器旁的统一登记册（{UNIFIED_README}）"
    return True, "标记 + store-version 匹配 + 位于统一目录"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--store", default=None,
                    help="store 目录（默认本脚本目录；显式给出却无 mutations/*.json ⇒ FAIL）")
    ap.add_argument("--readme", default=None,
                    help="README 路径（默认 <store>/README.md）")
    args = ap.parse_args()
    explicit_store = args.store is not None
    store = pathlib.Path(args.store).resolve() if explicit_store else SCRIPT.parent
    readme = pathlib.Path(args.readme).resolve() if args.readme else store / "README.md"

    files = sorted((store / "mutations").glob("*.json"))
    if not files:
        if not explicit_store:
            print(f"SKIP: {store} 没有 mutations/*.json —— 本目录是统一 replayer"
                  f"（基础设施补丁，不携带定义），默认跳过。")
            print("      要校验某个单位的表，请显式给：--store <单位 store> --readme <其 README>")
            return 0
        print(f"FAIL: store {store} 没有 mutations/*.json（--store 指错目录？）")
        return 1
    if not readme.exists():
        print(f"FAIL: README 不存在: {readme}")
        return 1

    table = readme.read_text(encoding="utf-8")
    # N1（r5）：登记册身份 = 标记 + store-version 校验 + 路径同一；**自报字符串不再生效**。
    is_registry, reason = registry_identity(table, readme)
    if is_registry:
        if args.readme:
            print(f"FAIL: {readme} 是统一登记册 README（{reason}），其 §4 是按单位/"
                  f"revision 的表，不是「一变异一行」的单位表——请传单位的 README。")
            return 1
        print(f"N/A: 默认 README {readme} 是统一登记册（{reason}），§4 是按单位/"
              f"revision 的表，不是变异表；本检查器对登记册**不适用**（既非通过也非失败）。")
        print("      单位表校验请显式给：--store <单位 store> --readme <单位 README>"
              "（期望 'OK: N 行 == N 个变异文件'，rc=0）")
        return 0
    if UNIFIED_MARKER in table:
        # 有标记但不满足登记册身份 ⇒ 不豁免，继续严格校验（这正是 N1 想堵的绕过）
        print(f"注意: {readme} 含登记册标记但未通过登记册身份校验（{reason}）"
              f"——按单位 README 严格校验，不给 N/A。")

    names = [json.loads(f.read_text(encoding="utf-8"))["name"] for f in files]
    rows = re.findall(r"^\| `([^`]+)` \|", table, re.M)
    problems = []
    if len(rows) != len(files):
        problems.append(f"表格行数 {len(rows)} != 变异文件数 {len(files)}")
    dupes = sorted({r for r in rows if rows.count(r) > 1})
    if dupes:
        problems.append(f"表格重复 key: {dupes}")
    missing = sorted(set(names) - set(rows))
    stale = sorted(set(rows) - set(names))
    if missing:
        problems.append(f"缺行: {missing}")
    if stale:
        problems.append(f"陈旧行（已无对应文件）: {stale}")
    if problems:
        for p in problems:
            print("FAIL:", p)
        return 1
    print(f"OK: {len(rows)} 行 == {len(files)} 个变异文件（key 唯一、无陈旧行）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
