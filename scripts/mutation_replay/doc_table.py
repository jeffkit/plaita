#!/usr/bin/env python3
"""把重放记录渲染成文档表格（``replay.py --record`` 的渲染链，两种 store 通用）。

记录格式（``replay.py --record`` 写出，JSON store 与 ``mutations.py`` store **同形**）::

    {"<变异名>": {"killed": true, "expect_survive": false,
                  "red": ["tests/unit/x.py::test_y", ...],
                  "note": "...", "source": "json:xx.json" | "python:mutations.py"}}

``error`` 记录（pytest 收集/导入错误，见 ``replay.py`` 的判死口径）额外带
``"error": true`` / ``"error_kind"`` / ``"rc"`` / ``"error_line"``，本脚本渲染成
``ERROR（rc=N，未判死）``，**绝不显示为 KILLED**。

用法::

    python scripts/mutation_replay/doc_table.py --record <record.json>            # 打印表行
    python scripts/mutation_replay/doc_table.py --record <r> --out <md>           # 写整篇
    python scripts/mutation_replay/doc_table.py --record <r> --doc <md> --write   # 就地重写表块
    python scripts/mutation_replay/doc_table.py --record <r> --doc <md> --check   # 逐行比对（rc）

``--check`` 做的是「记录 == 文档表」的**逐行**校验（变异 id、红数、红用例 id、**结论列**），
并把文档里**没有任何记录**的行（陈旧行）也判失败 —— 即 #133 的机器校验口径。
结论列口径只由 :func:`status_text` 一处给出（N2）：把 error 记录写成 ``KILLED``、
或把 documented-unobservable 写成 ``SURVIVED``，``--check`` 都会 FAIL（rc=1）。
**同名重复行**也判失败（r5 N2 补洞）：:func:`parse_rows` 是 last-wins，靠前的伪造
``KILLED`` 行会被靠后的正确行静默掩盖；:func:`duplicate_names` 把这种重复直接判 FAIL，
与 ``check_readme_table.py`` 的 ``dupes`` 口径对齐。
**陈旧行的历史豁免只认能解析的表行**（r6 R7）：:func:`historical_names` 要求该行同时能被
``ROW`` 解析、且「历史，已删除」标记紧跟解析出的名字；r5 的子串匹配让任何一行含该子串的
文字都能豁免任意的陈旧 ``KILLED`` 行。
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from typing import Dict, List

SCRIPT = pathlib.Path(__file__).resolve()
REPO = SCRIPT.parents[2]
DEFAULT_RECORD = SCRIPT.parent / "last_replay.json"
DEFAULT_DOC = REPO / "docs" / "mutation-testing.md"

ROW = re.compile(r"^\| \*\*([^*]+)\*\*.*?\| \*\*(\d+)\*\*：([^|]*)\| ([^|]*)\|$")
ANY_ROW = re.compile(r"^\| \*\*[^*]+\*\*")
# 历史行的**规范形状**：标记紧跟被解析出的名字（``| **<name>**（历史，已删除）…``）。
# r5 用的是「文档里任意位置含这个子串」，于是伪造一行**不是表行**的
# ``| **ZZZ**（历史，已删除）| 0 | KILLED |`` 就能豁免任意真实陈旧行（r6 R7）。
ROW_HISTORICAL = re.compile(r"^\| \*\*([^*]+)\*\*（历史，已删除）")
HISTORICAL_MARKER = "历史，已删除"


def status_text(entry: dict) -> str:
    """结论列的唯一来源（渲染与 ``--check`` 共用，杜绝两处口径漂移）。"""
    if entry.get("error"):
        # G1：error 记录（收集/导入错误等）不得渲染成 KILLED
        if entry.get("error_kind") == "anchors":
            # r6 R9：锚点失效没有 pytest rc（rc=None），不要渲染成「rc=None」
            return "ERROR（锚点失效，未判死）"
        return f"ERROR（rc={entry.get('rc')}，未判死）"
    if entry.get("expect_survive"):
        return "SURVIVED（documented-unobservable，无红测试）"
    return "KILLED" if entry.get("killed") else "SURVIVED"


def render_rows(record: Dict[str, dict]) -> List[str]:
    """每个变异一行；红数/红用例 id/结论逐字来自记录（不手写）。"""
    rows: List[str] = []
    for name in sorted(record):
        entry = record[name]
        red = list(entry.get("red") or [])
        reds = "、".join(f"`{item}`" for item in red) if red else "—"
        rows.append(f"| **{name}**：{entry.get('note', '')} | **{len(red)}**：{reds} "
                    f"| {status_text(entry)} |")
    return rows


def rewrite(doc_text: str, rows: List[str]) -> str:
    lines = doc_text.splitlines()
    first = next(index for index, line in enumerate(lines) if ANY_ROW.match(line))
    last = max(index for index, line in enumerate(lines) if ANY_ROW.match(line))
    historical = [line for line in lines[first:last + 1] if HISTORICAL_MARKER in line]
    return "\n".join(lines[:first] + rows + historical + lines[last + 1:]) + "\n"


def parse_rows(doc_text: str) -> Dict[str, tuple]:
    parsed = {}
    for line in doc_text.splitlines():
        match = ROW.match(line)
        if match:
            parsed[match.group(1).strip()] = (
                int(match.group(2)), re.findall(r"`([^`]+)`", match.group(3)),
                match.group(4).strip())
    return parsed


def duplicate_names(doc_text: str) -> List[str]:
    """文档表里**重复出现**的变异名（r5 N2：``parse_rows`` 是 last-wins）。

    靠前的重复行会被靠后的正确行静默掩盖，故 ``--check`` 必须把重复本身判失败；
    口径与 ``check_readme_table.py`` 的 ``dupes`` 一致。
    """
    counts: Dict[str, int] = {}
    for line in doc_text.splitlines():
        match = ROW.match(line)
        if match:
            name = match.group(1).strip()
            counts[name] = counts.get(name, 0) + 1
    return sorted(name for name, count in counts.items() if count > 1)


def historical_names(doc_text: str) -> set:
    """文档里**合法历史行**声明的名字集合（r6 R7）。

    一条历史行必须**同时**满足：① 能被 :data:`ROW` 解析（是真表行，不是随便一行文字）；
    ② 标记「历史，已删除」**紧跟解析出的名字**（:data:`ROW_HISTORICAL`）。

    r5 的判据是子串 ``f"| **{name}**（历史，已删除）" in doc``：只要文档**任意位置**含这个
    子串，名为 ``name`` 的陈旧行就被豁免——verifier 用一行解析不出的
    ``| **ZZZ**（历史，已删除）| 0 | KILLED |`` 就让任意的陈旧 ``KILLED`` 行过关（rc=0）。
    现在豁免只认**表行**，且标记必须在名字位（写进 note 里的标记不算）。
    """
    names = set()
    for line in doc_text.splitlines():
        match = ROW_HISTORICAL.match(line)
        if match and ROW.match(line):
            names.add(match.group(1).strip())
    return names


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", default=str(DEFAULT_RECORD))
    parser.add_argument("--doc", default=str(DEFAULT_DOC), help="文档（--write/--check 用）")
    parser.add_argument("--out", metavar="FILE", help="把整篇文档写到 FILE（不就地改）")
    parser.add_argument("--write", action="store_true", help="就地重写 --doc 的表块")
    parser.add_argument("--check", action="store_true", help="逐行校验记录 == 文档表（rc）")
    args = parser.parse_args(argv)

    record = json.loads(pathlib.Path(args.record).read_text(encoding="utf-8"))
    rows = render_rows(record)

    if args.check:
        doc = pathlib.Path(args.doc).read_text(encoding="utf-8")
        parsed = parse_rows(doc)
        problems = []
        # N2 补洞：parse_rows 是 last-wins，重复行必须显式判失败（否则靠前的伪造行被掩盖）
        dupes = duplicate_names(doc)
        if dupes:
            problems.append(f"文档表重复 key: {dupes}（last-wins 会掩盖靠前的重复行）")
        for name in sorted(record):
            if name not in parsed:
                problems.append(f"{name}: 文档缺行")
                continue
            count, ids, status = parsed[name]
            expected = list(record[name].get("red") or [])
            if count != len(expected):
                problems.append(f"{name}: 红数 {count} != 记录 {len(expected)}")
            if ids != expected:
                problems.append(f"{name}: 红用例 id 与记录不一致（{len(ids)} vs {len(expected)}）")
            # N2：结论列（KILLED / SURVIVED / ERROR / documented-unobservable）也必须一致，
            # 否则「error 记录被写成 KILLED」这种改坏会漏检（渲染器本身输出是对的）。
            want_status = status_text(record[name])
            if status != want_status:
                problems.append(f"{name}: 结论列 {status!r} != 记录应为 {want_status!r}")
        # 历史行（标了「历史，已删除」）允许不在记录里（#133 的口径）；
        # r6 R7：豁免只认**能解析的表行**（historical_names），不再按子串匹配。
        historical = historical_names(doc)
        stale = sorted(name for name in set(parsed) - set(record)
                       if name not in historical)
        if stale:
            problems.append(f"文档里有记录中不存在的行: {stale}")
        if problems:
            for problem in problems:
                print("FAIL:", problem)
            return 1
        print(f"OK: {len(record)} 行与记录逐行一致（无陈旧行）")
        return 0

    if args.write:
        doc_path = pathlib.Path(args.doc)
        doc_path.write_text(rewrite(doc_path.read_text(encoding="utf-8"), rows), encoding="utf-8")
        print(f"rewrote {len(rows)} rows in {doc_path}")
        return 0

    if args.out:
        pathlib.Path(args.out).write_text("\n".join(rows) + "\n", encoding="utf-8")
        print(f"wrote {len(rows)} rows to {args.out}")
        return 0

    print("\n".join(rows))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
