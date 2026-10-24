#!/usr/bin/env python3
"""自带注册面执行能力矩阵的导出与账本对账（plaita#331 ⑤）。

矩阵事实源是注册表本身：``plaita.node.capabilities.shipped_capability_matrix()``
（内置节点 + 本仓 ``pyproject.toml`` 声明的 ``plaita.nodes`` entry points +
opt-in 的 ``code`` 节点；**空声明节点同样入列**）。治理互锁三处联动：

  注册表（本脚本生成） ↔ 账本 ``docs/governance/architecture.yaml``
  ``gov.capability_matrix`` ↔ 大仓根 ``docs/DOC_ASSERTIONS.yml``
  ``gov/capability-matrix`` expected。

差分断言承重墙（A2 r61 评审 §1.3）：**注册表新增 node_type 而矩阵未变 → 红**。
差分语义：``{added, removed, changed}`` —— 注册表多出账本没有的 node_type =
``added``；账本多出注册表没有的 = ``removed``；同名 node_type 声明集不同 =
``changed``。任一非空即对账红（先红实例见
``tests/unit/test_issue331_execution_profile.py::TestCapabilityMatrix``）。

用法：
    python scripts/export_capability_matrix.py            # 打印矩阵 + 对账（默认 --check）
    python scripts/export_capability_matrix.py --check    # 对账：一致 exit 0，差分 exit 1（stdout 点名差分）
    python scripts/export_capability_matrix.py --json     # 机器可读矩阵（Python↔Go 对拍锚：Go 测试
                                                          # 读同一份与 pkg/ir/capability.go 比对）

维护动作：新增/改声明节点 → 跑本脚本看差分 → 回写账本（--check 绿为落点）
→ 同步大仓 DOC_ASSERTIONS.yml expected。夜间由 ``ci-slow.yml`` 独立步对账。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from plaita.core.capabilities import diff_capability_matrix  # noqa: E402
from plaita.node.capabilities import shipped_capability_matrix  # noqa: E402

DEFAULT_LEDGER = _REPO_ROOT / "docs" / "governance" / "architecture.yaml"


def _load_ledger_matrix(ledger: Path) -> dict:
    import yaml

    data = yaml.safe_load(ledger.read_text(encoding="utf-8"))
    try:
        matrix = data["gov"]["capability_matrix"]
    except (KeyError, TypeError):
        raise SystemExit(
            f"账本 {ledger} 缺少 gov.capability_matrix 键（#331 ⑤ 治理键）"
        )
    if not isinstance(matrix, dict):
        raise SystemExit(f"账本 {ledger} 的 gov.capability_matrix 不是 mapping")
    return matrix


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--ledger",
        type=Path,
        default=DEFAULT_LEDGER,
        help="治理账本路径（默认 docs/governance/architecture.yaml）",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="对账模式：矩阵与账本一致 exit 0，差分 exit 1（stdout 点名差分）",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="输出机器可读矩阵 JSON（stdout；跨引擎对拍消费）",
    )
    args = parser.parse_args()

    matrix = shipped_capability_matrix()
    if args.json:
        print(json.dumps(matrix, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    ledger = _load_ledger_matrix(args.ledger)
    diff = diff_capability_matrix(ledger, matrix)
    if not args.check:
        for node_type, tokens in matrix.items():
            print(f"{node_type}: {tokens}")
        print(f"-- ledger: {args.ledger}")
    if diff:
        print("capability matrix 与账本差分（新增/删改声明节点后须回写账本，"
              "并同步大仓 DOC_ASSERTIONS.yml expected）：")
        print(json.dumps(diff, ensure_ascii=False, indent=2, sort_keys=True))
        return 1
    if args.check:
        print(f"capability matrix 与账本一致（{len(matrix)} node types）：{args.ledger}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
