"""``python -m plaita`` 命令行入口。

目前提供一个子命令：``build``——把 ``@flow`` 源码文件编译成 JSON 产物，
统一大仓各子仓此前自带的编译脚手架（issue-keeper ``flows/build_*.py``、
recursive ``.dev/flows/compile_v2.py`` 等）：

    python -m plaita build flows/pipeline_flow.py \
        -o flows/pipeline.flow.json \
        --register plaita_nodes --code-backend subprocess --check

要点：

- **源码模式编译**：读文件全文走 ``compile_source``，AST 行号即源文件绝对
  行号（装饰器/import 模式的「相对 getsource 段」偏移问题不存在）。
- **显式注册业务节点**：``--register plaita_nodes``（默认调模块的
  ``register_all()``）或 ``--register pkg.mod:func``；不依赖 pip dist-info
  entry-points 的新鲜度。``--code-backend subprocess`` 注册 CODE 节点沙箱档。
- **编译即校验**：产物落盘前过 ``validate_flow_ir``（DEFAULT_RULES）硬门，
  与 ``flow_from_source`` / console 发布链同一校验口径。
- **--check**：不落盘，校验现有产物与重编译结果逐字节一致（CI 防产物
  落后源码）；不一致时输出统一 diff 前 80 行。
"""
from __future__ import annotations

import argparse
import difflib
import importlib
import sys
from pathlib import Path
from typing import List, Optional


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="plaita", description="plaita CLI（编译 / 校验 @flow 流程）")
    sub = parser.add_subparsers(dest="command")

    b = sub.add_parser("build", help="@flow 源码文件 → 编译产物 JSON")
    b.add_argument("source", help="@flow 源码 .py 文件路径")
    b.add_argument("-o", "--out", default=None,
                   help="产物路径（默认与源码同目录、同词干的 .plaita.json）")
    b.add_argument("--flow-id", default=None,
                   help="主流程函数名；源码里有多个候选函数时必填。"
                        "注意：传了它也会覆盖产物里的 flow_id（compile_source 语义）")
    b.add_argument("--format", choices=("canonical", "ir"), default="canonical",
                   help="产物格式：canonical=console 正典形态（默认，见"
                        " plaita.dsl.codeflow.to_canonical）；ir=compile_source 直出")
    b.add_argument("--register", action="append", default=[], metavar="MODULE[:FUNC]",
                   help="编译前注册业务节点模块（可重复）。无 :FUNC 时调模块的"
                        " register_all()（缺失则仅 import，靠副作用注册）")
    b.add_argument("--code-backend", default=None, metavar="BACKEND",
                   help="注册 CODE 节点并设默认沙箱后端（常用 subprocess；"
                        "不传则不注册 CODE）")
    b.add_argument("--embed-source", action="store_true",
                   help="把 @flow 源码原文写进产物 metadata.source（console"
                        " 源码页签/「查看权威源码」跳转的数据源，与 mediaflow"
                        " publish_console 注入等效）")
    b.add_argument("--check", action="store_true",
                   help="不落盘；校验现有产物与重编译结果逐字节一致")
    return parser


def _apply_registration(spec: str) -> None:
    module_name, _, func_name = spec.partition(":")
    mod = importlib.import_module(module_name)
    if func_name:
        fn = getattr(mod, func_name)
    else:
        fn = getattr(mod, "register_all", None)
        if fn is None:
            return  # import 即注册（模块副作用型）
    if callable(fn):
        fn()


def _load_ir(args: argparse.Namespace, source_text: str):
    """按 CLI 参数装配 registry 并编译源码 → IR dict（编译前过校验硬门）。"""
    from plaita.dsl.codeflow import compile_source
    from plaita.dsl.ir_validate import DEFAULT_RULES, validate_flow_ir
    from plaita.node import get_default_registry

    for spec in args.register:
        _apply_registration(spec)
    if args.code_backend:
        from plaita.node import register_code_node, resolve_sandbox_allowed_backends

        # plaita#22：生产入口必须显式传 allowed_backends=...，否则流程 JSON 可逐节点
        # 降级到 unsafe（编译期白名单形同虚设）。白名单来源走与 worker / console
        # 同一权威 resolver（plaita#115）：分隔符口径（逗号/冒号/空白）、非法后端
        # 校验、未配置时的默认档与 CRITICAL/WARNING 告警都以它为准，勿在此手拼。
        _backends = resolve_sandbox_allowed_backends(args.code_backend, "cli")
        register_code_node(default_backend=args.code_backend,
                           allowed_backends=list(_backends))
    get_default_registry()  # 触发 entry_points 懒发现（注册过的都数进来）

    ir = compile_source(source_text, args.flow_id)
    validate_flow_ir(ir, rules=DEFAULT_RULES)
    return ir


def _cmd_build(args: argparse.Namespace) -> int:
    from plaita.dsl.codeflow._canonical import (
        count_nodes,
        embed_source,
        serialize_canonical,
        to_canonical,
    )

    source_path = Path(args.source)
    if not source_path.is_file():
        sys.stderr.write(f"plaita build: 源码不存在: {source_path}\n")
        return 2
    source_text = source_path.read_text(encoding="utf-8")
    try:
        ir = _load_ir(args, source_text)
    except ValueError as exc:
        # plaita#115：配置/源码错误给一行可读输出（rc=2），不裸 traceback——
        # 兜底不替代口径统一，白名单校验仍以 resolver / register 为准。
        sys.stderr.write(f"plaita build: {exc}\n")
        return 2
    doc = ir if args.format == "ir" else to_canonical(ir)
    if args.embed_source:
        # @flow 源码原文随产物发布（console 源码页签 / 节点→源码行跳转的数据源）
        embed_source(doc, source_text)
    text = serialize_canonical(doc)

    out_path = (Path(args.out) if args.out
                else source_path.with_suffix(".plaita.json"))
    if args.check:
        if not out_path.is_file():
            sys.stderr.write(
                f"plaita build --check: 产物不存在: {out_path}"
                f"（先跑一次不带 --check 的 build 生成）\n")
            return 1
        current = out_path.read_text(encoding="utf-8")
        if current != text:
            diff = list(difflib.unified_diff(
                current.splitlines(), text.splitlines(),
                f"{out_path} (committed)", "recompiled", lineterm=""))
            tail = f"\n…（共 {len(diff)} 行 diff，截断）" if len(diff) > 80 else ""
            sys.stderr.write("\n".join(diff[:80]) + tail +
                             f"\n产物落后源码，重跑 plaita build 同步: {out_path}\n")
            return 1
        print(f"OK {out_path} 与源码编译产物逐字节一致"
              f"（{count_nodes(doc)} nodes, {len(text)} bytes）")
        return 0

    out_path.write_text(text, encoding="utf-8")
    print(f"compiled -> {out_path}（format={args.format}, "
          f"{count_nodes(doc)} nodes, {len(text)} bytes）")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        # 历史行为：裸 ``python -m plaita`` 打印版本号退出（安装验证入口）
        print(plaita_version())
        return 0
    if args.command == "build":
        return _cmd_build(args)
    parser.error(f"未知子命令 {args.command!r}")
    return 2


def plaita_version() -> str:
    # 惰性取版本：大仓根目录下 ``import plaita`` 会被同名仓库目录遮蔽成
    # namespace package（无 __version__），不能在模块顶层 import。
    try:
        from plaita import __version__

        return __version__
    except (ImportError, AttributeError):  # 大仓同名目录遮蔽成 namespace package
        from importlib import metadata

        try:
            return metadata.version("plaita")
        except metadata.PackageNotFoundError:
            return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
