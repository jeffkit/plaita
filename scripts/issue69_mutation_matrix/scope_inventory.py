#!/usr/bin/env python3
"""plaita#69 r6 —— **作用域/物化面清单**的发现器（committed inventory 的生成与守卫）。

X1/X2 之所以被漏掉，是因为"某个节点字段 / 状态值 / 状态派生对象变成求值作用域、
子流程输入、集合/初值/item"的**全部点**从未被枚举过：``Reduce.initial`` 与
嵌套表达式串的作用域都不在任何名单上。本模块把这一面变成机械可查的清单：

* :func:`discover` 用 ``ast`` 扫 ``plaita/``，把每个"形状命中"的调用点编码成稳定键
  ``(relpath, enclosing_qualname, callee, npos, kwargs[, #ordinal])``；
* :func:`load_inventory` 读 ``scope_inventory.json``（committed）；
* 守卫测试 ``tests/unit/test_issue69_scope_inventory.py`` 断言两者相等 ——
  新增一个未分类的作用域/物化点 ⇒ 红（"unclassified ⇒ refuse"）。

**r6 修正的两个缺陷**（verifier 报的）：

* 旧键含 ``col_offset`` ⇒ **只改缩进**也会让键变化（missing + stale 同时出现，假红）。
  r6 的键**既不含行号也不含列号**；同一函数里同形状的多个点用**遍历序序号** ``#k``
  区分（``ast.walk`` 顺序确定，缩进/空行不影响）。
* 旧扫描只认 ``state.evaluate(...)`` 这种直接属性调用；``getattr(state, "evaluate")(v)``
  这种间接分发**不被发现**。r6 把 ``getattr(<obj>, "evaluate")(...)`` 归一到
  ``getattr.evaluate`` 再分类。**已知局限**：属性名是**动态计算**的
  （``getattr(state, name)(v)``、``vars(state)["evaluate"](v)``、``operator.methodcaller``）
  仍无法静态发现 —— 见 ``tests/unit/test_issue69_scope_inventory.py`` 的负控
  （``test_dynamic_attribute_name_is_a_documented_blind_spot``）。

形状（kind）：

* ``explore-scope``  —— ``X.evaluate(value, context[, registry])``：**显式**把某个
  对象当求值容器（嵌套表达式串的作用域就是这里）。
* ``state-scope``    —— ``X.evaluate(value)``：以执行体自身的 live state 为作用域
  （所有节点字段的共同入口；持久根判据在这条路径上生效）。
* ``child-input``    —— ``run_compatible/arun_compatible``（位置输入或 ``item``/
  ``first``/``second``/``index`` 关键字）：状态派生对象变成子执行 ``$INPUT``。
* ``materialise``    —— ``_snapshot_value`` / ``_materialise_child_input``：边界去别名。
* ``fresh-register`` —— ``note_fresh_object`` / ``note_fresh_closure``：freshness 登记点
  （r6 起只在**活动求值作用域**内生效；r7 删除了 4 个作用域外的死调用点，见
  ``tests/unit/test_issue69_scope_inventory.py`` 的
  ``test_four_dead_boundary_registrations_are_gone``）。

用法::

    cd <checkout> && PYTHONPATH=. python scripts/issue69_mutation_matrix/scope_inventory.py
    cd <checkout> && PYTHONPATH=. python scripts/issue69_mutation_matrix/scope_inventory.py --write
"""
from __future__ import annotations

import ast
import json
import pathlib
import sys
from typing import Any, Dict, List, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "plaita"
INVENTORY_PATH = pathlib.Path(__file__).with_name("scope_inventory.json")

#: **行号与列号都不参与**：缩进、空行、上方插入代码都不得让键漂移。
_KEY_FMT = "{relpath}::{qualname}::{callee}::pos{npos}::kw[{kwargs}]"


def _callee_text(node: ast.Call) -> str:
    """Dotted callee name for *node*; ``""`` when it cannot be named statically.

    ``getattr(<obj>, "evaluate")(...)`` is folded to ``getattr.evaluate`` so the
    indirect-dispatch form is classified like the direct attribute call.
    """
    func = node.func
    if (isinstance(func, ast.Call) and isinstance(func.func, ast.Name)
            and func.func.id == "getattr"):
        if (len(func.args) >= 2 and isinstance(func.args[1], ast.Constant)
                and isinstance(func.args[1].value, str)):
            return f"getattr.{func.args[1].value}"
        return ""                              # dynamic attribute name: blind spot
    parts: List[str] = []
    while isinstance(func, ast.Attribute):
        parts.append(func.attr)
        func = func.value
    if isinstance(func, ast.Name):
        parts.append(func.id)
    else:
        parts.append(type(func).__name__)
    return ".".join(reversed(parts))


def _enclosing(tree: ast.AST) -> Dict[int, str]:
    """Map id(node) -> enclosing function qualname for every node."""
    out: Dict[int, str] = {}

    def walk(node: ast.AST, stack: Tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            name = None
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = child.name
            elif isinstance(child, ast.ClassDef):
                name = child.name
            new_stack = stack + (name,) if name else stack
            out[id(child)] = ".".join(new_stack)
            walk(child, new_stack)

    out[id(tree)] = ""
    walk(tree, ())
    return out


def _classify(node: ast.Call, callee: str, enclosing: str) -> str:
    if not callee:
        return ""
    npos = len(node.args)
    if callee == "_materialise_child_input" or callee == "_snapshot_value":
        return "materialise"
    if callee in ("note_fresh_object", "note_fresh_closure"):
        return "fresh-register"
    base = callee.split(".")[-1]
    if base in ("run_compatible", "arun_compatible"):
        return "child-input"
    if base == "evaluate":
        return "explore-scope" if npos >= 2 else "state-scope"
    return ""


def _discover_tree(tree: ast.AST, relpath: str) -> Dict[str, Dict[str, Any]]:
    found: Dict[str, Dict[str, Any]] = {}
    ordinals: Dict[str, int] = {}
    enclosing = _enclosing(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = _callee_text(node)
        kind = _classify(node, callee, enclosing.get(id(node), ""))
        if not kind:
            continue
        kwargs = ",".join(sorted(k.arg for k in node.keywords if k.arg))
        base = _KEY_FMT.format(relpath=relpath,
                               qualname=enclosing.get(id(node), ""),
                               callee=callee, npos=len(node.args),
                               kwargs=kwargs)
        # 同形状多点：按 ast.walk 的确定顺序编号，缩进/空行不改变顺序。
        ordinal = ordinals.get(base, 0)
        ordinals[base] = ordinal + 1
        key = base if ordinal == 0 else f"{base}::#{ordinal}"
        found[key] = {"kind": kind, "file": relpath, "line": node.lineno,
                      "callee": callee, "kwargs": kwargs}
    return found


def discover_source(source: str, relpath: str = "<source>") -> Dict[str, Dict[str, Any]]:
    """Discover the classified call sites of one in-memory source string (tests)."""
    return _discover_tree(ast.parse(source, filename=relpath), relpath)


def discover(root: pathlib.Path = PACKAGE) -> Dict[str, Dict[str, Any]]:
    found: Dict[str, Dict[str, Any]] = {}
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        found.update(_discover_tree(tree, str(path.relative_to(ROOT))))
    return found


def load_inventory(path: pathlib.Path = INVENTORY_PATH) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _stable(info: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Comparison key without the line number (edits above must not invalidate it)."""
    return (info["kind"], info["file"], info["callee"], info["kwargs"])


def main(argv: List[str]) -> int:
    found = discover()
    if "--write" in argv:
        INVENTORY_PATH.write_text(
            json.dumps(found, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8")
        print(f"wrote {len(found)} sites -> {INVENTORY_PATH}")
        return 0
    inventory = load_inventory()
    kinds: Dict[str, int] = {}
    for info in found.values():
        kinds[info["kind"]] = kinds.get(info["kind"], 0) + 1
    for key, info in sorted(found.items(), key=lambda kv: (kv[1]["file"], kv[1]["line"])):
        print(f"{info['kind']:15s} {info['file']}:{info['line']:<4d} {info['callee']}"
              f"({info['kwargs']})")
    print(f"\nsites={len(found)} kinds={dict(sorted(kinds.items()))}")
    missing = sorted(set(found) - set(inventory))
    stale = sorted(set(inventory) - set(found))
    changed = sorted(k for k in set(found) & set(inventory)
                     if _stable(found[k]) != _stable(inventory[k]))
    if missing or stale or changed:
        for key in missing:
            print(f"UNCLASSIFIED (new site): {key}")
        for key in stale:
            print(f"INVENTORY STALE (site gone): {key}")
        for key in changed:
            print(f"INVENTORY DRIFT: {key}")
        return 1
    print("inventory matches: every scope/materialisation site is classified.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
