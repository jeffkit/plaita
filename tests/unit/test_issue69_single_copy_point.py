"""plaita#69 r7 —— **单一拷贝入口**与**单一类型表**的机械守卫。

r6 的 verifier 报了 Finding 1：``plaita/node/loop.py`` 还留着 ``copy.deepcopy``，
于是宿主类覆写 ``__deepcopy__`` 返回 ``self`` 就能把**活对象**送进每次求值的
freshness 作用域（写穿活 state 与 checkpoint）。r7 把数据路径上的 copy 收敛成**一个**
入口 —— ``expression_parser._engine_copy`` —— 本文件用 AST 把它钉死：

* ``plaita/**/*.py`` 里**不允许**出现 `copy` 模块 API 的调用
  （``copy.copy`` / ``copy.deepcopy`` / ``from copy import …`` 后的裸调用），
  唯一例外是 ``state.py::_LazyRootSnapshot.__deepcopy__`` —— 一个**协议钩子**
  （外部对引擎惰性视图调用 ``copy.deepcopy`` 时的兼容实现），它不是数据路径调用点，
  且源码里带有行内标记 ``# issue69-single-copy: protocol-hook``（allowlist 与标记
  双向核对，不能静默扩大）。
* 数据路径上对数据模型值的拷贝调用点做成**普查**（census）：出现新的
  ``_engine_copy`` 调用点必须在此处显式更新（可见、可评审）。
* 类型表只有**一份**：``plaita.core.types`` 的 ``PERSISTABLE_*``；
  ``expression_parser`` 的登记判据/拷贝判据/不可变判据都指向同一批对象。

为什么用 AST 而不是文本 grep：历史注释与 docstring 里会提到 ``copy.deepcopy``
（解释为什么移除它），文本 grep 会把散文当调用；AST 只看**真正的调用**。

r8 G1 更正（r7 的这段书面承诺被证伪）：``dict.copy()`` / ``set.copy()`` /
``value.items()`` / ``enumerate(value)`` 这类**宿主方法/隐式迭代**形式**会**调用宿主
覆写的 ``copy`` / ``items`` / ``__iter__``（``set`` 子类 ``copy() -> self`` 时快照
就是活对象；``$GLOBAL.s is <flow 定义里的 set>``）。它们确实不查
``__copy__``/``__deepcopy__`` 协议，但"不调用任何宿主钩子"是错的，所以本文件新增
**兄弟守卫** ``snapshot_host_protocol_calls`` 覆盖物化边界
``plaita/core/state.py`` 的快照路径：那里只允许 **C 基类协议**读取
（``dict.items(sub)`` / ``list.__iter__(sub)`` / ``tuple.__iter__(sub)`` /
``set.__iter__(sub)`` / ``frozenset.__iter__(sub)``），allow-list 即"接收者是
``dict``/``list``/``tuple``/``set``/``frozenset`` 基类名"这一判据。真正的"第二个
拷贝入口"同样只可能是 copy 模块 API（守卫一）。

``plaita/core/state.py::_snapshot_value`` 是**物化边界**（checkpoint / ``$PARENT`` /
子流程输入 / ``$GLOBAL`` 的每 run 快照）：它是**引擎自有重建**——只按基类协议读宿主
容器、结果必然是普通 ``dict``/``list``/``tuple``/``set``/``frozenset``，产出因而满足
freshness 登记面的精确类型判据（``type(x) is …``，``_is_walkable_container``），且
**有意**按引用共享 JSON 不可表达的非容器对象（它们进不了 checkpoint、也做不了就地
改写目标——#69 门 default-deny 拒绝）；``__class__`` 被伪造成容器的"假容器"抛具名
``StateSnapshotError``（r8 G3）。它的登记方式与 ``_ENGINE_COPY_CENSUS`` 相同：
精确 ``(relpath, qualname)`` 键，见 ``_SNAPSHOT_PATH_REGISTRY``。

``plaita/node/loop.py`` 仍暴露一个模块级别名 ``deepcopy``：它**不是** copy 模块的
``deepcopy``，而是 ``_engine_copy``（保留名字只为让 ML1-C 的性能钉
``tests/unit/test_mfix_l_loop_context.py`` 对 ``loop_mod.deepcopy`` 的 spy 继续生效；
本测试的 ``test_loop_isolation_alias_points_at_the_engine_copy`` 断言二者同一对象）。
"""
from __future__ import annotations

import ast
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "plaita"

#: 唯一允许使用 copy 模块的位置（relpath, qualname, 必须出现在源码里的标记）。
_ALLOWED_COPY_USES = {
    ("plaita/core/state.py", "_LazyRootSnapshot.__deepcopy__",
     "issue69-single-copy: protocol-hook"),
}

#: 数据路径上数据模型拷贝调用点普查（relpath, enclosing qualname, 次数）。
#: 新增/删除一个调用点会在这里红 —— 单一拷贝入口是**结构**约定，不是惯例。
#: ``loop.py`` 的调用走模块级别名 ``deepcopy``（= ``_engine_copy``，见
#: ``test_loop_isolation_alias_points_at_the_engine_copy`` 与 ML1-C 性能钉），
#: 因此扫描同时认这两个名字。
_ENGINE_COPY_CENSUS = {
    ("plaita/core/expression_parser.py", "_detach_alias"): 1,
    # r9 Finding B：set/frozenset 成员逐个过同一个归一化入口（可哈希校验）。
    ("plaita/core/expression_parser.py", "_engine_copy_members"): 1,
    # r9 Finding C：快照路径的 dict 键 / set 成员复用**同一个**拷贝入口 —— 这是
    # "单一拷贝入口"的复用（归一化 or 具名拒绝），不是第二条拷贝实现。
    ("plaita/core/state.py", "_snapshot_key_or_member"): 1,
    ("plaita/node/loop.py", "_condition_context"): 1,
}

#: 递归调用是拷贝器自身的实现细节，不计入普查。
_ENGINE_COPY_SELF = ("plaita/core/expression_parser.py", "_engine_copy")
_COPY_CALL_NAMES = ("_engine_copy", "deepcopy")

#: 快照路径（物化边界）的**唯一实现**入口 —— 与 ``_ENGINE_COPY_CENSUS`` 同一种登记
#: 方式：精确的 ``(relpath, qualname)`` 谓词键。新增/删除一个入口、或把重建逻辑搬到
#: 别的函数，都必须显式更新本表（否则守卫静默失去覆盖）。
#: r8 G1：这些函数里**只**允许 C 基类协议调用；``value.copy()`` / ``sub.items()`` /
#: ``enumerate(value)`` 这类宿主钩子/隐式迭代一律红。
_SNAPSHOT_PATH_REGISTRY = {
    ("plaita/core/state.py", "_snapshot_value"),
    ("plaita/core/state.py", "_snapshot_container"),
    ("plaita/core/state.py", "_snapshot_items"),
    ("plaita/core/state.py", "_snapshot_len"),
    ("plaita/core/state.py", "_snapshot_set"),
}

#: allow-list：接收者必须是这些 **C 基类**之一的属性调用（``dict.items(sub)`` 形态）。
#: 描述符直接访问容器存储，永不落回子类覆写。
_SNAPSHOT_BASE_CLASS_NAMES = frozenset(
    {"dict", "list", "tuple", "set", "frozenset", "str", "bytes", "int", "float", "bool"})

#: 宿主协议属性名：出现在快照路径里就必须是上面的基类调用。
_HOST_PROTOCOL_ATTRS = frozenset(
    {"copy", "items", "keys", "values", "get", "__iter__", "__next__", "__getitem__",
     "__len__", "__contains__"})

#: 隐式迭代构造：``enumerate(value)`` / ``iter(sub)`` / ``set(sub)`` … 会调用宿主的
#: ``__iter__``。r7 的 ``enumerate(value)`` 反例即此形。参数必须是显式基类协议调用
#: （``dict.items(sub)`` / ``list.__iter__(sub)`` …）。
_IMPLICIT_ITERATION_CALLS = frozenset({"enumerate", "iter", "list", "set", "tuple", "frozenset"})
_ALLOWED_ITERATION_ATTRS = frozenset({"items", "__iter__"})
#: 引擎自有的局部名（同函数内现造的容器）：迭代它们不触碰宿主对象。
#: r9 Finding B/C 起 ``_snapshot_set`` 的 ``members`` 也在列：它是本函数内由
#: ``_snapshot_key_or_member``（归一化后的普通数据模型值）现造的 ``set``。
_ALLOWED_ITERATION_NAMES = frozenset({"child_copy", "root_copy", "members"})


def snapshot_host_protocol_calls(relpath: str, source: str) -> list:
    """快照路径里的宿主协议调用/隐式迭代：``(relpath, qualname, lineno, expr)``。

    只在 ``_SNAPSHOT_PATH_REGISTRY`` 登记的函数体内检查；allow-list = 接收者是 C
    基类名的属性调用，或参数是显式基类协议调用的隐式迭代构造。
    """
    tree = ast.parse(source, filename=relpath)
    enclosing = _enclosing_qualnames(tree)
    registered = {q for r, q in _SNAPSHOT_PATH_REGISTRY if r == relpath}
    hits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        qualname = enclosing.get(id(node), "")
        if qualname not in registered:
            continue
        func = node.func
        if isinstance(func, ast.Attribute):
            if func.attr not in _HOST_PROTOCOL_ATTRS:
                continue
            receiver = func.value
            if isinstance(receiver, ast.Name) and receiver.id in _SNAPSHOT_BASE_CLASS_NAMES:
                continue                                   # 基类协议：允许
            hits.append((relpath, qualname, node.lineno, ast.unparse(node)))
        elif isinstance(func, ast.Name) and func.id in _IMPLICIT_ITERATION_CALLS:
            if not node.args:
                continue
            argument = node.args[0]
            if isinstance(argument, ast.Name) and argument.id in _ALLOWED_ITERATION_NAMES:
                continue                                   # 引擎自有局部：允许
            if (isinstance(argument, ast.Call)
                    and isinstance(argument.func, ast.Attribute)
                    and argument.func.attr in _ALLOWED_ITERATION_ATTRS
                    and isinstance(argument.func.value, ast.Name)
                    and argument.func.value.id in _SNAPSHOT_BASE_CLASS_NAMES):
                continue                                   # 显式基类迭代器：允许
            hits.append((relpath, qualname, node.lineno, ast.unparse(node)))
    return sorted(hits)


def _enclosing_qualnames(tree: ast.AST) -> dict:
    """Map ``id(node) -> "Class.method"`` (or ``function``) for every node."""
    out: dict = {}

    def walk(node: ast.AST, stack: tuple) -> None:
        for child in ast.iter_child_nodes(node):
            name = None
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = child.name
            new_stack = stack + (name,) if name else stack
            out[id(child)] = ".".join(new_stack)
            walk(child, new_stack)

    out[id(tree)] = ""
    walk(tree, ())
    return out


def _copy_bindings(tree: ast.AST) -> dict:
    """Names in *tree* bound to the copy module or its ``copy``/``deepcopy`` members."""
    bindings: dict = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "copy":
                    bindings[alias.asname or "copy"] = "*"
        elif isinstance(node, ast.ImportFrom) and node.module == "copy":
            for alias in node.names:
                if alias.name in ("copy", "deepcopy"):
                    bindings[alias.asname or alias.name] = alias.name
    return bindings


def copy_module_calls(relpath: str, source: str) -> list:
    """Return ``(relpath, qualname, lineno, expr)`` for every copy-module call."""
    tree = ast.parse(source, filename=relpath)
    bindings = _copy_bindings(tree)
    enclosing = _enclosing_qualnames(tree)
    hits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if bindings.get(func.value.id) == "*" and func.attr in ("copy", "deepcopy"):
                hits.append((relpath, enclosing.get(id(node), ""), node.lineno,
                             f"{func.value.id}.{func.attr}"))
        elif isinstance(func, ast.Name) and bindings.get(func.id) in ("copy", "deepcopy"):
            hits.append((relpath, enclosing.get(id(node), ""), node.lineno, func.id))
    return sorted(hits)


def engine_copy_calls(relpath: str, source: str) -> list:
    tree = ast.parse(source, filename=relpath)
    enclosing = _enclosing_qualnames(tree)
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id in _COPY_CALL_NAMES:
            hits.append((relpath, enclosing.get(id(node), "")))
    return hits


def _package_sources() -> dict:
    return {
        str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
        for path in sorted(PACKAGE.rglob("*.py"))
    }


class TestIssue69SingleCopyPoint(unittest.TestCase):
    def test_no_copy_module_call_in_the_data_path(self):
        """除协议钩子外，``plaita/`` 里不得有任何 copy 模块调用。"""
        unexpected = []
        for relpath, source in _package_sources().items():
            for hit in copy_module_calls(relpath, source):
                key = (hit[0], hit[1])
                allowed = next((a for a in _ALLOWED_COPY_USES if (a[0], a[1]) == key), None)
                if allowed is None:
                    unexpected.append(hit)
                elif allowed[2] not in source:
                    unexpected.append((*hit, f"missing marker {allowed[2]!r}"))
        self.assertEqual([], unexpected, unexpected[:5])

    def test_allowlist_is_exactly_the_protocol_hook(self):
        """allowlist 不能静默扩大：每个允许项都必须真的在源码里被命中。"""
        found = set()
        for relpath, source in _package_sources().items():
            for hit in copy_module_calls(relpath, source):
                found.add((hit[0], hit[1]))
        allowed = {(relpath, qualname) for relpath, qualname, _ in _ALLOWED_COPY_USES}
        self.assertEqual(allowed, found)
        self.assertEqual(
            {("plaita/core/state.py", "_LazyRootSnapshot.__deepcopy__")}, allowed)

    def test_copy_module_is_imported_only_by_the_protocol_hook_module(self):
        importers = set()
        for relpath, source in _package_sources().items():
            tree = ast.parse(source, filename=relpath)
            if _copy_bindings(tree):
                importers.add(relpath)
        self.assertEqual({"plaita/core/state.py"}, importers)

    def test_engine_copy_call_sites_are_censused(self):
        """单一拷贝入口的调用点普查（新增一个必须显式更新本测试）。"""
        census: dict = {}
        for relpath, source in _package_sources().items():
            for relpath_hit, qualname in engine_copy_calls(relpath, source):
                key = (relpath_hit, qualname)
                if key == _ENGINE_COPY_SELF:
                    continue
                census[key] = census.get(key, 0) + 1
        self.assertEqual(_ENGINE_COPY_CENSUS, census)

    def test_loop_isolation_alias_points_at_the_engine_copy(self):
        """``loop.deepcopy`` 只是**名字**兼容（ML1-C 性能钉），指向 ``_engine_copy``。"""
        import copy as copy_module

        import plaita.core.expression_parser as pm
        import plaita.node.loop as loop_mod

        self.assertIs(loop_mod.deepcopy, pm._engine_copy)
        self.assertIsNot(loop_mod.deepcopy, copy_module.deepcopy)

    def test_guard_has_discriminating_power(self):
        """负控：合成源码里的数据路径 ``copy.deepcopy`` 必须被抓到。"""
        bad = ("import copy\n"
               "def evaluate(value):\n"
               "    return copy.deepcopy(value)\n")
        hits = copy_module_calls("plaita/node/synthetic.py", bad)
        self.assertEqual(1, len(hits))
        self.assertEqual("evaluate", hits[0][1])
        bare = ("from copy import deepcopy as dc\n"
                "def materialise(value):\n"
                "    return dc(value)\n")
        self.assertEqual(1, len(copy_module_calls("plaita/node/synthetic.py", bare)))
        clean = ("def evaluate(value):\n"
                 "    return dict(value)\n")
        self.assertEqual([], copy_module_calls("plaita/node/synthetic.py", clean))

    def test_snapshot_path_registry_covers_the_real_functions(self):
        """登记表不能漂移：每个登记项都必须是 ``state.py`` 里真实的函数定义。"""
        source = (ROOT / "plaita/core/state.py").read_text(encoding="utf-8")
        tree = ast.parse(source, filename="plaita/core/state.py")
        defined = {n.name for n in ast.walk(tree)
                   if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        for relpath, qualname in _SNAPSHOT_PATH_REGISTRY:
            self.assertEqual("plaita/core/state.py", relpath)
            self.assertIn(qualname, defined, f"{qualname} 不在 state.py 里 —— 登记表已漂移")

    def test_snapshot_base_protocol_allowlist_matches_the_module_constant(self):
        """单一事实来源：守卫的 allow-list 必须覆盖 ``state`` 的白名单元组。"""
        from plaita.core import state as state_mod

        self.assertLessEqual(
            {c.__name__ for c in state_mod._SNAPSHOT_BASE_PROTOCOLS},
            set(_SNAPSHOT_BASE_CLASS_NAMES))

    def test_snapshot_path_uses_only_base_class_protocols(self):
        """r8 G1 核心守卫：快照路径不得出现宿主 ``copy``/``items``/``__iter__`` 调用。"""
        source = (ROOT / "plaita/core/state.py").read_text(encoding="utf-8")
        self.assertEqual([], snapshot_host_protocol_calls("plaita/core/state.py", source))

    def test_snapshot_guard_has_discriminating_power(self):
        """负控：r7 的三种宿主钩子/隐式迭代形态逐一被抓，基类协议形态放行。"""
        cases = {
            "host copy()": ("def _snapshot_set(value):\n"
                            "    return value.copy()\n"),
            "host items()": ("def _snapshot_items(value):\n"
                             "    return iter(value.items())\n"),
            "host __iter__()": ("def _snapshot_items(value):\n"
                                "    return enumerate(value)\n"),
            "implicit iter in container": ("def _snapshot_container(root):\n"
                                           "    return tuple(root)\n"),
        }
        for label, bad in cases.items():
            with self.subTest(case=label):
                hits = snapshot_host_protocol_calls("plaita/core/state.py", bad)
                self.assertGreaterEqual(len(hits), 1, (label, hits))
        good = ("def _snapshot_set(value):\n"
                "    return {item for item in set.__iter__(value)}\n"
                "def _snapshot_items(value):\n"
                "    return iter(dict.items(value))\n"
                "def _snapshot_container(root):\n"
                "    return tuple(root_copy)\n")
        self.assertEqual([], snapshot_host_protocol_calls("plaita/core/state.py", good))
        # 未登记的函数不受本守卫约束（它只管物化边界的唯一实现）
        other = ("def unrelated(value):\n"
                 "    return value.copy()\n")
        self.assertEqual([], snapshot_host_protocol_calls("plaita/core/state.py", other))


if __name__ == "__main__":
    unittest.main()
