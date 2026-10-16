"""Stored mutation definitions for plaita#133 (plaita#16/G7).

Every aggregate whose red list is recorded in ``docs/mutation-testing.md`` must be
**independently replayable from the delivered tree**.  This module stores the verbatim
``(file, old, new)`` replacements so anyone can reproduce a run with::

    python scripts/mutation_replay/replay.py M16-sibling-revert

Rules (see ``scripts/mutation_replay/README.md``):

* ``old`` must occur exactly once in the target file at this revision;
* a mutation whose anchor no longer exists (the implementation moved on) must be
  dropped from this file **and** its red list removed from the doc — only the
  "KILLED" conclusion may stay.
"""
from __future__ import annotations

DEFAULT_TESTS = (
    "tests/unit/test_issue55_attr_traversal.py",
    "tests/unit/test_expression_registry_guards.py",
    "tests/unit/test_issue55_receiver_matrix.py",
)

MUTATIONS: dict = {
    # --- r16 (G1/G2/G3/G4/G5) -------------------------------------------------
    "M16-sibling-revert": {
        "note": "revert the sibling container helpers to raw author-overridable calls",
        "edits": [
            ("plaita/core/expression.py",
             'def _fn_pop(a, b): return _slot_call(a, "pop", (b,), _POP_BASES)',
             "def _fn_pop(a, b): return a.pop(b)"),
            ("plaita/core/expression.py",
             'def _fn_delete(a, b): return _slot_call(a, "pop", (b,), _DICT_BASES)',
             "def _fn_delete(a, b): return a.pop(b)"),
            ("plaita/core/expression.py",
             'def _fn_set(a, b, c): return _slot_call(a, "update", ({b: c},), _DICT_BASES)',
             "def _fn_set(a, b, c): return a.update({b: c})"),
            ("plaita/core/expression.py",
             'def _fn_clear(a): return _slot_call(a, "clear", (), _DICT_BASES)',
             "def _fn_clear(a): return a.clear()"),
        ],
    },
    "M16-cross-type-allow": {
        "note": 'allow a cross-type slot match (remove the issubclass guard)',
        "edits": [
            ("plaita/core/expression.py",
             """        if issubclass(tp, base):
            return _engine_slot(slot, a, args, f"$F.{name}")
        raise ValueError(
            f"$F helper refuses {name!r} re-bound across unrelated types (plaita#16/G1)")""",
             """        if True:
            return _engine_slot(slot, a, args, f"$F.{name}")
        raise ValueError(
            f"$F helper refuses {name!r} re-bound across unrelated types (plaita#16/G1)")"""),
        ],
    },
    "M16-sort-revert": {
        "note": 'revert $F.sort to the raw instance path (author __iter__ runs again)',
        "edits": [
            ("plaita/core/expression.py",
             """    items = list(_slot_call(a, "__iter__", (), _ITER_BASES))
    keys: list = []
    for item in items:
        key = (_engine_slot(_slot_of(dict, "get"), item, (sort_key,), "$F.sort")
               if issubclass(type(item), dict) else item)""",
             """    items = list(a)
    keys: list = []
    for item in items:
        key = item.get(sort_key) if isinstance(item, dict) else item"""),
        ],
    },
    "M16-g4log-revert": {
        "note": "revert the missing-input-key debug log to %r of the key name",
        "edits": [
            ("plaita/core/expression_parser.py",
             '''                    logger.debug(
                        "expression references a missing input key (name length %d); "
                        "evaluating to None",
                        str.__len__(raw),
                    )''',
             '''                    logger.debug(
                        "expression references missing input key %r; evaluating to None", raw,
                    )'''),
        ],
    },
    "M16-parse-contains-revert": {
        "note": "revert parse_function's C-level containment to the instance protocol",
        "edits": [
            ("plaita/core/expression_parser.py",
             '''        if not str.__contains__(expression, f"{self.prefix}F."):
            return expression''',
             '''        if f"{self.prefix}F." not in expression:
            return expression'''),
        ],
    },
    "M16-registry-guard": {
        "note": "inject an un-hardened helper and register it (the guard must go red)",
        "edits": [
            ("plaita/core/expression.py",
             "def _fn_len(a): return _len_call(a)",
             "def _fn_len(a): return _len_call(a)\ndef _fn_evilprobe(a, b): return a.pop(b)"),
            ("plaita/core/expression.py",
             "    for name, func, desc in _pure:\n        reg.register(name, func, FunctionCategory.ARRAY, description=desc)",
             '    for name, func, desc in _pure:\n        reg.register(name, func, FunctionCategory.ARRAY, description=desc)\n    reg.register("evilProbe", _fn_evilprobe, FunctionCategory.ARRAY, description="probe")'),
        ],
    },
    # --- r18 (J2/J3/J4) -------------------------------------------------------
    "M18-sort-iter-revert": {
        "note": "revert $F.sort's ancestor-slot materialisation to list(subclass)",
        "edits": [
            ("plaita/core/expression.py",
             'items = list(_slot_call(a, "__iter__", (), _ITER_BASES))',
             "items = list(a)"),
        ],
    },
    "M18-sort-nondata-gate-revert": {
        "note": "remove $F.sort's pre-comparison non-data element gate (author __lt__ runs)",
        "edits": [
            ("plaita/core/expression.py",
             """        if not _is_deeply_data(key):
            raise ValueError(
                "$F.sort refuses to compare a non-data element/sort key "
                f"(type '{type(key).__name__}'): only data may be ordered; see MIGRATION.md #55."
            )
""",
             ""),
        ],
    },
    "M18-json-normalise-revert": {
        "note": "revert $F.json_dumps's C-slot normalisation (encoder sees the subclass)",
        "edits": [
            ("plaita/core/expression.py",
             "        return json.dumps(_json_plain(a))",
             "        return json.dumps(a)"),
        ],
    },
    # --- r19 (K1/K2/K6) -------------------------------------------------------
    "M19-data-gate-fail-open": {
        "note": "restore the fail-open data-walk budget (unverified structure = data)",
        "edits": [
            ("plaita/core/expression.py",
             """        remaining -= 1
        if remaining < 0:
            raise ValueError(
                f"value exceeds the {_DATA_WALK_MAX_NODES}-node data-verification budget: "
                "structure too large to verify safely (plaita#133/#19/P4)."
            )""",
             """        remaining -= 1
        if remaining < 0:
            return True"""),
        ],
    },
    "M19-return-gate-fail-open": {
        "note": "restore the fail-open return-gate budget (unverified container = returned)",
        "edits": [
            ("plaita/core/expression_parser.py",
             """        if visits > _MAX_RETURN_GATE_NODES:
            _reject_non_data_field(
                path, where, value,
                kind=(f"container exceeding the {_MAX_RETURN_GATE_NODES}-node "
                      "return-verification budget"))
""",
             """        if visits > _MAX_RETURN_GATE_NODES:
            return
"""),
        ],
    },
    "M19-op-slot-revert": {
        "note": "revert $F.add to the raw operator (an int-subclass __add__ runs again)",
        "edits": [
            ("plaita/core/expression.py",
             'def _fn_add(a, b): return _binop(a, b, "__add__", "__radd__", operator.add, label="add")',
             "def _fn_add(a, b): return a + b"),
        ],
    },
    "M19-json-rehash": {
        "note": "rebuild json_dumps mapping keys with the subclass key (author __hash__ runs)",
        "edits": [
            ("plaita/core/expression.py",
             "            out[_json_key(key)] = _json_plain(item, seen)",
             "            out[key] = _json_plain(item, seen)"),
        ],
    },
    "M19-author-capability-inject": {
        "note": ("inject an operator.methodcaller alias + helper into the engine module "
                 "(the structural guard must refuse it)"),
        "edits": [
            ("plaita/core/expression.py",
             "def _fn_len(a): return _len_call(a)",
             "def _fn_len(a): return _len_call(a)\n"
             "_mc_probe = operator.methodcaller('get', 'k')\n"
             "def _fn_mcprobe(a): return _mc_probe(a)"),
        ],
    },
    # --- r20 (P1/P2/P3/P4) ----------------------------------------------------
    "M20-slot-getattr-fallback": {
        "note": "restore _slot_call's getattr fallback (an author method on a data subclass runs)",
        "edits": [
            ("plaita/core/expression.py",
             """    for engine_type in _engine_slot_types():
        if tp is engine_type:  # identity (R3): a subclass is author code, never trusted
            return getattr(a, name)(*args)
    raise _slot_refusal(name, tp)""",
             """    return getattr(a, name)(*args)"""),
        ],
    },
    "M20-registration-validator-off": {
        "note": "drop the production author-capability refusal at registration (P1)",
        "edits": [
            ("plaita/core/expression.py",
             "        _refuse_author_capability(func, name, allow_unverifiable=allow_unverifiable)\n",
             ""),
        ],
    },
    "M20-binop-left-only": {
        "note": 'route only the left operand (reflected __radd__/__eq__ run again)',
        "edits": [
            ("plaita/core/expression.py",
             """    _require_data_operand(a, label)
    route_b = _op_route(b, reflected)
    if route_b == "refuse":
        raise ValueError(_OP_REFUSAL.format(name=type(b).__name__))
    if isinstance(route_b, tuple):
        result = _engine_slot(type.__getattribute__(route_b[1], "__dict__").get(reflected),
                                  b, (a,), f"$F.{label}")
        if result is not NotImplemented:
            return result
        if identity is not None:
            return (a is b) if identity else (a is not b)
        raise TypeError(
            f"$F.{label}: the trusted C slot refused {type(a).__name__!r} and "
            f"{type(b).__name__!r} (unsupported operand types)."
        )
    _require_data_operand(b, label)
    return plain(a, b)""",
             """    return plain(a, b)"""),
        ],
    },
    "M20-len-contains-fallback": {
        "note": "restore the instance-protocol tails of _len_call / _contains_call",
        "edits": [
            ("plaita/core/expression.py",
             """    for engine_type in _engine_slot_types():
        if type(a) is engine_type:  # identity (R3)
            return len(a)
    raise ValueError(
        f"$F helper refuses len on type '{type(a).__name__}': not a trusted container type "
        "with an ancestor C slot (plaita#133/#19/P3)."
    )""",
             "    return len(a)"),
            ("plaita/core/expression.py",
             """    for engine_type in _engine_slot_types():
        if tp is engine_type:  # identity (R3)
            return item in container
    raise ValueError(
        f"$F helper refuses containment on type '{tp.__name__}': not a trusted container "
        "type with an ancestor C slot (plaita#133/#19/P3)."
    )""",
             "    return item in container"),
        ],
    },
    "M20-data-budget-low": {
        "note": "lower the data-verification budget back under realistic data",
        "edits": [
            ("plaita/core/expression.py",
             "_DATA_WALK_MAX_NODES = 1_000_000",
             "_DATA_WALK_MAX_NODES = 10_000"),
        ],
    },
    "M20-return-budget-low": {
        "note": "lower the return-verification budget back under realistic data",
        "edits": [
            ("plaita/core/expression_parser.py",
             "_MAX_RETURN_GATE_NODES = 1_000_000",
             "_MAX_RETURN_GATE_NODES = 100_000"),
        ],
    },
    # --- r21 (R1/R2/R3/R4) ---------------------------------------------------
    "M21-view-bases-empty": {
        "note": "R1: empty the view base tuple (legal view data is refused again)",
        "edits": [
            ("plaita/core/expression.py",
             "_VIEW_BASES: tuple = (type({}.keys()), type({}.values()), type({}.items()))",
             "_VIEW_BASES: tuple = ()"),
        ],
    },
    "M21-wrapper-silent-allow": {
        "note": 'R2: silently allow a callable with no readable source',
        "edits": [
            ("plaita/core/_author_capability.py",
             """        ccode = candidate.__code__ if (is_function(candidate) or is_method(candidate)) else None
        if ccode is None:
            if not _is_shell(candidate):
                unverifiable_leaf = True
            continue""",
             """        ccode = candidate.__code__ if (is_function(candidate) or is_method(candidate)) else None
        if ccode is None:
            return"""),
        ],
    },
    "M21-engine-slot-issubclass": {
        "note": "R3: compare the engine allowlist with issubclass instead of identity",
        "edits": [
            ("plaita/core/expression.py",
             "        if tp is engine_type:  # identity (R3): a subclass is author code, never trusted",
             "        if issubclass(tp, engine_type):"),
        ],
    },
    "M21-engine-slot-reason-drift": {
        "note": "R3: add an entry to the engine slot allowlist without pinning it",
        "edits": [
            ("plaita/core/expression.py",
             """    "plaita.core.state._LazyRootSnapshot":
        "engine's own lazy $PARENT snapshot (a Mapping over engine state); its get/keys/"
        "__len__ are engine code and it is not constructible from author data",
}""",
             """    "plaita.core.state._LazyRootSnapshot":
        "engine's own lazy $PARENT snapshot (a Mapping over engine state); its get/keys/"
        "__len__ are engine code and it is not constructible from author data",
    "plaita.core.state.CheckpointState":
        "engine state container used as a root; its mapping methods are engine code",
}"""),
        ],
    },
    "M21-contains-linear": {
        "note": "R4: drop the plain-needle C fast path (membership degrades to a linear scan)",
        "edits": [
            ("plaita/core/expression.py",
             """            c_result = _c_contains(container, item)
            if c_result is not None:
                return c_result""",
             """            pass"""),
        ],
    },
    # --- r22 (X1/X2/X3) ------------------------------------------------------
    "M22-twoface-call-lookup": {
        "note": "T1/Y2: normalize_leaf goes back to getattr(type(obj), '__call__')",
        "edits": [
            ("plaita/core/_engine_calls.py",
             """        _owner, entry = static_lookup(tp, "__call__")""",
             """        _owner, entry = (None, getattr(tp, "__call__", None))"""),
        ],
    },
    "M22-metaclass-hook-skip": {
        "note": 'X1: stop checking the metaclass for __getattribute__/__getattr__ overrides',
        "edits": [
            ("plaita/core/_author_capability.py",
             """    for klass in real_mro(metaclass):
        if klass is type or klass is object:
            continue  # ``type``/``object`` own the baseline slots, not an override
        namespace = real_class_dict(klass)
        if "__getattribute__" in namespace:
            return None, "metaclass-__getattribute__"
        if "__getattr__" in namespace:
            return None, "metaclass-__getattr__"
""",
             """    pass
"""),
        ],
    },
    "M22-hard-refuse-identity-only": {
        "note": 'X2/Y4: drop the instance/subclass branch of the hard-refusal table',
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if isinstance(obj, type) and (tp is obj or contains_identity(real_mro(tp), obj)):
            return name""",
             """        pass"""),
        ],
    },
    "M22-view-operand-refuse": {
        "note": "X3: operator/truthiness bases go back to _DATA_VALUE_TYPES (views refused)",
        "edits": [
            ("plaita/core/expression.py",
             """    bases = _operand_bases()
    is_data = any(issubclass(tp, base) for base in bases)""",
             """    bases = _data_bases()
    is_data = any(issubclass(tp, base) for base in bases)"""),
        ],
    },
    "M23-call-gate-off": {
        "note": 'T1: turn the enrollment check off (an unenrolled leaf executes)',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """    recorded = recorded_code(leaf)
    if recorded is _MISSING:
        raise ValueError(_gate_message("unregistered-call-target", label))
""",
             """    recorded = recorded_code(leaf)
    if False:
        raise ValueError(_gate_message("unregistered-call-target", label))
"""),
        ],
    },
    "M23-static-mro-revert": {
        "note": 'Y2: real_mro goes back to cls.__mro__ (metaclass __mro__ property wins)',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """    try:
        return type.__dict__["__mro__"].__get__(cls, type)
    except (AttributeError, KeyError, TypeError):
        return ()""",
             """    try:
        return cls.__mro__
    except AttributeError:
        return ()"""),
        ],
    },
    "M23-static-dict-revert": {
        "note": 'Y2: real_class_dict goes back to cls.__dict__ (metaclass __dict__ property wins)',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """    try:
        return type.__dict__["__dict__"].__get__(cls, type)
    except (AttributeError, KeyError, TypeError):
        return {}""",
             """    return getattr(cls, "__dict__", {})"""),
        ],
    },
    "M23-wrapped-function-only": {
        "note": 'Y3: follow __wrapped__ only for plain functions (lru_cache(hostile) slips through)',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """    if is_function(obj):
        return obj.__dict__.get("__wrapped__")
    for base in real_mro(type(obj)):
        entry = real_class_dict(base).get("__wrapped__")
        if entry is not None:
            return None if _is_descriptor(entry) else entry
    instance = instance_dict_static(obj)
    return instance.get("__wrapped__") if instance else None""",
             """    if is_function(obj):
        return obj.__dict__.get("__wrapped__")
    return None"""),
        ],
    },
    "M23-isinstance-refusal": {
        "note": 'Y4: reintroduce isinstance(candidate, obj) (an author __class__ property runs)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if isinstance(obj, type) and (tp is obj or contains_identity(real_mro(tp), obj)):
            return name""",
             """        if isinstance(obj, type) and isinstance(candidate, obj):
            return name"""),
        ],
    },
    "M23-accessor-raw": {
        "note": 'T1: public accessor hands out the raw desc.func again',
        "edits": [
            ("plaita/core/expression.py",
             """        desc = self._functions.get(name)
        return _gated_callable(desc.func, name) if desc else None""",
             """        desc = self._functions.get(name)
        return desc.func if desc else None"""),
        ],
    },
    "M23-code-recheck-off": {
        "note": 'T1: drop the call-site __code__ re-check',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """    elif recorded is not code:
        # the leaf's *current* code must be the code object recorded at enrollment
        raise ValueError(_gate_message("code-changed", label))
""",
             """    pass
"""),
        ],
    },
    "M23-wrapper-as-leaf": {
        "note": 'T1: record a __wrapped__ wrapper itself as a leaf (its inner code runs)',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """    if static_wrapped(target) is not None:
        return None, None, "wrapped-callable", None""",
             """    if static_wrapped(target) is not None:
        return target, None, None, None"""),
        ],
    },
    "M24-identity-membership-eq": {
        "note": 'AE1: contains_identity compares with == (a trapping metaclass __eq__ runs)',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """    for element in sequence:
        if element is value:
            return True""",
             """    for element in sequence:
        if element == value:
            return True"""),
        ],
    },
    "M24-routine-membership-in": {
        "note": 'AE1: is_routine reverts to tuple membership (candidate __eq__ runs)',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """def is_routine(x) -> bool:
    return contains_identity(_ROUTINE_TYPES, type(x))""",
             """def is_routine(x) -> bool:
    return type(x) in _ROUTINE_TYPES"""),
        ],
    },
    "M24-class-object-opaque": {
        "note": 'AE2: a class object is treated as an opaque enrollable leaf again',
        "edits": [
            ("plaita/core/_engine_calls.py",
             """        return None, None, "class-object", None""",
             """        return target, None, None, None"""),
        ],
    },
    "M24-gatedcall-trusts-code": {
        "note": "AE3: a GatedCall's stored code is trusted again (forge skips the re-check)",
        "edits": [
            ("plaita/core/_engine_calls.py",
             """        leaf, leaf_self, reason, code = normalize_leaf(target.leaf)
        self_obj = target.self_obj if target.self_obj is not None else leaf_self
        return leaf, self_obj, reason, code""",
             """        return target.leaf, target.self_obj, None, target.code"""),
        ],
    },
    "M25-node-by-name": {
        "note": 'AE6: locate the source node by name again (a renamed helper is validated against another body)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """        hits = _scan_code(ccode)""",
             """        hits = _scan_code_by_name(ccode, candidate.__name__)"""),
            ("plaita/core/_author_capability.py",
             """def _scan_code(code) -> Optional[Tuple[Tuple[str, int], ...]]:""",
             """def _scan_code_by_name(code, name) -> Optional[Tuple[Tuple[str, int], ...]]:
    tree = _tree_for_file(code.co_filename)
    if tree is None:
        return None
    for node in ast.walk(tree):
        if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and getattr(node, "name", None) == name):
            return tuple(_scan_with_delegation(
                node, tree, _aliases_for_file(code.co_filename, tree), follow=False))
    return None


def _scan_code(code) -> Optional[Tuple[Tuple[str, int], ...]]:"""),
        ],
    },
    "M25-capability-by-name": {
        "note": 'AE7: the hard-refused builtins table goes back to a name lookup (candidate strings compared)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """    for name, obj in _HARD_REFUSED_BUILTINS:
        if candidate is obj:
            return name
    if is_routine(candidate):
        # A bound builtin (``builtins.getattr`` fetched through a module attribute) is the
        # same object; identity alone decides, so nothing else is needed here.
        if getattr(candidate, "__self__", None) is builtins:
            for name, obj in _HARD_REFUSED_BUILTINS:
                if candidate is obj:
                    return name
    return None""",
             """    module = getattr(candidate, "__module__", None)
    name = getattr(candidate, "__name__", None)
    if module == "builtins" and name in _HARD_REFUSED_BUILTIN_NAMES:
        return name
    return None"""),
        ],
    },
    "M25-strip-type-str-guard": {
        "note": "AE7/AK2: drop the exact-str guard on co_filename (a str subclass enters the scan path)",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    filename = code.co_filename
    if type(filename) is not str:
        # AK2: the exact-str guard runs **before** any cache lookup.
        return (("forged-code-filename", 0),)""",
             """    filename = code.co_filename
    if False:
        return (("forged-code-filename", 0),)"""),
        ],
    },
    "M26-no-consistency-check": {
        "note": "AK1: skip the node<->code consistency check (a forged position is validated by another body)",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    for produced in _codes_at_line(compiled, code.co_firstlineno):
        fingerprint = _code_fingerprint(produced)
        if fingerprint is not _UNSAFE and fingerprint == target:
            return True
    return False
""",
             """    return True
"""),
        ],
    },
    "M26-cache-key-code-object": {
        "note": 'AK2: key the node cache by the code object again (value equality leaks across filenames)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """    key = (filename, firstlineno, _line_span(code), _code_min_column(code, firstlineno))""",
             """    key = code"""),
        ],
    },
    "M26-guard-after-cache": {
        "note": "AK2: run the exact-str co_filename guard after the cache lookup",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    filename = code.co_filename
    if type(filename) is not str:
        # AK2: the exact-str guard runs **before** any cache lookup.
        return (("forged-code-filename", 0),)
    candidates = _node_at_position(filename, code.co_firstlineno, code)
""",
             """    filename = code.co_filename
    candidates = _node_at_position(filename, code.co_firstlineno, code)
    if type(filename) is not str:
        return (("forged-code-filename", 0),)
"""),
        ],
    },
    "M27-no-frozenset-projection": {
        "note": 'AN1: the projection drops the frozenset branch (legal `x in {…}` helpers are refused)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """    if tp is frozenset:
        # Multiset projection (a canonical set of engine-built projections); element order
        # is irrelevant and no author comparison runs.
        projected = frozenset(_project_const(item, depth + 1) for item in value)
        return _UNSAFE if any(item is _UNSAFE for item in projected) else ("frozenset", projected)
""",
             """"""),
        ],
    },
    "M27-no-ellipsis": {
        "note": 'AN1: the projection drops Ellipsis (legal `...` helpers are refused)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """    if tp is types.EllipsisType:
        return ("ellipsis",)
""",
             """"""),
        ],
    },
    "M28-allow-rebindable-callee": {
        "note": "AQ1: a re-bound called name is trusted again (assignment/del/global/nonlocal shapes run)",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if name in rebound_scope.get(scope, ()):
            return None, "rebindable-callee"
""",
             """        if name in rebound_scope.get(scope, ()):
            return None, None
"""),
        ],
    },
    "M28-allow-parameter-callee": {
        "note": 'AQ1: parameters are no longer treated as re-bindable called names',
        "edits": [
            ("plaita/core/_author_capability.py",
             """            rebound |= set(parameters)
            bump(*parameters)""",
             """            bump(*parameters)"""),
        ],
    },
    "M28-no-default-expression-scan": {
        "note": "AQ1/AN1: construct scanning skips the function's default/annotation expressions",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    func_ids = {id(call.func) for call in ast.walk(node) if isinstance(call, ast.Call)}
    hits: List[Tuple[str, int]] = []
    for sub in ast.walk(node):""",
             """    func_ids = {id(call.func) for call in ast.walk(node) if isinstance(call, ast.Call)}
    hits: List[Tuple[str, int]] = []
    _construct_root = (ast.Module(body=node.body, type_ignores=[])
                       if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) else node)
    for sub in ast.walk(_construct_root):"""),
        ],
    },
    "M29-self-attribute-indirection-allow": {
        "note": "AT1/AW1: the whole self.m refusal branch is removed (subclass/instance/class-dict paths run)",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    if name.startswith(("self.", "cls.")):
        receiver, attribute = name.split(".", 1)
""",
             """    if name.startswith(("self.", "cls.")) and owner:
        _target = symbols.get(f"{owner}.{name.split('.', 1)[1]}")
        if _target is not None:
            return _target, None
        return None, None
    if False:
        receiver, attribute = name.split(".", 1)
"""),
        ],
    },
    "M29-readonly-binding-rebound": {
        "note": 'AT2: read-only global/nonlocal declarations are treated as rebinding again',
        "edits": [
            ("plaita/core/_author_capability.py",
             """            if isinstance(node, ast.Global) or isinstance(node, ast.Nonlocal):
                declared |= set(node.names)""",
             """            if isinstance(node, ast.Global) or isinstance(node, ast.Nonlocal):
                declared |= set(node.names)
                rebound |= set(node.names)"""),
        ],
    },
    "M30-attribute-indirection-always": {
        "note": 'AW1: the conditional self.m rule reverts to an unconditional refusal (benign OO calls refused)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """    if name.startswith(("self.", "cls.")):
        receiver, attribute = name.split(".", 1)
""",
             """    if name.startswith(("self.", "cls.")):
        return None, "attribute-indirection"
    if False:
        receiver, attribute = name.split(".", 1)
"""),
        ],
    },
    "M30-allow-self-rebind": {
        "note": 'AW1: the `self`-rebinding condition is dropped (path ① registers and runs the rebound callee)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if _receiver_rebound(node, receiver):
            return None, "rebindable-callee"
""",
             """        if False:
            return None, "rebindable-callee"
"""),
        ],
    },
    "M30-allow-attribute-write": {
        "note": 'AW1: the receiver-attribute-write condition is dropped (instance shadowing registers)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if _receiver_attribute_write(node, receiver):
            return None, "attribute-indirection"
""",
             """        if False:
            return None, "attribute-indirection"
"""),
        ],
    },
    "M30-allow-multi-class-ambiguity": {
        "note": 'AW1: the multi-class-dict ambiguity condition is dropped (subclass overrides register)',
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if len(definitions) > 1:
""",
             """        if False:
"""),
        ],
    },
    "M32-no-alias-resolution": {
        "note": "r33/X'': alias resolution is dropped (alias1/2/3, instance aliases re-register)",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if value.id in aliases:
            decisions = [_receiver_classify(bound, tables, depth + 1)
                         for bound in aliases[value.id]]""",
             """        if value.id in aliases:
            return "unknown"
        if False:
            decisions = [_receiver_classify(bound, tables, depth + 1)
                         for bound in aliases[value.id]]"""),
        ],
    },
    "M32-no-container-resolution": {
        "note": "r33/X'': container-literal / unpacking resolution is dropped (the bare-lambda VALUE branch is separate and corpus-inert)",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    if isinstance(value, (ast.Dict, ast.List, ast.Tuple)):
        items = list(value.values) if isinstance(value, ast.Dict) else list(value.elts)""",
             """    if False and isinstance(value, (ast.Dict, ast.List, ast.Tuple)):
        items = list(value.values) if isinstance(value, ast.Dict) else list(value.elts)"""),
        ],
    },
    "M32-no-function-return": {
        "note": "r33/X'': same-module function-return / closure-return resolution is dropped",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if isinstance(func, ast.Name) and func.id in functions:
            decisions = [_receiver_classify(r, tables, depth + 1) for r in functions[func.id]]""",
             """        if False and isinstance(func, ast.Name) and func.id in functions:
            decisions = [_receiver_classify(r, tables, depth + 1) for r in functions[func.id]]"""),
        ],
    },
    "M32-no-self-instance": {
        "note": "r33/X'': self no longer classifies as a module-ClassDef instance (alias propagation drops)",
        "edits": [
            ("plaita/core/_author_capability.py",
             '        if value.id == "self":\n            return "instance"',
             '        if value.id == "self":\n            return "unknown"'),
        ],
    },
    "M32-no-cls": {
        "note": "r33/X'': cls no longer classifies as the receiver class",
        "edits": [
            ("plaita/core/_author_capability.py",
             '        if value.id == "cls":\n            return "class"',
             '        if value.id == "cls":\n            return "unknown"'),
        ],
    },
    "M31-allow-setattr-path": {
        "note": "BA1: setattr/delattr are no longer construct names and their state-writer label is dropped",
        "edits": [
            ("plaita/core/_author_capability.py",
             """_CTOR_NAMES = frozenset({"methodcaller", "attrgetter", "itemgetter", "partial",
                         "getattr", "vars", "setattr", "delattr"})""",
             """_CTOR_NAMES = frozenset({"methodcaller", "attrgetter", "itemgetter", "partial",
                         "getattr", "vars"})"""),
            ("plaita/core/_author_capability.py",
             """    if name in ("setattr", "delattr") and _is_receiver_expr(call.args[0], receiver, aliases):
        return "attribute-indirection"
""",
             ""),
        ],
    },
    "M31-allow-object-setattr-path": {
        "note": "BA1: object.__setattr__/__delattr__ stop being reflection names and their label is dropped",
        "edits": [
            ("plaita/core/_author_capability.py",
             """_REFLECTION_NAMES = _CTOR_NAMES | frozenset({"call", "__setattr__", "__delattr__"})""",
             """_REFLECTION_NAMES = _CTOR_NAMES | frozenset({"call"})"""),
            ("plaita/core/_author_capability.py",
             """    if name in ("__setattr__", "__delattr__") and _is_receiver_expr(call.args[0], receiver, aliases):
        return "attribute-indirection"
""",
             ""),
        ],
    },
    "M31-allow-type-receiver-expr": {
        "note": "BA1: `type(self)`/`type(cls)` no longer counts as the receiver expression",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    if isinstance(value, ast.Call) and len(value.args) == 1:
        func = value.func
        name = func.id if isinstance(func, ast.Name) else None
        if name is not None and (name == "type" or aliases.get(name) == "type"):
            return _is_receiver_expr(value.args[0], receiver, aliases)
    return False""",
             """    return False"""),
        ],
    },
    "M31-allow-body-aliases": {
        "note": "BA1: one-level body aliases are ignored (aliased type/setattr writes register)",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    aliases: Dict[str, str] = {}
    for sub in _own_nodes(node):
        if not isinstance(sub, ast.Assign) or len(sub.targets) != 1:
            continue""",
             """    aliases: Dict[str, str] = {}
    for sub in []:
        if not isinstance(sub, ast.Assign) or len(sub.targets) != 1:
            continue"""),
        ],
    },
    # --- r20 closed-set drift (owner item ③: 8/8 must be red) -----------------
    "M20-drift-accepted-drop-none": {
        "note": "_ACCEPTED_DATA_TYPES: drop the None special case",
        "edits": [
            ("plaita/core/expression_parser.py",
             "_ACCEPTED_DATA_TYPES: Tuple[type, ...] = _DATA_VALUE_TYPES + (type(None),) + _VIEW_TYPES",
             "_ACCEPTED_DATA_TYPES: Tuple[type, ...] = _DATA_VALUE_TYPES + () + _VIEW_TYPES"),
        ],
    },
    "M20-drift-view-drop-items": {
        "note": "_VIEW_TYPES: drop the dict-items view",
        "edits": [
            ("plaita/core/expression_parser.py",
             "_VIEW_TYPES: Tuple[type, ...] = (type({}.keys()), type({}.values()), type({}.items()))",
             "_VIEW_TYPES: Tuple[type, ...] = (type({}.keys()), type({}.values()))"),
        ],
    },
    "M20-drift-data-add-complex": {
        "note": "_DATA_VALUE_TYPES: accept a non-data type (complex)",
        "edits": [
            ("plaita/core/expression_parser.py",
             "    datetime.datetime, datetime.date, datetime.time, datetime.timedelta, Decimal,\n    enum.Enum,\n)",
             "    datetime.datetime, datetime.date, datetime.time, datetime.timedelta, Decimal,\n    enum.Enum, complex,\n)"),
        ],
    },
    "M20-drift-data-drop-str": {
        "note": "_DATA_VALUE_TYPES: drop str from the closed whitelist",
        "edits": [
            ("plaita/core/expression_parser.py",
             "_DATA_VALUE_TYPES: Tuple[type, ...] = (\n    str, bytes, bytearray, bool, int, float, list, tuple, dict, set, frozenset,",
             "_DATA_VALUE_TYPES: Tuple[type, ...] = (\n    bytes, bytearray, bool, int, float, list, tuple, dict, set, frozenset,"),
        ],
    },
    "M20-drift-clevel-add-complex": {
        "note": "_C_LEVEL_RENDER_TYPES: accept complex",
        "edits": [
            ("plaita/core/expression_parser.py",
             "    datetime.datetime, datetime.date, datetime.time, datetime.timedelta, Decimal,\n)",
             "    datetime.datetime, datetime.date, datetime.time, datetime.timedelta, Decimal, complex,\n)"),
        ],
    },
    "M20-drift-clevel-drop-decimal": {
        "note": "_C_LEVEL_RENDER_TYPES: drop Decimal",
        "edits": [
            ("plaita/core/expression_parser.py",
             "    datetime.datetime, datetime.date, datetime.time, datetime.timedelta, Decimal,\n)",
             "    datetime.datetime, datetime.date, datetime.time, datetime.timedelta,\n)"),
        ],
    },
    "M20-drift-exactonly-add-dict": {
        "note": "_EXACT_ONLY_RENDER_TYPES: drift by adding dict",
        "edits": [
            ("plaita/core/expression_parser.py",
             "_EXACT_ONLY_RENDER_TYPES: Tuple[type, ...] = (OrderedDict, Counter, defaultdict)",
             "_EXACT_ONLY_RENDER_TYPES: Tuple[type, ...] = (OrderedDict, Counter, defaultdict, dict)"),
        ],
    },
    "M20-drift-enum-set-empty": {
        "note": "_ENUM_RENDER_TYPES: empty the enum render tier",
        "edits": [
            ("plaita/core/expression_parser.py",
             "    ) if isinstance(typ, type)\n)",
             "    ) if False\n)"),
        ],
    },
    # --- r15 (E1/E2/E4/E5/E6) -------------------------------------------------
    "M15-eq-forge": {
        "note": "use == instead of `is` in the trusted-slot identity check",
        "edits": [
            ("plaita/core/expression_parser.py",
             """        if slot is not None and slot is fn and issubclass(tp, base):
            return True""",
             """        if slot is not None and slot == fn and issubclass(tp, base):
            return True"""),
        ],
    },
    "M15-annotate-preview": {
        "note": "re-interpolate the expression text into the parse-error message",
        "edits": [
            ("plaita/core/expression_parser.py",
             '''            f"无法解析表达式（长度 {str.__len__(value)}，第 {col} 列附近）："''',
             '''            f"无法解析表达式：{value!r}（第 {col} 列附近）："'''),
        ],
    },
    "M15-warn-value": {
        "note": "re-interpolate the value into the unmatched-template warning",
        "edits": [
            ("plaita/core/expression_parser.py",
             '''        "expression (length %d) contains '{%%' but no valid {%% ... %%} expression matched "
        "(inner expression is malformed or unclosed); it was returned AS-IS without "
        "evaluation — fix the inner expression (e.g. {%% $INPUT.x %%}).",
        str.__len__(value),''',
             '''        "expression %r contains '{%%' but no valid {%% ... %%} expression matched "
        "(inner expression is malformed or unclosed); it was returned AS-IS without "
        "evaluation — fix the inner expression (e.g. {%% $INPUT.x %%}).",
        _plain_str(value),'''),
        ],
    },
    "M15-context-strip": {
        "note": "drop the __context__ chain sanitization for nested-expression errors",
        "edits": [
            ("plaita/core/expression_parser.py",
             """                        except KeyError as exc:
                            # plaita#15/E5：``from None`` 只压制**显示**，``__context__``
                            # 仍可取到原异常（其 args 可能含根名/字段值）——就地脱敏整条链。
                            _strip_exception_data(exc)
                            # 兜底归因""",
             """                        except KeyError:
                            # 兜底归因"""),
        ],
    },
    # --- r14 (DA/DB/DC) -------------------------------------------------------
    "M14-objclass-forge": {
        "note": "trust fn.__objclass__ (a writable attribute) instead of object identity",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    for base in bases:
        slot = _base_slot(base, name)
        if slot is not None and slot is fn and issubclass(tp, base):
            return True
    return False""",
             """    objclass = getattr(fn, "__objclass__", None)
    return objclass is None or issubclass(tp, objclass)"""),
        ],
    },
    "M14-mapping-get": {
        "note": "revert _mapping_get to the author-overridable obj.get(path, None)",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if issubclass(type(obj), dict):
        value = dict.get(obj, path, None)
    else:
        value = obj.get(path, None)""",
             "    value = obj.get(path, None)"),
        ],
    },
    "M14-dc-preview": {
        "note": "re-interpolate the field value into the nested-expression attribution",
        "edits": [
            ("plaita/core/expression_parser.py",
             '''                            raise KeyError(
                                f"解析 {field_path} 时，字段值是一段嵌套表达式字符串"
                                f"（长度 {str.__len__(attr)}）：嵌套字符串以**持有它的"''',
             '''                            preview = attr if len(attr) < 120 else attr[:117] + "..."
                            raise KeyError(
                                f"解析 {field_path} 时，字段值是一段嵌套表达式字符串"
                                f" {preview!r}（长度 {str.__len__(attr)}）：嵌套字符串以**持有它的"'''),
        ],
    },
    "M14-probe-attr-access": {
        "note": "make the container probe fall back to attribute access",
        "expect_survive": True,
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if hasattr(obj, "__contains__"):
        try:
            return root_key in obj
        except Exception as exc:  # noqa: BLE001 — exotic containers must not break eval""",
             """    if hasattr(obj, "__contains__"):
        try:
            return getattr(obj, root_key, _MISSING) is not _MISSING  # attribute-access probe
        except Exception as exc:  # noqa: BLE001 — exotic containers must not break eval"""),
        ],
    },
    # --- r13 (F1/P/R) ---------------------------------------------------------
    "M13-borrow-fn-set": {
        "note": "tier ① back to function-object set membership (undo the defining-class rule)",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if _is_c_level_render_type(tp):
        return (_is_known_c_level_dunder("__repr__", repr_fn, tp)
                and _is_known_c_level_dunder("__str__", str_fn, tp))""",
             """    if _is_c_level_render_type(tp):
        return (repr_fn in _SAFE_RENDER_FUNCTIONS and str_fn in _SAFE_RENDER_FUNCTIONS)"""),
        ],
    },
    "M13-view-exact": {
        "note": "make _is_view_type exact-type only (undo the odict_keys fix)",
        "edits": [
            ("plaita/core/expression_parser.py",
             "    return any(tp is view or issubclass(tp, view) for view in _VIEW_TYPES)",
             "    return tp in frozenset(_VIEW_TYPES)"),
        ],
    },
    "M13-hooks-scans-only": {
        "note": ("drop all three instance-protocol hook checks (render gate, _is_data_type, and the "
                 "type-layer guard in _reject_non_data_value); keeps the runtime __class__ check"),
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if _has_forbidden_override(facts, _GLOBAL_FORBIDDEN_NAMES):
        return False
    try:""",
             "    try:"),
            ("plaita/core/expression_parser.py",
             """    if _type_has_hook_override(tp):
        return False
    return _is_data_type_unchecked(tp)""",
             "    return _is_data_type_unchecked(tp)"),
            ("plaita/core/expression_parser.py",
             """    if _type_has_hook_override(tp):
        # The global instance-protocol ban, ahead of the Enum/module branches.
        _reject_non_data_field(path, where, value)
    if issubclass(tp, enum.Enum):""",
             "    if issubclass(tp, enum.Enum):"),
        ],
    },
    # --- r14/r15 extra hook variants ------------------------------------------
    "M14-hooks-both": {
        "note": "drop the hook checks everywhere (render gate, _is_data_type, E4 guard, runtime __class__)",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if _has_forbidden_override(facts, _GLOBAL_FORBIDDEN_NAMES):
        return False
    try:
        if object.__getattribute__(value, "__class__") is not tp:
            return False
    except Exception:  # noqa: BLE001 - any failure must not escape (plaita#12/F3)
        logger.debug("expression gate: __class__ probe failed; refusing render")
        return False
""",
             ""),
            ("plaita/core/expression_parser.py",
             """    if _type_has_hook_override(tp):
        return False
    return _is_data_type_unchecked(tp)""",
             "    return _is_data_type_unchecked(tp)"),
            ("plaita/core/expression_parser.py",
             """    if _type_has_hook_override(tp):
        # The global instance-protocol ban, ahead of the Enum/module branches.
        _reject_non_data_field(path, where, value)
    if issubclass(tp, enum.Enum):""",
             "    if issubclass(tp, enum.Enum):"),
        ],
    },
    "M15-e4-revert": {
        "note": "move the hook ban after the Enum/module branches and restore instance isinstance",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if _type_has_hook_override(tp):
        # The global instance-protocol ban, ahead of the Enum/module branches.
        _reject_non_data_field(path, where, value)
    if issubclass(tp, enum.Enum):""",
             "    if issubclass(tp, enum.Enum):"),
            ("plaita/core/expression_parser.py",
             """    if _is_data_type(type(value)):
        return
    _reject_non_data_field(path, where, value)""",
             """    if _type_has_hook_override(tp):
        _reject_non_data_field(path, where, value)
    if _is_data_type(type(value)):
        return
    _reject_non_data_field(path, where, value)"""),
            ("plaita/core/expression_parser.py",
             "    if not issubclass(type(value), dict):\n        return None",
             "    if not isinstance(value, dict):\n        return None"),
            ("plaita/core/expression_parser.py",
             "    if issubclass(type(obj), dict):\n        return dict.__contains__(obj, root_key)",
             "    if isinstance(obj, dict):\n        return root_key in obj"),
            ("plaita/core/expression_parser.py",
             "issubclass(type(attr), str)\n                        and (str.startswith(attr, prefix)",
             "isinstance(attr, str)\n                        and (str.startswith(attr, prefix)"),
        ],
    },
    # --- r9/r10/r11 -----------------------------------------------------------
    "M9-concat": {
        "note": "drop the fail-closed render gate on stringifying-helper arguments",
        "edits": [
            ("plaita/core/expression_parser.py",
             """            if func_name in _STRINGIFYING_HELPERS:
                # The helper is about to ``str()``/``repr()`` its arguments — gate them
                # with the fail-closed render gate *before* the call; gating the result
                # is too late once the live ``repr`` is inside the returned string.
                for arg in args:
                    _reject_non_data_render(arg, "…", call_label)
""",
             ""),
        ],
    },
    "M10-protocol-walk": {
        "note": "walk containers through the instance protocol instead of the C-level builtins",
        "edits": [
            ("plaita/core/expression_parser.py", "for key, item in dict.items(value):", "for key, item in value.items():"),
            ("plaita/core/expression_parser.py", "for position, element in enumerate(list.__iter__(value)):", "for position, element in enumerate(value):"),
            ("plaita/core/expression_parser.py", "for position, element in enumerate(tuple.__iter__(value)):", "for position, element in enumerate(value):"),
            ("plaita/core/expression_parser.py", "for position, element in enumerate(set.__iter__(value)):", "for position, element in enumerate(value):"),
            ("plaita/core/expression_parser.py", "for position, element in enumerate(frozenset.__iter__(value)):", "for position, element in enumerate(value):"),
        ],
    },
    "M10-enum-repr-only": {
        "note": "compare only Enum.__repr__, not __str__",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if repr_fn not in _SAFE_RENDER_FUNCTIONS or str_fn not in _SAFE_RENDER_FUNCTIONS:
        return False
    if not (_is_known_enum_dunder("__repr__", repr_fn, tp)
            and _is_known_enum_dunder("__str__", str_fn, tp)):
        return False
    return not _has_forbidden_override(facts, _ENUM_FORBIDDEN_NAMES)""",
             """    if repr_fn not in _SAFE_RENDER_FUNCTIONS:
        return False
    return not _has_forbidden_override(facts, _ENUM_FORBIDDEN_NAMES)"""),
        ],
    },
    "M11-one-dunder": {
        "note": "every tier compares only __repr__",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if repr_fn not in _SAFE_RENDER_FUNCTIONS or str_fn not in _SAFE_RENDER_FUNCTIONS:
        return False
    if not (_is_known_enum_dunder("__repr__", repr_fn, tp)
            and _is_known_enum_dunder("__str__", str_fn, tp)):
        return False
    return not _has_forbidden_override(facts, _ENUM_FORBIDDEN_NAMES)""",
             """    if repr_fn not in _SAFE_RENDER_FUNCTIONS:
        return False
    return not _has_forbidden_override(facts, _ENUM_FORBIDDEN_NAMES)"""),
            ("plaita/core/expression_parser.py",
             """            return (repr_klass is tp
                    and _is_known_c_level_dunder("__str__", str_fn, tp))
        if issubclass(tp, exact):""",
             """            return repr_klass is tp
        if issubclass(tp, exact):"""),
            ("plaita/core/expression_parser.py",
             """        return (repr_klass is tp
                and _is_known_c_level_dunder("__str__", str_fn, tp))
    if _is_c_level_render_type(tp):""",
             """        return repr_klass is tp
    if _is_c_level_render_type(tp):"""),
            ("plaita/core/expression_parser.py",
             """        return (_is_known_c_level_dunder("__repr__", repr_fn, tp)
                and _is_known_c_level_dunder("__str__", str_fn, tp))""",
             """        return _is_known_c_level_dunder("__repr__", repr_fn, tp)"""),
        ],
    },
    "M11-no-meta-guard": {
        "note": "accept every metatype",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    meta = type(tp)
    return meta is type or any(meta is allowed for allowed in _SAFE_METACLASSES)""",
             "    return True"),
        ],
    },
    "M11-class-facts-getattr": {
        "note": "read class facts through getattr (consistency validation kept)",
        "edits": [
            ("plaita/core/expression_parser.py",
             '        mro = type.__getattribute__(tp, "__mro__")',
             '        mro = getattr(tp, "__mro__")'),
            ("plaita/core/expression_parser.py",
             '            mapping = type.__getattribute__(klass, "__dict__")',
             '            mapping = getattr(klass, "__dict__")'),
        ],
    },
    "M11-class-facts-no-validation": {
        "note": "drop the consistency validation in _class_facts",
        "expect_survive": True,
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if not isinstance(tp, type):
        return None
    try:
        mro = type.__getattribute__(tp, "__mro__")
    except (AttributeError, TypeError):  # pragma: no cover - defensive
        return None
    if not isinstance(mro, tuple) or not mro or mro[0] is not tp:
        return None
    facts: List[Tuple[type, Any]] = []
    for klass in mro:
        if not isinstance(klass, type):
            return None
        try:
            mapping = type.__getattribute__(klass, "__dict__")
        except (AttributeError, TypeError):  # pragma: no cover - defensive
            return None
        if not isinstance(mapping, types.MappingProxyType):
            return None
        facts.append((klass, mapping))
    return facts""",
             """    if not isinstance(tp, type):
        return None
    mro = type.__getattribute__(tp, "__mro__")
    return [(klass, type.__getattribute__(klass, "__dict__")) for klass in mro]"""),
        ],
    },
    "M11-class-facts-getattr-no-validation": {
        "note": "compound: read via getattr AND drop the consistency validation",
        "edits": [
            ("plaita/core/expression_parser.py",
             """    if not isinstance(tp, type):
        return None
    try:
        mro = type.__getattribute__(tp, "__mro__")
    except (AttributeError, TypeError):  # pragma: no cover - defensive
        return None
    if not isinstance(mro, tuple) or not mro or mro[0] is not tp:
        return None
    facts: List[Tuple[type, Any]] = []
    for klass in mro:
        if not isinstance(klass, type):
            return None
        try:
            mapping = type.__getattribute__(klass, "__dict__")
        except (AttributeError, TypeError):  # pragma: no cover - defensive
            return None
        if not isinstance(mapping, types.MappingProxyType):
            return None
        facts.append((klass, mapping))
    return facts""",
             """    if not isinstance(tp, type):
        return None
    mro = getattr(tp, "__mro__")
    return [(klass, getattr(klass, "__dict__")) for klass in mro]"""),
        ],
    },
    # --- r33 / the shrunk Rule X'': one killable mutant per clause -----------------------
    "M33-no-class-name-match": {
        "note": "r33/X'': the module-ClassDef-name branch no longer classifies (Cls receivers re-register)",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if value.id in class_names:
            return "class"
""",
             """        if False and value.id in class_names:
            return "class"
"""),
        ],
    },
    "M33-no-subscript-propagation": {
        "note": "r33/X'': the subscript branch no longer propagates its base classification",
        "edits": [
            ("plaita/core/_author_capability.py",
             """    if isinstance(value, ast.Subscript):
        base = _receiver_classify(value.value, tables, depth + 1)
        if base in ("class", "instance"):
            return base
""",
             """    if isinstance(value, ast.Subscript):
        base = _receiver_classify(value.value, tables, depth + 1)
        if False:
            return base
"""),
        ],
    },
    "M33-no-type-call": {
        "note": "r33/X'': type(self) no longer classifies as the receiver class",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if name == "type":
""",
             """        if False and name == "type":
"""),
        ],
    },
    "M33-no-lambda-callee": {
        "note": "r33/X'': a lambda callee no longer resolves to its body's receiver",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if isinstance(func, ast.Lambda):
            return _receiver_classify(func.body, tables, depth + 1)
""",
             """        if False and isinstance(func, ast.Lambda):
            return _receiver_classify(func.body, tables, depth + 1)
"""),
        ],
    },
    "M33-no-dunder-class-attrs": {
        "note": "r33/X'': __class__/__mro__/__bases__ no longer classify as a class",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        if value.attr in ("__class__", "__mro__", "__bases__"):
            return "class"
""",
             """        if False and value.attr in ("__class__", "__mro__", "__bases__"):
            return "class"
"""),
        ],
    },
    "M33-no-write-channel": {
        "note": "r33/X'': the plain-attribute state-write channel is no longer extracted",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        targets = list(sub.targets) if isinstance(sub, ast.Assign) else [sub.target]
        for target in targets:
            if isinstance(target, ast.Attribute):
                out.append(target.value)
""",
             """        targets = list(sub.targets) if isinstance(sub, ast.Assign) else [sub.target]
        for target in targets:
            if False and isinstance(target, ast.Attribute):
                out.append(target.value)
"""),
        ],
    },
    "M33-carve-out-always": {
        "note": "r33/X'': the own self/cls receiver is carved out even for method-slot shadowing",
        "edits": [
            ("plaita/core/_author_capability.py",
             """        shadowing = own is not None and bool(_own_receiver_write_attrs(sub, own) & called)
""",
             """        shadowing = False
"""),
        ],
    },
    "M33-reinstate-own-receiver-refusal": {
        "note": "r33 (re-introduces the r32 regression): the own-receiver carve-out is removed, benign OO refused again",
        "edits": [
            ("plaita/core/_author_capability.py",
             """            if (not shadowing and own is not None and isinstance(receiver, ast.Name)
                    and receiver.id == own):
                continue
""",
             """            if False:
                continue
"""),
        ],
    },
}

