"""
Python bindings for the Haskell dataframe library via Arrow C Data Interface.

Usage::

    import hyrax as hx

    df  = hx.read_csv("data/titanic.csv")
    res = (df.filter(hx.col("Age") > 18)
             .derive("fare_x2", hx.col("Fare") * 2)
             .groupBy(["Sex"])
             .aggregate({
                 "survived_sum": hx.sum(hx.col("Survived")),
                 "n":            hx.count(hx.col("Sex")),
             }))
    print(res.to_pandas())

    clf = hx.DecisionTreeClassifier(max_depth=4).fit(df, target="Survived")
    preds = clf.predict(df).select(["prediction"]).collect()
"""

import ctypes
import json
import os
import weakref
import glob as _glob

_ARROW_SCHEMA_SIZE = 72   # 9 × 8 bytes
_ARROW_ARRAY_SIZE  = 80   # 10 × 8 bytes


def _find_dylib() -> str:
    """Find libdataframe-arrow.dylib / .so, preferring a copy next to this
    package and falling back to the cabal dist-newstyle build tree."""
    here = os.path.dirname(os.path.abspath(__file__))

    for ext in ("dylib", "so"):
        candidate = os.path.join(here, f"libdataframe-arrow.{ext}")
        if os.path.exists(candidate):
            return candidate

    repo_root = here
    for _ in range(6):
        repo_root = os.path.dirname(repo_root)
        patterns = [
            os.path.join(repo_root, "dist-newstyle", "**", "libdataframe-arrow.dylib"),
            os.path.join(repo_root, "dist-newstyle", "**", "libdataframe-arrow.so"),
        ]
        candidates = []
        for pat in patterns:
            candidates.extend(_glob.glob(pat, recursive=True))
        if candidates:
            return max(candidates, key=os.path.getmtime)
        if os.path.exists(os.path.join(repo_root, "dataframe.cabal")):
            break

    raise FileNotFoundError(
        "Could not locate libdataframe-arrow.dylib / .so.  "
        "Run `cabal build dataframe-arrow` first."
    )


_lib_path = _find_dylib()
_lib = ctypes.CDLL(_lib_path)

_lib.dfExecutePlan.restype = ctypes.c_int
_lib.dfExecutePlan.argtypes = [
    ctypes.c_char_p,
    ctypes.POINTER(ctypes.c_uint64),
    ctypes.POINTER(ctypes.c_uint64),
]

_lib.dfFitDecisionTree.restype = ctypes.c_int
_lib.dfFitDecisionTree.argtypes = [
    ctypes.c_char_p,                         # plan_json
    ctypes.c_char_p,                         # target_col
    ctypes.c_char_p,                         # target_type
    ctypes.c_char_p,                         # config_json
    ctypes.POINTER(ctypes.c_char_p),         # model_json_out
    ctypes.POINTER(ctypes.c_uint64),         # model_len_out
]

_lib.dfFreeModel.restype = None
_lib.dfFreeModel.argtypes = [ctypes.c_char_p]

_NUMERIC_TAGS = {
    "int", "int8", "int16", "int32", "int64",
    "word", "word8", "word16", "word32", "word64",
    "integer", "double", "float",
}

# Sentinel for "type to be resolved at execute time using the source schema".
_AUTO = "_auto"

# Python builtin types → wire-format type tags.
_PYTHON_TYPE_TAGS = {
    bool: "bool",
    int: "int",
    float: "double",
    str: "text",
}


def _normalize_type(t):
    """Accept either a wire-format type-tag string or a Python builtin type
    (``float``, ``int``, ``str``, ``bool``) and return the canonical string."""
    if t is None:
        return None
    if isinstance(t, str):
        return t
    tag = _PYTHON_TYPE_TAGS.get(t)
    if tag is None:
        raise TypeError(
            f"hyrax: unsupported type {t!r}; pass a type tag string "
            f"or one of {sorted(k.__name__ for k in _PYTHON_TYPE_TAGS)}"
        )
    return tag


def _infer_lit_type(value) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "text"
    raise TypeError(f"hyrax: cannot infer type of literal {value!r}")


def _coerce_python_value(value, type_tag: str):
    if type_tag in ("double", "float"):
        return float(value)
    if type_tag.startswith("int") or type_tag.startswith("word") or type_tag == "integer":
        return int(value)
    if type_tag == "bool":
        return bool(value)
    if type_tag in ("text", "string"):
        return str(value)
    if type_tag == "char":
        s = str(value)
        if len(s) != 1:
            raise ValueError("hyrax.lit: char must be a 1-character string")
        return s
    raise ValueError(f"hyrax.lit: unsupported type tag {type_tag!r}")


class Expr:
    """An expression IR node — a typed tree that the Haskell side decodes via
    DataFrame.IR.ExprJson. Build via :func:`col`, :func:`lit`, and operator
    overloads (e.g. ``hx.col("age") > 18``)."""

    __slots__ = ("_plan",)

    def __init__(self, plan: dict):
        self._plan = plan

    @property
    def out_type(self) -> str:
        return self._plan.get("out_type")

    # Forbid raw boolean coercion to catch `if col > 0:` mistakes early.
    def __bool__(self):
        raise TypeError(
            "Truth value of an Expr is ambiguous. Use bitwise operators "
            "(& | ~) and chain via .filter()/.derive() — not Python and/or."
        )

    # Disable hashing because __eq__ returns Expr (so the object is no longer
    # a value-typed dict key).
    __hash__ = None

    # --- Comparison ---

    def __lt__(self, other):  return _cmp("lt", self, other)
    def __le__(self, other):  return _cmp("leq", self, other)
    def __gt__(self, other):  return _cmp("gt", self, other)
    def __ge__(self, other):  return _cmp("geq", self, other)
    def __eq__(self, other):  return _cmp("eq", self, other)
    def __ne__(self, other):  return _cmp("neq", self, other)

    # --- Arithmetic ---

    def __add__(self, other):  return _arith("add",    self, other)
    def __sub__(self, other):  return _arith("sub",    self, other)
    def __mul__(self, other):  return _arith("mult",   self, other)
    def __truediv__(self, other):  return _arith("divide", self, other)

    def __radd__(self, other):  return _arith("add",    other, self)
    def __rsub__(self, other):  return _arith("sub",    other, self)
    def __rmul__(self, other):  return _arith("mult",   other, self)
    def __rtruediv__(self, other):  return _arith("divide", other, self)

    def __neg__(self):
        return Expr({
            "node": "unary",
            "op": "negate",
            "out_type": self.out_type,
            "arg_type": self.out_type,
            "arg": self._plan,
        })

    def __abs__(self):
        return Expr({
            "node": "unary",
            "op": "abs",
            "out_type": self.out_type,
            "arg_type": self.out_type,
            "arg": self._plan,
        })

    # --- Boolean (bitwise overloads stand in for and/or/not) ---

    def __and__(self, other):
        rhs = _ensure_expr(other, "bool")
        _expect_bool(self); _expect_bool(rhs)
        return Expr({
            "node": "binary", "op": "and", "out_type": "bool",
            "arg_type": "bool", "lhs": self._plan, "rhs": rhs._plan,
        })

    def __or__(self, other):
        rhs = _ensure_expr(other, "bool")
        _expect_bool(self); _expect_bool(rhs)
        return Expr({
            "node": "binary", "op": "or", "out_type": "bool",
            "arg_type": "bool", "lhs": self._plan, "rhs": rhs._plan,
        })

    def __rand__(self, other):
        return _ensure_expr(other, "bool") & self

    def __ror__(self, other):
        return _ensure_expr(other, "bool") | self

    def __invert__(self):
        _expect_bool(self)
        return Expr({
            "node": "unary", "op": "not", "out_type": "bool",
            "arg_type": "bool", "arg": self._plan,
        })

    # --- Casting helpers ---

    def cast(self, target) -> "Expr":
        """Cast a numeric column to Double via toDouble. ``target`` accepts
        either ``"double"`` / ``"float"`` or the Python types ``float``."""
        target_tag = _normalize_type(target)
        if target_tag != "double":
            raise NotImplementedError(
                f"hyrax: cast to {target_tag!r} not supported yet (only 'double')"
            )
        # If the underlying expression's type isn't yet known, defer the
        # numeric-vs-non-numeric check until resolve time. The resolver knows
        # the schema and will reject text-to-double, etc.
        if self.out_type == _AUTO:
            return Expr({
                "node": "unary", "op": "toDouble", "out_type": "double",
                "arg_type": _AUTO, "arg": self._plan,
            })
        if self.out_type not in _NUMERIC_TAGS:
            raise TypeError(f"hyrax: cannot cast non-numeric type {self.out_type!r} to double")
        if self.out_type == "double":
            return self
        return Expr({
            "node": "unary", "op": "toDouble", "out_type": "double",
            "arg_type": self.out_type, "arg": self._plan,
        })


class ColExpr(Expr):
    """A reference to a column. By default the type is resolved at execute
    time using the source frame's schema; callers can pin it explicitly via
    ``hx.col("Age", type=float)`` (Python type) or
    ``hx.col("Age", type="double")`` (string).

    The legacy ``_name`` attribute is retained so existing aggregation code
    (``hx.sum(hx.col("x"))``) keeps working."""

    __slots__ = ("_name",)

    def __init__(self, name: str, out_type: str = _AUTO):
        super().__init__({"node": "col", "out_type": out_type, "name": name})
        self._name = name


def col(name: str, type=None) -> "ColExpr":
    """Reference a column by name. By default the type is inferred from the
    source frame's Arrow schema at execute time; pass ``type=float`` (Python
    type) or ``type="text"`` (string) to override."""
    tag = _normalize_type(type) if type is not None else _AUTO
    return ColExpr(name, tag)


def lit(value, type=None) -> Expr:
    """Wrap a Python value as a literal expression.

    ``type`` is inferred from the Python value (bool→bool, int→int,
    float→double, str→text). Override with either a Python type
    (``lit(5, type=float)``) or a wire-format string (``lit(5, type="double")``)."""
    tag = _normalize_type(type) if type is not None else _infer_lit_type(value)
    return Expr({"node": "lit", "out_type": tag, "value": _coerce_python_value(value, tag)})


def when(cond: Expr) -> "_When":
    """Start an if-then-else chain: ``when(cond).then(a).otherwise(b)``."""
    _expect_bool(_ensure_expr(cond))
    return _When(_ensure_expr(cond))


class _When:
    def __init__(self, cond: Expr):
        self._cond = cond

    def then(self, value) -> "_Then":
        return _Then(self._cond, value)


class _Then:
    def __init__(self, cond: Expr, then_value):
        self._cond = cond
        self._then_value = then_value

    def otherwise(self, default) -> Expr:
        then_e = _ensure_expr(self._then_value)
        else_e = _ensure_expr(default, then_e.out_type)
        if then_e.out_type != else_e.out_type:
            raise TypeError(
                f"hyrax.when/otherwise: branch types differ — "
                f"then={then_e.out_type!r}, else={else_e.out_type!r}"
            )
        return Expr({
            "node": "if", "out_type": then_e.out_type,
            "cond": self._cond._plan,
            "then": then_e._plan,
            "else": else_e._plan,
        })


def _ensure_expr(value, hint=None) -> Expr:
    """Coerce a Python value into an Expr, lifting raw values via :func:`lit`."""
    if isinstance(value, Expr):
        return value
    return lit(value, type=hint)


def _expect_bool(expr: Expr):
    """Filter/and/or/not require Bool. Allow ``_AUTO`` because the actual
    type will be resolved at execute time using the source schema."""
    if expr.out_type not in ("bool", _AUTO):
        raise TypeError(
            f"hyrax: expected Expr Bool but got out_type={expr.out_type!r}"
        )


def _cmp(op: str, lhs, rhs) -> Expr:
    """Build a comparison binary op (out_type = bool, arg_type may be auto)."""
    lhs_e, rhs_e, arg_type = _coerce_pair(lhs, rhs)
    return Expr({
        "node": "binary", "op": op, "out_type": "bool",
        "arg_type": arg_type,
        "lhs": lhs_e._plan, "rhs": rhs_e._plan,
    })


def _arith(op: str, lhs, rhs) -> Expr:
    """Build an arithmetic binary op. If both sides have known numeric types
    we typecheck eagerly; if either is ``_AUTO`` we defer the check to
    execute-time resolution."""
    lhs_e, rhs_e, arg_type = _coerce_pair(lhs, rhs)
    if arg_type != _AUTO:
        if arg_type not in _NUMERIC_TAGS:
            raise TypeError(
                f"hyrax: arithmetic op {op!r} requires numeric arg_type, got {arg_type!r}"
            )
        if op == "divide" and arg_type not in ("double", "float"):
            # Promote to double so int/int division produces a Double.
            lhs_e = lhs_e.cast("double")
            rhs_e = rhs_e.cast("double")
            arg_type = "double"
    return Expr({
        "node": "binary", "op": op,
        "out_type": arg_type,  # = _AUTO if deferred
        "arg_type": arg_type,
        "lhs": lhs_e._plan, "rhs": rhs_e._plan,
    })


def _coerce_pair(lhs, rhs):
    """Make sure both sides are Expr; if one side is a Python literal, lift
    it. Returns ``(lhs_expr, rhs_expr, common_arg_type)``. The common type is
    ``_AUTO`` whenever either side is auto or the two sides disagree — the
    resolver resolves both at execute time."""
    lhs_is_expr = isinstance(lhs, Expr)
    rhs_is_expr = isinstance(rhs, Expr)
    if lhs_is_expr and rhs_is_expr:
        lt, rt = lhs.out_type, rhs.out_type
        if lt == _AUTO or rt == _AUTO:
            return lhs, rhs, _AUTO
        if lt != rt:
            return lhs, rhs, _AUTO  # let the resolver figure out promotion
        return lhs, rhs, lt
    if lhs_is_expr:
        if lhs.out_type == _AUTO:
            return lhs, lit(rhs), _AUTO
        rhs_e = lit(rhs, type=lhs.out_type)
        return lhs, rhs_e, lhs.out_type
    if rhs_is_expr:
        if rhs.out_type == _AUTO:
            return lit(lhs), rhs, _AUTO
        lhs_e = lit(lhs, type=rhs.out_type)
        return lhs_e, rhs, rhs.out_type
    raise TypeError("hyrax: at least one operand must be an Expr")


class AggExpr:
    """Aggregation expression for use in ``GroupedEagerFrame.aggregate``.
    Distinct from :class:`Expr` — group-by aggregates use a flat
    ``{name, agg, col}`` JSON shape, not the recursive expression IR."""

    __slots__ = ("_fn", "_col")

    def __init__(self, fn: str, c: ColExpr):
        if not isinstance(c, ColExpr):
            raise TypeError(
                f"hyrax: aggregation requires hx.col(...), got {type(c).__name__}"
            )
        self._fn = fn
        self._col = c._name


def sum(expr: ColExpr) -> AggExpr:
    """Sum aggregation."""
    return AggExpr("sum", expr)


def mean(expr: ColExpr) -> AggExpr:
    """Mean aggregation."""
    return AggExpr("mean", expr)


def count(expr: ColExpr) -> AggExpr:
    """Count aggregation."""
    return AggExpr("count", expr)


def min(expr: ColExpr) -> AggExpr:
    """Minimum value (preserves column type)."""
    return AggExpr("min", expr)


def max(expr: ColExpr) -> AggExpr:
    """Maximum value (preserves column type)."""
    return AggExpr("max", expr)


def median(expr: ColExpr) -> AggExpr:
    """Median (returns Double)."""
    return AggExpr("median", expr)


def variance(expr: ColExpr) -> AggExpr:
    """Variance (returns Double)."""
    return AggExpr("variance", expr)


def std(expr: ColExpr) -> AggExpr:
    """Standard deviation = sqrt(variance) (returns Double)."""
    return AggExpr("std", expr)


class EagerFrame:
    """Holds a query plan dict; terminal operations (``collect``,
    ``aggregate``, classifier ``fit``) execute it immediately."""

    def __init__(self, plan: dict, _refs=None):
        self._plan = plan
        self._refs = list(_refs or [])

    def select(self, cols: list) -> "EagerFrame":
        return EagerFrame(
            {"op": "Select", "cols": list(cols), "input": self._plan},
            self._refs,
        )

    def sort(self, cols: list, ascending: bool = True) -> "EagerFrame":
        return EagerFrame(
            {
                "op": "Sort",
                "cols": list(cols),
                "ascending": bool(ascending),
                "input": self._plan,
            },
            self._refs,
        )

    def limit(self, n: int) -> "EagerFrame":
        return EagerFrame(
            {"op": "Limit", "n": int(n), "input": self._plan},
            self._refs,
        )

    def filter(self, predicate: Expr) -> "EagerFrame":
        """Keep rows where ``predicate`` evaluates to True. The predicate must
        have ``out_type='bool'``."""
        if not isinstance(predicate, Expr):
            raise TypeError("hyrax: filter predicate must be an Expr")
        _expect_bool(predicate)
        return EagerFrame(
            {"op": "Filter", "predicate": predicate._plan, "input": self._plan},
            self._refs,
        )

    def derive(self, name: str, expr: Expr) -> "EagerFrame":
        """Add (or replace) a column ``name`` computed from ``expr``."""
        if not isinstance(expr, Expr):
            raise TypeError("hyrax: derive expression must be an Expr")
        return EagerFrame(
            {"op": "Derive", "name": str(name), "expr": expr._plan, "input": self._plan},
            self._refs,
        )

    # Polars-style alias
    def with_column(self, name: str, expr: Expr) -> "EagerFrame":
        return self.derive(name, expr)

    def exclude(self, cols: list) -> "EagerFrame":
        """Drop the named columns."""
        return EagerFrame(
            {"op": "Exclude", "cols": list(cols), "input": self._plan},
            self._refs,
        )

    def rename(self, pairs) -> "EagerFrame":
        """Rename columns. ``pairs`` may be a dict ``{old: new}`` or a list
        of ``(old, new)`` tuples."""
        if isinstance(pairs, dict):
            pairs_list = list(pairs.items())
        else:
            pairs_list = [list(p) for p in pairs]
        return EagerFrame(
            {"op": "Rename", "pairs": pairs_list, "input": self._plan},
            self._refs,
        )

    def distinct(self) -> "EagerFrame":
        """Drop duplicate rows."""
        return EagerFrame({"op": "Distinct", "input": self._plan}, self._refs)

    def tail(self, n: int) -> "EagerFrame":
        """Take the last n rows."""
        return EagerFrame(
            {"op": "TakeLast", "n": int(n), "input": self._plan}, self._refs
        )

    def drop_first(self, n: int) -> "EagerFrame":
        """Drop the first n rows. Named ``drop_first`` (not ``drop``) to avoid
        confusing it with column-dropping; use :meth:`exclude` to drop columns."""
        return EagerFrame(
            {"op": "Drop", "n": int(n), "input": self._plan}, self._refs
        )

    def drop_last(self, n: int) -> "EagerFrame":
        """Drop the last n rows."""
        return EagerFrame(
            {"op": "DropLast", "n": int(n), "input": self._plan}, self._refs
        )

    def slice(self, start: int, end: int) -> "EagerFrame":
        """Half-open row range ``[start, end)``."""
        return EagerFrame(
            {"op": "Range", "start": int(start), "end": int(end), "input": self._plan},
            self._refs,
        )

    def describe(self) -> "EagerFrame":
        """Per-column summary statistics (count, min, max, mean, std, …)."""
        return EagerFrame({"op": "Describe", "input": self._plan}, self._refs)

    def correlation(self, first: str, second: str) -> "EagerFrame":
        """Pearson correlation between two columns (returns a 1-row frame
        with columns ``first``, ``second``, ``correlation``)."""
        return EagerFrame(
            {
                "op": "Correlation",
                "first": str(first),
                "second": str(second),
                "input": self._plan,
            },
            self._refs,
        )

    def frequencies(self, col_name: str) -> "EagerFrame":
        """Value-counts for the given column. Returns a frame with columns
        ``<col_name>`` and ``count``. Implemented as a groupBy+count to keep
        the result schema Arrow-friendly."""
        return EagerFrame(
            {
                "op": "GroupBy",
                "keys": [str(col_name)],
                "aggregations": [
                    {"name": "count", "agg": "count", "col": str(col_name)},
                ],
                "input": self._plan,
            },
            self._refs,
        )

    def join(
        self,
        other: "EagerFrame",
        on: list,
        how: str = "inner",
    ) -> "EagerFrame":
        """SQL-style join on the shared key columns ``on``. ``how`` is one of
        ``"inner"`` (default), ``"left"``, ``"right"``, ``"outer"``."""
        if not isinstance(other, EagerFrame):
            raise TypeError("hyrax: join() requires another EagerFrame")
        if how not in ("inner", "left", "right", "outer", "full_outer"):
            raise ValueError(
                f"hyrax: unknown join type {how!r}; "
                f"expected inner|left|right|outer"
            )
        return EagerFrame(
            {
                "op": "Join",
                "how": how,
                "on": list(on),
                "left": self._plan,
                "right": other._plan,
            },
            list(self._refs) + list(other._refs),
        )

    def groupBy(self, keys: list) -> "GroupedEagerFrame":
        return GroupedEagerFrame(self._plan, list(keys), self._refs)

    def collect(self):
        """Execute the plan and return a pyarrow.RecordBatch."""
        return _execute(self._plan, self._refs)

    def write_csv(self, path: str) -> None:
        """Materialize the frame to a CSV file. Returns ``None`` (terminal op)."""
        plan = {"op": "WriteCsv", "path": str(path), "input": self._plan}
        _execute(plan, self._refs)


class GroupedEagerFrame:
    """Holds a plan + grouping keys; aggregate() executes immediately."""

    def __init__(self, plan: dict, keys: list, _refs=None):
        self._plan = plan
        self._keys = keys
        self._refs = _refs or []

    def aggregate(self, aggs: dict):
        """Run aggregation and return a pyarrow.RecordBatch.

        Parameters
        ----------
        aggs:
            ``{output_name: AggExpr}`` mapping, e.g.
            ``{"total": hx.sum(hx.col("Amount"))}``.
        """
        agg_list = [
            {"name": k, "agg": v._fn, "col": v._col}
            for k, v in aggs.items()
        ]
        plan = {
            "op": "GroupBy",
            "keys": self._keys,
            "aggregations": agg_list,
            "input": self._plan,
        }
        return _execute(plan, self._refs)


def _execute(plan: dict, _refs=None):
    """Serialize *plan* to JSON, call Haskell, and return a RecordBatch.

    Resolves ``_AUTO`` types in embedded expressions using the source
    schema, then re-exports any ``FromArrow`` nodes into fresh Arrow C Data
    Interface buffers (Arrow's ABI invalidates buffers after import, so a
    plan executed twice would otherwise read stale memory)."""
    import pyarrow as pa

    resolved, _ = _resolve_plan(plan)
    fresh_refs: list = []
    resolved = _resolve_from_arrow(resolved, fresh_refs)

    plan_bytes = json.dumps(resolved).encode()
    schema_ptr = ctypes.c_uint64(0)
    array_ptr = ctypes.c_uint64(0)

    rc = _lib.dfExecutePlan(
        plan_bytes,
        ctypes.byref(schema_ptr),
        ctypes.byref(array_ptr),
    )
    if rc != 0:
        raise RuntimeError("dfExecutePlan failed (check stderr for details)")

    result = pa.RecordBatch._import_from_c(array_ptr.value, schema_ptr.value)
    # Keep fresh_refs and _refs alive until after the import completes.
    del fresh_refs, _refs
    return result


# Lookup table: arrow_handle -> RecordBatch (Python side). Plans carry the
# handle, not the live buffer addresses; addresses are re-allocated on every
# execution so the same EagerFrame can be materialized multiple times.
#
# The dict only weakly references the RecordBatch — the EagerFrame's _refs
# list keeps the rb alive while the frame exists, and the entry is GC'd once
# every Python-side reference is dropped.
_arrow_handles: "weakref.WeakValueDictionary[int, object]" = weakref.WeakValueDictionary()
_arrow_handle_seq = [0]


def _register_arrow(rb) -> int:
    """Register a RecordBatch and return a stable integer handle."""
    handle = _arrow_handle_seq[0]
    _arrow_handle_seq[0] += 1
    _arrow_handles[handle] = rb
    return handle


def _resolve_from_arrow(plan, fresh_refs: list):
    """Walk *plan*; for each ``FromArrow`` node carrying ``"_arrow_handle"``,
    allocate fresh schema/array buffers and re-export the underlying
    RecordBatch into them. Returns a deep copy with the addresses substituted."""
    if isinstance(plan, dict):
        if plan.get("op") == "FromArrow" and "_arrow_handle" in plan:
            handle = plan["_arrow_handle"]
            rb = _arrow_handles.get(handle)
            if rb is None:
                raise RuntimeError(
                    f"hyrax: Arrow handle {handle} is no longer alive — "
                    "the EagerFrame backing this plan was garbage-collected"
                )
            schema_buf = ctypes.create_string_buffer(_ARROW_SCHEMA_SIZE)
            array_buf = ctypes.create_string_buffer(_ARROW_ARRAY_SIZE)
            schema_addr = ctypes.addressof(schema_buf)
            array_addr = ctypes.addressof(array_buf)
            rb._export_to_c(array_addr, schema_addr)
            fresh_refs.extend([schema_buf, array_buf])
            return {"op": "FromArrow", "schema": schema_addr, "array": array_addr}
        return {k: _resolve_from_arrow(v, fresh_refs) for k, v in plan.items()}
    if isinstance(plan, list):
        return [_resolve_from_arrow(x, fresh_refs) for x in plan]
    return plan


# pyarrow type-string -> wire-format type tag
_ARROW_TYPE_MAP = {
    "int8": "int8", "int16": "int16", "int32": "int32", "int64": "int",
    "uint8": "word8", "uint16": "word16", "uint32": "word32", "uint64": "word64",
    "float": "float", "double": "double", "halffloat": "float",
    "string": "text", "large_string": "text", "binary": "bytestring",
    "bool": "bool",
}


def _arrow_schema_to_tags(arrow_schema):
    """Turn a ``pyarrow.Schema`` into a ``{name: type-tag}`` dict.
    Unknown Arrow types map to ``None`` so callers can detect and skip them."""
    out = {}
    for field in arrow_schema:
        out[field.name] = _ARROW_TYPE_MAP.get(str(field.type))
    return out


# Cache for ReadCsv/ReadJson/ReadParquet schema peeks — keyed by op + path.
_peek_cache: dict = {}


def _peek_read_schema(plan: dict) -> dict:
    """Materialize a zero-row version of a Read* plan to get its schema."""
    op = plan.get("op")
    path = plan.get("path")
    cache_key = (op, path)
    if cache_key in _peek_cache:
        return _peek_cache[cache_key]
    zero = {"op": "Limit", "n": 0, "input": plan}
    rb = _execute(zero, [])
    schema = _arrow_schema_to_tags(rb.schema)
    _peek_cache[cache_key] = schema
    return schema


def _resolve_plan(plan: dict):
    """Recursively walk *plan*, computing each subplan's output schema and
    resolving any ``_AUTO`` types in embedded expressions. Returns a tuple
    ``(resolved_plan, output_schema)``."""
    op = plan.get("op")

    # ----- Source ops -----
    if op == "FromArrow":
        if "_arrow_handle" in plan:
            rb = _arrow_handles.get(plan["_arrow_handle"])
            if rb is not None:
                return plan, _arrow_schema_to_tags(rb.schema)
        return plan, {}

    if op in ("ReadCsv", "ReadTsv", "ReadJson", "ReadParquet"):
        try:
            schema = _peek_read_schema(plan)
        except Exception:
            schema = {}  # peek failed — auto cols will fall back to "double"
        return plan, schema

    if op in ("ScanCsv", "ScanParquet"):
        schema = {c: t for c, t in plan.get("schema", [])}
        return plan, schema

    # ----- Plan-tree ops -----
    if op == "Filter":
        ch_resolved, ch_schema = _resolve_plan(plan["input"])
        pred_resolved, _ = _resolve_expr(plan["predicate"], ch_schema)
        return {**plan, "input": ch_resolved, "predicate": pred_resolved}, ch_schema

    if op == "Derive":
        ch_resolved, ch_schema = _resolve_plan(plan["input"])
        expr_resolved, expr_type = _resolve_expr(plan["expr"], ch_schema)
        out_schema = dict(ch_schema)
        out_schema[plan["name"]] = expr_type
        return {**plan, "input": ch_resolved, "expr": expr_resolved}, out_schema

    if op == "Select":
        ch_resolved, ch_schema = _resolve_plan(plan["input"])
        out_schema = {c: ch_schema.get(c) for c in plan["cols"]}
        return {**plan, "input": ch_resolved}, out_schema

    if op == "Exclude":
        ch_resolved, ch_schema = _resolve_plan(plan["input"])
        excl = set(plan["cols"])
        out_schema = {k: v for k, v in ch_schema.items() if k not in excl}
        return {**plan, "input": ch_resolved}, out_schema

    if op == "Rename":
        ch_resolved, ch_schema = _resolve_plan(plan["input"])
        rn = {old: new for old, new in plan["pairs"]}
        out_schema = {rn.get(k, k): v for k, v in ch_schema.items()}
        return {**plan, "input": ch_resolved}, out_schema

    if op in ("Distinct", "Sort", "Limit", "TakeLast", "Drop", "DropLast", "Range", "WriteCsv"):
        ch_resolved, ch_schema = _resolve_plan(plan["input"])
        return {**plan, "input": ch_resolved}, ch_schema

    if op == "Join":
        l_resolved, l_schema = _resolve_plan(plan["left"])
        r_resolved, r_schema = _resolve_plan(plan["right"])
        out_schema = {**l_schema, **r_schema}
        return {**plan, "left": l_resolved, "right": r_resolved}, out_schema

    if op == "GroupBy":
        ch_resolved, ch_schema = _resolve_plan(plan["input"])
        out_schema = {k: ch_schema.get(k) for k in plan["keys"]}
        for a in plan["aggregations"]:
            col_t = ch_schema.get(a["col"])
            out_schema[a["name"]] = _agg_out_type(a["agg"], col_t)
        return {**plan, "input": ch_resolved}, out_schema

    if op in ("Describe", "Frequencies", "Correlation"):
        ch_resolved, _ = _resolve_plan(plan["input"])
        # Output schema is dynamic / heterogeneous — leave unspecified.
        return {**plan, "input": ch_resolved}, {}

    return plan, {}


def _agg_out_type(fn: str, col_type) -> str | None:
    """Output type of an aggregation given its input column type."""
    if fn == "count":
        return "int"
    if fn in ("sum", "min", "max"):
        return col_type
    if fn in ("mean", "median", "variance", "std"):
        return "double"
    return col_type


def _resolve_expr(expr: dict, schema: dict):
    """Recursively walk an expression dict, resolving ``_AUTO`` out_type and
    arg_type fields using *schema* (column-name → type-tag). Returns a tuple
    ``(resolved_expr, out_type)``."""
    node = expr.get("node")

    if node == "col":
        name = expr["name"]
        declared = expr.get("out_type")
        actual = schema.get(name)
        if declared in (_AUTO, None):
            # If the source schema doesn't pin a type, fall back to double —
            # works for the common ML case and surfaces a clear error from
            # Haskell if the column actually isn't numeric.
            resolved = actual or "double"
            return {**expr, "out_type": resolved}, resolved
        # Explicit declaration — if the actual is numeric and differs, cast.
        if actual is not None and actual != declared:
            if declared == "double" and actual in _NUMERIC_TAGS:
                base = {**expr, "out_type": actual}
                return {
                    "node": "unary", "op": "toDouble", "out_type": "double",
                    "arg_type": actual, "arg": base,
                }, "double"
        return expr, declared

    if node == "lit":
        out_type = expr.get("out_type")
        if out_type == _AUTO:
            out_type = _infer_lit_type(expr["value"])
            return ({**expr, "out_type": out_type,
                     "value": _coerce_python_value(expr["value"], out_type)}), out_type
        return expr, out_type

    if node == "unary":
        a_resolved, a_type = _resolve_expr(expr["arg"], schema)
        op = expr["op"]
        if op == "not":
            out_type = "bool"
        elif op == "toDouble":
            out_type = "double"
            # If the underlying expression isn't numeric, surface early.
            if a_type not in _NUMERIC_TAGS:
                raise TypeError(
                    f"hyrax: cannot cast non-numeric type {a_type!r} to double"
                )
        else:
            out_type = a_type
        return ({**expr, "arg": a_resolved, "arg_type": a_type, "out_type": out_type}, out_type)

    if node == "binary":
        l_resolved, l_type = _resolve_expr(expr["lhs"], schema)
        r_resolved, r_type = _resolve_expr(expr["rhs"], schema)
        op = expr["op"]
        # Promote mismatched types
        if l_type != r_type:
            common = _promote_types(l_type, r_type)
            if common is None:
                raise TypeError(
                    f"hyrax: cannot reconcile types in {op!r}: {l_type} vs {r_type}"
                )
            l_resolved = _cast_expr(l_resolved, l_type, common)
            r_resolved = _cast_expr(r_resolved, r_type, common)
            arg_type = common
        else:
            arg_type = l_type
        # Result type by op
        if op in ("eq", "neq", "lt", "leq", "gt", "geq", "and", "or"):
            out_type = "bool"
        elif op == "divide" and arg_type not in ("double", "float"):
            l_resolved = _cast_expr(l_resolved, arg_type, "double")
            r_resolved = _cast_expr(r_resolved, arg_type, "double")
            arg_type = "double"
            out_type = "double"
        elif op in ("add", "sub", "mult", "divide", "exponentiate"):
            out_type = arg_type
        else:
            out_type = expr.get("out_type", arg_type)
            if out_type == _AUTO:
                out_type = arg_type
        return ({**expr, "lhs": l_resolved, "rhs": r_resolved,
                 "arg_type": arg_type, "out_type": out_type}, out_type)

    if node == "if":
        c_resolved, _c = _resolve_expr(expr["cond"], schema)
        t_resolved, t_type = _resolve_expr(expr["then"], schema)
        e_resolved, e_type = _resolve_expr(expr["else"], schema)
        if t_type != e_type:
            common = _promote_types(t_type, e_type)
            if common is None:
                raise TypeError(
                    f"hyrax: when/otherwise branches differ: {t_type} vs {e_type}"
                )
            t_resolved = _cast_expr(t_resolved, t_type, common)
            e_resolved = _cast_expr(e_resolved, e_type, common)
            out_type = common
        else:
            out_type = t_type
        return ({**expr, "cond": c_resolved, "then": t_resolved,
                 "else": e_resolved, "out_type": out_type}, out_type)

    return expr, expr.get("out_type")


def _promote_types(a, b):
    """Find a common type both sides can be cast to. We only have ``toDouble``
    in the wire format, so any numeric mismatch ends up as double."""
    if a == b:
        return a
    if a in _NUMERIC_TAGS and b in _NUMERIC_TAGS:
        return "double"
    return None


def _cast_expr(expr: dict, from_type, to_type) -> dict:
    """Wrap *expr* in a cast if needed. Currently only ``toDouble`` is
    supported in the wire format."""
    if from_type == to_type:
        return expr
    if to_type == "double" and from_type in _NUMERIC_TAGS:
        return {
            "node": "unary", "op": "toDouble", "out_type": "double",
            "arg_type": from_type, "arg": expr,
        }
    raise TypeError(f"hyrax: don't know how to cast {from_type!r} → {to_type!r}")


_ARROW_FORMAT_TO_TAG = {
    # Integers
    "c": "int8", "C": "word8",
    "s": "int16", "S": "word16",
    "i": "int32", "I": "word32",
    "l": "int64", "L": "word64",
    # Floats
    "f": "float", "g": "double", "e": "float",  # half-float maps to float
    # Strings
    "u": "text", "U": "text",
    "z": "bytestring", "Z": "bytestring",
    # Bool
    "b": "bool",
    # Date / time / timestamp etc. — not yet supported as DT target types
}


def _peek_target_type(plan: dict, _refs, target: str) -> str:
    """Materialize a zero-row version of ``plan`` to introspect the target
    column's Arrow type; map to a wire-format tag."""
    zero_row_plan = {"op": "Limit", "n": 0, "input": plan}
    rb = _execute(zero_row_plan, _refs)
    if target not in rb.schema.names:
        raise KeyError(f"hyrax: target column {target!r} not found in schema")
    field = rb.schema.field(target)
    arrow_fmt = field.type.format if hasattr(field.type, "format") else None

    pa_type = field.type
    type_str = str(pa_type)
    # PyArrow types — fall back to string-matching since arrow_fmt isn't always set
    mapping = {
        "int8": "int8", "int16": "int16", "int32": "int32", "int64": "int64",
        "uint8": "word8", "uint16": "word16", "uint32": "word32", "uint64": "word64",
        "float": "float", "double": "double", "halffloat": "float",
        "string": "text", "large_string": "text", "binary": "bytestring",
        "bool": "bool",
    }
    if type_str in mapping:
        return mapping[type_str]
    raise TypeError(
        f"hyrax: cannot infer wire-format type for target column {target!r} "
        f"with Arrow type {type_str!r}"
    )


class DecisionTreeClassifier:
    """A sklearn-style wrapper around the TAO decision-tree trainer in
    `DataFrame.DecisionTree`. ``fit`` runs the trainer on the materialized
    input frame and stores the trained tree as a JSON expression; ``predict``
    applies that expression to a (possibly different) frame and returns an
    ``EagerFrame`` carrying a ``"prediction"`` column.

    Hyperparameters mirror the Haskell ``TreeConfig``."""

    def __init__(
        self,
        max_depth: int = 4,
        min_samples_split: int = 5,
        min_leaf_size: int = 1,
        percentiles: tuple = tuple(range(0, 101, 10)),
        expression_pairs: int = 10,
        tao_iterations: int = 10,
        tao_convergence_tol: float = 1e-6,
        max_expr_depth: int = 2,
        bool_expansion: int = 2,
        complexity_penalty: float = 0.05,
        enable_string_ops: bool = True,
        enable_cross_cols: bool = True,
        enable_arith_ops: bool = True,
    ):
        self._cfg = {
            "max_depth": int(max_depth),
            "min_samples_split": int(min_samples_split),
            "min_leaf_size": int(min_leaf_size),
            "percentiles": [int(p) for p in percentiles],
            "expression_pairs": int(expression_pairs),
            "tao_iterations": int(tao_iterations),
            "tao_convergence_tol": float(tao_convergence_tol),
            "max_expr_depth": int(max_expr_depth),
            "bool_expansion": int(bool_expansion),
            "complexity_penalty": float(complexity_penalty),
            "enable_string_ops": bool(enable_string_ops),
            "enable_cross_cols": bool(enable_cross_cols),
            "enable_arith_ops": bool(enable_arith_ops),
        }
        self._tree_json: str | None = None
        self._target: str | None = None
        self._target_type: str | None = None

    @property
    def tree_json(self) -> str:
        if self._tree_json is None:
            raise RuntimeError("DecisionTreeClassifier: must call .fit() first")
        return self._tree_json

    def fit(
        self,
        df: EagerFrame,
        target: str,
        target_type: str = "auto",
    ) -> "DecisionTreeClassifier":
        if not isinstance(df, EagerFrame):
            raise TypeError("hyrax: fit() expects an EagerFrame")

        # Resolve _AUTO types, then re-export any FromArrow nodes into fresh
        # buffers (Arrow ABI invalidates them after import); ship to Haskell.
        type_resolved, _ = _resolve_plan(df._plan)
        fresh_refs: list = []
        resolved_plan = _resolve_from_arrow(type_resolved, fresh_refs)
        plan_bytes = json.dumps(resolved_plan).encode()
        cfg_bytes = json.dumps(self._cfg).encode()

        out_ptr = ctypes.c_char_p()
        out_len = ctypes.c_uint64(0)

        # Keep _refs and fresh_refs alive across the FFI call so the buffers
        # they reference don't get freed mid-fit.
        _refs_alive = df._refs
        try:
            rc = _lib.dfFitDecisionTree(
                plan_bytes,
                target.encode(),
                target_type.encode(),
                cfg_bytes,
                ctypes.byref(out_ptr),
                ctypes.byref(out_len),
            )
            if rc != 0:
                raise RuntimeError(
                    "DecisionTreeClassifier.fit: dfFitDecisionTree failed "
                    "(see stderr for details)"
                )
            tree_bytes = ctypes.string_at(out_ptr, out_len.value)
            tree_str = tree_bytes.decode("utf-8")
        finally:
            if out_ptr.value is not None:
                _lib.dfFreeModel(out_ptr)
            del _refs_alive, fresh_refs  # noqa: F841

        self._tree_json = tree_str
        tree_obj = json.loads(tree_str)
        self._target_type = tree_obj.get("out_type")
        self._target = target
        return self

    def predict(self, df: EagerFrame, name: str = "prediction") -> EagerFrame:
        """Append a prediction column to ``df`` by applying the trained tree
        as a Derive expression. Returns an ``EagerFrame`` so callers can chain
        ``.select([name])``, ``.collect()``, etc."""
        if self._tree_json is None:
            raise RuntimeError("DecisionTreeClassifier: must call .fit() before .predict()")
        plan = {
            "op": "Derive",
            "name": name,
            "expr": json.loads(self._tree_json),
            "input": df._plan,
        }
        return EagerFrame(plan, _refs=df._refs)

    def predict_array(self, df: EagerFrame, name: str = "prediction"):
        """Convenience: project the prediction column out of the result and
        return it as a ``pyarrow.Array``."""
        rb = self.predict(df, name).select([name]).collect()
        return rb.column(0)

## | Top-level constructors.
def read_csv(path: str) -> EagerFrame:
    """Create an EagerFrame that reads a CSV file."""
    return EagerFrame({"op": "ReadCsv", "path": path})


def read_tsv(path: str) -> EagerFrame:
    """Create an EagerFrame that reads a TSV file."""
    return EagerFrame({"op": "ReadTsv", "path": path})


def read_parquet(path: str) -> EagerFrame:
    """Create an EagerFrame that reads a Parquet file."""
    return EagerFrame({"op": "ReadParquet", "path": path})


def read_json(path: str) -> EagerFrame:
    """Create an EagerFrame that reads a JSON file (one record per line, or
    a JSON array of objects)."""
    return EagerFrame({"op": "ReadJson", "path": path})


_VALID_SCAN_TYPES = {
    "int", "int8", "int16", "int32", "int64",
    "double", "float", "bool", "text", "string",
}


def _normalize_schema(schema) -> list:
    """Accept ``{col: type}`` (dict) or ``[(col, type), ...]`` (iterable);
    return a list of [col, type] pairs validated against the wire-format type
    tags supported by the lazy engine."""
    if isinstance(schema, dict):
        items = list(schema.items())
    else:
        items = [(c, t) for c, t in schema]
    out = []
    for c, t in items:
        if t not in _VALID_SCAN_TYPES:
            raise ValueError(
                f"hyrax.scan_*: unsupported schema type {t!r} for column {c!r}; "
                f"expected one of {sorted(_VALID_SCAN_TYPES)}"
            )
        out.append([str(c), str(t)])
    return out


def scan_csv(path: str, schema) -> EagerFrame:
    """Stream-scan a CSV file via the lazy engine. ``schema`` is a
    ``{col: type}`` dict or list of ``(col, type)`` pairs covering every
    column the query will need; columns not listed are not read.

    The lazy engine uses the schema to skip parsing unwanted columns and
    streams batches through the result. Downstream operations currently
    materialize before being applied — this is still useful for big files
    where column projection at scan time is the main win."""
    return EagerFrame(
        {"op": "ScanCsv", "path": str(path), "schema": _normalize_schema(schema)}
    )


def scan_parquet(path: str, schema) -> EagerFrame:
    """Stream-scan a Parquet file via the lazy engine. See :func:`scan_csv`
    for the schema format and the same caveats about downstream ops."""
    return EagerFrame(
        {"op": "ScanParquet", "path": str(path), "schema": _normalize_schema(schema)}
    )


def from_arrow(arrow_obj) -> EagerFrame:
    """Create an EagerFrame from any Arrow-compatible object.

    Parameters
    ----------
    arrow_obj:
        A pyarrow RecordBatch or Table (or any object with _export_to_c).
        Use ``polars_df.to_arrow()`` or ``pandas_df.to_arrow()`` to convert first.

    The frame can be materialized multiple times (e.g. fit() then predict()) —
    each execution re-exports the underlying RecordBatch into fresh Arrow C
    Data Interface buffers, since the protocol invalidates buffers after the
    first import.
    """
    import pyarrow as pa
    if isinstance(arrow_obj, pa.Table):
        rb = arrow_obj.combine_chunks().to_batches()[0]
    else:
        rb = arrow_obj
    handle = _register_arrow(rb)
    plan = {"op": "FromArrow", "_arrow_handle": handle}
    return EagerFrame(plan, _refs=[rb])
