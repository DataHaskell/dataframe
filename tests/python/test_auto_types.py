"""Tests for auto-type resolution and Python-type acceptance.

Goals:
  - ``hx.col("Age") >= 18.0`` works without ``type=`` annotation
  - mismatched numeric types auto-promote to double
  - ``type=float`` / ``type=int`` / ``type=str`` Python types accepted
  - ``.cast(float)`` accepts Python types"""

import polars as pl
import hyrax as hx


def test_col_no_type_resolves_against_arrow_schema(titanic):
    rb = titanic.filter(hx.col("Sex") == "female").collect()
    sex = rb.column(rb.schema.get_field_index("Sex")).to_pylist()
    assert set(sex) == {"female"}


def test_col_no_type_with_int_column(titanic):
    rb = titanic.filter(hx.col("Pclass") == 1).collect()
    pclasses = rb.column(rb.schema.get_field_index("Pclass")).to_pylist()
    assert all(p == 1 for p in pclasses)


def test_col_no_type_with_double_column(titanic):
    rb = titanic.filter(hx.col("Age") >= 18.0).collect()
    ages = rb.column(rb.schema.get_field_index("Age")).to_pylist()
    assert all(a >= 18.0 for a in ages)


def test_arithmetic_auto_promotes_int_and_double(titanic):
    """Fare is double, Pclass is int — division should auto-cast both to
    double, not error."""
    rb = (titanic
          .derive("ratio", hx.col("Fare") / hx.col("Pclass"))
          .select(["Fare", "Pclass", "ratio"])
          .limit(10).collect())
    fares = rb.column(0).to_pylist()
    pclasses = rb.column(1).to_pylist()
    ratios = rb.column(2).to_pylist()
    for f, p, r in zip(fares, pclasses, ratios):
        assert abs(r - f / p) < 1e-9


def test_python_type_in_col_kwarg_accepted(titanic):
    """``type=float`` should mean ``"double"``; ``type=str`` should mean
    ``"text"``."""
    a = titanic.filter(hx.col("Age", type=float) >= 18.0).collect().num_rows
    b = titanic.filter(hx.col("Age", type="double") >= 18.0).collect().num_rows
    assert a == b

    c = titanic.filter(hx.col("Sex", type=str) == "male").collect().num_rows
    d = titanic.filter(hx.col("Sex", type="text") == "male").collect().num_rows
    assert c == d


def test_python_type_in_lit_kwarg_accepted():
    assert hx.lit(5, type=float).out_type == "double"
    assert hx.lit(5, type=int).out_type == "int"
    assert hx.lit("x", type=str).out_type == "text"
    assert hx.lit(True, type=bool).out_type == "bool"


def test_cast_accepts_python_type(titanic):
    """``.cast(float)`` should be equivalent to ``.cast("double")``."""
    rb = (titanic
          .derive("pclass_d", hx.col("Pclass").cast(float))
          .select(["Pclass", "pclass_d"])
          .limit(3).collect())
    pclass = rb.column(0).to_pylist()
    pclass_d = rb.column(1).to_pylist()
    assert all(isinstance(p, int) for p in pclass)
    assert all(isinstance(d, float) for d in pclass_d)
    for p, d in zip(pclass, pclass_d):
        assert d == float(p)


def test_when_then_otherwise_no_type(titanic):
    """``when(...).then(...).otherwise(...)`` works without explicit types."""
    rb = (titanic
          .derive("is_adult",
                  hx.when(hx.col("Age") >= 18.0)
                    .then(hx.lit("yes"))
                    .otherwise(hx.lit("no")))
          .select(["Age", "is_adult"])
          .limit(5).collect())
    ages = rb.column(0).to_pylist()
    flags = rb.column(1).to_pylist()
    for a, f in zip(ages, flags):
        assert (f == "yes") == (a >= 18.0)


def test_unsupported_python_type_in_kwarg_raises():
    """Passing a Python type we don't have a tag for should raise TypeError."""
    try:
        hx.col("x", type=list)
    except TypeError:
        return
    assert False, "Expected TypeError for type=list"


def test_compound_predicate_no_type_hints(titanic):
    rb = titanic.filter(
        (hx.col("Sex") == "female") & (hx.col("Pclass") <= 2)
    ).collect()
    pclasses = rb.column(rb.schema.get_field_index("Pclass")).to_pylist()
    sexes = rb.column(rb.schema.get_field_index("Sex")).to_pylist()
    assert all(p <= 2 for p in pclasses)
    assert all(s == "female" for s in sexes)


def test_decision_tree_with_no_explicit_types(titanic):
    """The DT classifier's pipeline still works without explicit type
    annotations on the input frame."""
    df = (titanic
          .select(["Survived", "Pclass", "Sex", "Age", "Fare"])
          .filter(hx.col("Age") > 0.0))
    clf = hx.DecisionTreeClassifier(max_depth=3, min_samples_split=20).fit(
        df, target="Survived", target_type="int"
    )
    preds = clf.predict_array(df).to_pylist()
    truth = df.select(["Survived"]).collect().column(0).to_pylist()
    correct = sum(1 for t, p in zip(truth, preds) if t == p)
    assert correct / len(truth) > 0.7
