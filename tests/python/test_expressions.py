"""Round-trip tests for the Expression IR through the FFI boundary."""

import hyrax as hx


def test_filter_equality_text(titanic):
    df = titanic.filter(hx.col("Sex", type="text") == "female")
    rb = df.collect()
    sex_col = rb.column(rb.schema.get_field_index("Sex"))
    assert set(sex_col.to_pylist()) == {"female"}
    assert rb.num_rows > 200


def test_filter_numeric_compound(titanic):
    df = titanic.filter(
        (hx.col("Age", type="double") >= 18.0)
        & (hx.col("Pclass", type="int") == 1)
    )
    rb = df.collect()
    age_idx = rb.schema.get_field_index("Age")
    pclass_idx = rb.schema.get_field_index("Pclass")
    ages = rb.column(age_idx).to_pylist()
    pclasses = rb.column(pclass_idx).to_pylist()
    assert all(a >= 18.0 for a in ages)
    assert all(p == 1 for p in pclasses)
    assert rb.num_rows > 0


def test_derive_arithmetic(titanic):
    df = titanic.derive("fare_double", hx.col("Fare", type="double") * 2.0)
    rb = df.select(["Fare", "fare_double"]).collect()
    fares = rb.column(0).to_pylist()
    doubled = rb.column(1).to_pylist()
    for a, b in zip(fares, doubled):
        assert abs(b - 2 * a) < 1e-9


def test_derive_when_then_otherwise(titanic):
    df = titanic.derive(
        "is_child",
        hx.when(hx.col("Age", type="double") < 18.0)
            .then(hx.lit("yes"))
            .otherwise(hx.lit("no")),
    )
    rb = df.select(["Age", "is_child"]).collect()
    ages = rb.column(0).to_pylist()
    flags = rb.column(1).to_pylist()
    for age, flag in zip(ages, flags):
        assert (flag == "yes") == (age is not None and age < 18.0)


def test_filter_chains_with_groupby(titanic):
    df = (
        titanic
        .filter(hx.col("Sex", type="text") == "female")
        .groupBy(["Pclass"])
        .aggregate({"n": hx.count(hx.col("Pclass"))})
    )
    n_total = sum(df.column(1).to_pylist())
    assert n_total > 200


def test_invert_predicate(titanic):
    yes_count = titanic.filter(hx.col("Sex", type="text") == "female").collect().num_rows
    no_count = titanic.filter(~(hx.col("Sex", type="text") == "female")).collect().num_rows
    total = titanic.collect().num_rows
    assert yes_count + no_count == total


def test_or_predicate(titanic):
    df = titanic.filter(
        (hx.col("Pclass", type="int") == 1) | (hx.col("Pclass", type="int") == 2)
    )
    rb = df.collect()
    pclasses = rb.column(rb.schema.get_field_index("Pclass")).to_pylist()
    assert all(p in (1, 2) for p in pclasses)


def test_lit_type_inference():
    assert hx.lit(5).out_type == "int"
    assert hx.lit(1.5).out_type == "double"
    assert hx.lit("hi").out_type == "text"
    assert hx.lit(True).out_type == "bool"


def test_truthy_expr_raises():
    e = hx.col("x", type="int") > 0
    try:
        bool(e)
    except TypeError:
        return
    assert False, "Expected bool(Expr) to raise"


def test_reuse_frame_across_executions(titanic):
    """A FromArrow-backed frame must materialize correctly multiple times,
    since we re-export buffers on every execution."""
    a = titanic.collect()
    b = titanic.filter(hx.col("Pclass", type="int") == 1).collect()
    c = titanic.collect()
    assert a.num_rows == c.num_rows
    assert b.num_rows < a.num_rows
