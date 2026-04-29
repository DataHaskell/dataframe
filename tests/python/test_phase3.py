"""Smoke tests for Phase 3: extended aggregations + frame statistics."""

import math

import hyrax as hx


def _to_dict(rb):
    return rb.to_pydict()


def test_groupby_min_max(titanic):
    rb = titanic.groupBy(["Sex"]).aggregate({
        "min_age": hx.min(hx.col("Age")),
        "max_age": hx.max(hx.col("Age")),
    })
    out = _to_dict(rb)
    by_sex = dict(zip(out["Sex"], zip(out["min_age"], out["max_age"])))
    # Sanity: titanic Age min ~0.42, max 80
    assert by_sex["male"][0] >= 0.0
    assert by_sex["male"][1] >= 70.0
    assert by_sex["female"][0] >= 0.0
    assert by_sex["female"][1] >= 60.0


def test_groupby_median_variance_std(titanic):
    rb = titanic.groupBy(["Sex"]).aggregate({
        "median_age": hx.median(hx.col("Age")),
        "var_age": hx.variance(hx.col("Age")),
        "std_age": hx.std(hx.col("Age")),
    })
    out = _to_dict(rb)
    medians = dict(zip(out["Sex"], out["median_age"]))
    variances = dict(zip(out["Sex"], out["var_age"]))
    stds = dict(zip(out["Sex"], out["std_age"]))
    # std == sqrt(var) — invariant must hold for both groups
    for k in medians:
        assert math.isclose(stds[k], math.sqrt(variances[k]), rel_tol=1e-6), (
            f"std/var mismatch for {k}: std={stds[k]}, sqrt(var)={math.sqrt(variances[k])}"
        )


def test_describe(titanic):
    rb = titanic.select(["Age", "Fare", "Pclass"]).describe().collect()
    # Schema: Statistic + the three measured columns
    assert "Statistic" in rb.schema.names
    for col_name in ("Age", "Fare", "Pclass"):
        assert col_name in rb.schema.names
    # summarize emits ~10 statistics rows
    assert rb.num_rows >= 5


def test_correlation_age_fare(titanic):
    rb = titanic.correlation("Age", "Fare").collect()
    out = _to_dict(rb)
    assert out["first"] == ["Age"]
    assert out["second"] == ["Fare"]
    # Loose: titanic Age vs Fare correlation is positive but small.
    corr = out["correlation"][0]
    assert -1.0 <= corr <= 1.0


def test_frequencies(titanic):
    rb = titanic.frequencies("Sex").collect()
    out = _to_dict(rb)
    assert set(out["Sex"]) == {"male", "female"}
    total = sum(out["count"])
    assert total > 700  # titanic has ~714 non-null Age rows
