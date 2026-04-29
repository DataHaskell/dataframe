"""Regression test for the BoxedColumn pattern-shadowing fix in
``ffi/DataFrame/IO/Arrow.hs``.

Before the fix, ``columnToArrow`` matched ``BoxedColumn _ (vec :: V.Vector
Text)`` with a wildcard bitmap and forced ``V.toList vec`` — which crashed
on the bottom thunks ``fromMaybeVec`` writes at null positions. After the
fix, the wildcard clauses are pinned to ``BoxedColumn Nothing`` and the
nullable-aware clause at line 303 actually fires for ``Maybe Text``."""

import hyrax as hx


def test_read_csv_titanic_with_nullable_text_columns():
    """``Cabin`` and ``Embarked`` are nullable Text on titanic.csv. Reading
    end-to-end must not blow up on the null slots."""
    df = hx.read_csv("data/titanic.csv")
    rb = df.collect()
    assert rb.num_rows == 891
    assert "Cabin" in rb.schema.names
    assert "Embarked" in rb.schema.names


def test_nullable_text_nulls_round_trip_as_python_none():
    rb = (
        hx.read_csv("data/titanic.csv")
        .select(["Cabin", "Embarked"])
        .limit(5)
        .collect()
    )
    cabin = rb.column(rb.schema.get_field_index("Cabin")).to_pylist()
    # First and third rows of titanic have empty Cabin → None on the Python side
    assert None in cabin


def test_filter_on_nullable_column_after_read_csv():
    rb = (
        hx.read_csv("data/titanic.csv")
        .filter(hx.col("Age") > 0.0)
        .collect()
    )
    # 891 total - 177 missing Age = 714
    assert rb.num_rows == 714
