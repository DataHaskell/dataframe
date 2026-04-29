"""Smoke tests for Phase 2 operations: joins, exclude, rename, distinct,
take/drop/tail/slice."""

import polars as pl
import hyrax as hx


def test_distinct():
    raw = pl.DataFrame({"k": ["a", "b", "a", "c", "b"]}).to_arrow()
    df = hx.from_arrow(raw)
    assert df.distinct().collect().num_rows == 3


def test_exclude(titanic):
    rb = titanic.exclude(["SibSp", "Parch"]).limit(1).collect()
    names = rb.schema.names
    assert "SibSp" not in names
    assert "Parch" not in names
    assert "Sex" in names  # not excluded


def test_rename_dict(titanic):
    rb = titanic.rename({"Pclass": "Class"}).limit(1).collect()
    names = rb.schema.names
    assert "Class" in names
    assert "Pclass" not in names


def test_rename_pairs(titanic):
    rb = titanic.rename([("Pclass", "Class"), ("Sex", "Gender")]).limit(1).collect()
    names = rb.schema.names
    assert "Class" in names
    assert "Gender" in names
    assert "Pclass" not in names
    assert "Sex" not in names


def test_slice(titanic):
    rb = titanic.slice(10, 15).collect()
    assert rb.num_rows == 5


def test_tail(titanic):
    rb = titanic.tail(3).collect()
    assert rb.num_rows == 3


def test_drop_first(titanic):
    total = titanic.collect().num_rows
    rb = titanic.drop_first(5).collect()
    assert rb.num_rows == total - 5


def test_drop_last(titanic):
    total = titanic.collect().num_rows
    rb = titanic.drop_last(5).collect()
    assert rb.num_rows == total - 5


def test_inner_join():
    left = hx.from_arrow(pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).to_arrow())
    right = hx.from_arrow(pl.DataFrame({"id": [1, 2, 4], "val": [10, 20, 40]}).to_arrow())
    rb = left.join(right, on=["id"], how="inner").collect()
    assert rb.num_rows == 2
    assert set(rb.schema.names) == {"id", "name", "val"}


def test_left_join_keeps_all_left():
    left = hx.from_arrow(pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).to_arrow())
    right = hx.from_arrow(pl.DataFrame({"id": [1, 2, 4], "val": [10, 20, 40]}).to_arrow())
    rb = left.join(right, on=["id"], how="left").collect()
    assert rb.num_rows == 3
    ids = rb.column(rb.schema.get_field_index("id")).to_pylist()
    assert sorted(ids) == [1, 2, 3]


def test_join_invalid_how_raises():
    left = hx.from_arrow(pl.DataFrame({"id": [1]}).to_arrow())
    right = hx.from_arrow(pl.DataFrame({"id": [1]}).to_arrow())
    try:
        left.join(right, on=["id"], how="cross")
    except ValueError:
        return
    assert False, "Expected ValueError for unknown how"


def test_chain_filter_join_groupby():
    left = hx.from_arrow(pl.DataFrame({"id": [1, 2, 3, 4], "k": ["x", "y", "x", "z"]}).to_arrow())
    right = hx.from_arrow(pl.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]}).to_arrow())
    rb = (
        left.join(right, on=["id"], how="inner")
        .filter(hx.col("v", type="int") >= 20)
        .groupBy(["k"])
        .aggregate({"total": hx.sum(hx.col("v"))})
    )
    out = dict(zip(rb.column(0).to_pylist(), rb.column(1).to_pylist()))
    assert out == {"y": 20, "x": 30, "z": 40}
