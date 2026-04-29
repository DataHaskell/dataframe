"""Smoke tests for Phase 4: extra I/O (Parquet read, JSON read, CSV write)."""

import os
import tempfile

import polars as pl
import hyrax as hx


def test_read_parquet_iris():
    rb = hx.read_parquet("data/iris.parquet").limit(5).collect()
    assert rb.num_rows == 5
    assert "variety" in rb.schema.names


def test_read_parquet_then_filter():
    rb = (
        hx.read_parquet("data/iris.parquet")
        .filter(hx.col("sepal.length", type="double") > 5.0)
        .collect()
    )
    sepal = rb.column(rb.schema.get_field_index("sepal.length")).to_pylist()
    assert all(v > 5.0 for v in sepal)


def test_write_csv_roundtrip(tmp_path):
    src = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).to_arrow()
    df = hx.from_arrow(src)

    out = tmp_path / "out.csv"
    df.write_csv(str(out))

    assert out.exists()
    text = out.read_text()
    # CSV header + 3 rows
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) == 4
    assert "a" in lines[0]
    assert "b" in lines[0]


def test_write_csv_returns_none(tmp_path):
    df = hx.from_arrow(pl.DataFrame({"x": [1]}).to_arrow())
    out = tmp_path / "out.csv"
    result = df.write_csv(str(out))
    assert result is None
