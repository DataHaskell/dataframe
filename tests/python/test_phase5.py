"""Smoke tests for Phase 5: lazy / streaming engine.

`scan_csv` and `scan_parquet` route the file read through the Haskell lazy
engine — the schema lets it skip-and-project columns at scan time, which is
the main scaling win for big files. Downstream operations currently
materialize before being applied; that's still useful for column projection."""

import hyrax as hx


_CITY_SCHEMA = {
    "id": "int",
    "name": "text",
    "country_code": "text",
    "district": "text",
    "population": "int",
}


def test_scan_csv_basic():
    rb = hx.scan_csv("tests/data/city.csv", _CITY_SCHEMA).limit(3).collect()
    assert rb.num_rows == 3
    # Schema should reflect the requested types
    assert rb.schema.field("id").type.bit_width == 64
    assert rb.schema.field("population").type.bit_width == 64


def test_scan_csv_filter_select():
    rb = (
        hx.scan_csv("tests/data/city.csv", _CITY_SCHEMA)
        .filter(hx.col("population", type="int") > 1_000_000)
        .select(["name", "population"])
        .collect()
    )
    pops = rb.column(rb.schema.get_field_index("population")).to_pylist()
    assert all(p > 1_000_000 for p in pops)
    assert rb.num_rows > 0


def test_scan_csv_groupby():
    rb = (
        hx.scan_csv("tests/data/city.csv", _CITY_SCHEMA)
        .groupBy(["country_code"])
        .aggregate({
            "n": hx.count(hx.col("country_code")),
            "total_pop": hx.sum(hx.col("population")),
        })
    )
    out = rb.to_pydict()
    assert "country_code" in out
    assert "n" in out
    assert "total_pop" in out
    assert sum(out["n"]) > 0


def test_scan_csv_unsupported_type_raises():
    """Type tags must be in the supported set."""
    try:
        hx.scan_csv("tests/data/city.csv", {"id": "weird"})
    except ValueError:
        return
    assert False, "Expected ValueError for unsupported schema type"


def test_scan_csv_dict_or_pairs_accepted():
    rb1 = hx.scan_csv("tests/data/city.csv", _CITY_SCHEMA).limit(2).collect()
    rb2 = (
        hx.scan_csv("tests/data/city.csv", list(_CITY_SCHEMA.items()))
        .limit(2)
        .collect()
    )
    assert rb1.num_rows == rb2.num_rows
    assert rb1.schema.names == rb2.schema.names
