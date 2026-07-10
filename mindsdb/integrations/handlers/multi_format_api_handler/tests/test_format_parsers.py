"""Unit tests for multi_format_api_handler JSON record-array flattening.

These exercise `parse_json` directly (no network, no MindsDB executor) and lock
in the generic record-array detection, scalar-sibling reattachment, and the
`record_path` / `auto_explode` overrides.
"""

import json
import logging

import pandas as pd

from mindsdb.integrations.handlers.multi_format_api_handler.format_parsers import parse_json


def _dumps(obj):
    return json.dumps(obj)


def test_envelope_array_under_domain_key_explodes():
    """The bug: {"tickers": [...], "status": "OK", "count": N} collapsed into
    one row. It must now explode into one row per record."""
    payload = {
        "tickers": [
            {"symbol": "A", "price": 1.0},
            {"symbol": "B", "price": 2.0},
            {"symbol": "C", "price": 3.0},
        ],
        "status": "OK",
        "count": 3,
        "request_id": "abc123",
    }
    df = parse_json(_dumps(payload))

    assert len(df) == 3
    assert set(["symbol", "price"]).issubset(df.columns)
    # Scalar siblings reattached as constant columns.
    assert (df["status"] == "OK").all()
    assert (df["count"] == 3).all()
    assert (df["request_id"] == "abc123").all()
    # The array key itself is not reattached as a column.
    assert "tickers" not in df.columns


def test_whitelist_key_still_explodes():
    """Back-compat: an envelope under a whitelist key ('data') keeps exploding."""
    payload = {"data": [{"id": 1}, {"id": 2}], "status": "ok"}
    df = parse_json(_dumps(payload))

    assert len(df) == 2
    assert list(df["id"]) == [1, 2]
    assert (df["status"] == "ok").all()


def test_top_level_list():
    payload = [{"id": 1}, {"id": 2}, {"id": 3}]
    df = parse_json(_dumps(payload))

    assert len(df) == 3
    assert list(df["id"]) == [1, 2, 3]


def test_single_object():
    payload = {"name": "widget", "qty": 5}
    df = parse_json(_dumps(payload))

    assert len(df) == 1
    assert df.iloc[0]["name"] == "widget"
    assert df.iloc[0]["qty"] == 5


def test_list_of_scalars_not_exploded():
    """A dict whose only list is a list of scalars has no record array; it is a
    single record and the scalar list is serialized, not exploded."""
    payload = {"tags": ["a", "b", "c"], "status": "ok"}
    df = parse_json(_dumps(payload))

    assert len(df) == 1
    assert df.iloc[0]["status"] == "ok"
    # List-of-scalars serialized to a string cell (not exploded to 3 rows).
    assert df.iloc[0]["tags"] == "a, b, c"


def test_multiple_arrays_picks_longest_and_warns(caplog):
    payload = {
        "alpha": [{"a": 1}, {"a": 2}],
        "beta": [{"b": 1}, {"b": 2}, {"b": 3}],
    }
    with caplog.at_level(logging.WARNING):
        df = parse_json(_dumps(payload))

    # Longest array ('beta') wins.
    assert len(df) == 3
    assert "b" in df.columns
    assert any("Multiple record arrays" in rec.message for rec in caplog.records)


def test_explicit_record_path_nested():
    payload = {"result": {"records": [{"x": 1}, {"x": 2}]}}
    df = parse_json(_dumps(payload), record_path="result.records")

    assert len(df) == 2
    assert list(df["x"]) == [1, 2]


def test_record_path_miss_warns_and_autodetects(caplog):
    payload = {"data": [{"a": 1}]}
    with caplog.at_level(logging.WARNING):
        df = parse_json(_dumps(payload), record_path="does.not.exist")

    assert len(df) == 1
    assert df.iloc[0]["a"] == 1
    assert any("did not resolve to a list" in rec.message for rec in caplog.records)


def test_auto_explode_false_keeps_single_row():
    payload = {
        "tickers": [{"symbol": "A"}, {"symbol": "B"}],
        "status": "OK",
    }
    df = parse_json(_dumps(payload), auto_explode=False)

    assert len(df) == 1
    assert df.iloc[0]["status"] == "OK"


def test_nested_dict_produces_dotted_columns():
    payload = {"items": [{"a": {"b": 1}}, {"a": {"b": 2}}]}
    df = parse_json(_dumps(payload))

    assert len(df) == 2
    assert "a.b" in df.columns
    assert list(df["a.b"]) == [1, 2]


def test_sibling_collision_gets_meta_prefix():
    """A scalar sibling whose name collides with a normalized record column is
    reattached under a 'meta_' prefix."""
    payload = {"data": [{"status": "active"}, {"status": "inactive"}], "status": "OK"}
    df = parse_json(_dumps(payload))

    assert len(df) == 2
    # Record-level column preserved.
    assert list(df["status"]) == ["active", "inactive"]
    # Envelope scalar reattached under meta_ prefix.
    assert (df["meta_status"] == "OK").all()


def test_empty_array_returns_empty_dataframe():
    payload = {"data": []}
    df = parse_json(_dumps(payload))

    assert len(df) == 0
    # Must never be a (0, 0) frame: DuckDB's `SELECT * FROM df` requires at
    # least one column, else it raises "Need a DataFrame with at least one
    # column" and the agent misreads a valid empty result as a bad query.
    assert df.shape[1] >= 1


def test_empty_array_preserves_envelope_scalar_columns():
    # Regression: {"results": [], "next_url": "..."} previously produced a
    # (0, 0) DataFrame, crashing DuckDB downstream. It must now yield an empty
    # frame whose columns are the envelope's scalar siblings.
    payload = {"results": [], "next_url": "https://api.example.com/next", "count": 0}
    df = parse_json(_dumps(payload))

    assert len(df) == 0
    assert "next_url" in df.columns
    assert "count" in df.columns
    assert "results" not in df.columns


def test_empty_top_level_array_falls_back_to_sentinel_column():
    # A bare empty list has no envelope metadata to borrow columns from, so it
    # falls back to a sentinel column to keep DuckDB happy.
    df = parse_json(_dumps([]))

    assert len(df) == 0
    assert df.shape[1] >= 1


def test_primitive_wrapped_in_value_column():
    df = parse_json(_dumps(42))

    assert len(df) == 1
    assert df.iloc[0]["value"] == 42
