"""Regression tests for SC-064: ``where`` on text columns stored as ``string_view``.

Delta files written by some tools (e.g. Polars with the newest Arrow
compatibility level) hold ``string_view`` columns. pyarrow has no ``equal``
kernel mixing ``string`` and ``string_view``, so the filter DuckDB pushes down
to the dataset scanner used to fail.
"""

import pyarrow as pa
import pytest

from smallcat.datasets.delta_table_dataset import DeltaTableDataset


def _view_table() -> pa.Table:
    return pa.table(
        {
            "method": pa.array(["a", "b", "a", "c"], pa.string_view()),
            "n": [1, 2, 3, 4],
        }
    )


@pytest.fixture
def dataset(local_conn):
    ds = DeltaTableDataset(conn=local_conn)
    ds.save_arrow_table("t", _view_table())
    return ds


def test_where_on_string_view_column(dataset):
    out = dataset.load_arrow_table("t", where="method = 'a'")
    assert out.column("n").to_pylist() == [1, 3]


def test_where_text_and_numeric_with_projection(dataset):
    out = dataset.load_arrow_table(
        "t", where="method = 'a' AND n > 1", columns=["n", "method"]
    )
    assert out.to_pydict() == {"n": [3], "method": ["a"]}


def test_where_in_and_like(dataset):
    assert dataset.load_arrow_table("t", where="method IN ('a', 'b')").num_rows == 3
    assert dataset.load_arrow_table("t", where="method LIKE 'b%'").num_rows == 1


def test_no_where_still_works(dataset):
    assert dataset.load_arrow_table("t").num_rows == 4


def test_load_polars_where_on_string_view(dataset):
    df = dataset.load_polars("t", where="method = 'a'")
    assert df["n"].to_list() == [1, 3]


def test_plain_string_column_keeps_working(local_conn):
    ds = DeltaTableDataset(conn=local_conn)
    ds.save_arrow_table("p", pa.table({"method": ["a", "b"], "n": [1, 2]}))
    assert ds.load_arrow_table("p", where="method = 'b'").column("n").to_pylist() == [2]


def test_where_on_string_view_column_with_nulls(local_conn):
    ds = DeltaTableDataset(conn=local_conn)
    table = pa.table(
        {"method": pa.array(["a", "b", "a", None], pa.string_view()), "n": [1, 2, 3, 4]}
    )
    ds.save_arrow_table("nulls", table)
    out = ds.load_arrow_table("nulls", where="method = 'a'")
    assert out.column("n").to_pylist() == [1, 3]
