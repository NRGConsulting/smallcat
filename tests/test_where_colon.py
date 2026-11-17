"""SC-053: a ``:name`` inside a WHERE literal reaches DuckDB untouched."""

import polars as pl
import pytest

from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.datasets.parquet_dataset import ParquetDataset


@pytest.fixture
def conn(tmp_path):
    return {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}}


@pytest.fixture
def frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["a :b", "a NULL", "x:y", "plain"],
            "note": [":x y", "NULL y", "10:30", "ok"],
        }
    )


@pytest.mark.parametrize(
    ("where", "expected_ids"),
    [
        ("name = 'a :b'", [1]),
        ("name = 'a NULL'", [2]),
        ("note = ':x y'", [1]),
        ("note = '10:30'", [3]),
        ("name = 'x:y' OR note = ':x y'", [1, 3]),
        (r"name = 'a \:b'", []),
    ],
)
@pytest.mark.parametrize(
    ("dataset_cls", "path"), [(CSVDataset, "t.csv"), (ParquetDataset, "t.parquet")]
)
def test_a_colon_in_a_literal_is_not_a_bind_parameter(
    conn, frame, dataset_cls, path, where, expected_ids
):
    dataset_cls(conn).save_polars(path, frame)

    out = dataset_cls(conn).load_polars(path, where=where)

    assert out["id"].to_list() == expected_ids


@pytest.mark.parametrize("where", ["name = 'a :b'", "note = ':x y'", r"c = '\:z'"])
def test_the_where_text_is_kept_as_it_is(conn, where):
    query = CSVDataset(conn)._build_query("data", None, where)
    assert query == f"SELECT * FROM data WHERE ({where})"


def test_the_query_is_one_line_of_plain_text(conn):
    ds = CSVDataset(conn)
    assert (
        ds._build_query("data", ["id", "name"], "id > 1")
        == 'SELECT "id", "name" FROM data WHERE (id > 1)'
    )
    assert ds._build_query("data", None, None) == "SELECT * FROM data"
