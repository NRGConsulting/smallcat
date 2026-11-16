"""SC-040: readers use ``to_arrow_reader``, never the deprecated ``fetch_record_batch``."""

import re
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pyarrow as pa
import pytest

from smallcat.datasets.base_dataset import to_record_batch_reader
from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.datasets.delta_table_dataset import DeltaTableDataset
from smallcat.datasets.excel_dataset import ExcelDataset
from smallcat.datasets.parquet_dataset import ParquetDataset

SRC = Path(__file__).parent.parent / "src" / "smallcat"
DUCKDB_DEFAULT_BATCH = 131072  # DuckDB's default vector batch size


@pytest.fixture
def conn(tmp_path):
    return {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}}


def _frame(rows: int) -> pl.DataFrame:
    return pl.DataFrame({"id": list(range(rows)), "name": ["x"] * rows})


@pytest.mark.parametrize(
    ("dataset_cls", "path"),
    [
        (CSVDataset, "t.csv"),
        (ParquetDataset, "t.parquet"),
        (DeltaTableDataset, "t_delta"),
    ],
)
def test_the_reader_is_a_record_batch_reader_without_deprecation_warnings(
    conn, dataset_cls, path
):
    dataset_cls(conn).save_polars(path, _frame(10))

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        reader = dataset_cls(conn).load_arrow_record_batch_reader(path)
        table = reader.read_all()

    assert isinstance(reader, pa.RecordBatchReader)
    assert table.num_rows == 10


@pytest.mark.parametrize(
    ("dataset_cls", "path"),
    [(CSVDataset, "t.csv"), (ParquetDataset, "t.parquet")],
)
def test_where_and_columns_still_apply(conn, dataset_cls, path):
    dataset_cls(conn).save_polars(path, _frame(10))

    reader = dataset_cls(conn).load_arrow_record_batch_reader(
        path, where="id >= 7", columns=["id"]
    )

    assert reader.read_all().to_pydict() == {"id": [7, 8, 9]}


def test_batch_sizes_match_the_deprecated_method(conn, tmp_path):
    """Same file, same batches as before the migration (guards the batch size)."""
    ParquetDataset(conn).save_polars("big.parquet", _frame(DUCKDB_DEFAULT_BATCH + 10))
    uri = (tmp_path / "big.parquet").as_posix()

    def sizes(reader) -> list[int]:
        return [len(batch) for batch in reader]

    new = sizes(ParquetDataset(conn).load_arrow_record_batch_reader("big.parquet"))
    with duckdb.connect() as con, warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        old = sizes(con.read_parquet(uri).fetch_record_batch())

    assert new == old
    assert sum(new) == DUCKDB_DEFAULT_BATCH + 10


def test_excel_reads_through_to_arrow_reader():
    fake = MagicMock()
    fake.__enter__.return_value = fake
    with patch.object(ExcelDataset, "_duckdb_conn", return_value=fake):
        ds = ExcelDataset(conn={"conn_type": "fs", "extra": {}})
        result = ds.load_arrow_record_batch_reader("in.xlsx")

    relation = fake.sql.return_value.query.return_value
    relation.to_arrow_reader.assert_called_once_with()
    assert result is relation.to_arrow_reader.return_value
    relation.fetch_record_batch.assert_not_called()


def test_the_helper_refuses_a_missing_reader():
    rel = MagicMock()
    rel.to_arrow_reader.return_value = None
    with pytest.raises(RuntimeError, match="no Arrow reader"):
        to_record_batch_reader(rel)


def test_the_helper_refuses_a_missing_relation():
    with pytest.raises(RuntimeError, match="no relation"):
        to_record_batch_reader(None)


def test_the_deprecated_method_is_not_called_anywhere_in_src():
    call = re.compile(r"\.fetch_record_batch\(")
    offenders = [
        str(path.relative_to(SRC))
        for path in SRC.rglob("*.py")
        if call.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
