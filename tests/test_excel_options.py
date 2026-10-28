from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from smallcat.datasets.excel_dataset import (
    ExcelDataset,
    ExcelLoadOptions,
    ExcelSaveOptions,
)


@pytest.fixture
def con():
    """A fake DuckDB connection that records the SQL it is asked to run."""
    fake = MagicMock()
    fake.__enter__.return_value = fake
    with patch.object(ExcelDataset, "_duckdb_conn", return_value=fake):
        yield fake


def _read_sql(con, **load_options):
    ds = ExcelDataset(
        conn={"conn_type": "fs", "extra": {}},
        load_options=ExcelLoadOptions(**load_options),
    )
    ds.load_arrow_record_batch_reader("in.xlsx")
    return con.execute.call_args.args[0]


def _write_sql(con, **save_options):
    ds = ExcelDataset(
        conn={"conn_type": "fs", "extra": {}},
        save_options=ExcelSaveOptions(**save_options),
    )
    ds.save_arrow_table("out.xlsx", pa.table({"a": [1]}))
    return con.execute.call_args.args[0]


@pytest.mark.parametrize("option", ["header", "all_varchar", "empty_as_varchar"])
@pytest.mark.parametrize("value", [True, False])
def test_load_boolean_options_are_forwarded(con, option, value):
    sql = _read_sql(con, **{option: value})
    assert f"{option} = {str(value).lower()}" in sql


def test_load_options_left_unset_are_not_forwarded(con):
    sql = _read_sql(con)
    assert sql.count("read_xlsx(?)") == 1
    for option in ("header", "all_varchar", "empty_as_varchar"):
        assert option not in sql


@pytest.mark.parametrize("value", [True, False])
def test_save_header_is_forwarded(con, value):
    sql = _write_sql(con, header=value)
    assert f"HEADER {str(value).lower()}" in sql


def test_save_options_left_unset_are_not_forwarded(con):
    sql = _write_sql(con)
    assert "HEADER" not in sql
    assert "FORMAT xlsx)" in sql
