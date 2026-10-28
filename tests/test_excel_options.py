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
    return con.sql.call_args.args[0]


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
    assert sql == "SELECT * FROM read_xlsx(?)"
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


def test_where_clause_is_not_rewritten(con):
    """A literal that looks like `FROM data` must reach DuckDB untouched."""
    where = "note = 'copied FROM data sheet' OR note LIKE '%from  data%'"
    ds = ExcelDataset(conn={"conn_type": "fs", "extra": {}})
    ds.load_arrow_record_batch_reader("in.xlsx", where=where)
    view, query = con.sql.return_value.query.call_args.args
    assert view == "data"
    assert "FROM data" in query
    assert "'copied FROM data sheet'" in query
    assert "'%from  data%'" in query
    assert "read_xlsx" not in query


def test_file_uri_is_passed_as_a_parameter(con):
    ds = ExcelDataset(conn={"conn_type": "fs", "extra": {}})
    ds.load_arrow_record_batch_reader("in.xlsx")
    assert con.sql.call_args.kwargs["params"] == ["in.xlsx"]
