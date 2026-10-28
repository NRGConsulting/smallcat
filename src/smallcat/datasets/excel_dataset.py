"""Excel (.xlsx) dataset via DuckDB's `excel` extension.

This module provides :class:`ExcelDataset` for reading/writing **.xlsx** files
(legacy **.xls** is not supported). Paths are **relative** to the configured
base URI; the DuckDB `excel` extension is installed/loaded at runtime.

Capabilities:
  * Read a whole sheet or an A1 range with optional header handling.
  * Coerce empty columns or all columns to VARCHAR for schema stability.
  * Write Arrow tables to a specific sheet (with optional header row).

Example:
  >>> ds = ExcelDataset.from_conn_id("fs_conn")
  >>> tbl = ds.load_arrow_table("inputs/budget.xlsx")  # first sheet by default
  >>> ds.save_arrow_table("outputs/budget_out.xlsx", tbl)

Options:
  * `ExcelLoadOptions`: header, sheet, range, all_varchar, empty_as_varchar.
  * `ExcelSaveOptions`: header, sheet.
"""

import pyarrow as pa
from pydantic import BaseModel, Field

from smallcat.datasets.base_dataset import (
    BaseDataset,
    ensure_extension,
    sql_string_literal,
)


class ExcelLoadOptions(BaseModel):
    """Options that control how an .xlsx file is read.

    Attributes:
      header: If True, treat the first row as column headers.
      sheet: Optional worksheet name to read. If omitted, the first sheet is used.
      range: Excel A1-style range to read (e.g., "A1:D100").
             If omitted, the full sheet is read.
      all_varchar: If True, coerce all columns to VARCHAR (strings).
      empty_as_varchar: If True, treat empty columns as VARCHAR instead of NULL/typed.
    """

    header: bool | None = Field(None)
    sheet: str | None = Field(None)
    range: str | None = Field(None)
    all_varchar: bool | None = Field(None)
    empty_as_varchar: bool | None = Field(None)


class ExcelSaveOptions(BaseModel):
    """Options that control how an Arrow table is written to .xlsx.

    Attributes:
      header: If True, include column headers in the output file.
      sheet: Optional worksheet name to write into (created if missing).
    """

    header: bool | None = Field(None)
    sheet: str | None = Field(None)


class ExcelDataset(BaseDataset[ExcelLoadOptions, ExcelSaveOptions]):
    """Reads and writes **.xlsx** files via DuckDB's `excel` extension.

    Notes:
      * Legacy **.xls** format is **not** supported.
      * Paths are treated as relative to this dataset's base URI (e.g., `file://` or
        `gs://`); use the connection extras to set the base.
    """

    def load_arrow_record_batch_reader(
        self,
        path: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> pa.RecordBatchReader:
        """Stream .xlsx rows as record batches with an optional filter."""
        full_uri = self._full_uri(path)
        query = self._build_query("data", columns, where)
        with self._duckdb_conn() as con:
            ensure_extension(con, "excel")
            lo = self.load_options_dict()
            args_sql = ""
            if (header := lo.get("header")) is not None:
                args_sql += f", header = {str(header).lower()}"
            if sheet := lo.get("sheet"):
                args_sql += f", sheet = {sql_string_literal(sheet)}"
            if _range := lo.get("range"):
                args_sql += f", range = {sql_string_literal(_range)}"
            if (all_varchar := lo.get("all_varchar")) is not None:
                args_sql += f", all_varchar = {str(all_varchar).lower()}"
            if (empty_as_varchar := lo.get("empty_as_varchar")) is not None:
                args_sql += f", empty_as_varchar = {str(empty_as_varchar).lower()}"
            # Expose the sheet as the relation `data` and run the query on it,
            # instead of rewriting `FROM data` in the SQL text (which also
            # rewrote string literals in the user's WHERE clause).
            sql = f"SELECT * FROM read_xlsx(?{args_sql})"  # noqa: S608  (options are booleans or escaped literals)
            rel = con.sql(sql, params=[full_uri])
            return rel.query("data", query).fetch_record_batch()

    def save_arrow_table(self, path: str, table: pa.Table) -> None:
        """Write a PyArrow table to an .xlsx file.

        Args:
          path: Relative path of the output .xlsx file (joined under the dataset base).
          table: The `pyarrow.Table` to write.

        Notes:
          Uses DuckDB's `COPY ... TO ... WITH (FORMAT xlsx ...)` from the
          `excel` extension. Save-time options are translated into COPY options.
        """
        full_uri = self._full_uri(path)
        with self._duckdb_conn() as con:
            ensure_extension(con, "excel")
            lo = self.save_options_dict()
            args_sql = ""
            if (header := lo.get("header")) is not None:
                args_sql += f", HEADER {str(header).lower()}"
            if sheet := lo.get("sheet"):
                args_sql += f", SHEET {sql_string_literal(sheet)}"
            con.register("tmp_input", table)
            query = f"copy (select * from tmp_input) TO ? WITH (FORMAT xlsx{args_sql})"  # noqa: S608
            con.execute(query, [full_uri]).fetch_arrow_table()
