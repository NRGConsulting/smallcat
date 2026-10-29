"""Parquet dataset backed by DuckDB.

This module provides :class:`ParquetDataset`, a concrete implementation of
:class:`~smallcat.datasets.base_dataset.BaseDataset` that reads/writes Parquet
via DuckDB. Paths passed to public methods are **relative** to the configured
base (e.g., `file://` or `gs://`).

Features:
  * Read from a single file, directory, or glob pattern.
  * Hive partition discovery and schema union (optional).
  * Write with optional partitioning and overwrite.

Example:
  >>> ds = ParquetDataset.from_conn_id("gcs_conn")
  >>> tbl = ds.load_arrow_table("bronze/events/**/*.parquet")
  >>> ds.save_arrow_table("silver/events/", tbl)

Related options:
  * `ParquetLoadOptions`: binary_as_string, hive_partitioning, union_by_name...
  * `ParquetSaveOptions`: overwrite, partition_by, write_partition_columns.
"""

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field

from smallcat.datasets.base_dataset import BaseDataset


class ParquetLoadOptions(BaseModel):
    """Options that control how Parquet is read via DuckDB.

    Attributes:
      binary_as_string: If True, interpret BINARY columns as strings.
      file_row_number: If True, include a synthetic row-number column per file.
      hive_partitioning: If True, parse Hive-style directory partitions.
      union_by_name: If True, align/union schemas by column name across files.
    """

    model_config = ConfigDict(extra="forbid")

    binary_as_string: bool | None = Field(None)
    file_row_number: bool | None = Field(None)
    hive_partitioning: bool | None = Field(None)
    union_by_name: bool | None = Field(None)


class ParquetSaveOptions(BaseModel):
    """Options that control how Parquet is written via DuckDB.

    Attributes:
      overwrite: If True, overwrite existing output.
      partition_by: Columns to partition by (Hive-style layout).
      write_partition_columns: If True, also materialize partition cols in files.
    """

    model_config = ConfigDict(extra="forbid")

    overwrite: bool | None = Field(None)
    partition_by: list[str] | None = Field(None)
    write_partition_columns: bool | None = Field(None)


class ParquetDataset(BaseDataset):
    """Parquet dataset backed by DuckDB's Parquet reader/writer.

    Paths passed to public methods are treated as **relative** to the dataset's
    configured base (e.g., `file://` or `gs://`). Reads return a PyArrow
    table.

    Notes:
      * You can pass a single file, a directory (e.g., `/path/**.parquet`),
        or any glob DuckDB understands.
    """

    def load_arrow_record_batch_reader(
        self,
        path: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> pa.RecordBatchReader:
        """Stream Parquet rows as record batches with an optional filter."""
        full_uri = self._full_uri(path)
        query = self._build_query("data", columns, where)
        with self._duckdb_conn() as con:
            rel = con.read_parquet(full_uri, **self.load_options_dict())
            return rel.query("data", query).fetch_record_batch()

    def save_arrow_table(self, path: str, table: pa.Table) -> None:
        """Write a PyArrow table to Parquet.

        Args:
          path: Relative output path (file or directory) joined under the
            dataset base URI.
          table: The `pyarrow.Table` to write.

        Notes:
          Uses `Relation.write_parquet` with parameters from
          `save_options_dict()`.
        """
        full_uri = self._full_uri(path)
        with self._duckdb_conn() as con:
            con.register("tmp_input", table)
            con.sql("SELECT * FROM tmp_input").write_parquet(
                full_uri,
                **self.save_options_dict(),
            )
