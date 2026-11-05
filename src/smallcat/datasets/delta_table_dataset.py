"""Delta Lake dataset using delta-rs (deltalake) with Smallcat.

This module implements :class:`DeltaTableDataset`, a Delta Lake reader/writer
powered by `deltalake` (delta-rs). It resolves relative paths against the
connection base (e.g., `gs://bucket/prefix`) and returns/accepts Arrow tables.

Storage backends:
  * Local filesystem (`fs`) - no extra options.
  * Google Cloud Storage (`google_cloud_platform`) - credentials derived from
    connection extras: `keyfile_dict` / `keyfile` / `key_path`.
  * Databricks - not supported; raises ``NotImplementedError``.

Example:
  >>> ds = DeltaTableDataset.from_conn_id("gcs_delta")
  >>> tbl = ds.load_arrow_table("bronze/events_delta")
  >>> ds.save_arrow_table("silver/events_delta", tbl)

Options:
  * `DeltaTableLoadOptions`: version, without_files, log_buffer_size.
  * `DeltaTableSaveOptions`: mode, partition_by, schema_mode.

Notes:
  Databricks connections are rejected explicitly on both read and write.
  Credentials are never exported to the process environment.
"""

import inspect
from enum import StrEnum

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from pydantic import BaseModel, ConfigDict, Field

from smallcat.datasets.base_dataset import BaseDataset

DATABRICKS_UNSUPPORTED_MSG = (
    "Databricks-hosted Delta tables are not supported yet: "
    "use a filesystem or Google Cloud Storage connection."
)

# Older `deltalake` releases take an `engine` argument in `write_deltalake`;
# newer ones removed it. Decide once, up front, instead of retrying a failed
# write (which could duplicate data with `mode="append"`).
_WRITE_DELTALAKE_HAS_ENGINE = "engine" in inspect.signature(write_deltalake).parameters


class DeltaTableLoadOptions(BaseModel):
    """Options controlling how a Delta table is read.

    Attributes:
      version: Optional table version to read.
      without_files: If True, skip listing data files (metadata-only read).
      log_buffer_size: Buffer size for reading Delta logs.
    """

    model_config = ConfigDict(extra="forbid")

    version: int | None = Field(None)
    without_files: bool | None = Field(None)
    log_buffer_size: int | None = Field(None)


class WriteMode(StrEnum):
    """Write behavior when the target table already exists."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"


class SchemaMode(StrEnum):
    """How schema changes are handled during writes."""

    MERGE = "merge"
    OVERWRITE = "overwrite"


class DeltaTableSaveOptions(BaseModel):
    """Options controlling how a Delta table is written.

    Attributes:
      mode: Write mode to apply if the table exists.
      partition_by: Columns to partition by (Hive-style directory layout).
      schema_mode: Strategy to reconcile schema differences during write.
    """

    model_config = ConfigDict(extra="forbid")

    mode: WriteMode | None = Field(None, description="Write mode for existing tables.")
    partition_by: list[str] | None = Field(
        None,
        description="Columns to partition by (Hive-style directory layout).",
    )
    schema_mode: SchemaMode | None = Field(
        None,
        description="How to handle schema differences on write.",
    )


class DeltaTableDataset(BaseDataset[DeltaTableLoadOptions, DeltaTableSaveOptions]):
    """Delta Lake dataset that reads/writes via delta-rs (DeltaTable / write_deltalake).

    Paths passed to public methods are treated as **relative** to the dataset's
    configured base (e.g., local `file://` or `gs://`). Reads return a
    PyArrow table.

    Notes:
      * For Google Cloud Storage, credentials are derived from the connection's
        extras (e.g., `keyfile_dict`, `keyfile`, or `key_path`).
      * `conn_type == "databricks"` is not supported and raises
        ``NotImplementedError`` on both read and write.
    """

    def _delta_storage_options(self) -> dict:
        """Build `storage_options` for delta-rs reads/writes.

        Delegates to :meth:`BaseDataset._object_store_options`, which Polars
        scans share. See there for the keys, warnings and errors.
        """
        return self._object_store_options()

    def scan_polars(
        self,
        path: str,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Lazily scan the Delta table with Polars.

        The Delta log is read through delta-rs, which also resolves the
        connection credentials, and Polars scans only the Parquet files that
        the query needs (partition and statistics pruning, projection and
        predicate pushdown).

        Args:
          path: Relative path to the Delta table.
          columns: Optional list of columns to project.

        Returns:
          A `polars.LazyFrame`.

        Raises:
          NotImplementedError: For Databricks connections.
        """
        if self.conn.conn_type == "databricks":
            raise NotImplementedError(DATABRICKS_UNSUPPORTED_MSG)
        table = DeltaTable(
            self._full_uri(path),
            storage_options=self._delta_storage_options(),
            **self.load_options_dict(),
        )
        lf = pl.scan_delta(table)
        return lf.select(columns) if columns else lf

    def load_arrow_record_batch_reader(
        self,
        path: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> pa.RecordBatchReader:
        """Stream Delta Lake rows via DuckDB with an optional filter."""
        full_uri = self._full_uri(path)
        if self.conn.conn_type == "databricks":
            raise NotImplementedError(DATABRICKS_UNSUPPORTED_MSG)

        query = self._build_query("data", columns, where)
        storage_options = self._delta_storage_options()
        dt = DeltaTable(
            full_uri,
            storage_options=storage_options,
            **self.load_options_dict(),
        )
        dataset = dt.to_pyarrow_dataset()
        with self._duckdb_conn() as con:
            con.register("data", dataset)
            return con.sql(query).fetch_record_batch()

    def save_arrow_table(self, path: str, table: pa.Table) -> None:
        """Write a PyArrow table to Delta Lake using delta-rs.

        Args:
          path: Relative path to the target Delta table (joined under the
            dataset's base URI).
          table: The `pyarrow.Table` to write.

        Notes:
          The write is attempted exactly once; errors from `write_deltalake`
          (including `TypeError`) propagate unchanged.

        Raises:
          NotImplementedError: If the connection type is ``databricks``.
        """
        if self.conn.conn_type == "databricks":
            raise NotImplementedError(DATABRICKS_UNSUPPORTED_MSG)
        table_uri = self._full_uri(path)
        storage_options = self._delta_storage_options()
        write_kwargs = self.save_options_dict()
        if _WRITE_DELTALAKE_HAS_ENGINE:
            write_kwargs["engine"] = (
                "rust"
                if self.save_options is not None
                and self.save_options.schema_mode == SchemaMode.MERGE
                else "pyarrow"
            )
        write_deltalake(
            table_or_uri=table_uri,
            data=table,
            storage_options=storage_options,
            **write_kwargs,
        )
