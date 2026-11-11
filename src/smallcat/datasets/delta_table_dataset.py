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
  * `DeltaTableSaveOptions`: mode, partition_by, schema_mode, merge_keys,
    update_columns.

Merge:
  With ``mode: merge`` rows are matched on ``merge_keys``: matching rows are
  updated (all columns but the keys, or only ``update_columns``) and the rest
  are inserted. Nothing is deleted. The table is created if it does not exist.
  :meth:`DeltaTableDataset.update` changes rows selected by a predicate.

Notes:
  Databricks connections are rejected explicitly on both read and write.
  Credentials are never exported to the process environment.
"""

import inspect
import math
from datetime import date, datetime
from enum import StrEnum
from typing import Any

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from pydantic import BaseModel, ConfigDict, Field, model_validator

from smallcat.datasets.base_dataset import BaseDataset, sql_string_literal

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
    MERGE = "merge"


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
      merge_keys: Columns that identify a row in ``merge`` mode (the ``ON`` of
        the merge). Required with ``merge``, not allowed with other modes.
      update_columns: Columns overwritten in the rows that match, in ``merge``
        mode. By default every column of the data except the keys.
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
    merge_keys: list[str] | None = Field(
        None,
        description="Columns that identify a row (required with mode 'merge').",
    )
    update_columns: list[str] | None = Field(
        None,
        description="Columns updated on matching rows (mode 'merge' only).",
    )

    @model_validator(mode="after")
    def _check_merge_options(self) -> "DeltaTableSaveOptions":
        """Reject merge options that contradict the write mode."""
        if self.mode == WriteMode.MERGE:
            if not self.merge_keys:
                msg = "mode 'merge' requires a non-empty merge_keys"
                raise ValueError(msg)
            if self.schema_mode is not None:
                msg = "schema_mode is not supported with mode 'merge'"
                raise ValueError(msg)
        elif self.merge_keys is not None or self.update_columns is not None:
            shown = "no mode" if self.mode is None else f"mode '{self.mode.value}'"
            msg = (
                "merge_keys and update_columns are only valid with mode 'merge' "
                f"(got {shown})"
            )
            raise ValueError(msg)
        if self.merge_keys is not None and len(set(self.merge_keys)) != len(
            self.merge_keys,
        ):
            msg = "merge_keys has repeated columns"
            raise ValueError(msg)
        if self.update_columns is not None:
            if len(set(self.update_columns)) != len(self.update_columns):
                msg = "update_columns has repeated columns"
                raise ValueError(msg)
            overlap = sorted(set(self.update_columns) & set(self.merge_keys or []))
            if overlap:
                msg = f"update_columns must not include merge keys: {overlap}"
                raise ValueError(msg)
        return self


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

    def _write_kwargs(self) -> dict:
        """Options for `write_deltalake`, without the merge-only ones."""
        kwargs = self.save_options_dict()
        kwargs.pop("merge_keys", None)
        kwargs.pop("update_columns", None)
        if _WRITE_DELTALAKE_HAS_ENGINE:
            kwargs["engine"] = (
                "rust"
                if self.save_options is not None
                and self.save_options.schema_mode == SchemaMode.MERGE
                else "pyarrow"
            )
        return kwargs

    def save_arrow_table(self, path: str, table: pa.Table) -> None:
        """Write a PyArrow table to Delta Lake using delta-rs.

        With ``mode: merge`` the rows are merged by ``merge_keys`` instead of
        written as they are (see :meth:`_merge_arrow_table`).

        Args:
          path: Relative path to the target Delta table (joined under the
            dataset's base URI).
          table: The `pyarrow.Table` to write.

        Notes:
          The write is attempted exactly once; errors from `write_deltalake`
          (including `TypeError`) propagate unchanged.

        Raises:
          NotImplementedError: If the connection type is ``databricks``.
          ValueError: In merge mode, if the keys or columns are invalid or the
            keys are null or duplicated in `table`.
        """
        if self.conn.conn_type == "databricks":
            raise NotImplementedError(DATABRICKS_UNSUPPORTED_MSG)
        opts = self.save_options
        if opts is not None and opts.mode == WriteMode.MERGE:
            self._merge_arrow_table(path, table)
            return
        if opts is not None and (opts.merge_keys or opts.update_columns):
            msg = "merge_keys and update_columns are only valid with mode 'merge'"
            raise ValueError(msg)
        write_deltalake(
            table_or_uri=self._full_uri(path),
            data=table,
            storage_options=self._delta_storage_options(),
            **self._write_kwargs(),
        )

    def _merge_arrow_table(self, path: str, table: pa.Table) -> None:
        """Insert the new rows of `table` and update the ones that match.

        Rows are matched on ``merge_keys`` (never updated). Matching rows get
        ``update_columns`` (by default every column but the keys); the rest are
        inserted whole. Nothing is deleted. If the table does not exist it is
        created with the data.

        Everything is validated before writing, so a failure leaves the table
        untouched.
        """
        opts = self.save_options
        keys = list(opts.merge_keys or []) if opts else []
        self._check_merge_source(table, keys)
        uri = self._full_uri(path)
        storage_options = self._delta_storage_options()
        try:
            target = DeltaTable(uri, storage_options=storage_options)
        except TableNotFoundError:
            kwargs = self._write_kwargs()
            kwargs["mode"] = "error"
            write_deltalake(
                table_or_uri=uri,
                data=table,
                storage_options=storage_options,
                **kwargs,
            )
            return

        target_columns = {f.name for f in target.schema().fields}
        source_columns = list(table.column_names)
        missing_in_target = sorted(set(source_columns) - target_columns)
        if missing_in_target:
            msg = (
                f"columns {missing_in_target} are not in the Delta table "
                f"(it has {sorted(target_columns)}); merge does not change the schema"
            )
            raise ValueError(msg)
        updatable = (
            list(opts.update_columns)
            if opts is not None and opts.update_columns is not None
            else [c for c in source_columns if c not in keys]
        )
        unknown = sorted(set(updatable) - set(source_columns))
        if unknown:
            msg = f"update_columns {unknown} are not columns of the data"
            raise ValueError(msg)

        predicate = " AND ".join(
            f"target.{_ident(k)} = source.{_ident(k)}" for k in keys
        )
        merger = target.merge(
            source=table,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        if updatable:
            merger = merger.when_matched_update(
                updates={c: f"source.{_ident(c)}" for c in updatable},
            )
        merger.when_not_matched_insert(
            updates={c: f"source.{_ident(c)}" for c in source_columns},
        ).execute()

    @staticmethod
    def _check_merge_source(table: pa.Table, keys: list[str]) -> None:
        """Check that the keys exist in `table` and identify each row once."""
        missing = sorted(set(keys) - set(table.column_names))
        if missing:
            msg = (
                f"merge_keys {missing} are not columns of the data "
                f"(columns: {table.column_names})"
            )
            raise ValueError(msg)
        for key in keys:
            if table[key].null_count:
                msg = f"merge key '{key}' has null values: they cannot identify a row"
                raise ValueError(msg)
        if pl.DataFrame(table.select(keys)).is_duplicated().any():
            msg = (
                f"the data has several rows with the same merge_keys {keys}: "
                "deduplicate it before merging"
            )
            raise ValueError(msg)

    def update(
        self,
        path: str,
        predicate: str,
        values: dict[str, Any],
    ) -> dict[str, Any]:
        """Set columns to constant values on the rows that satisfy a predicate.

        Args:
          path: Relative path to the Delta table.
          predicate: SQL condition selecting the rows (same restrictions as a
            `where`: no subqueries). It cannot be empty.
          values: Column name to new value. Values are ``str``, ``bool``,
            ``int``, ``float``, ``date``, ``datetime`` or ``None`` and are
            written as escaped SQL literals.

        Returns:
          The metrics reported by delta-rs.

        Raises:
          NotImplementedError: For Databricks connections.
          ValueError: If the predicate is empty or unsafe, `values` is empty or
            a column does not exist.
          TypeError: If a value has an unsupported type.
        """
        if self.conn.conn_type == "databricks":
            raise NotImplementedError(DATABRICKS_UNSUPPORTED_MSG)
        if not predicate or not predicate.strip():
            msg = "update requires a non-empty predicate"
            raise ValueError(msg)
        if not values:
            msg = "update requires at least one value"
            raise ValueError(msg)
        self._validated_where(predicate)
        updates = {c: _sql_literal(v) for c, v in values.items()}
        target = DeltaTable(
            self._full_uri(path),
            storage_options=self._delta_storage_options(),
        )
        columns = {f.name for f in target.schema().fields}
        unknown = sorted(set(values) - columns)
        if unknown:
            msg = f"columns {unknown} are not in the Delta table"
            raise ValueError(msg)
        return target.update(updates=updates, predicate=predicate)


def _ident(name: str) -> str:
    """Quote a column name as a SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _sql_literal(value: object) -> str:  # noqa: PLR0911
    """Render a Python value as an escaped SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            msg = "NaN and infinity cannot be used as values"
            raise ValueError(msg)
        return repr(value)
    if isinstance(value, datetime):
        return f"CAST({sql_string_literal(value.isoformat(sep=' '))} AS TIMESTAMP)"
    if isinstance(value, date):
        return f"CAST({sql_string_literal(value.isoformat())} AS DATE)"
    if isinstance(value, str):
        return sql_string_literal(value)
    msg = f"unsupported value type {type(value).__name__}"
    raise TypeError(msg)
