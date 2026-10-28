"""Core dataset base class for Smallcat.

This module defines :class:`BaseDataset`, the abstract foundation for concrete
datasets (CSV, Parquet, Excel, Delta, etc.). It standardizes how a dataset:

* Receives an Airflow-like connection (or dict) and exposes its extras.
* Resolves relative paths to fully-qualified URIs (e.g., `file://`, `gs://`).
* Creates isolated DuckDB connections, loading extensions and provider secrets.
* Loads and saves data via a simple Arrow/pandas-friendly API.

Typical usage:
  >>> ds = MyDataset.from_conn_id("my_fs")
  >>> table = ds.load_arrow_table("bronze/my_table.parquet")
  >>> ds.save_pandas("silver/my_table.parquet", table.to_pandas())

Key concepts:
  * `_full_uri(rel_path)` joins relative paths to the connection's base URI.
  * `_duckdb_conn()` returns a fresh, configured DuckDB connection per call.
  * Subclasses must implement `load_arrow_record_batch_reader` and
    `save_arrow_table`.

Dependencies:
  duckdb, pyarrow, pandas, pydantic, and Smallcat connection utilities.
"""

import json
import re
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, TypeVar

import duckdb
import pandas as pd
import pyarrow as pa
from pydantic import BaseModel
from sqlalchemy import MetaData, Table, literal_column, select, text

from smallcat.connections import ConnectionLike, ConnectionProtocol
from smallcat.path_utils import norm_join_uri, to_relative_posix_path

try:
    from airflow.sdk.bases.hook import BaseHook  # type: ignore[attr-defined]
except ImportError:
    try:
        from airflow.hooks.base import BaseHook  # type: ignore[attr-defined,no-redef]
    except ImportError:
        BaseHook = None  # type: ignore[assignment,misc]


# Connection extras that carry Google service account credentials.
GCP_SERVICE_ACCOUNT_EXTRAS = ("keyfile_dict", "keyfile", "key_path")


def sql_string_literal(value: str) -> str:
    """Quote ``value`` as a SQL string literal (doubling single quotes)."""
    return "'" + value.replace("'", "''") + "'"


def _is_plain_filter(ast: dict) -> bool:
    """Return True if the AST is ``SELECT * FROM data WHERE ...`` and nothing else.

    A predicate such as ``1) UNION SELECT ... WHERE (1`` keeps the parentheses
    balanced, so the parsed statement must be checked to still be a bare
    filtered select: no set operations, grouping, ordering or limits.
    """
    try:
        node = ast["statements"][0]["node"]
    except (KeyError, IndexError, TypeError):
        return False
    table = node.get("from_table") or {}
    return (
        len(ast["statements"]) == 1
        and node.get("type") == "SELECT_NODE"
        and table.get("type") == "BASE_TABLE"
        and table.get("table_name") == "data"
        and not node.get("modifiers")
        and not node.get("group_expressions")
        and not node.get("group_sets")
        and node.get("having") is None
        and node.get("qualify") is None
        and node.get("sample") is None
        and not node.get("cte_map", {}).get("map")
    )


def _contains_subquery(node: object) -> bool:
    """Return True if a ``json_serialize_sql`` AST contains a subquery."""
    if isinstance(node, dict):
        if node.get("type") == "SUBQUERY" or node.get("class") == "SUBQUERY":
            return True
        return any(_contains_subquery(v) for v in node.values())
    if isinstance(node, list):
        return any(_contains_subquery(v) for v in node)
    return False


L = TypeVar("L", bound=BaseModel)  # Load options model
S = TypeVar("S", bound=BaseModel)  # Save options model


class BaseDataset(ABC, Generic[L, S]):
    """Base class for dataset loaders/savers backed by DuckDB and a connection.

    This class:
      * Wraps an Airflow-like connection (or connection dict).
      * Expands relative dataset paths into fully-qualified URIs using the
        connection's base settings.
      * Creates a fresh DuckDB connection per operation, configuring extensions
        and any provider-specific secrets (e.g., GCS).
      * Defines an abstract API to load and save Arrow tables, plus
        convenience pandas helpers.

    Type Parameters:
      L: Pydantic model describing load options.
      S: Pydantic model describing save options.

    Attributes:
      conn: The resolved connection implementing `ConnectionProtocol`.
      extras: Parsed `extra_dejson` from the connection.
      base_uri: Base URI inferred from the connection extras (e.g., `file://...`,
        `gs://bucket/prefix`). May be `None` for unknown types.
      load_options: Instance of `L` with load-time options, if any.
      save_options: Instance of `S` with save-time options, if any.
    """

    conn: ConnectionProtocol

    def __init__(
        self,
        conn: ConnectionProtocol | dict,
        load_options: L | None = None,
        save_options: S | None = None,
    ) -> None:
        """Initialize the dataset with a connection and optional options.

        Args:
          conn: An Airflow-like connection object implementing
            `ConnectionProtocol` or a plain dict compatible with
            `ConnectionLike`.
          load_options: Optional Pydantic model with load configuration.
          save_options: Optional Pydantic model with save configuration.

        Side Effects:
          May create a local directory if the inferred base URI is a
          `file://` path that does not exist.

        """
        if isinstance(conn, dict):
            self.conn = ConnectionLike(conn)
        else:
            self.conn = conn
        self.extras = self.conn.extra_dejson
        # For 'fs', prefer extras["base_uri"];
        #  fallback to extras["base_path"] for local paths.
        # For GCP, prefer extras["base_uri"]; else build from bucket: gs://{bucket}
        self.base_uri = self._infer_base_uri()
        if self.base_uri and self.base_uri.startswith("file://"):
            Path(self.base_uri[len("file://") :]).mkdir(exist_ok=True)

        self.load_options = load_options
        self.save_options = save_options

    @classmethod
    def from_conn_id(
        cls: type["BaseDataset[L, S]"],
        conn_id: str,
        *,
        load_options: L | None = None,
        save_options: S | None = None,
    ) -> "BaseDataset[L, S]":
        """Construct an instance by looking up an Airflow connection ID.

        Uses `airflow.hooks.base.BaseHook` (or the SDK alternative) to fetch
        the connection and then calls the class constructor.

        Args:
          conn_id: Airflow connection ID to resolve.
          load_options: Optional load options model.
          save_options: Optional save options model.

        Returns:
          A fully constructed `BaseDataset` subclass instance.
        """
        if BaseHook is None:
            raise RuntimeError("Airflow not available. Install smallcat[airflow]")  # noqa: TRY003, EM101

        conn = BaseHook.get_connection(conn_id)
        return cls(conn=conn, load_options=load_options, save_options=save_options)

    # ---------- Public API ----------

    def load_arrow_table(
        self,
        path: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> pa.Table:
        """Load data as a PyArrow table.

        Default implementation delegates to
        :meth:`load_arrow_record_batch_reader` (optionally with a filter) and
        consumes the resulting batches into a `pyarrow.Table`.

        Args:
          path: Relative dataset path (joined under the connection's base).
            Use `self._full_uri(path)` for the fully-qualified location and
            `self._duckdb_conn()` for an isolated DuckDB connection.
          where: Optional SQL filter predicate injected into the query.
          columns: Optional list of columns to project.

        Returns:
          A `pyarrow.Table` with the loaded data.
        """
        reader = self.load_arrow_record_batch_reader(
            path=path,
            where=where,
            columns=columns,
        )
        return reader.read_all()

    @abstractmethod
    def load_arrow_record_batch_reader(
        self,
        path: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> pa.RecordBatchReader:
        """Stream data as a RecordBatchReader with an optional SQL filter.

        Args:
          path: Relative dataset path.
          where: Optional SQL filter predicate injected into the query, e.g.
            "event_date > '2024-01-01'". Implementations should handle an
            empty string by returning all rows.
          columns: Optional list of columns to select.

        Returns:
          A `pyarrow.RecordBatchReader` yielding filtered batches.
        """
        ...

    @abstractmethod
    def save_arrow_table(self, path: str, table: pa.Table) -> None:
        """Persist a PyArrow table to the target destination.

        Args:
          path: Relative dataset path under the connection base.
          table: Table to write.

        Returns:
          None
        """
        ...

    def load_pandas(
        self,
        path: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load data as a pandas DataFrame.

        This is a convenience wrapper over `load_arrow_table` and pushes down
        filters when provided.

        Args:
          path: Relative dataset path.
          where: Optional SQL filter predicate injected into the query.
          columns: Optional list of columns to project.

        Returns:
          A pandas `DataFrame`.
        """
        arrow_table = self.load_arrow_table(
            path=path,
            where=where,
            columns=columns,
        )
        return arrow_table.to_pandas()

    def save_pandas(self, path: str, df: pd.DataFrame) -> None:
        """Persist a pandas DataFrame.

        Converts the DataFrame to a `pyarrow.Table` and delegates to
        `save_arrow_table`.

        Args:
            path: Relative dataset path.
            df: DataFrame to persist.

        Returns:
            None
        """
        arrow_table = pa.Table.from_pandas(df)
        self.save_arrow_table(path, table=arrow_table)

    def save_options_dict(self) -> dict:
        """Serialize save options to a plain dict.

        Uses `Pydantic.model_dump(exclude_unset=True)` when available.

        Returns:
          A dict of save options, or an empty dict if unset.
        """
        if self.save_options:
            return self.save_options.model_dump(exclude_unset=True)  # type: ignore[attr-defined]
        return {}

    def load_options_dict(self) -> dict:
        """Serialize load options to a plain dict.

        Uses `Pydantic.model_dump(exclude_unset=True)` when available.

        Returns:
          A dict of load options, or an empty dict if unset.
        """
        if self.load_options:
            return self.load_options.model_dump(exclude_unset=True)  # type: ignore[attr-defined]
        return {}

    # ---------- Helpers available to subclasses ----------

    def _full_uri(self, rel_path: str) -> str:
        """Build a fully-qualified URI for a relative path.

        Args:
          rel_path: Path relative to the connection's base.

        Returns:
          A fully-qualified URI (e.g., `file://...`, `gs://bucket/...`),
          or a normalized relative POSIX path if no base is known.
        """
        if not self.base_uri:
            return to_relative_posix_path(rel_path)
        return norm_join_uri(self.base_uri, rel_path)

    def _select_clause(self, columns: list[str] | None) -> str:
        """Return a sanitized SELECT clause for DuckDB queries.

        Only allows simple identifiers (with an optional single dot) to avoid
        basic SQL injection. Raises ``ValueError`` on invalid column names.
        """
        if not columns:
            return "*"

        ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
        for col in columns:
            if not ident_re.match(col):
                msg = "Unsafe column name provided"
                raise ValueError(msg)
        return ", ".join(columns)

    def _validated_where(self, where: str | None) -> str | None:
        """Validate a WHERE predicate by parsing it with DuckDB.

        The predicate is parsed as part of ``SELECT * FROM data WHERE (...)``
        and the resulting AST is inspected. It must be a single, syntactically
        valid expression (so ``;`` and unbalanced comments are rejected by the
        parser itself, while string literals such as ``'a--b'`` are fine) and
        it must not contain subqueries, which are the way to reach table
        functions such as ``read_text`` or other tables. The statement must
        remain a bare ``SELECT * FROM data WHERE ...``, so predicates that
        close the parenthesis to append ``UNION``, ``LIMIT``, ``GROUP BY``,
        etc. are rejected.

        Raises:
          ValueError: If the predicate does not parse, contains a subquery or
            alters the shape of the query.
        """
        if not where:
            return None

        probe = f"SELECT * FROM data WHERE ({where})"  # noqa: S608  (parsed, never run)
        with duckdb.connect() as con:
            try:
                raw = con.execute("SELECT json_serialize_sql(?)", [probe]).fetchone()
            except duckdb.Error as exc:
                msg = "Unsafe or invalid WHERE clause"
                raise ValueError(msg) from exc
        ast = json.loads(raw[0]) if raw else {"error": True}
        if ast.get("error") or not _is_plain_filter(ast) or _contains_subquery(ast):
            msg = "Unsafe or invalid WHERE clause"
            raise ValueError(msg)
        return where

    def _build_query(
        self,
        table_name: str,
        columns: list[str] | None,
        where: str | None,
    ) -> str:
        """Construct a SELECT statement using SQLAlchemy for safer quoting."""
        where = self._validated_where(where)
        select_clause = self._select_clause(columns)

        metadata = MetaData()
        tbl = Table(table_name, metadata)
        cols: list[Any] = (
            [literal_column("*")]
            if select_clause == "*"
            else [literal_column(col) for col in select_clause.split(", ")]
        )

        stmt = select(*cols).select_from(tbl)
        if where:
            stmt = stmt.where(text(f"({where})"))
        return stmt.compile(compile_kwargs={"literal_binds": True}).string

    def _infer_base_uri(self) -> str | None:  # noqa: PLR0911
        """Infer a base URI from the connection configuration.

        Rules:
          * `fs`: use `extras.base_uri`; otherwise derive from
            `extras.base_path` (normalized to `file://`). If neither is provided
            returns `None`, so paths are resolved relative to the current
            working directory.
          * `google_cloud_platform` / `google`: use `extras.base_uri`; otherwise
            derive from `gs://{bucket}` and optional `prefix`.
          * Other/unknown types: fall back to `extras.base_uri` if present.

        Returns:
          The inferred base URI string, or `None` if it cannot be determined.
        """
        ctype = (self.conn.conn_type or "").lower()
        x = self.extras

        if ctype == "fs":
            if x.get("base_uri"):
                return x["base_uri"]
            # allow local base path
            base_path = x.get("base_path")
            if base_path:
                # Normalize to file:// for consistency
                if re.match(r"^[a-zA-Z0-9+.-]+://", base_path):
                    return base_path
                return f"file://{base_path}"
            # No base: paths stay relative to the current working directory.
            return None

        if ctype in {"google_cloud_platform", "google"}:
            if x.get("base_uri"):
                return x["base_uri"]
            bucket = x.get("bucket")
            prefix = x.get("prefix", "")
            if bucket:
                return norm_join_uri(f"gs://{bucket}", prefix)
            # Last resort: no bucket provided
            return None

        # Unknown type: try an explicit base_uri if provided
        return x.get("base_uri")

    def _duckdb_conn(self) -> duckdb.DuckDBPyConnection:
        """Create and configure a fresh DuckDB connection.

        The connection installs and loads common extensions (`httpfs`, `json`,
        `parquet`). For GCP-like connections, it also configures a GCS secret.

        Returns:
          A new, isolated `duckdb.DuckDBPyConnection` instance.

        Notes:
          Each call returns a brand-new connection to avoid cross-task
          interference.
        """
        con = duckdb.connect()
        # Common extensions
        con.install_extension("httpfs")
        con.load_extension("json")
        con.load_extension("parquet")
        con.load_extension("httpfs")

        ctype = (self.conn.conn_type or "").lower()
        if ctype in {"google_cloud_platform", "google"}:
            self._configure_gcp_secret(con)

        return con

    # ---------- Backend configuration ----------

    def _configure_gcp_secret(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create a DuckDB GCS secret from the connection's HMAC keys.

        DuckDB authenticates ``gs://`` paths through the S3-compatible API,
        which only accepts **HMAC keys**: `login` is used as the key ID and
        `password` as the secret. Service account credentials (`keyfile_dict`,
        `keyfile`, `key_path` in `extras`) are understood by delta-rs only, so
        they cannot authenticate CSV, Parquet or Excel datasets. If they are
        present without HMAC keys, a warning is emitted instead of silently
        reading anonymously.

        Args:
          con: An open DuckDB connection to configure.

        Warns:
          UserWarning: If only service account credentials are configured.
        """
        if not (self.conn.login and self.conn.password):
            if any(self.extras.get(key) for key in GCP_SERVICE_ACCOUNT_EXTRAS):
                warnings.warn(
                    "DuckDB-based datasets (CSV, Parquet, Excel) cannot use Google "
                    "service account credentials (keyfile_dict/keyfile/key_path): "
                    "they need HMAC keys in the connection's login/password. "
                    "Accessing gs:// paths anonymously.",
                    UserWarning,
                    stacklevel=2,
                )
            return
        con.execute(
            f"""
          CREATE OR REPLACE SECRET (
              TYPE gcs,
              KEY_ID {sql_string_literal(self.conn.login)},
              SECRET {sql_string_literal(self.conn.password)}
          );
          """,
        )
