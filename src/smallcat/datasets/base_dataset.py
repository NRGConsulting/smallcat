"""Core dataset base class for Smallcat.

This module defines :class:`BaseDataset`, the abstract foundation for concrete
datasets (CSV, Parquet, Excel, Delta, etc.). It standardizes how a dataset:

* Receives an Airflow-like connection (or dict) and exposes its extras.
* Resolves relative paths to fully-qualified URIs (e.g., `file://`, `gs://`).
* Creates isolated DuckDB connections, loading extensions and provider secrets.
* Loads and saves data via a simple Arrow/Polars-friendly API.

Typical usage:
  >>> ds = MyDataset.from_conn_id("my_fs")
  >>> table = ds.load_arrow_table("bronze/my_table.parquet")
  >>> ds.save_polars("silver/my_table.parquet", pl.DataFrame(table))

Key concepts:
  * `_full_uri(rel_path)` joins relative paths to the connection's base URI.
  * `_duckdb_conn()` returns a fresh, configured DuckDB connection per call.
  * Subclasses must implement `load_arrow_record_batch_reader` and
    `save_arrow_table`.

Dependencies:
  duckdb, pyarrow, polars, pydantic, and Smallcat connection utilities.
"""

import json
import re
import unicodedata
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa
from pydantic import BaseModel

from smallcat._airflow import AIRFLOW_INSTALL_HINT, get_base_hook
from smallcat.connections import (
    AWS_CONN_TYPES,
    GCP_CONN_TYPES,
    GCP_SERVICE_ACCOUNT_EXTRAS,
    ConnectionLike,
    ConnectionProtocol,
    ConnType,
    validate_aws_credentials,
    validate_gcp_credentials,
)
from smallcat.path_utils import (
    local_path_from_uri,
    norm_join_uri,
    normalize_file_uri,
    to_file_uri,
    to_relative_posix_path,
)

# Credential chain used when DuckDB's default one fails: no `config` provider.
AWS_FALLBACK_CHAIN = "env;sso;process;web_identity;instance"


def ensure_extension(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """Load a DuckDB extension, installing it only if it is not available.

    DuckDB downloads an extension on `INSTALL` and caches it, so trying to load
    it first avoids a network round trip on every new connection and lets
    already-installed extensions work offline.

    Args:
      con: The DuckDB connection to load the extension into.
      name: Extension name, e.g. ``"httpfs"`` or ``"excel"``.

    Raises:
      duckdb.Error: If the extension is neither installed nor downloadable.
    """
    try:
        con.load_extension(name)
    except duckdb.Error:
        con.install_extension(name)
        con.load_extension(name)


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


def to_record_batch_reader(rel: duckdb.DuckDBPyRelation | None) -> pa.RecordBatchReader:
    """Return the result of a DuckDB relation as a PyArrow record batch reader.

    Uses ``DuckDBPyRelation.to_arrow_reader``, which replaces the deprecated
    ``fetch_record_batch`` (DuckDB 2). The DuckDB stubs type both the relation
    and the reader as optional; this is the one place where that is narrowed.

    Args:
        rel: The relation to read, such as the result of ``rel.query(...)``.

    Returns:
        A reader that streams the rows in record batches.

    Raises:
        RuntimeError: If DuckDB gives no relation or no reader, which would
            otherwise surface later as an obscure ``AttributeError``.
    """
    if rel is None:
        msg = "DuckDB returned no relation for the query"
        raise RuntimeError(msg)
    reader = rel.to_arrow_reader()
    if reader is None:
        msg = "DuckDB returned no Arrow reader for the query"
        raise RuntimeError(msg)
    return reader


class BaseDataset[L: BaseModel, S: BaseModel](ABC):
    """Base class for dataset loaders/savers backed by DuckDB and a connection.

    This class:
      * Wraps an Airflow-like connection (or connection dict).
      * Expands relative dataset paths into fully-qualified URIs using the
        connection's base settings.
      * Creates a fresh DuckDB connection per operation, configuring extensions
        and any provider-specific secrets (e.g., GCS).
      * Defines an abstract API to load and save Arrow tables, plus
        convenience Polars helpers.

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

        Note:
          The constructor has no side effects on the filesystem: local
          directories are created when saving, not when instantiating.

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
        base_hook = get_base_hook()
        if base_hook is None:
            msg = f"Airflow not available. {AIRFLOW_INSTALL_HINT}"
            raise RuntimeError(msg)

        conn = base_hook.get_connection(conn_id)
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

    def load_polars(
        self,
        path: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Load data as a Polars DataFrame.

        This is a convenience wrapper over `load_arrow_table` and pushes down
        filters when provided.

        Args:
          path: Relative dataset path.
          where: Optional SQL filter predicate injected into the query.
          columns: Optional list of columns to project.

        Returns:
          A Polars `DataFrame`.
        """
        arrow_table = self.load_arrow_table(
            path=path,
            where=where,
            columns=columns,
        )
        return pl.DataFrame(arrow_table)

    def save_polars(self, path: str, df: pl.DataFrame) -> None:
        """Persist a Polars DataFrame.

        Converts the DataFrame to a `pyarrow.Table` and delegates to
        `save_arrow_table`.

        Args:
            path: Relative dataset path.
            df: DataFrame to persist.

        Returns:
            None
        """
        self.save_arrow_table(path, table=df.to_arrow())

    def scan_polars(
        self,
        path: str,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Return a Polars `LazyFrame` for the dataset at ``path``.

        The default loads the data eagerly with :meth:`load_polars` and wraps it
        with ``.lazy()``, so the API is uniform but nothing is skipped on read.
        Formats that can avoid reading unneeded data (Parquet, Delta) override
        this with a real scan that pushes projections and filters down to the
        storage layer. Filter with Polars expressions on the result, e.g.
        ``ds.scan_polars(p).filter(pl.col("x") > 1).collect()``.

        Args:
          path: Relative dataset path.
          columns: Optional list of columns to project.

        Returns:
          A `polars.LazyFrame`.
        """
        return self.load_polars(path, columns=columns).lazy()

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

    @staticmethod
    def _ensure_parent_dir(full_uri: str) -> None:
        """Create the parent directory of a local destination before writing.

        Remote URIs (`gs://`, `s3://`, ...) are left alone: object stores have no
        directories to create.
        """
        local = local_path_from_uri(full_uri)
        if local:
            Path(local).parent.mkdir(parents=True, exist_ok=True)

    def _select_columns(self, columns: list[str] | None) -> list[str]:
        """Return the columns of a SELECT as quoted SQL identifiers.

        Every name is a literal column name: it is quoted (inner double quotes
        are doubled), so reserved words (``order``), non-ASCII names (``año``)
        and names with spaces work, and a dot is part of the name instead of a
        qualifier. Names that are empty, or contain a double quote, a
        semicolon or a control character, are refused.

        Args:
            columns: The requested column names, or ``None`` / empty for all.

        Returns:
            The quoted identifiers, or ``["*"]`` when no columns were requested.

        Raises:
            ValueError: If a column name is not acceptable.
        """
        if not columns:
            return ["*"]

        for col in columns:
            if (
                not col
                or any(c in col for c in ('"', ";"))
                or any(unicodedata.category(c) == "Cc" for c in col)
            ):
                msg = "Unsafe column name provided"
                raise ValueError(msg)
        return ['"' + col.replace('"', '""') + '"' for col in columns]

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
        """Construct ``SELECT <columns> FROM <table> [WHERE (<where>)]``.

        The text is built directly: the columns are quoted by
        :meth:`_select_columns`, the predicate is parsed by DuckDB in
        :meth:`_validated_where` and ``table_name`` is always an internal
        relation name. A SQL builder such as SQLAlchemy adds nothing here, and
        its ``text()`` reads ``:name`` inside a string literal as a bind
        parameter and rewrites it to ``NULL``.

        Args:
            table_name: The relation to select from (``"data"``).
            columns: The columns to select, or ``None``/empty for all of them.
            where: An optional SQL predicate, validated before use.

        Returns:
            The SQL text, with the predicate exactly as it was given.

        Raises:
            ValueError: If a column name or the predicate is not acceptable.
        """
        where = self._validated_where(where)
        select_list = ", ".join(self._select_columns(columns))
        query = f"SELECT {select_list} FROM {table_name}"  # noqa: S608  (checked parts)
        if where:
            query += f" WHERE ({where})"
        return query

    @property
    def _conn_type(self) -> ConnType | None:
        """The connection type as a :class:`ConnType`, ignoring case.

        ``None`` when the type is missing or not one smallcat knows, so callers
        can tell "not this type" apart from "no usable type".
        """
        return ConnType.parse(self.conn.conn_type)

    def _infer_base_uri(self) -> str | None:  # noqa: PLR0911, C901
        """Infer a base URI from the connection configuration.

        Rules:
          * `fs`: use `extras.base_uri`; otherwise derive from
            `extras.base_path` (normalized to `file://`). If neither is provided
            returns `None`, so paths are resolved relative to the current
            working directory.
          * `google_cloud_platform` / `google`: use `extras.base_uri`; otherwise
            derive from `gs://{bucket}` and optional `prefix`.
          * `aws` / `s3`: use `extras.base_uri`; otherwise derive from
            `s3://{bucket}` and optional `prefix`.
          * Other/unknown types: fall back to `extras.base_uri` if present.

        Returns:
          The inferred base URI string, or `None` if it cannot be determined.
        """
        ctype = self._conn_type
        x = self.extras

        if ctype is ConnType.FS:
            if x.get("base_uri"):
                return normalize_file_uri(x["base_uri"])
            # allow local base path
            base_path = x.get("base_path")
            if base_path:
                # Normalize to file:// for consistency
                if re.match(r"^[a-zA-Z0-9+.-]+://", base_path):
                    return normalize_file_uri(base_path)
                return to_file_uri(base_path)
            # No base: paths stay relative to the current working directory.
            return None

        if ctype in GCP_CONN_TYPES:
            if x.get("base_uri"):
                return x["base_uri"]
            bucket = x.get("bucket")
            prefix = x.get("prefix", "")
            if bucket:
                return norm_join_uri(f"gs://{bucket}", prefix)
            # Last resort: no bucket provided
            return None

        if ctype in AWS_CONN_TYPES:
            if x.get("base_uri"):
                return x["base_uri"]
            bucket = x.get("bucket")
            if bucket:
                return norm_join_uri(f"s3://{bucket}", x.get("prefix", ""))
            return None

        # Unknown type: try an explicit base_uri if provided
        return x.get("base_uri")

    def _needs_httpfs(self) -> bool:
        """Return True if DuckDB needs `httpfs` to reach this dataset's storage.

        True for Google and AWS connections (their secrets are provided by
        `httpfs`) and
        for any base URI with a scheme other than ``file://`` (``gs://``,
        ``s3://``, ``https://``...). Local paths do not need it.
        """
        if self._conn_type in GCP_CONN_TYPES | AWS_CONN_TYPES:
            return True
        scheme = re.match(r"^([a-zA-Z][a-zA-Z0-9+.-]*)://", self.base_uri or "")
        return scheme is not None and scheme.group(1).lower() != "file"

    def _duckdb_conn(self) -> duckdb.DuckDBPyConnection:
        """Create and configure a fresh DuckDB connection.

        The connection loads the `json` and `parquet` extensions. `httpfs` is
        loaded only for remote storage (see :meth:`_needs_httpfs`), so local
        files work without network access. For GCP-like and AWS connections, it
        also configures the matching secret.

        Returns:
          A new, isolated `duckdb.DuckDBPyConnection` instance.

        Notes:
          Each call returns a brand-new connection to avoid cross-task
          interference.
        """
        con = duckdb.connect()
        con.load_extension("json")
        con.load_extension("parquet")
        if self._needs_httpfs():
            ensure_extension(con, "httpfs")

        ctype = self._conn_type
        if ctype in GCP_CONN_TYPES:
            self._configure_gcp_secret(con)
        elif ctype in AWS_CONN_TYPES:
            self._configure_s3_secret(con)

        return con

    # ---------- Backend configuration ----------

    def _aws_connection_settings(self) -> dict[str, Any]:
        """Return the validated S3 settings of the connection.

        Returns:
          A dict with `region`, `endpoint` (host[:port]), `use_ssl`, `profile`,
          `key_id`, `secret` and `session_token`; unset values are `None`.

        Raises:
          ValueError: If the credential settings contradict each other.
        """
        x = self.extras
        validate_aws_credentials(self.conn.login, self.conn.password, x)
        endpoint = None
        use_ssl = True
        if x.get("endpoint_url"):
            url = x["endpoint_url"]
            match = re.match(r"^(?:(https?)://)?([^/]+)", url)
            endpoint = match.group(2) if match else url
            use_ssl = not (match and match.group(1) == "http")
        return {
            "region": x.get("region_name"),
            "endpoint": endpoint,
            "use_ssl": use_ssl,
            "profile": x.get("profile_name"),
            "key_id": self.conn.login,
            "secret": self.conn.password,
            "session_token": x.get("aws_session_token"),
        }

    def _object_store_options(self) -> dict:
        """Build `storage_options` for the Rust object-store engines.

        delta-rs (Delta reads/writes) and Polars (``scan_*``) use the same
        ``object_store`` crate, so one set of options serves both. They are
        derived from the active connection:
          * `fs` (local): returns `{}`.
          * `google_cloud_platform` / `google`: uses one of:
              - `extras.keyfile_dict` (dict or JSON string)
              - `extras.keyfile` (raw JSON string)
              - `extras.key_path` (path on worker)
          * `aws` / `s3`: see :meth:`_aws_object_store_options`.

        Returns:
          A mapping for the `storage_options` parameter of
          `deltalake.DeltaTable`, `deltalake.write_deltalake` and
          `polars.scan_*`. For GCS, keys include one of:
            * `google_service_account_key` (serialized JSON)
            * `google_service_account` (path to keyfile)

        Warns:
          UserWarning: If a Google connection only has HMAC keys (`login` /
            `password`), which these engines do not support.

        Raises:
          ValueError: If the connection type is not supported.
        """
        if self._conn_type in AWS_CONN_TYPES:
            return self._aws_object_store_options()
        if self._conn_type is not ConnType.FS and self._conn_type not in GCP_CONN_TYPES:
            msg = f"Storage options not implemented for type {self.conn.conn_type}"
            raise ValueError(
                msg,
            )
        x = self.extras
        if self._conn_type is not ConnType.FS:
            validate_gcp_credentials(self.conn.login, self.conn.password, x)

        # keyfile_dict can be dict or JSON string
        kfd = x.get("keyfile_dict")
        if isinstance(kfd, str):
            try:
                kfd = json.loads(kfd)
            except json.JSONDecodeError:
                # If it's not JSON, ignore and fall through
                kfd = None

        if isinstance(kfd, dict) and kfd:
            # Provide serialized key via 'google_service_account_key'
            return {"google_service_account_key": json.dumps(kfd)}

        if x.get("keyfile"):  # raw JSON string
            return {"google_service_account_key": x["keyfile"]}

        if x.get("key_path"):  # path on worker
            return {"google_service_account": x["key_path"]}

        is_local = self._conn_type is ConnType.FS
        if self.conn.login and self.conn.password and not is_local:
            warnings.warn(
                "Delta tables and Polars scans cannot use HMAC keys (login/password) "
                "for Google Cloud Storage: provide a service account in keyfile_dict, "
                "keyfile or key_path. Accessing without credentials.",
                UserWarning,
                stacklevel=2,
            )
        return {}

    def _aws_object_store_options(self) -> dict:
        """Build object-store `storage_options` for an S3 connection.

        Static keys are passed as they are. Without keys, the engine finds IAM
        role credentials by itself (environment, web identity, ECS, instance
        profile). If the connection names a `profile_name`, the credentials are
        resolved first through DuckDB's AWS credential chain.

        Returns:
          A mapping with `aws_*` keys.

        Raises:
          ValueError: If the credential settings contradict each other.
        """
        s = self._aws_connection_settings()
        options: dict[str, str] = {}
        if s["region"]:
            options["aws_region"] = s["region"]
        if s["endpoint"]:
            scheme = "https" if s["use_ssl"] else "http"
            options["aws_endpoint"] = f"{scheme}://{s['endpoint']}"
            if not s["use_ssl"]:
                options["aws_allow_http"] = "true"
        if s["key_id"]:
            options["aws_access_key_id"] = s["key_id"]
            options["aws_secret_access_key"] = s["secret"]
            if s["session_token"]:
                options["aws_session_token"] = s["session_token"]
        elif s["profile"]:
            options.update(self._resolve_aws_credentials())
        return options

    def _configure_s3_secret(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create a DuckDB S3 secret from the connection.

        With `login` and `password` the secret holds those static keys (plus the
        optional session token). Without them it uses the AWS credential chain,
        which is how **IAM roles** work: an instance profile, an ECS task role,
        a web identity (EKS service accounts), environment variables or SSO. A
        `profile_name` restricts it to that profile, which may itself assume an
        IAM role (`role_arn` + `source_profile` in the AWS config file).

        Args:
          con: An open DuckDB connection to configure.

        Raises:
          ValueError: If the credential settings contradict each other.
        """
        s = self._aws_connection_settings()
        options = []
        if s["key_id"]:
            options += [
                f"KEY_ID {sql_string_literal(s['key_id'])}",
                f"SECRET {sql_string_literal(s['secret'])}",
            ]
            if s["session_token"]:
                token = sql_string_literal(s["session_token"])
                options.append(f"SESSION_TOKEN {token}")
        else:
            ensure_extension(con, "aws")
            options.append("PROVIDER credential_chain")
            if s["profile"]:
                options += [
                    "CHAIN 'config'",
                    f"PROFILE {sql_string_literal(s['profile'])}",
                ]
        if s["region"]:
            options.append(f"REGION {sql_string_literal(s['region'])}")
        if s["endpoint"]:
            options += [
                f"ENDPOINT {sql_string_literal(s['endpoint'])}",
                "URL_STYLE 'path'",
                f"USE_SSL {'true' if s['use_ssl'] else 'false'}",
            ]
        try:
            self._create_s3_secret(con, options)
        except duckdb.Error:
            if s["key_id"] or s["profile"]:
                raise
            # DuckDB's default chain also reads the `default` profile and fails
            # when the AWS config file exists without one, even if the
            # environment or an IAM role would provide the credentials.
            self._create_s3_secret(con, [*options, f"CHAIN '{AWS_FALLBACK_CHAIN}'"])

    @staticmethod
    def _create_s3_secret(con: duckdb.DuckDBPyConnection, options: list[str]) -> None:
        con.execute(f"CREATE OR REPLACE SECRET (TYPE s3, {', '.join(options)});")

    def _resolve_aws_credentials(self) -> dict[str, str]:
        """Resolve the temporary credentials of the AWS credential chain.

        delta-rs understands environment variables, web identity, ECS and
        instance-profile credentials on its own, but not named profiles or SSO.
        For those, the chain is resolved by DuckDB's AWS extension on a
        throwaway connection and the result is handed to delta-rs. The
        credentials are temporary: they are resolved when the dataset is used
        and are not refreshed during a long operation.

        Returns:
          `aws_access_key_id`, `aws_secret_access_key` and, when present,
          `aws_session_token`.

        Raises:
          RuntimeError: If the chain yields no credentials.
        """
        con = duckdb.connect(config={"allow_unredacted_secrets": True})
        try:
            ensure_extension(con, "httpfs")
            self._configure_s3_secret(con)
            row = con.execute(
                "SELECT secret_string FROM duckdb_secrets(redact = false) "
                "WHERE type = 's3' AND secret_string LIKE '%;key_id=%'",
            ).fetchone()
        finally:
            con.close()
        parts = (row[0] if row else "").split(";")
        fields = dict(part.split("=", 1) for part in parts if "=" in part)
        if not fields.get("key_id") or not fields.get("secret"):
            msg = "The AWS credential chain did not return any credentials."
            raise RuntimeError(msg)
        creds = {
            "aws_access_key_id": fields["key_id"],
            "aws_secret_access_key": fields["secret"],
        }
        if fields.get("session_token"):
            creds["aws_session_token"] = fields["session_token"]
        return creds

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

        Raises:
          ValueError: If the credential settings are incoherent.

        Warns:
          UserWarning: If only service account credentials are configured.
        """
        validate_gcp_credentials(self.conn.login, self.conn.password, self.extras)
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
