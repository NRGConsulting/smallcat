"""Catalog utilities for data loading/saving.

This is a catalog in a similar style to Kedro's catalog, to abstract from
sources and formats in data loading and saving. This module lets you build a
catalog from a dictionary (which could be in an airflow variable).
"""

import typing
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from smallcat._airflow import get_base_hook
from smallcat.connections import ConnectionProtocol, SupportedConnectionSchemas
from smallcat.datasets.base_dataset import BaseDataset
from smallcat.datasets.csv_dataset import CSVDataset, CSVLoadOptions, CSVSaveOptions
from smallcat.datasets.delta_table_dataset import (
    DeltaTableDataset,
    DeltaTableLoadOptions,
    DeltaTableSaveOptions,
)
from smallcat.datasets.excel_dataset import (
    ExcelDataset,
    ExcelLoadOptions,
    ExcelSaveOptions,
)
from smallcat.datasets.parquet_dataset import (
    ParquetDataset,
    ParquetLoadOptions,
    ParquetSaveOptions,
)

if typing.TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


class EntryBase(BaseModel, ABC):
    """Base configuration shared by all catalog entries.

    Attributes:
        connection: Airflow connection ID (string) **or** a dict representing
            the connection configuration (e.g., credentials, file path, etc.).
    """

    model_config = ConfigDict(extra="forbid")

    connection: str | dict | SupportedConnectionSchemas = Field(
        ...,
        description="Airflow connection ID or dictionary representing a connection",
    )
    location: str = Field(..., description="Relative location of data")

    def get_connection(self) -> dict | ConnectionProtocol:
        """Resolve and return the connection for this entry.

        If `connection` is a string, it is treated as an Airflow connection ID
        and resolved via `BaseHook.get_connection` (compatible with Airflow 2 and 3).
        If `connection` is already a mapping/object implementing the connection
        protocol, it is returned as-is.

        Returns:
            dict | ConnectionProtocol: A resolved connection object or dictionary
            usable by datasets.
        """
        if isinstance(self.connection, str):
            base_hook = get_base_hook()
            if base_hook is None:
                msg = "Airflow is required to resolve a connection by its ID."
                raise ImportError(msg)
            return base_hook.get_connection(conn_id=self.connection)
        if isinstance(self.connection, SupportedConnectionSchemas):
            return self.connection.model_dump(by_alias=True)
        return self.connection

    @abstractmethod
    def build_dataset(self) -> "BaseDataset":
        """Construct and return the concrete dataset for this entry."""
        raise NotImplementedError

    def load_pandas(
        self,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> "pd.DataFrame":
        """Load this entry's dataset into a pandas DataFrame.

        This method builds the concrete dataset via :meth:`build_dataset` and
        delegates to its ``load_pandas`` method using this entry's ``location``.
        Any dataset-specific load options configured on the entry are respected.

        Args:
            where: Optional SQL filter predicate forwarded to the dataset.
            columns: Optional list of columns to project.

        Returns:
            pd.DataFrame: The loaded tabular data.

        Raises:
            FileNotFoundError: If the target path/table at ``location`` does not exist.
            ValueError: If the data cannot be parsed as tabular data.
            Exception: Any other error raised by the underlying dataset implementation.
        """
        return self.build_dataset().load_pandas(
            self.location,
            where=where,
            columns=columns,
        )

    def save_pandas(self, df: "pd.DataFrame") -> None:
        """Save a pandas DataFrame to this entry's dataset location.

        This method builds the concrete dataset via :meth:`build_dataset` and
        delegates to its ``save_pandas`` method using this entry's ``location``.
        Any dataset-specific save options configured on the entry are respected.

        Args:
            df (pd.DataFrame): The DataFrame to persist.

        Raises:
            PermissionError: If the target cannot be written to.
            ValueError: If the DataFrame is incompatible with the target format/options.
            Exception: Any other error raised by the underlying dataset implementation.
        """
        self.build_dataset().save_pandas(self.location, df)

    def load_arrow(
        self,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> "pa.Table":
        """Load this entry's dataset as an Apache Arrow Table.

        This method builds the concrete dataset via :meth:`build_dataset` and
        delegates to its `load_arrow_table`` method using this entry's `location`.
        Any dataset-specific load options configured on the entry are respected.

        Args:
            where: Optional SQL filter predicate forwarded to the dataset.
            columns: Optional list of columns to project.

        Returns:
            pa.Table: The loaded Arrow table.

        Raises:
            FileNotFoundError: If the source does not exist at the target location.
            PermissionError: If the source cannot be read.
            ValueError: If the source is incompatible with Arrow or configured options.
            Exception: Any other error raised by the underlying dataset implementation.
        """
        return self.build_dataset().load_arrow_table(
            self.location,
            where=where,
            columns=columns,
        )

    def save_arrow(self, table: "pa.Table") -> None:
        """Save an Apache Arrow Table to this entry's dataset location.

        This method builds the concrete dataset via :meth:`build_dataset` and
        delegates to its `save_arrow_table` method using this entry's `location`.
        Any dataset-specific save options configured on the entry are respected.

        Args:
            table (pa.Table): The Arrow table to persist.

        Raises:
            PermissionError: If the target cannot be written to.
            ValueError: If the table is incompatible with the target format/options.
            Exception: Any other error raised by the underlying dataset implementation.
        """
        self.build_dataset().save_arrow_table(self.location, table)


class CSVEntry(EntryBase):
    """Catalog entry describing a CSV dataset.

    Attributes:
        file_format: Literal string identifying the file format: `'csv'`.
        load_options: Options controlling CSV *reading* (see `CSVLoadOptions`).
        save_options: Options controlling CSV *writing* (see `CSVSaveOptions`).
    """

    file_format: Literal["csv"] = "csv"
    load_options: CSVLoadOptions | None = None
    save_options: CSVSaveOptions | None = None

    def build_dataset(self) -> CSVDataset:
        """Build a :class:`CSVDataset` using this entry's configuration.

        Returns:
            CSVDataset: A dataset configured with the resolved connection and options.
        """
        return CSVDataset(
            conn=self.get_connection(),
            load_options=self.load_options,
            save_options=self.save_options,
        )


class ExcelEntry(EntryBase):
    """Catalog entry describing an Excel dataset.

    Attributes:
        file_format: Literal string identifying the file format: `'excel'`.
        load_options: Options controlling Excel *reading* (see `ExcelLoadOptions`).
        save_options: Options controlling Excel *writing* (see `ExcelSaveOptions`).
    """

    file_format: Literal["excel"] = "excel"
    load_options: ExcelLoadOptions | None = None
    save_options: ExcelSaveOptions | None = None

    def build_dataset(self) -> ExcelDataset:
        """Build an :class:`ExcelDataset` using this entry's configuration.

        Returns:
            ExcelDataset: A dataset configured with the resolved connection and options.
        """
        return ExcelDataset(
            conn=self.get_connection(),
            load_options=self.load_options,
            save_options=self.save_options,
        )


class ParquetEntry(EntryBase):
    """Catalog entry describing a Parquet dataset.

    Attributes:
        file_format: Literal string identifying the file format: `'parquet'`.
        load_options: Optional configuration controlling Parquet *reading*
            behavior (see :class:`ParquetLoadOptions`).
        save_options: Optional configuration controlling Parquet *writing*
            behavior (see :class:`ParquetSaveOptions`).
    """

    file_format: Literal["parquet"] = "parquet"
    load_options: ParquetLoadOptions | None = None
    save_options: ParquetSaveOptions | None = None

    def build_dataset(self) -> ParquetDataset:
        """Build a :class:`ParquetDataset` using this entry's configuration.

        Returns:
            ParquetDataset: A dataset configured with the resolved connection
            and Parquet-specific options.
        """
        return ParquetDataset(
            conn=self.get_connection(),
            load_options=self.load_options,
            save_options=self.save_options,
        )


class DeltaTableEntry(EntryBase):
    """Catalog entry describing a Delta Lake table dataset.

    This entry specifies configuration for reading from or writing to Delta
    Lake tables, typically stored on local or cloud-backed storage. It includes
    both connection details and Delta-specific load/save options.

    Attributes:
        file_format: Literal string identifying the file format: `'delta_table'`.
        load_options: Optional configuration controlling Delta table *reading*
            behavior (see :class:`DeltaTableLoadOptions`).
        save_options: Optional configuration controlling Delta table *writing*
            behavior (see :class:`DeltaTableSaveOptions`).
    """

    file_format: Literal["delta_table"] = "delta_table"
    load_options: DeltaTableLoadOptions | None = None
    save_options: DeltaTableSaveOptions | None = None

    def build_dataset(self) -> DeltaTableDataset:
        """Build a :class:`DeltaTableDataset` using this entry's configuration.

        Returns:
            DeltaTableDataset: A dataset configured with the resolved connection
            and Delta Lake options.
        """
        return DeltaTableDataset(
            conn=self.get_connection(),
            load_options=self.load_options,
            save_options=self.save_options,
        )


CatalogEntry = CSVEntry | ExcelEntry | ParquetEntry | DeltaTableEntry


class Catalog(BaseModel):
    """A collection of named datasets with associated loader configuration.

    The catalog maps user-defined keys to concrete dataset entries (e.g., CSV or
    Excel). It can be constructed from an in-memory dictionary, an Airflow
    Variable (JSON), or a YAML file.

    Attributes:
        entries: Mapping of dataset names to their configurations.
    """

    model_config = ConfigDict(extra="forbid")

    entries: dict[
        str,
        Annotated[CatalogEntry, Field(discriminator="file_format")],
    ] = Field(
        ...,
        description="Named data sets",
    )

    @staticmethod
    def from_dict(dictionary: dict) -> "Catalog":
        """Create a catalog from a Python dictionary.

        The dictionary must conform to the `Catalog` schema (i.e., include an
        `entries` key mapping names to valid `CatalogEntry` objects).

        Args:
            dictionary: A dictionary matching the `Catalog` model.

        Returns:
            Catalog: A validated `Catalog` instance.

        Raises:
            pydantic.ValidationError: If the dictionary does not match the schema.
        """
        return Catalog.model_validate(dictionary)

    @staticmethod
    def from_airflow_variable(variable_id: str) -> "Catalog":
        """Create a catalog from an Airflow Variable containing JSON.

        The Airflow Variable should contain a JSON object compatible with the
        `Catalog` schema.

        Args:
            variable_id: The Airflow Variable ID to read (expects JSON).

        Returns:
            Catalog: A `Catalog` instance constructed from the Variable value.

        Raises:
            KeyError: If the Airflow Variable does not exist.
            pydantic.ValidationError: If the JSON payload is invalid for the model.
        """
        try:
            from airflow.sdk import Variable
        except ImportError:
            from airflow.models import Variable  # type: ignore[attr-defined,no-redef] # noqa: I001

        try:
            dictionary_entries = Variable.get(variable_id, deserialize_json=True)
        except TypeError:
            # LocalFilesystemBackend can return an object causing a TypeError
            # In this case we don't need to deserialize into JSON
            #  as it's not a string
            dictionary_entries = Variable.get(variable_id)
        except ImportError as e:
            # Airflow fails with import error if variable is not present and tries
            #  to talk to the Task Supervisor (the runner process) over an internal
            #  comms channel (SUPERVISOR_COMMS) to fetch it.
            msg = f"Variable {variable_id} not found in Airflow"
            raise KeyError(msg) from e
        return Catalog.from_dict(dictionary_entries)

    @staticmethod
    def from_yaml(dictionary_path: str | Path) -> "Catalog":
        """Create a catalog from a YAML file.

        Args:
            dictionary_path: Path to a YAML file whose content matches the
                `Catalog` schema.

        Returns:
            Catalog: A `Catalog` instance constructed from the YAML content.

        Raises:
            FileNotFoundError: If the YAML file cannot be found.
            pydantic.ValidationError: If the YAML content is invalid for the model.
        """
        with Path(dictionary_path).open() as f:
            catalog_dict = yaml.safe_load(f)
        return Catalog.from_dict(catalog_dict)

    def _get_entry(self, key: str) -> EntryBase:
        try:
            return self.entries[key]
        except KeyError as e:
            msg = f"Entry {key} not found in dictionary"
            raise KeyError(msg) from e

    def get_dataset(self, key: str) -> BaseDataset:
        """Instantiate a concrete dataset for a given catalog entry.

        Args:
            key: The name of the catalog entry to resolve.

        Returns:
            BaseDataset: A dataset instance ready to load/save the data.

        Raises:
            KeyError: If the key is not present in the catalog.
            ValueError: If the entry's `file_format` is not supported.
        """
        entry = self._get_entry(key)
        return entry.build_dataset()

    def load_pandas(
        self,
        key: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> "pd.DataFrame":
        """Load a dataset from the catalog into a pandas DataFrame.

        Resolves the catalog entry identified by ``key`` and delegates to
        :meth:`EntryBase.load_pandas`. This is equivalent to:

            ``self.entries[key].build_dataset().load_pandas(entry.location)``

        Args:
            key: The catalog entry name to load.
            where: Optional SQL filter predicate forwarded to the dataset.
            columns: Optional list of columns to project.

        Returns:
            pd.DataFrame: The loaded tabular data.

        Raises:
            KeyError: If ``key`` is not present in the catalog.
            Exception: Any error propagated from the underlying dataset's loader.
        """
        entry = self._get_entry(key)
        return entry.load_pandas(where=where, columns=columns)

    def save_pandas(self, key: str, df: "pd.DataFrame") -> None:
        """Save a pandas DataFrame to a dataset in the catalog.

        Resolves the catalog entry identified by ``key`` and delegates to
        :meth:`EntryBase.save_pandas`. This writes to the entry's configured
        ``location`` with any format-specific save options applied.

        Args:
            key: The catalog entry name to write to.
            df (pd.DataFrame): The DataFrame to persist.

        Raises:
            KeyError: If ``key`` is not present in the catalog.
            Exception: Any error propagated from the underlying dataset's saver.
        """
        entry = self._get_entry(key)
        entry.save_pandas(df)

    def load_arrow(
        self,
        key: str,
        where: str | None = None,
        columns: list[str] | None = None,
    ) -> "pa.Table":
        """Load a dataset from the catalog into an Apache Arrow Table.

        Resolves the catalog entry identified by `key` and delegates to
        :meth:`EntryBase.load_arrow`. This is equivalent to:

            `self.entries[key].build_dataset().load_arrow_table(entry.location)`

        Args:
            key: The catalog entry name to load.
            where: Optional SQL filter predicate forwarded to the dataset.
            columns: Optional list of columns to project.

        Returns:
            pa.Table: The loaded Arrow table.

        Raises:
            KeyError: If `key` is not present in the catalog.
            Exception: Any error propagated from the underlying dataset's loader.
        """
        entry = self._get_entry(key)
        return entry.load_arrow(where=where, columns=columns)

    def save_arrow(self, key: str, table: "pa.Table") -> None:
        """Save an Apache Arrow Table to a dataset in the catalog.

        Resolves the catalog entry identified by `key` and delegates to
        :meth:`EntryBase.save_arrow`. This writes to the entry's configured
        `location` with any format-specific save options applied.

        Args:
            key: The catalog entry name to write to.
            table (pa.Table): The Arrow table to persist.

        Raises:
            KeyError: If `key` is not present in the catalog.
            Exception: Any error propagated from the underlying dataset's saver.
        """
        entry = self._get_entry(key)
        entry.save_arrow(table)
