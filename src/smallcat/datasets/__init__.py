"""Dataset constructors for common table/file formats.

Re-exports the most common dataset classes so callers can import from
`smallcat.datasets` directly. The classes are imported on first access, so
importing this package does not load the dependencies of every format.

Available datasets:
  * :class:`ParquetDataset`
  * :class:`CSVDataset`
  * :class:`ExcelDataset`
  * :class:`DeltaTableDataset`
  * :class:`BaseDataset` (abstract)
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from smallcat.datasets.base_dataset import BaseDataset
    from smallcat.datasets.csv_dataset import CSVDataset
    from smallcat.datasets.delta_table_dataset import DeltaTableDataset
    from smallcat.datasets.excel_dataset import ExcelDataset
    from smallcat.datasets.parquet_dataset import ParquetDataset

_EXPORTS = {
    "BaseDataset": "smallcat.datasets.base_dataset",
    "CSVDataset": "smallcat.datasets.csv_dataset",
    "DeltaTableDataset": "smallcat.datasets.delta_table_dataset",
    "ExcelDataset": "smallcat.datasets.excel_dataset",
    "ParquetDataset": "smallcat.datasets.parquet_dataset",
}

__all__ = [
    "BaseDataset",
    "CSVDataset",
    "DeltaTableDataset",
    "ExcelDataset",
    "ParquetDataset",
]


def __getattr__(name: str) -> Any:  # noqa: ANN401
    """Import and return a re-exported dataset class on first access."""
    try:
        module_name = _EXPORTS[name]
    except KeyError:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg) from None
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value  # cache: later lookups bypass __getattr__
    return value


def __dir__() -> list[str]:
    """List the public names, including the lazily imported classes."""
    return sorted(__all__)
