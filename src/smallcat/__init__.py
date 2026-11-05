"""Smallcat — a lightweight, data catalog for data loading and saving.

Smallcat provides a simple abstraction over data I/O by describing datasets,
connections, and their options declaratively in YAML, while exposing a clean,
typed Python API for loading and saving data in code.

Concepts
--------
* **Datasets**
  Each entry in the catalog defines a file format (e.g., `parquet`, `csv`,
  `excel`, `delta`). The format maps to a Dataset class that knows how to
  read and write that file type using DuckDB and PyArrow under the hood.

* **Connections**
  Each dataset references a connection that provides its base URI and
  credentials. Connections follow the same structure as Airflow connections,
  but Smallcat includes its own lightweight adapter so it can also run
  standalone—without requiring Airflow to be installed.

* **Load and Save Options**
  Datasets can define optional `load` and `save` sections in YAML, which
  map to Pydantic models that validate the arguments passed to the underlying
  reader or writer.

Airflow Compatibility
---------------------
Smallcat aims to integrate smoothly with Airflow but can operate completely
independently.
If Airflow is present, connections may be resolved by ID through
`BaseHook.get_connection`.
If not, connection dictionaries are handled directly through Smallcat's
`ConnectionLike` adapter.

Key Features
------------
* Declarative configuration of datasets and connections in YAML.
* Unified, typed API to load or save data as PyArrow or Polars objects.
* Automatic URI resolution (e.g., `file://`, `gs://`) from connection info.
* Extensible dataset types—new formats can be added by subclassing
  :class:`smallcat.datasets.base_dataset.BaseDataset`.

Built-in Datasets
-----------------
Smallcat currently includes datasets for:
  * Parquet
  * CSV
  * Excel (.xlsx)
  * Delta Lake (delta-rs)

Each dataset class supports `load_arrow_table` and `save_arrow_table` for
consistent usage across all formats.

In short, Smallcat unifies the way your code accesses data across file formats
and environments—whether local, cloud, or orchestrated—while remaining simple,
typed, and dependency-light.
"""

import importlib
import typing

if typing.TYPE_CHECKING:
    from smallcat.connections_config import Connections
    from smallcat.models import (
        Model,
        ModelRegistry,
        ModelValidationMode,
        ModelWriteMode,
        use_registry,
    )
    from smallcat.models.registry import default_registry as registry

# Public names are resolved on first access, so `import smallcat` stays light:
# it does not import Polars, DuckDB or Airflow. Each maps to (module, attribute).
_LAZY_EXPORTS = {
    "Connections": ("smallcat.connections_config", "Connections"),
    "Model": ("smallcat.models", "Model"),
    "ModelRegistry": ("smallcat.models", "ModelRegistry"),
    "ModelValidationMode": ("smallcat.models", "ModelValidationMode"),
    "ModelWriteMode": ("smallcat.models", "ModelWriteMode"),
    "registry": ("smallcat.models.registry", "default_registry"),
    "use_registry": ("smallcat.models", "use_registry"),
}

__all__ = [
    "Connections",
    "Model",
    "ModelRegistry",
    "ModelValidationMode",
    "ModelWriteMode",
    "registry",
    "use_registry",
]


def __getattr__(name: str) -> object:
    """Resolve the public names of the package on first access."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        msg = f"module 'smallcat' has no attribute {name!r}"
        raise AttributeError(msg)
    module, attribute = target
    value = getattr(importlib.import_module(module), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """List the public names, including the lazy ones."""
    return sorted({*globals(), *_LAZY_EXPORTS})
