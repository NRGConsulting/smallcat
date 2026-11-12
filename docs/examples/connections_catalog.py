# ruff: noqa: D103, INP001, T201
"""Load a connection and use it from a catalog.

Run it with ``python docs/examples/connections_catalog.py``. It writes its
connections file, its catalog and its data under a temporary directory.
"""

import tempfile
from pathlib import Path

import polars as pl

from smallcat import Connections
from smallcat.catalog import Catalog
from smallcat.connections_config import ConnectionNotLoadedError

workdir = Path(tempfile.mkdtemp(prefix="smallcat-conn-catalog-"))


def title(text: str) -> None:
    print(f"\n=== {text}")


# 1. The connection: defined once, in a file -----------------------------------
(workdir / "connections.yaml").write_text(
    f"""
connections:
  lake:
    conn_type: fs
    extra:
      base_path: {(workdir / "lake").as_posix()}
""",
)
conns = Connections.from_yaml(workdir / "connections.yaml")
print(conns)

# 2. The catalog: each entry names the connection ------------------------------
(workdir / "catalog.yaml").write_text(
    """
entries:
  people:
    file_format: csv
    connection: lake
    location: people.csv
    load_options: {header: true}
    save_options: {header: true}
""",
)
catalog = Catalog.from_yaml(workdir / "catalog.yaml", connections=conns)

# 3. Basic use -----------------------------------------------------------------
title("save and load")
df = pl.DataFrame({"id": [1, 2, 3], "name": ["ada", "linus", "grace"]})
catalog.save_polars("people", df)
print("load           ", catalog.load_polars("people").rows())
print(
    "where + columns",
    catalog.load_polars("people", where="id > 1", columns=["name"]).rows(),
)

# 4. A name that was never loaded ----------------------------------------------
title("an entry that names a connection that is not loaded")
try:
    Catalog.from_dict(
        {
            "entries": {
                "sales": {
                    "file_format": "csv",
                    "connection": "warehouse",
                    "location": "sales.csv",
                },
            },
        },
        connections=conns,
    )
except ConnectionNotLoadedError as e:
    print(type(e).__name__, "-", e)
