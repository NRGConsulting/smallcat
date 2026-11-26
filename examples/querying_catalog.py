# ruff: noqa: CPY001, D103, INP001, PLR2004, T201
"""Querying a catalog, from the connection to every way of reading.

Run it with ``python docs/examples/querying_catalog.py``. It writes its
connections file, its catalog and its data under a temporary directory.
"""

import tempfile
from datetime import date
from pathlib import Path

import polars as pl

from smallcat import Connections
from smallcat.catalog import Catalog

workdir = Path(tempfile.mkdtemp(prefix="smallcat-catalog-"))


def title(text: str) -> None:
    print(f"\n=== {text}")


# 1. Load the connection -------------------------------------------------------
(workdir / "connections.yaml").write_text(
    f"""
connections:
  lake:
    conn_type: fs
    extra:
      base_path: {(workdir / "lake").as_posix()}
""",
)
connections = Connections.from_yaml(workdir / "connections.yaml")

# 2. Describe the datasets in a catalog -----------------------------------------
# The same data in three formats: each entry says where it lives and how it is
# read and written.
(workdir / "catalog.yaml").write_text(
    """
entries:
  orders_csv:
    file_format: csv
    connection: lake
    location: orders.csv
    load_options: {header: true}
    save_options: {header: true}
  orders_parquet:
    file_format: parquet
    connection: lake
    location: orders.parquet
  orders_delta:
    file_format: delta_table
    connection: lake
    location: orders_delta
    save_options: {mode: overwrite}
""",
)
catalog = Catalog.from_yaml(workdir / "catalog.yaml", connections=connections)

# 3. Write some data ---------------------------------------------------------------
orders = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6],
        "customer": ["ada", "linus", "grace", "ada", "alan", "grace"],
        "country": ["ES", "FI", "US", "ES", "UK", "US"],
        "amount": [120.0, 35.5, 980.0, 15.0, 240.0, 60.0],
        "coupon": [None, "WELCOME", None, "WELCOME", None, "50%OFF"],
        "placed_on": [
            date(2025, 1, 10),
            date(2025, 1, 21),
            date(2025, 2, 2),
            date(2025, 2, 14),
            date(2025, 3, 1),
            date(2025, 3, 9),
        ],
    },
)
for key in ("orders_csv", "orders_parquet", "orders_delta"):
    catalog.save_polars(key, orders)

# 4. Read everything -------------------------------------------------------------------
title("load_polars(): the whole dataset")
print(catalog.load_polars("orders_parquet").sort("id"))

# 5. where= pushes an SQL predicate down to the reader ---------------------------------
title("load_polars(where=...): SQL (DuckDB)")
print(catalog.load_polars("orders_parquet", where="country = 'US' AND amount >= 60"))

title("columns= keeps only some columns")
print(
    catalog.load_polars(
        "orders_parquet",
        where="amount > 100",
        columns=["id", "customer", "amount"],
    ).sort("id"),
)

title("the same predicate works on every format")
predicate = "placed_on >= DATE '2025-02-01' AND coupon IS NULL"
for key in ("orders_csv", "orders_parquet", "orders_delta"):
    ids = sorted(catalog.load_polars(key, where=predicate)["id"].to_list())
    print(f"{key:15}", ids)

title("text, IN, LIKE and OR are plain SQL")
for where in (
    "customer IN ('ada', 'alan')",
    "customer LIKE 'g%'",
    "coupon LIKE '%!%%' ESCAPE '!'",
    "country = 'FI' OR amount > 500",
    "amount BETWEEN 35.5 AND 120",
):
    ids = sorted(catalog.load_polars("orders_parquet", where=where)["id"].to_list())
    print(f"{where:34}", ids)

# 6. Arrow, for tools that speak Arrow -------------------------------------------------
title("load_arrow(): the same, as a pyarrow Table")
table = catalog.load_arrow("orders_delta", where="amount > 100", columns=["id"])
print(type(table).__name__, table.num_rows, "rows")

# 7. scan_polars(): Polars expressions, lazily -----------------------------------------
title("scan_polars(): filter with Polars expressions, nothing is read until collect()")
lazy = (
    catalog.scan_polars("orders_parquet")
    .filter(pl.col("country").is_in(["ES", "UK"]) & pl.col("coupon").is_null())
    .select("id", "customer", "amount")
)
print(lazy.collect())

title("scan_polars() on Parquet and Delta pushes the filter to the file scan")
print(
    catalog.scan_polars("orders_delta")
    .filter(pl.col("amount") > 100)
    .select("customer")
    .explain(),
)

title("scan_polars() then Polars: aggregate without loading everything")
print(
    catalog.scan_polars("orders_parquet")
    .group_by("country")
    .agg(pl.col("amount").sum().alias("revenue"))
    .sort("revenue", descending=True)
    .collect(),
)

# 8. What is refused -------------------------------------------------------------------
title("a where that is not a plain predicate is refused")
for where in (
    "1=1; DROP TABLE data",
    "id IN (SELECT id FROM read_csv('/etc/passwd'))",
    "amount >",
):
    try:
        catalog.load_polars("orders_parquet", where=where)
    except ValueError as error:
        print(f"ValueError: {error}")

title("an unknown key")
try:
    catalog.load_polars("nope")
except KeyError as error:
    print(f"KeyError: {error}")
