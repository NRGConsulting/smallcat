# ruff: noqa: CPY001, D101, D106, DTZ001, INP001, RUF012, T201
"""Querying a model on the local filesystem.

Run it with ``python docs/examples/models_querying.py``. It needs nothing but
smallcat-chonk: it writes its connections file and its data under a temporary
directory.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl

from smallcat import (
    Connections,
    Model,
    ModelFileFormat,
    ModelWriteMode,
    registry,
)
from smallcat.models import ModelQueryError

workdir = Path(tempfile.mkdtemp(prefix="smallcat-querying-"))

# 1. The connection and the model (see "Models" for the details). ------------------
(workdir / "connections.yaml").write_text(
    f"""
connections:
  lake:
    conn_type: fs
    extra:
      base_path: {(workdir / "lake").as_posix()}
""",
)


class Customer(Model):
    id: int
    name: str
    email: str | None = None
    created_at: datetime

    class Meta:
        location = "silver/customers.parquet"
        file_format = ModelFileFormat.PARQUET


registry.load_connections(Connections.from_yaml(workdir / "connections.yaml"))
registry.register(Customer, connection="lake")

Customer.objects.save(
    [
        Customer(
            id=1,
            name="Ada",
            email="ada@example.com",
            created_at=datetime(2024, 1, 5),
        ),
        Customer(id=2, name="Linus", created_at=datetime(2024, 3, 9)),
        Customer(
            id=3,
            name="Grace",
            email="grace@example.com",
            created_at=datetime(2025, 2, 1),
        ),
    ],
    mode=ModelWriteMode.OVERWRITE,
)

# 2. Query it. -----------------------------------------------------------------------
print("-- all customers")
print(Customer.objects.all().sort("id"))

print("-- filter with Polars expressions, project two fields")
print(
    Customer.objects.filter(pl.col("created_at") >= datetime(2024, 3, 1))
    .select("id", "name")
    .all()
    .sort("id"),
)

print("-- the same, lazily: the filter is pushed down into the Parquet scan")
print(Customer.objects.filter(pl.col("id") > 1).select("name").lazy().explain())

print("-- or with SQL")
print(Customer.objects.where("email IS NOT NULL").select("name").all().sort("name"))

print("-- or with lookups")
print(Customer.objects.where(id__gte=2).select("name").all().sort("name"))

print("-- as model instances")
print(Customer.objects.filter(pl.col("id") == 1).as_models())

# 3. A query is a value, and contradictory ones are refused where they are written. -
print("-- a query is not run until you ask")
recent = Customer.objects.filter(pl.col("id") > 1)
print("rows :", recent.all().height)
print("again:", recent.filter(pl.col("id") > 2).all().height, "(recent is unchanged)")

print("-- refusals")
try:
    Customer.objects.filter(pl.col("id") > 1).where("id < 3")
except ModelQueryError as error:
    print(f"[filter + where] {type(error).__name__}: {error}")
