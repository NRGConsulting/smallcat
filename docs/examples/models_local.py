# ruff: noqa: CPY001, D101, D106, DTZ001, INP001, RUF012, T201
"""Models end to end on the local filesystem.

Run it with ``python docs/examples/models_local.py``. It needs nothing but
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
    ModelRegistry,
    ModelWriteMode,
    registry,
    use_registry,
)
from smallcat.models import (
    ModelSchemaValidationError,
    ModelUnsupportedWriteModeError,
    ModelWriteNotAllowedError,
)

workdir = Path(tempfile.mkdtemp(prefix="smallcat-models-"))

# 1. Connections live in a file (or in Airflow), never in the code. ----------
(workdir / "connections.yaml").write_text(
    f"""
connections:
  lake:
    conn_type: fs
    extra:
      base_path: {(workdir / "lake").as_posix()}
""",
)


# 2. Models say what the data is. They know nothing about connections. --------
class Customer(Model):
    id: int
    name: str
    email: str | None = None
    created_at: datetime

    class Meta:
        location = "silver/customers.parquet"
        file_format = ModelFileFormat.PARQUET
        allowed_write_modes = [ModelWriteMode.OVERWRITE]  # a file is replaced whole


class Event(Model):
    id: int
    kind: str

    class Meta:
        location = "silver/events"
        file_format = ModelFileFormat.DELTA_TABLE  # Delta can also append and merge
        merge_keys = ["id"]  # what identifies a row for mode="merge"


class Report(Model):
    id: int
    total: float

    class Meta:
        location = "gold/report.parquet"
        file_format = ModelFileFormat.PARQUET
        allowed_write_modes = []  # read-only


# 3. The environment binds models to connections. -----------------------------
registry.load_connections(Connections.from_yaml(workdir / "connections.yaml"))
for model in (Customer, Event, Report):
    registry.register(model, connection="lake")

# 4. Write. `mode` is always explicit. ----------------------------------------
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
Event.objects.save(
    pl.DataFrame({"id": [1], "kind": ["signup"]}),
    mode=ModelWriteMode.APPEND,
)
Event.objects.save(
    pl.DataFrame({"id": [2], "kind": ["login"]}),
    mode=ModelWriteMode.APPEND,
)
# Merge: id 2 exists, so it is updated; id 3 is new, so it is inserted.
Event.objects.save(
    pl.DataFrame({"id": [2, 3], "kind": ["logout", "purchase"]}),
    mode=ModelWriteMode.MERGE,
)

# 5. Read. -----------------------------------------------------------------------
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

print("-- as model instances")
print(Customer.objects.filter(pl.col("id") == 1).as_models())

print("-- events (appended twice, then merged)")
print(Event.objects.all().sort("id"))

# 6. What the guard rails refuse. -------------------------------------------------
print("-- refusals")
for label, action in {
    "wrong types": lambda: Customer.objects.save(
        pl.DataFrame(
            {
                "id": ["1"],
                "name": ["x"],
                "email": [None],
                "created_at": [datetime(2024, 1, 1)],
            },
        ),
        mode=ModelWriteMode.OVERWRITE,
    ),
    "append, which the model does not allow": lambda: Customer.objects.save(
        [],
        mode=ModelWriteMode.APPEND,
    ),
    "write a read-only model": lambda: Report.objects.save(
        pl.DataFrame({"id": [1], "total": [1.0]}),
        mode=ModelWriteMode.OVERWRITE,
    ),
    "filter + where": lambda: Customer.objects.filter(pl.col("id") > 1).where("id < 3"),
}.items():
    try:
        action()
    except (
        ModelSchemaValidationError,
        ModelUnsupportedWriteModeError,
        ModelWriteNotAllowedError,
        ValueError,
    ) as error:
        print(f"[{label}] {type(error).__name__}: {error}")

# 7. Tests (or another environment) use a registry of their own. ------------------
test_registry = ModelRegistry()
test_registry.load_connections(
    Connections.from_dict(
        {
            "connections": {
                "lake": {
                    "conn_type": "fs",
                    "extra": {"base_path": str(workdir / "test-lake")},
                },
            },
        },
    ),
)
test_registry.register(Customer, connection="lake")
with use_registry(test_registry):
    Customer.objects.save([], mode=ModelWriteMode.OVERWRITE)
    print(
        "-- inside use_registry the data is separate:",
        Customer.objects.all().height,
        "rows",
    )
print("-- outside it is untouched:", Customer.objects.all().height, "rows")
