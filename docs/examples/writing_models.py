# ruff: noqa: D101, D103, D106, INP001, RUF012, T201
"""Writing data with a model, from the connection to every write mode.

Run it with ``python docs/examples/writing_models.py``. It writes its connections
file and its data under a temporary directory.
"""

import tempfile
from pathlib import Path

import polars as pl

from smallcat import Connections, Model, ModelFileFormat, ModelWriteMode, registry
from smallcat.models import ModelWriteNotAllowedError

workdir = Path(tempfile.mkdtemp(prefix="smallcat-writing-"))


def title(text: str) -> None:
    print(f"\n=== {text}")


def rows(df: pl.DataFrame) -> list[tuple]:
    """The rows sorted by id, to keep the output short."""
    return df.sort("id").rows()


def people(*items: tuple[int, str, str]) -> pl.DataFrame:
    return pl.DataFrame(items, schema=["id", "name", "city"], orient="row")


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
registry.load_connections(connections)

# 2. The models ----------------------------------------------------------------


class Customer(Model):
    id: int
    name: str
    city: str

    class Meta:
        location = "customers"
        file_format = ModelFileFormat.DELTA_TABLE
        merge_keys = ["id"]


class Archive(Model):
    id: int
    name: str
    city: str

    class Meta:
        location = "archive.parquet"
        file_format = ModelFileFormat.PARQUET


registry.register(Customer, connection="lake")
registry.register(Archive, connection="lake")

# With a model the mode is chosen on each call, and the data is validated first.
title("models: one mode per call")
Customer.objects.save(
    people((1, "ada", "Madrid"), (2, "linus", "Oslo")),
    mode="overwrite",
)
print("overwrite      ", rows(Customer.objects.all()))
Customer.objects.save(people((3, "grace", "Boston")), mode=ModelWriteMode.APPEND)
print("append         ", rows(Customer.objects.all()))
Customer.objects.save(
    [
        Customer(id=2, name="linus", city="Helsinki"),
        Customer(id=4, name="alan", city="London"),
    ],
    mode="merge",
)
print("merge          ", rows(Customer.objects.all()))
Customer.objects.save(people((9, "grace", "Austin")), mode="merge", merge_keys=["name"])
print("merge by name  ", rows(Customer.objects.all()))

title("models: update rows by a predicate")
customers_table = registry.entry_for(Customer).build_dataset()
metrics = customers_table.update("customers", "city = 'London'", {"city": "Bristol"})  # type: ignore[attr-defined]
print("rows updated   ", metrics["num_updated_rows"])
print("after update   ", rows(Customer.objects.all()))

title("models: a file is only ever replaced")
Archive.objects.save(people((1, "ada", "Madrid")), mode="overwrite")
Archive.objects.save(people((2, "linus", "Oslo")), mode="overwrite")
print("overwrite      ", rows(Archive.objects.all()))

title("models: what is refused, before anything is written")
attempts = {
    "no mode": lambda: Customer.objects.save(people((5, "x", "y"))),  # type: ignore[call-arg]
    "append a file": lambda: Archive.objects.save(people((5, "x", "y")), mode="append"),
    "merge a file": lambda: Archive.objects.save(
        people((5, "x", "y")),
        mode="merge",
        merge_keys=["id"],
    ),
    "keys, no merge": lambda: Customer.objects.save(
        people((5, "x", "y")),
        mode="append",
        merge_keys=["id"],
    ),
    "key not a field": lambda: Customer.objects.save(
        people((5, "x", "y")),
        mode="merge",
        merge_keys=["nope"],
    ),
    "wrong columns": lambda: Customer.objects.save(
        pl.DataFrame({"id": [5]}),
        mode="append",
    ),
}
for label, attempt in attempts.items():
    try:
        attempt()
    except Exception as e:  # noqa: BLE001
        print(f"{label:15}", type(e).__name__)

title("models: a registration can take modes away")
registry.register(
    Customer,
    connection="lake",
    allowed_write_modes=["merge"],
    replace=True,
)
try:
    Customer.objects.save(people((5, "x", "y")), mode="overwrite")
except ModelWriteNotAllowedError as e:
    print("overwrite      ", type(e).__name__, "-", e)
print("still the same ", rows(Customer.objects.all()))
