# ruff: noqa: D101, D103, D106, INP001, RUF012, T201
"""Writing data, from the connection to every write mode.

Run it with ``python docs/examples/writing_data.py``. It writes its connections
file, its catalog and its data under a temporary directory.
"""

import tempfile
from pathlib import Path

import polars as pl

from smallcat import Connections, Model, ModelFileFormat, ModelWriteMode, registry
from smallcat.catalog import Catalog
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

# ============================== CATALOG =====================================
# In a catalog the write mode is part of the entry: one entry, one way of
# writing. Several entries can point at the same location with different modes.
(workdir / "catalog.yaml").write_text(
    """
entries:
  people_csv:
    file_format: csv
    connection: lake
    location: people.csv
    load_options: {header: true}
    save_options: {header: true}
  people_replace:
    file_format: delta_table
    connection: lake
    location: people
    save_options: {mode: overwrite}
  people_append:
    file_format: delta_table
    connection: lake
    location: people
    save_options: {mode: append}
  people_once:
    file_format: delta_table
    connection: lake
    location: people
    save_options: {mode: ignore}
  countries_seed:
    file_format: delta_table
    connection: lake
    location: countries
    save_options: {mode: ignore}
  people_upsert:
    file_format: delta_table
    connection: lake
    location: people
    save_options: {mode: merge, merge_keys: [id]}
  people_upsert_city:
    file_format: delta_table
    connection: lake
    location: people
    save_options:
      mode: merge
      merge_keys: [id]
      update_columns: [city]
""",
)
catalog = Catalog.from_yaml(workdir / "catalog.yaml", connections=connections)

title("catalog: a file is always replaced")
catalog.save_polars("people_csv", people((1, "ada", "Madrid")))
catalog.save_polars("people_csv", people((2, "linus", "Helsinki")))
print("people_csv     ", rows(catalog.load_polars("people_csv")))

title("catalog: Delta modes, one after the other")
catalog.save_polars(
    "people_replace",
    people((1, "ada", "Madrid"), (2, "linus", "Oslo")),
)
print("overwrite      ", rows(catalog.load_polars("people_replace")))
catalog.save_polars("people_append", people((3, "grace", "Boston")))
print("append         ", rows(catalog.load_polars("people_append")))
catalog.save_polars("people_once", people((9, "nobody", "Nowhere")))
print("ignore         ", rows(catalog.load_polars("people_once")))
catalog.save_polars(
    "people_upsert",
    people((2, "linus", "Helsinki"), (4, "alan", "London")),
)
print("merge          ", rows(catalog.load_polars("people_upsert")))
catalog.save_polars("people_upsert_city", people((1, "IGNORED", "Seville")))
print("merge (city)   ", rows(catalog.load_polars("people_upsert_city")))

title("catalog: ignore, a load that is safe to run twice")
seed = pl.DataFrame({"code": ["ES", "FI"], "name": ["Spain", "Finland"]})
catalog.save_polars("countries_seed", seed)  # the table does not exist: created
print("first run      ", catalog.load_polars("countries_seed").height, "rows")
catalog.save_polars("countries_seed", seed.head(1))  # it exists: nothing happens
print("second run     ", catalog.load_polars("countries_seed").height, "rows")

title("catalog: update rows by a predicate")
people_table = catalog.get_dataset("people_replace")
metrics = people_table.update("people", "city = 'London'", {"city": "Bristol"})  # type: ignore[attr-defined]
print("rows updated   ", metrics["num_updated_rows"])
print("after update   ", rows(catalog.load_polars("people_replace")))

title("catalog: what is refused")
try:  # duplicated keys in the data: it would be ambiguous which row wins
    catalog.save_polars("people_upsert", people((7, "a", "x"), (7, "b", "y")))
except ValueError as e:
    print("duplicate keys ", type(e).__name__, "-", str(e).splitlines()[0][:60])
try:  # a merge entry without keys is refused when the catalog is loaded
    Catalog.from_dict(
        {
            "entries": {
                "bad": {
                    "file_format": "delta_table",
                    "connection": "lake",
                    "location": "people",
                    "save_options": {"mode": "merge"},
                },
            },
        },
        connections=connections,
    )
except ValueError as e:
    print("merge, no keys ", type(e).__name__)

# ============================== MODELS ======================================
registry.load_connections(connections)


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
