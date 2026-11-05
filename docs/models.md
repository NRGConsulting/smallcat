# Models

A model is a Pydantic class that describes **what a dataset is**: its columns,
where it lives and how it is stored. `Model.objects` then reads and writes the
dataset as Polars DataFrames, validated against the model.

```python
from datetime import datetime
from smallcat import Model, ModelWriteMode

class Customer(Model):
    id: int
    name: str
    email: str | None = None
    created_at: datetime

    class Meta:
        location = "silver/customers"
        file_format = "parquet"
```

A model does not say which connection it uses. That is decided when it is
registered, outside the model, so the same model runs against different storage
in each environment.

```python
import polars as pl
from smallcat import Connections, registry

registry.load_connections(Connections.from_yaml("connections.yaml"))
registry.register(Customer, connection="lake")

Customer.objects.filter(pl.col("id") > 10).all()
Customer.objects.save(df, mode=ModelWriteMode.OVERWRITE)
```

A complete, runnable example is in [Models on the local filesystem](examples/models.md).

## `Meta`

`Meta` is validated when the class is defined, so a mistake fails on import and
not on the first read. Unknown attributes are rejected and every problem is
reported together.

| Attribute | Required | Description |
|-----------|----------|-------------|
| `location` | yes | Path of the data, relative to the connection's base. |
| `file_format` | yes | `csv`, `excel`, `parquet` or `delta_table`. |
| `allowed_write_modes` | no | The [write modes](#writing) the model permits. By default all of them. `[]` makes the model read-only. |
| `validation_mode` | no | `ModelValidationMode.STRICT` (default) or `COERCE`. See [Validation](#validation). |
| `load_options` | no | Format options applied when reading. |
| `save_options` | no | Format options applied when writing. |

`load_options` and `save_options` are the options each format already accepts in
a [catalog](catalog-reference.md#options-per-format) (`sep`, `header`, `sheet`,
`hive_partitioning`, `partition_by`...) and are validated against the same
schemas. They configure the file format, not permissions. For Delta tables the
write `mode` cannot be set there: it is chosen on every `save`.

A `Meta` is inherited by subclasses. A class without a `Meta` is abstract and
cannot be registered.

## Field types

The fields give the expected Polars types.

| Python type | Polars type |
|-------------|-------------|
| `int`, `float`, `str`, `bool`, `bytes` | `Int64`, `Float64`, `String`, `Boolean`, `Binary` |
| `datetime`, `date`, `time`, `timedelta` | `Datetime("us")`, `Date`, `Time`, `Duration("us")` |
| `Literal["a", "b"]`, `Literal[1, 2]` | `String`, `Int64` |
| `list[T]` | `List(T)` |
| `T \| None` | `T`, and the field accepts nulls |

Any other type (`dict`, nested models, unions of several types, `Decimal`...)
is rejected with `ModelDefinitionError` when the class is defined, naming the
field. Columns are matched by field name; aliases are not used.

## Registering

```python
registry.load_connections(Connections.from_yaml("connections.yaml"))
registry.register(Customer, connection="lake")
```

* The connection must already be loaded (see [Connections](connections.md)),
  otherwise `ConnectionNotLoadedError` lists the names that are.
* Registering twice raises `ModelRegistrationError`, unless `replace=True`.
* Using a model that is not registered raises `ModelNotRegisteredError`, which
  shows the call to make.
* `smallcat.registry` is the default registry, shared by the whole process.

### Restricting what a model may do, per environment

`allowed_write_modes` can be narrowed when registering, **never widened**. A
model that is read-write in development can be made read-only in production
without changing its code:

```python
registry.register(Customer, connection="lake", allowed_write_modes=[])
```

Asking for a mode the model's `Meta` does not allow raises
`ModelRegistrationError`.

### Tests and other registries

`Model.objects` uses the *current* registry. `use_registry` replaces it inside a
block and restores the previous one on exit, even on an exception. It is kept
per context, so threads and async tasks do not affect each other.

```python
from smallcat import ModelRegistry, use_registry

test_registry = ModelRegistry()
test_registry.load_connections(Connections.from_dict(
    {"connections": {"lake": {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}}}}
))
test_registry.register(Customer, connection="lake")

with use_registry(test_registry):
    Customer.objects.save(df, mode=ModelWriteMode.OVERWRITE)
```

## Reading

`Customer.objects` offers `all()`, `lazy()`, `as_models()`, `filter()`,
`where()` and `select()`. The last three return a **query**: it is immutable
(each call returns a new one) and reads nothing until you run it.

```python
Customer.objects.all()                                      # pl.DataFrame
Customer.objects.filter(pl.col("id") > 10).all()
Customer.objects.select("id", "name").all()
Customer.objects.filter(pl.col("id") > 10).select("name").lazy()   # pl.LazyFrame
Customer.objects.where("id > 10 AND email IS NOT NULL").all()
Customer.objects.filter(pl.col("id") == 1).as_models()      # list[Customer]
```

| Method | Returns | Notes |
|--------|---------|-------|
| `all()` | `pl.DataFrame` | Columns in the model's order. |
| `lazy()` | `pl.LazyFrame` | Chain more Polars after it. See below. |
| `as_models()` | `list[Customer]` | Each row is validated by Pydantic, so field constraints apply. Cannot be combined with `select()`. |
| `filter(*exprs)` | query | Polars expressions; several calls are combined with AND. |
| `where(sql)` | query | An SQL predicate (DuckDB). Usable once: join conditions with `AND` in one string. |
| `select(*fields)` | query | Only some fields; names must be fields of the model. |

### `filter` or `where`, never both

`filter()` takes Polars expressions and `where()` takes SQL. They run on
different engines, so a query can use **only one of them**; combining them, in
either order, raises `ModelQueryError` where you write it, not when it runs.
`select()` combines with either.

### Lazy reads

For Parquet and Delta, `lazy()` scans natively and what you chain after it is
pushed down to the storage: only the needed columns and row groups are read.

```text
>>> print(Customer.objects.filter(pl.col("id") > 1).select("name").lazy().explain())
simple π 1/1 ["name"]
  Parquet SCAN [file:///.../silver/customers.parquet]
  PROJECT 2/4 COLUMNS
  SELECTION: col("id") > 1
```

CSV and Excel are read in full and then wrapped. A query with `where()` runs its
SQL immediately.

`lazy()` checks the columns and types, but two checks need data and are **not**
made on a frame you collect yourself: nulls in required fields (use `all()`),
and, in `COERCE` mode, values that cannot be cast, which fail with a Polars
error when collected.

## Writing

```python
Customer.objects.save(df, mode=ModelWriteMode.OVERWRITE)
Customer.objects.save([Customer(...), Customer(...)], mode="overwrite")
Customer.objects.save(Customer(...), mode=ModelWriteMode.APPEND)
```

`save` takes a DataFrame, an instance of the model or a list of instances (a
`LazyFrame` must be collected first). **The mode is required and has no
default**, because it decides what happens to the data that is already there:

| `ModelWriteMode` | Effect | Supported by |
|------------------|--------|--------------|
| `OVERWRITE` | **Replaces the whole dataset** with the data you pass. | `csv`, `excel` and `parquet` (a single file), and `delta_table` |
| `APPEND` | **Adds** the rows to what exists. | `delta_table` only |

There is no `upsert`: nothing in smallcat updates rows by key today. In
particular, saving one object with `OVERWRITE` leaves a dataset of one row.

A mode that is not a `ModelWriteMode` raises `ValueError` listing the valid
ones. These cases are refused *before anything is written*:

| Error | Cause |
|-------|-------|
| `ModelWriteNotAllowedError` | The mode is not in `allowed_write_modes`, or the model is read-only. |
| `ModelUnsupportedWriteModeError` | The format cannot do it: `APPEND` on a file, or `OVERWRITE` on partitioned Parquet. |
| `ModelSchemaValidationError` | The data does not match the model. See [Validation](#validation). |

!!! warning "Partitioned Parquet cannot be overwritten"
    With `partition_by`, DuckDB only adds files and keeps the old ones, so an
    "overwrite" would silently leave old partitions behind (this was checked: 3
    rows plus 1 gave 4). `OVERWRITE` is therefore refused for a Parquet model
    whose `save_options` has `partition_by`. Use a Delta table, or write without
    `partition_by`.

## Validation

A DataFrame is checked against the model on every read (`all`, `as_models`,
`lazy`, `where`) and every write.

| Check | `STRICT` | `COERCE` |
|-------|----------|----------|
| A column is missing | error | error |
| A column the model does not declare | error (see below) | error (see below) |
| A column's type | must be the declared *kind*: any integer width for `int`, any time unit or time zone for `datetime`... Nothing is converted | cast to the declared type |
| A value that cannot be cast | n/a | error |
| A float that would lose decimals as `int` (`42.7`) | n/a | error |
| A null in a field that is not `X \| None` | error | error |

`validation_mode` is set in `Meta` and applies to reads and writes alike.
`COERCE` suits sources whose columns arrive as text, such as CSV; `STRICT` is
the safer default for what you persist.

Unexpected columns follow Pydantic's own setting: an error by default, dropped
with `model_config = ConfigDict(extra="ignore")`, kept with `extra="allow"`.

All problems are reported together and **never include the values**, so a
failed validation cannot leak data into a log:

```text
# STRICT
ModelSchemaValidationError: Customer does not match its schema:
  - id: expected Int64, got String
  - email: column is missing
  - extra: unexpected column

# COERCE
ModelSchemaValidationError: Customer does not match its schema:
  - id: 1 value(s) cannot be cast to Int64
  - created_at: 2 value(s) cannot be cast to Datetime(time_unit='us', time_zone=None)
  - name: 1 null value(s) in a required field
```

The error has an `issues` list (column and problem) for programmatic use.

Pydantic validates *values* (constraints, validators) when instances are built
(`save([Customer(...)])`, `as_models()`). A DataFrame is checked for columns,
types and nulls only, not row by row.

## Errors

All inherit from `ModelError`.

| Error | Raised when |
|-------|-------------|
| `ModelDefinitionError` | `Meta` is invalid or a field type is not supported (on defining the class). |
| `ModelRegistrationError` | A model without `Meta`, registered twice, or asking for more write modes than `Meta` allows. |
| `ModelNotRegisteredError` | A model is used before being registered. |
| `ModelQueryError` | `filter` with `where`, `where` twice, an invalid `select`, `as_models` with `select`... |
| `ModelWriteNotAllowedError` | The write mode is not permitted. |
| `ModelUnsupportedWriteModeError` | The format cannot write with that mode. |
| `ModelSchemaValidationError` | The data does not match the model. |

## Models and the catalog

Models are built on the catalog: each registered model becomes a catalog entry
internally, so it reads and writes through the same datasets, options and
credentials. The [`Catalog`](catalog-reference.md) is still the right tool for
datasets defined in YAML or an Airflow Variable without code, and it does not
validate columns. A model adds typed columns and validation, and lives in code.

## API

See the [Models reference](api/models.md).
