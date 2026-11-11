# Writing data

How to save a DataFrame, update rows, and what each write mode does to the data
that is already there. It is the counterpart of [Querying models](querying-models.md)
and [Querying a catalog](querying-catalog.md), and it covers both ways of writing:
a **catalog** and a **model**.

The whole page is one runnable script, `docs/examples/writing_data.py`; every
output below is what it prints.

## The modes

| Mode | Effect on existing data | Formats |
|------|-------------------------|---------|
| `overwrite` | Replaces the whole dataset with what you write. | every format |
| `append` | Adds the rows. | `delta_table` |
| `ignore` | Writes only if the table does not exist; otherwise does nothing. | `delta_table` (catalog only) |
| `merge` | Inserts the rows whose key is new and updates the ones whose key exists. Never deletes. | `delta_table` |
| *update* | Sets columns on the rows a predicate selects. Not a save mode: a separate method. | `delta_table` |

CSV, Excel and single-file Parquet can only be **replaced**. To add or merge
rows, use a Delta table.

|  | Where the mode is chosen | Data validated first |
|--|--------------------------|----------------------|
| Catalog | In the entry (`save_options.mode`): one entry, one way of writing | No |
| Model | On every call: `Customer.objects.save(df, mode=...)`, no default | Yes, against the model |

## Catalog

### One entry per way of writing

Several entries may point at the same location with different modes:

```yaml
entries:
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
      update_columns: [city]     # on a match, only the city changes
```

```python
catalog = Catalog.from_yaml("catalog.yaml", connections=connections)
catalog.save_polars("people_append", df)
```

### What each mode does

A file is always replaced; the second write leaves one row:

```text
people_csv      [(2, 'linus', 'Helsinki')]
```

On a Delta table, starting from nothing and applying the modes in turn:

```text
overwrite       [(1, 'ada', 'Madrid'), (2, 'linus', 'Oslo')]
append          [(1, 'ada', 'Madrid'), (2, 'linus', 'Oslo'), (3, 'grace', 'Boston')]
ignore          [(1, 'ada', 'Madrid'), (2, 'linus', 'Oslo'), (3, 'grace', 'Boston')]
merge           [(1, 'ada', 'Madrid'), (2, 'linus', 'Helsinki'), (3, 'grace', 'Boston'), (4, 'alan', 'London')]
merge (city)    [(1, 'ada', 'Seville'), (2, 'linus', 'Helsinki'), (3, 'grace', 'Boston'), (4, 'alan', 'London')]
```

* `append` added 3. `ignore` found the table and **did nothing**.
* `merge` updated id 2 (Oslo to Helsinki) and inserted id 4.
* `merge (city)` matched id 1 and changed only the city: the name sent
  (`IGNORED`) was left out because of `update_columns`.

### Updating rows by a predicate

`update` sets columns to constant values on the rows a predicate selects. It lives
on the Delta dataset, which the catalog gives you with `get_dataset`:

```python
table = catalog.get_dataset("people_replace")
metrics = table.update("people", "city = 'London'", {"city": "Bristol"})
metrics["num_updated_rows"]    # 1
```

```text
rows updated    1
after update    [(1, 'ada', 'Seville'), (2, 'linus', 'Helsinki'), (3, 'grace', 'Boston'), (4, 'alan', 'Bristol')]
```

The predicate is SQL, checked like a `where` (no subqueries) and **required**:
there is no way to update every row by leaving it out. Values are Python values
(`str`, `bool`, `int`, `float`, `date`, `datetime` or `None`), written as escaped
literals, never pasted into the SQL. Naming a column the table does not have is an
error.

### What is refused

```text
duplicate keys  ValueError - the data has several rows with the same merge_keys ['id']: d
merge, no keys  ValidationError
```

* Repeated (or null) keys in the data fail **before writing**: smallcat does not
  guess which row should win.
* A `merge` entry without `merge_keys`, or `merge_keys` / `update_columns` without
  `mode: merge`, fails when the catalog is **loaded**, not on the first save.

## Models

Declare the keys once in `Meta`, or pass them in the call (the call wins):

```python
class Customer(Model):
    id: int
    name: str
    city: str

    class Meta:
        location = "customers"
        file_format = ModelFileFormat.DELTA_TABLE
        merge_keys = ["id"]

registry.register(Customer, connection="lake")

Customer.objects.save(df, mode="overwrite")
Customer.objects.save(df, mode=ModelWriteMode.APPEND)
Customer.objects.save([Customer(id=2, ...), Customer(id=4, ...)], mode="merge")
Customer.objects.save(df, mode="merge", merge_keys=["name"])
```

```text
overwrite       [(1, 'ada', 'Madrid'), (2, 'linus', 'Oslo')]
append          [(1, 'ada', 'Madrid'), (2, 'linus', 'Oslo'), (3, 'grace', 'Boston')]
merge           [(1, 'ada', 'Madrid'), (2, 'linus', 'Helsinki'), (3, 'grace', 'Boston'), (4, 'alan', 'London')]
merge by name   [(1, 'ada', 'Madrid'), (2, 'linus', 'Helsinki'), (4, 'alan', 'London'), (9, 'grace', 'Austin')]
```

The last line matched on `name`, so `grace` was **updated** (her id changed from 3
to 9) instead of inserted again.

### Updating rows

There is no `update` on `objects` yet. Use the Delta dataset of the model's entry;
its path is the model's `Meta.location`:

```python
table = registry.entry_for(Customer).build_dataset()
table.update("customers", "city = 'London'", {"city": "Bristol"})
```

```text
rows updated    1
after update    [(1, 'ada', 'Madrid'), (2, 'linus', 'Helsinki'), (4, 'alan', 'Bristol'), (9, 'grace', 'Austin')]
```

This writes straight to the table, **without validating against the model**.

### Files can only be replaced

```text
overwrite       [(2, 'linus', 'Oslo')]
```

### What is refused, before anything is written

```text
no mode         TypeError
append a file   ModelUnsupportedWriteModeError
merge a file    ModelUnsupportedWriteModeError
keys, no merge  ModelQueryError
key not a field ModelQueryError
wrong columns   ModelSchemaValidationError
```

| Case | Why |
|------|-----|
| no mode | `mode` is required: it decides what happens to the existing data. |
| append / merge a file | Only Delta tables can. |
| keys, no merge | `merge_keys` only applies to `mode="merge"`; it is not ignored. |
| key not a field | The keys must be fields of the model. |
| wrong columns | The data does not match the model ([Validation](models.md#validation)). |

### A registration can take modes away

```python
registry.register(Customer, connection="lake", allowed_write_modes=["merge"], replace=True)
Customer.objects.save(df, mode="overwrite")
```

```text
overwrite       ModelWriteNotAllowedError - Customer does not allow 'overwrite' (allowed: ['merge'])
still the same  [(1, 'ada', 'Madrid'), (2, 'linus', 'Helsinki'), (4, 'alan', 'Bristol'), (9, 'grace', 'Austin')]
```

A registration can only **restrict** what the model's `Meta` allows, never widen
it. Use it to give a production job `merge` only, so it cannot replace the table.

## Which one to use

| You want to | Use |
|-------------|-----|
| Replace a dataset with fresh data | `overwrite` |
| Add new rows, never touching old ones | `append` (Delta) |
| Load once, never overwrite a table that exists | `ignore` (catalog, Delta) |
| Keep a table in step with a source, by key | `merge` (Delta) |
| Change a column on some rows, whatever the data | `update` (Delta, dataset) |
| Delete rows | Not available yet: [#32](https://github.com/NRGConsulting/smallcat/issues/32) |

## The script

```python
--8<-- "docs/examples/writing_data.py"
```
