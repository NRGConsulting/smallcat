# Writing with a model

Save a DataFrame or a list of model instances through `Model.objects.save`. The
mode is chosen on every call (there is no default) and the data is validated
against the model before anything is written. It is the counterpart of
[Querying models](querying-models.md). To write through a catalog entry
instead, see [Writing with a catalog](writing-catalog.md).

## The modes

| Mode | Effect on existing data | Formats |
|------|-------------------------|---------|
| `overwrite` | Replaces the whole dataset with what you write. | every format |
| `append` | Adds the rows. | `delta_table` |
| `merge` | Inserts the rows whose key is new and updates the ones whose key exists. Never deletes. | `delta_table` |
| *update* | Sets columns on the rows a predicate selects. Not a save mode, and not on `objects` yet ([#36](https://github.com/NRGConsulting/smallcat/issues/36)). | `delta_table` |

CSV, Excel and single-file Parquet can only be **replaced**. To add or merge
rows, use a Delta table. A model has no `ignore`: `merge` already gives a load
that can be repeated without duplicates.

## Saving

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

## Updating rows

There is no `update` on `objects` yet ([#36](https://github.com/NRGConsulting/smallcat/issues/36)). Use the Delta dataset of the model's entry;
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

## Files can only be replaced

```text
overwrite       [(2, 'linus', 'Oslo')]
```

## What is refused, before anything is written

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

## A registration can take modes away

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
| Keep a table in step with a source, by key | `merge` (Delta) |
| Change a column on some rows, whatever the data | `update` on the model's Delta dataset ([#36](https://github.com/NRGConsulting/smallcat/issues/36) tracks exposing it) |
| Delete rows | Not available yet: [#32](https://github.com/NRGConsulting/smallcat/issues/32) |

## The script

```python
--8<-- "docs/examples/writing_models.py"
```
