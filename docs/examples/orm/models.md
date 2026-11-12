# Models on the local filesystem

A runnable walk-through of [models](../../models.md): connections from a file, a
Parquet model, a Delta model, a read-only model, writing, and what the guard
rails refuse. For reading and filtering, see [Querying](querying.md). It needs
nothing but smallcat-chonk; it writes its connections file and its data under a
temporary directory.

```sh
pip install smallcat-chonk
python models_local.py
```

## The script

```python
--8<-- "docs/examples/models_local.py"
```

## What it prints

The Parquet model was saved whole, and read back:

```text
-- all customers
shape: (3, 4)
┌─────┬───────┬───────────────────┬─────────────────────┐
│ id  ┆ name  ┆ email             ┆ created_at          │
│ --- ┆ ---   ┆ ---               ┆ ---                 │
│ i64 ┆ str   ┆ str               ┆ datetime[μs]        │
╞═════╪═══════╪═══════════════════╪═════════════════════╡
│ 1   ┆ Ada   ┆ ada@example.com   ┆ 2024-01-05 00:00:00 │
│ 2   ┆ Linus ┆ null              ┆ 2024-03-09 00:00:00 │
│ 3   ┆ Grace ┆ grace@example.com ┆ 2025-02-01 00:00:00 │
└─────┴───────┴───────────────────┴─────────────────────┘
```

The Delta model was appended to twice and then merged by `id`: row 2 already
existed, so it was updated (`login` became `logout`), and row 3 was new, so it
was inserted. Nothing was deleted:

```text
-- events (appended twice, then merged)
shape: (3, 2)
┌─────┬──────────┐
│ id  ┆ kind     │
│ --- ┆ ---      │
│ i64 ┆ str      │
╞═════╪══════════╡
│ 1   ┆ signup   │
│ 2   ┆ logout   │
│ 3   ┆ purchase │
└─────┴──────────┘
```

And what is refused, each one *before* anything is written:

```text
-- refusals
[wrong types] ModelSchemaValidationError: Customer does not match its schema:
  - id: expected Int64, got String
[append, which the model does not allow] ModelWriteNotAllowedError: Customer does not allow 'append' (allowed: ['overwrite'])
[write a read-only model] ModelWriteNotAllowedError: Report is read-only: it allows no write mode
-- inside use_registry the data is separate: 0 rows
-- outside it is untouched: 3 rows
```

## With Airflow

Only the connections change. Replace the YAML loader and nothing else:

```python
registry.load_connections(Connections.from_airflow(["lake"]))
registry.register(Customer, connection="lake")
```

The ids are resolved and validated at that line, so a connection that is
missing in Airflow fails at start-up and not on the first read.

## Next

[Querying](querying.md) reads the same kind of model with `filter`, `where`,
`lazy` and `as_models`. [Writing with a model](../../writing-models.md) covers
every write mode.
