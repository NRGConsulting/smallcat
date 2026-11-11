# Models on the local filesystem

A runnable walk-through of [models](../models.md): connections from a file, a
Parquet model, a Delta model, a read-only model, every way to read, and what the
guard rails refuse. It needs nothing but smallcat-chonk; it writes its
connections file and its data under a temporary directory.

```sh
pip install smallcat-chonk
python models_local.py
```

## The script

```python
--8<-- "docs/examples/models_local.py"
```

## What it prints

Reading everything, filtering with Polars, and the same query lazily (the
filter shows up as `SELECTION` inside the Parquet scan, so it is pushed down to
the file):

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
-- the same, lazily: the filter is pushed down into the Parquet scan
simple π 1/1 ["name"]
  Parquet SCAN [file:///.../lake/silver/customers.parquet]
  PROJECT 2/4 COLUMNS
  SELECTION: col("id") > 1
  ESTIMATED ROWS: 3
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

And what is refused, each one *before* anything is written or read:

```text
-- refusals
[wrong types] ModelSchemaValidationError: Customer does not match its schema:
  - id: expected Int64, got String
[append, which the model does not allow] ModelWriteNotAllowedError: Customer does not allow 'append' (allowed: ['overwrite'])
[write a read-only model] ModelWriteNotAllowedError: Report is read-only: it allows no write mode
[filter + where] ModelQueryError: filter() takes Polars expressions and where() takes an SQL predicate: a query can use only one of them
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
