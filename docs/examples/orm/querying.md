# Querying a model on the local filesystem

A runnable walk-through of reading a [model](../../models.md): `all()`, `filter()`
with Polars expressions, `where()` with SQL and with lookups, `select()`, `lazy()`
and `as_models()`. The connection, the model and the registration are the ones in
[Models](models.md) and [Connections with the ORM](connections.md); here they are
the first lines of the script and the rest is the query. It needs nothing but
smallcat-chonk; it writes its connections file and its data under a temporary
directory.

```sh
pip install smallcat-chonk
python models_querying.py
```

## The script

```python
--8<-- "docs/examples/models_querying.py"
```

## What it prints

Every row, and the same data filtered with Polars expressions, keeping two
fields:

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
-- filter with Polars expressions, project two fields
shape: (2, 2)
┌─────┬───────┐
│ id  ┆ name  │
│ --- ┆ ---   │
│ i64 ┆ str   │
╞═════╪═══════╡
│ 2   ┆ Linus │
│ 3   ┆ Grace │
└─────┴───────┘
```

The same query lazily. The filter shows up as `SELECTION` and the columns as
`PROJECT 2/4`, inside the Parquet scan: they are pushed down to the file, so only
what is needed is read:

```text
-- the same, lazily: the filter is pushed down into the Parquet scan
simple π 1/1 ["name"]
  Parquet SCAN [file:///.../lake/silver/customers.parquet]
  PROJECT 2/4 COLUMNS
  SELECTION: col("id") > 1
  ESTIMATED ROWS: 3
```

The same kind of filter written as SQL, as lookups, and read back as model
instances:

```text
-- or with SQL
shape: (2, 1)
┌───────┐
│ name  │
│ ---   │
│ str   │
╞═══════╡
│ Ada   │
│ Grace │
└───────┘
-- or with lookups
shape: (2, 1)
┌───────┐
│ name  │
│ ---   │
│ str   │
╞═══════╡
│ Grace │
│ Linus │
└───────┘
-- as model instances
[Customer(id=1, name='Ada', email='ada@example.com', created_at=datetime.datetime(2024, 1, 5, 0, 0))]
```

A query is a value: filtering it again returns a new query and leaves the first
one as it was. Nothing is read until `all()`, `lazy()` or `as_models()`. And
`filter()` with `where()` cannot be combined: it is refused where it is written,
not when it runs:

```text
-- a query is not run until you ask
rows : 2
again: 1 (recent is unchanged)
-- refusals
[filter + where] ModelQueryError: filter() takes Polars expressions and where() takes an SQL predicate: a query can use only one of them
```

## Next

[Querying models](../../querying-models.md) covers every operator, the restrictions
and what each engine does. To write the data, see [Models](models.md) and
[Writing with a model](../../writing-models.md).
