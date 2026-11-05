# Models: from the connection to the filtered data

A runnable walk-through of the whole path with [models](../models.md): load a
connection, register a model against it, and query it with `all()`, `filter()`,
`where()` and `lazy()`. It needs nothing but smallcat-chonk; it writes its
connections file and its data under a temporary directory.

```sh
pip install smallcat-chonk
python models_query.py
```

The steps, in order:

1. **Load the connection** from a YAML file. It is validated here; with Airflow
   you would use `Connections.from_airflow(["lake"])` instead.
2. **Define the model** (`Order`) and **register** it with
   `registry.register(Order, connection="lake")`. The model never names the
   connection.
3. **Query it.** `filter(...)` takes Polars expressions, `where(...)` takes an SQL
   predicate, `select(...)` keeps some fields, `all()` returns a DataFrame and
   `lazy()` a LazyFrame.

## The script

```python
--8<-- "docs/examples/models_query.py"
```

## What each call does

| Call | Engine | Returns |
|------|--------|---------|
| `Order.objects.all()` | Polars / DuckDB | `pl.DataFrame` with every row |
| `Order.objects.filter(pl.col("amount") > 100).all()` | Polars | `pl.DataFrame` |
| `Order.objects.where("country = 'US' AND amount >= 60").all()` | DuckDB (SQL) | `pl.DataFrame` |
| `Order.objects.filter(...).select("customer").lazy()` | Polars scan | `pl.LazyFrame`, nothing read yet |

* Several `filter()` calls are combined with AND.
* `filter()` and `where()` are alternatives: using both in one query raises
  `ModelQueryError`, in either order. `select()` goes with either.
* A query is a value. `big_us.filter(...)` returns a new query and leaves
  `big_us` as it was; nothing is read until `all()`, `lazy()` or `as_models()`.

## What it prints

Every row, and the same data filtered with Polars expressions:

```text
=== all()
shape: (6, 6)
┌─────┬──────────┬─────────┬────────┬─────────┬─────────────────────┐
│ id  ┆ customer ┆ country ┆ amount ┆ coupon  ┆ placed_at           │
│ --- ┆ ---      ┆ ---     ┆ ---    ┆ ---     ┆ ---                 │
│ i64 ┆ str      ┆ str     ┆ f64    ┆ str     ┆ datetime[μs]        │
╞═════╪══════════╪═════════╪════════╪═════════╪═════════════════════╡
│ 1   ┆ ada      ┆ ES      ┆ 120.0  ┆ null    ┆ 2025-01-10 00:00:00 │
│ 2   ┆ linus    ┆ FI      ┆ 35.5   ┆ WELCOME ┆ 2025-01-21 00:00:00 │
│ 3   ┆ grace    ┆ US      ┆ 980.0  ┆ null    ┆ 2025-02-02 00:00:00 │
│ 4   ┆ ada      ┆ ES      ┆ 15.0   ┆ WELCOME ┆ 2025-02-14 00:00:00 │
│ 5   ┆ alan     ┆ UK      ┆ 240.0  ┆ null    ┆ 2025-03-01 00:00:00 │
│ 6   ┆ grace    ┆ US      ┆ 60.0   ┆ null    ┆ 2025-03-09 00:00:00 │
└─────┴──────────┴─────────┴────────┴─────────┴─────────────────────┘

=== filter(): orders over 100
shape: (3, 6)
┌─────┬──────────┬─────────┬────────┬────────┬─────────────────────┐
│ id  ┆ customer ┆ country ┆ amount ┆ coupon ┆ placed_at           │
│ --- ┆ ---      ┆ ---     ┆ ---    ┆ ---    ┆ ---                 │
│ i64 ┆ str      ┆ str     ┆ f64    ┆ str    ┆ datetime[μs]        │
╞═════╪══════════╪═════════╪════════╪════════╪═════════════════════╡
│ 1   ┆ ada      ┆ ES      ┆ 120.0  ┆ null   ┆ 2025-01-10 00:00:00 │
│ 3   ┆ grace    ┆ US      ┆ 980.0  ┆ null   ┆ 2025-02-02 00:00:00 │
│ 5   ┆ alan     ┆ UK      ┆ 240.0  ┆ null   ┆ 2025-03-01 00:00:00 │
└─────┴──────────┴─────────┴────────┴────────┴─────────────────────┘

=== filter() twice is AND, select() keeps some fields
shape: (1, 3)
┌─────┬──────────┬────────┐
│ id  ┆ customer ┆ amount │
│ --- ┆ ---      ┆ ---    │
│ i64 ┆ str      ┆ f64    │
╞═════╪══════════╪════════╡
│ 6   ┆ grace    ┆ 60.0   │
└─────┴──────────┴────────┘

=== filter() with any Polars expression
shape: (2, 3)
┌─────┬─────────┬────────┐
│ id  ┆ country ┆ coupon │
│ --- ┆ ---     ┆ ---    │
│ i64 ┆ str     ┆ str    │
╞═════╪═════════╪════════╡
│ 1   ┆ ES      ┆ null   │
│ 5   ┆ UK      ┆ null   │
└─────┴─────────┴────────┘

```

The same filter written as SQL with `where()`:

```text
=== where(): SQL
shape: (2, 3)
┌─────┬──────────┬────────┐
│ id  ┆ customer ┆ amount │
│ --- ┆ ---      ┆ ---    │
│ i64 ┆ str      ┆ f64    │
╞═════╪══════════╪════════╡
│ 3   ┆ grace    ┆ 980.0  │
│ 6   ┆ grace    ┆ 60.0   │
└─────┴──────────┴────────┘

```

`lazy()` returns a LazyFrame you can keep building on. The second plan shows that the filter (`SELECTION`) and the columns (`PROJECT 2/6`) are pushed into the Parquet scan, so only what is needed is read:

```text
=== lazy(): revenue per country, computed by Polars
shape: (4, 3)
┌─────────┬─────────┬────────┐
│ country ┆ revenue ┆ orders │
│ ---     ┆ ---     ┆ ---    │
│ str     ┆ f64     ┆ u32    │
╞═════════╪═════════╪════════╡
│ US      ┆ 1040.0  ┆ 2      │
│ UK      ┆ 240.0   ┆ 1      │
│ ES      ┆ 120.0   ┆ 1      │
│ FI      ┆ 35.5    ┆ 1      │
└─────────┴─────────┴────────┘

=== lazy(): the filter and the columns reach the Parquet scan
simple π 1/1 ["customer"]
  Parquet SCAN [file:///.../lake/sales/orders.parquet]
  PROJECT 2/6 COLUMNS
  SELECTION: col("amount") > 100.0
  ESTIMATED ROWS: 6

```

Queries are values, and contradictory ones are refused where they are written:

```text
=== a query is not run until you ask
built: ModelQuery
rows : 2
again: 1 (a new query; big_us is unchanged)

=== filter() and where() cannot be combined
ModelQueryError: filter() takes Polars expressions and where() takes an SQL predicate: a query can use only one of them
ModelQueryError: filter() takes Polars expressions and where() takes an SQL predicate: a query can use only one of them
ModelQueryError: filter() takes Polars expressions such as pl.col('id') > 10, not str; use where() for an SQL string
ModelQueryError: select(): ['nope'] are not fields of Order (fields: ['id', 'customer', 'country', 'amount', 'coupon', 'placed_at'])
```

See [Models](../models.md#reading) for every read method and
[Connections](../connections.md) for the loaders.
