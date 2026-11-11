# Querying a catalog

A [catalog](catalog-reference.md) reads a dataset by its key, with no model. This
page covers what you can pass to read only the rows and columns you need, from
loading the connection to the result.

If you want the fields and types of the data checked, with Django-style lookups
such as `amount__gte=100`, use a model instead: see
[Querying models](querying-models.md). Lookups need a model to be checked
against, so **the catalog has only SQL and Polars**.

| You write | Language | Runs on | Returns |
|-----------|----------|---------|---------|
| `load_polars(key, where=..., columns=...)` | SQL (DuckDB) | DuckDB | `pl.DataFrame` |
| `load_arrow(key, where=..., columns=...)` | SQL (DuckDB) | DuckDB | `pyarrow.Table` |
| `scan_polars(key, columns=...)` then Polars | Polars expressions | Polars | `pl.LazyFrame` |

## From the connection to the data

### 1. The connection

Connections live in a file (or in Airflow) and are referred to by name:

```yaml
# connections.yaml
connections:
  lake:
    conn_type: fs
    extra:
      base_path: /tmp/lake/
```

### 2. The catalog

Each entry says where a dataset lives, in which format, and how it is read and
written. This one holds the same data in three formats:

```yaml
# catalog.yaml
entries:
  orders_csv:
    file_format: csv
    connection: lake
    location: orders.csv
    load_options: {header: true}
    save_options: {header: true}
  orders_parquet:
    file_format: parquet
    connection: lake
    location: orders.parquet
  orders_delta:
    file_format: delta_table
    connection: lake
    location: orders_delta
    save_options: {mode: overwrite}
```

### 3. Load both and write something

```python
import polars as pl
from smallcat import Connections
from smallcat.catalog import Catalog

connections = Connections.from_yaml("connections.yaml")
catalog = Catalog.from_yaml("catalog.yaml", connections=connections)

catalog.save_polars("orders_parquet", orders)   # orders is a pl.DataFrame
```

The examples below read six orders: ids 1 to 6, from customers `ada`, `linus`,
`grace`, `ada`, `alan` and `grace`, with the columns `id`, `customer`,
`country`, `amount`, `coupon` and `placed_on`.

## `load_polars`: the whole dataset, or part of it

```python
catalog.load_polars("orders_parquet")
```

```text
shape: (6, 6)
┌─────┬──────────┬─────────┬────────┬─────────┬────────────┐
│ id  ┆ customer ┆ country ┆ amount ┆ coupon  ┆ placed_on  │
│ --- ┆ ---      ┆ ---     ┆ ---    ┆ ---     ┆ ---        │
│ i64 ┆ str      ┆ str     ┆ f64    ┆ str     ┆ date       │
╞═════╪══════════╪═════════╪════════╪═════════╪════════════╡
│ 1   ┆ ada      ┆ ES      ┆ 120.0  ┆ null    ┆ 2025-01-10 │
│ 2   ┆ linus    ┆ FI      ┆ 35.5   ┆ WELCOME ┆ 2025-01-21 │
│ 3   ┆ grace    ┆ US      ┆ 980.0  ┆ null    ┆ 2025-02-02 │
│ 4   ┆ ada      ┆ ES      ┆ 15.0   ┆ WELCOME ┆ 2025-02-14 │
│ 5   ┆ alan     ┆ UK      ┆ 240.0  ┆ null    ┆ 2025-03-01 │
│ 6   ┆ grace    ┆ US      ┆ 60.0   ┆ 50%OFF  ┆ 2025-03-09 │
└─────┴──────────┴─────────┴────────┴─────────┴────────────┘
```

## `where`: an SQL predicate

`where` is a single boolean SQL expression (DuckDB) over the dataset's columns.
The reader applies it, so on Parquet and Delta only what is needed is read.

```python
catalog.load_polars("orders_parquet", where="country = 'US' AND amount >= 60")
```

```text
shape: (2, 6)
┌─────┬──────────┬─────────┬────────┬────────┬────────────┐
│ id  ┆ customer ┆ country ┆ amount ┆ coupon ┆ placed_on  │
│ --- ┆ ---      ┆ ---     ┆ ---    ┆ ---    ┆ ---        │
│ i64 ┆ str      ┆ str     ┆ f64    ┆ str    ┆ date       │
╞═════╪══════════╪═════════╪════════╪════════╪════════════╡
│ 3   ┆ grace    ┆ US      ┆ 980.0  ┆ null   ┆ 2025-02-02 │
│ 6   ┆ grace    ┆ US      ┆ 60.0   ┆ 50%OFF ┆ 2025-03-09 │
└─────┴──────────┴─────────┴────────┴────────┴────────────┘
```

Because it is plain SQL, everything DuckDB accepts in a `WHERE` works:

| `where=` | Ids |
|----------|-----|
| `customer IN ('ada', 'alan')` | 1, 4, 5 |
| `customer LIKE 'g%'` | 3, 6 |
| `coupon LIKE '%!%%' ESCAPE '!'` | 6 |
| `country = 'FI' OR amount > 500` | 2, 3 |
| `amount BETWEEN 35.5 AND 120` | 1, 2, 6 |

### `columns`: read only some columns

```python
catalog.load_polars(
    "orders_parquet",
    where="amount > 100",
    columns=["id", "customer", "amount"],
)
```

```text
shape: (3, 3)
┌─────┬──────────┬────────┐
│ id  ┆ customer ┆ amount │
│ --- ┆ ---      ┆ ---    │
│ i64 ┆ str      ┆ f64    │
╞═════╪══════════╪════════╡
│ 1   ┆ ada      ┆ 120.0  │
│ 3   ┆ grace    ┆ 980.0  │
│ 5   ┆ alan     ┆ 240.0  │
└─────┴──────────┴────────┘
```

`columns` accepts plain column names only, not expressions.

### The same predicate on every format

`where` does not depend on the format. The three entries hold the same data and
give the same rows:

```python
predicate = "placed_on >= DATE '2025-02-01' AND coupon IS NULL"
for key in ("orders_csv", "orders_parquet", "orders_delta"):
    print(key, sorted(catalog.load_polars(key, where=predicate)["id"].to_list()))
```

```text
orders_csv      [3, 5]
orders_parquet  [3, 5]
orders_delta    [3, 5]
```

## `load_arrow`: the same, as an Arrow table

```python
table = catalog.load_arrow("orders_delta", where="amount > 100", columns=["id"])
# pyarrow.Table with 3 rows
```

## `scan_polars`: Polars expressions, lazily

`scan_polars` returns a `pl.LazyFrame`. You filter it with **Polars
expressions**, not SQL, and nothing is read until `collect()`.

```python
(
    catalog.scan_polars("orders_parquet")
    .filter(pl.col("country").is_in(["ES", "UK"]) & pl.col("coupon").is_null())
    .select("id", "customer", "amount")
    .collect()
)
```

```text
shape: (2, 3)
┌─────┬──────────┬────────┐
│ id  ┆ customer ┆ amount │
│ --- ┆ ---      ┆ ---    │
│ i64 ┆ str      ┆ f64    │
╞═════╪══════════╪════════╡
│ 1   ┆ ada      ┆ 120.0  │
│ 5   ┆ alan     ┆ 240.0  │
└─────┴──────────┴────────┘
```

On Parquet and Delta the scan is native, so the filter and the columns reach the
file scan. Other formats are read in full and then wrapped.

```text
>>> print(catalog.scan_polars("orders_delta").filter(pl.col("amount") > 100)
...       .select("customer").explain())
simple π 1/1 ["customer"]
  Parquet SCAN [.../orders_delta/part-00000-....snappy.parquet]
  PROJECT 2/6 COLUMNS
  SELECTION: col("amount") > 100.0
```

Anything else Polars does can follow, such as aggregating without loading the
whole table first:

```python
(
    catalog.scan_polars("orders_parquet")
    .group_by("country")
    .agg(pl.col("amount").sum().alias("revenue"))
    .sort("revenue", descending=True)
    .collect()
)
```

```text
shape: (4, 2)
┌─────────┬─────────┐
│ country ┆ revenue │
│ ---     ┆ ---     │
│ str     ┆ f64     │
╞═════════╪═════════╡
│ US      ┆ 1040.0  │
│ UK      ┆ 240.0   │
│ ES      ┆ 135.0   │
│ FI      ┆ 35.5    │
└─────────┴─────────┘
```

## Which one to use

* **`where=`** when you know the condition as SQL. It works the same on CSV,
  Excel, Parquet and Delta.
* **`scan_polars`** when you want Polars expressions, a lazy read, or to chain
  more Polars after the filter. Prefer it on large Parquet or Delta tables.
* **A model** when you want the data checked against declared fields and types,
  or Django-style lookups: see [Querying models](querying-models.md).

## What is refused

`where` is parsed before it runs. A statement, a `;`, an unbalanced comment, a
subquery or anything that changes the shape of the query raises `ValueError`:

```text
ValueError: Unsafe or invalid WHERE clause     # 1=1; DROP TABLE data
ValueError: Unsafe or invalid WHERE clause     # id IN (SELECT id FROM read_csv('/etc/passwd'))
ValueError: Unsafe or invalid WHERE clause     # amount >
```

!!! warning "SQL is text"
    The predicate is checked for shape, not for column names or types, and it is
    not parametrised: do not build it by pasting user input into the string. If
    the condition comes from a variable, use a [model](querying-models.md) with
    lookups, which quote the values for you.

A key that is not in the catalog raises `KeyError`.

## The complete script

From the connection to every read above. It writes its files under a temporary
directory, and needs nothing but smallcat-chonk.

```sh
pip install smallcat-chonk
python querying_catalog.py
```

```python
--8<-- "docs/examples/querying_catalog.py"
```
