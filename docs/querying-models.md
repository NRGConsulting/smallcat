# Querying models (ORM)

A model reads its data through `Model.objects`. This page covers the three ways
of choosing which rows to read, and when to use each one:

| You write | Language | Runs on | Good for |
|-----------|----------|---------|----------|
| [`filter(pl.col("amount") > 100)`](#filter-polars-expressions) | Polars expressions | Polars | Anything Polars can express; lazy reads with pushdown on Parquet and Delta |
| [`where("amount > 100")`](#where-with-sql) | SQL (DuckDB) | DuckDB | Predicates you already have as SQL, `OR`, `LIKE`, `BETWEEN` |
| [`where(amount__gt=100)`](#where-with-lookups) | Django-style lookups | DuckDB | Simple conditions checked against the model, with no SQL to write |

The models themselves, `Meta` and registration are described in
[Models](models.md). A runnable script with every case on this page, from
loading the connection to the query, is at the [end](#the-complete-script).

## Setup used on this page

The examples query this model, registered against a connection named `lake`:

```python
class Order(Model):
    id: int
    customer: str
    country: str
    amount: float
    coupon: str | None = None
    status: Literal["new", "paid", "shipped"]
    paid: bool
    placed_on: date
    placed_at: datetime

    class Meta:
        location = "sales/orders.parquet"
        file_format = ModelFileFormat.PARQUET
```

The data has six orders: ids 1 to 6, from customers `ada`, `linus`, `grace`,
`ada`, `alan` and `grace`. The printed ids below are the ids of the rows each
query returns.

## One query, whichever way you filter

`filter()`, `where()` and `select()` return a **query**. It is immutable (each
call returns a new one) and reads nothing until you run it:

```python
query = Order.objects.where(country="ES", amount__gte=100)

query.all()          # pl.DataFrame
query.lazy()         # pl.LazyFrame
query.as_models()    # list[Order], validated by Pydantic
query.select("id", "customer").all()   # only some fields
```

!!! note "`filter` and `where` are alternatives"
    They run on different engines, so one query uses **only one of them**.
    Combining them, in either order, raises `ModelQueryError` where you write
    it. `select()` goes with either.

## `filter`: Polars expressions

`filter()` takes Polars expressions. Several calls are combined with AND.

```python
Order.objects.filter(pl.col("amount") > 100)                       # [1, 3, 5]
Order.objects.filter(pl.col("country") == "US").filter(pl.col("amount") < 500)  # [6]
Order.objects.filter(
    pl.col("country").is_in(["ES", "UK"]) & pl.col("coupon").is_null(),
)                                                                   # [1, 5]
Order.objects.filter(pl.col("customer").str.len_chars() == 3)      # [1, 4]
```

On Parquet and Delta, a filtered query scanned with `lazy()` pushes the filter
and the columns down to the storage, so only what is needed is read:

```text
>>> print(Order.objects.filter(pl.col("amount") > 100).select("customer").lazy().explain())
simple π 1/1 ["customer"]
  Parquet SCAN [file:///.../sales/orders.parquet]
  PROJECT 2/9 COLUMNS
  SELECTION: col("amount") > 100.0
```

`filter()` takes Polars expressions only: a string raises `ModelQueryError` that
points you to `where()`.

## `where` with SQL

`where()` takes an SQL predicate (DuckDB), as the first argument or as `sql=`:

```python
Order.objects.where("country = 'US' AND amount >= 60")              # [3, 6]
Order.objects.where(sql="country = 'US' AND amount >= 60")          # [3, 6]
Order.objects.where(sql="country = 'FI' OR amount > 500")           # [2, 3]
```

`sql=` is the clearer spelling when it sits next to lookups. The predicate is
parsed before it runs. Statements, `;`, comments left open and subqueries are
refused with `ValueError`.

!!! warning "SQL is text"
    The SQL is **not** checked against the model: a typo in a column name only
    fails when the query runs. Do not build it by pasting user input into the
    string. Use [lookups](#where-with-lookups), which validate the field and the
    value, when the condition comes from a variable.

## `where` with lookups

A lookup is `field=value` or `field__operator=value`, like Django:

```python
Order.objects.where(id=3)                    # id = 3
Order.objects.where(amount__lte=60)          # amount <= 60
Order.objects.where(country="US", paid=True) # country = 'US' AND paid
```

They are checked **when the query is built**, against the model's own fields and
types. Nothing is declared per model and nothing is generated: the fields come
from the model itself.

```python
Order.objects.where(custmer="ada")
# ModelQueryError: where(): 'custmer' is not a field of Order; did you mean 'customer'? ...
```

### Operators

| Operator | Meaning | Value you pass |
|----------|---------|----------------|
| `exact` | equal (the default when you write only the field) | one value, or `None` |
| `ne` | not equal | one value, or `None` |
| `lt` | less than | one value |
| `lte` | less than or equal | one value |
| `gt` | greater than | one value |
| `gte` | greater than or equal | one value |
| `in` | equal to any of the values | a list, tuple or set (an empty one matches nothing) |
| `range` | between two values, both included | a pair `(low, high)` |
| `isnull` | `True`: is null. `False`: is not null | a `bool` |
| `contains` | the text contains the value | a `str` |
| `startswith` | the text starts with the value | a `str` |
| `endswith` | the text ends with the value | a `str` |

```python
Order.objects.where(country__ne="US")                          # [1, 2, 4, 5]
Order.objects.where(amount__lt=60)                             # [2, 4]
Order.objects.where(amount__gte=240)                           # [3, 5]
Order.objects.where(status__in=["new", "shipped"])             # [2, 3, 5]
Order.objects.where(amount__range=(35.5, 120))                 # [1, 2, 6]
Order.objects.where(coupon__isnull=True)                       # [1, 3, 5]
Order.objects.where(customer__contains="a")                    # [1, 3, 4, 5, 6]
Order.objects.where(customer__startswith="gr")                 # [3, 6]
Order.objects.where(customer__endswith="n")                    # [5]
Order.objects.where(paid=False)                                # [2, 5]
Order.objects.where(placed_on__gte=date(2025, 2, 1))           # [3, 4, 5, 6]
Order.objects.where(placed_at__lt=datetime(2025, 2, 1, 12, 0)) # [1, 2]
```

### Which operators each type accepts

| Field type | Operators |
|------------|-----------|
| `int`, `float`, `date`, `datetime`, `time`, `timedelta` | `exact`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `range`, `isnull` |
| `str`, and `Literal` of strings | `exact`, `ne`, `in`, `contains`, `startswith`, `endswith`, `isnull` |
| `bool` | `exact`, `ne`, `isnull` |
| `list[...]`, `bytes` | `isnull` only |

An operator the type does not take is refused when you build the query, and the
message lists the ones that it does:

```text
ModelQueryError: where(): 'gt' is not available for 'customer', a str field
(available: exact, ne, in, contains, startswith, endswith, isnull)
```

### Restrictions

* **AND only.** Several lookups in one call are joined with AND. There is no
  `OR` or `NOT`: write it with `sql=` (see [below](#sql-and-lookups-together)).
* **`where` is used once per query.** Put every condition in the same call.
  A second `where()` raises `ModelQueryError`.
* **Not with `filter`.** See the note [above](#one-query-whichever-way-you-filter).
* **Values must match the field.** `amount="100"` on a float field, `id=True`
  or `id=1.5` on an int field, a `datetime` on a `date` field, and so on, raise
  `ModelQueryError`. A `datetime` must have **no time zone**.
* **`NaN` and infinity** are refused.
* **`None` has two spellings.** `coupon=None` and `coupon__isnull=True` mean the
  same, and `coupon__ne=None` is `coupon__isnull=False`. `None` inside `in` or
  `range` is refused: use `isnull`.
* **Nulls do not match comparisons.** As in SQL, a null is neither equal to nor
  different from a value, so `country__ne="US"` does not return rows whose
  `country` is null. To include them, write
  `sql="country IS NULL OR country <> 'US'"`.
* **`contains`, `startswith` and `endswith`** match literally: `%` and `_` in the
  value mean themselves, not wildcards, and they are case sensitive.
* **A field called `sql`.** `sql=` is always the SQL text. Reach a field with
  that name as `sql__exact=...`.
* **A `timedelta` field in Parquet** cannot be read back through smallcat today
  (DuckDB writes an `INTERVAL` that Polars cannot read). The `timedelta`
  lookups exist for other sources, but avoid that field in Parquet models.

### Values are data

Names come from the model and values are quoted for you, so nothing you pass is
interpreted as SQL:

```python
Order.objects.where(customer="x' OR '1'='1")    # []   a quote is just a quote
Order.objects.where(coupon__contains="%")       # [6]  a literal percent sign
Order.objects.where(customer__contains="_")     # []   not "any character"
```

### SQL and lookups together

`sql=` and lookups can share one call. They are joined with AND, and the SQL is
wrapped in its own parentheses, so an `OR` inside it stays inside:

```python
Order.objects.where(sql="country = 'FI' OR amount > 500", paid=True)   # [3]
```

This is how you write what lookups do not cover (`OR`, `NOT`, functions) and
still keep the simple conditions validated.

## The same rows, three ways

```python
Order.objects.filter((pl.col("amount") >= 60) & ~pl.col("paid"))   # [5]
Order.objects.where(sql="amount >= 60 AND NOT paid")               # [5]
Order.objects.where(amount__gte=60, paid=False)                    # [5]
```

Choose with this in mind:

* **Lookups** when the condition is simple and may come from a variable: they
  are validated, and cannot carry SQL.
* **SQL** for `OR`, `NOT`, `LIKE`, `BETWEEN` or anything lookups lack.
* **Polars** when you want the lazy read with pushdown on Parquet and Delta, or
  a function only Polars has. A query with `where()` runs its SQL straight away
  and wraps the result.

## What fails, and where

| Error | Cause |
|-------|-------|
| `ModelQueryError` | A field that does not exist, an operator the type does not take, a value of the wrong type, `where()` twice, `filter()` with `where()`, an empty `where()` |
| `ValueError` | The SQL is not a plain predicate (`;`, a subquery, ...). Raised when the query runs |

Lookup errors are raised on the line that builds the query, not when it runs.

```text
ModelQueryError: where(): 'between' is not a lookup operator (in 'amount__between'); 'amount' accepts: exact, ne, lt, lte, gt, gte, in, range, isnull
ModelQueryError: amount: a float field does not take '100' (str)
ModelQueryError: status__in: None is not allowed in the values; use status__isnull
ModelQueryError: amount__range: expects exactly 2 values, got 1
ModelQueryError: where() was already used: put every condition in one call (joined with AND)
```

## The complete script

From the connection to every query above. It writes its files under a temporary
directory, and needs nothing but smallcat-chonk.

```sh
pip install smallcat-chonk
python querying_models.py
```

```python
--8<-- "docs/examples/querying_models.py"
```
