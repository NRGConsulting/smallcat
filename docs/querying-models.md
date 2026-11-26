# Querying models (ORM)

A model reads its data through `Model.objects`. This page covers the methods that
read it (`all`, `lazy`, `as_models`) and the three ways of choosing which rows to
read, and when to use each one:

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

`filter()`, `where()`, `exclude()` and `select()` return a **query**. It is immutable (each
call returns a new one) and reads nothing until you run it:

```python
query = Order.objects.where(country="ES", amount__gte=100)

query.all()          # pl.DataFrame
query.lazy()         # pl.LazyFrame
query.all(as_models=True)   # list[Order], validated by Pydantic
query.as_models()    # the same, as a shortcut
query.select("id", "customer").all()   # only some fields
query.first()        # one row (or None)
query.count()        # number of rows
```

!!! note "`filter` and `where` are alternatives"
    They run on different engines, so one query uses **only one of them**.
    Combining them, in either order, raises `ModelQueryError` where you write
    it. `exclude()` belongs with `where()`. `select()` goes with either.

## The methods

| Method | Returns | Notes |
|--------|---------|-------|
| `all()` | `pl.DataFrame` | Columns in the model's order. With `all(as_models=True)` it returns `list[Order]` instead (see `as_models()`). Also available on `Order.objects`. |
| `lazy()` | `pl.LazyFrame` | Chain more Polars after it. See [Lazy reads](#lazy-reads). |
| `as_models()` | `list[Order]` | Shortcut for `all(as_models=True)`. Each row is validated by Pydantic, so field constraints apply. Cannot be combined with `select()`. |
| `filter(*exprs)` | query | Polars expressions; several calls are combined with AND. |
| `where(sql=None, **lookups)` | query | An SQL predicate (DuckDB) and/or Django-style lookups such as `id__gte=10`. Usable once per query. |
| `exclude(sql=None, **lookups)` | query | The opposite of `where()`: drops the rows that match. Can be repeated and goes before or after `where()`. See [`exclude`](#exclude). |
| `first(as_models=False)` | `pl.DataFrame \| Order \| None` | The first row, as a one-row DataFrame or an instance; `None` if there are none. See [First, last and count](#first-last-and-count). |
| `last(as_models=False)` | `pl.DataFrame \| Order \| None` | The last row, like `first()`. |
| `count()` | `int` | How many rows match. |
| `select(*fields)` | query | Only some fields; names must be fields of the model. |
| `create_table()` | `bool` | Creates the dataset empty, with the model's schema, if it does not exist. See [Creating the table](#creating-the-table). |

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
| `int`, `float`, `date`, `datetime`, `time`, `timedelta` (see the restrictions) | `exact`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `range`, `isnull` |
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
* **`timedelta` fields.** No file format can hold one written by smallcat (see
  [Types a file format cannot hold](models.md#types-a-file-format-cannot-hold)),
  so models rarely have one. The lookups exist for the read-only Parquet case,
  but there `where()` cannot read the column back: use `filter()` instead.

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

## `exclude`

`exclude()` is the opposite of `where()`: it takes the same arguments (an SQL
predicate and/or lookups) and drops the rows that match.

```python
Order.objects.exclude(country="US")              # [1, 2, 4, 5]
Order.objects.exclude("amount > 100")            # [2, 4, 6]
Order.objects.exclude(country="US", paid=True)   # drops only rows that are both
Order.objects.where(amount__gt=50).exclude(status="new")
```

- Several conditions in one call drop only the rows that satisfy **all** of
  them (`exclude(a=1, b=2)` drops `a=1 AND b=2`), as in Django.
- It can be called several times, before or after `where()`: each call drops
  its own rows. Unlike `where()`, it is not limited to one use.
- A row whose condition is unknown because of a null is **kept**:
  `exclude(coupon="X")` keeps the rows with no coupon.
- It takes SQL and lookups only, like `where()`, so it cannot be combined with
  `filter()` (`ModelQueryError`).

## First, last and count

```python
Order.objects.first()                         # one-row DataFrame, or None
Order.objects.where(country="ES").last(as_models=True)   # Order, or None
Order.objects.exclude(status="new").count()   # int
```

- `first()` and `last()` return `None` when no row matches. With
  `as_models=True` they return an instance (validated by Pydantic), which
  cannot be combined with `select()`.
- "First" and "last" follow the order in which the dataset stores the rows.
  Delta tables and several files read together do not guarantee one, and there
  is no `order_by()`. `last()` reads the whole result to find the last row.
- `count()` checks the schema but does not validate the data (nulls in
  required fields, values that cannot be cast), like `lazy()`. `select()` does
  not change it.
- All of them are also available on `Order.objects`.

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

## Lazy reads

For Parquet and Delta, `lazy()` scans natively and what you chain after it is
pushed down to the storage: only the needed columns and row groups are read (see
the plan under [`filter`](#filter-polars-expressions)). CSV and Excel are read in
full and then wrapped. A query with `where()` runs its SQL immediately.

`lazy()` checks the columns and types, but two checks need data and are **not**
made on a frame you collect yourself: nulls in required fields (use `all()`),
and, in `COERCE` mode, values that cannot be cast, which fail with a Polars
error when collected.

## Creating the table

Reading a model whose table or file has not been written yet fails (for Delta,
`TableNotFoundError: No files in log segment`). `create_table()` creates it empty:

```python
Jobs.objects.create_table()   # True: created; False: it already existed
Jobs.objects.all()            # an empty DataFrame with the model's columns
```

- Delta creates the table; CSV, Parquet and Excel create the file with no rows.
- If it exists, nothing is touched.
- A model that allows no write mode (read-only) is not created: a `UserWarning`
  is emitted and it returns `False`, with no error.
- An empty CSV or Excel file has no types: its columns are read as text. With the
  default `strict` validation that fails on read, so use
  `validation_mode = "coerce"` in `Meta` for those formats. Excel also needs
  `header: True` in `load_options` and `save_options`.

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

## Custom managers

`Order.objects` is a `ModelManager`. To give a model methods of its own, subclass
`ModelManager` and declare it as `objects`:

```python
from smallcat.models import Model, ModelManager


class OrderManager(ModelManager["Order"]):
    def get_by_id(self, id: int) -> "Order | None":
        rows = self.where(id=id).all(as_models=True)
        return rows[0] if rows else None


class Order(Model):
    id: int
    objects = OrderManager()

    class Meta:
        location = "silver/orders"
        file_format = "parquet"


Order.objects.get_by_id(3)   # Order | None
Order.objects.where(id=3)    # still available
```

* Without `objects = ...` a model uses the default `ModelManager`, typed for
  that model (`list[Order]`).
* `ModelManager["Order"]` is optional. Without it the results are typed
  `list[Model]`; with it, `list[Order]`. When given, it must name the model that
  declares the manager, or defining the model raises `ModelDefinitionError`.
* A model that inherits from `Order` gets the manager bound to itself.
* `objects` is a reserved name: anything that is not a `ModelManager` (a field,
  a number, the manager class instead of an instance) raises
  `ModelDefinitionError`.
