# ruff: noqa: CPY001, D101, D103, D106, DTZ001, INP001, PLR2004, PLW0108, T201
"""Querying models, from the connection to every way of filtering.

Run it with ``python docs/examples/querying_models.py``. It writes its
connections file and its data under a temporary directory.
"""

import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import polars as pl

from smallcat import Connections, Model, ModelFileFormat, ModelWriteMode, registry
from smallcat.models import ModelQuery, ModelQueryError

workdir = Path(tempfile.mkdtemp(prefix="smallcat-querying-"))


def title(text: str) -> None:
    print(f"\n=== {text}")


def ids(query: ModelQuery) -> list[int]:
    """The ids a query returns, to keep the output short."""
    return sorted(query.select("id").all()["id"].to_list())


# 1. Load the connection -------------------------------------------------------
(workdir / "connections.yaml").write_text(
    f"""
connections:
  lake:
    conn_type: fs
    extra:
      base_path: {(workdir / "lake").as_posix()}
""",
)
connections = Connections.from_yaml(workdir / "connections.yaml")


# 2. Define the model and register it ---------------------------------------------
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


registry.load_connections(connections)
registry.register(Order, connection="lake")

# 3. Some data to query ------------------------------------------------------------
Order.objects.save(
    pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "customer": ["ada", "linus", "grace", "ada", "alan", "grace"],
            "country": ["ES", "FI", "US", "ES", "UK", "US"],
            "amount": [120.0, 35.5, 980.0, 15.0, 240.0, 60.0],
            "coupon": [None, "WELCOME", None, "WELCOME", None, "50%OFF"],
            "status": ["paid", "new", "shipped", "paid", "new", "paid"],
            "paid": [True, False, True, True, False, True],
            "placed_on": [
                date(2025, 1, 10),
                date(2025, 1, 21),
                date(2025, 2, 2),
                date(2025, 2, 14),
                date(2025, 3, 1),
                date(2025, 3, 9),
            ],
            "placed_at": [
                datetime(2025, 1, 10, 9, 30),
                datetime(2025, 1, 21, 14, 0),
                datetime(2025, 2, 2, 8, 15),
                datetime(2025, 2, 14, 17, 45),
                datetime(2025, 3, 1, 11, 0),
                datetime(2025, 3, 9, 20, 5),
            ],
        },
    ),
    mode=ModelWriteMode.OVERWRITE,
)

# 4. filter(): Polars expressions ----------------------------------------------------
title("filter(): Polars expressions")
print("amount > 100        :", ids(Order.objects.filter(pl.col("amount") > 100)))
print(
    "US and amount < 500 :",
    ids(Order.objects.filter(pl.col("country") == "US").filter(pl.col("amount") < 500)),
)
print(
    "coupon missing, ES/UK:",
    ids(
        Order.objects.filter(
            pl.col("country").is_in(["ES", "UK"]) & pl.col("coupon").is_null(),
        ),
    ),
)
print(
    "any Polars function :",
    ids(Order.objects.filter(pl.col("customer").str.len_chars() == 3)),
)

title("filter() stays lazy: the filter and the columns reach the Parquet scan")
print(Order.objects.filter(pl.col("amount") > 100).select("customer").lazy().explain())

# 5. where() with SQL ---------------------------------------------------------------
title("where(): SQL, positional or with sql=")
print("positional :", ids(Order.objects.where("country = 'US' AND amount >= 60")))
print("sql=       :", ids(Order.objects.where(sql="country = 'US' AND amount >= 60")))
print("OR is fine :", ids(Order.objects.where(sql="country = 'FI' OR amount > 500")))

# 6. where() with lookups -------------------------------------------------------------
title("where(): lookups, field or field__operator")
print("exact       :", ids(Order.objects.where(id=3)))
print("explicit    :", ids(Order.objects.where(id__exact=3)))
print("ne          :", ids(Order.objects.where(country__ne="US")))
print("lt          :", ids(Order.objects.where(amount__lt=60)))
print("lte         :", ids(Order.objects.where(amount__lte=60)))
print("gt          :", ids(Order.objects.where(amount__gt=240)))
print("gte         :", ids(Order.objects.where(amount__gte=240)))
print("in          :", ids(Order.objects.where(status__in=["new", "shipped"])))
print("range       :", ids(Order.objects.where(amount__range=(35.5, 120))))
print("isnull True :", ids(Order.objects.where(coupon__isnull=True)))
print("isnull False:", ids(Order.objects.where(coupon__isnull=False)))
print("contains    :", ids(Order.objects.where(customer__contains="a")))
print("startswith  :", ids(Order.objects.where(customer__startswith="gr")))
print("endswith    :", ids(Order.objects.where(customer__endswith="n")))
print("bool        :", ids(Order.objects.where(paid=False)))
print("date        :", ids(Order.objects.where(placed_on__gte=date(2025, 2, 1))))
print(
    "datetime    :",
    ids(Order.objects.where(placed_at__lt=datetime(2025, 2, 1, 12, 0))),
)
print(
    "date range  :",
    ids(Order.objects.where(placed_on__range=(date(2025, 2, 1), date(2025, 2, 28)))),
)

title("several lookups are joined with AND")
print(ids(Order.objects.where(country="US", paid=True, amount__lt=500)))

title("values are data: quotes, % and _ mean what they say")
print("a quote           :", ids(Order.objects.where(customer="x' OR '1'='1")))
print("a literal percent :", ids(Order.objects.where(coupon__contains="%")))
print("not a wildcard    :", ids(Order.objects.where(customer__contains="_")))

# 7. SQL and lookups in the same call --------------------------------------------------
title("sql= and lookups together are joined with AND")
query = Order.objects.where(sql="country = 'FI' OR amount > 500", paid=True)
print(ids(query), "(the OR stays inside its own parentheses)")

# 8. The same question, three ways -----------------------------------------------------
title("the same rows, written three ways")
by_polars = ids(Order.objects.filter((pl.col("amount") >= 60) & ~pl.col("paid")))
by_sql = ids(Order.objects.where(sql="amount >= 60 AND NOT paid"))
by_lookups = ids(Order.objects.where(amount__gte=60, paid=False))
print(by_polars, by_sql, by_lookups)
assert by_polars == by_sql == by_lookups

# 9. Running the query -----------------------------------------------------------------
title("where() runs like any query: all(), lazy(), as_models(), select()")
query = Order.objects.where(country="ES", amount__gte=100)
print(query.select("id", "customer").all())
print([order.customer for order in query.as_models()])
print(query.lazy().collect().height, "row")

# 10. What is refused ------------------------------------------------------------------
title("a lookup that does not fit the model fails where you write it")
for build in (
    lambda: Order.objects.where(custmer="ada"),
    lambda: Order.objects.where(amount__between=(1, 2)),
    lambda: Order.objects.where(customer__gt="a"),
    lambda: Order.objects.where(amount="100"),
    lambda: Order.objects.where(status__in=[]),  # valid: matches nothing
    lambda: Order.objects.where(status__in=["new", None]),
    lambda: Order.objects.where(amount__range=(1,)),
    lambda: Order.objects.where(placed_at=datetime(2025, 1, 1, tzinfo=UTC)),
):
    try:
        print("accepted:", ids(build()))
    except ModelQueryError as error:
        print(f"{type(error).__name__}: {error}")

title("where() is used once and does not mix with filter()")
for build in (
    lambda: Order.objects.where(id=1).where(country="ES"),
    lambda: Order.objects.filter(pl.col("id") > 1).where(country="ES"),
    lambda: Order.objects.where(),
):
    try:
        build()
    except ModelQueryError as error:
        print(f"{type(error).__name__}: {error}")
