# ruff: noqa: CPY001, D101, D103, D106, DTZ001, INP001, PLR2004, T201
"""From loading a connection to filtering a model's data.

Run it with ``python docs/examples/models_query.py``. It writes its connections
file and its data under a temporary directory.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl

from smallcat import Connections, Model, ModelWriteMode, registry
from smallcat.models import ModelQueryError

workdir = Path(tempfile.mkdtemp(prefix="smallcat-query-"))


def title(text: str) -> None:
    print(f"\n=== {text}")


# 1. Load the connection ----------------------------------------------------
# It lives in a file (or in Airflow). Loading it validates it.
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
print(connections)  # names and types only, never the content


# 2. Define the model and register it against that connection -------------------
class Order(Model):
    id: int
    customer: str
    country: str
    amount: float
    coupon: str | None = None
    placed_at: datetime

    class Meta:
        location = "sales/orders.parquet"
        file_format = "parquet"


registry.load_connections(connections)
registry.register(Order, connection="lake")

# 3. Some data to query ---------------------------------------------------------
Order.objects.save(
    [
        Order(
            id=1,
            customer="ada",
            country="ES",
            amount=120.0,
            placed_at=datetime(2025, 1, 10),
        ),
        Order(
            id=2,
            customer="linus",
            country="FI",
            amount=35.5,
            coupon="WELCOME",
            placed_at=datetime(2025, 1, 21),
        ),
        Order(
            id=3,
            customer="grace",
            country="US",
            amount=980.0,
            placed_at=datetime(2025, 2, 2),
        ),
        Order(
            id=4,
            customer="ada",
            country="ES",
            amount=15.0,
            coupon="WELCOME",
            placed_at=datetime(2025, 2, 14),
        ),
        Order(
            id=5,
            customer="alan",
            country="UK",
            amount=240.0,
            placed_at=datetime(2025, 3, 1),
        ),
        Order(
            id=6,
            customer="grace",
            country="US",
            amount=60.0,
            placed_at=datetime(2025, 3, 9),
        ),
    ],
    mode=ModelWriteMode.OVERWRITE,
)

# 4. all(): everything, as a DataFrame ---------------------------------------
title("all()")
print(Order.objects.all().sort("id"))

# 5. filter(): Polars expressions -----------------------------------------------
title("filter(): orders over 100")
print(Order.objects.filter(pl.col("amount") > 100).all().sort("id"))

title("filter() twice is AND, select() keeps some fields")
print(
    Order.objects.filter(pl.col("country") == "US")
    .filter(pl.col("amount") < 500)
    .select("id", "customer", "amount")
    .all(),
)

title("filter() with any Polars expression")
print(
    Order.objects.filter(
        pl.col("country").is_in(["ES", "UK"]) & pl.col("coupon").is_null(),
    )
    .select("id", "country", "coupon")
    .all()
    .sort("id"),
)

# 6. where(): the same idea, written as an SQL predicate ---------------------
title("where(): SQL")
print(
    Order.objects.where("country = 'US' AND amount >= 60")
    .select("id", "customer", "amount")
    .all()
    .sort("id"),
)

# 7. lazy(): a LazyFrame to keep chaining ---------------------------------------
title("lazy(): revenue per country, computed by Polars")
revenue = (
    Order.objects.filter(pl.col("amount") > 20)
    .lazy()
    .group_by("country")
    .agg(pl.col("amount").sum().alias("revenue"), pl.len().alias("orders"))
    .sort("revenue", descending=True)
)
print(revenue.collect())

title("lazy(): the filter and the columns reach the Parquet scan")
plan = Order.objects.filter(pl.col("amount") > 100).select("customer").lazy()
print(plan.explain())

# 8. Queries are values: build them up, run them when you want ------------------
title("a query is not run until you ask")
big_us = Order.objects.filter(pl.col("country") == "US")  # nothing is read yet
print("built:", type(big_us).__name__)
print("rows :", big_us.all().height)
print(
    "again:",
    big_us.filter(pl.col("amount") > 100).all().height,
    "(a new query; big_us is unchanged)",
)

# 9. What is refused ---------------------------------------------------------------
title("filter() and where() cannot be combined")
for build in (
    lambda: Order.objects.filter(pl.col("amount") > 1).where("id < 3"),
    lambda: Order.objects.where("id < 3").filter(pl.col("amount") > 1),
    lambda: Order.objects.filter("amount > 1"),
    lambda: Order.objects.select("nope"),
):
    try:
        build()
    except ModelQueryError as error:
        print(f"{type(error).__name__}: {error}")
