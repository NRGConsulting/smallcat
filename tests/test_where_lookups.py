"""SC-038: Django-style lookups in `where()` (``job_id__lte=2``) and `sql=`."""

import datetime as dt
from typing import Literal

import polars as pl
import pytest

from smallcat import Connections
from smallcat.models import (
    Model,
    ModelQueryError,
    ModelRegistry,
    ModelWriteMode,
    use_registry,
)
from smallcat.models.lookups import OPERATORS, kind_of, lookups_to_sql


class Job(Model):
    id: int
    name: str
    score: float | None = None
    active: bool
    born: dt.date
    seen: dt.datetime | None = None
    at: dt.time
    status: Literal["new", "open", "done"]
    tags: list[str]

    class Meta:
        location = "jobs.parquet"
        file_format = "parquet"


class Weird(Model):
    """A model with a field called `sql`, to show how to reach it."""

    id: int
    sql: str

    class Meta:
        location = "weird.parquet"
        file_format = "parquet"


class JobDelta(Model):
    """Delta has no time or duration type, so this model leaves them out."""

    id: int
    name: str
    active: bool
    born: dt.date
    seen: dt.datetime | None = None

    class Meta:
        location = "jobs_delta"
        file_format = "delta_table"


def _jobs() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["alpha", "beta", "50% off", "a_b", "it's :x"],
            "score": [1.5, None, 3.5, 4.0, 0.0],
            "active": [True, False, True, False, True],
            "born": [
                dt.date(2020, 1, 1),
                dt.date(2021, 6, 15),
                dt.date(2022, 12, 31),
                dt.date(2023, 3, 3),
                dt.date(2024, 2, 29),
            ],
            "seen": [
                dt.datetime(2024, 1, 1, 10, 0),
                None,
                dt.datetime(2024, 1, 2, 11, 30, 5, 250000),
                dt.datetime(2024, 1, 3),
                dt.datetime(2024, 1, 4, 23, 59, 59),
            ],
            "at": [
                dt.time(8, 0),
                dt.time(12, 30, 15),
                dt.time(18, 45),
                dt.time(23, 59, 59),
                dt.time(0, 0),
            ],
            "status": ["new", "open", "done", "open", "new"],
            "tags": [["a"], ["b", "c"], [], ["a"], ["z"]],
        },
    )


@pytest.fixture
def registry(tmp_path) -> ModelRegistry:
    reg = ModelRegistry()
    reg.load_connections(
        Connections.from_dict(
            {
                "connections": {
                    "local": {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}},
                },
            },
        ),
    )
    for model in (Job, Weird, JobDelta):
        reg.register(model, connection="local")
    return reg


@pytest.fixture(autouse=True)
def _data(registry):
    with use_registry(registry):
        Job.objects.save(_jobs(), mode=ModelWriteMode.OVERWRITE)
        JobDelta.objects.save(
            _jobs().select("id", "name", "active", "born", "seen"),
            mode=ModelWriteMode.OVERWRITE,
        )
        Weird.objects.save(
            pl.DataFrame({"id": [1, 2], "sql": ["x", "y"]}),
            mode=ModelWriteMode.OVERWRITE,
        )
        yield


def _ids(query) -> list[int]:
    return sorted(query.select("id").all()["id"].to_list())


# ---------- every lookup must agree with the same filter written in Polars ----------

PARITY = [
    # exact and ne
    ({"id": 3}, pl.col("id") == 3),
    ({"id__exact": 3}, pl.col("id") == 3),
    ({"id__ne": 3}, pl.col("id") != 3),
    ({"name": "alpha"}, pl.col("name") == "alpha"),
    ({"active": True}, pl.col("active")),
    ({"active__ne": True}, ~pl.col("active")),
    ({"status": "open"}, pl.col("status") == "open"),
    # ordering, on every orderable type
    ({"id__lt": 3}, pl.col("id") < 3),
    ({"id__lte": 3}, pl.col("id") <= 3),
    ({"id__gt": 3}, pl.col("id") > 3),
    ({"id__gte": 3}, pl.col("id") >= 3),
    ({"score__gt": 1.5}, pl.col("score") > 1.5),
    ({"score__lte": 1}, pl.col("score") <= 1.0),
    ({"born__gte": dt.date(2022, 1, 1)}, pl.col("born") >= dt.date(2022, 1, 1)),
    ({"born__lt": dt.date(2021, 6, 15)}, pl.col("born") < dt.date(2021, 6, 15)),
    (
        {"seen__gt": dt.datetime(2024, 1, 2)},
        pl.col("seen") > dt.datetime(2024, 1, 2),
    ),
    (
        {"seen__lte": dt.datetime(2024, 1, 2, 11, 30, 5, 250000)},
        pl.col("seen") <= dt.datetime(2024, 1, 2, 11, 30, 5, 250000),
    ),
    ({"at__lt": dt.time(12, 30, 15)}, pl.col("at") < dt.time(12, 30, 15)),
    # in and range
    ({"id__in": [1, 4, 9]}, pl.col("id").is_in([1, 4, 9])),
    ({"id__in": (2,)}, pl.col("id").is_in([2])),
    ({"id__in": {2, 3}}, pl.col("id").is_in([2, 3])),
    ({"status__in": ["new", "done"]}, pl.col("status").is_in(["new", "done"])),
    ({"id__range": (2, 4)}, pl.col("id").is_between(2, 4)),
    ({"score__range": [1, 3.5]}, pl.col("score").is_between(1.0, 3.5)),
    (
        {"born__range": (dt.date(2021, 1, 1), dt.date(2023, 12, 31))},
        pl.col("born").is_between(dt.date(2021, 1, 1), dt.date(2023, 12, 31)),
    ),
    # text
    ({"name__contains": "ph"}, pl.col("name").str.contains("ph", literal=True)),
    ({"name__startswith": "a"}, pl.col("name").str.starts_with("a")),
    ({"name__endswith": "a"}, pl.col("name").str.ends_with("a")),
    # null handling
    ({"score__isnull": True}, pl.col("score").is_null()),
    ({"score__isnull": False}, pl.col("score").is_not_null()),
    ({"score": None}, pl.col("score").is_null()),
    ({"score__ne": None}, pl.col("score").is_not_null()),
    ({"seen__isnull": True}, pl.col("seen").is_null()),
    ({"tags__isnull": False}, pl.col("tags").is_not_null()),
    # several lookups are joined with AND
    (
        {"id__gte": 2, "active": False, "status": "open"},
        (pl.col("id") >= 2) & ~pl.col("active") & (pl.col("status") == "open"),
    ),
]


@pytest.mark.parametrize(("lookups", "expr"), PARITY)
def test_a_lookup_gives_the_same_rows_as_the_polars_filter(lookups, expr):
    from_lookup = _ids(Job.objects.where(**lookups))
    from_polars = _ids(Job.objects.filter(expr))
    assert from_lookup == from_polars


def test_the_parity_cases_cover_every_operator_and_type():
    used = {key.partition("__")[2] or "exact" for case, _ in PARITY for key in case}
    every = {op for ops in OPERATORS.values() for op in ops}
    assert used == every


def test_lookups_work_on_a_delta_table_too():
    assert _ids(JobDelta.objects.where(id__lte=2, active=True)) == [1]
    assert _ids(JobDelta.objects.where(name__contains="ph")) == [1]


# ---------- values are data, never SQL ----------


@pytest.mark.parametrize(
    ("lookups", "expected"),
    [
        ({"name": "x' OR '1'='1"}, []),
        ({"name": "x'; DROP TABLE jobs; --"}, []),
        ({"name": "it's :x"}, [5]),  # a quote and a colon, which look like SQL
        ({"name__contains": "it's"}, [5]),
        ({"name__contains": ":x"}, [5]),
        ({"name__contains": "%"}, [3]),  # a literal percent, not a wildcard
        ({"name__contains": "_"}, [4]),  # a literal underscore, not "any char"
        ({"name__startswith": "50%"}, [3]),
        ({"name__endswith": "_b"}, [4]),
        ({"name__contains": "!"}, []),
    ],
)
def test_values_are_quoted_not_interpreted(lookups, expected):
    assert _ids(Job.objects.where(**lookups)) == expected


def test_an_empty_in_matches_nothing():
    assert _ids(Job.objects.where(id__in=[])) == []


# ---------- sql= and combining ----------


def test_sql_keyword_and_positional_sql_still_work():
    assert _ids(Job.objects.where("id > 3")) == [4, 5]
    assert _ids(Job.objects.where(sql="id > 3")) == [4, 5]


def test_sql_and_lookups_are_joined_with_and():
    query = Job.objects.where(sql="id > 3 OR name = 'beta'", active=False)
    # Without the parentheses around the OR this would also return id 5.
    assert _ids(query) == [2, 4]


def test_where_with_lookups_combines_with_select_and_other_runners():
    query = Job.objects.where(id__gte=4).select("id", "name")
    assert query.all().columns == ["id", "name"]
    assert query.lazy().collect().height == 2
    assert [j.id for j in JobDelta.objects.where(id__lte=2).as_models()] == [1, 2]


def test_a_field_called_sql_is_reached_with_exact():
    assert _ids(Weird.objects.where(sql__exact="y")) == [2]
    # `sql=` is always the SQL text
    assert _ids(Weird.objects.where(sql="id = 1")) == [1]


def test_lookups_render_to_sql_without_a_connection():
    schema = Job.__smallcat_schema__
    assert lookups_to_sql(schema, {"id__lte": 2, "name": "a'b"}) == (
        "\"id\" <= 2 AND \"name\" = 'a''b'"
    )
    assert lookups_to_sql(schema, {"born__gt": dt.date(2020, 1, 1)}) == (
        "\"born\" > DATE '2020-01-01'"
    )


# ---------- errors are raised where the query is written ----------


@pytest.mark.parametrize(
    ("lookups", "message"),
    [
        ({"job_id": 1}, "not a field of Job"),
        ({"idd__lte": 1}, "did you mean 'id'"),
        ({"nme": "a"}, "did you mean 'name'"),
        ({"id__between": (1, 2)}, "not a lookup operator"),
        ({"name__lt": "a"}, "not available for 'name'"),
        ({"active__gt": True}, "not available for 'active'"),
        ({"tags__contains": "a"}, "not available for 'tags'"),
        ({"tags": ["a"]}, "cannot be compared"),
        ({"id": "1"}, "does not take '1'"),
        ({"id": True}, "does not take True"),
        ({"id": 1.5}, "does not take 1.5"),
        ({"score": "x"}, "does not take 'x'"),
        ({"score": float("nan")}, "does not take nan"),
        ({"name": 3}, "does not take 3"),
        ({"active": 1}, "does not take 1"),
        (
            {"born": dt.datetime(2020, 1, 1)},
            "does not take",
        ),  # a datetime is not a date
        ({"born": "2020-01-01"}, "does not take"),
        ({"seen": dt.datetime(2024, 1, 1, tzinfo=dt.UTC)}, "without time zone"),
        ({"at": dt.datetime(2024, 1, 1)}, "does not take"),
        ({"id__gte": dt.timedelta(hours=1)}, "does not take"),
        ({"id__in": 3}, "expects a list, tuple or set"),
        ({"id__in": [1, None]}, "None is not allowed"),
        ({"id__in": ["a"]}, "does not take 'a'"),
        ({"id__range": (1,)}, "exactly 2"),
        ({"id__range": 5}, "expects a pair"),
        ({"score__isnull": 1}, "does not take 1"),
        ({"name__contains": 3}, "does not take 3"),
    ],
)
def test_invalid_lookups_fail_when_the_query_is_built(lookups, message):
    with pytest.raises(ModelQueryError, match=message):
        Job.objects.where(**lookups)


def test_nothing_to_filter_by_is_an_error():
    with pytest.raises(ModelQueryError, match="SQL predicate"):
        Job.objects.where()


@pytest.mark.parametrize("sql", ["", "   ", 3])
def test_an_empty_or_wrong_sql_is_an_error(sql):
    with pytest.raises(ModelQueryError, match="non-empty SQL"):
        Job.objects.where(sql)
    with pytest.raises(ModelQueryError, match="non-empty SQL"):
        Job.objects.where(sql=sql, id=1)


def test_where_is_used_once_and_does_not_mix_with_filter():
    with pytest.raises(ModelQueryError, match="already used"):
        Job.objects.where(id=1).where(name="a")
    with pytest.raises(ModelQueryError, match="already used"):
        Job.objects.where("id = 1").where(name="a")
    with pytest.raises(ModelQueryError, match="only one of them"):
        Job.objects.filter(pl.col("id") > 1).where(id=1)
    with pytest.raises(ModelQueryError, match="only one of them"):
        Job.objects.where(id=1).filter(pl.col("id") > 1)


def test_an_unsafe_sql_is_still_refused_when_the_query_runs():
    query = Job.objects.where(sql="1=1; DROP TABLE x", id=1)
    with pytest.raises(ValueError, match="Unsafe or invalid WHERE"):
        query.all()


def test_a_duration_literal_is_a_duckdb_interval():
    # No format can hold a timedelta written by smallcat (see models/formats.py), so
    # there is no model to query: render the literal from a schema built by hand.
    import duckdb

    from smallcat.models.schema import FieldSpec, ModelSchema

    schema = ModelSchema(
        "Run",
        {"took": FieldSpec("took", pl.Duration("us"), nullable=False)},
        "forbid",
    )
    sql = lookups_to_sql(schema, {"took__gte": dt.timedelta(hours=1)})
    assert sql == '"took" >= to_microseconds(3600000000)'
    literal = sql.split(">= ")[1]
    assert duckdb.sql(f"SELECT {literal} = INTERVAL 1 HOUR").fetchall() == [(True,)]


def test_kinds_and_operators():
    assert kind_of(pl.Int32()) == "int"
    assert kind_of(pl.Float64()) == "float"
    assert kind_of(pl.Datetime("us")) == "datetime"
    assert kind_of(pl.Duration("us")) == "duration"
    assert kind_of(pl.List(pl.Int64())) == "other"
    assert OPERATORS["other"] == ("isnull",)
    assert "contains" not in OPERATORS["int"]
    assert "lt" not in OPERATORS["str"]
