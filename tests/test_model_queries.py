"""SC-065: exclude(), first(), last() and count()."""

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


class QParquet(Model):
    id: int
    name: str
    score: float | None = None

    class Meta:
        location = "q.parquet"
        file_format = "parquet"


class QCsv(Model):
    id: int
    name: str
    score: float | None = None

    class Meta:
        location = "q.csv"
        file_format = "csv"


class QDelta(Model):
    id: int
    name: str
    score: float | None = None

    class Meta:
        location = "q_delta"
        file_format = "delta_table"


MODELS = (QParquet, QCsv, QDelta)
DATA = pl.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "name": ["a", "b", "c", "d"],
        "score": [1.5, None, 3.5, 4.5],
    },
)


@pytest.fixture(autouse=True)
def _use(tmp_path):
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
    for model in MODELS:
        reg.register(model, connection="local")
    with use_registry(reg):
        for model in MODELS:
            model.objects.save(DATA, mode=ModelWriteMode.OVERWRITE)
        yield


@pytest.fixture(params=MODELS, ids=lambda m: m.__name__)
def model(request):
    return request.param


# ---------- exclude ----------


def test_exclude_is_the_opposite_of_where(model):
    kept = model.objects.where(id__gt=2).all()["id"].to_list()
    dropped = model.objects.exclude(id__gt=2).all()["id"].to_list()
    assert sorted(kept + dropped) == [1, 2, 3, 4]
    assert sorted(dropped) == [1, 2]


def test_exclude_with_sql(model):
    assert model.objects.exclude("id = 1 OR id = 4").all()["id"].to_list() == [2, 3]


def test_exclude_several_lookups_drops_only_rows_matching_all(model):
    ids = model.objects.exclude(id__lt=3, name="a").all()["id"].to_list()
    assert sorted(ids) == [2, 3, 4]


def test_exclude_sql_and_lookups_in_one_call(model):
    ids = model.objects.exclude("id > 1", name="b").all()["id"].to_list()
    assert sorted(ids) == [1, 3, 4]


def test_exclude_keeps_rows_where_the_condition_is_null(model):
    ids = model.objects.exclude(score__gt=2).all()["id"].to_list()
    assert sorted(ids) == [1, 2]


def test_exclude_combines_with_where_in_any_order(model):
    a = model.objects.where(id__gt=1).exclude(name="c").all()["id"].to_list()
    b = model.objects.exclude(name="c").where(id__gt=1).all()["id"].to_list()
    assert sorted(a) == sorted(b) == [2, 4]


def test_exclude_can_be_chained(model):
    ids = model.objects.exclude(id=1).exclude(id=2).all()["id"].to_list()
    assert sorted(ids) == [3, 4]


def test_exclude_with_select_and_models(model):
    query = model.objects.exclude(id__lt=4)
    assert query.select("id").all().columns == ["id"]
    assert [m.id for m in query.all(as_models=True)] == [4]


def test_exclude_does_not_change_the_original_query():
    base = QParquet.objects.where(id__gt=1)
    base.exclude(id=2)
    assert base.exclude_sql == ()


def test_exclude_lazy():
    frame = QParquet.objects.exclude(id=1).lazy().collect()
    assert sorted(frame["id"].to_list()) == [2, 3, 4]


def test_exclude_requires_a_condition():
    with pytest.raises(ModelQueryError, match="exclude"):
        QParquet.objects.exclude()


def test_exclude_rejects_an_empty_predicate():
    with pytest.raises(ModelQueryError, match="non-empty"):
        QParquet.objects.exclude("  ")


def test_exclude_rejects_an_invalid_lookup():
    with pytest.raises(ModelQueryError):
        QParquet.objects.exclude(nope=1)


def test_exclude_cannot_be_mixed_with_filter():
    with pytest.raises(ModelQueryError, match="only one"):
        QParquet.objects.filter(pl.col("id") > 1).exclude(id=2)
    with pytest.raises(ModelQueryError, match="only one"):
        QParquet.objects.exclude(id=2).filter(pl.col("id") > 1)


# ---------- first / last ----------


def test_first_and_last_return_one_row_frames(model):
    first = model.objects.first()
    last = model.objects.last()
    assert isinstance(first, pl.DataFrame)
    assert first.height == last.height == 1
    assert first["id"].to_list() == [1]
    assert last["id"].to_list() == [4]


def test_first_and_last_as_models(model):
    first = model.objects.first(as_models=True)
    last = model.objects.last(as_models=True)
    assert isinstance(first, model)
    assert (first.id, first.name) == (1, "a")
    assert (last.id, last.name) == (4, "d")


def test_first_and_last_respect_the_query(model):
    query = model.objects.where(id__gt=1).exclude(id=4)
    assert query.first(as_models=True).id == 2
    assert query.last(as_models=True).id == 3


def test_first_and_last_with_polars_filter():
    query = QParquet.objects.filter(pl.col("id") < 4)
    assert query.first()["id"].to_list() == [1]
    assert query.last()["id"].to_list() == [3]


def test_first_and_last_return_none_when_nothing_matches(model):
    query = model.objects.where(id__gt=100)
    assert query.first() is None
    assert query.last() is None
    assert query.first(as_models=True) is None
    assert query.last(as_models=True) is None


def test_first_with_select_returns_those_columns():
    assert QParquet.objects.select("name").first().columns == ["name"]
    assert QParquet.objects.select("name").last().columns == ["name"]


def test_first_and_last_as_models_reject_select():
    with pytest.raises(ModelQueryError, match="select"):
        QParquet.objects.select("id").first(as_models=True)
    with pytest.raises(ModelQueryError, match="select"):
        QParquet.objects.select("id").last(as_models=True)


def test_query_first_and_last():
    query = QParquet.objects.where(name__in=["b", "c"])
    assert query.first(as_models=True).name == "b"
    assert query.last(as_models=True).name == "c"


# ---------- count ----------


def test_count_everything(model):
    assert model.objects.count() == 4


def test_count_with_where_and_exclude(model):
    assert model.objects.where(id__gt=1).count() == 3
    assert model.objects.exclude(id__gt=1).count() == 1
    assert model.objects.where(id__gt=1).exclude(name="c").count() == 2


def test_count_with_polars_filter():
    assert QParquet.objects.filter(pl.col("id") > 2).count() == 2


def test_count_zero(model):
    assert model.objects.where(id__gt=100).count() == 0


def test_count_ignores_select():
    assert QParquet.objects.select("id").where(id__gt=1).count() == 3
    assert QParquet.objects.select("id").filter(pl.col("id") > 1).count() == 3


def test_count_returns_an_int():
    assert type(QParquet.objects.count()) is int
