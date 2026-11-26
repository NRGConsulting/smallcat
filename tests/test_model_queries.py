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


# ---------- order_by ----------


def test_order_by_ascending_and_descending(model):
    assert model.objects.order_by("-id").all()["id"].to_list() == [4, 3, 2, 1]
    assert model.objects.order_by("name").all()["name"].to_list() == list("abcd")


def test_order_by_nulls_last_ascending_first_descending(model):
    asc = model.objects.order_by("score").all()["id"].to_list()
    desc = model.objects.order_by("-score").all()["id"].to_list()
    assert asc == [1, 3, 4, 2]
    assert desc == [2, 4, 3, 1]


def test_order_by_several_fields_and_ties():
    frame = QParquet.objects.where(id__gt=0).order_by("-score", "id").all()
    assert frame["id"].to_list() == [2, 4, 3, 1]


def test_order_by_after_and_before_where(model):
    a = model.objects.where(id__gt=1).order_by("-id").all()["id"].to_list()
    b = model.objects.order_by("-id").where(id__gt=1).all()["id"].to_list()
    assert a == b == [4, 3, 2]


def test_order_by_with_exclude_and_models(model):
    rows = model.objects.exclude(id=2).order_by("-id").all(as_models=True)
    assert [m.id for m in rows] == [4, 3, 1]


def test_order_by_with_polars_filter():
    query = QParquet.objects.filter(pl.col("id") > 1).order_by("-id")
    assert query.all()["id"].to_list() == [4, 3, 2]
    assert query.lazy().collect()["id"].to_list() == [4, 3, 2]


def test_order_by_with_select_by_a_field_not_selected(model):
    frame = model.objects.select("name").order_by("-id").all()
    assert frame.columns == ["name"]
    assert frame["name"].to_list() == ["d", "c", "b", "a"]
    sql = model.objects.where(id__gt=0).select("name").order_by("-id").all()
    assert sql.columns == ["name"]
    assert sql["name"].to_list() == ["d", "c", "b", "a"]


def test_order_by_keeps_the_select_column_order():
    frame = QParquet.objects.select("name", "id").order_by("-id").all()
    assert frame.columns == QParquet.objects.select("name", "id").all().columns == ["name", "id"]


def test_order_by_first_and_last(model):
    assert model.objects.order_by("-id").first(as_models=True).id == 4
    assert model.objects.order_by("-id").last(as_models=True).id == 1
    query = model.objects.where(id__lt=4).order_by("-score")
    assert query.first(as_models=True).id == 2
    assert query.last(as_models=True).id == 1


def test_order_by_first_with_polars_filter():
    query = QParquet.objects.filter(pl.col("id") < 4).order_by("-id")
    assert query.first()["id"].to_list() == [3]
    assert query.last()["id"].to_list() == [1]


def test_order_by_does_not_change_count(model):
    assert model.objects.order_by("-id").count() == 4
    assert model.objects.where(id__gt=1).order_by("-id").count() == 3


def test_order_by_sorts_after_casting_in_coerce_mode(tmp_path):
    class Coerced(Model):
        id: int

        class Meta:
            location = "coerced.csv"
            file_format = "csv"
            validation_mode = "coerce"
            load_options = {"all_varchar": True}

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
    reg.register(Coerced, connection="local")
    (tmp_path / "coerced.csv").write_text("id\n9\n10\n2\n")
    with use_registry(reg):
        assert Coerced.objects.order_by("id").all()["id"].to_list() == [2, 9, 10]
        sql = Coerced.objects.where("id <> '0'").order_by("-id").all()
        assert sql["id"].to_list() == [10, 9, 2]


def test_order_by_does_not_change_the_original_query():
    base = QParquet.objects.where(id__gt=1)
    base.order_by("id")
    assert base.ordering is None


def test_order_by_requires_a_field():
    with pytest.raises(ModelQueryError, match="at least one"):
        QParquet.objects.order_by()


def test_order_by_rejects_an_unknown_field_and_suggests():
    with pytest.raises(ModelQueryError, match="did you mean 'name'"):
        QParquet.objects.order_by("-nme")


def test_order_by_rejects_a_repeated_field():
    with pytest.raises(ModelQueryError, match="repeated"):
        QParquet.objects.order_by("id", "-id")


def test_order_by_can_only_be_used_once():
    with pytest.raises(ModelQueryError, match="already used"):
        QParquet.objects.order_by("id").order_by("name")
