"""SC-035 phase 3: models, registry, manager and validation."""

import datetime as dt
from typing import Literal

import polars as pl
import pytest
from pydantic import ConfigDict

import smallcat
from smallcat import Connections
from smallcat.connections_config import (
    ConnectionNotLoadedError,
    DuplicateConnectionError,
)
from smallcat.models import (
    Model,
    ModelFileFormat,
    ModelDefinitionError,
    ModelNotRegisteredError,
    ModelQueryError,
    ModelRegistrationError,
    ModelRegistry,
    ModelSchemaValidationError,
    ModelUnsupportedWriteModeError,
    ModelValidationMode,
    ModelWriteMode,
    ModelWriteNotAllowedError,
    current_registry,
    default_registry,
    use_registry,
)
from smallcat.models.schema import ModelSchema, dtype_of, validate_frame

OVERWRITE = ModelWriteMode.OVERWRITE
APPEND = ModelWriteMode.APPEND


# ---------- models under test ----------


class Person(Model):
    id: int
    name: str
    score: float | None = None

    class Meta:
        location = "people.parquet"
        file_format = "parquet"


class Event(Model):
    id: int
    kind: str

    class Meta:
        location = "events"
        file_format = "delta_table"


class Readings(Model):
    id: int
    value: float

    class Meta:
        location = "readings.csv"
        file_format = "csv"
        validation_mode = ModelValidationMode.COERCE
        load_options = {"all_varchar": True}


class Frozen(Model):
    id: int

    class Meta:
        location = "frozen.parquet"
        file_format = "parquet"
        allowed_write_modes = []


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
    for model in (Person, Event, Readings, Frozen):
        reg.register(model, connection="local")
    return reg


@pytest.fixture
def people_df() -> pl.DataFrame:
    return pl.DataFrame(
        {"id": [1, 2, 3], "name": ["a", "b", "c"], "score": [1.5, None, 3.5]},
    )


@pytest.fixture(autouse=True)
def _use(registry):
    with use_registry(registry):
        yield


# ---------- Meta ----------


def _define(**meta_attrs):
    """Define a model with a Meta built from `meta_attrs`."""
    meta = type("Meta", (), meta_attrs)
    meta.__qualname__ = "Bad.Meta"
    return type(Model)(
        "Bad",
        (Model,),
        {
            "__module__": __name__,
            "__qualname__": "Bad",
            "__annotations__": {"id": int},
            "Meta": meta,
        },
    )


def test_meta_defaults():
    meta = Person.__smallcat_meta__
    assert meta.location == "people.parquet"
    assert meta.file_format == "parquet"
    assert meta.allowed_write_modes == {APPEND, OVERWRITE, ModelWriteMode.MERGE}
    assert meta.validation_mode is ModelValidationMode.STRICT
    assert meta.load_options is None
    assert meta.save_options is None


def test_meta_allowed_write_modes_empty_means_read_only():
    assert Frozen.__smallcat_meta__.allowed_write_modes == frozenset()


def test_meta_accepts_strings_for_the_enums():
    class M(Model):
        id: int

        class Meta:
            location = "m"
            file_format = "csv"
            allowed_write_modes = ["overwrite"]
            validation_mode = "coerce"

    assert M.__smallcat_meta__.allowed_write_modes == {OVERWRITE}
    assert M.__smallcat_meta__.validation_mode is ModelValidationMode.COERCE


def test_file_format_is_a_model_file_format():
    assert Person.__smallcat_meta__.file_format is ModelFileFormat.PARQUET
    assert Event.__smallcat_meta__.file_format is ModelFileFormat.DELTA_TABLE
    assert Readings.__smallcat_meta__.file_format is ModelFileFormat.CSV


@pytest.mark.parametrize("member", list(ModelFileFormat))
def test_file_format_accepts_the_enum_and_its_string(member):
    for value in (member, member.value):
        model = _define(location="x", file_format=value)
        assert model.__smallcat_meta__.file_format is member


def test_file_format_enum_values():
    assert {f.value for f in ModelFileFormat} == {"csv", "excel", "parquet", "delta_table"}


def test_an_invalid_file_format_lists_the_valid_ones():
    with pytest.raises(ModelDefinitionError) as exc:
        _define(location="x", file_format="json")
    message = str(exc.value)
    assert "Meta.file_format: 'json' is not a valid file format" in message
    for value in ("csv", "excel", "parquet", "delta_table"):
        assert repr(value) in message


def test_a_file_format_that_is_not_a_string_is_rejected():
    with pytest.raises(ModelDefinitionError, match="file format"):
        _define(location="x", file_format=3)


def test_a_model_without_meta_is_abstract():
    class Base(Model):
        id: int

    assert Base.__smallcat_meta__ is None
    with pytest.raises(ModelRegistrationError, match="no Meta"):
        ModelRegistry().register(Base, connection="x")


def test_meta_is_inherited_and_the_schema_follows_the_new_fields():
    class Vip(Person):
        level: int

    assert Vip.__smallcat_meta__.location == "people.parquet"
    assert list(Vip.__smallcat_schema__.fields) == ["id", "name", "score", "level"]


@pytest.mark.parametrize(
    ("attrs", "expected"),
    [
        ({"file_format": "csv"}, "Meta.location is required"),
        ({"location": "x"}, "Meta.file_format is required"),
        ({"location": "x", "file_format": "json"}, "file_format"),
        ({"location": "", "file_format": "csv"}, "location"),
        ({"location": "x", "file_format": "csv", "allowd_modes": []}, "allowd_modes"),
        (
            {"location": "x", "file_format": "csv", "allowed_write_modes": ["upsert"]},
            "upsert",
        ),
        (
            {"location": "x", "file_format": "csv", "validation_mode": "lenient"},
            "lenient",
        ),
        (
            {"location": "x", "file_format": "csv", "load_options": {"hedaer": 1}},
            "Meta.load_options.hedaer",
        ),
        (
            {"location": "x", "file_format": "csv", "save_options": {"sepp": ","}},
            "Meta.save_options.sepp",
        ),
        (
            {"location": "x", "file_format": "delta_table", "save_options": {"mode": "append"}},
            "write mode is chosen on each save",
        ),
    ],
)
def test_invalid_meta_fails_when_the_class_is_defined(attrs, expected):
    with pytest.raises(ModelDefinitionError, match=expected):
        _define(**attrs)


def test_all_meta_problems_are_reported_together():
    with pytest.raises(ModelDefinitionError) as exc:
        _define(file_format="csv", typo=1)
    assert "typo" in str(exc.value)
    assert "Meta.location is required" in str(exc.value)


def test_unsupported_field_types_fail_at_definition():
    class Bad(Model):
        id: int

    with pytest.raises(ModelDefinitionError, match="payload"):

        class Worse(Model):
            payload: dict[str, int]

            class Meta:
                location = "x"
                file_format = "csv"

    assert Bad  # defined fine


# ---------- types ----------


@pytest.mark.parametrize(
    ("annotation", "dtype", "nullable"),
    [
        (int, pl.Int64, False),
        (float, pl.Float64, False),
        (str, pl.String, False),
        (bool, pl.Boolean, False),
        (bytes, pl.Binary, False),
        (dt.date, pl.Date, False),
        (dt.datetime, pl.Datetime("us"), False),
        (dt.time, pl.Time, False),
        (dt.timedelta, pl.Duration("us"), False),
        (int | None, pl.Int64, True),
        (Literal["a", "b"], pl.String, False),
        (Literal[1, 2], pl.Int64, False),
        (list[int], pl.List(pl.Int64), False),
        (list[str] | None, pl.List(pl.String), True),
    ],
)
def test_python_types_map_to_polars_types(annotation, dtype, nullable):
    assert dtype_of(annotation) == (dtype, nullable)


# ---------- validation of frames (unit) ----------


def _spec(**overrides) -> ModelSchema:
    class V(Model):
        id: int
        name: str
        score: float | None = None
        model_config = ConfigDict(**overrides)

        class Meta:
            location = "v"
            file_format = "csv"

    return V.__smallcat_schema__


STRICT = ModelValidationMode.STRICT
COERCE = ModelValidationMode.COERCE


def test_strict_accepts_any_width_of_the_same_kind():
    df = pl.DataFrame(
        {"id": pl.Series([1], dtype=pl.Int32), "name": ["a"], "score": [1.0]},
        schema_overrides={"score": pl.Float32},
    )
    out = validate_frame(df, _spec(), STRICT)
    assert out.schema["id"] == pl.Int32  # strict does not convert


def test_strict_rejects_a_different_kind():
    df = pl.DataFrame({"id": ["1"], "name": ["a"], "score": [1.0]})
    with pytest.raises(ModelSchemaValidationError) as exc:
        validate_frame(df, _spec(), STRICT)
    assert [(i.column, i.problem) for i in exc.value.issues] == [
        ("id", "expected Int64, got String"),
    ]


def test_missing_columns_are_errors_in_both_modes():
    df = pl.DataFrame({"id": [1]})
    for mode in (STRICT, COERCE):
        with pytest.raises(ModelSchemaValidationError) as exc:
            validate_frame(df, _spec(), mode)
        assert {i.column for i in exc.value.issues} == {"name", "score"}


def test_nulls_in_a_required_field_are_errors():
    df = pl.DataFrame({"id": [1, None], "name": ["a", "b"], "score": [None, None]})
    with pytest.raises(ModelSchemaValidationError) as exc:
        validate_frame(df, _spec(), STRICT)
    assert [i.column for i in exc.value.issues] == ["id"]
    assert "1 null" in exc.value.issues[0].problem


def test_an_all_null_column_of_an_optional_field_is_fine():
    df = pl.DataFrame({"id": [1], "name": ["a"], "score": [None]})
    assert validate_frame(df, _spec(), STRICT).height == 1


def test_unexpected_columns_are_errors_by_default():
    df = pl.DataFrame({"id": [1], "name": ["a"], "score": [1.0], "extra": [1]})
    with pytest.raises(ModelSchemaValidationError, match="unexpected column"):
        validate_frame(df, _spec(), STRICT)


def test_extra_ignore_drops_unexpected_columns():
    df = pl.DataFrame({"id": [1], "name": ["a"], "score": [1.0], "extra": [1]})
    out = validate_frame(df, _spec(extra="ignore"), STRICT)
    assert out.columns == ["id", "name", "score"]


def test_extra_allow_keeps_unexpected_columns():
    df = pl.DataFrame({"extra": [1], "id": [1], "name": ["a"], "score": [1.0]})
    out = validate_frame(df, _spec(extra="allow"), STRICT)
    assert out.columns == ["id", "name", "score", "extra"]


def test_columns_come_out_in_the_models_order():
    df = pl.DataFrame({"score": [1.0], "name": ["a"], "id": [1]})
    assert validate_frame(df, _spec(), STRICT).columns == ["id", "name", "score"]


def test_coerce_casts_to_the_declared_types():
    df = pl.DataFrame({"id": ["42"], "name": ["a"], "score": ["1.5"]})
    out = validate_frame(df, _spec(), COERCE)
    assert out.schema == {"id": pl.Int64, "name": pl.String, "score": pl.Float64}
    assert out["id"].to_list() == [42]


def test_coerce_turns_a_whole_float_into_an_integer():
    df = pl.DataFrame({"id": [42.0], "name": ["a"], "score": [1.0]})
    assert validate_frame(df, _spec(), COERCE)["id"].to_list() == [42]


def test_coerce_rejects_a_float_that_would_lose_decimals():
    df = pl.DataFrame({"id": [42.7], "name": ["a"], "score": [1.0]})
    with pytest.raises(ModelSchemaValidationError, match="lose their decimals"):
        validate_frame(df, _spec(), COERCE)


def test_coerce_rejects_values_that_cannot_be_cast_without_echoing_them():
    df = pl.DataFrame({"id": ["1", "secret-value"], "name": ["a", "b"], "score": [1.0, 2.0]})
    with pytest.raises(ModelSchemaValidationError) as exc:
        validate_frame(df, _spec(), COERCE)
    assert "1 value(s) cannot be cast" in str(exc.value)
    assert "secret-value" not in str(exc.value)


def test_every_issue_is_reported_at_once():
    df = pl.DataFrame({"id": ["x"], "other": [1]})
    with pytest.raises(ModelSchemaValidationError) as exc:
        validate_frame(df, _spec(), COERCE)
    assert {i.column for i in exc.value.issues} == {"id", "name", "score", "other"}


def test_a_selection_only_requires_those_columns():
    df = pl.DataFrame({"id": [1]})
    assert validate_frame(df, _spec(), STRICT, ["id"]).columns == ["id"]


# ---------- registry ----------


def test_register_needs_a_loaded_connection():
    reg = ModelRegistry()
    with pytest.raises(ConnectionNotLoadedError, match="'local' is not loaded"):
        reg.register(Person, connection="local")


def test_registering_twice_needs_replace(registry):
    with pytest.raises(ModelRegistrationError, match="already registered"):
        registry.register(Person, connection="local")
    registry.register(Person, connection="local", replace=True)


def test_an_unregistered_model_says_how_to_register_it():
    reg = ModelRegistry()
    with use_registry(reg), pytest.raises(ModelNotRegisteredError, match="register"):
        Person.objects.all()


def test_registering_can_restrict_the_write_modes(registry):
    registration = registry.register(
        Person,
        connection="local",
        allowed_write_modes=["overwrite"],
        replace=True,
    )
    assert registration.allowed_write_modes == {OVERWRITE}


def test_registering_can_make_a_model_read_only(registry, people_df):
    registry.register(Person, connection="local", allowed_write_modes=[], replace=True)
    with pytest.raises(ModelWriteNotAllowedError, match="read-only"):
        Person.objects.save(people_df, mode=OVERWRITE)


def test_registering_cannot_widen_what_meta_allows(registry):
    with pytest.raises(ModelRegistrationError, match="can only restrict"):
        registry.register(
            Frozen,
            connection="local",
            allowed_write_modes=[OVERWRITE],
            replace=True,
        )


def test_registering_rejects_unknown_modes(registry):
    with pytest.raises(ValueError, match="upsert"):
        registry.register(
            Person,
            connection="local",
            allowed_write_modes=["upsert"],
            replace=True,
        )


def test_connections_cannot_be_loaded_twice_under_the_same_name(registry):
    again = Connections.from_dict({"connections": {"local": {"conn_type": "fs"}}})
    with pytest.raises(DuplicateConnectionError):
        registry.load_connections(again)


def test_use_registry_restores_the_previous_one(registry):
    other = ModelRegistry()
    assert current_registry() is registry
    with use_registry(other):
        assert current_registry() is other
    assert current_registry() is registry


def test_use_registry_restores_even_if_the_block_raises(registry):
    with pytest.raises(RuntimeError), use_registry(ModelRegistry()):
        raise RuntimeError
    assert current_registry() is registry


def test_registries_are_independent(registry, people_df, tmp_path):
    other = ModelRegistry()
    other_dir = tmp_path / "other"
    other.load_connections(
        Connections.from_dict(
            {"connections": {"local": {"conn_type": "fs", "extra": {"base_path": str(other_dir)}}}},
        ),
    )
    other.register(Person, connection="local")
    Person.objects.save(people_df, mode=OVERWRITE)
    with use_registry(other):
        Person.objects.save(people_df.head(1), mode=OVERWRITE)
        assert Person.objects.all().height == 1
    assert Person.objects.all().height == 3


def test_the_default_registry_is_exported_lazily():
    assert smallcat.registry is default_registry
    assert smallcat.Model is Model


# ---------- reading ----------


@pytest.fixture
def saved(people_df):
    Person.objects.save(people_df, mode=OVERWRITE)
    return people_df


def test_all(saved):
    out = Person.objects.all()
    assert out.sort("id").equals(saved)


def test_filter_with_polars_expressions(saved):
    out = Person.objects.filter(pl.col("id") > 1).all()
    assert sorted(out["id"].to_list()) == [2, 3]


def test_several_filters_are_combined_with_and(saved):
    out = Person.objects.filter(pl.col("id") > 1).filter(pl.col("id") < 3).all()
    assert out["id"].to_list() == [2]


def test_select_reads_only_those_fields(saved):
    out = Person.objects.select("name", "id").all()
    assert out.columns == ["name", "id"]


def test_select_and_filter_together(saved):
    out = Person.objects.filter(pl.col("id") == 1).select("name").all()
    assert out.to_dicts() == [{"name": "a"}]


def test_lazy_returns_a_lazyframe_that_can_be_chained(saved):
    lf = Person.objects.filter(pl.col("id") > 1).lazy()
    assert isinstance(lf, pl.LazyFrame)
    assert lf.select(pl.col("id").sum()).collect().item() == 5


def test_where_filters_with_sql(saved):
    out = Person.objects.where("id >= 2 AND name <> 'c'").all()
    assert out["id"].to_list() == [2]


def test_where_with_select(saved):
    out = Person.objects.where("id = 1").select("name").all()
    assert out.to_dicts() == [{"name": "a"}]


def test_lazy_after_where_runs_the_sql_and_wraps_the_result(saved):
    lf = Person.objects.where("id = 1").lazy()
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().height == 1


def test_as_models(saved):
    people = Person.objects.filter(pl.col("id") <= 2).as_models()
    assert sorted(p.id for p in people) == [1, 2]
    assert all(isinstance(p, Person) for p in people)


def test_as_models_with_select_is_an_error(saved):
    with pytest.raises(ModelQueryError, match="every field"):
        Person.objects.select("id").as_models()


def test_filter_then_where_is_an_error():
    with pytest.raises(ModelQueryError, match="only one of them"):
        Person.objects.filter(pl.col("id") > 1).where("id < 5")


def test_where_then_filter_is_an_error():
    with pytest.raises(ModelQueryError, match="only one of them"):
        Person.objects.where("id < 5").filter(pl.col("id") > 1)


def test_where_can_only_be_used_once():
    with pytest.raises(ModelQueryError, match="AND"):
        Person.objects.where("id < 5").where("id > 1")


def test_select_can_be_combined_with_either_filter():
    Person.objects.select("id").filter(pl.col("id") > 1)
    Person.objects.select("id").where("id > 1")
    Person.objects.filter(pl.col("id") > 1).select("id")
    Person.objects.where("id > 1").select("id")


def test_filter_rejects_a_string():
    with pytest.raises(ModelQueryError, match="use where"):
        Person.objects.filter("id > 1")


def test_filter_needs_an_expression():
    with pytest.raises(ModelQueryError, match="at least one"):
        Person.objects.filter()


def test_where_rejects_empty_and_non_strings():
    for bad in ("", "  ", pl.col("id") > 1):
        with pytest.raises(ModelQueryError):
            Person.objects.where(bad)


def test_select_validates_the_names():
    with pytest.raises(ModelQueryError, match="nope"):
        Person.objects.select("nope")
    with pytest.raises(ModelQueryError, match="repeated"):
        Person.objects.select("id", "id")
    with pytest.raises(ModelQueryError, match="already used"):
        Person.objects.select("id").select("name")
    with pytest.raises(ModelQueryError, match="at least one"):
        Person.objects.select()


def test_queries_are_immutable():
    base = Person.objects.filter(pl.col("id") > 1)
    base.select("id")
    assert base.columns is None
    other = Person.objects.where("id > 1")
    assert other.filters == ()
    assert base.where_sql is None


def test_objects_is_not_available_on_instances():
    with pytest.raises(AttributeError, match="only available on the model class"):
        Person(id=1, name="a").objects  # noqa: B018


def test_reading_validates_types_in_strict_mode(registry):
    pl.DataFrame({"id": ["1"], "name": ["a"], "score": [1.0]}).write_parquet(
        registry.connections.get("local")["extra"]["base_path"] + "/people.parquet",
    )
    with pytest.raises(ModelSchemaValidationError, match="expected Int64, got String"):
        Person.objects.all()
    with pytest.raises(ModelSchemaValidationError):
        Person.objects.lazy()


def test_reading_validates_nulls_when_collected(registry):
    pl.DataFrame({"id": [1, None], "name": ["a", "b"], "score": [1.0, 2.0]}).write_parquet(
        registry.connections.get("local")["extra"]["base_path"] + "/people.parquet",
    )
    with pytest.raises(ModelSchemaValidationError, match="null"):
        Person.objects.all()
    # nulls only matter for the rows returned
    assert Person.objects.filter(pl.col("id").is_not_null()).all().height == 1


def test_reading_reports_a_missing_column(registry):
    pl.DataFrame({"id": [1]}).write_parquet(
        registry.connections.get("local")["extra"]["base_path"] + "/people.parquet",
    )
    with pytest.raises(ModelSchemaValidationError) as exc:
        Person.objects.all()
    assert {i.column for i in exc.value.issues} == {"name", "score"}


def test_coerce_reads_strings_as_declared_types(registry):
    path = registry.connections.get("local")["extra"]["base_path"] + "/readings.csv"
    with open(path, "w") as f:  # noqa: PTH123
        f.write("id,value\n1,2.5\n2,3.5\n")
    out = Readings.objects.all()
    assert out.schema == {"id": pl.Int64, "value": pl.Float64}
    # the filter sees the coerced types
    assert Readings.objects.filter(pl.col("id") > 1).all()["id"].to_list() == [2]


def test_coerce_reports_the_column_that_cannot_be_cast(registry):
    path = registry.connections.get("local")["extra"]["base_path"] + "/readings.csv"
    with open(path, "w") as f:  # noqa: PTH123
        f.write("id,value\n1,2.5\nbad,3.5\n")
    with pytest.raises(ModelSchemaValidationError) as exc:
        Readings.objects.all()
    assert [i.column for i in exc.value.issues] == ["id"]
    assert "bad" not in str(exc.value)


# ---------- writing ----------


def test_save_a_dataframe_and_read_it_back(people_df):
    Person.objects.save(people_df, mode=OVERWRITE)
    assert Person.objects.all().sort("id").equals(people_df)


def test_save_accepts_the_mode_as_a_string(people_df):
    Person.objects.save(people_df, mode="overwrite")
    assert Person.objects.all().height == 3


def test_save_model_instances():
    Person.objects.save([Person(id=1, name="a"), Person(id=2, name="b", score=2.0)], mode=OVERWRITE)
    assert Person.objects.all().sort("id").to_dicts() == [
        {"id": 1, "name": "a", "score": None},
        {"id": 2, "name": "b", "score": 2.0},
    ]


def test_save_a_single_instance():
    Person.objects.save(Person(id=7, name="z"), mode=OVERWRITE)
    assert Person.objects.all()["id"].to_list() == [7]


def test_overwrite_replaces_everything(people_df):
    Person.objects.save(people_df, mode=OVERWRITE)
    Person.objects.save(people_df.head(1), mode=OVERWRITE)
    assert Person.objects.all().height == 1


def test_mode_is_required(people_df):
    with pytest.raises(TypeError, match="mode"):
        Person.objects.save(people_df)


def test_mode_must_be_a_write_mode(people_df):
    with pytest.raises(ValueError, match="'upsert' is not a valid write mode"):
        Person.objects.save(people_df, mode="upsert")


def test_files_cannot_append(people_df):
    with pytest.raises(ModelUnsupportedWriteModeError, match="cannot be appended"):
        Person.objects.save(people_df, mode=APPEND)


def test_a_read_only_model_cannot_be_written():
    with pytest.raises(ModelWriteNotAllowedError, match="read-only"):
        Frozen.objects.save(pl.DataFrame({"id": [1]}), mode=OVERWRITE)


def test_a_mode_not_in_allowed_write_modes_is_refused(people_df):
    class OnlyAppend(Model):
        id: int
        kind: str

        class Meta:
            location = "only_append"
            file_format = "delta_table"
            allowed_write_modes = [ModelWriteMode.APPEND]

    current_registry().register(OnlyAppend, connection="local")
    df = pl.DataFrame({"id": [1], "kind": ["x"]})
    OnlyAppend.objects.save(df, mode=APPEND)
    with pytest.raises(ModelWriteNotAllowedError, match=r"does not allow 'overwrite'"):
        OnlyAppend.objects.save(df, mode=OVERWRITE)


def test_a_refused_save_writes_nothing(registry, people_df):
    with pytest.raises(ModelUnsupportedWriteModeError):
        Person.objects.save(people_df, mode=APPEND)
    base = registry.connections.get("local")["extra"]["base_path"]
    assert not (pytest.importorskip("pathlib").Path(base) / "people.parquet").exists()


def test_invalid_data_is_not_written(registry):
    bad = pl.DataFrame({"id": ["x"], "name": ["a"], "score": [1.0]})
    with pytest.raises(ModelSchemaValidationError):
        Person.objects.save(bad, mode=OVERWRITE)
    base = registry.connections.get("local")["extra"]["base_path"]
    assert not (pytest.importorskip("pathlib").Path(base) / "people.parquet").exists()


def test_delta_append_adds_rows():
    Event.objects.save(pl.DataFrame({"id": [1], "kind": ["a"]}), mode=APPEND)
    Event.objects.save(pl.DataFrame({"id": [2], "kind": ["b"]}), mode=APPEND)
    assert sorted(Event.objects.all()["id"].to_list()) == [1, 2]


def test_delta_overwrite_replaces_the_table():
    Event.objects.save(pl.DataFrame({"id": [1, 2], "kind": ["a", "b"]}), mode=APPEND)
    Event.objects.save(pl.DataFrame({"id": [9], "kind": ["z"]}), mode=OVERWRITE)
    assert Event.objects.all()["id"].to_list() == [9]


def test_delta_first_write_with_overwrite_creates_the_table():
    Event.objects.save(pl.DataFrame({"id": [1], "kind": ["a"]}), mode=OVERWRITE)
    assert Event.objects.all().height == 1


def test_a_partitioned_parquet_dataset_cannot_be_overwritten(registry):
    class Partitioned(Model):
        id: int
        region: str

        class Meta:
            location = "partitioned"
            file_format = "parquet"
            save_options = {"partition_by": ["region"]}

    registry.register(Partitioned, connection="local")
    df = pl.DataFrame({"id": [1], "region": ["x"]})
    with pytest.raises(ModelUnsupportedWriteModeError, match="only adds files"):
        Partitioned.objects.save(df, mode=OVERWRITE)


def test_csv_with_coerce_validates_before_writing(registry):
    df = pl.DataFrame({"id": ["1", "2"], "value": ["1.5", "2.5"]})
    Readings.objects.save(df, mode=OVERWRITE)
    assert Readings.objects.all().sort("id")["value"].to_list() == [1.5, 2.5]


def test_save_rejects_a_lazyframe(people_df):
    with pytest.raises(TypeError, match="collect"):
        Person.objects.save(people_df.lazy(), mode=OVERWRITE)


def test_save_rejects_other_types_and_foreign_instances():
    with pytest.raises(TypeError, match="not int"):
        Person.objects.save(1, mode=OVERWRITE)
    with pytest.raises(TypeError, match="expected Person instances"):
        Person.objects.save([Event(id=1, kind="a")], mode=OVERWRITE)


def test_saving_an_empty_list_writes_an_empty_dataset():
    Person.objects.save([Person(id=1, name="a")], mode=OVERWRITE)
    Person.objects.save([], mode=OVERWRITE)
    assert Person.objects.all().height == 0
