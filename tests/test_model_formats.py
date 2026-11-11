"""Field types a file format cannot hold are refused when the model is defined."""

import datetime as dt
from typing import ClassVar

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import write_deltalake

from smallcat import Connections
from smallcat.models import (
    Model,
    ModelDefinitionError,
    ModelRegistry,
    ModelWriteMode,
    use_registry,
)
from smallcat.models.formats import UNSUPPORTED

TIME, TIMEDELTA = dt.time, dt.timedelta
FORMATS = {
    "parquet": "x.parquet",
    "delta_table": "x_delta",
    "csv": "x.csv",
    "excel": "x.xlsx",
}


def _define(annotation, file_format, **meta):
    attrs = {"location": FORMATS[file_format], "file_format": file_format, **meta}
    meta_cls = type("Meta", (), attrs)

    class M(Model):
        id: int
        x: annotation  # type: ignore[valid-type]

        Meta: ClassVar[type] = meta_cls

    return M


# what was found by writing and reading each type in each format
HOLDS = {
    (TIME, "parquet"): True,
    (TIME, "csv"): True,
    (TIME, "delta_table"): False,
    (TIME, "excel"): False,
    (TIMEDELTA, "parquet"): False,
    (TIMEDELTA, "delta_table"): False,
    (TIMEDELTA, "csv"): False,
    (TIMEDELTA, "excel"): False,
}


@pytest.mark.parametrize(("case", "holds"), HOLDS.items())
def test_each_type_in_each_format(case, holds):
    annotation, file_format = case
    if holds:
        assert _define(annotation, file_format).__smallcat_schema__ is not None
    else:
        with pytest.raises(ModelDefinitionError, match="cannot be stored in"):
            _define(annotation, file_format)


def test_a_list_of_an_unsupported_type_is_refused_too():
    with pytest.raises(ModelDefinitionError, match="a list of time"):
        _define(list[TIME], "delta_table")
    with pytest.raises(ModelDefinitionError, match="a list of timedelta"):
        _define(list[TIMEDELTA], "parquet")
    assert _define(list[TIME], "parquet")  # Parquet holds a list of times


def test_an_optional_field_is_checked_as_well():
    with pytest.raises(ModelDefinitionError, match="M.x is a timedelta"):
        _define(TIMEDELTA | None, "csv")


def test_the_error_says_what_to_do_instead():
    with pytest.raises(ModelDefinitionError) as error:
        _define(TIMEDELTA, "delta_table")
    text = str(error.value)
    assert "M has fields its file format cannot hold" in text
    assert "Delta Lake has no duration type" in text
    assert "seconds as a `float`" in text
    with pytest.raises(ModelDefinitionError, match="as a `str` such as '08:30:00'"):
        _define(TIME, "excel")


def test_every_field_that_cannot_be_held_is_reported():
    meta = type(
        "Meta",
        (),
        {"location": "x_delta", "file_format": "delta_table"},
    )

    with pytest.raises(ModelDefinitionError) as error:

        class Many(Model):
            id: int
            at: TIME
            took: TIMEDELTA

            Meta: ClassVar[type] = meta

    assert "Many.at is a time" in str(error.value)
    assert "Many.took is a timedelta" in str(error.value)


def test_the_other_types_are_not_affected():
    for annotation in (int, str, float, bool, dt.date, dt.datetime, list[int]):
        for file_format in ("parquet", "delta_table"):
            assert _define(annotation, file_format)


# ---------- the one exception: Parquet written elsewhere, read only ----------


def test_a_read_only_model_may_declare_a_timedelta_over_a_parquet_file(tmp_path):
    pq.write_table(
        pa.table(
            {"id": [1], "x": pa.array([dt.timedelta(hours=1)], pa.duration("us"))},
        ),
        tmp_path / "x.parquet",
    )
    model = _define(TIMEDELTA, "parquet", allowed_write_modes=[])
    registry = ModelRegistry()
    registry.load_connections(
        Connections.from_dict(
            {
                "connections": {
                    "local": {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}},
                },
            },
        ),
    )
    registry.register(model, connection="local")
    with use_registry(registry):
        assert model.objects.all()["x"].to_list() == [dt.timedelta(hours=1)]
        assert model.objects.filter(pl.col("id") == 1).all().height == 1


def test_the_read_only_exception_is_only_for_parquet():
    for file_format in ("delta_table", "csv", "excel"):
        with pytest.raises(ModelDefinitionError, match="cannot be stored in"):
            _define(TIMEDELTA, file_format, allowed_write_modes=[])
    with pytest.raises(ModelDefinitionError, match="cannot be stored in"):
        _define(TIME, "delta_table", allowed_write_modes=[])


def test_a_writable_parquet_model_is_told_about_read_only():
    with pytest.raises(ModelDefinitionError, match="allowed_write_modes = \[\]"):
        _define(TIMEDELTA, "parquet")


# ---------- the types that are held really do round-trip ----------


@pytest.mark.parametrize("file_format", ["parquet", "csv"])
def test_a_time_really_round_trips_where_it_is_allowed(tmp_path, file_format):
    model = _define(TIME, file_format)
    registry = ModelRegistry()
    registry.load_connections(
        Connections.from_dict(
            {
                "connections": {
                    "local": {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}},
                },
            },
        ),
    )
    registry.register(model, connection="local")
    value = dt.time(8, 30, 15)
    with use_registry(registry):
        model.objects.save(
            pl.DataFrame({"id": [1], "x": [value]}),
            mode=ModelWriteMode.OVERWRITE,
        )
        assert model.objects.all()["x"].to_list() == [value]


# ---------- the matrix still matches what the libraries do ----------


@pytest.mark.parametrize(
    "column",
    [
        pa.array([dt.time(1, 2, 3)], pa.time64("ns")),
        pa.array([dt.timedelta(hours=1)], pa.duration("us")),
    ],
)
def test_delta_lake_really_has_no_time_or_duration_type(tmp_path, column):
    # If a deltalake release ever accepts these, the matrix should be revisited.
    with pytest.raises(Exception, match="Invalid data type for Delta Lake"):
        write_deltalake(str(tmp_path / "t"), pa.table({"x": column}))


def test_the_table_names_every_format_it_refuses():
    assert set(UNSUPPORTED["duration"]) == {"parquet", "delta_table", "csv", "excel"}
    assert set(UNSUPPORTED["time"]) == {"delta_table", "excel"}
