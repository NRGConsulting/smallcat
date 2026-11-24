"""SC-061: a NaN given to an `X | None` field is stored as None."""

import math

import pytest

from smallcat.models import Model

NAN = float("nan")


class Row(Model):
    id: int
    name: str | None = None
    age: int | None = None
    score: float | None = None
    ratio: float = 0.0


class KeepNaN(Row):
    nan_as_none = False


class Child(Row):
    extra: str | None = None


def test_nan_becomes_none_in_nullable_fields() -> None:
    row = Row(id=1, name=NAN, age=NAN, score=NAN)  # type: ignore[arg-type]
    assert row.name is None
    assert row.age is None
    assert row.score is None


def test_values_are_untouched() -> None:
    row = Row(id=1, name="Ada", age=36, score=1.5)
    assert (row.name, row.age, row.score) == ("Ada", 36, 1.5)


def test_non_nullable_float_keeps_nan() -> None:
    assert math.isnan(Row(id=1, ratio=NAN).ratio)


def test_non_nullable_field_is_still_validated() -> None:
    with pytest.raises(ValueError):  # noqa: PT011
        Row(id=NAN)  # type: ignore[arg-type]


def test_opt_out_keeps_nan() -> None:
    assert math.isnan(KeepNaN(id=1, score=NAN).score)  # type: ignore[arg-type]


def test_subclass_inherits() -> None:
    assert Child(id=1, extra=NAN).extra is None  # type: ignore[arg-type]
    assert Child.nan_as_none is True


def test_model_validate_and_input_not_mutated() -> None:
    data = {"id": 1, "score": NAN}
    assert Row.model_validate(data).score is None
    assert math.isnan(data["score"])


class Stored(Model):
    id: int
    score: float | None = None

    class Meta:
        location = "x.parquet"
        file_format = "parquet"


def test_polars_nan_row_becomes_none_as_models() -> None:
    """`all(as_models=True)` builds each row with `model_validate`."""
    import polars as pl

    frame = pl.DataFrame({"id": [1], "score": [NAN]})
    (row,) = frame.iter_rows(named=True)
    assert math.isnan(row["score"])  # the DataFrame keeps the NaN...
    assert Stored.model_validate(row).score is None  # ...the instance does not


def test_save_of_instances_writes_null() -> None:
    from smallcat.models.query import _to_frame

    frame = _to_frame(Stored, [Stored(id=1, score=NAN)])  # type: ignore[arg-type]
    assert frame["score"].null_count() == 1
