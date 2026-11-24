"""`Model.objects.create_table()`: create the empty dataset if it is missing."""

import pathlib
import warnings
from typing import ClassVar

import polars as pl
import pytest

from smallcat import Connections
from smallcat.models import Model, ModelRegistry, use_registry

FORMATS = {
    "delta_table": "silver/jobs",
    "parquet": "silver/jobs.parquet",
    "csv": "silver/jobs.csv",
    "excel": "silver/jobs.xlsx",
}


def _model(file_format: str) -> type[Model]:
    attrs: dict = {"location": FORMATS[file_format], "file_format": file_format}
    if file_format in ("csv", "excel"):
        # an empty text file has no types: its columns come back as strings
        attrs["validation_mode"] = "coerce"
    if file_format == "excel":
        attrs["load_options"] = {"header": True}
        attrs["save_options"] = {"header": True}
    meta = type("Meta", (), attrs)

    class Jobs(Model):
        id: int
        name: str

        Meta: ClassVar[type] = meta

    return Jobs


def _registry(tmp_path: pathlib.Path, model: type[Model], **kwargs) -> ModelRegistry:
    reg = ModelRegistry()
    reg.load_connections(
        Connections.from_dict(
            {
                "connections": {
                    "local": {
                        "conn_type": "fs",
                        "extra": {"base_path": str(tmp_path)},
                    },
                },
            },
        ),
    )
    reg.register(model, connection="local", **kwargs)
    return reg


@pytest.mark.parametrize("file_format", FORMATS)
def test_creates_the_empty_dataset_once(tmp_path, file_format):
    model = _model(file_format)
    with use_registry(_registry(tmp_path, model)):
        assert model.objects.create_table() is True
        df = model.objects.all()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0
        assert df.columns == ["id", "name"]
        assert model.objects.create_table() is False


@pytest.mark.parametrize("file_format", FORMATS)
def test_does_not_touch_existing_data(tmp_path, file_format):
    model = _model(file_format)
    with use_registry(_registry(tmp_path, model)):
        model.objects.save(pl.DataFrame({"id": [1], "name": ["a"]}), mode="overwrite")
        assert model.objects.create_table() is False
        assert model.objects.all().height == 1


@pytest.mark.parametrize("file_format", FORMATS)
def test_read_only_model_warns_and_creates_nothing(tmp_path, file_format):
    model = _model(file_format)
    reg = _registry(tmp_path, model, allowed_write_modes=[])
    with use_registry(reg):
        with pytest.warns(UserWarning, match="read-only"):
            assert model.objects.create_table() is False
    assert not (tmp_path / FORMATS[file_format]).exists()


def test_no_warning_when_it_is_created(tmp_path):
    model = _model("parquet")
    with use_registry(_registry(tmp_path, model)), warnings.catch_warnings():
        warnings.simplefilter("error")
        assert model.objects.create_table() is True
