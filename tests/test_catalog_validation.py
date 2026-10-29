import pytest
from pydantic import ValidationError

from smallcat.catalog import (
    Catalog,
    CSVEntry,
    DeltaTableEntry,
    ExcelEntry,
    ParquetEntry,
)

CONN = {"conn_type": "fs", "extra": {"base_path": "."}}

# file_format -> (entry class, a bogus option name, a typo of a real option)
FORMATS = {
    "csv": CSVEntry,
    "excel": ExcelEntry,
    "parquet": ParquetEntry,
    "delta_table": DeltaTableEntry,
}


def _entry(file_format="csv", **extra):
    return {"connection": CONN, "file_format": file_format, "location": "x", **extra}


def _catalog(entry):
    return Catalog.model_validate({"entries": {"e": entry}})


def _error_types(excinfo):
    return {err["type"] for err in excinfo.value.errors()}


@pytest.mark.parametrize(("file_format", "cls"), FORMATS.items())
def test_load_and_save_options_are_optional(file_format, cls):
    entry = _catalog(_entry(file_format)).entries["e"]
    assert isinstance(entry, cls)
    assert entry.load_options is None
    assert entry.save_options is None


@pytest.mark.parametrize(("file_format", "cls"), FORMATS.items())
def test_entry_can_be_built_in_code_without_file_format_or_options(file_format, cls):
    entry = cls(connection=CONN, location="x")
    assert entry.file_format == file_format


def test_missing_file_format_is_rejected_instead_of_defaulting_to_csv():
    with pytest.raises(ValidationError) as excinfo:
        _catalog({"connection": CONN, "location": "datos.parquet"})
    assert "union_tag_not_found" in _error_types(excinfo)


def test_unknown_file_format_is_rejected():
    with pytest.raises(ValidationError) as excinfo:
        _catalog(_entry("json"))
    assert "union_tag_invalid" in _error_types(excinfo)


@pytest.mark.parametrize("file_format", FORMATS)
def test_unknown_key_in_an_entry_is_rejected(file_format):
    with pytest.raises(ValidationError) as excinfo:
        _catalog(_entry(file_format, locaton="typo"))
    assert "extra_forbidden" in _error_types(excinfo)


@pytest.mark.parametrize("file_format", FORMATS)
@pytest.mark.parametrize("which", ["load_options", "save_options"])
def test_unknown_key_in_options_is_rejected(file_format, which):
    with pytest.raises(ValidationError) as excinfo:
        _catalog(_entry(file_format, **{which: {"definitely_not_an_option": 1}}))
    assert "extra_forbidden" in _error_types(excinfo)


def test_unknown_top_level_key_is_rejected():
    with pytest.raises(ValidationError) as excinfo:
        Catalog.model_validate({"entries": {"e": _entry()}, "entires": {}})
    assert "extra_forbidden" in _error_types(excinfo)


def test_valid_options_are_still_accepted():
    entry = _catalog(
        _entry("csv", load_options={"header": True}, save_options={"header": True}),
    ).entries["e"]
    assert entry.load_options.header is True
    assert entry.save_options.header is True
