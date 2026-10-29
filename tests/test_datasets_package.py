import importlib
import subprocess
import sys
import typing

import pytest

import smallcat.datasets as datasets
from smallcat.catalog import Catalog
from smallcat.datasets import csv_dataset
from smallcat.datasets.csv_dataset import CSVLoadOptions, CSVSaveOptions
from smallcat.datasets.parquet_dataset import (
    ParquetDataset,
    ParquetLoadOptions,
    ParquetSaveOptions,
)

EXPORTS = {
    "BaseDataset": "smallcat.datasets.base_dataset",
    "CSVDataset": "smallcat.datasets.csv_dataset",
    "DeltaTableDataset": "smallcat.datasets.delta_table_dataset",
    "ExcelDataset": "smallcat.datasets.excel_dataset",
    "ParquetDataset": "smallcat.datasets.parquet_dataset",
}


@pytest.mark.parametrize(("name", "module"), EXPORTS.items())
def test_package_reexports_the_dataset_classes(name, module):
    expected = getattr(importlib.import_module(module), name)
    assert getattr(datasets, name) is expected
    assert name in datasets.__all__


def test_all_matches_the_documented_exports():
    assert sorted(datasets.__all__) == sorted(EXPORTS)
    assert dir(datasets) == sorted(EXPORTS)


def test_unknown_attribute_raises_attribute_error():
    with pytest.raises(AttributeError, match="no attribute 'Nope'"):
        _ = datasets.Nope


def test_importing_the_package_is_lazy():
    code = (
        "import sys, smallcat.datasets as d;"
        "assert 'deltalake' not in sys.modules, 'deltalake imported';"
        "d.CSVDataset;"
        "assert 'deltalake' not in sys.modules, 'deltalake imported by CSV'"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_parquet_dataset_is_parametrized_with_its_option_models():
    (base,) = ParquetDataset.__orig_bases__
    assert typing.get_args(base) == (ParquetLoadOptions, ParquetSaveOptions)


def test_csv_docstring_documents_the_real_options_only():
    doc = csv_dataset.__doc__
    for model in (CSVLoadOptions, CSVSaveOptions):
        for field in model.model_fields:
            assert f"`{field}`" in doc, f"{field} missing from the CSV module docstring"
    for stale in ("delimiter", "nullstr", "partition_by", "Hive"):
        assert stale not in doc


def test_get_dataset_raises_key_error_for_unknown_entries():
    catalog = Catalog.model_validate({"entries": {}})
    with pytest.raises(KeyError):
        catalog.get_dataset("missing")
    assert "ValueError" not in (Catalog.get_dataset.__doc__ or "")
