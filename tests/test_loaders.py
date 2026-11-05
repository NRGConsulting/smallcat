from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from smallcat.datasets.csv_dataset import CSVDataset, CSVLoadOptions, CSVSaveOptions
from smallcat.datasets.delta_table_dataset import (
    DeltaTableDataset,
    DeltaTableLoadOptions,
    DeltaTableSaveOptions,
    SchemaMode,
)
from smallcat.datasets.excel_dataset import (
    ExcelDataset,
    ExcelLoadOptions,
    ExcelSaveOptions,
)
from smallcat.datasets.parquet_dataset import (
    ParquetDataset,
    ParquetLoadOptions,
)


@pytest.mark.parametrize(
    "save_options,load_options",
    [
        (None, None),
        (CSVSaveOptions(), CSVLoadOptions()),
        (None, CSVLoadOptions(sep=",", header=True)),
        (CSVSaveOptions(header=True, sep=";"), None),
        (CSVSaveOptions(header=True, sep=";"), CSVLoadOptions(sep=";", header=True)),
    ],
)
def test_csv_dataset(example_df, local_conn, save_options, load_options):
    file_name = "foo.csv"
    saver = CSVDataset(local_conn, save_options=save_options)
    saver.save_polars(file_name, example_df)

    dataset = CSVDataset(local_conn, load_options=load_options)
    loaded_df = dataset.load_polars(file_name)

    assert_frame_equal(example_df, loaded_df)


@pytest.mark.parametrize(
    ("save_options", "load_options"),
    [
        (ExcelSaveOptions(header=True), None),
        (ExcelSaveOptions(header=True), ExcelLoadOptions(header=True)),
        (
            ExcelSaveOptions(header=True, sheet="sheet2"),
            ExcelLoadOptions(
                header=True,
                sheet="sheet2",
                all_varchar=False,
                empty_as_varchar=True,
            ),
        ),
    ],
)
def test_excel_dataset(example_df, local_conn, save_options, load_options):
    file_name = "foo.xlsx"
    saver = ExcelDataset(local_conn, save_options=save_options)
    saver.save_polars(file_name, example_df)

    dataset = ExcelDataset(local_conn, load_options=load_options)
    loaded_df = dataset.load_polars(file_name)

    assert_frame_equal(example_df, loaded_df)


@pytest.mark.parametrize(
    "save_options,load_options",
    [
        (None, None),
        (None, ParquetLoadOptions(hive_partitioning=True)),
    ],
)
def test_parquet_dataset(
    example_df,
    another_example_df,
    local_conn,
    save_options,
    load_options,
):
    file_name = "foo/year=2024/part001.parquet"
    another_file_name = "foo/year=2025/part002.parquet"

    (Path(local_conn["extra"]["base_path"]) / file_name).parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    (Path(local_conn["extra"]["base_path"]) / another_file_name).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    saver = ParquetDataset(local_conn, save_options=save_options)
    saver.save_polars(file_name, example_df)
    saver.save_polars(another_file_name, another_example_df)

    dataset = ParquetDataset(local_conn, load_options=load_options)
    loaded_df = dataset.load_polars("foo/**/*.parquet")

    hive_partitioning = load_options and load_options.hive_partitioning
    validation_df = pl.concat(
        [
            example_df.with_columns(year=pl.lit(2024, dtype=pl.Int64))
            if hive_partitioning
            else example_df,
            another_example_df.with_columns(year=pl.lit(2025, dtype=pl.Int64))
            if hive_partitioning
            else another_example_df,
        ],
    )

    assert_frame_equal(validation_df, loaded_df)


@pytest.mark.parametrize(
    "save_options,load_options",
    [
        (None, None),
        (DeltaTableSaveOptions(), DeltaTableLoadOptions()),
        (
            DeltaTableSaveOptions(schema_mode=SchemaMode.MERGE),
            DeltaTableLoadOptions(version=0),
        ),
    ],
)
def test_delta_table_dataset(example_df, local_conn, save_options, load_options):
    table_path = "foo/"
    saver = DeltaTableDataset(local_conn, save_options=save_options)
    saver.save_polars(table_path, example_df)

    dataset = DeltaTableDataset(local_conn, load_options=load_options)
    loaded_df = dataset.load_polars(table_path)

    assert_frame_equal(example_df, loaded_df)


def test_delta_table_dataset_with_where(example_df, local_conn):
    table_path = "foo/"
    saver = DeltaTableDataset(local_conn)
    saver.save_polars(table_path, example_df)

    dataset = DeltaTableDataset(local_conn)
    loaded_df = dataset.load_polars(
        table_path,
        where="amount > 10",
        columns=["id", "amount"],
    )

    expected_df = example_df.filter(pl.col("amount") > 10).select("id", "amount")
    assert_frame_equal(expected_df, loaded_df)


def test_csv_dataset_with_columns(example_df, local_conn):
    file_name = "foo.csv"
    saver = CSVDataset(local_conn)
    saver.save_polars(file_name, example_df)

    dataset = CSVDataset(local_conn)
    loaded_df = dataset.load_polars(file_name, columns=["name", "id"])

    expected_df = example_df.select("name", "id")
    assert_frame_equal(expected_df, loaded_df)


def test_csv_dataset_rejects_unsafe_inputs(example_df, local_conn):
    file_name = "foo.csv"
    saver = CSVDataset(local_conn)
    saver.save_polars(file_name, example_df)

    dataset = CSVDataset(local_conn)
    with pytest.raises(ValueError):
        dataset.load_polars(file_name, where="amount > 10; drop table data")
    with pytest.raises(ValueError):
        dataset.load_polars(file_name, columns=["name;drop"])


@pytest.mark.parametrize("dataset_cls", [ParquetDataset, DeltaTableDataset])
def test_polars_roundtrip_keeps_nulls_and_types(dataset_cls, local_conn):
    from datetime import datetime

    df = pl.DataFrame(
        {
            "id": pl.Series([1, None, 3], dtype=pl.Int64),
            "name": ["a", None, "c"],
            "ts": [datetime(2024, 1, 1), None, datetime(2024, 3, 1, 12, 30)],
        },
        schema_overrides={"ts": pl.Datetime("us")},
    )
    path = "rt.parquet" if dataset_cls is ParquetDataset else "rt/"

    dataset_cls(local_conn).save_polars(path, df)
    loaded = dataset_cls(local_conn).load_polars(path)

    assert_frame_equal(df, loaded, check_row_order=False)
    assert "__index_level_0__" not in loaded.columns
