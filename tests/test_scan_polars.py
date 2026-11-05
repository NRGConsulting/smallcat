"""Lazy scans with Polars: parity with the DuckDB loaders, pushdown, credentials."""

from datetime import datetime
from unittest.mock import patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from smallcat.catalog import Catalog
from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.datasets.delta_table_dataset import (
    DeltaTableDataset,
    DeltaTableLoadOptions,
    DeltaTableSaveOptions,
    WriteMode,
)
from smallcat.datasets.parquet_dataset import ParquetDataset, ParquetLoadOptions

DATABRICKS_CONN = {
    "conn_type": "databricks",
    "host": "https://example.cloud.databricks.com",
    "password": "token",
    "extra": {},
}


@pytest.fixture
def rich_df():
    return pl.DataFrame(
        {
            "id": pl.Series([1, None, 3, 4], dtype=pl.Int64),
            "name": ["a", None, "c", "d"],
            "amount": [1.5, 2.5, None, 10.0],
            "ts": [datetime(2024, 1, 1), None, datetime(2024, 3, 1, 12), None],
        },
        schema_overrides={"ts": pl.Datetime("us")},
    )


def _save(dataset_cls, conn, df):
    path = "t.parquet" if dataset_cls is ParquetDataset else "t/"
    dataset_cls(conn).save_polars(path, df)
    return path


# ---------- parity with load_polars ----------


@pytest.mark.parametrize("dataset_cls", [ParquetDataset, DeltaTableDataset])
def test_scan_matches_load(dataset_cls, local_conn, rich_df):
    path = _save(dataset_cls, local_conn, rich_df)
    ds = dataset_cls(local_conn)

    lazy = ds.scan_polars(path)

    assert isinstance(lazy, pl.LazyFrame)
    assert_frame_equal(lazy.collect(), ds.load_polars(path), check_row_order=False)


@pytest.mark.parametrize("dataset_cls", [ParquetDataset, DeltaTableDataset])
def test_scan_filter_matches_where(dataset_cls, local_conn, rich_df):
    path = _save(dataset_cls, local_conn, rich_df)
    ds = dataset_cls(local_conn)

    scanned = ds.scan_polars(path).filter(pl.col("amount") > 2).collect()
    loaded = ds.load_polars(path, where="amount > 2")

    assert_frame_equal(scanned, loaded, check_row_order=False)


@pytest.mark.parametrize("dataset_cls", [ParquetDataset, DeltaTableDataset])
def test_scan_columns(dataset_cls, local_conn, rich_df):
    path = _save(dataset_cls, local_conn, rich_df)

    out = dataset_cls(local_conn).scan_polars(path, columns=["name", "id"]).collect()

    assert out.columns == ["name", "id"]


# ---------- pushdown ----------


@pytest.mark.parametrize("dataset_cls", [ParquetDataset, DeltaTableDataset])
def test_filter_and_projection_are_pushed_into_the_scan(
    dataset_cls,
    local_conn,
    rich_df,
):
    path = _save(dataset_cls, local_conn, rich_df)

    plan = (
        dataset_cls(local_conn)
        .scan_polars(path)
        .filter(pl.col("id") > 1)
        .select("name")
        .explain()
    )

    assert "SELECTION" in plan  # the predicate runs inside the scan
    assert "PROJECT 2/4 COLUMNS" in plan  # only `name` and `id` are read


# ---------- Parquet specifics ----------


def test_parquet_scan_reads_globs_and_hive_partitions(local_conn, example_df):
    saver = ParquetDataset(local_conn)
    saver.save_polars("foo/year=2024/p1.parquet", example_df)
    saver.save_polars("foo/year=2025/p2.parquet", example_df)

    ds = ParquetDataset(
        local_conn,
        load_options=ParquetLoadOptions(hive_partitioning=True),
    )
    out = ds.scan_polars("foo/**/*.parquet").collect()

    assert out.height == 2 * example_df.height
    assert sorted(out["year"].unique().to_list()) == [2024, 2025]


@pytest.mark.parametrize("option", ["binary_as_string", "file_row_number"])
def test_parquet_scan_rejects_options_without_a_polars_equivalent(
    option,
    local_conn,
    example_df,
):
    ParquetDataset(local_conn).save_polars("t.parquet", example_df)
    ds = ParquetDataset(
        local_conn,
        load_options=ParquetLoadOptions(**{option: True}),
    )

    with pytest.raises(ValueError, match=option):
        ds.scan_polars("t.parquet")


def test_parquet_scan_cannot_read_outside_its_base(tmp_path):
    base = tmp_path / "base"
    base.mkdir()
    ds = ParquetDataset(conn={"conn_type": "fs", "extra": {"base_uri": str(base)}})

    with pytest.raises(ValueError, match="escapes the base"):
        ds.scan_polars("../outside.parquet")


# ---------- Delta specifics ----------


def test_delta_scan_honours_the_table_version(local_conn, example_df):
    saver = DeltaTableDataset(
        local_conn,
        save_options=DeltaTableSaveOptions(mode=WriteMode.APPEND),
    )
    saver.save_polars("t/", example_df)  # version 0
    saver.save_polars("t/", example_df.head(1))  # version 1 adds a row
    ds_v0 = DeltaTableDataset(local_conn, load_options=DeltaTableLoadOptions(version=0))

    out = ds_v0.scan_polars("t/").collect()

    assert_frame_equal(out, example_df, check_row_order=False)


def test_delta_scan_rejects_databricks():
    with pytest.raises(NotImplementedError, match="Databricks"):
        DeltaTableDataset(conn=DATABRICKS_CONN).scan_polars("t")


# ---------- fallback for formats without a real scan ----------


def test_csv_scan_falls_back_to_an_eager_load(local_conn, example_df):
    CSVDataset(local_conn).save_polars("foo.csv", example_df)
    ds = CSVDataset(local_conn)

    lazy = ds.scan_polars("foo.csv")

    assert isinstance(lazy, pl.LazyFrame)
    assert_frame_equal(lazy.collect(), example_df)
    assert ds.scan_polars("foo.csv", columns=["name"]).collect().columns == ["name"]


def test_csv_scan_still_rejects_unsafe_column_names(local_conn, example_df):
    CSVDataset(local_conn).save_polars("foo.csv", example_df)

    with pytest.raises(ValueError, match="Unsafe column"):
        CSVDataset(local_conn).scan_polars("foo.csv", columns=["name;drop"])


# ---------- catalog ----------


@pytest.mark.parametrize(
    ("file_format", "location"),
    [("parquet", "t.parquet"), ("delta_table", "t"), ("csv", "t.csv")],
)
def test_catalog_scan_polars(local_conn, example_df, file_format, location):
    catalog = Catalog.from_dict(
        {
            "entries": {
                "e": {
                    "connection": local_conn,
                    "file_format": file_format,
                    "location": location,
                },
            },
        },
    )
    catalog.save_polars("e", example_df)

    out = catalog.scan_polars("e").filter(pl.col("amount") > 10).collect()

    assert out["name"].to_list() == ["Alice", "Cara"]
    with pytest.raises(KeyError):
        catalog.scan_polars("missing")


# ---------- credentials handed to Polars ----------


def _s3_conn(extra=None, **fields):
    return {"conn_type": "aws", "extra": {"bucket": "b", **(extra or {})}, **fields}


def test_parquet_scan_passes_aws_options_to_polars():
    ds = ParquetDataset(
        conn=_s3_conn(
            {"region_name": "eu-west-1", "endpoint_url": "http://127.0.0.1:9000"},
            login="AKIAEXAMPLE",
            password="s3cr3t",
        ),
    )

    with patch("smallcat.datasets.parquet_dataset.pl.scan_parquet") as scan:
        ds.scan_polars("t.parquet")

    uri = scan.call_args.args[0]
    assert uri.startswith("s3://b/") and uri.endswith("t.parquet")
    assert scan.call_args.kwargs["storage_options"] == {
        "aws_region": "eu-west-1",
        "aws_endpoint": "http://127.0.0.1:9000",
        "aws_allow_http": "true",
        "aws_access_key_id": "AKIAEXAMPLE",
        "aws_secret_access_key": "s3cr3t",
    }


def test_parquet_scan_of_local_files_passes_no_storage_options(local_conn, example_df):
    ParquetDataset(local_conn).save_polars("t.parquet", example_df)

    with patch(
        "smallcat.datasets.parquet_dataset.pl.scan_parquet",
        wraps=pl.scan_parquet,
    ) as scan:
        ParquetDataset(local_conn).scan_polars("t.parquet").collect()

    assert scan.call_args.kwargs["storage_options"] is None


def test_parquet_scan_with_gcs_service_account_uses_the_same_options_as_delta():
    sa = '{"type": "service_account"}'
    conn = {
        "conn_type": "google_cloud_platform",
        "extra": {"bucket": "b", "keyfile": sa},
    }

    with patch("smallcat.datasets.parquet_dataset.pl.scan_parquet") as scan:
        ParquetDataset(conn=conn).scan_polars("t.parquet")

    expected = DeltaTableDataset(conn=conn)._delta_storage_options()  # noqa: SLF001
    assert scan.call_args.kwargs["storage_options"] == expected
    assert expected == {"google_service_account_key": sa}
