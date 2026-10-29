"""SC-017: inline connections are validated against the connection schemas."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from smallcat.catalog import Catalog, CSVEntry
from smallcat.connections import (
    GoogleCloudPlatformConnectionSchema,
    LocalFsConnectionSchema,
)
from smallcat.datasets.csv_dataset import CSVDataset


def _entry(connection: object) -> CSVEntry:
    return CSVEntry(connection=connection, location="a.csv")


def test_dict_is_instantiated_as_its_schema() -> None:
    fs = _entry({"conn_type": "fs", "extra": {"base_path": "/data"}})
    gcs = _entry({"conn_type": "google_cloud_platform", "extra": {"bucket": "b"}})
    assert isinstance(fs.connection, LocalFsConnectionSchema)
    assert isinstance(gcs.connection, GoogleCloudPlatformConnectionSchema)


def test_google_alias_is_accepted() -> None:
    entry = _entry({"conn_type": "google", "extra": {"bucket": "b"}})
    assert entry.get_connection()["conn_type"] == "google"


@pytest.mark.parametrize(
    "connection",
    [
        {"conn_type": "bogus"},
        {"extra": {"base_path": "/data"}},  # conn_type is required
        {"conn_type": "fs", "unknown": 1},
        {"conn_type": "fs", "extra": {"base_pth": "/data"}},  # typo
        {"conn_type": "google_cloud_platform", "extra": {"buckett": "b"}},
        {"conn_type": "fs", "extra": {"base_path": 3}},
    ],
)
def test_invalid_inline_connection_is_rejected(connection: dict) -> None:
    with pytest.raises(ValidationError):
        _entry(connection)


def test_connection_id_still_accepted() -> None:
    assert _entry("my_airflow_conn").connection == "my_airflow_conn"


def test_invalid_connection_in_yaml_catalog_is_rejected() -> None:
    data = {
        "entries": {
            "a": {
                "file_format": "csv",
                "location": "a.csv",
                "connection": {"conn_type": "fs", "extra": {"base_pth": "/x"}},
            },
        },
    }
    with pytest.raises(ValidationError, match="base_pth"):
        Catalog.model_validate(data)


def test_validated_fs_connection_resolves_base_uri(tmp_path: Path) -> None:
    base = tmp_path.as_posix()
    by_path = _entry({"conn_type": "fs", "extra": {"base_path": base}})
    by_uri = _entry({"conn_type": "fs", "extra": {"base_uri": f"file://{base}"}})
    assert CSVDataset(conn=by_path.get_connection()).base_uri == f"file://{base}"
    assert CSVDataset(conn=by_uri.get_connection()).base_uri == f"file://{base}"
    bare = _entry({"conn_type": "fs"}).get_connection()
    assert CSVDataset(conn=bare).base_uri is None


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        ({"bucket": "b", "prefix": "raw"}, "gs://b/raw"),
        ({"bucket": "b"}, "gs://b/"),
        ({"base_uri": "gs://other/x"}, "gs://other/x"),
    ],
)
def test_validated_gcs_connection_resolves_base_uri(extra: dict, expected: str) -> None:
    conn = _entry({"conn_type": "google_cloud_platform", "extra": extra})
    assert CSVDataset(conn=conn.get_connection()).base_uri == expected


def test_get_connection_omits_unset_fields() -> None:
    conn = _entry({"conn_type": "fs", "extra": {"base_path": "/d"}}).get_connection()
    assert conn == {"conn_type": "fs", "extra": {"base_path": "/d"}}
