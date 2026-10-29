"""Google Cloud Storage connection: credential validation and sources."""

import warnings
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from smallcat.catalog import CSVEntry
from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.datasets.delta_table_dataset import DeltaTableDataset

HMAC = {"login": "GOOGKEY", "password": "secret"}
SA = {"keyfile": '{"type": "service_account"}'}


def _conn(extra=None, conn_type="google_cloud_platform", **fields):
    return {"conn_type": conn_type, "extra": {"bucket": "b", **(extra or {})}, **fields}


@pytest.mark.parametrize("conn_type", ["google_cloud_platform", "google"])
def test_hmac_and_service_account_together_are_valid(conn_type):
    CSVEntry(connection=_conn(SA, conn_type, **HMAC), location="a.csv")


@pytest.mark.parametrize(
    "conn",
    [
        _conn(login="only-a-key-id"),
        _conn(password="only-a-secret"),
        _conn({"keyfile": "{}", "key_path": "/k.json"}),
        _conn({"keyfile_dict": {"a": 1}, "keyfile": "{}", "key_path": "/k.json"}),
    ],
)
def test_incoherent_gcs_connection_is_rejected(conn):
    with pytest.raises(ValidationError):
        CSVEntry(connection=conn, location="a.csv")


def test_incoherent_connection_from_airflow_is_rejected_by_duckdb_datasets():
    ds = CSVDataset(conn=_conn({"keyfile": "{}", "key_path": "/k.json"}))
    with pytest.raises(ValueError, match="only one service account"):
        ds._configure_gcp_secret(MagicMock())  # noqa: SLF001


def test_incoherent_connection_from_airflow_is_rejected_by_delta():
    ds = DeltaTableDataset(conn=_conn({"keyfile": "{}", "key_path": "/k.json"}))
    with pytest.raises(ValueError, match="only one service account"):
        ds._delta_storage_options()  # noqa: SLF001


def test_each_engine_uses_the_credentials_it_understands():
    conn = _conn(SA, **HMAC)
    con = MagicMock()
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # no "wrong credentials" warning
        CSVDataset(conn=conn)._configure_gcp_secret(con)  # noqa: SLF001
        options = DeltaTableDataset(conn=conn)._delta_storage_options()  # noqa: SLF001
    assert "KEY_ID 'GOOGKEY'" in con.execute.call_args.args[0]
    assert options == {"google_service_account_key": SA["keyfile"]}
