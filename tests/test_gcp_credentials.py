import json
import warnings
from unittest.mock import MagicMock

import pytest

from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.datasets.delta_table_dataset import DeltaTableDataset

SA = {"type": "service_account", "project_id": "p"}
HMAC = {"login": "GOOG1EKEYID", "password": "hmac-secret"}


def _conn(conn_type="google_cloud_platform", extra=None, **fields):
    return {
        "conn_type": conn_type,
        "extra": {"bucket": "b", **(extra or {})},
        **fields,
    }


# ---------- DuckDB datasets (CSV, Parquet, Excel) ----------


def test_duckdb_hmac_keys_create_a_gcs_secret():
    ds = CSVDataset(conn=_conn(**HMAC))
    con = MagicMock()
    ds._configure_gcp_secret(con)
    sql = con.execute.call_args.args[0]
    assert "TYPE gcs" in sql
    assert "KEY_ID 'GOOG1EKEYID'" in sql
    assert "SECRET 'hmac-secret'" in sql


@pytest.mark.parametrize(
    "extra",
    [{"keyfile_dict": SA}, {"keyfile": json.dumps(SA)}, {"key_path": "/k.json"}],
)
def test_duckdb_warns_when_only_service_account_is_configured(extra):
    ds = CSVDataset(conn=_conn(extra=extra))
    con = MagicMock()
    with pytest.warns(UserWarning, match="HMAC"):
        ds._configure_gcp_secret(con)
    con.execute.assert_not_called()


def test_duckdb_does_not_warn_without_any_credentials():
    ds = CSVDataset(conn=_conn())
    con = MagicMock()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        ds._configure_gcp_secret(con)
    con.execute.assert_not_called()


def test_duckdb_hmac_with_service_account_does_not_warn():
    ds = CSVDataset(conn=_conn(extra={"keyfile_dict": SA}, **HMAC))
    con = MagicMock()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        ds._configure_gcp_secret(con)
    con.execute.assert_called_once()


# ---------- Delta ----------


@pytest.mark.parametrize("conn_type", ["google_cloud_platform", "google"])
@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        ({"keyfile_dict": SA}, {"google_service_account_key": json.dumps(SA)}),
        ({"keyfile_dict": json.dumps(SA)}, {"google_service_account_key": json.dumps(SA)}),
        ({"keyfile": json.dumps(SA)}, {"google_service_account_key": json.dumps(SA)}),
        ({"key_path": "/k.json"}, {"google_service_account": "/k.json"}),
    ],
)
def test_delta_storage_options_from_service_account(conn_type, extra, expected):
    ds = DeltaTableDataset(conn=_conn(conn_type, extra))
    assert ds._delta_storage_options() == expected


def test_delta_warns_when_only_hmac_keys_are_configured():
    ds = DeltaTableDataset(conn=_conn(**HMAC))
    with pytest.warns(UserWarning, match="service account"):
        assert ds._delta_storage_options() == {}


def test_delta_local_fs_ignores_login_password_silently():
    ds = DeltaTableDataset(conn={"conn_type": "fs", "extra": {}, **HMAC})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert ds._delta_storage_options() == {}


def test_delta_rejects_unsupported_connection_types():
    ds = DeltaTableDataset(conn={"conn_type": "postgres", "extra": {}})
    with pytest.raises(ValueError, match="not implemented"):
        ds._delta_storage_options()
