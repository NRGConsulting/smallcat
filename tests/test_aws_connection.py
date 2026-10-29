"""Amazon S3 connection: schema, DuckDB secret, delta-rs options and base URI."""

import os
import urllib.error
import urllib.request
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from smallcat.catalog import Catalog, CSVEntry
from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.datasets.delta_table_dataset import DeltaTableDataset

KEYS = {"login": "AKIAEXAMPLE", "password": "s3cr3t"}


def _conn(extra=None, **fields):
    return {"conn_type": "aws", "extra": {"bucket": "b", **(extra or {})}, **fields}


def _sql(ds, con=None):
    con = con or MagicMock()
    with patch("smallcat.datasets.base_dataset.ensure_extension"):
        ds._configure_s3_secret(con)
    return con.execute.call_args.args[0]


# ---------- schema ----------


@pytest.mark.parametrize("conn_type", ["aws", "s3"])
def test_aws_connection_is_accepted_inline(conn_type):
    entry = CSVEntry(connection={**_conn(), "conn_type": conn_type}, location="a.csv")
    assert entry.get_connection()["conn_type"] == conn_type


@pytest.mark.parametrize(
    "conn",
    [
        _conn({"buckett": "x"}),  # typo in extra
        _conn(login="only-a-key-id"),  # keys must come in pairs
        _conn({"aws_session_token": "t"}),  # token without keys
        _conn({"profile_name": "p"}, **KEYS),  # profile and static keys
        {"conn_type": "aws", "unknown": 1},
    ],
)
def test_incoherent_aws_connection_is_rejected(conn):
    with pytest.raises(ValidationError):
        CSVEntry(connection=conn, location="a.csv")


def test_keys_with_session_token_are_valid():
    CSVEntry(connection=_conn({"aws_session_token": "t"}, **KEYS), location="a.csv")


def test_inline_catalog_accepts_aws_connection():
    data = {
        "entries": {
            "a": {
                "file_format": "parquet",
                "location": "a.parquet",
                "connection": _conn({"region_name": "eu-west-1", "prefix": "raw"}),
            }
        }
    }
    assert isinstance(Catalog.model_validate(data), Catalog)


# ---------- base URI and httpfs ----------


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        ({"bucket": "b", "prefix": "raw"}, "s3://b/raw"),
        ({"bucket": "b"}, "s3://b/"),
        ({"base_uri": "s3://other/x"}, "s3://other/x"),
        ({}, None),
    ],
)
def test_base_uri_is_built_from_bucket_and_prefix(extra, expected):
    ds = CSVDataset(conn={"conn_type": "aws", "extra": extra})
    assert ds.base_uri == expected


def test_aws_connection_needs_httpfs():
    assert CSVDataset(conn=_conn())._needs_httpfs()  # noqa: SLF001


# ---------- DuckDB secret ----------


def test_static_keys_create_an_s3_secret_with_those_keys():
    sql = _sql(CSVDataset(conn=_conn({"region_name": "eu-west-1"}, **KEYS)))
    assert "TYPE s3" in sql
    assert "KEY_ID 'AKIAEXAMPLE'" in sql
    assert "SECRET 's3cr3t'" in sql
    assert "REGION 'eu-west-1'" in sql
    assert "credential_chain" not in sql
    assert "SESSION_TOKEN" not in sql


def test_session_token_is_forwarded():
    sql = _sql(CSVDataset(conn=_conn({"aws_session_token": "tok"}, **KEYS)))
    assert "SESSION_TOKEN 'tok'" in sql


def test_secrets_are_quoted():
    sql = _sql(CSVDataset(conn=_conn(login="a'b", password="c'd")))
    assert "KEY_ID 'a''b'" in sql
    assert "SECRET 'c''d'" in sql


def test_without_keys_the_aws_credential_chain_is_used():
    # This is how IAM roles work: instance profile, ECS task role, web identity...
    ds = CSVDataset(conn=_conn())
    con = MagicMock()
    with patch("smallcat.datasets.base_dataset.ensure_extension") as ensure:
        ds._configure_s3_secret(con)  # noqa: SLF001
    ensure.assert_called_once_with(con, "aws")
    sql = con.execute.call_args.args[0]
    assert "PROVIDER credential_chain" in sql
    assert "KEY_ID" not in sql
    assert "CHAIN" not in sql


def test_profile_restricts_the_chain_to_that_profile():
    sql = _sql(CSVDataset(conn=_conn({"profile_name": "analytics"})))
    assert "PROVIDER credential_chain" in sql
    assert "CHAIN 'config'" in sql
    assert "PROFILE 'analytics'" in sql


def test_chain_falls_back_when_the_default_one_fails():
    ds = CSVDataset(conn=_conn())
    con = MagicMock()
    con.execute.side_effect = [duckdb.Error("no profile 'default'"), None]
    _sql(ds, con)
    assert con.execute.call_count == 2
    assert "CHAIN 'env;" in con.execute.call_args.args[0]


def test_failure_is_not_retried_for_keys_or_profiles():
    for conn in (_conn(**KEYS), _conn({"profile_name": "p"})):
        con = MagicMock()
        con.execute.side_effect = duckdb.Error("boom")
        with pytest.raises(duckdb.Error):
            _sql(CSVDataset(conn=conn), con)
        assert con.execute.call_count == 1


@pytest.mark.parametrize(
    ("endpoint", "ssl"),
    [("http://minio:9000", "false"), ("https://s3.example.com", "true"), ("minio:9000", "true")],
)
def test_custom_endpoint_uses_path_style(endpoint, ssl):
    sql = _sql(CSVDataset(conn=_conn({"endpoint_url": endpoint}, **KEYS)))
    host = endpoint.split("://")[-1]
    assert f"ENDPOINT '{host}'" in sql
    assert "URL_STYLE 'path'" in sql
    assert f"USE_SSL {ssl}" in sql


def test_incoherent_connection_from_airflow_is_rejected_when_used():
    # Airflow connection ids skip the schema, so the dataset checks again.
    ds = CSVDataset(conn=_conn({"profile_name": "p"}, **KEYS))
    with pytest.raises(ValueError, match="profile_name"):
        _sql(ds)


# ---------- delta-rs storage options ----------


def test_delta_static_keys_become_aws_options():
    ds = DeltaTableDataset(
        conn=_conn({"region_name": "eu-west-1", "aws_session_token": "t"}, **KEYS),
    )
    assert ds._delta_storage_options() == {  # noqa: SLF001
        "aws_region": "eu-west-1",
        "aws_access_key_id": "AKIAEXAMPLE",
        "aws_secret_access_key": "s3cr3t",
        "aws_session_token": "t",
    }


def test_delta_without_keys_leaves_credentials_to_delta_rs():
    ds = DeltaTableDataset(conn=_conn({"region_name": "eu-west-1"}))
    assert ds._delta_storage_options() == {"aws_region": "eu-west-1"}  # noqa: SLF001


def test_delta_http_endpoint_is_allowed_explicitly():
    ds = DeltaTableDataset(conn=_conn({"endpoint_url": "http://minio:9000"}))
    options = ds._delta_storage_options()  # noqa: SLF001
    assert options["aws_endpoint"] == "http://minio:9000"
    assert options["aws_allow_http"] == "true"


def test_delta_https_endpoint_does_not_allow_http():
    ds = DeltaTableDataset(conn=_conn({"endpoint_url": "https://s3.example.com"}))
    assert "aws_allow_http" not in ds._delta_storage_options()  # noqa: SLF001


def test_delta_profile_credentials_are_resolved_through_duckdb():
    ds = DeltaTableDataset(conn=_conn({"profile_name": "p"}))
    creds = {"aws_access_key_id": "K", "aws_secret_access_key": "S"}
    with patch.object(ds, "_resolve_aws_credentials", return_value=creds):
        assert ds._delta_storage_options() == creds  # noqa: SLF001


def _resolver_with_row(row):
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = row
    return patch("smallcat.datasets.base_dataset.duckdb.connect", return_value=con)


def test_resolved_credentials_are_read_from_the_secret():
    ds = DeltaTableDataset(conn=_conn({"profile_name": "p"}))
    secret = "name=x;type=s3;key_id=K;secret=S;session_token=T;region=r"
    with (
        _resolver_with_row((secret,)),
        patch("smallcat.datasets.base_dataset.ensure_extension"),
        patch.object(ds, "_configure_s3_secret"),
    ):
        assert ds._resolve_aws_credentials() == {  # noqa: SLF001
            "aws_access_key_id": "K",
            "aws_secret_access_key": "S",
            "aws_session_token": "T",
        }


def test_resolving_without_credentials_is_an_error():
    ds = DeltaTableDataset(conn=_conn({"profile_name": "p"}))
    with (
        _resolver_with_row(None),
        patch("smallcat.datasets.base_dataset.ensure_extension"),
        patch.object(ds, "_configure_s3_secret"),
        pytest.raises(RuntimeError, match="did not return any credentials"),
    ):
        ds._resolve_aws_credentials()  # noqa: SLF001


# ---------- against an S3-compatible server (optional) ----------
# Set SMALLCAT_TEST_S3_ENDPOINT to a running S3 mock, for example
# `moto_server -p 5555` -> http://127.0.0.1:5555. Skipped otherwise.

ENDPOINT = os.environ.get("SMALLCAT_TEST_S3_ENDPOINT")
needs_s3_server = pytest.mark.skipif(
    not ENDPOINT, reason="SMALLCAT_TEST_S3_ENDPOINT is not set"
)


@pytest.fixture
def s3_catalog():
    bucket = "smallcat-test"
    try:
        urllib.request.urlopen(  # noqa: S310
            urllib.request.Request(f"{ENDPOINT}/{bucket}", method="PUT"),  # noqa: S310
        )
    except urllib.error.HTTPError:
        pass  # the bucket already exists

    def make(file_format, location):
        conn = _conn(
            {"bucket": bucket, "region_name": "us-east-1", "endpoint_url": ENDPOINT},
            login="test",
            password="test",
        )
        return Catalog.model_validate(
            {
                "entries": {
                    "e": {
                        "file_format": file_format,
                        "location": location,
                        "connection": conn,
                    }
                }
            }
        )

    return make


@needs_s3_server
@pytest.mark.parametrize(
    ("file_format", "location"),
    [("parquet", "t.parquet"), ("csv", "t.csv"), ("delta_table", "delta")],
)
def test_roundtrip_against_an_s3_server(s3_catalog, file_format, location):
    import uuid

    catalog = s3_catalog(file_format, f"{uuid.uuid4().hex}/{location}")
    df = pd.DataFrame({"a": [1, 2, 3], "b": list("xyz")})
    catalog.save_pandas("e", df)
    assert catalog.load_pandas("e").sort_values("a").reset_index(drop=True).equals(df)
