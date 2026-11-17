"""SC-045: ``conn_type`` is compared as a ``ConnType``, whatever its case."""

import warnings
from typing import get_args

import pyarrow as pa
import pytest

from smallcat.connections import (
    AWS_CONN_TYPES,
    GCP_CONN_TYPES,
    ConnType,
    SupportedConnectionSchemas,
)
from smallcat.connections_config import SUPPORTED_CONN_TYPES
from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.datasets.delta_table_dataset import DeltaTableDataset



def _spellings(conn_type: ConnType) -> list[str]:
    value = conn_type.value
    return [value, value.upper(), value.title(), value.swapcase()]


def _conn(conn_type: str, **fields):
    return {"conn_type": conn_type, "extra": {"bucket": "b"}, **fields}


# ---------- the enum ----------


@pytest.mark.parametrize("member", list(ConnType))
def test_parse_ignores_case(member):
    for spelling in _spellings(member):
        assert ConnType.parse(spelling) is member


@pytest.mark.parametrize("value", [None, "", "mysql", " fs"])
def test_parse_returns_none_for_missing_or_unknown_types(value):
    assert ConnType.parse(value) is None


def test_members_still_compare_equal_to_their_text():
    assert ConnType.FS == "fs"
    assert "s3" in AWS_CONN_TYPES
    assert "google" in GCP_CONN_TYPES


def test_the_enum_covers_every_type_the_schemas_declare():
    """A schema can not gain a ``conn_type`` that the enum does not know."""
    declared = {
        value
        for schema in get_args(SupportedConnectionSchemas)
        for value in get_args(schema.model_fields["conn_type"].annotation)
    }
    assert declared <= {member.value for member in ConnType}
    assert SUPPORTED_CONN_TYPES == declared
    assert {member.value for member in ConnType} - declared == {"databricks"}


# ---------- Databricks is refused whatever its case ----------


@pytest.mark.parametrize("spelling", _spellings(ConnType.DATABRICKS))
def test_databricks_is_refused_with_its_own_message_in_every_case(spelling):
    conn = _conn(spelling, host="https://x.cloud.databricks.com", password="t")
    ds = DeltaTableDataset(conn=conn)

    with pytest.raises(NotImplementedError, match="Databricks"):
        ds.save_arrow_table("t", pa.table({"a": [1]}))
    with pytest.raises(NotImplementedError, match="Databricks"):
        ds.load_arrow_record_batch_reader("t")
    with pytest.raises(NotImplementedError, match="Databricks"):
        ds.scan_polars("t")
    with pytest.raises(NotImplementedError, match="Databricks"):
        ds.update("t", "a = 1", {"a": 2})


# ---------- the HMAC warning depends on the type, not on its case ----------

HMAC = {"login": "GOOG1EKEYID", "password": "hmac-secret"}


@pytest.mark.parametrize("spelling", _spellings(ConnType.FS))
def test_a_local_connection_never_warns_about_gcs_keys(spelling):
    ds = CSVDataset(conn=_conn(spelling, **HMAC))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert ds._object_store_options() == {}


@pytest.mark.parametrize("member", sorted(GCP_CONN_TYPES))
def test_a_google_connection_warns_in_every_case(member):
    for spelling in _spellings(member):
        ds = CSVDataset(conn=_conn(spelling, **HMAC))
        with pytest.warns(UserWarning, match="HMAC"):
            ds._object_store_options()


# ---------- the rest of the code resolves the type the same way ----------


@pytest.mark.parametrize(
    ("member", "expected"),
    [
        (ConnType.GOOGLE_CLOUD_PLATFORM, "gs://b/"),
        (ConnType.GOOGLE, "gs://b/"),
        (ConnType.AWS, "s3://b/"),
        (ConnType.S3, "s3://b/"),
    ],
)
def test_the_base_uri_is_inferred_in_every_case(member, expected):
    for spelling in _spellings(member):
        assert CSVDataset(conn=_conn(spelling)).base_uri == expected


@pytest.mark.parametrize("spelling", _spellings(ConnType.FS))
def test_a_local_connection_resolves_its_base_path_in_every_case(spelling, tmp_path):
    conn = {"conn_type": spelling, "extra": {"base_path": str(tmp_path)}}
    assert CSVDataset(conn=conn).base_uri.startswith("file://")


def test_an_unknown_type_is_refused_when_object_store_options_are_needed():
    ds = CSVDataset(conn=_conn("mysql"))
    with pytest.raises(ValueError, match="not implemented for type mysql"):
        ds._object_store_options()
