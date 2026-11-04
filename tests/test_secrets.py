"""SC-021: secrets are not exposed by repr, str or model_dump."""
# ruff: noqa: S105, S106


import json

from smallcat.catalog import CSVEntry
from smallcat.connections import (
    AwsConnectionSchema,
    ConnectionLike,
    GoogleCloudPlatformConnectionSchema,
)

SA = {"type": "service_account", "private_key": "PRIVATE-KEY-VALUE"}


def _gcs(**extra: object) -> GoogleCloudPlatformConnectionSchema:
    return GoogleCloudPlatformConnectionSchema(
        login="key-id",
        password="HMAC-SECRET",
        extra={"bucket": "b", **extra},
    )


def test_gcs_secrets_are_hidden_from_repr_and_dump() -> None:
    conn = _gcs(keyfile_dict=SA)
    for text in (repr(conn), str(conn), str(conn.model_dump())):
        assert "HMAC-SECRET" not in text
        assert "PRIVATE-KEY-VALUE" not in text


def test_keyfile_string_is_hidden() -> None:
    conn = _gcs(keyfile=json.dumps(SA))
    assert "PRIVATE-KEY-VALUE" not in repr(conn)


def test_aws_secrets_are_hidden() -> None:
    conn = AwsConnectionSchema(
        login="AKIA",
        password="AWS-SECRET",
        extra={"aws_session_token": "SESSION-TOKEN"},
    )
    text = repr(conn) + str(conn.model_dump())
    assert "AWS-SECRET" not in text
    assert "SESSION-TOKEN" not in text


def test_entry_repr_hides_secrets() -> None:
    entry = CSVEntry(connection=_gcs(keyfile_dict=SA), location="a.csv")
    assert "HMAC-SECRET" not in repr(entry)
    assert "PRIVATE-KEY-VALUE" not in repr(entry)


def test_get_connection_gives_usable_plain_values() -> None:
    entry = CSVEntry(connection=_gcs(keyfile_dict=SA), location="a.csv")
    conn = ConnectionLike(entry.get_connection())
    assert conn.password == "HMAC-SECRET"
    assert conn.extra_dejson["keyfile_dict"] == json.dumps(SA)


def test_connection_like_repr_hides_secrets() -> None:
    conn = ConnectionLike(
        {"conn_type": "aws", "password": "AWS-SECRET", "extra": {"k": "EXTRA-VALUE"}},
    )
    assert "AWS-SECRET" not in repr(conn)
    assert "EXTRA-VALUE" not in repr(conn)
