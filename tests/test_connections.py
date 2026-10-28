import pytest

from smallcat.catalog import CSVEntry
from smallcat.connections import (
    ConnectionLike,
    GoogleCloudPlatformConnectionSchema,
    LocalFsConnectionSchema,
)


@pytest.mark.parametrize("conn", [{"conn_type": "fs"}, {"conn_type": "fs", "extra": None}])
def test_connection_like_without_extra(conn):
    c = ConnectionLike(conn)
    assert c.extra is None
    assert c.extra_dejson == {}


def test_connection_like_extra_dict_and_json_string():
    assert ConnectionLike({"conn_type": "fs", "extra": {"a": 1}}).extra_dejson == {"a": 1}
    assert ConnectionLike({"conn_type": "fs", "extra": '{"a": 1}'}).extra_dejson == {"a": 1}


def test_connection_like_rejects_invalid_extra_type():
    with pytest.raises(TypeError, match="extra"):
        ConnectionLike({"conn_type": "fs", "extra": 42})


def test_schema_without_extra_builds_dataset(tmp_path):
    entry = CSVEntry(connection=LocalFsConnectionSchema(), location="x.csv", load_options=None, save_options=None)
    conn = entry.get_connection()
    assert conn["conn_type"] == "fs"
    assert ConnectionLike(conn).extra_dejson == {}


def test_schema_is_serialized_with_alias():
    schema = GoogleCloudPlatformConnectionSchema(schema="my_schema")
    entry = CSVEntry(connection=schema, location="x.csv", load_options=None, save_options=None)
    conn = entry.get_connection()
    assert conn["schema"] == "my_schema"
    assert "schema_" not in conn
    assert ConnectionLike(conn).schema == "my_schema"
