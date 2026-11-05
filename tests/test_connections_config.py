"""SC-035 phase 1: ``Connections`` is loaded and validated before it is used."""

import sys
import types
from pathlib import Path

import pytest

from smallcat import connections_config
from smallcat.connections import ConnectionLike
from smallcat.connections_config import (
    ConnectionNotFoundError,
    ConnectionNotLoadedError,
    Connections,
    ConnectionsValidationError,
    DuplicateConnectionError,
)

SECRET = "super-secret-value"  # noqa: S105

DEFINITIONS = {
    "connections": {
        "local": {"conn_type": "fs", "extra": {"base_path": "/data"}},
        "lake": {
            "conn_type": "google_cloud_platform",
            "extra": {"bucket": "b"},
        },
        "s3": {
            "conn_type": "aws",
            "login": "key",
            "password": SECRET,
            "extra": {"bucket": "b"},
        },
    },
}


class FakeAirflowConnection:
    def __init__(self, conn_type: str, extra: str | None = None) -> None:
        self.conn_type = conn_type
        self.host = None
        self.schema = None
        self.login = None
        self.password = SECRET
        self.extra = extra

    @property
    def extra_dejson(self) -> dict:
        import json

        return json.loads(self.extra) if self.extra else {}


class AirflowNotFoundException(Exception):  # noqa: N818
    """Same name as Airflow's, which is what the loader recognises."""


class FakeHook:
    def __init__(self, connections: dict) -> None:
        self.connections = connections

    def get_connection(self, conn_id: str):
        if conn_id not in self.connections:
            raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")
        return self.connections[conn_id]


@pytest.fixture
def fake_hook(monkeypatch):
    def install(connections: dict) -> FakeHook:
        hook = FakeHook(connections)
        monkeypatch.setattr(connections_config, "get_base_hook", lambda: hook)
        return hook

    return install


# ---------- from_dict / from_yaml ----------


def test_from_dict_loads_and_validates_every_connection():
    conns = Connections.from_dict(DEFINITIONS)
    assert conns.names() == ["local", "lake", "s3"]
    assert "local" in conns
    assert "nope" not in conns
    assert len(conns) == 3
    assert list(conns) == ["local", "lake", "s3"]


def test_get_returns_a_plain_dict_with_secrets_revealed():
    conn = Connections.from_dict(DEFINITIONS).get("s3")
    assert conn["conn_type"] == "aws"
    assert conn["password"] == SECRET
    # a dict the datasets know how to wrap
    assert ConnectionLike(conn).extra_dejson["bucket"] == "b"


def test_google_alias_is_accepted():
    conns = Connections.from_dict(
        {"connections": {"g": {"conn_type": "google", "extra": {"bucket": "b"}}}},
    )
    assert conns.get("g")["conn_type"] == "google"


def test_get_unknown_name_lists_what_is_available():
    conns = Connections.from_dict(DEFINITIONS)
    with pytest.raises(ConnectionNotLoadedError) as exc:
        conns.get("other")
    assert "'other'" in str(exc.value)
    assert "local, lake, s3" in str(exc.value)


def test_get_on_empty_connections_says_none_are_available():
    with pytest.raises(ConnectionNotLoadedError, match="available: none"):
        Connections().get("x")


def test_unknown_key_in_a_connection_is_rejected():
    definitions = {
        "connections": {"local": {"conn_type": "fs", "extra": {"base_pth": "/d"}}},
    }
    with pytest.raises(ConnectionsValidationError, match=r"local\.extra\.base_pth"):
        Connections.from_dict(definitions)


def test_unknown_conn_type_is_rejected():
    with pytest.raises(ConnectionsValidationError, match="ftp"):
        Connections.from_dict({"connections": {"x": {"conn_type": "ftp"}}})


def test_all_problems_are_reported_together():
    definitions = {
        "connections": {
            "a": {"conn_type": "fs", "extra": {"oops": 1}},
            "b": {"conn_type": "ftp"},
            "c": "not a mapping",
        },
    }
    with pytest.raises(ConnectionsValidationError) as exc:
        Connections.from_dict(definitions)
    message = str(exc.value)
    assert message.count("\n  - ") >= 3
    assert "a.extra.oops" in message
    assert "b" in message
    assert "c: the definition must be a mapping" in message


def test_validation_errors_never_contain_the_values_provided():
    definitions = {
        "connections": {
            "s3": {
                "conn_type": "aws",
                "login": "key",
                "password": SECRET,
                "extra": {"bucket": "b", "unknown_option": SECRET},
            },
        },
    }
    with pytest.raises(ConnectionsValidationError) as exc:
        Connections.from_dict(definitions)
    assert SECRET not in str(exc.value)
    assert SECRET not in repr(exc.value)


def test_repr_shows_names_and_types_but_no_content():
    text = repr(Connections.from_dict(DEFINITIONS))
    assert "local:fs" in text
    assert "s3:aws" in text
    assert SECRET not in text


@pytest.mark.parametrize(
    "content",
    [None, [], "text", {}, {"connections": []}, {"connections": 1}],
)
def test_wrong_structure_is_rejected(content):
    with pytest.raises(ConnectionsValidationError):
        Connections.from_dict(content)


def test_extra_top_level_keys_are_rejected():
    with pytest.raises(ConnectionsValidationError, match="entries"):
        Connections.from_dict({"connections": {}, "entries": {}})


def test_empty_connection_name_is_rejected():
    with pytest.raises(ConnectionsValidationError, match="non-empty"):
        Connections.from_dict({"connections": {"": {"conn_type": "fs"}}})


def test_from_yaml(tmp_path: Path):
    path = tmp_path / "connections.yaml"
    path.write_text(
        "connections:\n"
        "  local:\n"
        "    conn_type: fs\n"
        "    extra:\n"
        "      base_path: /data\n",
    )
    assert Connections.from_yaml(path).names() == ["local"]


def test_from_yaml_accepts_a_string_path(tmp_path: Path):
    path = tmp_path / "c.yaml"
    path.write_text("connections:\n  local:\n    conn_type: fs\n")
    assert "local" in Connections.from_yaml(str(path))


def test_from_yaml_missing_file():
    with pytest.raises(FileNotFoundError):
        Connections.from_yaml("does-not-exist.yaml")


def test_from_yaml_empty_file_is_rejected(tmp_path: Path):
    path = tmp_path / "empty.yaml"
    path.write_text("")
    with pytest.raises(ConnectionsValidationError):
        Connections.from_yaml(path)


# ---------- from_airflow ----------


def test_from_airflow_resolves_and_validates_ids(fake_hook):
    fake_hook(
        {
            "lake": FakeAirflowConnection("google_cloud_platform", '{"bucket": "b"}'),
            "local": FakeAirflowConnection("fs"),
        },
    )
    conns = Connections.from_airflow(["lake", "local"])
    assert conns.names() == ["lake", "local"]
    assert conns.get("lake").extra_dejson == {"bucket": "b"}


def test_from_airflow_ignores_repeated_ids(fake_hook):
    fake_hook({"local": FakeAirflowConnection("fs")})
    assert Connections.from_airflow(["local", "local"]).names() == ["local"]


def test_from_airflow_missing_id_fails_when_loading(fake_hook):
    fake_hook({"local": FakeAirflowConnection("fs")})
    with pytest.raises(ConnectionNotFoundError, match="'ghost'"):
        Connections.from_airflow(["local", "ghost"])


def test_from_airflow_missing_id_hides_the_original_error(fake_hook):
    fake_hook({})
    with pytest.raises(ConnectionNotFoundError) as exc:
        Connections.from_airflow(["ghost"])
    assert exc.value.__cause__ is None


def test_from_airflow_unsupported_conn_type(fake_hook):
    fake_hook({"db": FakeAirflowConnection("postgres")})
    with pytest.raises(ConnectionsValidationError, match="postgres"):
        Connections.from_airflow(["db"])


def test_from_airflow_extra_must_be_json(fake_hook):
    fake_hook({"local": FakeAirflowConnection("fs", "{not json")})
    with pytest.raises(ConnectionsValidationError, match="local.extra"):
        Connections.from_airflow(["local"])


def test_from_airflow_reports_every_invalid_connection(fake_hook):
    fake_hook(
        {
            "a": FakeAirflowConnection("postgres"),
            "b": FakeAirflowConnection("fs", "{bad"),
        },
    )
    with pytest.raises(ConnectionsValidationError) as exc:
        Connections.from_airflow(["a", "b"])
    assert "a.conn_type" in str(exc.value)
    assert "b.extra" in str(exc.value)
    assert SECRET not in str(exc.value)


def test_from_airflow_rejects_a_bare_string(fake_hook):
    fake_hook({"local": FakeAirflowConnection("fs")})
    with pytest.raises(TypeError, match="list"):
        Connections.from_airflow("local")


def test_from_airflow_without_airflow(monkeypatch):
    monkeypatch.setattr(connections_config, "get_base_hook", lambda: None)
    with pytest.raises(ImportError, match="Airflow is required"):
        Connections.from_airflow(["x"])


def test_from_airflow_does_not_hide_unrelated_errors(monkeypatch):
    class BrokenHook:
        @staticmethod
        def get_connection(conn_id: str):
            msg = "metadata database is down"
            raise RuntimeError(msg)

    monkeypatch.setattr(connections_config, "get_base_hook", lambda: BrokenHook)
    with pytest.raises(RuntimeError, match="database is down"):
        Connections.from_airflow(["x"])


def test_from_airflow_variable(monkeypatch):
    class Variable:
        @staticmethod
        def get(variable_id: str, deserialize_json: bool = False):  # noqa: FBT001, FBT002
            assert variable_id == "my_connections"
            return DEFINITIONS

    sdk = types.ModuleType("airflow.sdk")
    sdk.Variable = Variable  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    assert Connections.from_airflow_variable("my_connections").names() == [
        "local",
        "lake",
        "s3",
    ]


def test_from_airflow_variable_missing(monkeypatch):
    class Variable:
        @staticmethod
        def get(variable_id: str, deserialize_json: bool = False):  # noqa: FBT001, FBT002
            raise ImportError

    sdk = types.ModuleType("airflow.sdk")
    sdk.Variable = Variable  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    with pytest.raises(KeyError, match="not found"):
        Connections.from_airflow_variable("ghost")


# ---------- update ----------


def test_update_adds_connections():
    first = Connections.from_dict(
        {"connections": {"a": {"conn_type": "fs"}}},
    )
    second = Connections.from_dict(
        {"connections": {"b": {"conn_type": "fs"}}},
    )
    first.update(second)
    assert first.names() == ["a", "b"]


def test_update_with_a_duplicate_name_adds_nothing():
    first = Connections.from_dict(
        {"connections": {"a": {"conn_type": "fs"}}},
    )
    second = Connections.from_dict(
        {"connections": {"b": {"conn_type": "fs"}, "a": {"conn_type": "fs"}}},
    )
    with pytest.raises(DuplicateConnectionError, match="a"):
        first.update(second)
    assert first.names() == ["a"]
