"""SC-035 phase 2: ``Catalog`` resolves connection names against ``Connections``."""

import sys
import types
import warnings

import polars as pl
import pytest

from smallcat import catalog as catalog_module
from smallcat.catalog import Catalog, CSVEntry
from smallcat.connections_config import ConnectionNotLoadedError, Connections


@pytest.fixture
def conns(tmp_path) -> Connections:
    return Connections.from_dict(
        {
            "connections": {
                "local": {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}},
                "other": {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}},
            },
        },
    )


def _catalog_dict(connection="local", file_format="csv", **entry):
    return {
        "entries": {
            "people": {
                "file_format": file_format,
                "connection": connection,
                "location": "people.csv",
                **entry,
            },
        },
    }


def test_names_are_resolved_against_the_loaded_connections(conns):
    catalog = Catalog.from_dict(_catalog_dict(), connections=conns)
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    catalog.save_polars("people", df)
    assert catalog.load_polars("people").equals(df)


def test_get_connection_comes_from_connections(conns):
    catalog = Catalog.from_dict(_catalog_dict(), connections=conns)
    assert catalog.entries["people"].get_connection() == conns.get("local")


def test_a_missing_name_fails_when_the_catalog_is_loaded(conns):
    with pytest.raises(ConnectionNotLoadedError) as exc:
        Catalog.from_dict(_catalog_dict("ghost"), connections=conns)
    message = str(exc.value)
    assert "entry 'people' uses 'ghost'" in message
    assert "available: local, other" in message


def test_every_missing_entry_is_listed(conns):
    dictionary = {
        "entries": {
            "a": {"file_format": "csv", "connection": "x", "location": "a.csv"},
            "b": {"file_format": "csv", "connection": "local", "location": "b.csv"},
            "c": {"file_format": "csv", "connection": "y", "location": "c.csv"},
        },
    }
    with pytest.raises(ConnectionNotLoadedError) as exc:
        Catalog.from_dict(dictionary, connections=conns)
    message = str(exc.value)
    assert "entry 'a' uses 'x'" in message
    assert "entry 'c' uses 'y'" in message
    assert "entry 'b'" not in message


def test_nothing_is_bound_when_one_name_is_missing(conns):
    catalog = Catalog.model_validate(
        {
            "entries": {
                "a": {"file_format": "csv", "connection": "local", "location": "a"},
                "b": {"file_format": "csv", "connection": "ghost", "location": "b"},
            },
        },
    )
    with pytest.raises(ConnectionNotLoadedError):
        catalog.bind_connections(conns)
    assert catalog.entries["a"]._connections is None  # noqa: SLF001


def test_without_connections_names_still_go_through_airflow(monkeypatch):
    airflow_conn = object()

    class Hook:
        @staticmethod
        def get_connection(conn_id: str):
            assert conn_id == "airflow_id"
            return airflow_conn

    monkeypatch.setattr(catalog_module, "get_base_hook", lambda: Hook)
    catalog = Catalog.from_dict(_catalog_dict("airflow_id"))
    assert catalog.entries["people"].get_connection() is airflow_conn


def test_bound_names_never_reach_airflow(conns, monkeypatch):
    def fail():
        msg = "Airflow must not be used when connections are bound"
        raise AssertionError(msg)

    monkeypatch.setattr(catalog_module, "get_base_hook", fail)
    catalog = Catalog.from_dict(_catalog_dict(), connections=conns)
    assert catalog.entries["people"].get_connection()["conn_type"] == "fs"


def test_binding_none_goes_back_to_airflow(conns, monkeypatch):
    sentinel = object()

    class Hook:
        @staticmethod
        def get_connection(conn_id: str):
            return sentinel

    monkeypatch.setattr(catalog_module, "get_base_hook", lambda: Hook)
    catalog = Catalog.from_dict(_catalog_dict(), connections=conns)
    catalog.bind_connections(None)
    assert catalog.entries["people"].get_connection() is sentinel


def test_from_yaml_accepts_connections(conns, tmp_path):
    path = tmp_path / "catalog.yaml"
    path.write_text(
        "entries:\n"
        "  people:\n"
        "    file_format: csv\n"
        "    connection: local\n"
        "    location: people.csv\n",
    )
    catalog = Catalog.from_yaml(path, connections=conns)
    assert catalog.entries["people"].get_connection() == conns.get("local")
    with pytest.raises(ConnectionNotLoadedError):
        Catalog.from_yaml(path, connections=Connections())


def test_from_airflow_variable_accepts_connections(conns, monkeypatch):
    class Variable:
        @staticmethod
        def get(variable_id: str, deserialize_json: bool = False):  # noqa: FBT001, FBT002
            return _catalog_dict()

    sdk = types.ModuleType("airflow.sdk")
    sdk.Variable = Variable  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    catalog = Catalog.from_airflow_variable("cat", connections=conns)
    assert catalog.entries["people"].get_connection() == conns.get("local")
    with pytest.raises(ConnectionNotLoadedError):
        Catalog.from_airflow_variable("cat", connections=Connections())


def test_an_entry_built_in_python_can_be_bound(conns):
    entry = CSVEntry(connection="local", location="p.csv")
    entry.bind_connections(conns)
    assert entry.get_connection() == conns.get("local")
    with pytest.raises(ConnectionNotLoadedError):
        CSVEntry(connection="ghost", location="p.csv").bind_connections(conns)


def test_inline_connections_are_deprecated(tmp_path):
    inline = {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}}
    with pytest.warns(DeprecationWarning, match=r"\['people'\]"):
        catalog = Catalog.from_dict(_catalog_dict(inline))
    # still works: deprecated, not removed
    df = pl.DataFrame({"id": [1]})
    catalog.save_polars("people", df)
    assert catalog.load_polars("people").equals(df)


def test_inline_connections_warn_even_with_connections(conns, tmp_path):
    inline = {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}}
    with pytest.warns(DeprecationWarning):
        Catalog.from_dict(_catalog_dict(inline), connections=conns)


def test_named_connections_do_not_warn(conns):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        Catalog.from_dict(_catalog_dict(), connections=conns)
