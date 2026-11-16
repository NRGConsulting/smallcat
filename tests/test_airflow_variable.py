"""Reading JSON Airflow Variables: which errors become a KeyError and which do not."""

import sys
import types
from typing import NoReturn

import pytest

from smallcat.catalog import Catalog
from smallcat.connections_config import Connections
from smallcat._airflow import get_variable_json


def _install_variable(monkeypatch, get):
    class Variable:
        pass

    Variable.get = staticmethod(get)  # type: ignore[attr-defined]
    sdk = types.ModuleType("airflow.sdk")
    sdk.Variable = Variable  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)


def test_a_json_string_is_parsed(monkeypatch):
    _install_variable(monkeypatch, lambda _id: '{"a": 1}')
    assert get_variable_json("v") == {"a": 1}


def test_an_already_decoded_value_is_returned_as_it_is(monkeypatch):
    _install_variable(monkeypatch, lambda _id: {"a": 1})
    assert get_variable_json("v") == {"a": 1}


def test_get_is_called_without_deserialize_json(monkeypatch):
    calls = []

    def get(*args, **kwargs):
        calls.append((args, kwargs))
        return "{}"

    _install_variable(monkeypatch, get)
    get_variable_json("v")
    assert calls == [(("v",), {})]


def test_the_airflow_3_missing_variable_error_is_a_key_error(monkeypatch):
    def get(_id) -> NoReturn:
        msg = "cannot import name 'SUPERVISOR_COMMS' from 'task_runner'"
        raise ImportError(msg)

    _install_variable(monkeypatch, get)
    with pytest.raises(KeyError, match="Variable v not found"):
        get_variable_json("v")


def _runtime_error(kind: str) -> Exception:
    """Build Airflow 3's own ``AirflowRuntimeError`` (needs no server)."""
    from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
    from airflow.sdk.execution_time.comms import ErrorResponse

    return AirflowRuntimeError(
        ErrorResponse(error=ErrorType[kind], detail={"message": "x"})
    )


def test_airflow_3_runtime_error_for_a_missing_variable_is_a_key_error(monkeypatch):
    pytest.importorskip("airflow.sdk.exceptions")
    error = _runtime_error("VARIABLE_NOT_FOUND")

    def get(_id) -> NoReturn:
        raise error

    _install_variable(monkeypatch, get)
    with pytest.raises(KeyError, match="Variable v not found"):
        get_variable_json("v")


@pytest.mark.parametrize("kind", ["PERMISSION_DENIED", "API_SERVER_ERROR", "GENERIC_ERROR"])
def test_other_airflow_3_runtime_errors_are_not_hidden(monkeypatch, kind):
    pytest.importorskip("airflow.sdk.exceptions")
    error = _runtime_error(kind)

    def get(_id) -> NoReturn:
        raise error

    _install_variable(monkeypatch, get)
    with pytest.raises(type(error)):
        get_variable_json("v")


def test_a_native_key_error_from_airflow_2_passes_through(monkeypatch):
    def get(_id) -> NoReturn:
        msg = "Variable v does not exist"
        raise KeyError(msg)

    _install_variable(monkeypatch, get)
    with pytest.raises(KeyError, match="does not exist"):
        get_variable_json("v")


def test_an_unrelated_import_error_is_not_hidden(monkeypatch):
    def get(_id) -> NoReturn:
        msg = "No module named 'airflow.providers.hashicorp'"
        raise ImportError(msg)

    _install_variable(monkeypatch, get)
    with pytest.raises(ImportError, match="hashicorp"):
        get_variable_json("v")


def test_an_unrelated_type_error_is_not_retried(monkeypatch):
    calls = []

    def get(_id) -> NoReturn:
        calls.append(_id)
        msg = "backend bug"
        raise TypeError(msg)

    _install_variable(monkeypatch, get)
    with pytest.raises(TypeError, match="backend bug"):
        get_variable_json("v")
    assert calls == ["v"]


def test_invalid_json_names_the_variable_but_not_its_content(monkeypatch):
    _install_variable(monkeypatch, lambda _id: "{password: s3cr3t")
    with pytest.raises(ValueError, match="Variable v") as exc:
        get_variable_json("v")
    assert "s3cr3t" not in str(exc.value)


@pytest.mark.parametrize("load", [Catalog.from_airflow_variable, Connections.from_airflow_variable])
def test_both_loaders_share_the_behaviour(monkeypatch, load):
    def get(_id) -> NoReturn:
        msg = "cannot import name 'SUPERVISOR_COMMS'"
        raise ImportError(msg)

    _install_variable(monkeypatch, get)
    with pytest.raises(KeyError, match="not found"):
        load("ghost")

    def broken(_id) -> NoReturn:
        msg = "No module named 'provider'"
        raise ImportError(msg)

    _install_variable(monkeypatch, broken)
    with pytest.raises(ImportError, match="provider"):
        load("ghost")
