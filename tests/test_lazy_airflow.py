import subprocess
import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from smallcat import _airflow
from smallcat.catalog import CSVEntry
from smallcat.datasets.csv_dataset import CSVDataset


@pytest.mark.parametrize(
    "module",
    ["smallcat", "smallcat.catalog", "smallcat.datasets.base_dataset"],
)
def test_importing_smallcat_does_not_import_airflow(module):
    code = f"import sys, {module}; assert 'airflow' not in sys.modules, 'airflow imported'"
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _fake_module(name, **attrs):
    module = ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


def test_get_base_hook_prefers_the_airflow_3_sdk():
    sdk_hook, legacy_hook = object(), object()
    modules = {
        "airflow": _fake_module("airflow"),
        "airflow.sdk": _fake_module("airflow.sdk", BaseHook=sdk_hook),
        "airflow.hooks": _fake_module("airflow.hooks"),
        "airflow.hooks.base": _fake_module("airflow.hooks.base", BaseHook=legacy_hook),
    }
    with patch.dict(sys.modules, modules):
        assert _airflow.get_base_hook() is sdk_hook


def test_get_base_hook_falls_back_to_airflow_2():
    legacy_hook = object()
    modules = {
        "airflow": _fake_module("airflow"),
        "airflow.sdk": None,  # makes `from airflow.sdk import ...` raise ImportError
        "airflow.hooks": _fake_module("airflow.hooks"),
        "airflow.hooks.base": _fake_module("airflow.hooks.base", BaseHook=legacy_hook),
    }
    with patch.dict(sys.modules, modules):
        assert _airflow.get_base_hook() is legacy_hook


def test_get_base_hook_returns_none_without_airflow():
    modules = {"airflow": None, "airflow.sdk": None, "airflow.hooks.base": None}
    with patch.dict(sys.modules, modules):
        assert _airflow.get_base_hook() is None


def test_from_conn_id_uses_the_resolved_hook(local_conn):
    hook = MagicMock()
    hook.get_connection.return_value = MagicMock(
        conn_type="fs", host=None, schema=None, login=None, password=None,
        extra=None, extra_dejson={},
    )
    with patch("smallcat.datasets.base_dataset.get_base_hook", return_value=hook):
        ds = CSVDataset.from_conn_id("my_conn")
    hook.get_connection.assert_called_once_with("my_conn")
    assert isinstance(ds, CSVDataset)


def test_from_conn_id_without_airflow_raises_runtime_error():
    with (
        patch("smallcat.datasets.base_dataset.get_base_hook", return_value=None),
        pytest.raises(RuntimeError, match="Airflow not available"),
    ):
        CSVDataset.from_conn_id("my_conn")


def test_entry_resolves_connection_id_through_the_hook():
    hook = MagicMock()
    entry = CSVEntry(
        connection="my_conn", location="x.csv", load_options=None, save_options=None,
    )
    with patch("smallcat.catalog.get_base_hook", return_value=hook):
        assert entry.get_connection() is hook.get_connection.return_value
    hook.get_connection.assert_called_once_with(conn_id="my_conn")


def test_entry_without_airflow_raises_import_error():
    entry = CSVEntry(
        connection="my_conn", location="x.csv", load_options=None, save_options=None,
    )
    with (
        patch("smallcat.catalog.get_base_hook", return_value=None),
        pytest.raises(ImportError, match="Airflow is required"),
    ):
        entry.get_connection()
