import importlib
import subprocess
import sys

import pytest

import smallcat
import smallcat.models as models

PACKAGES = [smallcat, models]

REEXPORTED = {
    "smallcat": {
        "Catalog": "smallcat.catalog",
        "ConnectionNotFoundError": "smallcat.connections_config",
        "ConnectionNotLoadedError": "smallcat.connections_config",
        "ConnectionsError": "smallcat.connections_config",
        "ConnectionsValidationError": "smallcat.connections_config",
        "DuplicateConnectionError": "smallcat.connections_config",
    },
    "smallcat.models": {
        "ManagerDescriptor": "smallcat.models.query",
        "UnsupportedFieldTypeError": "smallcat.models.schema",
    },
}


@pytest.mark.parametrize("package", PACKAGES, ids=lambda p: p.__name__)
def test_every_name_in_all_resolves(package):
    for name in package.__all__:
        assert getattr(package, name) is not None, name


@pytest.mark.parametrize("package", PACKAGES, ids=lambda p: p.__name__)
def test_all_has_no_duplicates(package):
    assert len(set(package.__all__)) == len(package.__all__)


@pytest.mark.parametrize(
    ("package", "name", "module"),
    [(pkg, n, m) for pkg, items in REEXPORTED.items() for n, m in items.items()],
)
def test_names_are_reexported_from_their_defining_module(package, name, module):
    expected = getattr(importlib.import_module(module), name)
    assert getattr(importlib.import_module(package), name) is expected
    assert name in importlib.import_module(package).__all__


def test_importing_the_root_package_stays_light():
    code = (
        "import sys, smallcat;"
        "smallcat.Connections; smallcat.ConnectionsError;"
        "assert 'polars' not in sys.modules, 'polars imported';"
        "assert 'duckdb' not in sys.modules, 'duckdb imported'"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
