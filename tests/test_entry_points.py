import importlib
import tomllib
from pathlib import Path

import pytest

PYPROJECT = Path(__file__).parent.parent / "pyproject.toml"
SCRIPTS = tomllib.loads(PYPROJECT.read_text(encoding="utf-8")).get("project", {}).get(
    "scripts",
    {},
)


@pytest.mark.parametrize(("name", "target"), SCRIPTS.items())
def test_declared_entry_points_exist(name, target):
    module_name, _, attr = target.partition(":")
    module = importlib.import_module(module_name)
    assert callable(getattr(module, attr)), f"{name} -> {target} is not callable"
