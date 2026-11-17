"""#31: the published name is written once in code and matches the packaging."""

import re
import tomllib
from pathlib import Path

import pytest

from smallcat._airflow import AIRFLOW_INSTALL_HINT, DISTRIBUTION_NAME

ROOT = Path(__file__).parent.parent
PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
INSTALL = re.compile(r"(?:pip install|uv add)\s+\"?(smallcat[\w-]*)")


def test_the_constant_matches_the_name_in_pyproject():
    assert DISTRIBUTION_NAME == PYPROJECT["project"]["name"]


def test_the_airflow_hint_uses_the_distribution_name():
    assert f"pip install {DISTRIBUTION_NAME}[airflow]" in AIRFLOW_INSTALL_HINT


def test_the_importable_package_is_not_renamed():
    """Only the distribution is called smallcat-chonk; ``import smallcat`` stays."""
    wheel = PYPROJECT["tool"]["hatch"]["build"]["targets"]["wheel"]
    assert wheel["packages"] == ["src/smallcat"]
    assert (ROOT / "src" / "smallcat" / "__init__.py").exists()


@pytest.mark.parametrize(
    "path",
    [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md"))],
    ids=lambda p: p.relative_to(ROOT).as_posix(),
)
def test_install_instructions_name_the_published_distribution(path):
    names = INSTALL.findall(path.read_text(encoding="utf-8"))
    assert set(names) <= {DISTRIBUTION_NAME}, names


def test_the_readme_warns_not_to_install_the_original_next_to_it():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "Do not install it next to the original `smallcat`" in readme


def test_the_project_links_point_at_the_fork():
    urls = PYPROJECT["project"]["urls"]
    assert set(urls) == {"Homepage", "Documentation", "Repository", "Issues"}
    assert urls["Repository"] == "https://github.com/NRGConsulting/smallcat"
    assert urls["Issues"].startswith(urls["Repository"])
