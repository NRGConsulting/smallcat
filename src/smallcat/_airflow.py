"""Lazy access to Airflow, which is an optional dependency of Smallcat.

Importing Airflow is slow (hundreds of modules) and can have side effects, so
it must never happen when ``smallcat`` is imported. Callers resolve it on
demand through :func:`get_base_hook`.
"""

from typing import Any

AIRFLOW_INSTALL_HINT = (
    "Install it with `pip install smallcat[airflow]`, or use the Apache Airflow "
    "already available in your environment."
)
"""Appended to the errors raised when an operation needs Airflow."""


def get_base_hook() -> Any:  # noqa: ANN401
    """Return Airflow's ``BaseHook`` class, or ``None`` if Airflow is missing.

    Tries the Airflow 3 SDK first and falls back to the Airflow 2 location, so
    the same import path is used everywhere in Smallcat. Airflow is imported
    on the first call, not when Smallcat is imported.

    Returns:
        The ``BaseHook`` class, or ``None`` when Airflow is not installed.
    """
    try:
        from airflow.sdk import BaseHook  # type: ignore[attr-defined]
    except ImportError:
        try:
            from airflow.hooks.base import BaseHook  # type: ignore[attr-defined,no-redef]
        except ImportError:
            return None
    return BaseHook
