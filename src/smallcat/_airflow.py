"""Lazy access to Airflow, which is an optional dependency of Smallcat.

Importing Airflow is slow (hundreds of modules) and can have side effects, so
it must never happen when ``smallcat`` is imported. Callers resolve it on
demand through :func:`get_base_hook`.
"""

import json
from typing import Any

DISTRIBUTION_NAME = "smallcat-chonk"
"""The name this package is published under (``pip install smallcat-chonk``).

It is not the name to import: the package stays ``smallcat``. A test checks that
it matches ``name`` in ``pyproject.toml``, so a rename cannot leave it behind.
"""

AIRFLOW_INSTALL_HINT = (
    f"Install it with `pip install {DISTRIBUTION_NAME}[airflow]`, or use the "
    "Apache Airflow already available in your environment."
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


_MISSING_VARIABLE_MARKER = "SUPERVISOR_COMMS"
"""Early Airflow 3 releases raise an ``ImportError`` naming this symbol when a
Variable does not exist outside a task runner: they try to reach the task
supervisor over a channel that is not there. Not verified against those releases;
the symbol comes from how they were reported."""


def _is_variable_not_found(error: Exception) -> bool:
    """Tell whether ``error`` is Airflow 3's "this Variable does not exist".

    Recent Airflow 3 releases (checked against 3.3.2) raise
    ``AirflowRuntimeError`` with ``error.error == ErrorType.VARIABLE_NOT_FOUND``.
    The error is recognised by its shape instead of by importing
    ``airflow.sdk.exceptions``, which does not exist in Airflow 2.
    """
    if type(error).__name__ != "AirflowRuntimeError":
        return False
    kind = getattr(getattr(error, "error", None), "error", None)
    return getattr(kind, "name", None) == "VARIABLE_NOT_FOUND"


def get_variable_json(variable_id: str) -> Any:  # noqa: ANN401
    """Return the content of an Airflow Variable holding JSON.

    The Variable is read without asking Airflow to deserialize it, so a secrets
    backend that already returns an object (for example
    ``LocalFilesystemBackend``) works too: a string is parsed as JSON and
    anything else is returned as it is.

    Args:
        variable_id: The Airflow Variable ID to read.

    Returns:
        The decoded content of the Variable.

    Raises:
        KeyError: If the Variable does not exist. Only the errors Airflow 3
            raises for a missing Variable are turned into a ``KeyError``;
            any other error is left as it is.
        ValueError: If the Variable is not valid JSON. The message names the
            Variable but never quotes its content, which may hold secrets.
        ImportError: If Airflow, or something it needs, cannot be imported.
    """
    try:
        from airflow.sdk import Variable
    except ImportError:
        from airflow.models import Variable  # type: ignore[attr-defined,no-redef]

    try:
        value = Variable.get(variable_id)
    except Exception as e:
        missing = _is_variable_not_found(e) or (
            isinstance(e, ImportError) and _MISSING_VARIABLE_MARKER in str(e)
        )
        if not missing:
            raise
        msg = f"Variable {variable_id} not found in Airflow"
        raise KeyError(msg) from e

    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            msg = f"Variable {variable_id} in Airflow is not valid JSON ({e.msg})"
            raise ValueError(msg) from None
    return value
