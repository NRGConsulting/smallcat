"""Named connections loaded from YAML or Airflow.

:class:`Connections` is the single place where connections are defined. It is
loaded **before** anything uses it (a :class:`~smallcat.catalog.Catalog` or a
model registry) and every connection is validated while it loads, so a missing
or malformed connection fails at start-up instead of on the first read.

Connections can only come from a YAML file, a dictionary, an Airflow Variable or
Airflow connection ids. They are never passed inline to the objects that use
them, which keeps credentials out of application code.

Example:
  >>> conns = Connections.from_yaml("connections.yaml")
  >>> conns = Connections.from_airflow(["datalake_gcs"])
  >>> conns.get("datalake_gcs")
"""

import json
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Annotated, Any, get_args

import yaml
from pydantic import Field, TypeAdapter, ValidationError

from smallcat._airflow import AIRFLOW_INSTALL_HINT, get_base_hook
from smallcat.connections import (
    BaseConnectionSchema,
    ConnectionProtocol,
    SupportedConnectionSchemas,
    reveal_secrets,
)


class ConnectionsError(Exception):
    """Base class of the errors raised by :class:`Connections`."""


class ConnectionsValidationError(ConnectionsError, ValueError):
    """The connections definition is invalid.

    The message lists every problem found, by connection name and field. It
    never includes the values that were provided, so it cannot leak secrets.
    """


class ConnectionNotFoundError(ConnectionsError, LookupError):
    """An Airflow connection id does not exist."""


class ConnectionNotLoadedError(ConnectionsError, LookupError):
    """A connection name was used before being loaded into :class:`Connections`."""


class DuplicateConnectionError(ConnectionsError, ValueError):
    """Two connections with the same name were loaded."""


def _supported_conn_types() -> frozenset[str]:
    """Collect the ``conn_type`` values of every supported connection schema."""
    types: set[str] = set()
    for schema in get_args(SupportedConnectionSchemas):
        types.update(get_args(schema.model_fields["conn_type"].annotation))
    return frozenset(types)


SUPPORTED_CONN_TYPES = _supported_conn_types()
"""``conn_type`` values that smallcat knows how to use."""

_CONNECTION_ADAPTER: TypeAdapter[Any] = TypeAdapter(
    Annotated[SupportedConnectionSchemas, Field(discriminator="conn_type")],
)

_AIRFLOW_NOT_FOUND = (ImportError, KeyError, LookupError)


def _format_errors(name: str, error: ValidationError) -> list[str]:
    """Describe a validation error without echoing the input values."""
    lines = []
    for err in error.errors(
        include_input=False,
        include_url=False,
        include_context=False,
    ):
        parts = [str(part) for part in err["loc"]]
        if parts and parts[0] in SUPPORTED_CONN_TYPES:
            parts.pop(0)  # the union tag, which is not part of the user's YAML
        location = ".".join([name, *parts])
        lines.append(f"{location}: {err['msg']}")
    return lines


class Connections:
    """A set of named, validated connections.

    Attributes are not exposed: use :meth:`get`, :meth:`names` and the
    container protocol (``in``, ``len``, iteration over names).
    """

    def __init__(self) -> None:
        """Create an empty set of connections."""
        self._items: dict[str, SupportedConnectionSchemas | ConnectionProtocol] = {}

    # ---------- Loaders ----------

    @classmethod
    def from_dict(cls, dictionary: Mapping[str, Any]) -> "Connections":
        """Create connections from a dictionary.

        Args:
            dictionary: A mapping with a single ``connections`` key that maps
                each connection name to its definition. Each definition is
                validated against the schema selected by its ``conn_type``
                (``fs``, ``google_cloud_platform``, ``aws``).

        Returns:
            The validated connections.

        Raises:
            ConnectionsValidationError: If the structure, a name or any
                connection is invalid. All problems are reported together.
        """
        if not isinstance(dictionary, Mapping):
            msg = "Connections must be a mapping with a 'connections' key"
            raise ConnectionsValidationError(msg)
        unknown = set(dictionary) - {"connections"}
        if unknown or "connections" not in dictionary:
            msg = "Connections must be a mapping with a single 'connections' key" + (
                f"; unexpected keys: {sorted(map(str, unknown))}" if unknown else ""
            )
            raise ConnectionsValidationError(msg)
        raw = dictionary["connections"]
        if not isinstance(raw, Mapping):
            msg = "'connections' must map each connection name to its definition"
            raise ConnectionsValidationError(msg)

        result = cls()
        problems: list[str] = []
        for name, definition in raw.items():
            if not isinstance(name, str) or not name:
                problems.append(f"{name!r}: connection names must be non-empty strings")
                continue
            if not isinstance(definition, Mapping):
                problems.append(f"{name}: the definition must be a mapping")
                continue
            try:
                result._items[name] = _CONNECTION_ADAPTER.validate_python(definition)
            except ValidationError as e:
                problems.extend(_format_errors(name, e))
        if problems:
            raise ConnectionsValidationError(
                "Invalid connections:\n  - " + "\n  - ".join(problems),
            )
        return result

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Connections":
        """Create connections from a YAML file.

        Args:
            path: Path to a YAML file with a ``connections`` mapping.

        Returns:
            The validated connections.

        Raises:
            FileNotFoundError: If the file does not exist.
            ConnectionsValidationError: If the content is invalid.
        """
        with Path(path).open() as f:
            content = yaml.safe_load(f)
        return cls.from_dict(content)

    @classmethod
    def from_airflow_variable(cls, variable_id: str) -> "Connections":
        """Create connections from an Airflow Variable containing JSON.

        The Variable holds the same structure as the YAML file.

        Args:
            variable_id: The Airflow Variable ID to read (expects JSON).

        Returns:
            The validated connections.

        Raises:
            KeyError: If the Airflow Variable does not exist.
            ConnectionsValidationError: If the content is invalid.
        """
        try:
            from airflow.sdk import Variable
        except ImportError:
            from airflow.models import Variable  # type: ignore[attr-defined,no-redef]

        try:
            content = Variable.get(variable_id, deserialize_json=True)
        except TypeError:
            content = Variable.get(variable_id)
        except ImportError as e:
            msg = f"Variable {variable_id} not found in Airflow"
            raise KeyError(msg) from e
        return cls.from_dict(content)

    @classmethod
    def from_airflow(cls, conn_ids: Iterable[str]) -> "Connections":
        """Create connections by resolving Airflow connection ids.

        Each connection is resolved with Airflow's ``BaseHook`` and checked
        right away, so a missing id fails here and not on the first read. The
        name of each connection is its Airflow connection id.

        Args:
            conn_ids: The Airflow connection ids to load.

        Returns:
            The validated connections.

        Raises:
            ImportError: If Airflow is not installed.
            ConnectionNotFoundError: If an id does not exist in Airflow.
            ConnectionsValidationError: If a connection has an unsupported
                ``conn_type`` or an ``extra`` that is not valid JSON.
        """
        if isinstance(conn_ids, str):
            msg = "conn_ids must be a list of connection ids, not a single string"
            raise TypeError(msg)
        base_hook = get_base_hook()
        if base_hook is None:
            msg = (
                f"Airflow is required to load connections by id. {AIRFLOW_INSTALL_HINT}"
            )
            raise ImportError(msg)

        result = cls()
        problems: list[str] = []
        for conn_id in dict.fromkeys(conn_ids):
            try:
                conn = base_hook.get_connection(conn_id)
            except Exception as e:
                if not (
                    isinstance(e, _AIRFLOW_NOT_FOUND)
                    or type(e).__name__ == "AirflowNotFoundException"
                ):
                    raise
                msg = f"Airflow connection '{conn_id}' does not exist"
                raise ConnectionNotFoundError(msg) from None
            problem = cls._check_airflow_connection(conn_id, conn)
            if problem:
                problems.append(problem)
                continue
            result._items[conn_id] = conn
        if problems:
            raise ConnectionsValidationError(
                "Invalid connections:\n  - " + "\n  - ".join(problems),
            )
        return result

    @staticmethod
    def _check_airflow_connection(name: str, conn: ConnectionProtocol) -> str | None:
        """Return a description of what is wrong with a connection, if anything."""
        conn_type = (conn.conn_type or "").lower()
        if conn_type not in SUPPORTED_CONN_TYPES:
            return (
                f"{name}.conn_type: '{conn.conn_type}' is not supported "
                f"(expected one of {sorted(SUPPORTED_CONN_TYPES)})"
            )
        try:
            conn.extra_dejson  # noqa: B018
        except (json.JSONDecodeError, TypeError):
            return f"{name}.extra: is not valid JSON"
        return None

    # ---------- Access ----------

    def get(self, name: str) -> dict | ConnectionProtocol:
        """Return a connection ready to be handed to a dataset.

        Connections defined in YAML or a dictionary are returned as a plain
        dictionary (with their secrets revealed); Airflow connections are
        returned as the Airflow object.

        Args:
            name: The connection name.

        Returns:
            The connection.

        Raises:
            ConnectionNotLoadedError: If no connection has that name.
        """
        try:
            item = self._items[name]
        except KeyError:
            available = ", ".join(self._items) or "none"
            msg = f"Connection '{name}' is not loaded (available: {available})"
            raise ConnectionNotLoadedError(msg) from None
        if isinstance(item, BaseConnectionSchema):
            return reveal_secrets(item.model_dump(by_alias=True, exclude_none=True))
        return item

    def names(self) -> list[str]:
        """Return the names of the loaded connections."""
        return list(self._items)

    def update(self, other: "Connections") -> None:
        """Add the connections of another instance.

        Args:
            other: The connections to add.

        Raises:
            DuplicateConnectionError: If a name is already loaded. Nothing is
                added in that case.
        """
        duplicated = sorted(set(self._items) & set(other._items))  # noqa: SLF001
        if duplicated:
            msg = f"Connections already loaded: {', '.join(duplicated)}"
            raise DuplicateConnectionError(msg)
        self._items.update(other._items)  # noqa: SLF001

    def __contains__(self, name: object) -> bool:
        """Return whether a connection with this name is loaded."""
        return name in self._items

    def __len__(self) -> int:
        """Return the number of loaded connections."""
        return len(self._items)

    def __iter__(self) -> Iterator[str]:
        """Iterate over the connection names."""
        return iter(self._items)

    def __repr__(self) -> str:
        """Represent the connections by name and type, never by content."""
        parts = ", ".join(
            f"{name}:{item.conn_type}" for name, item in self._items.items()
        )
        return f"Connections({parts})"


__all__ = [
    "SUPPORTED_CONN_TYPES",
    "ConnectionNotFoundError",
    "ConnectionNotLoadedError",
    "Connections",
    "ConnectionsError",
    "ConnectionsValidationError",
    "DuplicateConnectionError",
]
