"""Connection interface abstractions with optional Airflow compatibility.

This module defines a lightweight protocol for connection objects and a small
adapter (:class:`ConnectionLike`) that lets you supply a plain `dict` with
Airflow-compatible fields while still satisfying the protocol expected by the
rest of the library.

Smallcat can operate with or without Airflow installed. When Airflow is
available, objects implementing the same attributes (e.g., `BaseConnection`)
are compatible with :class:`ConnectionProtocol`.
"""

import json
from typing import Literal, Protocol

from pydantic import BaseModel, Field


class ConnectionProtocol(Protocol):
    """Protocol describing the minimal connection interface.

    Attributes:
        conn_type: Provider/type identifier (e.g., `"fs"`, `"google"`).
        host: Optional hostname/base URL used by some providers.
        schema: Optional logical schema/namespace (provider-specific).
        login: Optional username or key identifier.
        password: Optional password, token, or secret value.
        extra: JSON string with provider-specific extras.

    Properties:
        extra_dejson: Parsed `extra` as a dictionary. Returns `{}` if
            `extra` is falsy.
    """

    conn_type: str
    host: str | None
    schema: str | None
    login: str | None
    password: str | None
    extra: str | None

    @property
    def extra_dejson(self) -> dict:
        """JSON parsed view of `extra`; returns {} when missing."""
        ...


class ConnectionLike(ConnectionProtocol):
    """Dictionary-backed connection compatible with :class:`ConnectionProtocol`.

    This adapter allows passing a plain dict (e.g., from YAML or environment)
    instead of an Airflow connection instance. The expected keys mirror the
    Airflow connection structure.

    Expected dictionary keys:
        - `conn_type` (required)
        - `host` (optional)
        - `schema` (optional)
        - `login` (optional)
        - `password` (optional)
        - `extra` (optional; `str` JSON, `dict` or `None`)

    Notes:
        If `extra` is provided as a `dict`, it is serialized to JSON. If it
        is a string, it must be valid JSON when accessed via `extra_dejson`.
    """

    def __init__(self, connection: dict) -> None:
        """Initialize the adapter from a mapping.

        Args:
            connection: Mapping with Airflow-like connection fields. See class
                docstring for expected keys.
        """
        self.conn_type = connection["conn_type"]
        self.host = connection.get("host")
        self.schema = connection.get("schema")
        self.login = connection.get("login")
        self.password = connection.get("password")
        extra = connection.get("extra")
        if extra is None or isinstance(extra, str):
            self.extra = extra
        elif isinstance(extra, dict):
            self.extra = json.dumps(extra)
        else:
            msg = f"`extra` must be a dict, a JSON string or None, got {type(extra)}"
            raise TypeError(msg)

    @property
    def extra_dejson(self) -> dict:
        """Return `extra` parsed as JSON, or an empty dict if missing/empty.

        Returns:
            Dictionary representation of `extra`. If `extra` is falsy,
            returns `{}`. If `extra` is a non-JSON string, this will raise
            `json.JSONDecodeError`.
        """
        return json.loads(self.extra) if self.extra else {}


class BaseConnectionSchema(BaseModel):
    """Common connection properties.

    This base schema models the shared fields used to describe a generic
    connection to a remote system. Concrete connection types should inherit
    from this class and extend it with type-specific configuration.

    Attributes:
        host: Network host or endpoint (e.g., domain name or IP). If not
            required for the target system, leave as `None`.
        schema: Logical schema, namespace, or protocol segment associated
            with the connection (e.g., database name, URI scheme); optional.
        login: Username or identity used for authentication; optional.
        password: Secret or token associated with the identity; optional.
    """

    host: str | None = Field(
        None,
        description=(
            "Network host or endpoint (e.g., domain name or IP). "
            "Leave unset if not applicable."
        ),
    )
    # NOTE: internal name avoids BaseModel.schema() clash
    schema_: str | None = Field(
        None,
        validation_alias="schema",
        serialization_alias="schema",
        description="Logical schema / namespace / protocol segment.",
    )
    login: str | None = Field(
        None,
        description="Username or identity used for authentication.",
    )
    password: str | None = Field(
        None,
        description="Secret or token used for authentication (keep secure).",
    )


class LocalFsConnectionExtraSchema(BaseModel):
    """Extra options for a local filesystem connection.

    Attributes:
        base_path: Absolute base path on the local filesystem used to resolve
            relative dataset or resource paths.
    """

    base_path: str = Field(
        ...,
        description=(
            "Absolute base path on the local filesystem used to resolve "
            "relative dataset or resource paths."
        ),
    )


class LocalFsConnectionSchema(BaseConnectionSchema):
    """Local filesystem connection.

    Describes access to data stored on the same machine or mounted volumes.

    Attributes:
        conn_type: Constant discriminator for this connection type (`"fs"`).
        extra: Local filesystem-specific configuration (e.g., base path).
    """

    conn_type: Literal["fs"] = Field(
        "fs",
        description='Connection type discriminator. Always "fs" for local filesystem.',
    )
    extra: LocalFsConnectionExtraSchema | None = Field(
        None,
        description="Type-specific options for local filesystem access.",
    )


class GoogleCloudPlatformConnectionExtraSchema(BaseModel):
    """Extra options for a Google Cloud Storage-style connection.

    Exactly one of the key sources may be provided, depending on how
    credentials are supplied.

    Attributes:
        keyfile_dict: Service account credentials as a mapping (already-parsed
            JSON). Useful when credentials are injected as structured data.
        keyfile: Service account credentials as a raw JSON string.
        key_path: Filesystem path to a service account key file accessible at
            runtime.
    """

    keyfile_dict: dict | None = Field(
        None,
        description=(
            "Service account credentials as a mapping (parsed JSON). "
            "Use when credentials are injected as structured data."
        ),
    )
    keyfile: str | None = Field(
        None,
        description=(
            "Service account credentials as a raw JSON string. "
            "Mutually exclusive with 'keyfile_dict' and 'key_path'."
        ),
    )
    key_path: str | None = Field(
        None,
        description=(
            "Path to a service account key file on the local or worker filesystem. "
            "Mutually exclusive with 'keyfile_dict' and 'keyfile'."
        ),
    )


class GoogleCloudPlatformConnectionSchema(BaseConnectionSchema):
    """Google Cloud Storage-style connection.

    Describes access to objects stored in a GCS-compatible bucket. Credentials
    can be provided via a parsed dictionary, raw JSON, or a file path.

    Attributes:
        conn_type: Constant discriminator for this connection type
            (`"google_cloud_platform"`).
        extra: Provider-specific options, including credential sources.
    """

    conn_type: Literal["google_cloud_platform"] = Field(
        "google_cloud_platform",
        description=(
            'Connection type discriminator. Always "google_cloud_platform" '
            "for this provider."
        ),
    )
    extra: GoogleCloudPlatformConnectionExtraSchema | None = Field(
        None,
        description="Type-specific options including credential configuration.",
    )


SupportedConnectionSchemas = (
    LocalFsConnectionSchema | GoogleCloudPlatformConnectionSchema
)
