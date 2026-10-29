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
from typing import Literal, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


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

    model_config = ConfigDict(extra="forbid")

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

    Both keys are optional. Without either of them, dataset locations are
    resolved relative to the current working directory.

    Attributes:
        base_path: Base path on the local filesystem used to resolve relative
            dataset or resource paths.
        base_uri: Explicit base URI (for example `file:///data`). Takes
            precedence over `base_path`.
    """

    model_config = ConfigDict(extra="forbid")

    base_path: str | None = Field(
        None,
        description=(
            "Base path on the local filesystem used to resolve relative "
            "dataset or resource paths."
        ),
    )
    base_uri: str | None = Field(
        None,
        description="Explicit base URI. Takes precedence over 'base_path'.",
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
        base_uri: Explicit base URI (for example `gs://bucket/prefix`). Takes
            precedence over `bucket` and `prefix`.
        bucket: Bucket name used to build the base URI.
        prefix: Optional path inside the bucket, appended to the bucket.
    """

    model_config = ConfigDict(extra="forbid")

    base_uri: str | None = Field(
        None,
        description="Explicit base URI. Takes precedence over 'bucket' and 'prefix'.",
    )
    bucket: str | None = Field(
        None,
        description="Bucket name used to build the base URI (gs://bucket).",
    )
    prefix: str | None = Field(
        None,
        description="Optional path inside the bucket, appended to the base URI.",
    )
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
        conn_type: Discriminator for this connection type
            (`"google_cloud_platform"`, or its alias `"google"`).
        extra: Provider-specific options, including credential sources.
    """

    conn_type: Literal["google_cloud_platform", "google"] = Field(
        "google_cloud_platform",
        description=(
            'Connection type discriminator: "google_cloud_platform" or its '
            'alias "google".'
        ),
    )
    extra: GoogleCloudPlatformConnectionExtraSchema | None = Field(
        None,
        description="Type-specific options including credential configuration.",
    )


AWS_CONN_TYPES = ("aws", "s3")
"""Connection types handled as Amazon S3 (`aws` is the Airflow type)."""


def validate_aws_credentials(
    login: str | None,
    password: str | None,
    extras: dict,
) -> None:
    """Check that the credential settings of an S3 connection are coherent.

    An S3 connection authenticates in one of two ways: **static keys**
    (`login` + `password`, optionally with `aws_session_token`) or the AWS
    **credential chain** (IAM roles and everything else the AWS SDK finds),
    optionally narrowed with `profile_name`. They cannot be mixed.

    Args:
        login: Access key id, if any.
        password: Secret access key, if any.
        extras: The connection's `extra` mapping.

    Raises:
        ValueError: If the combination of settings is not supported.
    """
    if bool(login) != bool(password):
        msg = "AWS static keys need both `login` (access key id) and `password`."
        raise ValueError(msg)
    if login:
        if extras.get("profile_name"):
            msg = (
                "`profile_name` cannot be combined with static keys "
                "(login/password): remove the keys to use the AWS credential chain."
            )
            raise ValueError(msg)
    elif extras.get("aws_session_token"):
        msg = "`aws_session_token` needs static keys (login/password)."
        raise ValueError(msg)


class AwsConnectionExtraSchema(BaseModel):
    """Extra options for an Amazon S3 (or S3-compatible) connection.

    The names match the ones of the Airflow `aws` connection, so an Airflow
    connection can be reused as it is.

    Attributes:
        base_uri: Explicit base URI (for example `s3://bucket/prefix`). Takes
            precedence over `bucket` and `prefix`.
        bucket: Bucket name used to build the base URI.
        prefix: Optional path inside the bucket, appended to the bucket.
        region_name: AWS region (for example `eu-west-1`).
        endpoint_url: Custom endpoint for S3-compatible stores such as MinIO
            (for example `http://minio:9000`). Switches to path-style requests.
        profile_name: Named profile of the AWS config files to take credentials
            from. A profile that sets `role_arn` and `source_profile` assumes
            that IAM role.
        aws_session_token: Session token that goes with static keys.
    """

    model_config = ConfigDict(extra="forbid")

    base_uri: str | None = Field(
        None,
        description="Explicit base URI. Takes precedence over 'bucket' and 'prefix'.",
    )
    bucket: str | None = Field(
        None,
        description="Bucket name used to build the base URI (s3://bucket).",
    )
    prefix: str | None = Field(
        None,
        description="Optional path inside the bucket, appended to the base URI.",
    )
    region_name: str | None = Field(None, description="AWS region, e.g. eu-west-1.")
    endpoint_url: str | None = Field(
        None,
        description=(
            "Custom endpoint for S3-compatible stores, e.g. http://minio:9000."
        ),
    )
    profile_name: str | None = Field(
        None,
        description=(
            "Profile of the AWS config files to take credentials from. A profile "
            "with `role_arn` and `source_profile` assumes that IAM role."
        ),
    )
    aws_session_token: str | None = Field(
        None,
        description="Session token that goes with static keys (login/password).",
    )


class AwsConnectionSchema(BaseConnectionSchema):
    """Amazon S3 (or S3-compatible) connection.

    Credentials are, in order of preference: IAM roles and the rest of the AWS
    credential chain (leave `login` and `password` empty), or static keys in
    `login` (access key id) and `password` (secret access key).

    Attributes:
        conn_type: Discriminator for this connection type (`"aws"`, or its alias
            `"s3"`).
        extra: S3-specific options.
    """

    conn_type: Literal["aws", "s3"] = Field(
        "aws",
        description='Connection type discriminator: "aws" or its alias "s3".',
    )
    extra: AwsConnectionExtraSchema | None = Field(
        None,
        description="Type-specific options including credential configuration.",
    )

    @model_validator(mode="after")
    def _check_credentials(self) -> Self:
        extras = self.extra.model_dump(exclude_none=True) if self.extra else {}
        validate_aws_credentials(self.login, self.password, extras)
        return self


SupportedConnectionSchemas = (
    LocalFsConnectionSchema
    | GoogleCloudPlatformConnectionSchema
    | AwsConnectionSchema
)
