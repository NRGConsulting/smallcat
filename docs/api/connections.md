# Connections

Where the storage of the data is defined, for a [catalog](catalog.md) and for the
[ORM](models.md) alike. For how to define and load them, see the
[Connections guide](../connections.md) and the
[examples](../examples/connections/csv.md).

::: smallcat.connections_config.Connections
    options:
      show_root_heading: true
      members: true

## Connection types

The definition of a connection is validated against the schema selected by its
`conn_type`. Unknown keys are rejected, both in the definition and in `extra`.

### Local filesystem

`conn_type: fs`

::: smallcat.connections.LocalFsConnectionSchema
    options:
      heading_level: 4
      show_root_heading: true
      members: true

::: smallcat.connections.LocalFsConnectionExtraSchema
    options:
      heading_level: 4
      show_root_heading: true
      members: true

### Google Cloud Storage

`conn_type: google_cloud_platform` (alias `google`)

::: smallcat.connections.GoogleCloudPlatformConnectionSchema
    options:
      heading_level: 4
      show_root_heading: true
      members: true

::: smallcat.connections.GoogleCloudPlatformConnectionExtraSchema
    options:
      heading_level: 4
      show_root_heading: true
      members: true

### Amazon S3

`conn_type: aws` (alias `s3`)

::: smallcat.connections.AwsConnectionSchema
    options:
      heading_level: 4
      show_root_heading: true
      members: true

::: smallcat.connections.AwsConnectionExtraSchema
    options:
      heading_level: 4
      show_root_heading: true
      members: true

## Errors
::: smallcat.connections_config.ConnectionsError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.connections_config.ConnectionsValidationError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.connections_config.ConnectionNotFoundError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.connections_config.ConnectionNotLoadedError
    options:
      heading_level: 3
      show_root_heading: true

::: smallcat.connections_config.DuplicateConnectionError
    options:
      heading_level: 3
      show_root_heading: true
