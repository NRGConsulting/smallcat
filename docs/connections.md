# Connections

`Connections` is the one place where connections are defined. You load it
first, and then hand it to whatever needs connections: a
[`Catalog`](catalog-reference.md) or a model registry (see [Models](models.md)).
Both refer to a connection **by name**.

```python
from smallcat import Connections

conns = Connections.from_yaml("connections.yaml")
```

```yaml
# connections.yaml
connections:
  lake:
    conn_type: fs
    extra:
      base_path: /data
  warehouse:
    conn_type: google_cloud_platform
    extra:
      bucket: my-bucket
```

Each definition is the same mapping described in the
[catalog reference](catalog-reference.md#connections) (`fs`,
`google_cloud_platform` and `aws`, with the same keys and the same credentials).

## Where connections come from

| Loader | Source | Name of each connection |
|--------|--------|-------------------------|
| `Connections.from_yaml(path)` | A YAML file with a `connections` mapping | The key |
| `Connections.from_dict(dict)` | A dictionary with the same structure | The key |
| `Connections.from_airflow_variable(id)` | An Airflow Variable holding that structure as JSON | The key |
| `Connections.from_airflow([ids])` | Airflow connections, resolved with `BaseHook` | The Airflow connection id |

There is no way to pass a connection inline to a catalog or a registry on
purpose: everything comes from a file or from Airflow, so credentials stay out
of application code.

```python
# Airflow: the ids are resolved and checked right now
conns = Connections.from_airflow(["lake", "warehouse"])
```

## Validated when loaded

Every loader checks the connections as it loads them, so a mistake fails at
start-up and not on the first read.

* Definitions from YAML, a dictionary or a Variable are validated against the
  schema chosen by `conn_type`. Unknown keys are rejected, so `base_pth` is
  reported instead of ignored.
* Airflow connections must exist, have a supported `conn_type` (`fs`,
  `google_cloud_platform`/`google`, `aws`/`s3`) and an `extra` that is valid
  JSON. Their other fields are not checked: Airflow connections usually carry
  keys that smallcat does not use.
* All the problems are reported together:

```text
ConnectionsValidationError: Invalid connections:
  - lake.extra.base_pth: Extra inputs are not permitted
  - warehouse: Input tag 'ftp' found using 'conn_type' does not match any of the expected tags: ...
```

Error messages name the connection and the field but **never include the
values**, so a password cannot end up in a log. `repr(conns)` shows only names
and types: `Connections(lake:fs, warehouse:google_cloud_platform)`.

| Error | Raised when |
|-------|-------------|
| `ConnectionsValidationError` | A definition is invalid (also a `ValueError`). |
| `ConnectionNotFoundError` | An Airflow connection id does not exist (also a `LookupError`). |
| `ConnectionNotLoadedError` | A name is used that was never loaded. The message lists the names that are. |
| `DuplicateConnectionError` | Two sets of connections define the same name. |

## Using them

```python
from smallcat.catalog import Catalog

catalog = Catalog.from_yaml("catalog.yaml", connections=conns)
```

```yaml
# catalog.yaml
entries:
  sales:
    file_format: parquet
    connection: warehouse        # a name in `conns`
    location: sales/
```

The names are checked when the catalog is loaded. If `sales` pointed to a name
that is not in `conns`, `Catalog.from_yaml` would raise
`ConnectionNotLoadedError` listing every entry affected and the names that are
available, before anything is read.

Without `connections=`, a name is still resolved through Airflow the first time
it is used, as before, so existing catalogs keep working.

!!! warning "Inline connections are deprecated"
    A catalog entry whose `connection` is a mapping instead of a name still
    works, but emits a `DeprecationWarning`: it puts credentials in the
    catalog. Move the definition to a `Connections` and refer to it by name.

## Example

How to connect to [a CSV file](examples/connections/csv.md),
[Google Cloud Storage](examples/connections/gcs.md) and
[Amazon S3](examples/connections/s3.md).

## API

See the [Connections reference](api/connections.md).
