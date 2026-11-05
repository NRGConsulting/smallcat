# Catalog reference

A catalog is a mapping of dataset names to *entries*. It can be written in YAML,
stored in an Airflow Variable, or built from a Python dictionary.

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/NRGConsulting/smallcat/refs/heads/main/schemas/catalog.schema.json
entries:
  customers:
    file_format: csv
    connection:
      conn_type: fs
      extra:
        base_path: /data/raw
    location: customers.csv

  sales:
    file_format: parquet
    connection: datalake_gcs        # id of an Airflow connection
    location: sales/2025/
    load_options:
      hive_partitioning: true
    save_options:
      overwrite: true
      partition_by: [region]

  events:
    file_format: delta_table
    connection: datalake_gcs
    location: silver/events
    save_options:
      mode: append
```

## Entry fields

| Field          | Required | Description |
|----------------|----------|-------------|
| `file_format`  | yes      | One of `csv`, `excel`, `parquet`, `delta_table`. |
| `connection`   | yes      | An Airflow connection id (string) or an inline connection mapping. |
| `location`     | yes      | Path of the dataset, relative to the connection's base. |
| `load_options` | no       | Options applied when reading. Omit it to use the defaults. |
| `save_options` | no       | Options applied when writing. Omit it to use the defaults. |

## Options per format

| Format        | `load_options`                                                              | `save_options`                                        |
|---------------|-----------------------------------------------------------------------------|-------------------------------------------------------|
| `csv`         | `columns`, `sep`, `header`, `sample_size`, `all_varchar`                    | `header`, `sep`, `overwrite`                          |
| `parquet`     | `binary_as_string`, `file_row_number`, `hive_partitioning`, `union_by_name` | `overwrite`, `partition_by`, `write_partition_columns` |
| `excel`       | `header`, `sheet`, `range`, `all_varchar`, `empty_as_varchar`               | `header`, `sheet`                                     |
| `delta_table` | `version`, `without_files`, `log_buffer_size`                               | `mode`, `partition_by`, `schema_mode`                 |

Boolean options are always forwarded, so `header: false` really turns the
header off.

## Validation

The catalog is validated strictly when it is loaded:

* `file_format` is required in YAML and dictionaries. An entry without it is
  rejected instead of being read as CSV.
* Unknown keys are rejected everywhere (catalog, entries and options), so a
  typo such as `hedaer` is reported instead of silently ignored.
* `load_options` and `save_options` may be omitted.

```yaml
entries:
  broken:
    file_format: csv
    connection: my_conn
    location: a.csv
    load_options:
      hedaer: true      # extra_forbidden: unknown option
```

!!! note
    Entries built in Python (`CSVEntry(connection=..., location=...)`) keep
    `file_format` as a default, so it only has to be written explicitly in
    YAML and dictionaries.

## Locations

`location` is always joined *under* the connection's base, and leading slashes
are ignored. The path is normalized (`a/./b//c` becomes `a/b/c`) and glob
patterns such as `data/**/*.parquet` are kept as they are.

A location that would climb out of the base (`../x`, `a/../../x`) is rejected
with a `ValueError` when the entry is used:

```yaml
entries:
  leak:
    file_format: csv
    connection:
      conn_type: fs
      extra:
        base_path: /data/raw
    location: ../../etc/passwd    # escapes /data/raw
```

```python
catalog.load_polars("leak")   # ValueError: Path escapes the base location
```

Without `base_uri` or `base_path`, an `fs` connection resolves paths relative
to the current working directory.

## Connections

`connection` is either the id of an Airflow connection (a string) or an inline
mapping. An inline mapping is validated when the catalog is loaded, against the
schema selected by its `conn_type`:

* `conn_type` is required and must be `fs`, `google_cloud_platform` (or its
  alias `google`) or `aws` (alias `s3`). Any other value is rejected.
* Unknown keys are rejected, both at the top level and inside `extra`, so a
  typo such as `base_pth` is reported instead of ignored.

Supported top-level keys are `conn_type`, `host`, `schema`, `login`, `password`
and `extra`.

### Secrets

`password`, `aws_session_token`, `keyfile` and `keyfile_dict` are secret values.
Once the catalog is loaded they are held as Pydantic `SecretStr`, so they do not
appear in `repr()`, `str()`, logs, validation errors or `model_dump()`:

```python
entry = catalog.entries["raw"]
entry.connection.password                       # SecretStr('**********')
entry.connection.password.get_secret_value()    # the real value
```

* `entry.get_connection()` returns the plain values (it is what the datasets
  receive, like an Airflow connection). Do not log its result.
* `keyfile_dict` accepts a mapping or a JSON string; it is stored as JSON text.
* A connection given by id is resolved by Airflow, which keeps its own
  protection of the password.
* The secrets are **not** encrypted at rest: in a YAML file or an Airflow
  Variable they are plain text. Prefer an Airflow connection (or a secrets
  backend) over an inline `connection` for real credentials.

### Local filesystem (`fs`)

```yaml
connection:
  conn_type: fs
  extra:
    base_path: /data/raw       # or base_uri: file:///data/raw
```

`extra` is optional; both keys are optional too (see *Locations*).

`base_path` and `base_uri` may be written as Windows paths (`C:\data`,
`C:/data` or `file://C:\data`): they are normalized to `file:///C:/data`.

Creating a dataset (or a catalog) never touches the filesystem. The directory of
a destination, including nested ones such as `reports/2025/out.csv`, is created
when the data is saved. `delta_table` entries leave that to delta-rs.

### Google Cloud Storage (`google_cloud_platform`)

```yaml
connection:
  conn_type: google_cloud_platform
  extra:
    bucket: my-bucket
    prefix: raw                # or base_uri: gs://my-bucket/raw
```

Credentials depend on the engine that reads the data, which is not only a
matter of the format: `load_*` and `scan_polars` use different engines for
Parquet.

| Read with                    | Formats                   | Engine                | Credentials |
|------------------------------|---------------------------|-----------------------|-------------|
| `load_*`                     | `csv`, `parquet`, `excel` | DuckDB                | HMAC keys: the connection's `login` (key id) and `password` (secret). |
| `load_*`                     | `delta_table`             | delta-rs              | Service account: `keyfile_dict`, `keyfile` or `key_path` in `extra`. |
| `scan_polars`                | `parquet`                 | Polars                | Service account (same keys as above). |
| `scan_polars`                | `delta_table`             | delta-rs and Polars   | Service account (same keys as above). |
| `scan_polars`                | `csv`, `excel`            | DuckDB (eager)        | HMAC keys: they are loaded with `load_polars` and wrapped. |

`google` is accepted as an alias of `google_cloud_platform` by every format.

A connection can carry both kinds of credentials: each engine uses the one it
understands, so the same connection serves `load_*` and `scan_polars` on the
same Parquet file. A mismatch does not fail silently: a DuckDB-based read with
only a service account, or a delta-rs or Polars read with only HMAC keys, emits a
`UserWarning` and accesses the bucket without credentials.

Incoherent settings are rejected when the connection is validated (and again
when a dataset uses an Airflow connection id):

* `login` and `password` must come together.
* Only one of `keyfile_dict`, `keyfile` and `key_path` may be set.

!!! note
    DuckDB cannot authenticate with a service account, so `load_*` on CSV,
    Parquet and Excel needs HMAC keys. Delta and every `scan_polars` on Parquet
    need a service account. To use both ways of reading the same Parquet file,
    put both kinds of credentials in the connection (see
    [Parquet on GCS: load and scan](examples/gcs.md)).

### Amazon S3 (`aws`)

```yaml
connection:
  conn_type: aws
  extra:
    bucket: my-bucket
    prefix: raw                    # or base_uri: s3://my-bucket/raw
    region_name: eu-west-1
```

The `extra` names are the ones of the Airflow `aws` connection, so an Airflow
connection id of that type can be used as well.

| `extra` key         | Description |
|---------------------|-------------|
| `bucket`, `prefix`  | Build the base URI `s3://bucket/prefix`. |
| `base_uri`          | Explicit base URI; takes precedence over `bucket`/`prefix`. |
| `region_name`       | AWS region. |
| `endpoint_url`      | Endpoint of an S3-compatible store such as MinIO (`http://minio:9000`). Uses path-style requests, and plain HTTP when the URL says `http://`. |
| `profile_name`      | Profile of the AWS config files to take credentials from. |
| `aws_session_token` | Session token that goes with static keys. |

**Credentials.** There are two mutually exclusive ways to authenticate:

* **IAM roles and the AWS credential chain** (recommended): leave `login` and
  `password` empty. The credentials are found the way the AWS SDK finds them:
  environment variables, web identity (EKS service accounts), ECS task roles,
  EC2 instance profiles, SSO and the shared AWS files. To assume a role, define
  a profile with `role_arn` and `source_profile` in `~/.aws/config` and name it
  in `profile_name`.
* **Static keys**: `login` is the access key id and `password` the secret
  access key (plus `aws_session_token` for temporary keys).

Combining static keys with `profile_name`, or giving only one of `login` and
`password`, is rejected.

| Formats                   | Engine   | How the credential chain is resolved |
|---------------------------|----------|--------------------------------------|
| `csv`, `parquet`, `excel` | DuckDB   | By DuckDB's `aws` extension. |
| `parquet` (`scan_polars`) | Polars   | By Polars itself, with the same rules as delta-rs below. |
| `delta_table`             | delta-rs | By delta-rs itself (environment, web identity, ECS, instance profile). With `profile_name`, the credentials are resolved by DuckDB and handed to delta-rs; they are temporary and are not refreshed during a long operation. |

Delta tables on S3 are written without DynamoDB locking, which relies on S3
conditional writes.

### Not supported

Databricks connections are rejected with `NotImplementedError` when reading or
writing Delta tables. An inline mapping with another `conn_type` is rejected
when the catalog is loaded.

## Filtering on load

`where` must be a single boolean expression over the dataset's columns, for
example `amount > 10 AND name IN ('a', 'b')`. It is parsed before it runs and a
`ValueError` is raised for statements, comments left open, subqueries or
anything that changes the shape of the query. `columns` accepts plain column
names only.

## Extensions and network access

Reading or writing local files needs no network access. The DuckDB `httpfs`
extension is loaded only for remote storage (`gs://`, `s3://`, `https://`), and
the `excel` extension (and the `aws` extension, for the credential chain of S3
connections) is installed on first use and then reused from DuckDB's cache.

## Airflow

Airflow is optional and is only imported when needed, for instance to resolve a
connection id or to call `Catalog.from_airflow_variable`. Importing `smallcat`
does not import Airflow.
