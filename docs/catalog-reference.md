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
catalog.load_pandas("leak")   # ValueError: Path escapes the base location
```

Without `base_uri` or `base_path`, an `fs` connection resolves paths relative
to the current working directory.

## Connections

`connection` is either the id of an Airflow connection (a string) or an inline
mapping. An inline mapping is validated when the catalog is loaded, against the
schema selected by its `conn_type`:

* `conn_type` is required and must be `fs`, `google_cloud_platform` (or its
  alias `google`). Any other value is rejected.
* Unknown keys are rejected, both at the top level and inside `extra`, so a
  typo such as `base_pth` is reported instead of ignored.

Supported top-level keys are `conn_type`, `host`, `schema`, `login`, `password`
and `extra`.

### Local filesystem (`fs`)

```yaml
connection:
  conn_type: fs
  extra:
    base_path: /data/raw       # or base_uri: file:///data/raw
```

`extra` is optional; both keys are optional too (see *Locations*).

### Google Cloud Storage (`google_cloud_platform`)

```yaml
connection:
  conn_type: google_cloud_platform
  extra:
    bucket: my-bucket
    prefix: raw                # or base_uri: gs://my-bucket/raw
```

Credentials depend on the engine that reads the format:

| Formats                  | Engine | Credentials                                                     |
|--------------------------|--------|-----------------------------------------------------------------|
| `csv`, `parquet`, `excel` | DuckDB | HMAC keys: the connection's `login` (key id) and `password` (secret). |
| `delta_table`            | delta-rs | Service account: `keyfile_dict`, `keyfile` or `key_path` in `extra`. |

A mismatch does not fail silently: a DuckDB-based dataset with only a service
account, or a Delta dataset with only HMAC keys, emits a `UserWarning` and
accesses the bucket without credentials.

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
the `excel` extension is installed on first use and then reused from DuckDB's
cache.

## Airflow

Airflow is optional and is only imported when needed, for instance to resolve a
connection id or to call `Catalog.from_airflow_variable`. Importing `smallcat`
does not import Airflow.
