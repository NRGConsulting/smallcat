# Connections with a catalog

How a connection reaches a [catalog](../../catalog-reference.md), without any other
dependency: define it once in a file, load it, hand it to the catalog, and name it
in each entry. To do the same inside Airflow, see [Airflow](airflow.md).

## Setup

```sh
pip install smallcat-chonk
```

or

```sh
uv add smallcat-chonk
```

The script below writes its files under a temporary directory, so you can run it
as it is:

```sh
python connections_catalog.py
```

## 1. Define and load the connection

Connections live apart from the catalog and are referred to by name.

```yaml
# connections.yaml
connections:
  lake:
    conn_type: fs
    extra:
      base_path: /data
```

```python
from smallcat import Connections

conns = Connections.from_yaml("connections.yaml")
```

For a bucket, only this definition changes: see
[Google Cloud Storage](../connections/gcs.md) and [Amazon S3](../connections/s3.md).

## 2. Hand it to the catalog

```python
from smallcat.catalog import Catalog

catalog = Catalog.from_yaml("catalog.yaml", connections=conns)
```

Each entry names the connection in `connection:`. The names are checked when the
catalog is loaded:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/NRGConsulting/smallcat/refs/heads/main/schemas/catalog.schema.json
entries:
  people:
    file_format: csv
    connection: lake           # a name in `conns`
    location: people.csv       # /data/people.csv
    load_options: {header: true}
    save_options: {header: true}
```

The `# yaml-language-server` line gives you validation and auto-complete for the
catalog in VS Code.

## 3. Basic use

```python
catalog.save_polars("people", df)
catalog.load_polars("people")
catalog.load_polars("people", where="id > 1", columns=["name"])
```

## The script

```python
--8<-- "docs/examples/connections_catalog.py"
```

## What it prints

```text
Connections(lake:fs)

=== save and load
load            [(1, 'ada'), (2, 'linus'), (3, 'grace')]
where + columns [('linus',), ('grace',)]
```

An entry that names a connection that was not loaded fails when the catalog is
**loaded**, before anything is read. The message lists the entry and the names
that are available:

```text
=== an entry that names a connection that is not loaded
ConnectionNotLoadedError - Connections not loaded: entry 'sales' uses 'warehouse' (available: lake)
```

Without `connections=`, a name is still resolved through Airflow the first time it
is used. To load from Airflow instead of a file, see
[Connections](../../connections.md#where-connections-come-from).

## Next

* [Airflow](airflow.md): the same catalog in a DAG.
* [Querying a catalog](../../querying-catalog.md),
  [Writing with a catalog](../../writing-catalog.md) and the
  [catalog reference](../../catalog-reference.md).
* The same connection used from a model:
  [Connections with the ORM](../orm/connections.md).
