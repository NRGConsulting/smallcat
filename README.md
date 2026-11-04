<p align="center">
  <img src="docs/assets/images/smallcat-logo.png" width="140" alt="smallcat logo">
</p>

<h1 align="center">smallcat-chonk</h1>
<p align="center"><em>A small, modular data catalog.</em></p>

<p align="center">
  <a href="https://pypi.org/project/smallcat-chonk/"><img src="https://img.shields.io/pypi/v/smallcat-chonk.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/smallcat-chonk/"><img src="https://img.shields.io/pypi/pyversions/smallcat-chonk.svg" alt="Python versions"></a>
  <a href="https://github.com/NRGConsulting/smallcat/actions"><img src="https://img.shields.io/github/actions/workflow/status/NRGConsulting/smallcat/publish.yml?label=CI" alt="CI"></a>
  <a href="https://codecov.io/gh/NRGConsulting/smallcat"><img src="https://img.shields.io/codecov/c/github/NRGConsulting/smallcat" alt="coverage"></a>
  <a href="https://github.com/NRGConsulting/smallcat/blob/main/LICENSE"><img src="https://img.shields.io/github/license/NRGConsulting/smallcat.svg" alt="license"></a>
  <a href="https://pepy.tech/project/smallcat-chonk"><img src="https://static.pepy.tech/badge/smallcat-chonk" alt="downloads"></a>
  <a href="https://nrgconsulting.github.io/smallcat/"><img src="https://img.shields.io/badge/docs-mkdocs%20material-blue" alt="docs"></a>
</p>

## Install
```bash
pip install smallcat-chonk
```

## Quickstart
### Create Catalog

Local catalogs can be kept in YAML files.

```yaml
entries:
    foo:
        file_format: csv
        connection:
            conn_type: fs
            extra:
                base_path: /tmp/smallcat-example/
        location: foo.csv
        load_options:
            header: true
    bar:
        file_format: parquet
        connection:
            conn_type: google_cloud_platform
            extra:
                bucket: my-bucket
        location: bar.parquet
        save_options:
            partition_by:
                - year
                - month
```

### Standalone

```python
from smallcat.catalog import Catalog

catalog = Catalog.from_yaml("catalog.yaml")
catalog.save_polars("foo", df)
df2 = catalog.load_polars("foo")
```

### Filter on load

`load_polars` (and the lower-level Arrow loaders) accept optional `where` and
`columns` arguments to push filters and projections down to DuckDB/Arrow when reading:

```python
df = catalog.load_polars(
    "bar",
    where="event_date >= '2024-01-01'",
    columns=["event_date", "user_id"],
)
```

### Lazy scans

`scan_polars` returns a Polars `LazyFrame`: nothing is read until `collect()`, and
the filters and columns you chain are pushed down to the storage layer. On S3/GCS
this means only the needed columns and row groups are downloaded.

```python
import polars as pl

df = (
    catalog.scan_polars("bar")
    .filter(pl.col("event_date") >= pl.date(2024, 1, 1))  # Polars expressions, not SQL
    .select("event_date", "user_id")
    .collect()
)
```

| | `load_polars` | `scan_polars` |
|---|---|---|
| Engine | DuckDB | Polars' own reader (Parquet) / delta-rs + Polars (Delta) |
| Filter | `where="amount > 10"` (SQL string) | `.filter(pl.col("amount") > 10)` |
| Reads | Everything the query needs, immediately | Nothing until `collect()` |
| Real lazy scan | n/a | Parquet and Delta. CSV and Excel are loaded eagerly and wrapped, so nothing is skipped |

Notes:

* Parquet and Delta use the connection's credentials through the same options
  (`aws_*`, `google_service_account*`). For Parquet, the DuckDB-only load options
  `binary_as_string` and `file_row_number` raise a `ValueError` in `scan_polars`;
  use `load_polars` for those.
* Existing limits apply: IAM role, SSO and `AssumeRole` credentials are unverified
  against real AWS, and GCS needs a service account for Delta/`scan_polars` but HMAC
  keys for DuckDB.
* Requires `polars>=1.31.0`.

### With Airflow
```python
from smallcat.catalog import Catalog

catalog = Catalog.from_airflow_variable("example_catalog")
df = catalog.load_polars("bar")
```

## Catalog reference

`load_options` and `save_options` are optional, `file_format` is required and unknown
keys are rejected. See the [catalog reference](https://nrgconsulting.github.io/smallcat/catalog-reference/)
for every option, the validation rules and the credentials each format uses.

## Docs
Read more at [the official docs](https://nrgconsulting.github.io/smallcat/).
