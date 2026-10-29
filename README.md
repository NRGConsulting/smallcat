<p align="center">
  <img src="docs/assets/images/smallcat-logo.png" width="140" alt="smallcat logo">
</p>

<h1 align="center">smallcat</h1>
<p align="center"><em>A small, modular data catalog.</em></p>

<p align="center">
  <a href="https://pypi.org/project/smallcat/"><img src="https://img.shields.io/pypi/v/smallcat.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/smallcat/"><img src="https://img.shields.io/pypi/pyversions/smallcat.svg" alt="Python versions"></a>
  <a href="https://github.com/NRGConsulting/smallcat/actions"><img src="https://img.shields.io/github/actions/workflow/status/NRGConsulting/smallcat/publish.yml?label=CI" alt="CI"></a>
  <a href="https://codecov.io/gh/NRGConsulting/smallcat"><img src="https://img.shields.io/codecov/c/github/NRGConsulting/smallcat" alt="coverage"></a>
  <a href="https://github.com/NRGConsulting/smallcat/blob/main/LICENSE"><img src="https://img.shields.io/github/license/NRGConsulting/smallcat.svg" alt="license"></a>
  <a href="https://pepy.tech/project/smallcat"><img src="https://static.pepy.tech/badge/smallcat" alt="downloads"></a>
  <a href="https://nrgconsulting.github.io/smallcat/"><img src="https://img.shields.io/badge/docs-mkdocs%20material-blue" alt="docs"></a>
</p>

## Install
```bash
pip install smallcat
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
catalog.save_pandas("foo", df)
df2 = catalog.load_pandas("foo")
```

### Filter on load

`load_pandas` (and the lower-level Arrow loaders) accept optional `where` and
`columns` arguments to push filters and projections down to DuckDB/Arrow when reading:

```python
df = catalog.load_pandas(
    "bar",
    where="event_date >= '2024-01-01'",
    columns=["event_date", "user_id"],
)
```

### With Airflow
```python
from smallcat.catalog import Catalog

catalog = Catalog.from_airflow_variable("example_catalog")
df = catalog.load_pandas("bar")
```

## Catalog reference

`load_options` and `save_options` are optional, `file_format` is required and unknown
keys are rejected. See the [catalog reference](https://nrgconsulting.github.io/smallcat/catalog-reference/)
for every option, the validation rules and the credentials each format uses.

## Docs
Read more at [the official docs](https://nrgconsulting.github.io/smallcat/).
