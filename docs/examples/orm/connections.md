# Connections with the ORM

How a connection reaches a [model](../../models.md): define it once in a file,
load it into the registry, and say which connection the model uses when you
register it. The model itself never names a connection. It needs nothing but
smallcat-chonk; the script writes its files under a temporary directory.

```sh
pip install smallcat-chonk
python connections_orm.py
```

## 1. Define and load the connections

```yaml
# connections.yaml
connections:
  lake:
    conn_type: fs
    extra:
      base_path: /data
  archive:
    conn_type: fs
    extra:
      base_path: /archive
```

```python
from smallcat import Connections

conns = Connections.from_yaml("connections.yaml")
```

For a bucket, only this definition changes: see
[Google Cloud Storage](../connections/gcs.md) and [Amazon S3](../connections/s3.md).

## 2. Define the model

```python
from smallcat import Model, ModelFileFormat

class Person(Model):
    id: int
    name: str

    class Meta:
        location = "people.csv"
        file_format = ModelFileFormat.CSV
```

## 3. Load the connections into the registry and register the model

```python
from smallcat import registry

registry.load_connections(conns)
registry.register(Person, connection="lake")
```

`register` is where the connection is chosen, outside the model, so the same model
can run against different storage in each environment.

## 4. Basic use

```python
import polars as pl
from smallcat import ModelWriteMode

Person.objects.save(df, mode=ModelWriteMode.OVERWRITE)
Person.objects.all()
Person.objects.filter(pl.col("id") > 1).all()
```

## The script

```python
--8<-- "docs/examples/connections_orm.py"
```

## What it prints

```text
Connections(lake:fs, archive:fs)

=== save and read
all             [(1, 'ada'), (2, 'linus'), (3, 'grace')]
filter          [(2, 'linus'), (3, 'grace')]
```

The same model on other storage: only the registration changes. Here it is saved
to `archive` and the `lake` data is left as it was:

```text
=== the same model on other storage
archive         [(9, 'alan')]
lake, untouched [(1, 'ada'), (2, 'linus'), (3, 'grace')]
```

Registering against a name that was never loaded fails right away, listing the
names that are:

```text
=== a connection that is not loaded
ConnectionNotLoadedError - Connection 'warehouse' is not loaded (available: lake, archive)
```

## Next

[Models](../../models.md), [Querying models](../../querying-models.md) and
[Writing with a model](../../writing-models.md). For the same connection used from
a catalog, see [Connections with a catalog](../catalog/connections.md).
