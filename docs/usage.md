# Catalog and ORM

smallcat gives you two ways to read and write the same kind of data
(CSV, Excel, Parquet and Delta tables, on a local disk, S3 or GCS). Both get
their storage from [Connections](connections.md) and refer to a connection **by
name**. They differ in where the dataset is described.

|  | Catalog | ORM (models) |
|--|---------|--------------|
| A dataset is described by | An **entry** in a YAML file, an Airflow Variable or a dict | A **model**: a Pydantic class |
| Where it is described | Outside the code, as configuration | In Python, next to the code that uses it |
| Connection | Named in the entry | Named when the model is registered, never in the model |
| Columns and types | Not declared | Declared, and the data is validated against them |
| Write mode | Fixed in the entry (`save_options.mode`) | Chosen on every `save` call |
| Query | `load_polars(..., where=...)` and `scan_polars` | `Model.objects.filter(...)`, `where`, `all`, `lazy` |
| Reference | [Catalog reference](catalog-reference.md) | [Models](models.md) |

## Using a catalog

Use it when you want datasets to be configuration: a pipeline names an entry and
the YAML decides the format, the location and how it is written.

```yaml
# catalog.yaml
entries:
  customers:
    file_format: parquet
    connection: lake
    location: silver/customers.parquet
```

```python
from smallcat import Connections
from smallcat.catalog import Catalog

conns = Connections.from_yaml("connections.yaml")
catalog = Catalog.from_yaml("catalog.yaml", connections=conns)

df = catalog.load_polars("customers", where="id > 10")
catalog.save_polars("customers", df)
```

Next: [Catalog reference](catalog-reference.md), [Querying a catalog](querying-catalog.md)
and [Writing with a catalog](writing-catalog.md).

## Using the ORM

Use it when you want the shape of the data to be part of the code: reading and
writing return and take DataFrames that are checked against the model.

```python
import polars as pl
from smallcat import Connections, Model, ModelFileFormat, ModelWriteMode, registry

class Customer(Model):
    id: int
    name: str

    class Meta:
        location = "silver/customers"
        file_format = ModelFileFormat.PARQUET

registry.load_connections(Connections.from_yaml("connections.yaml"))
registry.register(Customer, connection="lake")

Customer.objects.filter(pl.col("id") > 10).all()
Customer.objects.save(df, mode=ModelWriteMode.OVERWRITE)
```

Next: [Models](models.md), [Querying models](querying-models.md) and
[Writing with a model](writing-models.md).

## Which one to use

| You want to | Use |
|-------------|-----|
| Keep dataset definitions in config, outside the code | Catalog |
| Run the same job against different environments by changing a file | Either: swap the connections file (and, for the ORM, the registration) |
| Have the columns and types checked on every read and write | ORM |
| Decide the write mode in code, per call | ORM |
| Fix one way of writing per name (replace here, append there) | Catalog |

Both can be used in the same project: they share the connections.
