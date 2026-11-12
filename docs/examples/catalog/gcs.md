# Parquet on GCS: load and scan

On Google Cloud Storage the two ways of reading a Parquet file use different
engines, and each one understands a different kind of credential:

| Method          | Engine | Credential it needs                              |
|-----------------|--------|--------------------------------------------------|
| `load_polars`   | DuckDB | HMAC keys (`login` and `password`)               |
| `scan_polars`   | Polars | Service account (`keyfile_dict`, `keyfile` or `key_path`) |

You do **not** need two connections. One connection can carry both, and each
engine picks the one it understands.

## Connection

```yaml
# connections.yaml
connections:
  gcs:
    conn_type: google_cloud_platform
    login: GOOG1E...                   # HMAC key id      -> load_polars
    password: <hmac-secret>            # HMAC secret      -> load_polars
    extra:
      bucket: my-bucket
      key_path: /secrets/sa.json       # service account  -> scan_polars
```

Only one of `keyfile_dict`, `keyfile` and `key_path` may be set. `login` and
`password` must come together. See
[Connect to Google Cloud Storage](../connections/gcs.md) for every credential
option, and how to load the connection.

## Catalog

```yaml
# catalog.yaml
entries:
  sales:
    file_format: parquet
    connection: gcs                    # the name defined above
    location: sales/2024/*.parquet
```

## Usage

```python
import polars as pl
from smallcat import Connections
from smallcat.catalog import Catalog

conns = Connections.from_yaml("connections.yaml")
catalog = Catalog.from_yaml("catalog.yaml", connections=conns)

# DuckDB reads and filters with SQL. Uses the HMAC keys.
eager = catalog.load_polars(
    "sales",
    where="country = 'ES' AND amount > 100",
    columns=["date", "amount"],
)

# Polars reads lazily. Uses the service account.
lazy = (
    catalog.scan_polars("sales")
    .filter((pl.col("country") == "ES") & (pl.col("amount") > 100))
    .select("date", "amount")
)
df = lazy.collect()   # only the needed columns and row groups are downloaded
```

Both return the same rows. `scan_polars` pushes the filter and the columns into
the read, which is what saves bandwidth on a bucket; see
[Lazy scans](../../index.md#lazy-scans).

## When a credential is missing

A missing credential does not fail silently at connection time. `smallcat`
emits a `UserWarning` and the read goes on **without credentials**, so on a
private bucket it then fails with a permissions error from GCS:

| Connection has          | `load_polars`                       | `scan_polars`                          |
|-------------------------|-------------------------------------|----------------------------------------|
| HMAC keys and service account | works                         | works                                  |
| Only HMAC keys          | works                               | `UserWarning`, reads anonymously       |
| Only a service account  | `UserWarning`, reads anonymously    | works                                  |

If you only ever use one of the two methods, one kind of credential is enough.

## Limitations

* Unifying the authentication so that one credential serves both engines is
  tracked in issue #9.
* The service account path of `scan_polars` is covered by unit tests that check
  the options handed to Polars, not by a test against a real bucket.
* `scan_polars` on Parquet does not support the DuckDB-only load options
  `binary_as_string` and `file_row_number`; it raises a `ValueError`. Use
  `load_polars` for those.
