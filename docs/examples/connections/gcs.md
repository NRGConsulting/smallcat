# Connect to Google Cloud Storage

A connection to a bucket. It says which bucket and prefix to use and how to
authenticate.

```yaml
# connections.yaml
connections:
  gcs_hmac:
    conn_type: google_cloud_platform
    login: GOOG1E...               # HMAC key id
    password: <hmac-secret>        # HMAC secret
    extra:
      bucket: my-bucket
      prefix: raw                  # or base_uri: gs://my-bucket/raw
```

```python
from smallcat import Connections

conns = Connections.from_yaml("connections.yaml")
```

Asking for `sales/2024.csv` through this connection means
`gs://my-bucket/raw/sales/2024.csv`. `google` is accepted as an alias of
`google_cloud_platform`.

## Which credential

It depends on the engine that reads the data, not only on the format:

| Data | Read with | Credential |
|------|-----------|------------|
| CSV, Excel or Parquet, loaded eagerly | DuckDB | **HMAC keys**: `login` and `password` |
| Parquet, scanned lazily | Polars | **Service account** |
| A Delta table | delta-rs | **Service account** |

A service account goes in `extra`, with **one** of `key_path` (a file), `keyfile`
(the JSON as a string) or `keyfile_dict` (the JSON as a mapping):

```yaml
connections:
  gcs_sa:
    conn_type: google_cloud_platform
    extra:
      bucket: my-bucket
      key_path: /secrets/service-account.json
```

One connection can carry both kinds, and each engine picks the one it
understands.

## Validated when loaded

Nothing is read when the connections are loaded, but a wrong definition fails
right there. The script below loads these definitions with placeholder keys, so
it needs no bucket and no network:

```text
loaded          Connections(gcs_hmac:google_cloud_platform, gcs_sa:google_cloud_platform, s3_chain:aws, s3_keys:aws, s3_minio:aws)

=== secrets never show up
password        False
get() is plain  example-hmac-secret

=== what is refused, when the connections are loaded
gcs: key id without secret  Value error, GCS HMAC keys need both `login` (key id) and `password` (secret).
gcs: two service accounts   Value error, Provide only one service account source, found `keyfile`, `key_path`.
s3: keys and a profile      Value error, `profile_name` cannot be combined with static keys (login/password): remove the keys to use the AWS credential chain.
s3: session token only      Value error, `aws_session_token` needs static keys (login/password).
```

* The secret is not in `repr(conns)`, so it cannot end up in a log.
* The last two lines are the S3 cases of [Amazon S3](s3.md).
* A missing credential is **not** an error: smallcat warns and the read goes on
  without credentials, so on a private bucket it fails with a permissions error
  from GCS.

!!! note
    The secrets are plain text in the YAML file. Keep the file out of version
    control, or load the connections from Airflow
    ([`Connections.from_airflow`](../../connections.md#where-connections-come-from)).

## The script

```python
--8<-- "docs/examples/connections_cloud.py"
```

Run it with `python connections_cloud.py`.
