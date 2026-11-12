# ruff: noqa: D103, INP001, T201
"""Define connections to Google Cloud Storage and Amazon S3, without a network.

Run it with ``python docs/examples/connections_cloud.py``. It only loads and
validates the definitions, so it needs no bucket and no credentials: the keys
below are placeholders. Nothing is read or written.
"""

import yaml

from smallcat import Connections
from smallcat.connections_config import ConnectionsValidationError

CONNECTIONS = yaml.safe_load(
    """
connections:
  gcs_hmac:                       # HMAC keys: DuckDB
    conn_type: google_cloud_platform
    login: GOOG1EXAMPLEKEYID      # HMAC key id
    password: example-hmac-secret # HMAC secret
    extra:
      bucket: my-bucket
      prefix: raw
  gcs_sa:                         # service account: Polars and delta-rs
    conn_type: google_cloud_platform
    extra:
      bucket: my-bucket
      key_path: /secrets/service-account.json
  s3_chain:                       # IAM role / AWS credential chain
    conn_type: aws
    extra:
      bucket: my-bucket
      prefix: raw
      region_name: eu-west-1
  s3_keys:                        # static keys
    conn_type: aws
    login: AKIAEXAMPLEKEYID
    password: example-secret-access-key
    extra:
      bucket: my-bucket
      region_name: eu-west-1
  s3_minio:                       # an S3-compatible store
    conn_type: aws
    login: minio
    password: minio-secret
    extra:
      bucket: my-bucket
      endpoint_url: http://minio:9000
""",
)

conns = Connections.from_dict(CONNECTIONS)
print("loaded         ", conns)

print("\n=== secrets never show up")
print("password       ", "example-hmac-secret" in repr(conns))
print("get() is plain ", conns.get("gcs_hmac")["password"])  # type: ignore[index]

print("\n=== what is refused, when the connections are loaded")
BAD = {
    "gcs: key id without secret": {
        "conn_type": "google_cloud_platform",
        "login": "GOOG1EXAMPLEKEYID",
        "extra": {"bucket": "b"},
    },
    "gcs: two service accounts": {
        "conn_type": "google_cloud_platform",
        "extra": {"bucket": "b", "key_path": "/a.json", "keyfile": "{}"},
    },
    "s3: keys and a profile": {
        "conn_type": "aws",
        "login": "AKIAEXAMPLEKEYID",
        "password": "example-secret-access-key",
        "extra": {"bucket": "b", "profile_name": "dev"},
    },
    "s3: session token only": {
        "conn_type": "aws",
        "extra": {"bucket": "b", "aws_session_token": "token"},
    },
}
for label, definition in BAD.items():
    try:
        Connections.from_dict({"connections": {"bad": definition}})
    except ConnectionsValidationError as e:
        reason = str(e).splitlines()[1].strip()
        print(f"{label:27}", reason.removeprefix("- bad: "))
