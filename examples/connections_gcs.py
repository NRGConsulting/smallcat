# ruff: noqa: D103, INP001, T201
"""Define connections to Google Cloud Storage, without a network.

Run it with ``python docs/examples/connections_gcs.py``. It only loads and
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
""",
)

conns = Connections.from_dict(CONNECTIONS)
print("loaded         ", conns)

print("\n=== secrets never show up")
print("password       ", "example-hmac-secret" in repr(conns))
print("get() is plain ", conns.get("gcs_hmac")["password"])  # type: ignore[index]

print("\n=== what is refused, when the connections are loaded")
BAD = {
    "key id without secret": {
        "conn_type": "google_cloud_platform",
        "login": "GOOG1EXAMPLEKEYID",
        "extra": {"bucket": "b"},
    },
    "two service accounts": {
        "conn_type": "google_cloud_platform",
        "extra": {"bucket": "b", "key_path": "/a.json", "keyfile": "{}"},
    },
}
for label, definition in BAD.items():
    try:
        Connections.from_dict({"connections": {"bad": definition}})
    except ConnectionsValidationError as e:
        reason = str(e).splitlines()[1].strip()
        print(f"{label:22}", reason.removeprefix("- bad: "))
