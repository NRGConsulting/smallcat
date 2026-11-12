# ruff: noqa: D103, INP001, T201
"""Define connections to Amazon S3, without a network.

Run it with ``python docs/examples/connections_s3.py``. It only loads and
validates the definitions, so it needs no bucket and no credentials: the keys
below are placeholders. Nothing is read or written.
"""

import yaml

from smallcat import Connections
from smallcat.connections_config import ConnectionsValidationError

CONNECTIONS = yaml.safe_load(
    """
connections:
  s3_chain:                       # IAM role / AWS credential chain
    conn_type: aws
    extra:
      bucket: my-bucket
      prefix: raw
      region_name: eu-west-1
  s3_profile:                     # a profile of ~/.aws/config
    conn_type: aws
    extra:
      bucket: my-bucket
      profile_name: dev
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
print("password       ", "example-secret-access-key" in repr(conns))
print("get() is plain ", conns.get("s3_keys")["password"])  # type: ignore[index]

print("\n=== what is refused, when the connections are loaded")
BAD = {
    "key id without secret": {
        "conn_type": "aws",
        "login": "AKIAEXAMPLEKEYID",
        "extra": {"bucket": "b"},
    },
    "keys and a profile": {
        "conn_type": "aws",
        "login": "AKIAEXAMPLEKEYID",
        "password": "example-secret-access-key",
        "extra": {"bucket": "b", "profile_name": "dev"},
    },
    "session token only": {
        "conn_type": "aws",
        "extra": {"bucket": "b", "aws_session_token": "token"},
    },
}
for label, definition in BAD.items():
    try:
        Connections.from_dict({"connections": {"bad": definition}})
    except ConnectionsValidationError as e:
        reason = str(e).splitlines()[1].strip()
        print(f"{label:22}", reason.removeprefix("- bad: "))
