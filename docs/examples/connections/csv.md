# Connect to CSV files

A connection to a folder of files on the local filesystem. It says **where** the
files are; what is in them and how they are read is up to whatever uses the
connection. It needs nothing but smallcat-chonk, and the script writes its file
under a temporary directory.

```sh
pip install smallcat-chonk
python connections_csv.py
```

## Define it

```yaml
# connections.yaml
connections:
  local:
    conn_type: fs
    extra:
      base_path: /data         # or base_uri: file:///data
```

`fs` has no credentials. `base_path` is the folder every location is relative to:
asking for `people.csv` through this connection means `/data/people.csv`. Without
`extra`, locations are relative to the working directory.

Each connection has a **name** (`local`). Whatever uses it refers to it by that
name, never by its content.

## Load it

```python
from smallcat import Connections

conns = Connections.from_yaml("connections.yaml")
```

It is validated here, so a mistake fails at start-up and not on the first read.

## The script

```python
--8<-- "docs/examples/connections_csv.py"
```

## What it prints

```text
=== what is loaded
repr            Connections(local:fs)
names           ['local']
'local' in      True

=== the definition, as a dataset receives it
conn_type       fs
base_path       data
```

`repr` shows names and types only, so a credential can never end up in a log. `get`
hands out the definition as a plain dictionary, ready for a dataset.

A wrong definition is refused when it is loaded. Unknown keys are rejected, so a
typo is reported instead of silently ignored:

```text
=== a typo is reported, not ignored
Invalid connections:
  - local.extra.base_pth: Extra inputs are not permitted
```

## Next

The same file can hold connections to a bucket:
[Google Cloud Storage](gcs.md) and [Amazon S3](s3.md). See
[Connections](../../connections.md) for the loaders and the errors.
