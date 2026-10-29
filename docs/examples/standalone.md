# smallcat-chonk standalone
Use smallcat-chonk without any dependencies.

## Setup
### Install smallcat-chonk

```sh
pip install smallcat-chonk
```

or

```sh
uv add smallcat-chonk
```

### Create an example catalog
Create a file named `catalog.yaml` with the following contents.

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/NRGConsulting/smallcat/refs/heads/main/schemas/catalog.schema.json
entries:
  foo:
    file_format: csv
    connection:
      conn_type: fs
      extra:
        base_path: /tmp/
    location: foo.csv
    save_options:
      header: true
    load_options:
      header: true

```

The `# yaml-language-server` line gives you intellisense in VS code, so it will validate and auto-complete your catalog.

## Usage

### Create main.py

Create a python file with the following contents:

```python
import pandas as pd
from smallcat.catalog import Catalog


def get_example_dataset() -> pd.DataFrame:
    url = "https://raw.githubusercontent.com/vega/vega-datasets/master/data/seattle-weather.csv"
    return pd.read_csv(url, parse_dates=["date"])


def main():
    print("Hello from smallcat-local-example!")

    catalog = Catalog.from_yaml('catalog.yaml')
    catalog.save_pandas('foo', df=get_example_dataset())

    df = catalog.load_pandas('foo', where="precipitation > 0")
    print(df.head())


if __name__ == "__main__":
    main()
```

### Run the file

```sh
uv run main.py
```

Output:

```
Hello from smallcat-local-example!
        date  precipitation  temp_max  temp_min  wind  weather
0 2012-01-01            0.0      12.8       5.0   4.7  drizzle
1 2012-01-02           10.9      10.6       2.8   4.5     rain
2 2012-01-03            0.8      11.7       7.2   2.3     rain
3 2012-01-04           20.3      12.2       5.6   4.7     rain
4 2012-01-05            1.3       8.9       2.8   6.1     rain
```
