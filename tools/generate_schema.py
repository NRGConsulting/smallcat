"""Generate JSON schema for catalog."""

import json
from pathlib import Path

from smallcat.catalog import Catalog


def main() -> None:
    """Generate JSON schema for catalog.

    To be used with VS code YAML extension for IntelliSense
    """
    schema = Catalog.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = (
        "https://raw.githubusercontent.com/NRGConsulting/smallcat/main/schemas/catalog.schema.json"
    )
    output_path = Path(__file__).parent.parent / "schemas/catalog.schema.json"

    with output_path.open("w") as f:
        json.dump(schema, f, indent=2)


if __name__ == "__main__":
    main()
