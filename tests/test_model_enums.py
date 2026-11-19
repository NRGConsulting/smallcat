"""SC-054: `Enum` fields in models (type mapping, save, read, `where`)."""

from enum import Enum, IntEnum, StrEnum

import polars as pl
import pytest

from smallcat import Connections
from smallcat.models import Model, ModelRegistry, ModelWriteMode, use_registry
from smallcat.models.schema import UnsupportedFieldTypeError, dtype_of


class Color(StrEnum):
    RED = "red"
    BLUE = "blue"


class Level(IntEnum):
    LOW = 1
    HIGH = 2


class Plain(Enum):
    A = "a"
    B = "b"


class Mixed(Enum):
    A = "a"
    B = 1


class Bools(Enum):
    YES = True
    NO = False


@pytest.mark.parametrize(
    ("annotation", "dtype", "nullable"),
    [
        (Color, pl.String, False),
        (Level, pl.Int64, False),
        (Plain, pl.String, False),
        (Color | None, pl.String, True),
        (list[Level], pl.List(pl.Int64), False),
    ],
)
def test_enums_map_to_the_type_of_their_values(annotation, dtype, nullable):
    assert dtype_of(annotation) == (dtype, nullable)


@pytest.mark.parametrize("enum", [Mixed, Bools])
def test_enums_with_other_value_types_are_rejected(enum):
    with pytest.raises(UnsupportedFieldTypeError, match="all str or all int"):
        dtype_of(enum)


class Paint(Model):
    id: int
    color: Color
    level: Level
    kind: Plain
    other: Color | None = None
    levels: list[Level]

    class Meta:
        location = "paint.parquet"
        file_format = "parquet"


@pytest.fixture
def registry(tmp_path) -> ModelRegistry:
    reg = ModelRegistry()
    reg.load_connections(
        Connections.from_dict(
            {
                "connections": {
                    "local": {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}},
                },
            },
        ),
    )
    reg.register(Paint, connection="local")
    return reg


def _items() -> list[Paint]:
    return [
        Paint(
            id=1,
            color=Color.RED,
            level=Level.LOW,
            kind=Plain.A,
            levels=[Level.LOW],
        ),
        Paint(
            id=2,
            color=Color.BLUE,
            level=Level.HIGH,
            kind=Plain.B,
            other=Color.RED,
            levels=[Level.LOW, Level.HIGH],
        ),
    ]


def test_save_stores_values_and_as_models_returns_members(registry):
    with use_registry(registry):
        Paint.objects.save(_items(), mode=ModelWriteMode.OVERWRITE)
        df = Paint.objects.all()
        assert df.schema["color"] == pl.String
        assert df.schema["level"] == pl.Int64
        assert df["kind"].to_list() == ["a", "b"]
        assert df["levels"].to_list() == [[1], [1, 2]]
        assert Paint.objects.as_models() == _items()


@pytest.mark.parametrize(
    ("lookup", "ids"),
    [
        ({"color": Color.RED}, [1]),
        ({"level": Level.HIGH}, [2]),
        ({"kind": Plain.B}, [2]),
        ({"color__in": [Color.RED, Color.BLUE]}, [1, 2]),
        ({"kind__ne": Plain.A}, [2]),
        ({"other": None}, [1]),
        ({"other": Color.RED}, [2]),
    ],
)
def test_where_accepts_members(registry, lookup, ids):
    with use_registry(registry):
        Paint.objects.save(_items(), mode=ModelWriteMode.OVERWRITE)
        found = Paint.objects.where(**lookup).select("id").all()
        assert sorted(found["id"].to_list()) == ids
