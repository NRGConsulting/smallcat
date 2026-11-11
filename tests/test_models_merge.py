"""Models: `ModelWriteMode.MERGE`, `Meta.merge_keys` and `save(merge_keys=...)`."""

import pathlib
from typing import ClassVar

import polars as pl
import pytest

from smallcat import Connections
from smallcat.models import (
    Model,
    ModelDefinitionError,
    ModelQueryError,
    ModelRegistrationError,
    ModelRegistry,
    ModelUnsupportedWriteModeError,
    ModelWriteMode,
    ModelWriteNotAllowedError,
    use_registry,
)

MERGE = ModelWriteMode.MERGE


class Customer(Model):
    id: int
    name: str
    email: str

    class Meta:
        location = "silver/customers"
        file_format = "delta_table"
        merge_keys = ["id"]


class NoKeys(Model):
    id: int
    name: str

    class Meta:
        location = "silver/no_keys"
        file_format = "delta_table"


class ParquetCustomer(Model):
    id: int
    name: str

    class Meta:
        location = "customers.parquet"
        file_format = "parquet"


@pytest.fixture
def base(tmp_path) -> pathlib.Path:
    return tmp_path


@pytest.fixture
def registry(base):
    reg = ModelRegistry()
    reg.load_connections(
        Connections.from_dict(
            {
                "connections": {
                    "local": {"conn_type": "fs", "extra": {"base_path": str(base)}},
                },
            },
        ),
    )
    for model in (Customer, NoKeys, ParquetCustomer):
        reg.register(model, connection="local")
    with use_registry(reg):
        yield reg


def _df(ids, names=None, emails=None):
    return pl.DataFrame(
        {
            "id": ids,
            "name": names or [f"n{i}" for i in ids],
            "email": emails or [f"{i}@x" for i in ids],
        },
    )


def test_merge_uses_the_keys_of_meta(registry):
    Customer.objects.save(_df([1, 2]), mode="merge")
    Customer.objects.save(_df([2, 3], names=["B", "c"]), mode=MERGE)
    out = Customer.objects.all().sort("id")
    assert out["id"].to_list() == [1, 2, 3]
    assert out["name"].to_list() == ["n1", "B", "c"]


def test_keys_of_the_call_take_precedence_over_meta(registry):
    Customer.objects.save(_df([1, 2], emails=["a@x", "b@x"]), mode="merge")
    # Same email, different id: with keys=["email"] it is an update, not an insert
    Customer.objects.save(
        _df([10], names=["renamed"], emails=["a@x"]),
        mode="merge",
        merge_keys=["email"],
    )
    out = Customer.objects.all().sort("email")
    assert out.height == 2
    assert out.row(0, named=True) == {"id": 10, "name": "renamed", "email": "a@x"}


def test_merge_accepts_model_instances(registry):
    Customer.objects.save([Customer(id=1, name="a", email="a@x")], mode="merge")
    assert Customer.objects.all().height == 1


def test_model_without_keys_fails_when_saving_not_when_registering(registry):
    df = pl.DataFrame({"id": [1], "name": ["a"]})
    with pytest.raises(ModelQueryError, match="no merge keys"):
        NoKeys.objects.save(df, mode="merge")
    NoKeys.objects.save(df, mode="append")


def test_keys_of_the_call_make_a_keyless_model_work(registry):
    df = pl.DataFrame({"id": [1], "name": ["a"]})
    NoKeys.objects.save(df, mode="merge", merge_keys=["id"])
    assert NoKeys.objects.all().height == 1


def test_merge_keys_with_another_mode_is_an_error(registry, base):
    with pytest.raises(ModelQueryError, match="only applies to mode='merge'"):
        Customer.objects.save(_df([1]), mode="append", merge_keys=["id"])
    assert not (base / "silver").exists()


def test_a_key_that_is_not_a_field_fails_before_writing(registry, base):
    with pytest.raises(ModelQueryError, match="not fields of Customer"):
        Customer.objects.save(_df([1]), mode="merge", merge_keys=["nope"])
    assert not (base / "silver").exists()


def test_repeated_and_string_keys_are_rejected(registry):
    with pytest.raises(ModelQueryError, match="repeated"):
        Customer.objects.save(_df([1]), mode="merge", merge_keys=["id", "id"])
    with pytest.raises(ModelQueryError, match="not a string"):
        Customer.objects.save(_df([1]), mode="merge", merge_keys="id")


def test_duplicate_keys_in_the_data_fail_before_writing(registry, base):
    with pytest.raises(ValueError, match="several rows"):
        Customer.objects.save(_df([1, 1]), mode="merge")
    assert not (base / "silver").exists()


def test_merge_on_a_non_delta_format_is_unsupported(registry, base):
    with pytest.raises(ModelUnsupportedWriteModeError, match="only Delta tables"):
        ParquetCustomer.objects.save(
            pl.DataFrame({"id": [1], "name": ["a"]}),
            mode="merge",
            merge_keys=["id"],
        )
    assert not (base / "customers.parquet").exists()


def test_allowed_write_modes_can_exclude_merge(registry):
    registry.register(
        Customer,
        connection="local",
        allowed_write_modes=["append"],
        replace=True,
    )
    with pytest.raises(ModelWriteNotAllowedError, match="does not allow 'merge'"):
        Customer.objects.save(_df([1]), mode="merge")


def test_update_columns_from_meta_save_options(registry):
    class Partial(Model):
        id: int
        name: str
        city: str

        class Meta:
            location = "partial"
            file_format = "delta_table"
            merge_keys = ["id"]
            save_options = {"update_columns": ["name"]}

    registry.register(Partial, connection="local")
    one = pl.DataFrame({"id": [1], "name": ["a"], "city": ["x"]})
    two = pl.DataFrame({"id": [1, 2], "name": ["A", "b"], "city": ["Y", "z"]})
    Partial.objects.save(one, mode="merge")
    Partial.objects.save(two, mode="merge")
    out = Partial.objects.all().sort("id")
    assert out["name"].to_list() == ["A", "b"]
    assert out["city"].to_list() == ["x", "z"]
    # the option does not break the other modes
    Partial.objects.save(one, mode="append")


# ---------- registration / definition ----------


def _define(**meta):
    attrs = {"location": "m", "file_format": "delta_table", **meta}
    meta_cls = type("Meta", (), attrs)

    class M(Model):
        id: int
        name: str
        note: str | None = None

        Meta: ClassVar[type] = meta_cls

    return M


def test_register_rejects_keys_that_are_not_fields(registry):
    model = _define(merge_keys=["id", "ghost"])
    with pytest.raises(
        ModelRegistrationError,
        match=r"\['ghost'\].*fields: \['id', 'name', 'note'\]",
    ):
        registry.register(model, connection="local")


@pytest.mark.parametrize(
    ("keys", "message"),
    [([], "empty"), (["id", "id"], "repeated"), (["note"], "nullable")],
)
def test_register_rejects_empty_repeated_and_nullable_keys(registry, keys, message):
    model = _define(merge_keys=keys)
    with pytest.raises(ModelRegistrationError, match=message):
        registry.register(model, connection="local")


def test_replace_revalidates_the_keys(registry):
    registry.register(Customer, connection="local", replace=True)
    model = _define(merge_keys=["nope"])
    with pytest.raises(ModelRegistrationError):
        registry.register(model, connection="local", replace=True)


def test_register_rejects_update_columns_that_are_not_fields(registry):
    model = _define(merge_keys=["id"], save_options={"update_columns": ["ghost"]})
    with pytest.raises(ModelRegistrationError, match="update_columns"):
        registry.register(model, connection="local")


def test_meta_merge_keys_on_a_non_delta_format_is_a_definition_error():
    with pytest.raises(ModelDefinitionError, match="only valid for delta_table"):

        class Bad(Model):
            id: int

            class Meta:
                location = "x.parquet"
                file_format = "parquet"
                merge_keys = ["id"]


def test_meta_save_options_cannot_carry_merge_keys():
    with pytest.raises(ModelDefinitionError, match="use Meta.merge_keys"):
        _define(save_options={"merge_keys": ["id"]})
