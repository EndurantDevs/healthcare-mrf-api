# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Raw query cells preserve independent legacy scalar kinds and field states."""

from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import BigInteger, Column, MetaData, Table
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateTable

from process.custom_import.family_block_scalar_layout import (
    append_scalar_columns,
    pack_query_scalars,
    query_scalar_fields,
)
from process.custom_import.materialization import TypedScalar

KINDS = ("string", "integer", "decimal", "boolean", "date", "timestamp")
VALUES = ("text", 7, Decimal("1.200000000000"), False, date(2026, 1, 2), datetime(2026, 1, 2, tzinfo=timezone.utc))


def field(index, kind):
    return SimpleNamespace(field_id=f"f{index}", field_slot=index + 1, value_type=kind, nullable=False)


@pytest.mark.parametrize("kind,value", zip(KINDS, VALUES, strict=True))
def test_raw_missing_null_value_are_independent(kind, value):
    fields = (field(0, kind), field(1, kind), field(2, kind))
    scalar = TypedScalar(kind, "value", **{f"{kind}_value": value})
    result = pack_query_scalars(fields, {"f1": TypedScalar(kind, "null"), "f2": scalar})
    assert result == (b"$", None, None, None, value)
    assert result[-1] is value


@pytest.mark.parametrize("declared", KINDS)
@pytest.mark.parametrize("actual,value", zip(KINDS, VALUES, strict=True))
def test_retained_kind_override_is_not_changed_to_missing_or_null(declared, actual, value):
    fields = (field(0, declared),)
    scalar = TypedScalar(actual, "value", **{f"{actual}_value": value})
    states, overrides, cell = pack_query_scalars(fields, {"f0": scalar})
    assert states == b"\x02"
    assert overrides == (None if actual == declared else bytes([KINDS.index(actual) + 1]))
    assert cell == (value if actual == declared else None)
    states, overrides, cell = pack_query_scalars(fields, {"f0": TypedScalar(actual, "null")})
    assert states == b"\x01" and cell is None
    assert overrides == (None if actual == declared else bytes([KINDS.index(actual) + 1]))


def test_states_and_kind_overrides_cross_byte_boundaries():
    fields = tuple(field(index, "decimal") for index in range(9))
    scalars_by_name = {
        item.field_id: TypedScalar("integer", "value", integer_value=index) for index, item in enumerate(fields)
    }
    states, overrides, *cells = pack_query_scalars(fields, scalars_by_name)
    assert states == b"\xaa\xaa\x02"
    assert overrides == b'""""\x02'
    assert cells == [None] * 9
    assert pack_query_scalars((), {}) == (b"", None)


def test_only_declared_query_collection_is_projected():
    root, child, hidden = field(0, "integer"), field(1, "decimal"), field(2, "string")
    definition = SimpleNamespace(
        fields_by_id={item.field_id: item for item in (root, child, hidden)},
        query=SimpleNamespace(root_fields=("f0",), child_fields=("f1",), child_collection="details"),
    )
    assert query_scalar_fields(definition) == (root,)
    assert query_scalar_fields(definition, "details") == (child,)
    with pytest.raises(ValueError, match="collection differs"):
        query_scalar_fields(definition, "hidden")


def test_native_shape_uses_existing_typed_domains_and_coupled_presence():
    table = Table("raw_query_fixture", MetaData(), Column("revision_id", BigInteger, primary_key=True))
    append_scalar_columns(table, tuple(field(index, kind) for index, kind in enumerate(KINDS)))
    ddl = str(CreateTable(table).compile(dialect=dialect()))
    assert "NUMERIC(30, 12)" in ddl and "TIMESTAMP WITH TIME ZONE" in ddl
    assert "raw_type_overrides IS NULL" in ddl and "octet_length(raw_states)=2" in ddl
    assert "raw_field_3<>'NaN'::numeric" in ddl


def test_malformed_scalar_is_refused():
    with pytest.raises(ValueError, match="malformed"):
        pack_query_scalars((field(0, "integer"),), {"f0": 3})
