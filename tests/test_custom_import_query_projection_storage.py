# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compile-only checks for isolated projection storage and completion evidence."""

from decimal import Decimal

import pytest
from sqlalchemy import CheckConstraint, UniqueConstraint, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.custom_import_query_projection import CustomImportQueryProjection
from process.custom_import import query_projection_storage as storage
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.storage_layout import SNAPSHOT_MODELS, snapshot_tables
from tests.test_custom_import_derived_query import derived_document


def _definition(*, enabled=None):
    document = derived_document()
    document["query"]["derived_fields"][2]["expression"]["field_id"] = "quantity"
    if enabled is not None:
        document["query"].update(filterable_fields=list(enabled), sortable_fields=[], order=[])
    return CustomImportDefinition.from_mapping(document)


@pytest.mark.parametrize("family_id", [True, False, None, [], {}, "1", "1; DROP SCHEMA public", 0, -1, 2**63])
def test_projection_names_require_a_native_snapshot_identity(family_id):
    with pytest.raises(ValueError, match="positive bigint"):
        storage.query_projection_tables(family_id, _definition())


@pytest.mark.parametrize("definition", [None, {}, (), "caller layout"])
def test_projection_shape_requires_a_parsed_definition(definition):
    with pytest.raises(ValueError, match="parsed definition"):
        storage.query_projection_tables(17, definition)
    with pytest.raises(ValueError, match="parsed definition"):
        storage.query_projection_index_count(definition)


def test_projection_cells_do_not_extend_or_redirect_existing_snapshots():
    definition = _definition()
    tables = storage.query_projection_tables(17, definition)
    assert tuple(tables) == storage.QUERY_PROJECTION_TABLE_NAMES
    assert len(snapshot_tables(17)) == len(SNAPSHOT_MODELS) == 15
    for table in tables.values():
        assert table.schema == "ci_snapshot_17"
        assert not table.foreign_keys
        assert not table.indexes
        assert set(table.c) >= {table.c.entity_binding_id, table.c.selected_value}
    old_query = str(select(tables["custom_import_derived_root"]).compile(dialect=postgresql.dialect()))
    storage.query_projection_tables(18, definition)
    assert "ci_snapshot_17.custom_import_derived_root" in old_query
    assert "ci_snapshot_18" not in old_query
    with pytest.raises(TypeError):
        tables["caller_table"] = None


def test_projection_ddl_uses_only_fixed_cells_native_keys_and_value_checks():
    definition = _definition()
    statements = storage.query_projection_load_statements(17, definition)
    assert len(statements) == 2
    assert all(
        statement.lstrip().startswith("CREATE TABLE ci_snapshot_17.custom_import_derived_") for statement in statements
    )
    assert all("PRIMARY KEY" in statement and "CHECK" in statement for statement in statements)
    root, child = storage.query_projection_tables(17, definition).values()
    assert tuple(root.c.keys()) == ("entity_binding_id", "selected_value", "field_1")
    assert tuple(child.c.keys()) == (
        "entity_binding_id",
        "selected_value",
        "child_key_sha256",
        "canonical_child_key",
        "field_2",
        "field_3",
    )
    assert root.c.field_1.nullable and child.c.field_2.nullable and child.c.field_3.nullable
    assert "field_1 <> 'NaN'::numeric" in statements[0]
    assert "field_3 BIGINT" in statements[1]
    assert not any(
        forbidden in statement
        for statement in statements
        for forbidden in ("FOREIGN KEY", "TRIGGER", "FUNCTION", "CREATE SCHEMA", "mrf.")
    )
    assert tuple(root.primary_key.columns.keys()) == ("entity_binding_id", "selected_value")
    assert tuple(child.primary_key.columns.keys()) == (
        "entity_binding_id",
        "selected_value",
        "child_key_sha256",
    )
    assert "canonical_child_key" not in child.primary_key.columns
    assert "octet_length(canonical_child_key) BETWEEN 1 AND 8192" in statements[1]
    assert "octet_length(child_key_sha256) = 32" in statements[1]
    assert child.c.canonical_child_key.type.collation == "C"


def test_decimal_cells_preserve_twelve_places_at_both_bigint_boundaries():
    definition = _definition()
    table = storage.query_projection_tables(17, definition)["custom_import_derived_root"]
    numeric = table.c.field_1.type
    assert (numeric.precision, numeric.scale) == (31, 12)
    for value in (-(2**63), 2**63 - 1):
        scaled = Decimal(f"{value}.000000000000")
        integer_digits = scaled.adjusted() + 1
        assert integer_digits + numeric.scale <= numeric.precision
    assert "field_1 NUMERIC(31, 12)" in storage.query_projection_load_statements(17, definition)[0]


def test_serving_indexes_keep_typed_values_and_stable_identity_on_one_candidate():
    definition = _definition()
    binding_indexes = set(snapshot_tables(17)["custom_import_entity_binding"].indexes)
    statements = storage.query_projection_index_statements(17, definition)
    assert len(statements) == storage.query_projection_index_count(definition) == 4
    assert all("ON ci_snapshot_17.custom_import_derived_" in statement for statement in statements[:3])
    assert all("WHERE" not in statement for statement in statements)
    assert "(selected_value, field_1, entity_binding_id)" in statements[0]
    assert "(selected_value, child_key_sha256, field_2, entity_binding_id)" in statements[1]
    assert "(selected_value, child_key_sha256, field_3, entity_binding_id)" in statements[2]
    assert statements[3] == (
        f"CREATE INDEX {storage.QUERY_PROJECTION_BINDING_INDEX} ON ci_snapshot_17.custom_import_entity_binding "
        '(dataset_id, adapter_id, (canonical_value COLLATE "C"), entity_binding_id)'
    )
    assert storage.query_projection_index_statements(17, definition) == statements
    assert all(not table.indexes for table in storage.query_projection_tables(17, definition).values())
    assert snapshot_tables(17)["custom_import_entity_binding"].indexes == binding_indexes


def test_indexes_cover_enabled_fields_and_include_nulls():
    assert storage.query_projection_index_statements(17, _definition(enabled=())) == ()
    assert storage.query_projection_index_count(_definition(enabled=())) == 0
    definition = _definition(enabled=("combined_quality",))
    statements = storage.query_projection_index_statements(17, definition)
    assert len(statements) == storage.query_projection_index_count(definition) == 2
    assert "field_2" in statements[0] and "WHERE" not in statements[0]
    assert storage.QUERY_PROJECTION_BINDING_INDEX in statements[1]
    tables = storage.query_projection_tables(17, definition)
    assert tuple(tables["custom_import_derived_root"].c.keys()) == ("entity_binding_id", "selected_value")
    assert (
        "field_2" in tables["custom_import_derived_child"].c
        and "field_3" not in tables["custom_import_derived_child"].c
    )
    assert "field_3" in storage.query_projection_tables(17, _definition())["custom_import_derived_child"].c


def test_manifest_has_native_owner_binding_and_explicit_completion_shape():
    table = CustomImportQueryProjection.__table__
    ddl = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    assert not CustomImportQueryProjection.__runtime_schema_sync__
    assert tuple(table.primary_key.columns.keys()) == ("family_id",)
    assert any(
        isinstance(constraint, UniqueConstraint) and tuple(constraint.columns.keys()) == ("generation_id",)
        for constraint in table.constraints
    )
    assert len(table.foreign_key_constraints) == 2
    assert "REFERENCES mrf.custom_import_snapshot_family (family_id) ON DELETE RESTRICT" in ddl
    assert (
        "REFERENCES mrf.custom_import_generation "
        "(generation_id, dataset_id, definition_revision_id, schema_revision_id)" in ddl
    )
    checks = " ".join(
        str(constraint.sqltext) for constraint in table.constraints if isinstance(constraint, CheckConstraint)
    )
    assert f"projection_contract = '{storage.QUERY_PROJECTION_CONTRACT}'" in checks
    assert "producing_fence > 0 AND octet_length(producing_token_sha256) = 32" in checks
    assert "after_entity_binding_id >= 0 AND root_count >= 0 AND child_count >= 0" in checks
    assert "complete_at IS NULL AND projection_sha256 IS NULL" in checks
    assert (
        "complete_at IS NOT NULL AND projection_sha256 IS NOT NULL AND octet_length(projection_sha256) = 32" in checks
    )
    for scope in ("root", "child"):
        for suffix in ("table_oid", "table_owner", "columns_sha256"):
            assert not table.c[f"{scope}_{suffix}"].nullable
    assert str(table.c.after_entity_binding_id.server_default.arg) == "0"
