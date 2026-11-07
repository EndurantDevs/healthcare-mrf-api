# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compile-only scalar and derived-view compatibility for pinned block layouts."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import String, inspect, select
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql.util import find_tables

from db.models.custom_import import CustomImportChildScalar, CustomImportRootScalar
from process.custom_import import read_core
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import FamilyBlockBinding, FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_read_sql import field_scalar_model, query_projection_views
from process.custom_import.query_projection_storage import query_projection_tables
from tests.test_custom_import_query_projection_storage import _definition as derived_definition
from tests.test_custom_import_snowflake_bundle import _definition

KINDS = ("string", "integer", "decimal", "boolean", "date", "timestamp")


def definition():
    document = json.loads(_definition().canonical)
    root, child = document["schema"]["root"]["fields"], document["schema"]["children"][0]["fields"]
    for item in root:
        item["projection_slot"] = item["slot"]
    child[1]["projection_slot"], child[2]["projection_slot"] = 4, 5
    for slot, name, kind in ((7, "root_decimal", "decimal"), (8, "root_date", "date"), (9, "root_time", "timestamp")):
        root.append({"id": name, "slot": slot, "type": kind, "nullable": True, "projection_slot": slot})
    for slot, kind in enumerate(("integer", "boolean", "date", "timestamp"), 10):
        child.append({"id": "child_" + kind, "slot": slot, "type": kind, "nullable": True, "projection_slot": slot})
    document["query"] = {
        "root_fields": [item["id"] for item in root],
        "child": {"collection": "details", "fields": [item["id"] for item in child if "projection_slot" in item]},
        "order": [],
    }
    return CustomImportDefinition.from_mapping(document)


def context(parsed=None):
    parsed = parsed or definition()
    slots_by_collection = {item.name: index + 7 for index, item in enumerate(parsed.child_collections)}
    layout = FamilyBlockLayout(parsed, slots_by_collection)
    binding = FamilyBlockBinding(17, layout, layout.tables(17), {}, 1)
    return SimpleNamespace(
        definition=parsed,
        family_block_binding=binding,
        collection_slots_by_name=slots_by_collection,
        target=SimpleNamespace(dataset_id=11, schema_revision_id=31),
    )


def compiled(statement):
    return str(statement.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))


def field_for(ctx, kind, child):
    fields = ctx.definition.child_fields if child else ctx.definition.root_fields
    return next(item for item in fields if item.value_type == kind and item.projection_slot is not None)


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("kind", KINDS)
def test_scalar_alias_has_complete_original_column_shape_and_types(child, kind):
    ctx = context()
    field = field_for(ctx, kind, child)
    alias = field_scalar_model(ctx, field)
    original = CustomImportChildScalar if child else CustomImportRootScalar
    relation = inspect(alias).selectable
    assert tuple(relation.c.keys()) == tuple(original.__table__.c.keys())
    for column in original.__table__.c:
        assert str(relation.c[column.name].type.compile(dialect=dialect())) == str(
            column.type.compile(dialect=dialect())
        )
    assert isinstance(relation.c.field_type.type, String) and relation.c.field_type.type.length == 16
    sql = compiled(select(alias))
    assert "CAST(11 AS BIGINT)" in sql and "CAST(31 AS BIGINT)" in sql
    assert f"CAST({field.field_slot} AS SMALLINT)" in sql
    assert f"CAST({field.projection_slot} AS SMALLINT)" in sql
    assert f"raw_field_{field.field_slot}" in sql
    assert "CAST(family_block" not in sql


@pytest.mark.parametrize("child", [False, True])
def test_scalar_views_read_only_the_pinned_native_roles(child):
    ctx = context()
    alias = field_scalar_model(ctx, field_for(ctx, "integer", child))
    tables = set(find_tables(select(alias), include_aliases=False))
    expected_items = {ctx.family_block_binding.tables_by_role["family"]}
    if child:
        expected_items.add(ctx.family_block_binding.tables_by_role["query_record"])
    assert tables == expected_items
    sql = compiled(select(alias))
    assert "mrf.custom_import" not in sql and "child_scalar" not in sql and "root_scalar" not in sql
    if child:
        assert "custom_import_block_query_record JOIN ci_snapshot_17.custom_import_block_family ON" in sql
        assert (
            "custom_import_block_query_record.family_revision_id = ci_snapshot_17.custom_import_block_family.family_revision_id"
            in sql
        )
        assert "custom_import_block_query_record.collection_slot = 7" in sql
    else:
        assert " JOIN " not in sql


@pytest.mark.parametrize("child", [False, True])
def test_missing_null_and_mismatched_kind_do_not_collapse(child):
    ctx = context()
    field = field_for(ctx, "integer", child)
    alias = field_scalar_model(ctx, field)
    base = compiled(select(alias))
    assert "raw_type_overrides IS NULL" in base
    assert "WHEN 0 THEN 'integer'" in base
    for code, kind in enumerate(KINDS, 1):
        assert f"WHEN {code} THEN '{kind}'" in base
    assert "WHEN 1 THEN 'null' WHEN 2 THEN 'value' END" in base
    assert "& 3) != 0" in base
    assert " = 0) THEN ci_snapshot_17." in base
    assert "AS integer_value" in base and "CAST(NULL AS NUMERIC(30, 12)) AS decimal_value" in base
    for operator, expected in (
        ("is_missing", "NOT (EXISTS"),
        ("is_null", "value_state = 'null'"),
        ("eq", "value_state = 'value'"),
    ):
        predicate = SimpleNamespace(field=field, operator=operator, value=7)
        sql = compiled(select(read_core._scalar_predicate(alias, predicate, (alias.field_slot == field.field_slot,))))
        assert expected in sql
        assert "& 3) != 0" in sql
        if operator == "eq":
            assert ".integer_value = 7" in sql


def test_packed_indexes_follow_declaration_order_across_byte_boundaries():
    ctx = context()
    root = ctx.definition.fields_by_id["root_time"]
    child = ctx.definition.fields_by_id["child_timestamp"]
    for field, role in ((root, "family"), (child, "query_record")):
        sql = compiled(select(field_scalar_model(ctx, field)))
        name = ctx.family_block_binding.tables_by_role[role].name
        assert f"get_byte(ci_snapshot_17.{name}.raw_states, 1) >> 2" in sql
        assert f"get_byte(ci_snapshot_17.{name}.raw_type_overrides, 2) >> 4" in sql


def test_unqueryable_foreign_or_mismatched_scope_is_refused():
    ctx = context()
    hidden = ctx.definition.fields_by_id["detail_npi"]
    with pytest.raises(FamilyBlockUnavailable, match="not queryable"):
        field_scalar_model(ctx, hidden)
    field = ctx.definition.fields_by_id["score"]
    with pytest.raises(FamilyBlockUnavailable, match="not queryable"):
        field_scalar_model(ctx, replace(field, field_slot=19))
    ctx.target.dataset_id = True
    with pytest.raises(FamilyBlockUnavailable, match="bounded integer"):
        field_scalar_model(ctx, field)
    ctx.target.dataset_id = 11
    ctx.collection_slots_by_name = {"details": 1}
    with pytest.raises(FamilyBlockUnavailable, match="collection differs"):
        field_scalar_model(ctx, field_for(ctx, "decimal", True))


@pytest.mark.parametrize("enabled", [None, (), ("combined_quality",)])
def test_derived_views_preserve_existing_shape_and_numeric_domains(enabled):
    ctx = context(derived_definition(enabled=enabled))
    views = query_projection_views(ctx.family_block_binding)
    expected = query_projection_tables(17, ctx.definition)
    assert tuple(views) == tuple(expected)
    for name, view in views.items():
        assert tuple(view.c.keys()) == tuple(expected[name].c.keys())
        for column in expected[name].c:
            assert str(view.c[column.name].type.compile(dialect=dialect())) == str(
                column.type.compile(dialect=dialect())
            )
    with pytest.raises(TypeError):
        views["another_table"] = None


def test_derived_child_uses_both_dictionary_ids_without_new_ordering_or_hashes():
    ctx = context(derived_definition())
    view = query_projection_views(ctx.family_block_binding)["custom_import_derived_child"]
    sql = compiled(select(view))
    assert "custom_import_block_query_child.key_rank = ci_snapshot_17.custom_import_block_key.key_rank" in sql
    assert "custom_import_block_query_child.key_variant = ci_snapshot_17.custom_import_block_key.key_variant" in sql
    assert "custom_import_block_key.child_key_sha256" in sql
    assert "custom_import_block_key.canonical_child_key" in sql
    assert "ORDER BY" not in sql and "DISTINCT" not in sql and "digest(" not in sql
    assert set(find_tables(select(view), include_aliases=False)) == {
        ctx.family_block_binding.tables_by_role["query_child"],
        ctx.family_block_binding.tables_by_role["key_dictionary"],
    }
