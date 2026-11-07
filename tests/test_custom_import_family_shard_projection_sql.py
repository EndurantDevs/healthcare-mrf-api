# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compile/expression parity only; native input and completion proof stay separate."""

from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import BigInteger, Numeric, and_, case, literal, select
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql import visitors
from sqlalchemy.sql.util import find_tables

from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_read_sql import _state_and_kind
from process.custom_import.family_block_scalar_layout import query_scalar_fields, scalar_column
from process.custom_import.family_shard_projection_sql import projection_sources
from process.custom_import.materialization import GenerationIdentity
from process.custom_import.query_projection_sources import (
    child_hash_collision_statement,
    child_source,
    field_slots,
    root_source,
)
from process.custom_import.query_projection_values import ambiguity_statement, configured_fields, reduction_statement
from tests.test_custom_import_query_projection_sources import _context, _definition
from tests.test_custom_import_query_projection_storage import _definition as _integer_definition

_GENERATION = GenerationIdentity(4, 1, 2, 3)


def _binding(definition=None):
    definition = definition or _definition()
    layout = FamilyBlockLayout(definition, {"rates": 7})
    return FamilyBlockBinding(17, layout, layout.tables(17), {}, 9)


def _sql(expression):
    return str(expression.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))


def _scope_fields(definition, child):
    return tuple(field for field in configured_fields(definition) if (field.collection is not None) == child)


def _reduction(source, values_by_field, definition, child):
    return reduction_statement(
        source,
        _scope_fields(definition, child),
        values_by_field=values_by_field,
        field_slots=field_slots(definition),
    )


@pytest.mark.parametrize("reserved", [False, True])
def test_sources_keep_original_shapes_and_separate_field_ids(reserved):
    definition = _definition(reserved=reserved)
    binding = _binding(definition)
    roots, root_values, children, child_values = projection_sources(binding, _GENERATION)
    original = _context(definition)
    old_roots, old_root_values = root_source(original, [10])
    old_children, old_child_values = child_source(original, old_roots, 1)
    assert tuple(roots.c.keys()) == tuple(old_roots.c.keys())
    assert tuple(children.c.keys()) == tuple(old_children.c.keys())
    assert tuple(root_values) == tuple(old_root_values)
    assert tuple(child_values) == tuple(old_child_values)
    for current, previous in ((root_values, old_root_values), (child_values, old_child_values)):
        for field_id, expression in current.items():
            if isinstance(previous[field_id].type, Numeric | BigInteger):
                assert expression.type.compile(dialect=dialect()) == previous[field_id].type.compile(dialect=dialect())
            assert expression.name.startswith("field_") and expression.name != field_id
    physical_tables = set(find_tables(select(children), include_aliases=False))
    assert physical_tables == {
        binding.tables_by_role[role]
        for role in ("winner", "generation_family", "family", "query_record", "key_dictionary")
    }


def test_sources_join_exact_native_scope_members_family_and_dictionary():
    binding = _binding()
    roots, _root_values, children, _child_values = projection_sources(binding, _GENERATION)
    root_sql, child_sql = _sql(select(roots)), _sql(select(children))
    for name, scope_id in (
        ("generation_id", 4),
        ("dataset_id", 1),
        ("definition_revision_id", 2),
        ("schema_revision_id", 3),
    ):
        assert f"custom_import_winner.{name} = {scope_id}" in root_sql
        assert f"custom_import_generation_family.{name} = ci_snapshot_17.custom_import_winner.{name}" in root_sql
    for condition in (
        "custom_import_generation_family.family_revision_id = ci_snapshot_17.custom_import_winner.family_revision_id",
        "custom_import_block_family.entity_binding_id = ci_snapshot_17.custom_import_winner.entity_binding_id",
        "custom_import_block_family.root_record_id = ci_snapshot_17.custom_import_generation_family.root_record_id",
        "custom_import_winner.profile_slot = 1",
        "custom_import_winner.context_collection_slot = 0",
        "custom_import_winner.context_child_revision_id IS NULL",
    ):
        assert condition in root_sql
    assert (
        "custom_import_block_query_record.family_revision_id = block_projection_root_sources.family_revision_id"
        in child_sql
    )
    assert "custom_import_block_query_record.collection_slot = 7" in child_sql
    for name in ("key_rank", "key_variant"):
        assert f"custom_import_block_key.{name} = ci_snapshot_17.custom_import_block_query_record.{name}" in child_sql
    assert 'custom_import_block_key.canonical_child_key COLLATE "C"' in child_sql
    for forbidden in ("root_scalar", "child_scalar", "generation_seal", "LIMIT", "OFFSET", "json", "INSERT", "UPDATE"):
        assert forbidden not in child_sql


def test_profile_is_resolved_by_id_even_when_profile_order_changes():
    binding = _binding()
    definition = replace(
        binding.layout.definition, selection_profiles=binding.layout.definition.selection_profiles[::-1]
    )
    roots, _values, _children, _child_values = projection_sources(_binding(definition), _GENERATION)
    assert "custom_import_winner.profile_slot = 2" in _sql(select(roots))


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_raw_sources_use_exposed_state_position_and_physical_field_slot(child, reverse):
    definition = _definition()
    if reverse:
        definition = replace(
            definition,
            query=replace(
                definition.query,
                root_fields=definition.query.root_fields[::-1],
                child_fields=definition.query.child_fields[::-1],
            ),
        )
    binding = _binding(definition)
    roots, root_values, children, child_values = projection_sources(binding, _GENERATION)
    source, values_by_field = (children, child_values) if child else (roots, root_values)
    table = binding.tables_by_role["query_record" if child else "family"]
    exposed = query_scalar_fields(definition, "rates" if child else None)
    for field_id, column in values_by_field.items():
        field = definition.fields_by_id[field_id]
        state, override, _kind = _state_and_kind(table, exposed.index(field), field)
        expected = case((and_(state == 2, override == 0), table.c[scalar_column(field)]))
        assert source.element.selected_columns[column.name].element.compare(expected)
        assert scalar_column(field) == f"raw_field_{field.field_slot}"
    # The weight's source slot, old projection slot and exposed index are distinct.
    field = definition.fields_by_id["quantity" if child else "weight"]
    assert field.field_slot != field.projection_slot != exposed.index(field)


def test_root_selection_and_groups_filter_original_raw_values_only():
    binding = _binding()
    roots, values_by_field, _children, _child_values = projection_sources(binding, _GENERATION)
    predicates = roots.element._where_criteria
    selected = binding.layout.definition.query.entity_selection
    raw_selected = roots.element.selected_columns[values_by_field[selected.field_id].name].element
    raw_group = roots.element.selected_columns[values_by_field[selected.group_field_id].name].element
    assert any(predicate.compare(raw_selected.is_not(None)) for predicate in predicates)
    assert any(predicate.compare(raw_group.in_(selected.group_values)) for predicate in predicates)
    assert "segment_a" in _sql(select(roots)) and "segment_b" in _sql(select(roots))


@pytest.mark.parametrize("enabled", [(), ("combined_score",), ("combined_quality",), ("merged_cost",)])
def test_disabled_fields_keep_declaration_gaps_and_skip_only_disabled_child_scope(enabled):
    definition = _definition()
    definition = replace(
        definition, query=replace(definition.query, filterable_fields=enabled, sortable_fields=(), order_terms=())
    )
    roots, root_values, children, child_values = projection_sources(_binding(definition), _GENERATION)
    assert set(root_values) >= {
        definition.query.entity_selection.field_id,
        definition.query.entity_selection.group_field_id,
    }
    assert (children is None) == (not any(field.collection is not None for field in configured_fields(definition)))
    source = roots if "combined_score" in enabled else children
    if source is None:
        assert child_values == {}
        return
    is_child = source is children
    statement = _reduction(source, child_values if is_child else root_values, definition, is_child)
    expected_slots = {field_slots(definition)[field_id] for field_id in enabled}
    assert {name for name in statement.selected_columns.keys() if name.startswith("field_")} == {
        f"field_{slot}" for slot in expected_slots
    }


@pytest.mark.parametrize("child", [False, True])
def test_existing_complete_ambiguity_and_hash_guards_accept_the_sources(child):
    roots, _root_values, children, _child_values = projection_sources(_binding(), _GENERATION)
    source = children if child else roots
    statement = ambiguity_statement(source)
    assert {column.name for column in statement._group_by_clauses} == {
        "entity_binding_id",
        "selected_value",
        "group_value",
        *(("canonical_child_key",) if child else ()),
    }
    assert len(statement._where_criteria) == 0 and "HAVING count(*) > 1" in _sql(statement)
    if child:
        checks = child_hash_collision_statement(source)
        assert len(checks.selects) == 2
        assert {column.name for column in checks.selects[0]._group_by_clauses} == {
            "entity_binding_id",
            "selected_value",
            "child_key_sha256",
        }
        assert {column.name for column in checks.selects[1]._group_by_clauses} == {
            "entity_binding_id",
            "selected_value",
            "canonical_child_key",
        }


def _literal_reducer(expression, source, values_by_field, supplied_values):
    substitution_by_column = {source.c.group_value: literal("segment_a")}
    substitution_by_column.update(
        {column: literal(supplied_values[field_id], type_=column.type) for field_id, column in values_by_field.items()}
    )
    return visitors.replacement_traverse(expression, {}, substitution_by_column.get)


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize(
    ("value", "weight"),
    [
        (None, None),
        (None, 2),
        (Decimal("0"), 0),
        (Decimal("1.2"), -1),
        (Decimal("0.000000000001"), 1),
        (Decimal("-0.000000000001"), 1),
        (Decimal("999999999999999999.999999999999"), 1),
    ],
)
def test_adapter_reducers_keep_original_weight_and_half_even_expressions(child, value, weight):
    definition = _definition()
    roots, root_values, children, child_values = projection_sources(_binding(definition), _GENERATION)
    old = _context(definition)
    old_roots, old_root_values = root_source(old, [10])
    old_children, old_child_values = child_source(old, old_roots, 1)
    source, values_by_field = (children, child_values) if child else (roots, root_values)
    previous, previous_values = (old_children, old_child_values) if child else (old_roots, old_root_values)
    field = _scope_fields(definition, child)[0]
    supplied = dict.fromkeys(values_by_field)
    supplied[field.source_field_id], supplied[field.weight_field_id] = value, weight
    current_metric = _reduction(source, values_by_field, definition, child).selected_columns[
        f"field_{field_slots(definition)[field.field_id]}"
    ]
    previous_metric = _reduction(previous, previous_values, definition, child).selected_columns[current_metric.name]
    current = _literal_reducer(current_metric, source, values_by_field, supplied)
    original = _literal_reducer(previous_metric, previous, previous_values, supplied)
    compiled, previous_sql = current.compile(dialect=dialect()), original.compile(dialect=dialect())
    # SQL semantics and all bound inputs must match; old VARCHAR versus compact
    # TEXT CTE metadata is intentionally not an expression-object identity test.
    assert str(compiled) == str(previous_sql) and compiled.params == previous_sql.params
    assert "div(" in str(compiled) and "mod(" in str(compiled) and "round(" not in str(compiled)
    assert (current.type.precision, current.type.scale) == (31, 12)


@pytest.mark.parametrize("first", [None, 0, 9223372036854775807])
def test_preferred_reducer_keeps_original_group_order_and_integer_domain(first):
    definition = _integer_definition()
    _roots, _root_values, children, values_by_field = projection_sources(_binding(definition), _GENERATION)
    old = _context(definition)
    old_roots, _old_root_values = root_source(old, [10])
    old_children, old_values = child_source(old, old_roots, 1)
    current_metric = _reduction(children, values_by_field, definition, True).selected_columns.field_3
    old_metric = _reduction(old_children, old_values, definition, True).selected_columns.field_3
    supplied = dict.fromkeys(values_by_field)
    supplied["quantity"] = first
    current = _literal_reducer(current_metric, children, values_by_field, supplied).compile(dialect=dialect())
    original = _literal_reducer(old_metric, old_children, old_values, supplied).compile(dialect=dialect())
    assert str(current) == str(original) and current.params == original.params
    assert isinstance(current_metric.type, BigInteger)
    semantic_sql = _sql(current_metric)
    assert "coalesce(" in semantic_sql and "div(" not in semantic_sql
    assert semantic_sql.index("segment_a") < semantic_sql.index("segment_b")


@pytest.mark.parametrize("bad", [None, {}, object()])
def test_foreign_candidate_or_generation_is_refused(bad):
    with pytest.raises(ValueError, match="exact candidate"):
        projection_sources(bad, _GENERATION)
    with pytest.raises(ValueError, match="exact candidate"):
        projection_sources(_binding(), bad)


def test_incomplete_roles_invalid_generation_and_ungrouped_definition_are_refused():
    binding = _binding()
    with pytest.raises(ValueError, match="role census"):
        projection_sources(replace(binding, tables_by_role={"family": binding.tables_by_role["family"]}), _GENERATION)
    with pytest.raises(ValueError, match="generation_id"):
        projection_sources(binding, GenerationIdentity(2**63, 1, 2, 3))
    definition = replace(
        binding.layout.definition, query=replace(binding.layout.definition.query, entity_selection=None)
    )
    with pytest.raises(ValueError, match="grouped entity"):
        projection_sources(_binding(definition), _GENERATION)
