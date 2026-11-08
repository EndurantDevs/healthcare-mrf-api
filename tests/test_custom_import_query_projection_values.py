# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Configured derived contexts keep their scope and exact numeric types."""

from dataclasses import replace

import pytest
from sqlalchemy import BigInteger, LargeBinary, Numeric, String, column, values
from sqlalchemy.dialects import postgresql

from process.custom_import.definition import CustomImportDefinition, SortTerm
from process.custom_import.derived_query import DerivedQueryField
from process.custom_import.query_projection_values import (
    ambiguity_statement,
    configured_fields,
    reduction_statement,
)
from tests.test_custom_import_derived_query import derived_document


def _fields(*, child=False):
    scope = {"collection": "details", "child_key": ("detail_id",)} if child else {"collection": None, "child_key": ()}
    return (
        DerivedQueryField(
            field_id="combined_metric",
            value_type="decimal",
            kind="group_weighted_mean",
            source_field_id="metric",
            weight_field_id="weight",
            group_values=("segment_a", "segment_b"),
            **scope,
        ),
        DerivedQueryField(
            field_id="preferred_weight",
            value_type="integer",
            kind="preferred_non_null",
            source_field_id="weight",
            weight_field_id=None,
            group_values=("segment_a", "segment_b"),
            **scope,
        ),
    )


def _source(rows=(), *, child=False):
    columns = [
        column("entity_binding_id", BigInteger),
        column("selected_value", BigInteger),
        column("group_value", String),
        column("metric", Numeric),
        column("weight", Numeric),
    ]
    if child:
        columns += [column("canonical_child_key", String), column("child_key_sha256", LargeBinary)]
    return values(*columns, name="finalized_sources").data(rows)


def test_configured_fields_follow_capabilities_in_declaration_order():
    document = derived_document()
    definition = CustomImportDefinition.from_mapping(document)
    assert configured_fields(definition) == definition.query.derived_fields
    query = replace(
        definition.query,
        filterable_fields=("combined_quality",),
        sortable_fields=("combined_score",),
        order_terms=(SortTerm("merged_cost", "desc", "last"),),
    )
    assert configured_fields(replace(definition, query=query)) == definition.query.derived_fields
    query = replace(query, filterable_fields=(), sortable_fields=(), order_terms=())
    assert configured_fields(replace(definition, query=query)) == ()
    query = replace(query, filterable_fields=None)
    assert configured_fields(replace(definition, query=query)) == definition.query.derived_fields


def test_output_metadata_and_aliases_do_not_enable_disabled_fields():
    document = derived_document()
    document["query"]["derived_fields"][0]["output_path"] = ["combined", "metric"]
    document["query"].update(filterable_fields=[], sortable_fields=[], order=[])
    definition = CustomImportDefinition.from_mapping(document)
    assert definition.query.aliases
    assert configured_fields(definition) == ()


@pytest.mark.parametrize("child", (False, True))
def test_contexts_reduce_once_and_keep_explicit_storage_precision(child):
    source = _source(child=child)
    statement = reduction_statement(
        source,
        _fields(child=child),
        values_by_field={"metric": source.c.metric, "weight": source.c.weight},
        field_slots={"combined_metric": 9, "preferred_weight": 4},
    )
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert sql.count("FROM (VALUES") == 1
    assert "UNION" not in sql
    assert "NUMERIC(31, 12)" in sql and "AS BIGINT" in sql
    assert "div(" in sql and "mod(" in sql
    assert tuple(statement.selected_columns.keys()) == (
        "entity_binding_id",
        "selected_value",
        *(("canonical_child_key", "child_key_sha256") if child else ()),
        "field_9",
        "field_4",
    )
    assert isinstance(statement.selected_columns.field_9.type, Numeric)
    assert (statement.selected_columns.field_9.type.precision, statement.selected_columns.field_9.type.scale) == (
        31,
        12,
    )
    assert isinstance(statement.selected_columns.field_4.type, BigInteger)


@pytest.mark.parametrize("child", (False, True))
def test_ambiguity_checks_full_identity_before_numeric_contribution(child):
    statement = ambiguity_statement(_source(child=child))
    sql = str(statement.compile(dialect=postgresql.dialect()))
    group_by = sql.split("GROUP BY ", 1)[1].split(" HAVING", 1)[0]
    assert "entity_binding_id" in group_by and "selected_value" in group_by and "group_value" in group_by
    assert ("canonical_child_key" in group_by) == child
    assert "child_key_sha256" not in group_by
    assert "metric" not in group_by and "weight" not in group_by
    assert "WHERE" not in sql and "HAVING count(*) >" in sql


@pytest.mark.parametrize("child", (False, True))
def test_empty_capabilities_preserve_only_distinct_context_identities(child):
    statement = reduction_statement(_source(child=child), (), values_by_field={}, field_slots={})
    assert tuple(statement.selected_columns.keys()) == (
        "entity_binding_id",
        "selected_value",
        *(("canonical_child_key", "child_key_sha256") if child else ()),
    )
    assert "GROUP BY" in str(statement.compile(dialect=postgresql.dialect()))


@pytest.mark.parametrize("child", (False, True))
def test_root_and_child_fields_cannot_share_a_source(child):
    with pytest.raises(ValueError, match="scope"):
        reduction_statement(_source(child=child), _fields(child=not child), values_by_field={}, field_slots={})


def test_configured_field_ids_can_match_projection_envelope_columns():
    source = _source()
    field = replace(_fields()[0], source_field_id="selected_value", weight_field_id="group_value")
    statement = reduction_statement(
        source,
        (field,),
        values_by_field={"selected_value": source.c.metric, "group_value": source.c.weight},
        field_slots={"combined_metric": 1},
    )
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "max(finalized_sources.metric)" in sql and "max(finalized_sources.weight)" in sql
    assert "max(finalized_sources.selected_value)" not in sql and "max(finalized_sources.group_value)" not in sql


@pytest.mark.parametrize("slot", (None, 0, 33, True, "1"))
def test_context_columns_require_the_exact_numeric_declaration_slot(slot):
    source = _source()
    with pytest.raises(ValueError, match="slot"):
        reduction_statement(
            source,
            _fields()[:1],
            values_by_field={"metric": source.c.metric, "weight": source.c.weight},
            field_slots={"combined_metric": slot},
        )


def test_child_source_requires_both_complete_key_columns():
    source = values(
        column("entity_binding_id", BigInteger),
        column("selected_value", BigInteger),
        column("group_value", String),
        column("canonical_child_key", String),
    )
    with pytest.raises(ValueError, match="complete-key"):
        ambiguity_statement(source)
