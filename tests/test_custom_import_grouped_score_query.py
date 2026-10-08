# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Finite output score grants remain separate from entity selectors."""

import json
from dataclasses import replace

import pytest
from sqlalchemy.dialects import postgresql

from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from process.custom_import import grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition
from tests import custom_import_grouped_child_support as fixture
from tests import test_custom_import_grouped_score_query_postgres as scores
from tests import test_custom_import_provider_http as provider_fixture


def _context():
    document = json.loads(scores._definition().canonical)
    document["query"]["filterable_fields"] = ["root_cost", "root_quality", "child_cost", "child_quality"]
    document["query"]["sortable_fields"] = list(document["query"]["filterable_fields"])
    document["query"]["aliases"].update({"output_cost": "root_cost", "hidden_cost": "score"})
    return replace(fixture.context(), definition=CustomImportDefinition.from_mapping(document))


def test_output_metric_allowlist_preserves_raw_context_and_normalizes_aliases():
    query = fixture.query(
        context_filters=(read_core.ReadFilter("year_alias", "eq", 2024),),
        filters=(read_core.ReadFilter("output_cost", "gt", 0),),
        order_terms=(read_core.ReadOrderTerm("output_cost", "asc", "last"),),
    )
    plan = grouped_read.normalize_plan(_context(), query, read_core.ExtensionReadScope("synthetic"))
    assert plan.selected_value == 2024
    assert plan.filters[0].field.field_id == plan.order_terms[0].field_id == "root_cost"
    for alias in ("score", "hidden_cost"):
        with pytest.raises(read_core.CustomImportReadRequestError, match="opted in"):
            grouped_read.normalize_plan(
                _context(),
                replace(query, filters=(read_core.ReadFilter(alias, "gt", 0),)),
                read_core.ExtensionReadScope("synthetic"),
            )
        with pytest.raises(read_core.CustomImportReadRequestError, match="order"):
            grouped_read.normalize_plan(
                _context(),
                replace(query, order_terms=(read_core.ReadOrderTerm(alias, "asc", "last"),)),
                read_core.ExtensionReadScope("synthetic"),
            )


def test_grouped_transport_allows_four_scores_and_closes_fifth_term():
    predicates = [
        {"field_id": field, "operator": "gt", "value": 0}
        for field in ("root_cost", "root_quality", "child_cost", "child_quality")
    ]
    orders = [{"field_id": predicate["field_id"], "direction": "asc"} for predicate in predicates]
    changes_map = {
        "context": [
            {"field_id": "period", "operator": "eq", "value": 2024},
            {"field_id": "segment", "operator": "eq", "value": "segment_a"},
            {"field_id": "service_code", "operator": "eq", "value": "chosen"},
        ],
        "filters": predicates,
        "order": orders,
        "require_match": True,
        "grouped_entity_selection": fixture.grouped.selection_document(),
        "grouped_child_query": fixture.child_descriptor(),
    }
    parsed = provider_http._parse_provider_request(provider_fixture._body(**changes_map))
    assert len(parsed.filters) == len(parsed.order_terms) == 4 and len(parsed.context) == 3
    for field in ("filters", "order"):
        with pytest.raises(transport.CustomImportReadTransportError):
            provider_http._parse_provider_request(
                provider_fixture._body(**(changes_map | {field: changes_map[field] + changes_map[field][:1]}))
            )
    legacy_map = {
        key: setting
        for key, setting in changes_map.items()
        if key not in {"grouped_entity_selection", "grouped_child_query"}
    }
    with pytest.raises(transport.CustomImportReadTransportError):
        provider_http._parse_provider_request(provider_fixture._body(**legacy_map))


def test_configured_output_default_order_preserves_explicit_override_and_detail_selectors():
    document = json.loads(_context().definition.canonical)
    document["query"]["order"] = [{"field": "root_cost", "direction": "desc", "nulls": "last"}]
    context = replace(_context(), definition=CustomImportDefinition.from_mapping(document))
    query = fixture.query(context_filters=(read_core.ReadFilter("period", "eq", 2024),))
    scope = read_core.ExtensionReadScope("synthetic")
    assert grouped_read.normalize_plan(context, query, scope).order_terms == (
        read_core.ReadOrderTerm("root_cost", "desc", "last"),
    )
    explicit = replace(query, order_terms=(read_core.ReadOrderTerm("root_quality", "asc", "last"),))
    assert grouped_read.normalize_plan(context, explicit, scope).order_terms == explicit.order_terms
    document["query"]["order"][0]["field"] = "child_cost"
    context = replace(context, definition=CustomImportDefinition.from_mapping(document))
    with pytest.raises(read_core.CustomImportReadRequestError, match="complete-key"):
        grouped_read.normalize_plan(context, query, scope)
    detail = grouped_read.normalize_plan(context, query, scope, projection="full_family", use_default_order=False)
    assert detail.order_terms == () and detail.selected_value == 2024


def test_derived_child_relation_retains_roots_and_validates_before_reduction():
    query = fixture.query(
        context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),),
        filters=(read_core.ReadFilter("child_cost", "gt", 0),),
        order_terms=(read_core.ReadOrderTerm("root_cost", "asc", "last"),),
    )
    prepared = grouped_read.prepare_relation(_context(), query, read_core.ExtensionReadScope("synthetic"))
    sql = str(prepared.statement.compile(dialect=postgresql.dialect()))
    assert "complete_score_child_keys AS MATERIALIZED" not in sql
    assert "derived_root_score_values AS MATERIALIZED" in sql
    assert "FROM derived_root_score_values LEFT OUTER JOIN (mrf.custom_import_family_child" in sql
    for identity in ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
        assert f"complete_score_child_keys.{identity} = mrf.custom_import_family_child.{identity}" in sql
        assert f"mrf.custom_import_child_revision.{identity} = mrf.custom_import_family_child.{identity}" in sql
    assert "count(selected_score_children.child_revision_id) OVER" in sql
    assert "validated_score_children AS MATERIALIZED" in sql
    assert "bool_or(" in sql and "selected_child_revision_id IS NOT NULL" in sql
    assert "complete_score_child_identities" not in sql and "selected_child_counts" not in sql
    assert "JOIN selected_score_children" not in sql and "JOIN derived_root_score_values" not in sql
