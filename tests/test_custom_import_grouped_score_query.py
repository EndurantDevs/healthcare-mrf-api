# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Finite output score grants remain separate from entity selectors."""

import json
import re
from dataclasses import replace

import pytest
from sqlalchemy import BigInteger, Boolean, String, column, literal, select, text, values
from sqlalchemy.dialects import postgresql

from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from api.custom_import_provider_sql import compile_npi_entity_relation
from process.custom_import import grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.storage_layout import snapshot_models
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


@pytest.mark.parametrize("operator", ("gte", "lte"))
def test_grouped_inclusive_comparisons_resolve_output_aliases(operator):
    query = fixture.query(
        context_filters=(read_core.ReadFilter("year_alias", "eq", 2024),),
        filters=(read_core.ReadFilter("output_cost", operator, "15"),),
    )
    plan = grouped_read.normalize_plan(_context(), query, read_core.ExtensionReadScope("synthetic"))
    assert plan.filters[0].field.field_id == "root_cost"
    assert plan.filters[0].operator == operator and str(plan.filters[0].value) == "15"
    assert plan.selected_value == 2024


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


def test_native_scope_precedes_default_year_helper_and_all_group_reducers():
    query = fixture.query(
        context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),),
        order_terms=(read_core.ReadOrderTerm("root_cost", "asc", "last"),),
        entity_values=("1234567893", "1234567802"),
    )
    scope = read_core.ExtensionReadScope("synthetic")
    prepared = grouped_read.prepare_relation(_context(), query, scope)
    compiled = prepared.statement.compile(dialect=postgresql.dialect())
    assert list(query.entity_values) in compiled.params.values()
    assert list(compiled.params.values()).count(list(query.entity_values)) == 2
    assert "selected_entity_value AS MATERIALIZED" not in str(compiled)
    assert "WHERE selected_entity_value.entity_binding_id = mrf.custom_import_winner.entity_binding_id" in str(compiled)
    assert "derived_score_values" in str(compiled) and str(compiled).count("LIMIT ALL OFFSET") == 1
    assert "LIMIT" not in str(compiled).replace("LIMIT ALL OFFSET", "OFFSET")
    broad = grouped_read.prepare_relation(_context(), replace(query, entity_values=None), scope)
    assert prepared.query_fingerprint != broad.query_fingerprint


@pytest.mark.parametrize(
    ("value_type", "selector_value"),
    [
        ("string", "chosen"),
        ("integer", 7),
        ("decimal", "7.50"),
        ("boolean", True),
        ("date", "2024-01-01"),
        ("timestamp", "2024-01-01T00:00:00Z"),
    ],
)
@pytest.mark.parametrize("entity_values", (None, (), ("1234567893",)))
@pytest.mark.parametrize("snapshot", (False, True))
def test_derived_child_relation_retains_roots_and_validates_before_reduction(
    entity_values, snapshot, value_type, selector_value
):
    query = fixture.query(
        context_filters=(read_core.ReadFilter("service_code", "eq", selector_value),),
        filters=(read_core.ReadFilter("child_cost", "gt", 0),),
        order_terms=(read_core.ReadOrderTerm("root_cost", "asc", "last"),),
        entity_values=entity_values,
    )
    document = json.loads(_context().definition.canonical)
    document["schema"]["children"][0]["fields"][1]["type"] = value_type
    context = replace(_context(), definition=CustomImportDefinition.from_mapping(document))
    if snapshot:
        context = replace(context, storage_models=snapshot_models(1))
    prepared = grouped_read.prepare_relation(context, query, read_core.ExtensionReadScope("synthetic"))
    sql = str(prepared.statement.compile(dialect=postgresql.dialect()))
    native_sql = compile_npi_entity_relation(prepared.statement).sql
    assert "jsonb_object_agg" not in native_sql and "jsonb_extract_path_text" not in native_sql
    assert "complete_score_child_identities AS MATERIALIZED" not in native_sql
    assert "complete_score_child_keys AS MATERIALIZED" not in sql
    assert "derived_root_score_values AS MATERIALIZED" in sql
    namespace = "ci_snapshot_1" if snapshot else "mrf"
    assert "FROM derived_root_score_values LEFT OUTER JOIN (LATERAL (SELECT" in sql
    assert "selected_score_child_members" not in sql
    for identity in ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
        assert f"complete_score_child_keys.{identity} = {namespace}.custom_import_family_child.{identity}" in sql
        assert f"{namespace}.custom_import_child_revision.{identity} = keyed_score_members.{identity}" in sql
    assert "count(selected_score_children.child_revision_id) OVER" in sql
    assert "validated_score_children AS MATERIALIZED" in sql
    assert "bool_or(" in sql and "selected_child_revision_id IS NOT NULL" in sql
    assert "selected_child_counts" not in sql
    assert "JOIN selected_score_children" not in sql and "JOIN derived_root_score_values" not in sql
    assert sql.count("complete_score_child_keys AS") == 1
    assert ("candidate_complete_score_keys AS MATERIALIZED" in sql) is (entity_values is None)
    assert ("child_candidate_score_entities AS MATERIALIZED" in sql) is (entity_values is None)
    assert ("FROM child_candidate_score_entities JOIN LATERAL" in sql) is (entity_values is None)
    assert f"complete_child_key.{read_core._SCALAR_COLUMNS[value_type]} AS selected_key_value" in sql
    assert "keyed_score_members.selected_key_value AS" in sql
    assert sql.count("LIMIT ALL OFFSET") == (2 if entity_values is None else 1)
    assert "LIMIT" not in sql.replace("LIMIT ALL OFFSET", "OFFSET")
    assert (
        f"{namespace}.custom_import_family_child.family_revision_id = derived_root_score_values.family_revision_id"
        in sql
    )
    assert f"{namespace}.custom_import_family_child.root_record_id = derived_root_score_values.root_record_id" in sql
    assert f"{namespace}.custom_import_child_scalar AS score_child_4" not in sql


@pytest.mark.parametrize("metric,raw_metric", (("root_cost", None), ("child_cost", None), ("root_cost", "amount")))
def test_complete_key_join_preserves_selected_roots(metric, raw_metric):
    context = replace(fixture.context(), definition=scores._definition())
    selectors = (read_core.ReadFilter("service_code", "eq", "chosen"),)
    filters = (read_core.ReadFilter(metric, "gt", 0),)
    if raw_metric is not None:
        selectors += (read_core.ReadFilter("segment", "eq", "segment_a"),)
        filters += (read_core.ReadFilter(raw_metric, "gt", 1),)
    query = fixture.query(
        context_filters=selectors,
        filters=filters,
        order_terms=(read_core.ReadOrderTerm(metric, "asc", "last"),),
    )
    prepared = grouped_read.prepare_relation(context, query, read_core.ExtensionReadScope("synthetic"))
    compiled = prepared.statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert " ON true" in sql
    assert all(type(value) is not bool for value in compiled.params.values())
    assert "jsonb_object_agg" not in sql and "jsonb_extract_path_text" not in sql
    assert "JOIN complete_score_child_keys ON" in sql
    for identity in ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
        assert f"complete_score_child_keys.{identity} = mrf.custom_import_family_child.{identity}" in sql
        assert f"mrf.custom_import_child_revision.{identity} = keyed_score_members.{identity}" in sql
    assert "FROM selected_score_families" in sql
    assert "FROM derived_root_score_values LEFT OUTER JOIN" in sql
    assert "child_candidate_score_entities AS MATERIALIZED" in sql
    assert sql.count("complete_score_child_keys AS") == 1
    assert "validated_score_children AS MATERIALIZED" in sql


@pytest.mark.parametrize("snapshot", (False, True))
@pytest.mark.parametrize("period", (None, 2024))
def test_broad_child_candidates_precede_independent_group_selection(snapshot, period):
    selectors = (
        read_core.ReadFilter("service_code", "eq", "chosen"),
        read_core.ReadFilter("segment", "eq", "segment_a"),
    )
    if period is not None:
        selectors += (read_core.ReadFilter("period", "eq", period),)
    query = fixture.query(
        context_filters=selectors,
        filters=(read_core.ReadFilter("root_cost", "gt", 15),),
        order_terms=(read_core.ReadOrderTerm("child_cost", "asc", "last"),),
    )
    context = _context()
    if snapshot:
        context = replace(context, storage_models=snapshot_models(1))
    prepared = grouped_read.prepare_relation(context, query, read_core.ExtensionReadScope("synthetic"))
    sql = str(prepared.statement.compile(dialect=postgresql.dialect()))
    eligible, families = sql.split("child_candidate_score_entities AS MATERIALIZED \n(", 1)[1].split(
        "), \nselected_score_families AS \n(", 1
    )
    families = families.split("), \nderived_root_score_values AS", 1)[0]
    namespace = "ci_snapshot_1" if snapshot else "mrf"
    assert eligible.startswith(f"SELECT DISTINCT {namespace}.custom_import_family_revision.entity_binding_id")
    assert all(
        token not in eligible
        for token in ("custom_import_winner", "selected_score_families", "group_value", "score_root_", "score_child_")
    )
    assert "FROM candidate_complete_score_keys JOIN" in eligible
    for identity in ("dataset_id", "schema_revision_id", "root_record_id"):
        assert (
            f"{namespace}.custom_import_family_revision.{identity} = candidate_complete_score_keys.{identity}"
            in eligible
        )
    assert (
        f"{namespace}.custom_import_family_child.family_revision_id = "
        f"{namespace}.custom_import_family_revision.family_revision_id" in eligible
    )
    for identity in ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
        for owner in ("custom_import_family_child", "custom_import_child_revision"):
            assert f"{namespace}.{owner}.{identity} = candidate_complete_score_keys.{identity}" in eligible
    assert "FROM child_candidate_score_entities JOIN LATERAL (SELECT" in families
    assert "candidate_score_families.group_value AS group_value" in families
    assert (
        f"{namespace}.custom_import_winner.entity_binding_id = child_candidate_score_entities.entity_binding_id"
        in families
    )
    assert "candidate_complete_score_keys" not in families
    assert ("selected_entity_value.entity_binding_id" in families) is (period is None)
    assert "FROM selected_score_families LEFT OUTER JOIN" in sql
    assert sql.count("candidate_complete_score_keys AS MATERIALIZED") == 1
    assert sql.count("complete_score_child_keys AS") == 1
    assert sql.count("JOIN complete_score_child_keys ON") == 1
    assert "count(selected_score_children.child_revision_id) OVER" in sql
    assert "validated_score_children AS MATERIALIZED" in sql


@pytest.mark.parametrize("identity_type", (BigInteger, String))
@pytest.mark.parametrize("as_cte", (False, True))
@pytest.mark.parametrize("snapshot", (False, True))
@pytest.mark.parametrize("period", (None, 2024))
@pytest.mark.parametrize("metric,raw_metric", (("child_cost", None), ("root_cost", None), ("root_cost", "amount")))
def test_native_sql_scope_precedes_family_reduction_without_a_page_bound(
    identity_type, as_cte, snapshot, period, metric, raw_metric
):
    identities = [1000000001 + index for index in range(201)]
    native_rows = (
        values(column("candidate_identity", identity_type))
        .data([(str(identity) if identity_type is String else identity,) for identity in identities])
        .cte("synthetic_native_rows")
    )
    native_scope = select(native_rows.c.candidate_identity)
    if as_cte:
        native_scope = native_scope.cte("eligible_native_scope")
    selectors = (read_core.ReadFilter("service_code", "eq", "chosen"),)
    if period is not None:
        selectors += (read_core.ReadFilter("period", "eq", period),)
    filters = (read_core.ReadFilter(metric, "gt", 0),)
    if raw_metric:
        filters += (read_core.ReadFilter(raw_metric, "gt", 1),)
        selectors += (read_core.ReadFilter("segment", "eq", "segment_a"),)
    query = fixture.query(
        context_filters=selectors,
        filters=filters,
        order_terms=(read_core.ReadOrderTerm(metric, "asc", "last"),),
    )
    context = replace(fixture.context(), definition=scores._definition())
    if snapshot:
        context = replace(context, storage_models=snapshot_models(1))
    authorization_scope = read_core.ExtensionReadScope("synthetic")
    broad = grouped_read.prepare_relation(context, query, authorization_scope)
    prepared = grouped_read.prepare_relation(context, query, authorization_scope, native_entity_scope=native_scope)
    compiled = compile_npi_entity_relation(prepared.statement)
    sql_compilation = prepared.statement.compile(dialect=postgresql.dialect())
    sql = str(sql_compilation)
    assert prepared.query_fingerprint == broad.query_fingerprint
    assert prepared.authorization_scope_sha256 == broad.authorization_scope_sha256
    assert prepared.normalized_order_terms == broad.normalized_order_terms
    assert query.entity_values is None
    expected_identities = tuple(str(identity) if identity_type is String else identity for identity in identities)
    assert (
        len([parameter_value for parameter_value in compiled.values.values() if parameter_value in expected_identities])
        == 201
    )
    scope_name = "eligible_native_scope" if as_cte else "native_score_entities"
    assert sql.count(f"canonical_value IN (SELECT CAST({scope_name}.candidate_identity AS VARCHAR)") == (
        2 if period is None else 1
    )
    assert sql.count("LIMIT ALL OFFSET") == 1 and "LIMIT" not in sql.replace("LIMIT ALL OFFSET", "OFFSET")
    assert sql_compilation.params[re.search(r"LIMIT ALL OFFSET %\((\w+)\)s", sql).group(1)] == 0
    assert "complete_score_child_identities" not in sql and "child_candidate_score_entities" not in sql
    assert ("native_score_entities AS MATERIALIZED" in sql) is (not as_cte)
    assert "derived_root_score_values AS MATERIALIZED" in sql
    assert "selected_score_child_members" not in sql
    assert "FROM derived_root_score_values LEFT OUTER JOIN" in sql
    assert "count(selected_score_children.child_revision_id) OVER" in sql
    assert "validated_score_children AS MATERIALIZED" in sql
    namespace = "ci_snapshot_1" if snapshot else "mrf"
    for identity in ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
        assert f"complete_score_child_keys.{identity} = {namespace}.custom_import_family_child.{identity}" in sql
        assert f"{namespace}.custom_import_child_revision.{identity} = keyed_score_members.{identity}" in sql


@pytest.mark.parametrize("entity_values", ((), ("1234567893",)))
def test_native_sql_scope_intersects_public_entity_values_without_changing_the_fingerprint(entity_values):
    native_scope = select(column("npi", BigInteger)).select_from(text("synthetic_native_providers"))
    query = fixture.query(
        context_filters=(read_core.ReadFilter("service_code", "eq", "chosen"),),
        order_terms=(read_core.ReadOrderTerm("root_cost", "asc", "last"),),
        entity_values=entity_values,
    )
    context = replace(fixture.context(), definition=scores._definition())
    authorization_scope = read_core.ExtensionReadScope("synthetic")
    broad = grouped_read.prepare_relation(context, query, authorization_scope)
    prepared = grouped_read.prepare_relation(context, query, authorization_scope, native_entity_scope=native_scope)
    compiled = compile_npi_entity_relation(prepared.statement)
    assert prepared.query_fingerprint == broad.query_fingerprint
    assert query.entity_values == entity_values
    if entity_values:
        assert entity_values[0] in compiled.values.values()
        assert "CAST(native_score_entities.npi AS VARCHAR) IN" in compiled.sql
    else:
        assert "WHERE 1!=1" in compiled.sql


@pytest.mark.parametrize(
    "native_scope",
    [
        text("SELECT npi FROM synthetic_native_providers"),
        select(column("npi")),
        select(literal(True, type_=Boolean)),
        select(literal(1), literal(2)),
        select(literal(1)).limit(1),
        select(literal(1)).offset(0),
        select(literal(1)).fetch(1),
        select(literal(1)).limit(1).cte("capped_native_scope"),
    ],
)
def test_native_sql_scope_rejects_paged_or_untyped_identity_relations(native_scope):
    with pytest.raises(read_core.CustomImportReadRequestError, match="native entity scope"):
        grouped_read.prepare_relation(
            _context(), fixture.query(), read_core.ExtensionReadScope("synthetic"), native_entity_scope=native_scope
        )


@pytest.mark.parametrize("metric", ("root_cost", "score"))
def test_queries_without_child_context_do_not_prefilter_entities(metric):
    context = replace(fixture.context(), definition=scores._definition())
    query = fixture.query(
        context_filters=(read_core.ReadFilter("segment", "eq", "segment_a"),),
        order_terms=(read_core.ReadOrderTerm(metric, "asc", "last"),),
        require_match=False,
    )
    prepared = grouped_read.prepare_relation(context, query, read_core.ExtensionReadScope("synthetic"))
    sql = str(prepared.statement.compile(dialect=postgresql.dialect()))
    assert "child_candidate_score_entities" not in sql and "complete_score_child_keys" not in sql
    assert prepared.effective_require_match is False


def test_stored_child_query_keeps_existing_cardinality_path():
    query = fixture.query(
        context_filters=(
            read_core.ReadFilter("segment", "eq", "segment_a"),
            read_core.ReadFilter("service_code", "eq", "chosen"),
        ),
        filters=(read_core.ReadFilter("amount", "gt", 0),),
        order_terms=(read_core.ReadOrderTerm("amount", "asc", "last"),),
    )
    prepared = grouped_read.prepare_relation(fixture.context(), query, read_core.ExtensionReadScope("synthetic"))
    sql = str(prepared.statement.compile(dialect=postgresql.dialect()))
    assert "child_candidate_score_entities" not in sql and "derived_score_values" not in sql
    assert "complete_score_child_keys AS MATERIALIZED" in sql
    assert "validated_score_children AS MATERIALIZED" in sql
