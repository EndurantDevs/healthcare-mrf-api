# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed child identity and same-member SQL for grouped reads."""

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from db.models.custom_import import CustomImportFamilyRevision, CustomImportWinner
from process.custom_import import grouped_child_read, grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.read_contracts import canonical_read_document
from process.custom_import.read_core import CustomImportReadRequestError, ExtensionReadScope, ReadFilter, ReadOrderTerm
from process.custom_import.runner_graph import winner_candidates
from process.custom_import.runner_types import CandidateRegistry, PublishedCandidateChild, PublishedCandidateFamily
from tests import custom_import_grouped_child_support as fixture
from tests import custom_import_grouped_support as grouped
from tests import test_custom_import_provider_geo_cursor as cursor_fixture
from tests import test_custom_import_read_http as http_fixture

_SCOPE = ExtensionReadScope("synthetic:grouped")
_PANEL = ReadFilter("segment", "eq", "segment_a")
_KEY = ReadFilter("service_code", "eq", "chosen")


def _plan(**changes):
    return grouped_read.normalize_plan(fixture.context(), fixture.query(**changes), _SCOPE)


def _filters(*predicates):
    return read_core._normalized_filters(predicates, fixture.context(), query_child_collection="rates")


def test_child_fields_remain_rejected_without_the_declared_scope():
    context = fixture.context()
    for collection in (None, "undeclared"):
        with pytest.raises(CustomImportReadRequestError, match="permitted context"):
            read_core._normalized_filters(
                (ReadFilter("amount", "gt", "1"),), context, query_child_collection=collection
            )
        with pytest.raises(CustomImportReadRequestError, match="permitted context"):
            read_core._normalize_query_order_terms(
                (ReadOrderTerm("amount", "asc", "last"),),
                context,
                explicit=True,
                query_child_collection=collection,
            )


def test_child_normalization_reuses_exact_types_aliases_and_order_allowlist():
    assert _filters(ReadFilter("cost_alias", "gt", "1.200")) == _filters(ReadFilter("amount", "gt", "1.200"))
    with pytest.raises(CustomImportReadRequestError, match="repeat"):
        _filters(ReadFilter("cost_alias", "gt", "1"), ReadFilter("amount", "gt", "1"))
    with pytest.raises(CustomImportReadRequestError, match="metric-compatible"):
        _filters(ReadFilter("service_code", "gt", "synthetic"))
    assert read_core._normalize_query_order_terms(
        (ReadOrderTerm("cost_alias", "desc", "last"),),
        fixture.context(),
        explicit=True,
        query_child_collection="rates",
    ) == (ReadOrderTerm("amount", "desc", "last"),)


@pytest.mark.parametrize("mutation", ["none", "not_grouped", "composite", "unprojected", "not_queryable", "no_slot"])
def test_child_capability_requires_the_complete_projected_key(mutation):
    document = fixture.definition_document()
    if mutation == "not_grouped":
        document["query"].pop("entity_selection")
    if mutation == "none":
        document["query"].pop("child")
        document["query"]["sortable_fields"] = ["score"]
        document["query"]["aliases"] = {}
    if mutation == "composite":
        document["schema"]["children"][0]["child_key"].append("rate_period")
    if mutation == "unprojected":
        document["schema"]["children"][0]["fields"][1].pop("projection_slot")
    if mutation in {"unprojected", "not_queryable"}:
        document["query"]["child"]["fields"].remove("service_code")
        document["query"]["aliases"].pop("child_alias")
    context = replace(fixture.context(), definition=CustomImportDefinition.from_mapping(document))
    if mutation == "no_slot":
        context = replace(context, collection_slots_by_name={})
    with pytest.raises(CustomImportReadRequestError, match="child key|child queries"):
        grouped_child_read.declared_child_key(context)


@pytest.mark.parametrize(
    "predicates",
    [
        (ReadFilter("amount", "eq", "1"),),
        (ReadFilter("service_code", "eq", "a"), ReadFilter("service_code", "eq", "b")),
        (ReadFilter("service_code", "is_null"),),
    ],
)
def test_context_requires_one_exact_child_key(predicates):
    child_key = grouped_child_read.declared_child_key(fixture.context())
    with pytest.raises(CustomImportReadRequestError, match="complete-key equality"):
        grouped_child_read.verify_child_selectors(_filters(*predicates), child_key)


def test_child_relation_keeps_all_ownership_conditions_and_one_child_reference():
    context = fixture.context()
    predicates = _filters(ReadFilter("amount", "gt", "1"), ReadFilter("quality", "lt", "3"))
    statement = (
        select(CustomImportWinner.family_revision_id)
        .join(
            CustomImportFamilyRevision,
            CustomImportFamilyRevision.family_revision_id == CustomImportWinner.family_revision_id,
        )
        .where(grouped_child_read.matching_child_statement(context, predicates).exists())
    )
    rendered = str(statement.compile(dialect=postgresql.dialect()))
    assert rendered.count("FROM mrf.custom_import_child_scalar") == 2
    assert (
        rendered.count(
            "custom_import_child_scalar.child_revision_id = mrf.custom_import_child_revision.child_revision_id"
        )
        == 2
    )
    assert "custom_import_family_child.family_revision_id = mrf.custom_import_winner.family_revision_id" in rendered
    assert "custom_import_family_child.root_record_id = mrf.custom_import_family_revision.root_record_id" in rendered
    assert (
        "custom_import_family_child.dataset_id" in rendered
        and "custom_import_family_child.schema_revision_id" in rendered
    )
    assert "custom_import_family_child.collection_slot" in rendered
    assert "LIMIT" not in rendered


@pytest.mark.parametrize("descriptor", [{}, {"contract": "unknown"}, None, False])
def test_child_semantics_require_exact_authorized_descriptor(descriptor):
    query = grouped.query(context_filters=(_KEY,))
    with pytest.raises(CustomImportReadRequestError):
        grouped_read.normalize_plan(fixture.context(), replace(query, grouped_child_query=descriptor), _SCOPE)


@pytest.mark.parametrize(
    "changes",
    [{"collection": "other"}, {"key_field_id": "amount"}, {"extra": "value"}],
)
def test_child_descriptor_rejects_stale_or_expanded_capability(changes):
    with pytest.raises(CustomImportReadRequestError, match="pinned definition"):
        grouped_child_read.verified_child_key(fixture.context(), {**fixture.child_descriptor(), **changes})


@pytest.mark.parametrize(
    ("value_type", "value"),
    [
        ("string", "chosen"),
        ("integer", 7),
        ("decimal", "7.50"),
        ("boolean", True),
        ("date", "2024-01-01"),
        ("timestamp", "2024-01-01T00:00:00Z"),
    ],
)
def test_complete_key_uses_existing_scalar_equality_codec(value_type, value):
    document = fixture.definition_document()
    document["schema"]["children"][0]["fields"][1]["type"] = value_type
    context = replace(fixture.context(), definition=CustomImportDefinition.from_mapping(document))
    query = fixture.query(context_filters=(ReadFilter("service_code", "eq", value),))
    assert grouped_read.normalize_plan(context, query, _SCOPE).context_filters[0].field.value_type == value_type


def test_child_budget_counts_implicit_year_without_changing_root_or_legacy_limits():
    metrics = (ReadFilter("amount", "gt", "1"), ReadFilter("quality", "lt", "10"))
    assert _plan(context_filters=(_PANEL, _KEY), filters=metrics).selected_value is None
    assert (
        _plan(context_filters=(_PANEL, _KEY, ReadFilter("period", "eq", 2024)), filters=metrics).selected_value == 2024
    )
    with pytest.raises(CustomImportReadRequestError, match="count"):
        _plan(context_filters=(_PANEL, _KEY), filters=metrics + (ReadFilter("score", "gt", "1"),))
    root_query = grouped.query(
        context_filters=(_PANEL,),
        filters=(
            ReadFilter("score", "gt", "1"),
            ReadFilter("score", "lt", "10"),
            ReadFilter("npi", "eq", "1234567893"),
        ),
    )
    with pytest.raises(CustomImportReadRequestError, match="count"):
        grouped_read.normalize_plan(fixture.context(), root_query, _SCOPE)
    with pytest.raises(CustomImportReadRequestError, match="count"):
        read_core._validate_npi_entity_relation_request(metrics, None, context_filters=(_PANEL, _KEY))


@pytest.mark.parametrize(
    "changes",
    [
        {"context_filters": (_PANEL,), "order_terms": (ReadOrderTerm("amount", "asc", "last"),)},
        {"context_filters": (_KEY,), "order_terms": (ReadOrderTerm("amount", "asc", "last"),)},
        {"context_filters": (_PANEL, ReadFilter("quality", "eq", "7"))},
        {"context_filters": (_PANEL, _KEY, ReadFilter("child_alias", "eq", "different"))},
        {"context_filters": (_PANEL,), "filters": (ReadFilter("service_code", "gt", "chosen"),)},
        {"context_filters": (_PANEL,), "filters": (ReadFilter("quality", "gt", "1"),), "require_match": False},
        {"require_exact_context": True},
    ],
)
def test_child_context_and_order_fail_closed(changes):
    with pytest.raises(CustomImportReadRequestError):
        _plan(**changes)


def test_child_order_requires_exact_context():
    assert _plan(context_filters=(_PANEL,), filters=(ReadFilter("amount", "gt", "1"),)).order_terms == ()
    assert _plan(context_filters=(_KEY,)).context_filters[0].value == "chosen"
    order = (ReadOrderTerm("amount", "desc", "last"),)
    assert _plan(context_filters=(_PANEL, _KEY), order_terms=order, require_match=False).order_terms == order
    with pytest.raises(CustomImportReadRequestError, match="metric filters are invalid"):
        _plan(context_filters=(_PANEL,), filters=(_KEY,), order_terms=order)


@pytest.mark.parametrize("field_id", ["service_code", "child_alias"])
def test_child_key_cannot_be_a_metric(field_id):
    with pytest.raises(CustomImportReadRequestError, match="metric filters are invalid"):
        _plan(context_filters=(_PANEL,), filters=(ReadFilter(field_id, "eq", "chosen"),))


def test_alias_fingerprints_and_cursor_binding_include_child_identity_and_capability():
    query = fixture.query(
        context_filters=(_PANEL, _KEY), order_terms=(ReadOrderTerm("amount", "asc", "last"),), require_match=False
    )
    fingerprint = grouped_read.normalize_plan(fixture.context(), query, _SCOPE).fingerprint
    alias = replace(
        query,
        context_filters=(ReadFilter("panel_alias", "eq", "segment_a"), ReadFilter("child_alias", "eq", "chosen")),
        order_terms=(ReadOrderTerm("cost_alias", "asc", "last"),),
    )
    assert grouped_read.normalize_plan(fixture.context(), alias, _SCOPE).fingerprint == fingerprint
    changed = replace(query, context_filters=(_PANEL, ReadFilter("service_code", "eq", "different")))
    changed_fingerprint = grouped_read.normalize_plan(fixture.context(), changed, _SCOPE).fingerprint
    token = cursor_fixture.geo.issue_geo_cursor(
        replace(cursor_fixture.STATE, query_fingerprint=fingerprint), secret=cursor_fixture.SECRET
    )
    assert cursor_fixture._open(token, query_fingerprint=fingerprint).query_fingerprint == fingerprint
    with pytest.raises(cursor_fixture.CustomImportReadCursorError):
        cursor_fixture._open(token, query_fingerprint=changed_fingerprint)
    root_plan = grouped_read.normalize_plan(fixture.context(), grouped.query(), _SCOPE)
    assert _plan().fingerprint != root_plan.fingerprint
    descriptor_map = {
        "contract": "custom-import/grouped-entity-selection/v1",
        "target": read_core._target_document(fixture.context().target),
        "grouped_entity_selection": grouped.selection_document(),
        "selection": {"mode": "max_per_entity"},
        "context": [],
        "filters": [],
        "order": [],
        "projection": "query_projection",
        "require_match": True,
        "authorization_scope_sha256": read_core._scope_digest(_SCOPE),
    }
    assert (
        root_plan.fingerprint
        == hashlib.sha256(b"custom-import-grouped-read/v1\0" + canonical_read_document(descriptor_map)).hexdigest()
    )


def _provider_document():
    return {
        "target": http_fixture._TARGET,
        "native_query": {},
        "order": None,
        "require_match": True,
        "context": [
            {"field_id": field, "operator": "eq", "value": value}
            for field, value in [("period", 2024), ("segment", "segment_a"), ("service_code", "chosen")]
        ],
        "filters": [{"field_id": field, "operator": "gt", "value": "1"} for field in ("amount", "quality")],
        "grouped_entity_selection": grouped.selection_document(),
        "grouped_child_query": fixture.child_descriptor(),
    }


def test_transport_admits_only_opt_in_five_terms_and_preserves_exact_descriptor():
    document = _provider_document()
    parsed = provider_http._parse_provider_request(transport._canonical_json_bytes(document))
    query = provider_http._provider_relation_query(parsed)
    assert query.grouped_child_query == fixture.child_descriptor()
    assert grouped_read.normalize_plan(fixture.context(), query, _SCOPE).selected_value == 2024
    document.pop("grouped_child_query")
    with pytest.raises(CustomImportReadRequestError):
        provider_http._parse_provider_request(transport._canonical_json_bytes(document))


@pytest.mark.parametrize(
    "child",
    [None, False, {}, {**fixture.child_descriptor(), "extra": 1}, {**fixture.child_descriptor(), "collection": 1}],
)
def test_transport_rejects_malformed_optional_child_capability(child):
    document_map = {**_provider_document(), "grouped_child_query": child}
    with pytest.raises(transport.CustomImportReadTransportError):
        provider_http._parse_provider_request(transport._canonical_json_bytes(document_map))


def test_detail_binds_child_context_and_requires_grouped_parent_descriptor():
    document_map = {
        "target": http_fixture._TARGET,
        "entity": {"adapter_id": "npi", "value": "1234567893"},
        "family_entitlement": "full_family",
        "context": _provider_document()["context"],
        "grouped_entity_selection": grouped.selection_document(),
        "grouped_child_query": fixture.child_descriptor(),
    }
    parsed = transport._parse_detail_request(transport._canonical_json_bytes(document_map)).bind(1)
    assert parsed.grouped_child_query == fixture.child_descriptor() and len(parsed.context_filters) == 3
    document_map.pop("grouped_entity_selection")
    with pytest.raises(transport.CustomImportReadTransportError):
        transport._parse_detail_request(transport._canonical_json_bytes(document_map))


@pytest.mark.parametrize("profile_scope", ["root", "child_context", "child_selection"])
def test_candidates_follow_declared_profile_scopes(profile_scope):
    document_map = fixture.definition_document()
    if profile_scope != "root":
        document_map["query"].pop("entity_selection")
        document_map["selection_profiles"] = [
            {
                "id": "selected_child",
                "context_dimensions": ["service_code"] if profile_scope == "child_context" else [],
                "selection": [{"field": "amount", "direction": "asc", "nulls": "last"}]
                if profile_scope == "child_selection"
                else [],
            }
        ]
    definition = CustomImportDefinition.from_mapping(document_map)
    family = PublishedCandidateFamily(
        1,
        2,
        3,
        4,
        b"a" * 32,
        {"npi": "1234567893", "period": 2024, "segment": "segment_a"},
        (PublishedCandidateChild("rates", 5, b"b" * 32, {"service_code": "chosen"}),),
    )
    candidates = winner_candidates(
        SimpleNamespace(definition=definition), CandidateRegistry({"rates": 1}, {}, 1), (family,)
    )
    assert [candidate.context_collection_slot for candidate in candidates] == (
        [0] if profile_scope == "root" else [0, 1]
    )
