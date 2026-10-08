# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed definition, typed selector, and identity-bound grouped read checks."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from process.custom_import import grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition, DefinitionError, canonical_json, canonical_sha256
from process.custom_import.read_core import CustomImportReadRequestError, ExtensionReadScope, ReadFilter, ReadOrderTerm
from tests import custom_import_grouped_support as fixture
from tests import test_custom_import_provider_http as provider_fixture
from tests import test_custom_import_provider_query as query_fixture
from tests import test_custom_import_read_http as http_fixture

_SCOPE = ExtensionReadScope("synthetic:grouped")


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["search", "winner_detail", "unselected_context"])
async def test_grouped_context_rejects_legacy_entrypoints(monkeypatch, route):
    context = fixture.context()
    if route == "unselected_context":
        definition_document = fixture.definition_document()
        definition_document["query"].pop("entity_selection")
        context = replace(context, definition=CustomImportDefinition.from_mapping(definition_document))

    @asynccontextmanager
    async def bounded_window(_session, **_kwargs):
        yield

    monkeypatch.setattr(read_core, "_bounded_read_window", bounded_window)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=context))
    lookup = AsyncMock(side_effect=AssertionError("invalid route must not select legacy winners"))
    monkeypatch.setattr(read_core, "_entity_winner_locator", lookup)
    monkeypatch.setattr(read_core, "_selected_winner_row", lookup)
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow(), cursor_secret=b"s" * 32)
    authorization = read_core.ExtensionReadAuthorization("synthetic")
    with pytest.raises(CustomImportReadRequestError, match="generic search|entity selector|detail context"):
        if route == "search":
            await service.search(None, authorization=authorization, request=read_core.SearchRequest(context.target))
        elif route == "winner_detail":
            await service.root_detail(
                None,
                authorization=authorization,
                target=context.target,
                winner=read_core.WinnerLocator(1, 2, 3, b"w" * 32),
            )
        else:
            request = read_core.RootDetailRequest(
                context.target, read_core.EntityLocator("npi", "1234567893"), "full_family", ()
            )
            await service.root_detail_for_entity(None, authorization=authorization, request=request)
    lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter_id,is_missing_family", [("synthetic", False), ("npi", True)])
async def test_grouped_detail_rejects_incomplete_identity(monkeypatch, adapter_id, is_missing_family):
    context = fixture.context()
    request = read_core.RootDetailRequest(
        context.target,
        read_core.EntityLocator(adapter_id, "1234567893"),
        "full_family",
        (),
        fixture.selection_document(),
    )
    session = SimpleNamespace(scalar=AsyncMock(return_value=1), execute=AsyncMock())
    family_reader = AsyncMock(return_value=())
    monkeypatch.setattr(grouped_read, "_selected_family_rows", family_reader)
    error_type = read_core.CustomImportReadUnavailableError if is_missing_family else CustomImportReadRequestError
    with pytest.raises(error_type, match="selected families|NPI adapter"):
        await grouped_read.hydrate_detail(session, context, request, _SCOPE)
    if is_missing_family:
        family_reader.assert_awaited_once()
    else:
        session.scalar.assert_not_awaited()
        family_reader.assert_not_awaited()


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("query", "entity_selection", "contract"), "unknown"),
        (("query", "entity_selection", "unknown"), True),
        (("query", "entity_selection", "default"), "first"),
        (("query", "entity_selection", "field"), "npi"),
        (("query", "entity_selection", "default_profile"), "families_by_period"),
        (("query", "entity_selection", "family_dimension", "values"), []),
        (("query", "entity_selection", "family_dimension", "values"), ["a", "b", "c"]),
        (("query", "entity_selection", "family_dimension", "values"), ["a", "a"]),
        (("query", "entity_selection", "family_dimension", "values"), [""]),
        (("query", "entity_selection", "family_dimension", "values"), [1]),
        (("query", "entity_selection", "family_dimension", "values"), ["\x00"]),
        (("query", "entity_selection", "family_dimension", "values"), ["\ud800"]),
        (("query", "entity_selection", "family_dimension", "values"), ["é" * 1025]),
        (("query", "root_fields"), ["npi", "segment", "score"]),
        (("schema", "root", "fields", 2, "nullable"), True),
        (("schema", "root", "fields", 2, "type"), "string"),
        (("selection_profiles", 0, "context_dimensions"), ["segment", "period"]),
        (("selection_profiles", 1, "context_dimensions"), ["segment"]),
        (("selection_profiles", 1, "selection", 0, "direction"), "asc"),
        (("selection_profiles", 1, "selection", 0, "nulls"), "first"),
    ],
)
def test_definition_rejects_invalid_grouped_contract(path, value):
    document_map = fixture.definition_document()
    parent = document_map
    for member in path[:-1]:
        parent = parent[member]
    parent[path[-1]] = value
    with pytest.raises(DefinitionError):
        CustomImportDefinition.from_mapping(document_map)


def test_definition_opt_in_changes_only_definition_identity():
    document_map = fixture.definition_document()
    opted = CustomImportDefinition.from_mapping(document_map)
    assert opted.query.entity_selection.document() == fixture.selection_document()
    document_map["query"].pop("entity_selection")
    legacy = CustomImportDefinition.from_mapping(document_map)
    assert legacy.query.entity_selection is None
    assert legacy.canonical == canonical_json(document_map)
    assert opted.schema_digest == legacy.schema_digest
    assert opted.digest != legacy.digest
    assert opted.selection_profiles == legacy.selection_profiles


@pytest.mark.parametrize("value", ["2024", True, 2024.0, None, 2**63, -(2**63) - 1])
def test_year_selector_rejects_non_bigint(value):
    with pytest.raises(CustomImportReadRequestError):
        grouped_read.normalize_plan(
            fixture.context(), fixture.query(context_filters=(ReadFilter("period", "eq", value),)), _SCOPE
        )


@pytest.mark.parametrize("value", [-(2**63), 2**63 - 1])
def test_year_selector_keeps_exact_signed_bigint(value):
    plan = grouped_read.normalize_plan(
        fixture.context(), fixture.query(context_filters=(ReadFilter("year_alias", "eq", value),)), _SCOPE
    )
    assert plan.selected_value == value


@pytest.mark.parametrize(
    "selectors",
    [
        (ReadFilter("period", "eq", 2024), ReadFilter("year_alias", "eq", 2025)),
        (ReadFilter("segment", "eq", "segment_a"), ReadFilter("panel_alias", "eq", "segment_b")),
        (ReadFilter("period", "gt", 2024),),
        (ReadFilter("score", "eq", "10"),),
        (ReadFilter("service_code", "eq", "synthetic"),),
    ],
)
def test_selectors_reject_duplicate_aliases_metrics_and_child_fields(selectors):
    with pytest.raises(CustomImportReadRequestError):
        grouped_read.normalize_plan(fixture.context(), fixture.query(context_filters=selectors), _SCOPE)


@pytest.mark.parametrize(
    "changes",
    [
        {"filters": (ReadFilter("score", "gt", "10"),)},
        {"order_terms": (ReadOrderTerm("score", "asc", "last"),)},
        {
            "context_filters": (ReadFilter("segment", "eq", "segment_a"),),
            "filters": (ReadFilter("period", "eq", 2024),),
        },
    ],
)
def test_metrics_and_order_require_one_explicit_group(changes):
    with pytest.raises(CustomImportReadRequestError):
        grouped_read.normalize_plan(fixture.context(), fixture.query(**changes), _SCOPE)


def test_grouped_predicate_budget_counts_default_year_and_leaves_legacy_cap_unchanged():
    metrics = (ReadFilter("score", "gt", "1"), ReadFilter("score", "lt", "100"), ReadFilter("npi", "eq", "1234567893"))
    selectors = (ReadFilter("segment", "eq", "segment_a"),)
    assert (
        len(
            grouped_read.normalize_plan(
                fixture.context(), fixture.query(context_filters=selectors, filters=metrics), _SCOPE
            ).filters
        )
        == 3
    )
    with pytest.raises(CustomImportReadRequestError):
        grouped_read.normalize_plan(
            fixture.context(),
            fixture.query(
                context_filters=selectors,
                filters=metrics + (ReadFilter("score", "eq", "10"), ReadFilter("score", "eq", "20")),
            ),
            _SCOPE,
        )
    allowed = fixture.query(context_filters=selectors, filters=metrics[:2])
    assert grouped_read.normalize_plan(fixture.context(), allowed, _SCOPE).selected_value is None
    explicit = replace(allowed, context_filters=selectors + (ReadFilter("period", "eq", 2024),))
    assert grouped_read.normalize_plan(fixture.context(), explicit, _SCOPE).selected_value == 2024
    with pytest.raises(CustomImportReadRequestError):
        read_core._validate_npi_entity_relation_request(
            explicit.filters, None, context_filters=explicit.context_filters
        )


def test_fingerprint_binds_canonical_aliases_selection_scope_projection_and_target():
    context = fixture.context()
    implicit = fixture.query()
    canonical = fixture.query(context_filters=(ReadFilter("period", "eq", 2024),))
    aliased = fixture.query(context_filters=(ReadFilter("year_alias", "eq", 2024),))
    baseline = grouped_read.normalize_plan(context, canonical, _SCOPE).fingerprint
    assert baseline == grouped_read.normalize_plan(context, aliased, _SCOPE).fingerprint
    assert grouped_read.normalize_plan(context, implicit, _SCOPE) == grouped_read.normalize_plan(
        context, replace(implicit, context_filters=()), _SCOPE
    )
    variants = [
        grouped_read.normalize_plan(context, implicit, _SCOPE),
        grouped_read.normalize_plan(context, canonical, ExtensionReadScope("synthetic:other")),
        grouped_read.normalize_plan(context, canonical, _SCOPE, projection="full_family"),
        grouped_read.normalize_plan(
            replace(context, target=replace(context.target, generation_id=9)), canonical, _SCOPE
        ),
    ]
    assert all(plan.fingerprint != baseline for plan in variants)


@pytest.mark.parametrize("descriptor", [None, {}, {**fixture.selection_document(), "default_profile": "other"}])
def test_descriptor_must_exactly_match_pinned_definition(descriptor):
    request = replace(fixture.query(), grouped_entity_selection=descriptor)
    with pytest.raises(CustomImportReadRequestError):
        grouped_read.normalize_plan(fixture.context(), request, _SCOPE)


def test_legacy_detail_stays_closed_and_opt_in_normalizes_omitted_context():
    document_map = {
        "target": http_fixture._TARGET,
        "entity": {"adapter_id": "npi", "value": "1234567893"},
        "family_entitlement": "full_family",
    }
    legacy_body = transport._canonical_json_bytes(document_map)
    assert transport._parse_detail_request(legacy_body).context is None
    document_map["context"] = []
    with pytest.raises(transport.CustomImportReadTransportError):
        transport._parse_detail_request(transport._canonical_json_bytes(document_map))
    document_map["grouped_entity_selection"] = fixture.selection_document()
    parsed = transport._parse_detail_request(transport._canonical_json_bytes(document_map))
    document_map.pop("context")
    assert parsed == transport._parse_detail_request(transport._canonical_json_bytes(document_map))


@pytest.mark.asyncio
@pytest.mark.parametrize("descriptor", [None, False, [], "families"])
async def test_malformed_grouped_descriptors_stop_before_storage(monkeypatch, descriptor):
    http_fixture._install_keyring(monkeypatch)
    session = provider_fixture._Session()
    provider_request = provider_fixture._request(provider_fixture._body(grouped_entity_selection=descriptor))
    detail_body = transport._canonical_json_bytes(
        {
            "target": http_fixture._TARGET,
            "entity": {"adapter_id": "npi", "value": "1234567893"},
            "family_entitlement": "full_family",
            "grouped_entity_selection": descriptor,
        }
    )
    detail_request = http_fixture._Request(
        detail_body,
        http_fixture._headers(body=detail_body, path=transport.CUSTOM_IMPORT_DETAIL_PATH),
        path=transport.CUSTOM_IMPORT_DETAIL_PATH,
    )

    provider_reply = await provider_http.serve_custom_import_providers(provider_request, session)
    detail_reply = await transport.serve_custom_import_detail(detail_request, session)

    for reply in (provider_reply, detail_reply):
        assert reply.status == 404
        assert reply.headers["cache-control"] == "private, no-store"
        assert orjson.loads(reply.body) == {"error": {"code": "resource_not_found", "message": "Resource not found."}}
    assert session.events == []


def test_provider_parser_admits_only_opt_in_four_predicate_total():
    document_map = {
        "target": http_fixture._TARGET,
        "native_query": {},
        "order": None,
        "require_match": True,
        "context": [
            {"field_id": "period", "operator": "eq", "value": 2024},
            {"field_id": "segment", "operator": "eq", "value": "segment_a"},
        ],
        "filters": [
            {"field_id": "score", "operator": "gt", "value": "1"},
            {"field_id": "score", "operator": "lt", "value": "20"},
        ],
        "grouped_entity_selection": fixture.selection_document(),
    }
    parsed = provider_http._parse_provider_request(transport._canonical_json_bytes(document_map))
    assert parsed.grouped_entity_selection == fixture.selection_document()
    document_map.pop("grouped_entity_selection")
    with pytest.raises(CustomImportReadRequestError):
        provider_http._parse_provider_request(transport._canonical_json_bytes(document_map))


@pytest.mark.parametrize("entitlement", [None, False, 1, {}, "query_projection", "unknown"])
def test_page_entitlement_rejects_non_exact_signed_values(entitlement):
    request_map = {
        "target": http_fixture._TARGET,
        "native_query": {},
        "context": [],
        "filters": [],
        "order": None,
        "require_match": True,
        "grouped_entity_selection": fixture.selection_document(),
        "family_entitlement": entitlement,
    }
    with pytest.raises(transport.CustomImportReadTransportError):
        provider_http._parse_provider_request(transport._canonical_json_bytes(request_map))


def test_page_entitlement_requires_grouped_metadata_and_binds_projection():
    request_map = {
        "target": http_fixture._TARGET,
        "native_query": {},
        "context": [],
        "filters": [],
        "order": None,
        "require_match": True,
        "family_entitlement": "full_family",
    }
    with pytest.raises(transport.CustomImportReadTransportError):
        provider_http._parse_provider_request(transport._canonical_json_bytes(request_map))
    with pytest.raises(CustomImportReadRequestError):
        read_core.NpiEntityRelationQuery(family_entitlement="full_family")
    request_map["grouped_entity_selection"] = fixture.selection_document()
    parsed = provider_http._parse_provider_request(transport._canonical_json_bytes(request_map))
    full_query = provider_http._provider_relation_query(parsed)
    complete = grouped_read.normalize_plan(fixture.context(), full_query, _SCOPE)
    projection = grouped_read.normalize_plan(fixture.context(), fixture.query(), _SCOPE)
    assert projection.fingerprint == "27821a4cfe52d3e87138684f38ba7308278703152dc3381a233a0d0c1afe183a"
    assert complete.projection == "full_family" and projection.projection == "query_projection"
    assert complete.fingerprint != projection.fingerprint
    request_map.pop("family_entitlement")
    omitted = provider_http._parse_provider_request(transport._canonical_json_bytes(request_map))
    assert (
        grouped_read.normalize_plan(fixture.context(), provider_http._provider_relation_query(omitted), _SCOPE)
        == projection
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["none", "digest", "document", "slot"])
async def test_helper_profile_is_derived_from_exact_target_and_reverified(monkeypatch, mutation):
    context = fixture.context()
    helper = next(profile for profile in context.definition.selection_profiles if profile.profile_id == "latest_period")
    profile_map = read_core._profile_document(helper, None)
    persisted = SimpleNamespace(
        profile_id="latest_period",
        profile_slot=2,
        context_collection_slot=0,
        canonical_profile=canonical_json(profile_map),
        profile_sha256=bytes.fromhex(canonical_sha256(profile_map, domain="profile")),
    )
    if mutation == "digest":
        persisted.profile_sha256 = b"x" * 32
    if mutation == "document":
        persisted.canonical_profile = "{}"
    if mutation == "slot":
        persisted.profile_slot = 1

    async def exact_profile(session, target):
        assert target == replace(context.target, profile_id="latest_period")
        return persisted

    monkeypatch.setattr(read_core, "_exact_profile", exact_profile)
    if mutation == "none":
        assert (
            await grouped_read.bind_default_profile(None, replace(context, default_profile_slot=None))
        ).default_profile_slot == 2
    else:
        with pytest.raises(read_core.CustomImportReadUnavailableError):
            await grouped_read.bind_default_profile(None, context)


@pytest.mark.parametrize("mutation", ["none", "digest", "projected_year", "projected_type", "duplicate", "mixed_year"])
def test_emitted_family_identity_is_checked_and_order_is_configured(mutation):
    context = fixture.context()
    plan = grouped_read.normalize_plan(context, fixture.query(), _SCOPE)
    selected_rows, projections = [], []
    for group in ("segment_b", "segment_a"):
        year = 2023 if mutation == "mixed_year" and group == "segment_b" else 2024
        digest = grouped_read._profile_context_digest(context, "families_by_period", {"period": year, "segment": group})
        winner = SimpleNamespace(context_key_sha256=b"x" * 32 if mutation == "digest" else digest)
        selected_rows.append((winner, None, None, "1234567893", year, group))
        projections.append(
            SimpleNamespace(
                root_fields=(
                    read_core.ReadFieldValue(
                        "period",
                        "string" if mutation == "projected_type" else "integer",
                        "value",
                        2022 if mutation == "projected_year" else year,
                    ),
                    read_core.ReadFieldValue("segment", "string", "value", group),
                )
            )
        )
    if mutation == "duplicate":
        selected_rows[1] = selected_rows[0]
    if mutation == "none":
        family_set = grouped_read._family_sets(
            context, plan, selected_rows, projections, _SCOPE, projection="query_projection"
        )["1234567893"]
        assert [group for group, _ in family_set.families] == ["segment_a", "segment_b"]
    else:
        with pytest.raises(read_core.CustomImportReadUnavailableError):
            grouped_read._family_sets(context, plan, selected_rows, projections, _SCOPE, projection="query_projection")


def _complete_family_case(child_counts, actual_counts=None):
    selected_rows, root_scalars, memberships, child_scalars = [], [], [], []
    for index, child_count in enumerate(child_counts):
        family = SimpleNamespace(family_revision_id=index + 1, root_record_id=index + 101, child_count=child_count)
        root = SimpleNamespace(root_revision_id=index + 301)
        winner = SimpleNamespace(entity_binding_id=index + 201, context_key_sha256=b"a" * 32)
        selected_rows.append((winner, family, root, str(1000000000 + index // 2), 2024, "segment_a"))
        root_scalars.append(
            SimpleNamespace(
                root_revision_id=root.root_revision_id,
                field_slot=2,
                field_type="string",
                value_state="value",
                string_value=f"Root {family.family_revision_id}",
            )
        )
        for ordinal in range(child_count if actual_counts is None else actual_counts[index]):
            child_id = index * 10 + 1001 + ordinal
            memberships.append(
                (
                    SimpleNamespace(family_revision_id=family.family_revision_id, collection_slot=1),
                    SimpleNamespace(child_revision_id=child_id, source_ordinal=ordinal),
                )
            )
            child_scalars.append(
                SimpleNamespace(
                    child_revision_id=child_id,
                    field_slot=4,
                    field_type="string",
                    value_state="value",
                    string_value=f"Service {child_id}",
                )
            )
            if ordinal == 0:
                child_scalars.append(
                    SimpleNamespace(child_revision_id=child_id, field_slot=5, field_type="decimal", value_state="null")
                )
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: root_scalars)),
                SimpleNamespace(all=lambda: memberships),
                SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: child_scalars)),
            ]
        )
    )
    return tuple(reversed(selected_rows)), session


@pytest.mark.asyncio
@pytest.mark.parametrize("family_count", [1, 2, 100])
async def test_complete_families_batch_three_queries_without_mixing_projections(family_count):
    context = fixture.context()
    selected_rows, session = _complete_family_case((2,) * family_count)
    details = await grouped_read._hydrate_complete_families(session, context, selected_rows, _SCOPE)
    assert session.execute.await_count == 3
    assert len(details) == family_count
    for selected_row, detail in zip(selected_rows, details, strict=True):
        winner, family = selected_row[:2]
        assert detail.target == context.target
        assert detail.winner == read_core._winner_locator(winner, family)
        assert detail.authorization_scope_sha256 == read_core._scope_digest(_SCOPE)
        assert next(field.value for field in detail.root_fields if field.field_id == "display_name") == (
            f"Root {family.family_revision_id}"
        )
        assert next(field.state for field in detail.root_fields if field.field_id == "score") == "missing"
        child_ids = [(family.family_revision_id - 1) * 10 + 1001 + ordinal for ordinal in range(2)]
        assert [child.child_revision_id for child in detail.children] == child_ids
        assert [child.collection for child in detail.children] == ["rates", "rates"]
        assert [child.fields[0].value for child in detail.children] == [f"Service {child_id}" for child_id in child_ids]
        assert [child.fields[1].state for child in detail.children] == ["null", "missing"]

    statement = session.execute.await_args_list[1].args[0]
    sql = str(statement.compile(compile_kwargs={"literal_binds": True}))
    membership = "mrf.custom_import_family_child"
    child = "mrf.custom_import_child_revision"
    for field in ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
        assert f"{child}.{field} = {membership}.{field}" in sql
    assert f"({membership}.family_revision_id, {membership}.root_record_id) IN (" in sql
    for selected_row in selected_rows:
        assert f"({selected_row[1].family_revision_id}, {selected_row[1].root_record_id})" in sql
    assert f"{membership}.dataset_id = {context.target.dataset_id}" in sql
    assert f"{membership}.schema_revision_id = {context.target.schema_revision_id}" in sql
    assert sql.endswith(
        f"ORDER BY {membership}.family_revision_id, {membership}.collection_slot, "
        f"{child}.source_ordinal, {child}.child_revision_id"
    )
    if family_count == 1:
        _selected_rows, singleton_session = _complete_family_case((2,))
        detail = await read_core._hydrate_root_detail(
            singleton_session,
            context,
            read_core._winner_row(selected_rows[0][:3], False),
            read_core._scope_digest(_SCOPE),
        )
        assert detail == details[0]
        assert singleton_session.execute.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("actual_counts", [(0, 2), (2, 1), (1, 3)])
async def test_complete_families_require_each_exact_count_before_child_scalar_hydration(actual_counts):
    selected_rows, session = _complete_family_case((1, 2), actual_counts)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="membership is incomplete"):
        await grouped_read._hydrate_complete_families(session, fixture.context(), selected_rows, _SCOPE)
    assert session.execute.await_count == 2


@pytest.mark.asyncio
async def test_complete_families_preflight_aggregate_limit_and_skip_empty_queries(monkeypatch):
    context = fixture.context()
    selected_rows, session = _complete_family_case((2, 2))
    monkeypatch.setattr(read_core, "MAX_DETAIL_CHILDREN", 3)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="bounded detail child limit"):
        await grouped_read._hydrate_complete_families(session, context, selected_rows, _SCOPE)
    assert await grouped_read._hydrate_complete_families(session, context, (), _SCOPE) == ()
    session.execute.assert_not_awaited()
    selected_rows, session = _complete_family_case((0, 0))
    details = await grouped_read._hydrate_complete_families(session, context, selected_rows, _SCOPE)
    assert len(details) == 2 and all(detail.children == () for detail in details)
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_full_family_cardinality_guards_stop_before_storage():
    context = fixture.context()
    query = fixture.query(family_entitlement="full_family")
    plan = grouped_read.normalize_plan(context, query, _SCOPE)
    prepared = read_core.PreparedNpiEntityRelation(None, (), plan.fingerprint, read_core._scope_digest(_SCOPE))
    selected_rows, session = _complete_family_case((0,) * 102)
    entity_values = tuple(dict.fromkeys(selected_row[-3] for selected_row in selected_rows))
    assert len(entity_values) == 51

    with pytest.raises(CustomImportReadRequestError, match="full-family provider page exceeds its bound"):
        await grouped_read.hydrate_page(session, context, query, prepared, entity_values, _SCOPE)
    session.execute.assert_not_awaited()

    with pytest.raises(read_core.CustomImportReadUnavailableError, match="full-family provider page exceeds its bound"):
        await grouped_read._hydrate_complete_families(session, context, selected_rows, _SCOPE, is_page=True)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("providers", [25, 50])
async def test_full_family_pages_batch_within_detail_bounds_and_preserve_order(monkeypatch, providers):
    selected_rows, _session = _complete_family_case((0,) * (providers * 2))
    for selected_row in selected_rows:
        selected_row[1].child_count = 21

    async def hydrate(_session, context, rows, _scope):
        assert sum(row[1].child_count for row in rows) <= read_core.MAX_DETAIL_CHILDREN
        return tuple(
            SimpleNamespace(
                root_fields=(read_core.ReadFieldValue("name", "string", "value", str(row[1].family_revision_id)),),
                children=tuple(SimpleNamespace(collection="rates", fields=()) for _ in range(row[1].child_count)),
            )
            for row in rows
        )

    hydration = AsyncMock(side_effect=hydrate)
    monkeypatch.setattr(read_core, "_hydrate_selected_families", hydration)
    details = await grouped_read._hydrate_complete_families(None, fixture.context(), selected_rows, _SCOPE)
    assert sum(len(detail.children) for detail in details) == providers * 42
    assert [detail.root_fields[0].value for detail in details] == [
        str(selected_row[1].family_revision_id) for selected_row in selected_rows
    ]
    assert hydration.await_count == (2 if providers == 25 else 3)

    # An oversized entity anywhere in the page prevents all child queries.
    hydration.reset_mock()
    selected_rows[-1][1].child_count = read_core.MAX_DETAIL_CHILDREN
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="bounded detail child limit"):
        await grouped_read._hydrate_complete_families(None, fixture.context(), selected_rows, _SCOPE)
    hydration.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_family_page_rejects_large_entity_before_retaining_next_batch(monkeypatch):
    selected_rows, _session = _complete_family_case((0, 0, 0, 0))
    for row in selected_rows:
        row[1].child_count = read_core.MAX_DETAIL_CHILDREN // 2
    hydration = AsyncMock(
        return_value=(
            SimpleNamespace(
                root_fields=(
                    read_core.ReadFieldValue("text", "string", "value", "x" * grouped_read.MAX_FAMILY_RESPONSE_BYTES),
                ),
                children=(),
            ),
        )
        * 2
    )
    monkeypatch.setattr(read_core, "_hydrate_selected_families", hydration)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="family response exceeds"):
        await grouped_read._hydrate_complete_families(None, fixture.context(), selected_rows, _SCOPE, is_page=True)
    assert hydration.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("children_per_group", [40, 80])
async def test_unicode_detail_and_page_keep_their_wire_bounds(monkeypatch, children_per_group):
    context = fixture.context()
    entity_npi = "1234567893"
    selected_rows, projections = [], []
    for group in ("segment_a", "segment_b"):
        winner = SimpleNamespace(
            context_key_sha256=grouped_read._profile_context_digest(
                context, "families_by_period", {"period": 2024, "segment": group}
            )
        )
        selected_rows.append((winner, SimpleNamespace(child_count=children_per_group), None, entity_npi, 2024, group))
        projections.append(
            SimpleNamespace(
                root_fields=(
                    read_core.ReadFieldValue("period", "integer", "value", 2024),
                    read_core.ReadFieldValue("segment", "string", "value", group),
                ),
                children=tuple(
                    SimpleNamespace(
                        collection="rates",
                        fields=(
                            read_core.ReadFieldValue("service_code", "string", "value", "界" * 300 + str(ordinal)),
                        ),
                    )
                    for ordinal in range(children_per_group)
                ),
            )
        )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=1),
        execute=AsyncMock(return_value=SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [entity_npi]))),
    )
    monkeypatch.setattr(grouped_read, "_selected_family_rows", AsyncMock(return_value=tuple(selected_rows)))
    monkeypatch.setattr(read_core, "_hydrate_selected_families", AsyncMock(return_value=tuple(projections)))
    monkeypatch.setattr(read_core, "verify_published_generation", AsyncMock())
    detail_request = read_core.RootDetailRequest(
        context.target, read_core.EntityLocator("npi", entity_npi), "full_family", (), fixture.selection_document()
    )
    detail = await grouped_read.hydrate_detail(session, context, detail_request, _SCOPE)
    external_target = transport._TransportTarget(**http_fixture._TARGET)
    detail_payload = transport._detail_payload(detail, external_target)
    assert len(orjson.dumps(detail_payload)) < transport._MAX_RESPONSE_BYTES
    assert sum(len(family.children) for _, family in detail.families) == children_per_group * 2

    page_query = fixture.query(family_entitlement="full_family")
    page_plan = grouped_read.normalize_plan(context, page_query, _SCOPE)
    prepared = grouped_read.prepare_relation(context, page_query, _SCOPE)
    if children_per_group == 80:
        assert len(transport._canonical_json_bytes(detail_payload)) > transport._MAX_RESPONSE_BYTES
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="family response exceeds"):
            await grouped_read.hydrate_page(session, context, page_query, prepared, (entity_npi,), _SCOPE)
    else:
        assert len(transport._canonical_json_bytes(detail_payload)) < transport._MAX_RESPONSE_BYTES
        imported = await grouped_read.hydrate_page(session, context, page_query, prepared, (entity_npi,), _SCOPE)
        assert transport._provider_import_payload(imported[entity_npi], external_target) == detail_payload


@pytest.mark.asyncio
@pytest.mark.parametrize(("children_per_group", "status"), [(80, 200), (160, 503)])
async def test_grouped_detail_enforces_final_utf8_envelope(monkeypatch, children_per_group, status):
    http_fixture._install_keyring(monkeypatch)
    session = provider_fixture._Session()
    child_projections = tuple(
        SimpleNamespace(
            collection="rates", fields=(read_core.ReadFieldValue("service_code", "string", "value", "界" * 300),)
        )
        for _ in range(children_per_group)
    )
    families = tuple(
        (
            group,
            SimpleNamespace(
                root_fields=(
                    read_core.ReadFieldValue("period", "integer", "value", 2024),
                    read_core.ReadFieldValue("segment", "string", "value", group),
                ),
                children=child_projections,
            ),
        )
        for group in ("segment_a", "segment_b")
    )
    detail = read_core.EntityFamilySet(None, "full_family", "period", 2024, families, (), "b" * 64, "c" * 64)
    service = SimpleNamespace(root_detail_for_entity=AsyncMock(return_value=detail))
    monkeypatch.setattr(transport, "CustomImportReadService", lambda **_kwargs: service)
    monkeypatch.setattr(transport, "_resolve_pinned_target", http_fixture._resolved_target)
    body = transport._canonical_json_bytes(
        {
            "target": http_fixture._TARGET,
            "entity": {"adapter_id": "npi", "value": "1234567893"},
            "family_entitlement": "full_family",
            "grouped_entity_selection": fixture.selection_document(),
        }
    )
    request = http_fixture._Request(
        body,
        http_fixture._headers(body=body, path=transport.CUSTOM_IMPORT_DETAIL_PATH),
        path=transport.CUSTOM_IMPORT_DETAIL_PATH,
    )
    encoded = orjson.dumps(transport._detail_payload(detail, transport._TransportTarget(**http_fixture._TARGET)))
    assert children_per_group * 2 <= read_core.MAX_DETAIL_CHILDREN
    assert (len(encoded) <= transport._MAX_RESPONSE_BYTES) is (status == 200)

    reply = await transport.serve_custom_import_detail(request, session)

    assert reply.status == status
    assert reply.headers["cache-control"] == "private, no-store"
    assert session.events == ["begin", "snapshot", "end"]
    assert session.rolled_back is (status == 503)
    service.root_detail_for_entity.assert_awaited_once()
    if status == 200:
        assert reply.body == encoded
    else:
        assert orjson.loads(reply.body) == {
            "error": {
                "code": "custom_import_read_unavailable",
                "message": "Custom import read is temporarily unavailable.",
            }
        }
