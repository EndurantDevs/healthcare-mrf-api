# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real materializer and native SQL proof for bounded grouped root reads."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal

import pytest
from sanic import response
from sqlalchemy import func, literal, select, text

from api import custom_import_detail_batch as batch_http
from api import custom_import_provider_geo as geo_http
from api import custom_import_provider_http as provider_http
from api import custom_import_provider_service_http as service_http
from api import custom_import_read_http as transport
from api.custom_import_provider_sql import compile_npi_entity_relation
from process.custom_import import read_core
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.read_core import (
    CustomImportReadEntityAbsentError,
    CustomImportReadRequestError,
    EntityLocator,
    ExtensionReadAuthorization,
    ExtensionReadScope,
    NpiEntityRelationQuery,
    PinnedReadTarget,
    ReadFilter,
    ReadOrderTerm,
    RootDetailRequest,
)
from process.custom_import.runner import run_candidate
from tests import custom_import_grouped_support as fixture
from tests import test_custom_import_provider_query_postgres as npi_fixture
from tests import test_custom_import_read_core_postgres as read_fixture
from tests import test_custom_import_read_http as http_fixture
from tests import test_custom_import_runner_postgres as runner_fixture
from tests.custom_import_postgres_support import (
    _quoted_publication_schema,
    digest,
    isolated_publication_case,
    transaction_session,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("child_index", "state"), ((0, "missing"), (1, "null")))
async def test_native_page_hydration_uses_only_matching_context_and_family(monkeypatch, child_index, state):
    npi_fixture._seed_npi_bindings(monkeypatch)
    npi_fixture._seed_selected_contexts(monkeypatch, (0, 1))
    async with transaction_session() as session, session.begin():
        fixture = await npi_fixture._seed_npi_fixture(session)
        service = read_fixture._service()
        filters = (ReadFilter("synthetic_alpha", "eq", ("high-1", "low-1")[child_index]),)
        query_map = {
            "authorization": ExtensionReadAuthorization("synthetic"),
            "query": NpiEntityRelationQuery(filters=filters),
        }
        prepared = await service.prepare_npi_entity_relation(session, target=fixture.target, **query_map)
        hydrated_by_npi = await service.hydrate_npi_page(
            session,
            pinned_target=fixture.target,
            **query_map,
            prepared=prepared,
            entity_values=(npi_fixture._NPI, npi_fixture._ABSENT_NPI),
        )

        assert set(hydrated_by_npi) == {npi_fixture._NPI}
        selected_item = hydrated_by_npi[npi_fixture._NPI]
        assert selected_item.winner.family_revision_id == fixture.selected_family.family_revision_id
        assert selected_item.context_child_revision_id == fixture.selected_family.child_revision_ids[child_index]
        assert (
            next(field_value for field_value in selected_item.context_fields if field_value.field_id == "amount").state
            == state
        )
        assert selected_item.root_fields[0].value == "synthetic-root"
        assert (
            tuple(child.child_revision_id for child in selected_item.children)
            == fixture.selected_family.child_revision_ids
        )
        assert fixture.foreign_child_revision_id not in {child.child_revision_id for child in selected_item.children}


@pytest.mark.asyncio
async def test_filter_only_multi_context_hydration_is_deterministic_and_bounded(monkeypatch):
    npi_fixture._seed_npi_bindings(monkeypatch)
    npi_fixture._seed_selected_contexts(monkeypatch, (0, 1))
    async with transaction_session() as session, session.begin():
        fixture = await npi_fixture._seed_npi_fixture(session)
        service = read_fixture._service()
        query_map = {
            "authorization": ExtensionReadAuthorization("synthetic"),
            "query": NpiEntityRelationQuery(context_filters=()),
        }
        prepared = await service.prepare_npi_entity_relation(session, target=fixture.target, **query_map)
        entities = tuple(str(1000000000 + index) for index in range(200))
        result = await service.hydrate_npi_page(
            session, pinned_target=fixture.target, **query_map, prepared=prepared, entity_values=entities
        )
        expected_index = min((0, 1), key=lambda index: digest(f"provider-query-context:{index}"))

        assert set(result) == {npi_fixture._NPI}
        assert (
            result[npi_fixture._NPI].context_child_revision_id
            == fixture.selected_family.child_revision_ids[expected_index]
        )
        assert (
            await service.hydrate_npi_page(
                session, pinned_target=fixture.target, **query_map, prepared=prepared, entity_values=()
            )
            == {}
        )


_A, _B, _C = "1234567893", "1000000012", "1000000020"
_AUTHORIZATION = ExtensionReadAuthorization("synthetic")


class _ExactAuthorizer:
    def __init__(self, target):
        self.target = target
        self.targets = []

    def authorize(self, authorization, *, target):
        self.targets.append(target)
        return ExtensionReadScope("synthetic:grouped") if target == self.target else None


def _roots():
    return [
        {"npi": npi, "display_name": "Synthetic", "period": period, "segment": segment, "score": Decimal(score)}
        for npi, period, segment, score in (
            (_A, 2024, "segment_a", "10"),
            (_A, 2024, "segment_b", "20"),
            (_A, 2023, "segment_a", "100"),
            (_A, 2023, "segment_b", "100"),
            (_B, 2025, "segment_a", "30"),
            (_B, 2024, "segment_b", "90"),
            (_C, 2026, "segment_c", "0"),
            (_C, 2025, "segment_a", "999"),
        )
    ]


def _children():
    return [
        {"rate_npi": _A, "rate_period": year, "rate_segment": segment, "service_code": code, "amount": Decimal("2")}
        for year, segment, code in (
            (2024, "segment_a", "current_a"),
            (2024, "segment_b", "current_b"),
            (2023, "segment_a", "older"),
        )
    ]


@asynccontextmanager
async def _case(*, children=None):
    async with isolated_publication_case() as case:
        seed = await runner_fixture._seed_case(case, "grouped", fixture.definition())
        execution_id, token = await runner_fixture._new_execution(case, seed, "grouped")
        result = await run_candidate(
            case.sessions,
            runner_fixture._request(seed, execution_id, token, _roots(), _children() if children is None else children),
        )
        assert result.status == "activated"
        assert result.accepted_family_count == 8 and result.rejection_count == 0
        target = PinnedReadTarget(
            seed.dataset_id,
            result.generation_id,
            seed.definition_revision_id,
            seed.schema_revision_id,
            "families_by_period",
        )
        yield case, target


def _service(target):
    return read_core.CustomImportReadService(authorizer=_ExactAuthorizer(target))


async def _relation(session, target, query):
    service = _service(target)
    prepared = await service.prepare_npi_entity_relation(
        session, authorization=_AUTHORIZATION, target=target, query=query
    )
    rows = (await session.execute(prepared.statement)).all()
    compiled = compile_npi_entity_relation(prepared.statement)
    # Textual native SQL needs the fixture's exact isolated schema translation.
    schema = _quoted_publication_schema(session.get_bind().get_execution_options()["schema_translate_map"]["mrf"])
    scoped_sql = text(compiled.sql.replace("mrf.", f"{schema}.")).bindparams(*compiled.typed_binds)
    compiled_rows = (await session.execute(scoped_sql)).all()
    assert sorted(tuple(row) for row in compiled_rows) == sorted(tuple(row) for row in rows)
    total = await session.scalar(select(func.count()).select_from(prepared.statement.subquery()))
    assert total == len(rows) == len({row.entity_value for row in rows})
    return prepared, rows


async def _page(session, target, query):
    prepared, _rows = await _relation(session, target, query)
    return await _service(target).hydrate_npi_page(
        session,
        authorization=_AUTHORIZATION,
        pinned_target=target,
        query=query,
        prepared=prepared,
        entity_values=(_A, _B, _C),
    )


@pytest.mark.asyncio
async def test_signed_exact_batch_hydrates_full_families_without_membership_filtering(monkeypatch):
    http_fixture._install_keyring(monkeypatch)
    async with _case() as (case, pinned_target):
        external_target_map = transport._target_document(_external_target(pinned_target))
        npi_values = [_B, "1999999999", _A]
        body = transport._canonical_json_bytes(
            {
                "target": external_target_map,
                "entities": {"adapter_id": "npi", "values": npi_values},
                "family_entitlement": "full_family",
                "grouped_entity_selection": fixture.selection_document(),
            }
        )
        request = http_fixture._Request(
            body,
            http_fixture._resigned_headers(
                body=body, path=batch_http.CUSTOM_IMPORT_DETAIL_BATCH_PATH, target=external_target_map
            ),
            path=batch_http.CUSTOM_IMPORT_DETAIL_BATCH_PATH,
        )
        async with case.sessions() as session:
            reply = await batch_http.serve_custom_import_detail_batch(request, session)
        assert reply.status == 200, reply.body
        provider_items = json.loads(reply.body)["items"]
        assert [provider["npi"] for provider in provider_items] == npi_values
        assert provider_items[1]["custom_import"] is None
        assert provider_items[0]["custom_import"]["selection"]["value"] == 2025
        imported = provider_items[2]["custom_import"]
        assert imported["projection"] == "full_family" and imported["selection"]["value"] == 2024
        assert [family["group_value"] for family in imported["families"]] == ["segment_a", "segment_b"]
        assert sum(len(family["children"]) for family in imported["families"]) == 2


@asynccontextmanager
async def _ordinary_root_case():
    """Publish a family whose complete root projection exceeds its query fields."""

    ordinary_definition_map = json.loads(runner_fixture._definition().canonical)
    ordinary_definition_map["query"]["root_fields"] = ["npi"]
    ordinary_definition = CustomImportDefinition.from_mapping(ordinary_definition_map)
    async with isolated_publication_case() as ordinary_case:
        ordinary_seed = await runner_fixture._seed_case(ordinary_case, "batch_ordinary", ordinary_definition)
        execution_id, lease_token = await runner_fixture._new_execution(ordinary_case, ordinary_seed, "batch_ordinary")
        run_result = await run_candidate(
            ordinary_case.sessions,
            runner_fixture._request(
                ordinary_seed,
                execution_id,
                lease_token,
                [{"npi": _A, "display_name": "Synthetic Complete"}],
                [
                    {"rate_npi": _A, "service_code": "first", "amount": Decimal("2")},
                    {"rate_npi": _A, "service_code": "second", "amount": None},
                ],
            ),
        )
        assert run_result.status == "activated", run_result
        pinned_target = PinnedReadTarget(
            ordinary_seed.dataset_id,
            run_result.generation_id,
            ordinary_seed.definition_revision_id,
            ordinary_seed.schema_revision_id,
            "default",
        )
        yield ordinary_case, pinned_target


@pytest.mark.asyncio
async def test_signed_ordinary_batch_matches_full_detail_without_expanding_provider_projection(monkeypatch):
    """Keep batch detail complete without widening legacy provider projections."""

    http_fixture._install_keyring(monkeypatch)
    async with _ordinary_root_case() as (ordinary_case, pinned_target):
        external_target = _external_target(pinned_target, "synthetic_runner_batch_ordinary")
        target_map = transport._target_document(external_target)
        requested_npis = [_A, "1999999999"]
        response_documents = []
        for path, entity_document, detail_endpoint in (
            (
                batch_http.CUSTOM_IMPORT_DETAIL_BATCH_PATH,
                {"entities": {"adapter_id": "npi", "values": requested_npis}},
                batch_http.serve_custom_import_detail_batch,
            ),
            (
                transport.CUSTOM_IMPORT_DETAIL_PATH,
                {"entity": {"adapter_id": "npi", "value": _A}},
                transport.serve_custom_import_detail,
            ),
        ):
            canonical_body = transport._canonical_json_bytes(
                {"target": target_map, "family_entitlement": "full_family", **entity_document}
            )
            signed_headers = http_fixture._resigned_headers(body=canonical_body, path=path, target=target_map)
            signed_request = http_fixture._Request(canonical_body, signed_headers, path=path)
            async with ordinary_case.sessions() as session:
                http_reply = await detail_endpoint(signed_request, session)
            assert http_reply.status == 200, http_reply.body
            response_documents.append(json.loads(http_reply.body))
        batch_document, detail_document = response_documents
        assert [provider_item["npi"] for provider_item in batch_document["items"]] == requested_npis
        assert batch_document["items"][1]["custom_import"] is None
        assert batch_document["items"][0]["custom_import"] == detail_document
        assert [field["field_id"] for field in detail_document["root_fields"]] == ["npi", "display_name"]
        assert detail_document["root_fields"][1]["value"] == "Synthetic Complete"
        assert len(detail_document["children"]) == 2
        child_values = [
            {field["field_id"]: field["value"] for field in child["fields"]} for child in detail_document["children"]
        ]
        assert {child["service_code"]: child["amount"] for child in child_values} == {
            "first": "2.000000000000",
            "second": None,
        }

        async with ordinary_case.sessions() as session:
            projected = await _page(
                session,
                pinned_target,
                NpiEntityRelationQuery(context_filters=(ReadFilter("service_code", "eq", "first"),)),
            )
        assert set(projected) == {_A}
        assert [field.field_id for field in projected[_A].root_fields] == ["npi"]


def _external_target(
    target: PinnedReadTarget, attachment_id: str = "synthetic_runner_grouped"
) -> transport._TransportTarget:
    return transport._TransportTarget(
        attachment_id,
        target.generation_id,
        target.definition_revision_id,
        target.schema_revision_id,
        target.profile_id,
    )


@asynccontextmanager
async def _ordinary_ambiguous_case():
    """Materialize two valid ordinary roots sharing one canonical NPI."""

    definition_map = fixture.definition_document()
    definition_map["query"].pop("entity_selection")
    definition = CustomImportDefinition.from_mapping(definition_map)
    async with isolated_publication_case() as case:
        seed = await runner_fixture._seed_case(case, "ordinary_ambiguous", definition)
        execution_id, token = await runner_fixture._new_execution(case, seed, "ordinary_ambiguous")
        result = await run_candidate(
            case.sessions,
            runner_fixture._request(seed, execution_id, token, _roots()[:2], _children()[:2]),
        )
        assert result.status == "activated"
        assert result.accepted_family_count == 2 and result.rejection_count == 0
        target = PinnedReadTarget(
            seed.dataset_id,
            result.generation_id,
            seed.definition_revision_id,
            seed.schema_revision_id,
            "families_by_period",
        )
        yield case, target


@pytest.mark.asyncio
async def test_ordinary_full_family_batch_rejects_ambiguous_npi_like_scalar_detail():
    """Separate root families fail closed without changing provider search projection."""

    async with _ordinary_ambiguous_case() as (case, target), case.sessions() as session:
        service = _service(target)
        prepared = await service.prepare_npi_entity_relation(session, authorization=_AUTHORIZATION, target=target)
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="selected entity is not eligible"):
            await service.root_detail_for_entity(
                session,
                authorization=_AUTHORIZATION,
                request=RootDetailRequest(target, EntityLocator("npi", _A), "full_family"),
            )
        hydration_map = {
            "authorization": _AUTHORIZATION,
            "pinned_target": target,
            "prepared": prepared,
            "entity_values": (_A,),
        }
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="selected entity is not eligible"):
            await service.hydrate_npi_page(session, **hydration_map, full_family=True)
        projected = await service.hydrate_npi_page(session, **hydration_map)
        assert set(projected) == {_A}


def _detail_request(target, npi=_A, *, selectors=()):
    return RootDetailRequest(target, EntityLocator("npi", npi), "full_family", selectors, fixture.selection_document())


@pytest.mark.asyncio
async def test_latest_across_all_groups_deduplicates_entities_before_projection():
    async with _case() as (case, pinned_target), case.sessions() as session:
        prepared, relation_rows = await _relation(session, pinned_target, fixture.query())
        assert {relation_row.entity_value for relation_row in relation_rows} == {_A, _B}
        # A newer unconfigured group suppresses older configured families.
        projected = await _page(session, pinned_target, fixture.query())
        assert set(projected) == {_A, _B}
        assert projected[_A].selection_value == 2024
        assert [group for group, _ in projected[_A].families] == ["segment_a", "segment_b"]
        assert projected[_B].selection_value == 2025
        assert projected[_B].missing_group_values == ("segment_b",)
        family_set_document = transport._provider_import_payload(projected[_A], _external_target(pinned_target))
        assert set(family_set_document) == {
            "contract",
            "target",
            "projection",
            "selection",
            "families",
            "missing_group_values",
        }
        assert family_set_document["projection"] == "query_projection"
        assert all(
            set(family) == {"group_value", "root_fields", "context_fields"}
            for family in family_set_document["families"]
        )
        # The native address relation retains its own grain, not one relation_row per panel.
        addresses = (
            select(literal(_A).label("npi"), literal("one").label("address_key"))
            .union_all(
                select(literal(_A), literal("two")),
                select(literal(_B), literal("one")),
                select(literal(_C), literal("one")),
            )
            .subquery()
        )
        imported = prepared.statement.subquery()
        matched = (
            select(addresses.c.npi, addresses.c.address_key)
            .join(imported, addresses.c.npi == imported.c.entity_value)
            .distinct()
            .subquery()
        )
        assert await session.scalar(select(func.count()).select_from(matched)) == 3


@pytest.mark.asyncio
async def test_panel_and_metric_eligibility_never_change_year_or_hide_sibling_group():
    async with _case() as (case, target), case.sessions() as session:
        panel_b = (ReadFilter("panel_alias", "eq", "segment_b"),)
        projected = await _page(session, target, fixture.query(context_filters=panel_b))
        assert set(projected) == {_A}
        assert len(projected[_A].families) == 2
        no_fallback = fixture.query(context_filters=panel_b, filters=(ReadFilter("score", "gt", "50"),))
        assert await _page(session, target, no_fallback) == {}
        ordered = fixture.query(
            context_filters=(ReadFilter("segment", "eq", "segment_a"),),
            order_terms=(ReadOrderTerm("score", "desc", "last"),),
        )
        _prepared, rows = await _relation(session, target, ordered)
        assert {row.entity_value: row.sort_0 for row in rows} == {_A: Decimal("10"), _B: Decimal("30")}
        for query in (
            fixture.query(filters=(ReadFilter("score", "gt", "1"),)),
            fixture.query(order_terms=ordered.order_terms),
        ):
            with pytest.raises(CustomImportReadRequestError):
                await _relation(session, target, query)


@pytest.mark.asyncio
async def test_explicit_year_and_full_detail_preserve_exact_family_set():
    async with _case() as (case, pinned_target), case.sessions() as session:
        service = _service(pinned_target)
        for year, expected in ((2023, {_A}), (2024, {_A, _B}), (2022, set())):
            selectors = (ReadFilter("year_alias", "eq", year),)
            projected = await _page(session, pinned_target, fixture.query(context_filters=selectors))
            assert set(projected) == expected
            assert all(family_set.selection_value == year for family_set in projected.values())
        detail = await service.root_detail_for_entity(
            session, authorization=_AUTHORIZATION, request=_detail_request(pinned_target)
        )
        family_set_document = transport._detail_payload(detail, _external_target(pinned_target))
        assert family_set_document["selection"] == {"field_id": "period", "value": 2024}
        assert family_set_document["projection"] == "full_family"
        assert [family["group_value"] for family in family_set_document["families"]] == ["segment_a", "segment_b"]
        assert all(
            set(family) == {"group_value", "root_fields", "children"} for family in family_set_document["families"]
        )
        assert (
            len(family_set_document["families"][0]["children"])
            == len(family_set_document["families"][1]["children"])
            == 1
        )
        codes = [
            field["value"]
            for family in family_set_document["families"]
            for child in family["children"]
            for field in child["fields"]
            if field["field_id"] == "service_code"
        ]
        assert codes == ["current_a", "current_b"]
        absent = _detail_request(pinned_target, selectors=(ReadFilter("period", "eq", 2022),))
        with pytest.raises(CustomImportReadEntityAbsentError):
            await service.root_detail_for_entity(session, authorization=_AUTHORIZATION, request=absent)
        # Only the authorized active target ever reaches the host authorizer.
        assert service._authorizer.targets == [pinned_target, pinned_target]


@pytest.mark.asyncio
async def test_signed_detail_checks_descriptor_against_pinned_definition(monkeypatch):
    http_fixture._install_keyring(monkeypatch)
    async with _case() as (case, target):
        external = transport._target_document(_external_target(target))
        document_map = {
            "target": external,
            "entity": {"adapter_id": "npi", "value": _A},
            "family_entitlement": "full_family",
            "grouped_entity_selection": fixture.selection_document(),
        }
        for mode, status in (("valid", 200), ("mismatched", 400), ("missing", 400), ("absent", 404)):
            body_map = {**document_map}
            if mode == "mismatched":
                body_map["grouped_entity_selection"] = {**fixture.selection_document(), "default_profile": "other"}
            if mode == "missing":
                body_map.pop("grouped_entity_selection")
            if mode == "absent":
                body_map["context"] = [{"field_id": "period", "operator": "eq", "value": 2022}]
            body = transport._canonical_json_bytes(body_map)
            request = http_fixture._Request(
                body,
                http_fixture._resigned_headers(body=body, path=transport.CUSTOM_IMPORT_DETAIL_PATH, target=external),
                path=transport.CUSTOM_IMPORT_DETAIL_PATH,
            )
            async with case.sessions() as session:
                response = await transport.serve_custom_import_detail(request, session)
            assert response.status == status, (mode, response.body)
            if mode == "valid":
                assert len(json.loads(response.body)["families"]) == 2


@pytest.mark.asyncio
async def test_grouped_page_rejects_changed_query_identity_after_prepare():
    async with _case() as (case, target), case.sessions() as session:
        prepared, _rows = await _relation(session, target, fixture.query())
        changed = fixture.query(context_filters=(ReadFilter("period", "eq", 2023),))
        with pytest.raises(read_core.CustomImportReadUnavailableError):
            await _service(target).hydrate_npi_page(
                session,
                authorization=_AUTHORIZATION,
                pinned_target=target,
                query=changed,
                prepared=prepared,
                entity_values=(_A,),
            )
        with pytest.raises(CustomImportReadRequestError):
            await _service(replace(target, profile_id="latest_period")).prepare_npi_entity_relation(
                session,
                authorization=_AUTHORIZATION,
                target=replace(target, profile_id="latest_period"),
                query=fixture.query(),
            )


@pytest.mark.asyncio
async def test_aggregate_child_limit_is_checked_before_any_detail_hydration(monkeypatch):
    async with _case() as (case, target), case.sessions() as session:

        async def no_hydration(*args):
            pytest.fail("detail hydration must not begin above the aggregate child limit")

        monkeypatch.setattr(read_core, "MAX_DETAIL_CHILDREN", 1)
        monkeypatch.setattr(read_core, "_hydrate_selected_families", no_hydration)
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="bounded detail child limit"):
            await _service(target).root_detail_for_entity(
                session, authorization=_AUTHORIZATION, request=_detail_request(target)
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("entitlement", [None, "full_family"])
async def test_signed_grouped_service_read_rejects_before_native_claims(monkeypatch, entitlement):
    http_fixture._install_keyring(monkeypatch)

    async def no_native_claims(*args, **kwargs):
        pytest.fail("grouped root reads must not enter the native claims lane")

    monkeypatch.setattr(service_http, "_service_page", no_native_claims)
    async with _case() as (case, target), case.sessions() as session:
        target_map = transport._target_document(_external_target(target))
        request_map = {
            "target": target_map,
            "native_query": {"code": "99213", "code_system": "HP_PROCEDURE_CODE", "year": "2024"},
            "context": [],
            "filters": [],
            "order": None,
            "require_match": True,
            "grouped_entity_selection": fixture.selection_document(),
        }
        if entitlement is not None:
            request_map["family_entitlement"] = entitlement
        body = transport._canonical_json_bytes(request_map)
        path = service_http.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH
        request = http_fixture._Request(
            body, http_fixture._resigned_provider_headers(body=body, path=path, target=target_map), path=path
        )
        reply = await service_http.serve_custom_import_provider_service(request, session)
        assert reply.status == 400


def _page_children():
    return _children() + [
        {
            "rate_npi": npi,
            "rate_period": year,
            "rate_segment": "segment_a",
            "service_code": code,
            "amount": Decimal("3"),
        }
        for npi, year, code in ((_A, 2024, "second_a"), (_B, 2025, "current_b_entity"))
    ]


@pytest.mark.asyncio
async def test_full_page_uses_same_relation_and_exact_selected_families():
    async with _case(children=_page_children()) as (case, target), case.sessions() as session:
        projected_query = fixture.query(context_filters=(ReadFilter("segment", "eq", "segment_b"),))
        complete_query = replace(projected_query, family_entitlement="full_family")
        projected, projected_rows = await _relation(session, target, projected_query)
        complete, complete_rows = await _relation(session, target, complete_query)
        assert projected_rows == complete_rows
        assert projected.query_fingerprint != complete.query_fingerprint
        family_sets = await _page(session, target, complete_query)
        assert set(family_sets) == {_A}
        assert family_sets[_A].projection == "full_family"
        assert [(group, len(family.children)) for group, family in family_sets[_A].families] == [
            ("segment_a", 2),
            ("segment_b", 1),
        ]
        older = fixture.query(family_entitlement="full_family", context_filters=(ReadFilter("period", "eq", 2023),))
        old_family_set = (await _page(session, target, older))[_A]
        assert [
            field.value
            for _, family in old_family_set.families
            for child in family.children
            for field in child.fields
            if field.field_id == "service_code"
        ] == ["older"]
        empty = replace(older, context_filters=(ReadFilter("period", "eq", 2025),))
        empty_family_set = (await _page(session, target, empty))[_C]
        assert empty_family_set.projection == "full_family"
        assert empty_family_set.families[0][1].children == ()


@pytest.mark.asyncio
async def test_full_page_keeps_each_provider_bound_and_preflights_before_hydration(monkeypatch):
    async with _case(children=_page_children()) as (case, target), case.sessions() as session:
        # Both providers fit separately, even though the page has four children.
        monkeypatch.setattr(read_core, "MAX_DETAIL_CHILDREN", 3)
        page = await _page(session, target, fixture.query(family_entitlement="full_family"))
        assert sum(len(family.children) for entity in page.values() for _, family in entity.families) == 4

        async def forbidden_hydration(*args):
            pytest.fail("every provider must be preflighted before family hydration")

        monkeypatch.setattr(read_core, "MAX_DETAIL_CHILDREN", 2)
        monkeypatch.setattr(read_core, "_hydrate_selected_families", forbidden_hydration)
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="bounded detail child limit"):
            await _page(session, target, fixture.query(family_entitlement="full_family"))


@pytest.mark.asyncio
async def test_full_page_rejects_projection_reuse_and_incomplete_membership(monkeypatch):
    async with _case(children=_page_children()) as (case, target), case.sessions() as session:
        prepared, _rows = await _relation(session, target, fixture.query())
        full_query = fixture.query(family_entitlement="full_family")
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="query identity"):
            await _service(target).hydrate_npi_page(
                session,
                authorization=_AUTHORIZATION,
                pinned_target=target,
                query=full_query,
                prepared=prepared,
                entity_values=(_A,),
            )

        async def missing_membership(*args):
            return ()

        monkeypatch.setattr(read_core, "_family_child_rows", missing_membership)
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="membership is incomplete"):
            await _page(session, target, full_query)


def _page_request(target, path, *, full_family):
    request_map = {
        "target": transport._target_document(_external_target(target)),
        "native_query": {},
        "context": [{"field_id": "segment", "operator": "eq", "value": "segment_a"}],
        "filters": [],
        "order": [{"field_id": "score", "direction": "desc"}],
        "require_match": False,
        "grouped_entity_selection": fixture.selection_document(),
    }
    if full_family:
        request_map["family_entitlement"] = "full_family"
    body = transport._canonical_json_bytes(request_map)
    return http_fixture._Request(
        body,
        http_fixture._resigned_provider_headers(body=body, path=path, target=request_map["target"]),
        path=path,
    )


async def _native_page_reply(session, import_context, *, is_geo):
    identities = [(_A, 1), (_B, 1), (_C, 1)] + ([(_A, 2)] if is_geo else [])
    native_selects = [
        select(literal(npi).label("npi"), literal(f"00000000-0000-4000-8000-{ordinal:012d}").label("address_key"))
        for npi, ordinal in identities
    ]
    native = native_selects[0].union_all(*native_selects[1:]).subquery()
    imported = import_context.prepared.statement.subquery()
    joined = native.outerjoin(imported, native.c.npi == imported.c.entity_value)
    total = await session.scalar(select(func.count()).select_from(joined))
    statement = (
        select(native.c.npi, native.c.address_key)
        .select_from(joined)
        .order_by(
            imported.c.sort_0.desc().nullslast(),
            native.c.npi,
            native.c.address_key,
        )
    )
    provider_rows = [dict(native_row) for native_row in (await session.execute(statement)).mappings()]
    if not is_geo:
        return response.json({"rows": provider_rows, "total": total})
    return response.json(
        {
            "items": provider_rows,
            "total_count": total,
            "next_cursor": None,
            "has_more": False,
            "result_identity": ["npi", "address_key"],
            "_custom_import_next_anchor": None,
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_geo", [False, True])
async def test_signed_full_pages_preserve_native_order_counts_absence_and_geo_repeats(monkeypatch, is_geo):
    http_fixture._install_keyring(monkeypatch)
    module = geo_http if is_geo else provider_http
    path = geo_http.CUSTOM_IMPORT_PROVIDER_GEO_PATH if is_geo else provider_http.CUSTOM_IMPORT_PROVIDERS_PATH
    endpoint = geo_http.serve_custom_import_provider_geo if is_geo else provider_http.serve_custom_import_providers
    async with _case(children=_page_children()) as (case, target):
        seen_statements = []

        async def native_page(request, *, native_args, import_context, prepare_cursor=None):
            seen_statements.append(str(import_context.prepared.statement))
            if prepare_cursor is not None:
                await prepare_cursor(session=session, coordinates=[40, -73])
            return await _native_page_reply(session, import_context, is_geo=is_geo)

        monkeypatch.setattr(module, "_geo_page" if is_geo else "_provider_page", native_page)
        # Unique selected families contain four children; repeated addresses must not multiply the preflight.
        monkeypatch.setattr(read_core, "MAX_DETAIL_CHILDREN", 4)
        replies = []
        for full_family in (False, True):
            async with case.sessions() as session:
                reply = await endpoint(_page_request(target, path, full_family=full_family), session)
            assert reply.status == 200, reply.body
            replies.append(json.loads(reply.body))
        _assert_complete_page_parity(replies, is_geo=is_geo)
        assert seen_statements[0] == seen_statements[1]


def _assert_complete_page_parity(replies, *, is_geo):
    page_key, count_key = ("items", "total_count") if is_geo else ("rows", "total")
    projected, complete = replies
    assert projected[count_key] == complete[count_key] == (4 if is_geo else 3)
    assert [row["npi"] for row in projected[page_key]] == [row["npi"] for row in complete[page_key]]
    assert complete[page_key][-1]["npi"] == _C and complete[page_key][-1]["custom_import"] is None
    for row in complete[page_key][:-1]:
        family_set = row["custom_import"]
        assert family_set["projection"] == "full_family"
        assert all(set(family) == {"group_value", "root_fields", "children"} for family in family_set["families"])
    if is_geo:
        repeated_families = [row["custom_import"] for row in complete[page_key] if row["npi"] == _A]
        assert len(repeated_families) == 2 and repeated_families[0] == repeated_families[1]


@pytest.mark.asyncio
async def test_signed_page_entitlement_cannot_be_added_after_signing(monkeypatch):
    http_fixture._install_keyring(monkeypatch)
    async with _case() as (case, target), case.sessions() as session:
        path = provider_http.CUSTOM_IMPORT_PROVIDERS_PATH
        projected = _page_request(target, path, full_family=False)
        complete = _page_request(target, path, full_family=True)
        request = http_fixture._Request(complete.body, projected.headers, path=path)
        reply = await provider_http.serve_custom_import_providers(request, session)
        assert reply.status == 404
        assert not session.in_transaction()
