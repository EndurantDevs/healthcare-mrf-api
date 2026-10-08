# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proofs for custom-import provider-service composition."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import date
from decimal import Decimal
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.request import RequestParameters
from sqlalchemy import MetaData, exists, func, literal, select, text, union_all
from sqlalchemy.ext.asyncio import create_async_engine

from api import custom_import_plan_pricing as imported
from api import custom_import_plan_sql as plan_sql
from api import custom_import_provider_service_http as service_http
from api import custom_import_read_http as transport
from api import provider_list_sql
from api import ptg2_code_scope as code_scope
from api import ptg2_serving as serving
from api.custom_import_provider_service_sql import (
    ProviderServiceImportQuery,
    build_provider_service_claims_statements,
)
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from api.endpoint import npi as npi_module
from api.endpoint import pricing
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from db.models.custom_import import (
    CustomImportChildScalar,
    CustomImportDataset,
    CustomImportEntityBinding,
    CustomImportWinner,
)
from process.custom_import.read_core import (
    CustomImportReadRequestError,
    ExtensionReadAuthorization,
    NpiEntityRelationQuery,
    PreparedNpiEntityRelation,
    ReadFilter,
    ReadOrderTerm,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests import custom_import_postgres_support as postgres_support
from tests import test_custom_import_provider_geo_sql as geo_fixture
from tests import test_custom_import_provider_list as list_fixture
from tests import test_custom_import_read_core_postgres as read_fixture
from tests import test_custom_import_read_http as http_fixture
from tests.custom_import_postgres_support import _database_url, digest, isolated_publication_case, transaction_session
from tests.test_custom_import_plan_pricing import (
    _import_context,
    _install_code_lookup_scope,
    _install_location_fixture,
    _native_fixture,
    _plan_session,
    _rate,
    _selection,
    _typed_import_context,
)

_NPI = "1000000001"
_ABSENT_NPI = "1000000000"


def _seed_npi_bindings(monkeypatch) -> None:
    original_seed = postgres_support._seed_or_reuse_entity_binding

    async def seed_npi_binding(session, graph, material_spec, suffix):
        if material_spec.entity_binding_id is not None:
            return await original_seed(session, graph, material_spec, suffix)
        binding = CustomImportEntityBinding(
            dataset_id=graph.dataset_id,
            adapter_id="npi",
            canonical_value=_NPI,
            value_sha256=digest(f"provider-query-npi-binding:{suffix}"),
        )
        session.add(binding)
        await session.flush()
        return binding

    monkeypatch.setattr(postgres_support, "_seed_or_reuse_entity_binding", seed_npi_binding)


async def _seed_npi_fixture(session):
    return await read_fixture._seed_read_fixture(session)


def _seed_selected_contexts(monkeypatch, child_indexes: tuple[int, ...]) -> None:
    async def add_selected_winners(session, graph, attempt, family) -> None:
        session.add_all(
            CustomImportWinner(
                generation_id=attempt.generation_id,
                dataset_id=graph.dataset_id,
                definition_revision_id=graph.definition_revision_id,
                schema_revision_id=graph.schema_revision_id,
                profile_slot=1,
                entity_binding_id=family.entity_binding_id,
                family_revision_id=family.family_revision_id,
                context_collection_slot=1,
                context_key_sha256=digest(f"provider-query-context:{child_index}"),
                context_child_revision_id=family.child_revision_ids[child_index],
            )
            for child_index in child_indexes
        )
        await session.flush()

    monkeypatch.setattr(read_fixture, "_add_selected_winners", add_selected_winners)


def _seed_selected_metric(monkeypatch, child_index: int, amount: Decimal) -> None:
    """Add one selected-child metric before immutable generation sealing."""

    original_add_selected_scalars = read_fixture._add_selected_scalars

    async def add_selected_scalars(session, graph, family) -> None:
        await original_add_selected_scalars(session, graph, family)
        session.add(
            CustomImportChildScalar(
                child_revision_id=family.child_revision_ids[child_index],
                dataset_id=graph.dataset_id,
                schema_revision_id=graph.schema_revision_id,
                root_record_id=family.root_record_id,
                collection_slot=1,
                field_slot=6,
                field_collection_slot=1,
                projection_slot=4,
                field_type="decimal",
                value_state="value",
                decimal_value=amount,
            )
        )
        await session.flush()

    monkeypatch.setattr(read_fixture, "_add_selected_scalars", add_selected_scalars)


def _native_provider_relation():
    return (
        select(literal(_NPI).label("npi")).union_all(select(literal(_ABSENT_NPI).label("npi"))).cte("native_provider")
    )


def _native_service_grouped_relation(native):
    """Give each synthetic provider the scalar columns used by claims ordering."""

    return select(
        native.c.npi,
        literal("Provider").label("provider_name"),
        literal(5).label("total_services"),
        literal(30).label("total_submitted_charges"),
        literal(20).label("total_allowed_amount"),
        literal(2).label("total_beneficiaries"),
        literal(1).label("matched_service_codes"),
    ).select_from(native)


async def _create_pricing_claim_tables(case) -> None:
    metadata = MetaData()
    pricing.provider_table.to_metadata(metadata)
    pricing.provider_procedure_table.to_metadata(metadata)
    async with case.engine.begin() as connection:
        await connection.run_sync(metadata.create_all)


async def _seed_pricing_claim_rows(session) -> None:
    await session.execute(
        pricing.provider_table.insert(),
        (
            {
                "provider_key": 1,
                "npi": int(_NPI),
                "year": 2024,
                "provider_name": "Synthetic Imported Provider",
            },
            {
                "provider_key": 2,
                "npi": int(_ABSENT_NPI),
                "year": 2024,
                "provider_name": "Synthetic Unmatched Provider",
            },
        ),
    )
    await session.execute(
        pricing.provider_procedure_table.insert(),
        (
            {
                "npi": int(_NPI),
                "year": 2024,
                "procedure_code": 99213,
                "total_services": 5.0,
                "total_submitted_charges": 25.0,
                "total_allowed_amount": 20.0,
                "total_beneficiaries": 2.0,
            },
            {
                "npi": int(_ABSENT_NPI),
                "year": 2024,
                "procedure_code": 99213,
                "total_services": 50.0,
                "total_submitted_charges": 250.0,
                "total_allowed_amount": 200.0,
                "total_beneficiaries": 20.0,
            },
        ),
    )


async def _assert_native_exists_composition(session, prepared_relation) -> None:
    relation = prepared_relation.statement.subquery("imported_relation")
    native = _native_provider_relation()
    matches_import = exists(select(1).select_from(relation).where(relation.c.entity_value == native.c.npi))

    relation_rows = (await session.execute(prepared_relation.statement)).all()
    total = await session.scalar(select(func.count()).select_from(native).where(matches_import))
    page = (
        (await session.execute(select(native.c.npi).where(matches_import).order_by(native.c.npi).offset(0).limit(1)))
        .scalars()
        .all()
    )

    assert len(relation_rows) == 1
    assert total == 1
    assert page == [_NPI]
    assert "LIMIT" not in str(prepared_relation.statement)


async def _assert_native_list_count_page(session, prepared_relation) -> None:
    compiled = compile_npi_entity_relation(prepared_relation.statement)
    schema_name = session.get_bind().get_execution_options()["schema_translate_map"]["mrf"]
    relation_sql = compiled.sql.replace("mrf.", f"{postgres_support._quoted_publication_schema(schema_name)}.")
    import_context = ProviderImportQuery(prepared_relation, compiled, True)
    native_sql = "SELECT CAST(:native_npi AS bigint) AS npi UNION ALL SELECT CAST(:absent_npi AS bigint)"
    count_ctes, page_sql = provider_list_sql._provider_import_count_page(native_sql, import_context)
    statement = text(
        f"WITH custom_import_provider_relation AS ({relation_sql}), {count_ctes}, "
        f"provider_page AS ({page_sql}) "
        "SELECT provider_totals._provider_total, provider_page.npi "
        "FROM provider_totals LEFT JOIN provider_page ON TRUE"
    ).bindparams(*compiled.typed_binds)
    for offset, expected_npi in ((0, int(_NPI)), (1, None)):
        parameters_by_name = {
            **compiled.values,
            "native_npi": int(_NPI),
            "absent_npi": int(_ABSENT_NPI),
            "limit": 1,
            "start": offset,
        }
        page_rows = (await session.execute(statement, parameters_by_name)).all()
        assert page_rows == [(1, expected_npi)]


async def _relation_rows(session, read_service, authorization, pinned_target, filters):
    prepared_relation = await read_service.prepare_npi_entity_relation(
        session,
        authorization=authorization,
        target=pinned_target,
        query=NpiEntityRelationQuery(filters=filters),
    )
    return (await session.execute(prepared_relation.statement)).all()


@pytest.mark.asyncio
async def test_provider_v2_preserves_context_winner(monkeypatch):
    """A metric may qualify only the winner selected by the v2 context selector."""

    _seed_npi_bindings(monkeypatch)
    _seed_selected_contexts(monkeypatch, (0, 1))
    _seed_selected_metric(monkeypatch, 0, Decimal("10"))
    async with transaction_session() as session, session.begin():
        fixture = await _seed_npi_fixture(session)
        service = read_fixture._service()
        authorization = ExtensionReadAuthorization("synthetic-provider-query")
        metric_filter = (ReadFilter("amount", "gt", "5"),)

        high_prepared = await service.prepare_npi_entity_relation(
            session,
            authorization=authorization,
            target=fixture.target,
            query=NpiEntityRelationQuery(
                context_filters=(ReadFilter("synthetic_alpha", "eq", "high-1"),),
                filters=metric_filter,
            ),
        )
        low_relation_query = NpiEntityRelationQuery(
            context_filters=(ReadFilter("synthetic_alpha", "eq", "low-1"),),
            filters=metric_filter,
        )
        low_prepared = await service.prepare_npi_entity_relation(
            session,
            authorization=authorization,
            target=fixture.target,
            query=low_relation_query,
        )

        assert {relation_row.entity_value for relation_row in await session.execute(high_prepared.statement)} == {_NPI}
        assert (await session.execute(low_prepared.statement)).all() == []
        assert (
            await service.hydrate_npi_page(
                session,
                authorization=authorization,
                pinned_target=fixture.target,
                prepared=low_prepared,
                entity_values=(_NPI,),
                query=low_relation_query,
            )
            == {}
        )


async def _assert_filter_semantics(session, read_service, authorization, pinned_target) -> None:
    mixed_context_rows = await _relation_rows(
        session,
        read_service,
        authorization,
        pinned_target,
        (ReadFilter("synthetic_alpha", "eq", "high-1"), ReadFilter("synthetic_beta", "eq", "low-2")),
    )
    root_child_rows = await _relation_rows(
        session,
        read_service,
        authorization,
        pinned_target,
        (ReadFilter("npi", "eq", "synthetic-root"), ReadFilter("synthetic_alpha", "eq", "low-1")),
    )
    null_amount_rows = await _relation_rows(
        session,
        read_service,
        authorization,
        pinned_target,
        (ReadFilter("amount", "is_null"),),
    )
    missing_amount_rows = await _relation_rows(
        session,
        read_service,
        authorization,
        pinned_target,
        (ReadFilter("amount", "is_missing"),),
    )

    assert mixed_context_rows == []
    assert {relation_row.entity_value for relation_row in root_child_rows} == {_NPI}
    assert len(null_amount_rows) == 1
    assert {relation_row.entity_value for relation_row in missing_amount_rows} == {_NPI}
    with pytest.raises(CustomImportReadRequestError, match="not a decimal"):
        await _relation_rows(
            session,
            read_service,
            authorization,
            pinned_target,
            (ReadFilter("amount", "eq", "not-a-decimal"),),
        )


@pytest.mark.asyncio
async def test_provider_query_filter_relation_preserves_contexts_while_host_exists_deduplicates_npis(monkeypatch):
    """Two winner contexts yield one imported NPI and one counted native page."""

    _seed_npi_bindings(monkeypatch)
    _seed_selected_contexts(monkeypatch, (0, 1))
    async with transaction_session() as session, session.begin():
        fixture = await _seed_npi_fixture(session)
        assert (
            await session.scalar(
                select(func.count())
                .select_from(CustomImportWinner)
                .where(
                    CustomImportWinner.generation_id == fixture.target.generation_id,
                    CustomImportWinner.entity_binding_id == fixture.selected_family.entity_binding_id,
                    CustomImportWinner.profile_slot == 1,
                )
            )
            == 2
        )
        service = read_fixture._service()
        authorization = ExtensionReadAuthorization("synthetic-provider-query")
        prepared = await service.prepare_npi_entity_relation(
            session,
            authorization=authorization,
            target=fixture.target,
            query=NpiEntityRelationQuery(filters=(ReadFilter("npi", "eq", "synthetic-root"),)),
        )
        await _assert_native_exists_composition(session, prepared)
        await _assert_native_list_count_page(session, prepared)
        await _assert_filter_semantics(session, service, authorization, fixture.target)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("child_index", "direction", "expected_amount_state"),
    ((0, "asc", "missing"), (0, "desc", "missing"), (1, "asc", "null"), (1, "desc", "null")),
)
async def test_provider_query_order_only_left_join_keeps_absent_native_npis_last(
    monkeypatch,
    child_index,
    direction,
    expected_amount_state,
):
    """Matched null and missing values still sort before absent native providers."""

    definition_document = read_fixture._definition_document()
    definition_document["selection_profiles"][0]["context_dimensions"] = []
    definition_document["query"]["sortable_fields"] = ["amount"]
    profile_document = read_fixture._profile_document()
    profile_document["context_dimensions"] = []
    monkeypatch.setattr(read_fixture, "_definition_document", lambda: deepcopy(definition_document))
    monkeypatch.setattr(read_fixture, "_profile_document", lambda: deepcopy(profile_document))
    _seed_npi_bindings(monkeypatch)
    _seed_selected_contexts(monkeypatch, (child_index,))

    async with transaction_session() as session, session.begin():
        fixture = await _seed_npi_fixture(session)
        prepared = await read_fixture._service().prepare_npi_entity_relation(
            session,
            authorization=ExtensionReadAuthorization("synthetic-provider-query"),
            target=fixture.target,
            query=NpiEntityRelationQuery(order_terms=(ReadOrderTerm("amount", direction, "last"),)),
        )
        native = _native_provider_relation()
        claims_statements = build_provider_service_claims_statements(
            _native_service_grouped_relation(native),
            ProviderServiceImportQuery(prepared, require_match=False),
            5,
            10,
            "npi",
            "asc",
        )
        scalar = await session.scalar(
            select(CustomImportChildScalar).where(
                CustomImportChildScalar.child_revision_id == fixture.selected_family.child_revision_ids[child_index],
                CustomImportChildScalar.field_slot == 6,
            )
        )
        first_page = (await session.execute(claims_statements.page_statement.offset(0).limit(1))).all()
        second_page = (await session.execute(claims_statements.page_statement.offset(1).limit(1))).all()

        assert prepared.normalized_order_terms == (ReadOrderTerm("amount", direction, "last"),)
        if expected_amount_state == "missing":
            assert scalar is None
        else:
            assert scalar is not None
            assert scalar.value_state == "null"
        assert await session.scalar(claims_statements.count_statement) == 2
        assert [provider_row.npi for provider_row in first_page + second_page] == [_NPI, _ABSENT_NPI]


@pytest.mark.asyncio
async def test_signed_provider_service_http_joins_imported_npi_to_real_claim_rows(monkeypatch):
    """A signed extension read reaches the native claims handler and hydrates its match."""

    _seed_npi_bindings(monkeypatch)
    http_fixture._install_keyring(monkeypatch)
    async with isolated_publication_case() as case:
        await _create_pricing_claim_tables(case)
        async with case.sessions() as seed_session, seed_session.begin():
            fixture = await _seed_npi_fixture(seed_session)
            dataset_key = await seed_session.scalar(
                select(CustomImportDataset.dataset_key).where(
                    CustomImportDataset.dataset_id == fixture.target.dataset_id
                )
            )
            assert dataset_key is not None
            await _seed_pricing_claim_rows(seed_session)
        monkeypatch.setattr(pricing, "PRICING_SCHEMA", case.schema_name)
        target_map = {
            "dataset_key": dataset_key,
            "generation_id": fixture.target.generation_id,
            "definition_revision_id": fixture.target.definition_revision_id,
            "schema_revision_id": fixture.target.schema_revision_id,
            "profile_id": fixture.target.profile_id,
        }
        body = transport._canonical_json_bytes(
            {
                "target": target_map,
                "native_query": {
                    "code": "99213",
                    "code_system": "HP_PROCEDURE_CODE",
                    "year": "2024",
                    "limit": "2",
                },
                "context": [{"field_id": "synthetic_alpha", "operator": "eq", "value": "low-1"}],
                "filters": [],
                "order": None,
                "require_match": True,
            }
        )
        async with case.sessions() as session:
            request = SimpleNamespace(
                args=RequestParameters({}),
                body=body,
                headers=http_fixture._resigned_provider_headers(
                    body=body,
                    path=service_http.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH,
                    target=target_map,
                ),
                method="POST",
                path=service_http.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH,
                query_string="",
                ctx=SimpleNamespace(sa_session=session),
            )
            reply = await service_http.serve_custom_import_provider_service(request, session)
    reply_payload = json.loads(reply.body)
    assert reply.status == 200
    assert reply_payload["pagination"]["total"] == 1
    assert [str(provider_item["npi"]) for provider_item in reply_payload["items"]] == [_NPI]
    assert reply_payload["items"][0]["provider_name"] == "Synthetic Imported Provider"
    assert reply_payload["items"][0]["custom_import"]["target"] == target_map


@pytest.mark.asyncio
@pytest.mark.parametrize("system,code,expected_keys", [("CPT", "99213", [1]), ("RC", "360", [2, 3])])
async def test_postgres_code_lookup_binds_canonical_and_compatible_values(monkeypatch, system, code, expected_keys):
    """Exact and compatible codes retain their bound SQL order on native PostgreSQL."""

    _install_code_lookup_scope(monkeypatch)
    async with transaction_session() as session, session.begin():
        await session.execute(
            text("""
            CREATE TEMP TABLE sealed_code_fixture (
                code_key bigint, snapshot_key bigint, reported_code_system text, reported_code text,
                negotiation_arrangement text, billing_code_type_version text,
                source_name text, source_description text, rate_count integer
            ) ON COMMIT DROP
        """)
        )
        await session.execute(
            text("""
            INSERT INTO sealed_code_fixture(code_key, snapshot_key, reported_code_system, reported_code)
            VALUES (1, 1, 'CPT', '99213'), (2, 1, 'RC', '0360'), (3, 1, 'RC', '360')
        """)
        )
        code_records = await code_scope.load_sealed_code_rows(
            session,
            SimpleNamespace(shared_snapshot_key=1, uses_shared_blocks=True),
            {"code_system": system, "code": code},
        )
        assert [code_record["code_key"] for code_record in code_records] == expected_keys


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "offset,expected_npis", [(0, [1000000002, 1000000002]), (2, [1000000003, 1000000003]), (10, [])]
)
async def test_nonprojected_plan_keeps_native_groups_total_and_global_score_page(monkeypatch, offset, expected_npis):
    forward_read, prices, providers = _native_fixture(monkeypatch)
    selection = _selection()
    assert selection.pricing_projection_id is None
    pagination = SimpleNamespace(offset=offset, limit=2, page=offset // 2 + 1)
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT", include_providers="true"),
            pagination,
            selection,
            _import_context(),
        )
    assert response["pagination"]["total"] == 6
    assert response["pagination"]["total_is_exact"] is True
    assert [entry["npi"] for entry in response["items"]] == expected_npis
    assert len(set(response[imported.NATIVE_ENTRY_IDS])) == len(expected_npis)
    assert forward_read.await_args.kwargs["limit"] is None
    assert forward_read.await_args.kwargs["offset"] == 0
    assert forward_read.await_args.kwargs["provider_set_keys"] == (7,)
    assert forward_read.await_count == 1
    if expected_npis:
        assert providers.await_args.kwargs["npis"] == (expected_npis[0],)
        assert prices.await_count == 1
        assert response["items"][0]["provider_sex_code"] == "F"
        assert len(response["items"][0]["prices"]) == 2
    else:
        providers.assert_not_awaited()
        prices.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_price_filter_changes_complete_entry_total_before_score_page(monkeypatch):
    _native_fixture(monkeypatch)
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT", rate="13"),
            SimpleNamespace(offset=0, limit=2, page=1),
            _selection(),
            _import_context(),
        )
    assert response["pagination"]["total"] == 3
    assert [entry["npi"] for entry in response["items"]] == [1000000002, 1000000003]
    assert all(len(entry["prices"]) == 1 for entry in response["items"])


@pytest.mark.asyncio
async def test_membership_only_preserves_native_cost_order_and_multibinding_entries(monkeypatch):
    _native_fixture(monkeypatch)
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT", order_by="cost", order="asc"),
            SimpleNamespace(offset=0, limit=8, page=1),
            _selection(2),
            _import_context(ordered=False),
        )
    assert response["pagination"]["total"] == 12
    assert len(response["items"]) == 8
    assert len(set(response[imported.NATIVE_ENTRY_IDS])) == 8
    assert [entry["npi"] for entry in response["items"][:4]] == [1000000001, 1000000001, 1000000002, 1000000002]


@pytest.mark.asyncio
@pytest.mark.parametrize("matching_snapshot", [None, "snapshot-1"])
async def test_plan_stages_scores_only_for_nonempty_code_bindings(monkeypatch, matching_snapshot):
    forward_read, prices, providers = _native_fixture(monkeypatch)

    async def code_rows_for_binding(_session, _tables, native_args):
        return [dict(code_key=1)] if native_args["snapshot_id"] == matching_snapshot else []

    monkeypatch.setattr(imported, "load_sealed_code_rows", code_rows_for_binding)
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT"),
            SimpleNamespace(offset=0, limit=10, page=1),
            _selection(2),
            _import_context(),
        )
        imported_count = await session.scalar(text(f"SELECT COUNT(*) FROM pg_temp.{plan_sql.IMPORTED}"))
        binding_ordinals = list(
            (await session.scalars(text(f"SELECT DISTINCT binding_ordinal FROM pg_temp.{plan_sql.CANDIDATES}"))).all()
        )
    assert imported_count == (3 if matching_snapshot else 0)
    assert binding_ordinals == ([1] if matching_snapshot else [])
    assert response["pagination"]["total"] == (6 if matching_snapshot else 0)
    if matching_snapshot:
        assert len(response["items"]) == 6
        assert all(entry["network"] == "source-1" for entry in response["items"])
        assert forward_read.await_count == 1
    else:
        assert response["items"] == []
        forward_read.assert_not_awaited()
        prices.assert_not_awaited()
        providers.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_value,second_value,first_npi,nulls",
    [
        ("z", "a", 1000000002, "last"),
        (date(2026, 2, 1), date(2026, 1, 1), 1000000002, "last"),
        (True, False, 1000000002, "last"),
        (2, 1, 1000000002, "last"),
        (Decimal("2.000000000001"), Decimal("2.000000000000"), 1000000002, "last"),
        (None, 1, 1000000001, "first"),
    ],
)
async def test_sql_plan_order_keeps_typed_and_null_values(monkeypatch, first_value, second_value, first_npi, nulls):
    _native_fixture(monkeypatch)
    query = _typed_import_context(first_value, second_value, nulls=nulls)
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT"),
            SimpleNamespace(offset=0, limit=10, page=1),
            _selection(),
            query,
        )
    assert response["pagination"]["total"] == 6
    assert [entry["npi"] for entry in response["items"][:2]] == [first_npi] * 2
    assert [entry["npi"] for entry in response["items"][-2:]] == [1000000003] * 2


@pytest.mark.asyncio
async def test_filter_only_multiple_contexts_count_each_native_entry_once(monkeypatch):
    _native_fixture(monkeypatch)
    source_relation = union_all(
        select(literal("1000000001").label("entity_value")),
        select(literal("1000000001").label("entity_value")),
    ).subquery()
    prepared = PreparedNpiEntityRelation(select(source_relation.c.entity_value), (), "a" * 64, "b" * 64)
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT"),
            SimpleNamespace(offset=0, limit=10, page=1),
            _selection(),
            ProviderServiceImportQuery(prepared, True),
        )
    assert response["pagination"]["total"] == 2
    assert [entry["npi"] for entry in response["items"]] == [1000000001] * 2


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["membership", "occurrences", "prices"])
async def test_sql_plan_incomplete_native_data_fails_closed(monkeypatch, missing):
    _native_fixture(monkeypatch)
    if missing == "membership":
        monkeypatch.setattr(serving, "_provider_set_keys_for_ids", AsyncMock(return_value={}))
    elif missing == "occurrences":
        monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", AsyncMock(return_value=None))
    else:
        monkeypatch.setattr(serving, "_version_three_prices_by_key", AsyncMock(return_value={}))
    async with _plan_session() as session:
        with pytest.raises(PTG2ManifestArtifactError):
            await imported.search_imported_plan_providers(
                session,
                dict(code="27447", code_system="CPT", rate="13"),
                SimpleNamespace(offset=0, limit=10, page=1),
                _selection(),
                _import_context(),
            )


@pytest.mark.asyncio
async def test_sql_plan_reads_each_physical_provider_shard_once(monkeypatch):
    forward_read, _prices, _providers = _native_fixture(monkeypatch)
    provider_keys = (1000, 1023, 1024, 2049)
    keys_by_id = {f"{provider_key:032x}": provider_key for provider_key in provider_keys}
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", AsyncMock(return_value=keys_by_id))
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_selected_npis",
        AsyncMock(side_effect=lambda _session, _tables, npis: {npi: tuple(keys_by_id) for npi in npis}),
    )
    forward_read.side_effect = lambda *_args, **kwargs: [
        _rate(provider_key, provider_key, provider_key) for provider_key in kwargs["provider_set_keys"]
    ]
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT"),
            SimpleNamespace(offset=0, limit=20, page=1),
            _selection(),
            _import_context(),
        )
    assert response["pagination"]["total"] == 12
    assert len(response["items"]) == 12
    assert [native_call.kwargs["provider_set_keys"] for native_call in forward_read.await_args_list] == [
        (1000, 1023),
        (1024,),
        (2049,),
    ]
    assert all(native_call.kwargs["limit"] is None for native_call in forward_read.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("driver", ["asyncpg", "psycopg"])
async def test_plan_copy_preserves_decimal_prices_and_driver(monkeypatch, driver):
    if driver == "psycopg":
        pytest.importorskip("psycopg")
    _native_fixture(monkeypatch)

    async def prices_for_keys(_session, _tables, price_keys):
        return {
            price_key: [dict(negotiated_rate=Decimal(price_key) + Decimal("0.000000000001"))]
            for price_key in price_keys
        }

    monkeypatch.setattr(serving, "_version_three_prices_by_key", AsyncMock(side_effect=prices_for_keys))
    async with _plan_session(driver) as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT", rate="13.000000000001", rate_tolerance="0"),
            SimpleNamespace(offset=0, limit=2, page=1),
            _selection(),
            _import_context(),
        )
        minimum_rate = await session.scalar(
            text(f"SELECT minimum_rate FROM pg_temp.{plan_sql.PRICES} WHERE price_key=13")
        )
        assert minimum_rate == Decimal("13.000000000001")
    assert response["pagination"]["total"] == 3
    assert [entry["npi"] for entry in response["items"]] == [1000000002, 1000000003]


@pytest.mark.asyncio
@pytest.mark.parametrize("is_complete", [False, True])
async def test_plan_distance_page_keeps_exact_location_witness(monkeypatch, is_complete):
    _native_fixture(monkeypatch)
    location_query = _install_location_fixture(monkeypatch, is_complete)
    async with _plan_session() as session:
        if not is_complete:
            with pytest.raises(PTG2ManifestArtifactError, match="address witness"):
                await imported.search_imported_plan_providers(
                    session,
                    dict(
                        code="27447",
                        code_system="CPT",
                        zip5="12345",
                        order_by="distance",
                        include_unverified_addresses="true",
                    ),
                    SimpleNamespace(offset=0, limit=4, page=1),
                    _selection(),
                    _import_context(ordered=False),
                )
        else:
            response = await imported.search_imported_plan_providers(
                session,
                dict(
                    code="27447",
                    code_system="CPT",
                    zip5="12345",
                    order_by="distance",
                    include_unverified_addresses="true",
                ),
                SimpleNamespace(offset=0, limit=4, page=1),
                _selection(),
                _import_context(ordered=False),
            )
            assert response["pagination"]["total"] == 4
            assert [entry["npi"] for entry in response["items"]] == [1000000002] * 2 + [1000000001] * 2
            assert [entry["distance_miles"] for entry in response["items"]] == [0.5] * 2 + [1.5] * 2
    assert location_query.await_args.kwargs["limit"] == location_query.await_args.kwargs["offset"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("pos,modifier,expected_total", [("11", "GT", 6), ("22", "95", 3), ("11", '["GT","95"]', 3)])
async def test_plan_price_filters_keep_only_complete_eligible_options(monkeypatch, pos, modifier, expected_total):
    _native_fixture(monkeypatch)
    prices_by_key = {
        11: [
            dict(negotiated_rate=Decimal("12"), service_code=["11"], billing_code_modifier=["GT"]),
            dict(negotiated_rate=Decimal("14"), service_code=["22"], billing_code_modifier=["95"]),
        ],
        12: [dict(negotiated_rate=Decimal("12"), service_code=["11"], billing_code_modifier=["GT", "95"])],
        13: [dict(negotiated_rate=Decimal("12"), service_code=["11"], billing_code_modifier=["GT"])],
    }
    monkeypatch.setattr(
        serving,
        "_version_three_prices_by_key",
        AsyncMock(
            side_effect=lambda _session, _tables, price_keys: {
                price_key: prices_by_key[price_key] for price_key in price_keys
            }
        ),
    )
    args_by_name = dict(code="27447", code_system="CPT", pos=pos, modifier=modifier)
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            args_by_name,
            SimpleNamespace(offset=0, limit=10, page=1),
            _selection(),
            _import_context(),
        )
    assert response["pagination"]["total"] == expected_total
    assert len(response["items"]) == expected_total
    assert all(len(entry["prices"]) == 1 for entry in response["items"])
    assert all(serving._is_price_filter_match(entry["prices"][0], args_by_name) for entry in response["items"])


@pytest.mark.asyncio
async def test_plan_rate_options_keep_native_reverse_membership_order(monkeypatch):
    forward_read, _prices, _providers = _native_fixture(monkeypatch)
    forward_read.return_value = [_rate(1, 11, 7), _rate(1, 12, 8)]
    ids_by_key = {provider_key: f"{provider_key:032x}" for provider_key in (7, 8)}
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={provider_id: provider_key for provider_key, provider_id in ids_by_key.items()}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_selected_npis",
        AsyncMock(side_effect=lambda _session, _tables, npis: {npi: (ids_by_key[8], ids_by_key[7]) for npi in npis}),
    )
    async with _plan_session() as session:
        response = await imported.search_imported_plan_providers(
            session,
            dict(code="27447", code_system="CPT"),
            SimpleNamespace(offset=0, limit=10, page=1),
            _selection(),
            _import_context(),
        )
    assert response["pagination"]["total"] == 3
    assert all(
        [rate_option["provider_set_ref"] for rate_option in entry["rate_options"]] == [ids_by_key[8], ids_by_key[7]]
        for entry in response["items"]
    )


async def _dispatched_plan_page(session, **argument_overrides):
    return await serving.search_current_ptg2_index(
        session,
        {"code": "27447", "code_system": "CPT", "plan_release_id": "release-1", **argument_overrides},
        SimpleNamespace(offset=0, limit=2, page=1),
        release_selection=_selection(),
        import_context=_import_context(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("npi,expected_total", [("1000000001", 2), ("1000000009", 0)])
async def test_plan_dispatch_keeps_exact_npi_count(monkeypatch, npi, expected_total):
    _forward, _prices, providers = _native_fixture(monkeypatch)
    async with _plan_session() as session:
        response = await _dispatched_plan_page(session, npi=npi)
    assert response["pagination"]["total"] == expected_total
    assert response["pagination"]["total_is_exact"] is True
    assert [entry["npi"] for entry in response["items"]] == [int(npi)] * expected_total
    assert len(set(response[imported.NATIVE_ENTRY_IDS])) == expected_total
    if expected_total:
        assert providers.await_args.kwargs["npis"] == (int(npi),)
        assert sorted(len(entry["prices"]) for entry in response["items"]) == [1, 2]
    else:
        providers.assert_not_awaited()


async def _assert_plan_backend_clean(backend_pid, temp_schema):
    observer_engine = create_async_engine(_database_url())
    try:
        async with observer_engine.connect() as connection:
            assert (
                await connection.scalar(
                    text("SELECT COUNT(*) FROM pg_stat_activity WHERE pid = :backend_pid"),
                    {"backend_pid": backend_pid},
                )
                == 0
            )
            assert (
                await connection.scalar(
                    text("""SELECT COUNT(*) FROM pg_class relation JOIN pg_namespace namespace
                    ON namespace.oid = relation.relnamespace WHERE namespace.nspname = :temp_schema"""),
                    {"temp_schema": temp_schema},
                )
                == 0
            )
    finally:
        await observer_engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate_scope", ["imported", "native"])
async def test_plan_duplicate_identity_rolls_back(monkeypatch, duplicate_scope):
    forward, prices, providers = _native_fixture(monkeypatch)
    query = _typed_import_context(1, 2)
    if duplicate_scope == "imported":
        duplicate_relation = union_all(
            select(literal("1000000001").label("entity_value"), literal(1).label("sort_0")),
            select(literal("1000000001").label("entity_value"), literal(2).label("sort_0")),
        ).subquery()
        query = ProviderServiceImportQuery(
            PreparedNpiEntityRelation(
                select(*duplicate_relation.c), query.prepared.normalized_order_terms, "a" * 64, "b" * 64
            ),
            True,
        )
    with pytest.raises(PTG2ManifestArtifactError, match=f"{duplicate_scope} eligibility relation repeated an NPI"):
        async with _plan_session() as session:
            backend_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            temp_schema = await session.scalar(text("SELECT nspname FROM pg_namespace WHERE oid=pg_my_temp_schema()"))
            if duplicate_scope == "native":
                await session.execute(text("INSERT INTO native_npi_fixture VALUES (1, 1000000001)"))
            await imported.search_imported_plan_providers(
                session,
                {"code": "27447", "code_system": "CPT"},
                SimpleNamespace(offset=0, limit=2, page=1),
                _selection(),
                query,
            )
    await _assert_plan_backend_clean(backend_pid, temp_schema)
    forward.assert_not_awaited()
    prices.assert_not_awaited()
    providers.assert_not_awaited()


def _changed_native_identity(native_merge, failure, provider_rates, associations):
    merged = native_merge(provider_rates, associations)
    if failure == "entry_missing":
        return []
    if failure == "entry_duplicate":
        return merged * 2
    return [{**merged[0], "npi": 1000000009}]


def _break_plan_completion(monkeypatch, failure):
    if failure == "provider_unavailable":
        monkeypatch.setattr(serving, "_enriched_provider_rows_for_npis", AsyncMock(return_value=None))
        return
    if failure == "provider_missing":
        monkeypatch.setattr(serving, "_enriched_provider_rows_for_npis", AsyncMock(return_value=[]))
        return
    if failure == "price_missing":
        monkeypatch.setattr(serving, "_version_three_prices_by_key", AsyncMock(return_value={}))
        return
    if failure == "price_ineligible":
        monkeypatch.setattr(serving, "_ptg2_manifest_filter_prices", lambda *_args: [])
        return
    if failure == "occurrences_missing":
        monkeypatch.setattr(plan_sql, "plan_completion_statement", lambda: text("SELECT 1 WHERE FALSE"))
        return
    monkeypatch.setattr(
        serving,
        "_merge_provider_rates_for_request",
        partial(_changed_native_identity, serving._merge_provider_rates_for_request, failure),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,error_type,reason",
    [
        ("provider_unavailable", PlanPricingProjectionUnavailable, "completion is unavailable"),
        ("provider_missing", PTG2ManifestArtifactError, "selected provider"),
        ("price_missing", PTG2ManifestArtifactError, "incomplete native price"),
        ("price_ineligible", PTG2ManifestArtifactError, "price eligibility"),
        ("occurrences_missing", PTG2ManifestArtifactError, "complete occurrences"),
        ("entry_missing", PTG2ManifestArtifactError, "entry identity"),
        ("entry_duplicate", PTG2ManifestArtifactError, "entry identity"),
        ("source_changed", PTG2ManifestArtifactError, "source identity"),
    ],
)
async def test_plan_dispatch_rejects_incomplete_page(monkeypatch, failure, error_type, reason):
    _native_fixture(monkeypatch)
    _break_plan_completion(monkeypatch, failure)
    with pytest.raises(error_type, match=reason):
        async with _plan_session() as session:
            await _dispatched_plan_page(session)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_valid", [False, True])
async def test_plan_page_preserves_source_provenance(monkeypatch, is_valid):
    _native_fixture(monkeypatch)
    provenance_by_key = {
        source_key: dict(
            source_key=source_key if is_valid else None,
            source_type="in_network",
            identity_kind="logical_json",
            identity_sha256="d" * 64,
        )
        for source_key in (1, 2)
    }
    provenance_reader = AsyncMock(return_value=provenance_by_key)
    monkeypatch.setattr(serving, "_ptg2_source_provenance_for_rows", provenance_reader)
    if not is_valid:
        with pytest.raises(PTG2ManifestArtifactError, match="dense artifact key"):
            async with _plan_session() as session:
                await _dispatched_plan_page(session, include_details="true")
    else:
        async with _plan_session() as session:
            response = await _dispatched_plan_page(session, include_details="true")
        assert response["pagination"]["total"] == 6
        assert {entry["source_artifact_key"] for entry in response["items"]} == {1, 2}
        assert all(entry["source_key"] == "source-0" for entry in response["items"])
        assert all(entry["identity_sha256"] == "d" * 64 for entry in response["items"])
        assert all(entry["source_type"] == "in_network" for entry in response["items"])
        assert all(entry["identity_kind"] == "logical_json" for entry in response["items"])
    assert provenance_reader.await_count == 1


@pytest.mark.asyncio
async def test_plan_http_retains_imported_score_order(monkeypatch):
    _native_fixture(monkeypatch)
    selection = replace(_selection(), plan_release_id="hprelease_" + "1" * 26)
    release_reader = AsyncMock(return_value=selection)
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", release_reader)
    monkeypatch.setattr(
        pricing,
        "_resolve_ptg_specialty_or_raise",
        AsyncMock(return_value=SimpleNamespace(unresolved_specialty=False, taxonomy_codes=[])),
    )
    query = _import_context()
    async with _plan_session() as session:
        request = SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session))
        reply = await pricing.list_providers_by_procedure(
            request,
            native_args={
                "code": "27447",
                "code_system": "CPT",
                "plan_release_id": selection.plan_release_id,
                "limit": "2",
                "include_allowed_amounts": "false",
            },
            import_context=query,
        )
    response = json.loads(reply.body)
    assert reply.status == 200
    assert response["pagination"]["total"] == 6
    assert [entry["npi"] for entry in response["items"]] == [1000000002] * 2
    assert sorted(len(entry["prices"]) for entry in response["items"]) == [1, 2]
    assert len(set(response[imported.NATIVE_ENTRY_IDS])) == 2
    assert release_reader.await_args.args[1] == selection.plan_release_id


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ["sitemap", "count"])
async def test_imported_list_requires_provider_items(view):
    session = SimpleNamespace()
    args_by_name = {"include_total": ["true"]}
    args_by_name["view" if view == "sitemap" else "count_only"] = ["sitemap" if view == "sitemap" else "1"]
    with pytest.raises(pricing.InvalidUsage, match="require provider results"):
        await npi_module.list_providers(
            SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
            native_args=RequestParameters(args_by_name),
            import_context=list_fixture._import_context(direction="asc", require_match=True),
        )


@pytest.mark.asyncio
async def test_imported_geo_rejects_malformed_plan_network():
    with pytest.raises(pricing.InvalidUsage, match="plan_network must contain integers"):
        await npi_module.get_near_npi(
            SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=object())),
            native_args=RequestParameters({"lat": ["0"], "long": ["0"], "plan_network": ["invalid"]}),
            import_context=geo_fixture._context(),
            prepare_cursor=AsyncMock(return_value=None),
        )


@pytest.mark.asyncio
async def test_imported_geo_requires_exact_count_receipt(monkeypatch):
    session = SimpleNamespace(calls=[])
    original_all = geo_fixture._RecordingGeoProxy.all

    async def rows_without_total(proxy, statement, **parameters):
        rows = await original_all(proxy, statement, **parameters)
        for row in rows:
            row._mapping.pop("_geo_total", None)
        return rows

    monkeypatch.setattr(geo_fixture._RecordingGeoProxy, "all", rows_without_total)
    monkeypatch.setattr(provider_list_sql, "ConnectionProxy", geo_fixture._RecordingGeoProxy)
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", geo_fixture._legacy_address_table)
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", geo_fixture._empty_plan_scope)
    with pytest.raises(RuntimeError, match="geo count is required"):
        await npi_module.get_near_npi(
            SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
            native_args=RequestParameters({"lat": ["0"], "long": ["0"], "include_total": ["true"]}),
            import_context=geo_fixture._context(),
            prepare_cursor=AsyncMock(return_value=None),
        )
    assert len(session.calls) == 1
