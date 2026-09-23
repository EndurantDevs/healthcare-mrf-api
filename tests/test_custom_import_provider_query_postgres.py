# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proofs for custom-import provider-service composition."""

from __future__ import annotations

import json
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData, exists, func, literal, select

from api import custom_import_provider_service_http as service_http
from api import custom_import_read_http as transport
from api.endpoint import pricing
from api.custom_import_provider_service_sql import (
    ProviderServiceImportQuery,
    build_provider_service_claims_statements,
)
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
    ReadFilter,
    ReadOrderTerm,
)
from tests import custom_import_postgres_support as postgres_support
from tests import test_custom_import_read_core_postgres as read_fixture
from tests import test_custom_import_read_http as http_fixture
from tests.custom_import_postgres_support import digest, isolated_publication_case, transaction_session

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

    assert len(relation_rows) == 2
    assert total == 1
    assert page == [_NPI]
    assert "LIMIT" not in str(prepared_relation.statement)


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
    """Predicate contexts stay correlated and host SQL counts/pages through EXISTS."""

    _seed_npi_bindings(monkeypatch)
    _seed_selected_contexts(monkeypatch, (0, 1))
    async with transaction_session() as session, session.begin():
        fixture = await _seed_npi_fixture(session)
        service = read_fixture._service()
        authorization = ExtensionReadAuthorization("synthetic-provider-query")
        prepared = await service.prepare_npi_entity_relation(
            session,
            authorization=authorization,
            target=fixture.target,
        )
        await _assert_native_exists_composition(session, prepared)
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
