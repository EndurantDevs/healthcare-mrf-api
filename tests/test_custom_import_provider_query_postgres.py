# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL composition proof for the provider-query custom-import seam."""

from __future__ import annotations

from copy import deepcopy

import pytest
from sqlalchemy import exists, func, literal, select

from db.models.custom_import import (
    CustomImportChildScalar,
    CustomImportEntityBinding,
    CustomImportWinner,
)
from process.custom_import.read_core import (
    CustomImportReadRequestError,
    ExtensionReadAuthorization,
    ReadFilter,
    ReadOrderTerm,
)
from tests import custom_import_postgres_support as postgres_support
from tests import test_custom_import_read_core_postgres as read_fixture
from tests.custom_import_postgres_support import digest, transaction_session

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


def _native_provider_relation():
    return (
        select(literal(_NPI).label("npi")).union_all(select(literal(_ABSENT_NPI).label("npi"))).cte("native_provider")
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
        filters=filters,
    )
    return (await session.execute(prepared_relation.statement)).all()


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
            order_terms=(ReadOrderTerm("amount", direction, "last"),),
        )
        relation = prepared.statement.subquery("ordered_import")
        native = _native_provider_relation()
        scalar = await session.scalar(
            select(CustomImportChildScalar).where(
                CustomImportChildScalar.child_revision_id == fixture.selected_family.child_revision_ids[child_index],
                CustomImportChildScalar.field_slot == 6,
            )
        )
        sort_order = relation.c.sort_0.asc().nullslast()
        if direction == "desc":
            sort_order = relation.c.sort_0.desc().nullslast()
        ordered_native_rows = (
            await session.execute(
                select(native.c.npi, relation.c.sort_0)
                .select_from(native.outerjoin(relation, relation.c.entity_value == native.c.npi))
                .order_by(relation.c.entity_value.is_(None).asc(), sort_order, native.c.npi)
                .offset(0)
                .limit(2)
            )
        ).all()

        assert prepared.normalized_order_terms == (ReadOrderTerm("amount", direction, "last"),)
        if expected_amount_state == "missing":
            assert scalar is None
        else:
            assert scalar is not None
            assert scalar.value_state == "null"
        assert await session.scalar(select(func.count()).select_from(native)) == 2
        assert ordered_native_rows == [(_NPI, None), (_ABSENT_NPI, None)]
