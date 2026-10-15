# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native legacy projections share graph rollback and ordinary publication."""

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select, text

from db.models.custom_import import (
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
)
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import query_projection_finality as finality
from process.custom_import import query_projection_legacy as legacy
from process.custom_import import read_core
from process.custom_import.query_projection_evidence import (
    completed_projection_evidence,
    read_seal_projection_evidence,
)
from process.custom_import.query_projection_storage import query_projection_index_statements, query_projection_tables
from process.custom_import.runner import run_candidate
from process.custom_import.runner_types import CandidateRunnerError
from tests.custom_import_postgres_support import isolated_publication_case
from tests.custom_import_query_projection_build_postgres import _definition, _records
from tests.test_custom_import_read_core_postgres import _service
from tests.test_custom_import_runner_postgres import _new_execution, _request, _Seed, _seed_case


def _flatten_records(records_by_stream):
    return (
        [record for page in records_by_stream["providers"] for record in page],
        [record for page in records_by_stream["rates"] for record in page],
    )


async def _legacy_request(case, suffix, *, small=False):
    definition = _definition()
    seed = await _seed_case(case, suffix, definition)
    execution_id, token = await _new_execution(case, seed, suffix)
    roots, child_records = _flatten_records(_records())
    if small:
        roots = roots[:2]
        child_records = [
            record for record in child_records if record["rate_period"] == 10 and record["service_code"] == "S000"
        ]
    return _request(seed, execution_id, token, roots, child_records)


async def _assert_completed(case, request, generation_id):
    async with case.sessions() as session, session.begin():
        generation = await session.get(CustomImportGeneration, generation_id)
        manifest = (
            await session.scalars(
                select(CustomImportQueryProjection).where(CustomImportQueryProjection.generation_id == generation_id)
            )
        ).one()
        seal = await session.get(CustomImportGenerationSeal, generation_id)
        evidence = completed_projection_evidence(
            manifest, generation=generation, definition=request.definition, seal=seal
        )
        assert evidence == read_seal_projection_evidence(seal)
        assert (evidence.root_count, evidence.child_count) == (3, 261)
        assert manifest.phase == "complete"
        assert manifest.index_count == len(query_projection_index_statements(manifest.family_id, request.definition))
        assert (manifest.verified_root_count, manifest.digested_root_count) == (3, 3)
        assert (manifest.verified_child_count, manifest.digested_child_count) == (261, 261)
        tables = query_projection_tables(manifest.family_id, request.definition)
        root_rows = (await session.execute(select(tables["custom_import_derived_root"]))).all()
        assert sorted(
            (context_row.selected_value, context_row.field_1)
            for context_row in root_rows
            if context_row.field_1 is not None
        ) == [
            (10, Decimal("0.000000000000")),
            (20, Decimal("0.000000000000")),
        ]
        assert sum(context_row.field_1 is None for context_row in root_rows) == 1
        child_rows = (await session.execute(select(tables["custom_import_derived_child"]))).all()
        assert len(child_rows) == 261 and all(context_row.field_3 == 3 for context_row in child_rows)
        assert sum(context_row.field_2 is None for context_row in child_rows) == 260
        assert (
            len(
                {
                    (context_row.entity_binding_id, context_row.selected_value, context_row.canonical_child_key)
                    for context_row in child_rows
                }
            )
            == 261
        )
        family = await session.get(CustomImportSnapshotFamily, manifest.family_id)
        assert family.frozen_at is not None


@pytest.mark.asyncio
async def test_legacy_runner_persists_complete_context_pages_before_normal_publication():
    async with isolated_publication_case() as case:
        request = await _legacy_request(case, "derived_publish")
        result = await run_candidate(case.sessions, request)
        assert result.status == "activated"
        await _assert_completed(case, request, result.generation_id)


@pytest.mark.asyncio
async def test_legacy_parity_failure_rolls_back_contexts_indexes_and_graph(monkeypatch):
    original = legacy._verify_scope

    async def reject_verified_contexts(session, context, manifest, registry, *, child):
        await original(session, context, manifest, registry, child=child)
        assert manifest.root_count and manifest.index_count
        raise CandidateRunnerError("synthetic persisted projection mismatch")

    monkeypatch.setattr(legacy, "_verify_scope", reject_verified_contexts)
    async with isolated_publication_case() as case:
        request = await _legacy_request(case, "derived_rollback", small=True)
        with pytest.raises(CandidateRunnerError, match="persisted projection mismatch"):
            await run_candidate(case.sessions, request)
        async with case.sessions() as session:
            for model in (CustomImportQueryProjection, CustomImportGeneration, CustomImportSnapshotFamily):
                assert await session.scalar(select(func.count()).select_from(model)) == 0


async def _repeat_request(case, request, suffix):
    async with case.sessions() as session:
        execution = await session.get(CustomImportExecution, request.execution_id)
        seed = _Seed(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            capture_bundle_id=execution.capture_bundle_id,
            definition=request.definition,
        )
    execution_id, token = await _new_execution(case, seed, suffix)
    return _request(seed, execution_id, token, request.roots, request.children_by_collection["rates"])


async def _pinned_page(session, service, request, generation_id):
    pinned_target = read_core.PinnedReadTarget(
        request.dataset_id,
        generation_id,
        request.definition_revision_id,
        request.schema_revision_id,
        "families_by_period",
    )
    query = read_core.NpiEntityRelationQuery(
        grouped_entity_selection=request.definition.query.entity_selection.document(),
        family_entitlement="full_family",
    )
    authorization = read_core.ExtensionReadAuthorization("synthetic-projection-upgrade")
    prepared = await service.prepare_npi_entity_relation(
        session,
        authorization=authorization,
        target=pinned_target,
        query=query,
    )
    context_rows = (
        await session.execute(prepared.statement.order_by(prepared.statement.selected_columns.entity_value))
    ).all()
    entity_values = tuple(context_row.entity_value for context_row in context_rows)
    hydrated_by_npi = await service.hydrate_npi_page(
        session,
        authorization=authorization,
        pinned_target=pinned_target,
        prepared=prepared,
        entity_values=entity_values,
        query=query,
    )
    return tuple(tuple(context_row) for context_row in context_rows), hydrated_by_npi


@pytest.mark.asyncio
async def test_legacy_projection_upgrade_activates_then_identical_output_is_no_change(monkeypatch):
    async with isolated_publication_case() as case:
        request = await _legacy_request(case, "derived_old_seal", small=True)
        with monkeypatch.context() as old_producer:
            old_producer.setattr(legacy, "persist_legacy_query_projection", AsyncMock(return_value=None))
            old_producer.setattr(finality, "configured_fields", lambda definition: ())
            original = await run_candidate(case.sessions, request)
        assert original.status == "activated"
        async with case.sessions() as session:
            old_seal = await session.get(CustomImportGenerationSeal, original.generation_id)
            assert read_seal_projection_evidence(old_seal) is None
            assert await session.scalar(select(func.count()).select_from(CustomImportQueryProjection)) == 0
        service = _service()
        async with case.sessions() as pinned, pinned.begin():
            await pinned.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            before = await _pinned_page(pinned, service, request, original.generation_id)
            assert len(before[0]) == len(before[1]) == 1
            upgrade_request = await _repeat_request(case, request, "derived_upgrade")
            upgraded = await run_candidate(case.sessions, upgrade_request)
            assert upgraded.status == "activated" and upgraded.generation_id != original.generation_id
            assert await _pinned_page(pinned, service, request, original.generation_id) == before
            repeat_request = await _repeat_request(case, request, "derived_repeat")
            repeated = await run_candidate(case.sessions, repeat_request)
            assert repeated.status == "no_change"
            assert await _pinned_page(pinned, service, request, original.generation_id) == before
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
            assert pointer.generation_id == upgraded.generation_id
            projected_seal = await session.get(CustomImportGenerationSeal, upgraded.generation_id)
            evidence = read_seal_projection_evidence(projected_seal)
            assert evidence is not None
            repeated_generation = await session.get(CustomImportGeneration, repeated.generation_id)
            repeated_manifest = await session.scalar(
                select(CustomImportQueryProjection).where(
                    CustomImportQueryProjection.generation_id == repeated.generation_id
                )
            )
            assert (
                completed_projection_evidence(
                    repeated_manifest, generation=repeated_generation, definition=request.definition
                )
                == evidence
            )
