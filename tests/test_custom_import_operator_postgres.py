# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import CustomImportDataset, CustomImportPublicationEvent
from process.custom_import.operator import OperatorObjectNotFound, inspect_execution, inspect_generation
from process.custom_import.publication import activate_generation, record_no_change
from tests.custom_import_postgres_support import digest, isolated_publication_case, seed_publication_graph
from tests.test_custom_import_publication_postgres import (
    _downgrade_finality_schema,
    _finality_insert_duplicate_legacy_publication_events,
    _finality_insert_legacy_completed_execution,
    _finality_insert_legacy_generation,
    _finality_insert_minimal_identity,
    _finality_table,
    _upgrade_finality_schema,
)


async def _read_execution_status(session: AsyncSession, *, dataset_id: int, execution_id: int):
    return await inspect_execution(session, dataset_id=dataset_id, execution_id=execution_id)


async def _read_generation_status(session: AsyncSession, *, dataset_id: int, generation_id: int):
    return await inspect_generation(session, dataset_id=dataset_id, generation_id=generation_id)


async def _seed_graph_with_mismatched_event(case):
    async with case.sessions() as session:
        async with session.begin():
            graph = await seed_publication_graph(session)
            session.add(
                CustomImportPublicationEvent(
                    dataset_id=graph.dataset_id,
                    definition_revision_id=graph.definition_revision_id,
                    schema_revision_id=graph.schema_revision_id,
                    execution_id=graph.second_execution_id,
                    event_kind="activated",
                    from_generation_id=None,
                    to_generation_id=graph.first_generation_id,
                    expected_pointer_version=98,
                    committed_pointer_version=99,
                    finality_contract="custom-import-finality/v1",
                    canonical_event="{}",
                    event_sha256=digest("wrong-producing-execution-event"),
                )
            )
    return graph


async def _assert_pending_dataset_is_unflushed(case, graph):
    session = AsyncSession(case.engine, expire_on_commit=False, autoflush=True)
    try:
        await session.begin()
        pending_dataset = CustomImportDataset(dataset_key="synthetic_pending_operator")
        session.add(pending_dataset)
        await _read_execution_status(
            session,
            dataset_id=graph.dataset_id,
            execution_id=graph.first_execution_id,
        )
        assert pending_dataset.dataset_id is None
        assert pending_dataset in session.new
        await session.rollback()
    finally:
        await session.close()


async def _assert_initial_operator_states(case, graph):
    async with case.sessions() as session:
        async with session.begin():
            execution_status = await _read_execution_status(
                session,
                dataset_id=graph.dataset_id,
                execution_id=graph.first_execution_id,
            )
            first_generation_status = await _read_generation_status(
                session,
                dataset_id=graph.dataset_id,
                generation_id=graph.first_generation_id,
            )
            with pytest.raises(OperatorObjectNotFound):
                await _read_generation_status(
                    session,
                    dataset_id=graph.dataset_id + 1,
                    generation_id=graph.first_generation_id,
                )
            assert execution_status.state == "completed"
            assert first_generation_status.publication_state == "sealed_unpublished"
            assert first_generation_status.current is None
            assert not first_generation_status.ever_published


async def _activate_generation_in_transaction(
    case,
    *,
    dataset_id: int,
    target_generation_id: int,
    expected_generation_id: int | None,
    expected_pointer_version: int,
):
    async with case.sessions() as session:
        async with session.begin():
            await activate_generation(
                session,
                dataset_id=dataset_id,
                target_generation_id=target_generation_id,
                expected_generation_id=expected_generation_id,
                expected_pointer_version=expected_pointer_version,
            )


async def _assert_current_generation_state(case, graph):
    async with case.sessions() as session:
        async with session.begin():
            first_generation_status = await _read_generation_status(
                session,
                dataset_id=graph.dataset_id,
                generation_id=graph.first_generation_id,
            )
            assert first_generation_status.publication_state == "current"
            assert first_generation_status.current is not None and first_generation_status.current.pointer_version == 1
            assert first_generation_status.ever_published


async def _assert_superseded_and_current_states(case, graph):
    async with case.sessions() as session:
        async with session.begin():
            first_generation_status = await _read_generation_status(
                session,
                dataset_id=graph.dataset_id,
                generation_id=graph.first_generation_id,
            )
            second_generation_status = await _read_generation_status(
                session,
                dataset_id=graph.dataset_id,
                generation_id=graph.second_generation_id,
            )
            assert first_generation_status.publication_state == "superseded"
            assert second_generation_status.publication_state == "current"
            assert (
                second_generation_status.current is not None and second_generation_status.current.pointer_version == 2
            )


async def _record_graph_no_change_in_transaction(case, graph):
    async with case.sessions() as session:
        async with session.begin():
            await record_no_change(
                session,
                dataset_id=graph.dataset_id,
                execution_id=graph.no_change_execution_id,
                expected_generation_id=graph.second_generation_id,
                expected_pointer_version=2,
                candidate_generation_id=graph.no_change_candidate_generation_id,
                lease_fence=graph.no_change_fence,
                lease_token=graph.no_change_token,
            )


async def _assert_no_change_state(case, graph):
    async with case.sessions() as session:
        async with session.begin():
            no_change_execution_status = await _read_execution_status(
                session,
                dataset_id=graph.dataset_id,
                execution_id=graph.no_change_execution_id,
            )
            candidate_generation_status = await _read_generation_status(
                session,
                dataset_id=graph.dataset_id,
                generation_id=graph.no_change_candidate_generation_id,
            )
            assert no_change_execution_status.state == "no_change"
            assert candidate_generation_status.publication_state == "no_change"
            assert candidate_generation_status.no_change is not None
            assert candidate_generation_status.no_change.base_generation_id == graph.second_generation_id
            assert candidate_generation_status.current is not None
            assert candidate_generation_status.current.generation_id == graph.second_generation_id
            assert candidate_generation_status.current.pointer_version == 2


async def _insert_legacy_current_pointer(connection, schema_name: str, identity, generation_id: int):
    dataset_id, definition_revision_id, schema_revision_id, _capture_bundle_id = identity
    await connection.execute(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_current_generation')} "
            "(dataset_id, definition_revision_id, schema_revision_id, generation_id, pointer_version) "
            "VALUES (:dataset_id, :definition_revision_id, :schema_revision_id, :generation_id, 1)"
        ),
        {
            "dataset_id": dataset_id,
            "definition_revision_id": definition_revision_id,
            "schema_revision_id": schema_revision_id,
            "generation_id": generation_id,
        },
    )


async def _seed_legacy_current_generation(case):
    async with case.engine.begin() as connection:
        await connection.run_sync(_downgrade_finality_schema, case.schema_name)
        identity = await _finality_insert_minimal_identity(
            connection,
            case.schema_name,
            label="legacy_operator",
        )
        execution_id = await _finality_insert_legacy_completed_execution(connection, case.schema_name, identity)
        generation_id = await _finality_insert_legacy_generation(
            connection,
            case.schema_name,
            identity,
            execution_id,
        )
        await _insert_legacy_current_pointer(connection, case.schema_name, identity, generation_id)
        await _finality_insert_duplicate_legacy_publication_events(
            connection,
            case.schema_name,
            identity,
            execution_id,
            generation_id,
        )
        await connection.run_sync(_upgrade_finality_schema, case.schema_name)
    return identity[0], generation_id


async def _delete_current_pointer(case, dataset_id: int):
    async with case.engine.begin() as connection:
        await connection.execute(
            text(
                f"DELETE FROM {_finality_table(case.schema_name, 'custom_import_current_generation')} "
                "WHERE dataset_id = :dataset_id"
            ),
            {"dataset_id": dataset_id},
        )


@pytest.mark.asyncio
async def test_operator_inspection_tracks_sealed_current_superseded_and_no_change_states():
    """Inspect sealed, current, superseded, and no-change publication states."""

    async with isolated_publication_case() as case:
        graph = await _seed_graph_with_mismatched_event(case)
        await _assert_pending_dataset_is_unflushed(case, graph)
        await _assert_initial_operator_states(case, graph)
        await _activate_generation_in_transaction(
            case,
            dataset_id=graph.dataset_id,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        await _assert_current_generation_state(case, graph)
        await _activate_generation_in_transaction(
            case,
            dataset_id=graph.dataset_id,
            target_generation_id=graph.second_generation_id,
            expected_generation_id=graph.first_generation_id,
            expected_pointer_version=1,
        )
        await _assert_superseded_and_current_states(case, graph)
        await _record_graph_no_change_in_transaction(case, graph)
        await _assert_no_change_state(case, graph)


@pytest.mark.asyncio
async def test_operator_inspection_retains_unsealed_legacy_current_and_superseded_states():
    """Inspect legacy generations before and after their current pointer is removed."""

    async with isolated_publication_case() as case:
        dataset_id, generation_id = await _seed_legacy_current_generation(case)
        async with case.sessions() as session:
            async with session.begin():
                current_generation_status = await _read_generation_status(
                    session,
                    dataset_id=dataset_id,
                    generation_id=generation_id,
                )
        assert current_generation_status.publication_state == "current"
        assert (
            current_generation_status.current is not None
            and current_generation_status.current.generation_id == generation_id
        )
        assert current_generation_status.seal is None
        assert current_generation_status.ever_published is True

        await _delete_current_pointer(case, dataset_id)

        async with case.sessions() as session:
            async with session.begin():
                superseded_generation_status = await _read_generation_status(
                    session,
                    dataset_id=dataset_id,
                    generation_id=generation_id,
                )
        assert superseded_generation_status.publication_state == "superseded"
        assert superseded_generation_status.current is None
        assert superseded_generation_status.seal is None
        assert superseded_generation_status.ever_published is True
