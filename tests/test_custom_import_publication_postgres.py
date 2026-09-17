# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import importlib.util
from dataclasses import dataclass
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCurrentGeneration,
    CustomImportEntityBinding,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPack,
    CustomImportPublicationEvent,
    CustomImportRejection,
    CustomImportRootRecord,
    CustomImportRootRevision,
    CustomImportSchemaRevision,
    CustomImportWinner,
)
from process.custom_import import publication as publication_module
from process.custom_import.execution import (
    ExecutionLifecycleError,
    finish_execution,
    heartbeat_execution,
    request_cancellation,
    resume_execution,
)
from process.custom_import.publication import (
    PublicationConflict,
    activate_generation,
    record_no_change,
    rollback_generation,
    seal_generation,
)
from tests.custom_import_postgres_support import (
    FamilyMaterial,
    FamilyMaterialSpec,
    GenerationAttempt,
    PublicationGraph,
    attach_generation_family,
    digest,
    isolated_publication_case,
    lease_digest,
    seed_family_material,
    seed_publication_graph,
    seed_running_generation,
    seed_sealed_generation,
    transaction_session,
)

_RECOVERY_CREDENTIAL = "synthetic-recovery-holder"


async def _seed_committed_publication_graph(case):
    async with case.sessions() as session:
        async with session.begin():
            return await seed_publication_graph(session)


async def _seed_competing_generation(case, graph, *, base_generation_id: int | None = None) -> int:
    async with case.sessions() as session:
        async with session.begin():
            return await seed_sealed_generation(
                session,
                graph,
                suffix=f"{graph.dataset_id}_{base_generation_id or 0}",
                base_generation_id=base_generation_id,
            )


def _table(case, table_name: str) -> str:
    assert case.schema_name.startswith("custom_import_publication_")
    assert table_name.startswith("custom_import_")
    return f'"{case.schema_name}"."{table_name}"'


async def _seed_distinct_capture_no_change_execution(session, graph: PublicationGraph):
    """Create a live execution over a new capture identity with equal content."""

    capture_bundle = await _seed_distinct_capture_bundle(session, graph)
    return await _seed_capture_no_change_execution(session, graph, capture_bundle)


async def _seed_distinct_capture_bundle(session, graph: PublicationGraph) -> CustomImportCaptureBundle:
    """Copy every capture into a bundle with a distinct snapshot identity."""

    source_bundle = await session.get(CustomImportCaptureBundle, graph.capture_bundle_id)
    assert source_bundle is not None
    source_captures = (
        (
            await session.execute(
                select(CustomImportCapture)
                .where(
                    CustomImportCapture.capture_bundle_id == graph.capture_bundle_id,
                )
                .order_by(CustomImportCapture.stream_slot)
            )
        )
        .scalars()
        .all()
    )
    capture_bundle = CustomImportCaptureBundle(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        snapshot_token=f"new-snapshot-{graph.dataset_id}",
        snapshot_token_sha256=digest(f"new-snapshot:{graph.dataset_id}"),
        canonical_manifest=source_bundle.canonical_manifest,
        manifest_sha256=source_bundle.manifest_sha256,
        stream_count=source_bundle.stream_count,
    )
    session.add(capture_bundle)
    await session.flush()
    session.add_all(
        CustomImportCapture(
            capture_bundle_id=capture_bundle.capture_bundle_id,
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            stream_slot=source_capture.stream_slot,
            content_sha256=source_capture.content_sha256,
            byte_count=source_capture.byte_count,
            canonical_manifest=source_capture.canonical_manifest,
            manifest_sha256=source_capture.manifest_sha256,
        )
        for source_capture in source_captures
    )
    await session.flush()
    return capture_bundle


async def _seed_capture_no_change_execution(
    session,
    graph: PublicationGraph,
    capture_bundle: CustomImportCaptureBundle,
) -> tuple[int, int, str]:
    """Create a live no-change execution and candidate over one capture bundle."""

    token = f"new-capture-lease-{graph.dataset_id}"
    execution = CustomImportExecution(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        capture_bundle_id=capture_bundle.capture_bundle_id,
        idempotency_key=f"new-capture-no-change-{graph.dataset_id}",
        mechanism="local",
        state="running",
    )
    session.add(execution)
    await session.flush()
    now = await session.scalar(select(func.clock_timestamp()))
    session.add(
        CustomImportLease(
            execution_id=execution.execution_id,
            fence=1,
            token_sha256=lease_digest(token),
            heartbeat_at=now,
            expires_at=now + dt.timedelta(minutes=5),
        )
    )
    await session.flush()
    source_generation = await session.get(CustomImportGeneration, graph.first_generation_id)
    assert source_generation is not None
    candidate = CustomImportGeneration(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        execution_id=execution.execution_id,
        capture_bundle_id=capture_bundle.capture_bundle_id,
        source_bundle_sha256=source_generation.source_bundle_sha256,
        candidate_sha256=digest(f"distinct-capture-candidate:{graph.dataset_id}"),
        root_count=0,
        family_count=0,
        producing_fence=1,
        producing_token_sha256=lease_digest(token),
    )
    session.add(candidate)
    await session.flush()
    return execution.execution_id, candidate.generation_id, token


async def _add_different_no_change_output(session, graph: PublicationGraph) -> GenerationAttempt:
    """Materialize one family so the owned no-change candidate cannot compare equal."""

    attempt = await seed_running_generation(
        session,
        graph,
        suffix=f"different-no-change-output-{graph.dataset_id}",
        base_generation_id=graph.first_generation_id,
        root_count=1,
        family_count=1,
    )
    material = await seed_family_material(
        session,
        graph,
        attempt,
        FamilyMaterialSpec(
            suffix=f"different-no-change-output-{graph.dataset_id}",
            include_root_winner=True,
        ),
    )
    await attach_generation_family(session, graph, attempt, material)
    return attempt


async def _wait_for_backend_lock(case, backend_pid: int) -> None:
    async with case.sessions() as observer:
        for _ in range(200):
            wait_event_type = await observer.scalar(
                text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :backend_pid"),
                {"backend_pid": backend_pid},
            )
            if wait_event_type == "Lock":
                return
            await asyncio.sleep(0.01)
    pytest.fail("competing PostgreSQL session did not wait on the expected row lock")


async def _contending_activation(
    case,
    *,
    dataset_id: int,
    target_generation_id: int,
    expected_generation_id: int | None,
    expected_pointer_version: int,
    backend_pid_ready: asyncio.Future[int],
):
    async with case.sessions() as session:
        async with session.begin():
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            backend_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            backend_pid_ready.set_result(backend_pid)
            return await activate_generation(
                session,
                dataset_id=dataset_id,
                target_generation_id=target_generation_id,
                expected_generation_id=expected_generation_id,
                expected_pointer_version=expected_pointer_version,
            )


async def _contending_no_change(case, graph, backend_pid_ready: asyncio.Future[int]):
    async with case.sessions() as session:
        async with session.begin():
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            backend_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            backend_pid_ready.set_result(backend_pid)
            return await record_no_change(
                session,
                dataset_id=graph.dataset_id,
                execution_id=graph.no_change_execution_id,
                expected_generation_id=graph.first_generation_id,
                expected_pointer_version=1,
                candidate_generation_id=graph.no_change_candidate_generation_id,
                lease_fence=graph.no_change_fence,
                lease_token=graph.no_change_token,
            )


async def _activate_committed(
    case,
    graph: PublicationGraph,
    *,
    target_generation_id: int,
    expected_generation_id: int | None,
    expected_pointer_version: int,
):
    async with case.sessions() as session:
        async with session.begin():
            return await activate_generation(
                session,
                dataset_id=graph.dataset_id,
                target_generation_id=target_generation_id,
                expected_generation_id=expected_generation_id,
                expected_pointer_version=expected_pointer_version,
            )


async def _reject_activation(
    case,
    graph: PublicationGraph,
    *,
    target_generation_id: int,
    expected_generation_id: int | None,
    expected_pointer_version: int,
    match: str,
) -> None:
    async with case.sessions() as session:
        async with session.begin():
            with pytest.raises(PublicationConflict, match=match):
                await activate_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    target_generation_id=target_generation_id,
                    expected_generation_id=expected_generation_id,
                    expected_pointer_version=expected_pointer_version,
                )


async def _rollback_committed(
    case,
    graph: PublicationGraph,
    *,
    target_generation_id: int,
    expected_generation_id: int,
    expected_pointer_version: int,
):
    async with case.sessions() as session:
        async with session.begin():
            return await rollback_generation(
                session,
                dataset_id=graph.dataset_id,
                target_generation_id=target_generation_id,
                expected_generation_id=expected_generation_id,
                expected_pointer_version=expected_pointer_version,
            )


async def _assert_pointer_and_no_candidate_event(
    case,
    graph: PublicationGraph,
    candidate_generation_id: int,
) -> None:
    async with case.sessions() as verification_session:
        pointer = await verification_session.get(CustomImportCurrentGeneration, graph.dataset_id)
        assert pointer is not None
        assert (pointer.generation_id, pointer.pointer_version) == (graph.second_generation_id, 2)
        assert (
            await verification_session.scalar(
                select(func.count())
                .select_from(CustomImportPublicationEvent)
                .where(
                    CustomImportPublicationEvent.dataset_id == graph.dataset_id,
                    CustomImportPublicationEvent.to_generation_id == candidate_generation_id,
                )
            )
            == 0
        )


async def _expire_no_change_lease(case, graph: PublicationGraph) -> None:
    async with case.sessions() as session:
        async with session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == graph.no_change_execution_id)
                .values(expires_at=func.clock_timestamp() - func.make_interval(0, 0, 0, 0, 0, 1))
            )


async def _invalid_no_change_authority(
    case,
    graph: PublicationGraph,
    authority_case: str,
) -> tuple[int, str]:
    if authority_case == "wrong_token":
        return graph.no_change_fence, "synthetic-wrong-token"
    if authority_case == "stale_fence":
        return graph.no_change_fence + 1, graph.no_change_token
    await _expire_no_change_lease(case, graph)
    return graph.no_change_fence, graph.no_change_token


async def _assert_no_change_remains_unpublished(case, graph: PublicationGraph, state: str) -> None:
    async with case.sessions() as verification_session:
        assert (
            await verification_session.scalar(
                select(CustomImportExecution.state).where(
                    CustomImportExecution.execution_id == graph.no_change_execution_id
                )
            )
            == state
        )
        assert (
            await verification_session.scalar(
                select(func.count())
                .select_from(CustomImportPublicationEvent)
                .where(CustomImportPublicationEvent.execution_id == graph.no_change_execution_id)
            )
            == 0
        )


async def _cancel_pending_task(task) -> None:
    if task is not None and not task.done():
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def _activate_first_generation_and_replay(session, graph: PublicationGraph) -> None:
    first = await activate_generation(
        session,
        dataset_id=graph.dataset_id,
        target_generation_id=graph.first_generation_id,
        expected_generation_id=None,
        expected_pointer_version=0,
    )
    assert first.event_kind == "activated"
    assert first.committed_pointer_version == 1
    assert first.replayed is False
    first_replay = await activate_generation(
        session,
        dataset_id=graph.dataset_id,
        target_generation_id=graph.first_generation_id,
        expected_generation_id=None,
        expected_pointer_version=0,
    )
    assert first_replay.publication_event_id == first.publication_event_id
    assert first_replay.replayed is True


async def _activate_second_generation_and_rollback(session, graph: PublicationGraph) -> None:
    second = await activate_generation(
        session,
        dataset_id=graph.dataset_id,
        target_generation_id=graph.second_generation_id,
        expected_generation_id=graph.first_generation_id,
        expected_pointer_version=1,
    )
    assert second.from_generation_id == graph.first_generation_id
    assert second.committed_pointer_version == 2
    rollback = await rollback_generation(
        session,
        dataset_id=graph.dataset_id,
        target_generation_id=graph.first_generation_id,
        expected_generation_id=graph.second_generation_id,
        expected_pointer_version=2,
    )
    assert rollback.event_kind == "rolled_back"
    assert rollback.committed_pointer_version == 3


async def _record_no_change_and_replay(session, graph: PublicationGraph) -> None:
    unchanged = await record_no_change(
        session,
        dataset_id=graph.dataset_id,
        execution_id=graph.no_change_execution_id,
        expected_generation_id=graph.first_generation_id,
        expected_pointer_version=3,
        candidate_generation_id=graph.no_change_candidate_generation_id,
        lease_fence=graph.no_change_fence,
        lease_token=graph.no_change_token.encode("utf-8"),
    )
    assert unchanged.event_kind == "no_change"
    assert unchanged.committed_pointer_version == 3
    assert unchanged.replayed is False
    unchanged_replay = await record_no_change(
        session,
        dataset_id=graph.dataset_id,
        execution_id=graph.no_change_execution_id,
        expected_generation_id=graph.first_generation_id,
        expected_pointer_version=3,
        candidate_generation_id=graph.no_change_candidate_generation_id,
        lease_fence=graph.no_change_fence,
        lease_token=graph.no_change_token,
    )
    assert unchanged_replay.publication_event_id == unchanged.publication_event_id
    assert unchanged_replay.replayed is True


async def _assert_atomic_publication_state(session, graph: PublicationGraph) -> None:
    pointer = (
        await session.execute(
            select(CustomImportCurrentGeneration).where(CustomImportCurrentGeneration.dataset_id == graph.dataset_id)
        )
    ).scalar_one()
    assert (pointer.generation_id, pointer.pointer_version) == (graph.first_generation_id, 3)
    assert (
        await session.scalar(
            select(func.count())
            .select_from(CustomImportPublicationEvent)
            .where(CustomImportPublicationEvent.dataset_id == graph.dataset_id)
        )
        == 4
    )
    assert (
        await session.scalar(
            select(CustomImportExecution.state).where(
                CustomImportExecution.execution_id == graph.no_change_execution_id
            )
        )
        == "no_change"
    )
    lease = (
        await session.execute(
            select(CustomImportLease).where(CustomImportLease.execution_id == graph.no_change_execution_id)
        )
    ).scalar_one()
    assert lease.fence == graph.no_change_fence
    assert lease.expires_at == lease.heartbeat_at


@pytest.mark.asyncio
async def test_activation_rollback_no_change_and_exact_replays_are_atomic():
    async with transaction_session() as session, session.begin():
        graph = await seed_publication_graph(session)
        await _activate_first_generation_and_replay(session, graph)
        await _activate_second_generation_and_rollback(session, graph)
        await _record_no_change_and_replay(session, graph)
        await _assert_atomic_publication_state(session, graph)


@pytest.mark.asyncio
async def test_activation_requires_an_exact_immutable_base_generation_even_after_cas_refresh():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        candidate_generation_id = await _seed_competing_generation(
            case,
            graph,
            base_generation_id=graph.first_generation_id,
        )
        await _reject_activation(
            case,
            graph,
            target_generation_id=candidate_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
            match="base generation",
        )
        first = await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        assert first.replayed is False
        replay = await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        assert replay.publication_event_id == first.publication_event_id
        assert replay.replayed is True
        second = await _activate_committed(
            case,
            graph,
            target_generation_id=graph.second_generation_id,
            expected_generation_id=graph.first_generation_id,
            expected_pointer_version=1,
        )
        assert second.committed_pointer_version == 2
        await _reject_activation(
            case,
            graph,
            target_generation_id=candidate_generation_id,
            expected_generation_id=graph.second_generation_id,
            expected_pointer_version=2,
            match="base generation",
        )
        await _assert_pointer_and_no_candidate_event(case, graph, candidate_generation_id)
        rollback = await _rollback_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=graph.second_generation_id,
            expected_pointer_version=2,
        )
        assert rollback.event_kind == "rolled_back"
        assert rollback.committed_pointer_version == 3


@pytest.mark.asyncio
async def test_no_change_mismatch_leaves_execution_and_pointer_unchanged():
    async with transaction_session() as session, session.begin():
        graph = await seed_publication_graph(session)
        await activate_generation(
            session,
            dataset_id=graph.dataset_id,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )

        attempt = await _add_different_no_change_output(session, graph)

        with pytest.raises(PublicationConflict, match="effective output differs"):
            await record_no_change(
                session,
                dataset_id=graph.dataset_id,
                execution_id=attempt.execution_id,
                expected_generation_id=graph.first_generation_id,
                expected_pointer_version=1,
                candidate_generation_id=attempt.generation_id,
                lease_fence=attempt.fence,
                lease_token=attempt.token,
            )

        assert (
            await session.scalar(
                select(CustomImportExecution.state).where(CustomImportExecution.execution_id == attempt.execution_id)
            )
            == "running"
        )


async def _assert_unsealed_pointer_insert_rejected(case, graph: PublicationGraph, generation_id: int) -> None:
    """Prove the database pointer cannot reference an unsealed candidate."""

    async with case.sessions() as session:
        async with session.begin():
            with pytest.raises(DBAPIError, match="custom_import_current_generation_seal_fkey"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f"INSERT INTO {_table(case, 'custom_import_current_generation')} "
                            "(dataset_id, definition_revision_id, schema_revision_id, generation_id, pointer_version) "
                            "VALUES (:dataset_id, :definition_revision_id, :schema_revision_id, "
                            ":generation_id, 1)"
                        ),
                        {
                            "dataset_id": graph.dataset_id,
                            "definition_revision_id": graph.definition_revision_id,
                            "schema_revision_id": graph.schema_revision_id,
                            "generation_id": generation_id,
                        },
                    )


@pytest.mark.asyncio
async def test_unsealed_candidate_cannot_publish_and_database_pointer_fk_fails_closed():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.sessions() as session:
            async with session.begin():
                attempt = await seed_running_generation(
                    session,
                    graph,
                    suffix=f"unsealed-{graph.dataset_id}",
                    base_generation_id=None,
                )
        await _reject_activation(
            case,
            graph,
            target_generation_id=attempt.generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
            match="not sealed",
        )
        await _assert_unsealed_pointer_insert_rejected(case, graph, attempt.generation_id)


async def _seed_stale_fence_execution(
    session,
    graph: PublicationGraph,
) -> tuple[CustomImportExecution, bytes]:
    """Create an execution whose current lease has superseded authority."""

    source_generation = await session.get(CustomImportGeneration, graph.first_generation_id)
    assert source_generation is not None
    execution = CustomImportExecution(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        capture_bundle_id=graph.capture_bundle_id,
        idempotency_key=f"stale-generation-{graph.dataset_id}",
        mechanism="local",
        state="running",
    )
    session.add(execution)
    await session.flush()
    now = await session.scalar(select(func.clock_timestamp()))
    session.add(
        CustomImportLease(
            execution_id=execution.execution_id,
            fence=2,
            token_sha256=lease_digest("current-fence"),
            heartbeat_at=now,
            expires_at=now + dt.timedelta(minutes=5),
        )
    )
    await session.flush()
    return execution, source_generation.source_bundle_sha256


async def _assert_stale_generation_insert_rejected(case, graph: PublicationGraph) -> None:
    """Prove the generation insert trigger rejects an old producing fence."""

    async with case.sessions() as session:
        async with session.begin():
            execution, source_bundle_sha256 = await _seed_stale_fence_execution(session, graph)
            with pytest.raises(DBAPIError, match="custom_import_generation_producing_lease_lost"):
                async with session.begin_nested():
                    session.add(
                        CustomImportGeneration(
                            dataset_id=graph.dataset_id,
                            definition_revision_id=graph.definition_revision_id,
                            schema_revision_id=graph.schema_revision_id,
                            execution_id=execution.execution_id,
                            capture_bundle_id=graph.capture_bundle_id,
                            source_bundle_sha256=source_bundle_sha256,
                            candidate_sha256=digest(f"stale-generation-sha:{graph.dataset_id}"),
                            root_count=0,
                            family_count=0,
                            producing_fence=1,
                            producing_token_sha256=lease_digest("old-fence"),
                        )
                    )
                    await session.flush()


async def _take_over_generation(case, graph: PublicationGraph):
    """Expire a candidate lease and return its replacement authority."""

    async with case.sessions() as session:
        async with session.begin():
            attempt = await seed_running_generation(
                session,
                graph,
                suffix=f"seal-takeover-{graph.dataset_id}",
                base_generation_id=None,
            )
    async with case.sessions() as session:
        async with session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == attempt.execution_id)
                .values(expires_at=func.clock_timestamp() - text("interval '1 second'"))
            )
    async with case.sessions() as session:
        async with session.begin():
            takeover = await resume_execution(
                session,
                execution_id=attempt.execution_id,
                token=_RECOVERY_CREDENTIAL,
            )
            assert takeover is not None
            assert takeover.fence == 2
    return attempt, takeover


async def _assert_stale_candidate_recovery(case, graph: PublicationGraph, attempt, takeover) -> None:
    """Prove stale candidate rows remain immutable while a fresh candidate seals."""

    async with case.sessions() as session:
        async with session.begin():
            with pytest.raises(PublicationConflict, match="producing authority is stale"):
                await seal_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    generation_id=attempt.generation_id,
                    lease_fence=2,
                    lease_token=_RECOVERY_CREDENTIAL,
                )
            stale_candidate = await session.get(CustomImportGeneration, attempt.generation_id)
            assert stale_candidate is not None
            with pytest.raises(DBAPIError, match="custom_import_immutable_row"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f"UPDATE {_table(case, 'custom_import_generation')} "
                            "SET root_count = 1 WHERE generation_id = :generation_id"
                        ),
                        {"generation_id": stale_candidate.generation_id},
                    )
            recovered_candidate = CustomImportGeneration(
                dataset_id=graph.dataset_id,
                definition_revision_id=graph.definition_revision_id,
                schema_revision_id=graph.schema_revision_id,
                execution_id=attempt.execution_id,
                capture_bundle_id=graph.capture_bundle_id,
                source_bundle_sha256=stale_candidate.source_bundle_sha256,
                candidate_sha256=stale_candidate.candidate_sha256,
                root_count=0,
                family_count=0,
                producing_fence=takeover.fence,
                producing_token_sha256=lease_digest(_RECOVERY_CREDENTIAL),
            )
            session.add(recovered_candidate)
            await session.flush()
            recovered = await seal_generation(
                session,
                dataset_id=graph.dataset_id,
                generation_id=recovered_candidate.generation_id,
                lease_fence=takeover.fence,
                lease_token=_RECOVERY_CREDENTIAL,
            )
            assert recovered.replayed is False
            assert recovered.execution_id == attempt.execution_id
            assert recovered_candidate.candidate_sha256 == stale_candidate.candidate_sha256
            assert await session.get(CustomImportGenerationSeal, attempt.generation_id) is None
            assert await session.get(CustomImportGenerationSeal, recovered_candidate.generation_id) is not None


@pytest.mark.asyncio
async def test_generation_insert_and_seal_reject_stale_fence_authority():
    """Stale lease authority cannot be reused for an immutable generation seal."""

    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        await _assert_stale_generation_insert_rejected(case, graph)
        attempt, takeover = await _take_over_generation(case, graph)
        await _assert_stale_candidate_recovery(case, graph, attempt, takeover)


async def _seed_fence_one_output(case, graph: PublicationGraph):
    """Retain ordinary fence-one output before its lease is deliberately taken over."""

    async with case.sessions() as session:
        async with session.begin():
            attempt = await seed_running_generation(
                session,
                graph,
                suffix=f"fence-one-output-{graph.dataset_id}",
                base_generation_id=None,
                root_count=1,
                family_count=1,
            )
            family = await seed_family_material(
                session,
                graph,
                attempt,
                FamilyMaterialSpec(
                    suffix=f"fence-one-output-{graph.dataset_id}",
                    child_keys=("fence-one-child",),
                ),
            )
            await attach_generation_family(session, graph, attempt, family)
            root_pack = await session.scalar(
                select(CustomImportPack).where(
                    CustomImportPack.execution_id == attempt.execution_id,
                    CustomImportPack.producing_fence == attempt.fence,
                    CustomImportPack.stream_slot == 1,
                )
            )
            assert root_pack is not None
            session.add(
                CustomImportRejection(
                    execution_id=attempt.execution_id,
                    rejection_ordinal=0,
                    dataset_id=graph.dataset_id,
                    definition_revision_id=graph.definition_revision_id,
                    schema_revision_id=graph.schema_revision_id,
                    pack_id=root_pack.pack_id,
                    root_key_sha256=None,
                    canonical_root_key=None,
                    collection_slot=None,
                    source_ordinal=None,
                    code="synthetic_rejection",
                    field_slot=None,
                    canonical_evidence='{"reason":"fence-one"}',
                    producing_fence=attempt.fence,
                    producing_token_sha256=lease_digest(attempt.token),
                )
            )
            await session.flush()
    return attempt, family, root_pack.pack_id


async def _take_over_attempt(case, attempt: GenerationAttempt):
    """Expire one live lease and return its new holder without changing its execution."""

    async with case.sessions() as session:
        async with session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == attempt.execution_id)
                .values(expires_at=func.clock_timestamp() - text("interval '1 second'"))
            )
    async with case.sessions() as session:
        async with session.begin():
            takeover = await resume_execution(
                session,
                execution_id=attempt.execution_id,
                token=_RECOVERY_CREDENTIAL,
            )
            assert takeover is not None
            assert takeover.fence == attempt.fence + 1
            return takeover


async def _assert_stale_pack_write_rejected(session, graph: PublicationGraph, attempt: GenerationAttempt) -> None:
    """Prove the former holder cannot append a pack after its takeover."""

    with pytest.raises(DBAPIError, match="custom_import_output_producing_lease_lost"):
        async with session.begin_nested():
            session.add(
                CustomImportPack(
                    execution_id=attempt.execution_id,
                    dataset_id=graph.dataset_id,
                    definition_revision_id=graph.definition_revision_id,
                    schema_revision_id=graph.schema_revision_id,
                    stream_slot=1,
                    pack_ordinal=9,
                    capture_bundle_id=graph.capture_bundle_id,
                    record_count=1,
                    pack_sha256=digest(f"stale-pack:{graph.dataset_id}"),
                    producing_fence=attempt.fence,
                    producing_token_sha256=lease_digest(attempt.token),
                )
            )
            await session.flush()


async def _assert_stale_rejection_write_rejected(
    session,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    root_pack_id: int,
) -> None:
    """Prove the former holder cannot append a rejection after its takeover."""

    with pytest.raises(DBAPIError, match="custom_import_output_producing_lease_lost"):
        async with session.begin_nested():
            session.add(
                CustomImportRejection(
                    execution_id=attempt.execution_id,
                    rejection_ordinal=9,
                    dataset_id=graph.dataset_id,
                    definition_revision_id=graph.definition_revision_id,
                    schema_revision_id=graph.schema_revision_id,
                    pack_id=root_pack_id,
                    root_key_sha256=None,
                    canonical_root_key=None,
                    collection_slot=None,
                    source_ordinal=None,
                    code="stale_rejection",
                    field_slot=None,
                    canonical_evidence='{"reason":"stale"}',
                    producing_fence=attempt.fence,
                    producing_token_sha256=lease_digest(attempt.token),
                )
            )
            await session.flush()


async def _assert_stale_root_revision_rejected(
    session,
    graph: PublicationGraph,
    root_record: CustomImportRootRecord,
    root_pack_id: int,
) -> None:
    """Prove a graph row cannot reuse the prior fence's pack authority."""

    with pytest.raises(DBAPIError, match="custom_import_output_producing_lease_lost"):
        async with session.begin_nested():
            session.add(
                CustomImportRootRevision(
                    dataset_id=graph.dataset_id,
                    definition_revision_id=graph.definition_revision_id,
                    schema_revision_id=graph.schema_revision_id,
                    root_record_id=root_record.root_record_id,
                    pack_id=root_pack_id,
                    source_ordinal=9,
                    canonical_payload='{"payload":"stale"}',
                    payload_sha256=digest(f"stale-root-payload:{graph.dataset_id}"),
                )
            )
            await session.flush()


async def _assert_stale_attempt_writes_fail(
    case,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    root_pack_id: int,
) -> None:
    """Exercise stale pack, rejection, and transitive graph inserts after takeover."""

    async with case.sessions() as session:
        async with session.begin():
            root_record = CustomImportRootRecord(
                dataset_id=graph.dataset_id,
                key_contract_sha256=digest(f"stale-root-contract:{graph.dataset_id}"),
                canonical_logical_key='{"root":"stale"}',
                logical_key_sha256=digest(f"stale-root-key:{graph.dataset_id}"),
            )
            session.add(root_record)
            await session.flush()
            await _assert_stale_pack_write_rejected(session, graph, attempt)
            await _assert_stale_rejection_write_rejected(session, graph, attempt, root_pack_id)
            await _assert_stale_root_revision_rejected(session, graph, root_record, root_pack_id)


async def _seal_second_fence_without_stale_material(
    case,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    takeover,
    family,
):
    """Prove a recovered candidate cannot reference or hash the prior fence output."""

    async with case.sessions() as session:
        async with session.begin():
            stale_candidate = await session.get(CustomImportGeneration, attempt.generation_id)
            assert stale_candidate is not None
            recovered_candidate = CustomImportGeneration(
                dataset_id=graph.dataset_id,
                definition_revision_id=graph.definition_revision_id,
                schema_revision_id=graph.schema_revision_id,
                execution_id=attempt.execution_id,
                capture_bundle_id=graph.capture_bundle_id,
                source_bundle_sha256=stale_candidate.source_bundle_sha256,
                candidate_sha256=stale_candidate.candidate_sha256,
                root_count=0,
                family_count=0,
                producing_fence=takeover.fence,
                producing_token_sha256=lease_digest(_RECOVERY_CREDENTIAL),
            )
            session.add(recovered_candidate)
            await session.flush()
            with pytest.raises(DBAPIError, match="custom_import_generation_family_authority_mismatch"):
                async with session.begin_nested():
                    session.add(
                        CustomImportGenerationFamily(
                            generation_id=recovered_candidate.generation_id,
                            dataset_id=graph.dataset_id,
                            definition_revision_id=graph.definition_revision_id,
                            schema_revision_id=graph.schema_revision_id,
                            root_record_id=family.root_record_id,
                            family_revision_id=family.family_revision_id,
                        )
                    )
                    await session.flush()
            recovered = await seal_generation(
                session,
                dataset_id=graph.dataset_id,
                generation_id=recovered_candidate.generation_id,
                lease_fence=takeover.fence,
                lease_token=_RECOVERY_CREDENTIAL,
            )
            baseline = await session.get(CustomImportGenerationSeal, graph.first_generation_id)
            assert baseline is not None
            assert recovered.materialization_sha256 == bytes(baseline.materialization_sha256).hex()
            assert recovered.effective_output_sha256 == bytes(baseline.effective_output_sha256).hex()


@pytest.mark.asyncio
async def test_takeover_fences_stale_output_and_second_seal():
    """Post-takeover output is fenced at every graph entry and seal materialization boundary."""

    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        attempt, family, root_pack_id = await _seed_fence_one_output(case, graph)
        takeover = await _take_over_attempt(case, attempt)
        await _assert_stale_attempt_writes_fail(case, graph, attempt, root_pack_id)
        await _seal_second_fence_without_stale_material(case, graph, attempt, takeover, family)


async def _seed_dataset_first_attempt(case, graph: PublicationGraph) -> GenerationAttempt:
    """Create the live attempt used by the shared-identity lock-order proof."""

    async with case.sessions() as setup_session:
        async with setup_session.begin():
            return await seed_running_generation(
                setup_session,
                graph,
                suffix=f"dataset-first-{graph.dataset_id}",
                base_generation_id=None,
            )


async def _insert_shared_identity_rows(session, graph: PublicationGraph) -> None:
    """Insert direct-dataset identities before the worker appends output."""

    session.add_all(
        (
            CustomImportSchemaRevision(
                dataset_id=graph.dataset_id,
                revision_number=2,
                canonical_schema='{"schema":"dataset-first"}',
                schema_sha256=digest(f"dataset-first-schema:{graph.dataset_id}"),
            ),
            CustomImportRootRecord(
                dataset_id=graph.dataset_id,
                key_contract_sha256=digest(f"dataset-first-contract:{graph.dataset_id}"),
                canonical_logical_key='{"root":"dataset-first"}',
                logical_key_sha256=digest(f"dataset-first-key:{graph.dataset_id}"),
            ),
            CustomImportEntityBinding(
                dataset_id=graph.dataset_id,
                adapter_id="synthetic",
                canonical_value=f"dataset-first-{graph.dataset_id}",
                value_sha256=digest(f"dataset-first-binding:{graph.dataset_id}"),
            ),
        )
    )
    await session.flush()


async def _start_queued_activation(case, graph: PublicationGraph):
    """Start an activation and wait until it is queued behind the worker lock."""

    backend_pid_ready = asyncio.get_running_loop().create_future()
    contender = asyncio.create_task(
        _contending_activation(
            case,
            dataset_id=graph.dataset_id,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
            backend_pid_ready=backend_pid_ready,
        )
    )
    backend_pid = await asyncio.wait_for(backend_pid_ready, timeout=2)
    await _wait_for_backend_lock(case, backend_pid)
    return contender


async def _append_dataset_first_pack(session, graph: PublicationGraph, attempt: GenerationAttempt) -> None:
    """Append the guarded pack that must not deadlock with queued finality."""

    session.add(
        CustomImportPack(
            execution_id=attempt.execution_id,
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            stream_slot=1,
            pack_ordinal=0,
            capture_bundle_id=graph.capture_bundle_id,
            record_count=0,
            pack_sha256=digest(f"dataset-first-pack:{graph.dataset_id}"),
            producing_fence=attempt.fence,
            producing_token_sha256=lease_digest(attempt.token),
        )
    )
    await session.flush()


async def _run_dataset_first_worker(case, graph: PublicationGraph, attempt: GenerationAttempt) -> int:
    """Commit a worker's identity and output writes ahead of queued finality."""

    worker_session = case.sessions()
    worker_transaction = await worker_session.begin()
    contender = None
    try:
        await _insert_shared_identity_rows(worker_session, graph)
        contender = await _start_queued_activation(case, graph)
        await _append_dataset_first_pack(worker_session, graph, attempt)
        await worker_transaction.commit()
        activated = await asyncio.wait_for(contender, timeout=5)
        return activated.committed_pointer_version
    finally:
        if worker_transaction.is_active:
            await worker_transaction.rollback()
        await _cancel_pending_task(contender)
        await worker_session.close()


@pytest.mark.asyncio
async def test_shared_identity_inserts_lock_dataset_before_output_and_finality():
    """Direct-dataset rows cannot cause an FK-share-to-update deadlock."""

    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        attempt = await _seed_dataset_first_attempt(case, graph)
        committed_pointer_version = await _run_dataset_first_worker(case, graph, attempt)

    assert committed_pointer_version == 1


@pytest.mark.asyncio
async def test_seal_renews_exact_lease_before_a_slow_finality_scan(monkeypatch):
    """A scan that outlives the normal lease still seals under its bounded seal lease."""

    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.sessions() as session:
            async with session.begin():
                attempt = await seed_running_generation(
                    session,
                    graph,
                    suffix=f"seal-window-{graph.dataset_id}",
                    base_generation_id=None,
                )
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == attempt.execution_id)
                    .values(expires_at=func.clock_timestamp() + text("interval '1 second'"))
                )

        materialization = publication_module._materialization

        async def delayed_materialization(session, generation):
            await asyncio.sleep(1.15)
            return await materialization(session, generation)

        monkeypatch.setattr(publication_module, "_materialization", delayed_materialization)
        async with case.sessions() as session:
            async with session.begin():
                receipt = await seal_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    generation_id=attempt.generation_id,
                    lease_fence=attempt.fence,
                    lease_token=attempt.token,
                )
                assert receipt.replayed is False
                assert receipt.generation_id == attempt.generation_id
        async with case.sessions() as session:
            assert (
                await session.scalar(
                    select(CustomImportExecution.state).where(
                        CustomImportExecution.execution_id == attempt.execution_id
                    )
                )
                == "completed"
            )


async def _seed_competing_live_attempts(case, graph: PublicationGraph) -> tuple[GenerationAttempt, GenerationAttempt]:
    """Create the sealing and heartbeat attempts used by the starvation proof."""

    async with case.sessions() as session:
        async with session.begin():
            sealing_attempt = await seed_running_generation(
                session,
                graph,
                suffix=f"slow-seal-{graph.dataset_id}",
                base_generation_id=None,
            )
            heartbeat_attempt = await seed_running_generation(
                session,
                graph,
                suffix=f"heartbeat-during-seal-{graph.dataset_id}",
                base_generation_id=None,
            )
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == heartbeat_attempt.execution_id)
                .values(expires_at=func.clock_timestamp() + text("interval '1 second'"))
            )
    return sealing_attempt, heartbeat_attempt


def _delayed_materialization_for_attempt(
    original_materialization,
    sealing_generation_id: int,
    scan_started: asyncio.Event,
    release_scan: asyncio.Event,
):
    """Return a materialization wrapper that stops only the selected seal."""

    async def delayed_materialization(session, generation):
        if generation.generation_id == sealing_generation_id:
            scan_started.set()
            await release_scan.wait()
        return await original_materialization(session, generation)

    return delayed_materialization


async def _seal_in_own_transaction(case, graph: PublicationGraph, attempt: GenerationAttempt):
    """Seal one candidate in a separate transaction for a concurrent test."""

    async with case.sessions() as session:
        async with session.begin():
            return await seal_generation(
                session,
                dataset_id=graph.dataset_id,
                generation_id=attempt.generation_id,
                lease_fence=attempt.fence,
                lease_token=attempt.token,
            )


async def _renew_heartbeat_during_scan(case, attempt: GenerationAttempt) -> None:
    """Renew a competing attempt while the unrelated seal remains frozen."""

    async with case.sessions() as session:
        async with session.begin():
            heartbeat = await asyncio.wait_for(
                heartbeat_execution(
                    session,
                    execution_id=attempt.execution_id,
                    fence=attempt.fence,
                    token=attempt.token,
                    lease_seconds=60,
                ),
                timeout=0.5,
            )
            assert heartbeat is not None
            assert heartbeat.execution_id == attempt.execution_id


async def _assert_heartbeat_lease_is_live(case, attempt: GenerationAttempt) -> None:
    """Assert a competing heartbeat retained a future PostgreSQL expiry."""

    async with case.sessions() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        expires_at = await session.scalar(
            select(CustomImportLease.expires_at).where(CustomImportLease.execution_id == attempt.execution_id)
        )
    assert expires_at is not None
    assert expires_at > now


async def _finish_delayed_seal(seal_task, release_scan: asyncio.Event, generation_id: int) -> None:
    """Release the delayed seal and preserve cleanup if it raises or times out."""

    release_scan.set()
    try:
        receipt = await asyncio.wait_for(seal_task, timeout=5)
        assert receipt.generation_id == generation_id
    finally:
        await _cancel_pending_task(seal_task)


@pytest.mark.asyncio
async def test_slow_seal_does_not_starve_another_execution_heartbeat(monkeypatch):
    """An unrelated live execution renews while a same-dataset seal is frozen."""

    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        sealing_attempt, heartbeat_attempt = await _seed_competing_live_attempts(case, graph)
        scan_started = asyncio.Event()
        release_scan = asyncio.Event()
        delayed_materialization = _delayed_materialization_for_attempt(
            publication_module._materialization,
            sealing_attempt.generation_id,
            scan_started,
            release_scan,
        )
        monkeypatch.setattr(publication_module, "_materialization", delayed_materialization)
        seal_task = asyncio.create_task(_seal_in_own_transaction(case, graph, sealing_attempt))
        try:
            await asyncio.wait_for(scan_started.wait(), timeout=2)
            await _renew_heartbeat_during_scan(case, heartbeat_attempt)
            await asyncio.sleep(1.1)
            await _assert_heartbeat_lease_is_live(case, heartbeat_attempt)
        finally:
            await _finish_delayed_seal(seal_task, release_scan, sealing_attempt.generation_id)


async def _finality_scan_timeout_during_success(session, now: dt.datetime) -> str:
    """Run a successful scan window and return its temporary SQL timeout."""

    async with publication_module._finality_scan_window(
        session,
        now=now,
        expires_at=now + dt.timedelta(seconds=10),
    ):
        await publication_module._prepare_bounded_materialization_statement(session)
        timeout_during_scan = await session.scalar(select(func.current_setting("statement_timeout")))
        assert isinstance(timeout_during_scan, str)
        return timeout_during_scan


async def _run_failing_finality_scan_window(session, now: dt.datetime) -> None:
    """Run a failing scan window so its restoration path is exercised."""

    with pytest.raises(PublicationConflict, match="scan failed"):
        async with publication_module._finality_scan_window(
            session,
            now=now,
            expires_at=now + dt.timedelta(seconds=10),
        ):
            await publication_module._prepare_bounded_materialization_statement(session)
            raise PublicationConflict("scan failed")


@pytest.mark.asyncio
async def test_finality_scan_window_restores_the_callers_statement_timeout():
    """A bounded graph scan must not leak its local SQL budget to finalization."""

    async with isolated_publication_case() as case:
        async with case.sessions() as session:
            async with session.begin():
                await session.execute(text("SET LOCAL statement_timeout = '1234ms'"))
                timeout_before_scan = await session.scalar(select(func.current_setting("statement_timeout")))
                assert isinstance(timeout_before_scan, str)
                now = await publication_module._database_now(session)
                timeout_during_scan = await _finality_scan_timeout_during_success(session, now)
                assert timeout_during_scan != timeout_before_scan
                assert await session.scalar(select(func.current_setting("statement_timeout"))) == timeout_before_scan
                await _run_failing_finality_scan_window(session, now)
                assert await session.scalar(select(func.current_setting("statement_timeout"))) == timeout_before_scan


async def _assert_direct_generation_seal_rejected(
    session,
    case,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    pattern: str,
) -> None:
    """Exercise the database seal guard without trusting the application path."""

    generation = await session.get(CustomImportGeneration, attempt.generation_id)
    assert generation is not None
    with pytest.raises(DBAPIError, match=pattern):
        async with session.begin_nested():
            await session.execute(
                text(
                    f"INSERT INTO {_table(case, 'custom_import_generation_seal')} "
                    "(generation_id, dataset_id, definition_revision_id, schema_revision_id, "
                    "execution_id, capture_bundle_id, seal_contract, sealing_fence, "
                    "sealing_token_sha256, root_count, family_count, generation_family_count, "
                    "family_child_count, winner_count, profile_count, root_scalar_count, "
                    "child_scalar_count, materialization_sha256, effective_output_sha256) "
                    "VALUES (:generation_id, :dataset_id, :definition_revision_id, "
                    ":schema_revision_id, :execution_id, :capture_bundle_id, :seal_contract, "
                    ":sealing_fence, :sealing_token_sha256, 0, 0, 0, 0, 0, 0, 0, 0, "
                    ":materialization_sha256, :effective_output_sha256)"
                ),
                {
                    "generation_id": attempt.generation_id,
                    "dataset_id": graph.dataset_id,
                    "definition_revision_id": graph.definition_revision_id,
                    "schema_revision_id": graph.schema_revision_id,
                    "execution_id": attempt.execution_id,
                    "capture_bundle_id": generation.capture_bundle_id,
                    "seal_contract": "custom-import-generation-seal/v1",
                    "sealing_fence": attempt.fence,
                    "sealing_token_sha256": lease_digest(attempt.token),
                    "materialization_sha256": digest(f"direct-seal-materialization:{attempt.generation_id}"),
                    "effective_output_sha256": digest(f"direct-seal-effective:{attempt.generation_id}"),
                },
            )


async def _seed_incomplete_capture_attempt(session, graph: PublicationGraph) -> GenerationAttempt:
    """Retain a candidate whose bundle omits one definition stream capture."""

    source_bundle = await session.get(CustomImportCaptureBundle, graph.capture_bundle_id)
    source_capture = await session.scalar(
        select(CustomImportCapture)
        .where(
            CustomImportCapture.capture_bundle_id == graph.capture_bundle_id,
            CustomImportCapture.stream_slot == 1,
        )
        .order_by(CustomImportCapture.stream_slot)
    )
    assert source_bundle is not None
    assert source_capture is not None
    incomplete_bundle = CustomImportCaptureBundle(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        snapshot_token=f"incomplete-capture-{graph.dataset_id}",
        snapshot_token_sha256=digest(f"incomplete-capture:{graph.dataset_id}"),
        canonical_manifest=source_bundle.canonical_manifest,
        manifest_sha256=source_bundle.manifest_sha256,
        stream_count=source_bundle.stream_count,
    )
    session.add(incomplete_bundle)
    await session.flush()
    session.add(
        CustomImportCapture(
            capture_bundle_id=incomplete_bundle.capture_bundle_id,
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            stream_slot=source_capture.stream_slot,
            content_sha256=source_capture.content_sha256,
            byte_count=source_capture.byte_count,
            canonical_manifest=source_capture.canonical_manifest,
            manifest_sha256=source_capture.manifest_sha256,
        )
    )
    await session.flush()
    execution_id, generation_id, token = await _seed_capture_no_change_execution(session, graph, incomplete_bundle)
    return GenerationAttempt(
        execution_id=execution_id,
        generation_id=generation_id,
        token=token,
        fence=1,
    )


@pytest.mark.asyncio
async def test_seal_requires_exact_capture_coverage_in_app_and_database():
    """Bundle count, definition streams, and captures must agree before finality."""

    async with isolated_publication_case() as case:
        async with case.sessions() as session:
            async with session.begin():
                graph = await seed_publication_graph(session)
                attempt = await _seed_incomplete_capture_attempt(session, graph)
                with pytest.raises(PublicationConflict, match="exactly cover the definition streams"):
                    await seal_generation(
                        session,
                        dataset_id=graph.dataset_id,
                        generation_id=attempt.generation_id,
                        lease_fence=attempt.fence,
                        lease_token=attempt.token,
                    )
                await _assert_direct_generation_seal_rejected(
                    session,
                    case,
                    graph,
                    attempt,
                    "custom_import_generation_seal_capture_incomplete",
                )


@pytest.mark.asyncio
async def test_seal_rejects_child_parent_identity_mismatch_in_app_and_database():
    """A family child must carry the selected root's canonical key and hash."""

    async with isolated_publication_case() as case:
        async with case.sessions() as session:
            async with session.begin():
                graph = await seed_publication_graph(session)
                attempt = await seed_running_generation(
                    session,
                    graph,
                    suffix=f"parent-mismatch-{graph.dataset_id}",
                    base_generation_id=None,
                    root_count=1,
                    family_count=1,
                )
                family = await seed_family_material(
                    session,
                    graph,
                    attempt,
                    FamilyMaterialSpec(
                        suffix=f"parent-mismatch-{graph.dataset_id}",
                        child_keys=("parent-mismatch-child",),
                        parent_mismatch=True,
                    ),
                )
                await attach_generation_family(session, graph, attempt, family)
                with pytest.raises(PublicationConflict, match="child parent identity"):
                    await seal_generation(
                        session,
                        dataset_id=graph.dataset_id,
                        generation_id=attempt.generation_id,
                        lease_fence=attempt.fence,
                        lease_token=attempt.token,
                    )
                await _assert_direct_generation_seal_rejected(
                    session,
                    case,
                    graph,
                    attempt,
                    "custom_import_generation_seal_child_parent_mismatch",
                )


async def _seed_winner_context_candidate(case) -> tuple[PublicationGraph, GenerationAttempt, object]:
    """Create one family candidate with a context child for winner validation."""

    async with case.sessions() as session:
        async with session.begin():
            graph = await seed_publication_graph(session)
            attempt = await seed_running_generation(
                session,
                graph,
                suffix=f"winner-context-{graph.dataset_id}",
                base_generation_id=None,
                root_count=1,
                family_count=1,
            )
            family = await seed_family_material(
                session,
                graph,
                attempt,
                FamilyMaterialSpec(
                    suffix=f"winner-context-{graph.dataset_id}",
                    child_keys=("winner-context-child",),
                ),
            )
            await attach_generation_family(session, graph, attempt, family)
    return graph, attempt, family


def _invalid_winner_fields_by_name(graph: PublicationGraph, attempt: GenerationAttempt, family) -> dict[str, object]:
    """Return a winner payload whose collection slot contradicts its profile."""

    return {
        "generation_id": attempt.generation_id,
        "dataset_id": graph.dataset_id,
        "definition_revision_id": graph.definition_revision_id,
        "schema_revision_id": graph.schema_revision_id,
        "profile_slot": 1,
        "entity_binding_id": family.entity_binding_id,
        "family_revision_id": family.family_revision_id,
        "context_collection_slot": 1,
        "context_key_sha256": digest(f"winner-context:{graph.dataset_id}"),
        "context_child_revision_id": family.child_revision_ids[0],
    }


async def _assert_database_winner_context_rejected(case, winner_fields_by_name: dict[str, object]) -> None:
    """Prove the direct PostgreSQL winner guard rejects the mismatched slot."""

    async with case.sessions() as session:
        async with session.begin():
            with pytest.raises(DBAPIError, match="custom_import_winner_profile_context_mismatch"):
                async with session.begin_nested():
                    session.add(CustomImportWinner(**winner_fields_by_name))
                    await session.flush()


async def _assert_application_winner_context_rejected(
    session,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    winner_fields_by_name: dict[str, object],
) -> None:
    """Prove materialization rejects a mismatch when the direct insert guard is disabled."""

    nested = await session.begin_nested()
    try:
        session.add(CustomImportWinner(**winner_fields_by_name))
        await session.flush()
        with pytest.raises(PublicationConflict, match="winner context collection"):
            await seal_generation(
                session,
                dataset_id=graph.dataset_id,
                generation_id=attempt.generation_id,
                lease_fence=attempt.fence,
                lease_token=attempt.token,
            )
    finally:
        if nested.is_active:
            await nested.rollback()


async def _assert_application_winner_context_guard(
    case,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    winner_fields_by_name: dict[str, object],
) -> None:
    """Temporarily disable the trigger to exercise the application materialization guard."""

    winner_table = _table(case, "custom_import_winner")
    async with case.sessions() as session:
        async with session.begin():
            await session.execute(
                text(f"ALTER TABLE {winner_table} DISABLE TRIGGER custom_import_winner_sealed_append_guard")
            )
            try:
                await _assert_application_winner_context_rejected(session, graph, attempt, winner_fields_by_name)
            finally:
                await session.execute(
                    text(f"ALTER TABLE {winner_table} ENABLE TRIGGER custom_import_winner_sealed_append_guard")
                )


@pytest.mark.asyncio
async def test_winner_context_collection_must_match_its_profile_in_app_and_database():
    """The selection profile is the sole authority for a winner context slot."""

    async with isolated_publication_case() as case:
        graph, attempt, family = await _seed_winner_context_candidate(case)
        winner_fields_by_name = _invalid_winner_fields_by_name(graph, attempt, family)
        await _assert_database_winner_context_rejected(case, winner_fields_by_name)
        await _assert_application_winner_context_guard(case, graph, attempt, winner_fields_by_name)


async def _seed_counted_family(case, graph: PublicationGraph):
    """Attach and seal one one-child family for post-seal trigger checks."""

    async with case.sessions() as session:
        async with session.begin():
            attempt = await seed_running_generation(
                session,
                graph,
                suffix=f"counted-family-{graph.dataset_id}",
                base_generation_id=None,
                root_count=1,
                family_count=1,
            )
            family = await seed_family_material(
                session,
                graph,
                attempt,
                FamilyMaterialSpec(
                    suffix=f"counted-family-{graph.dataset_id}",
                    child_keys=("guarded-child",),
                    include_root_winner=True,
                ),
            )
            await attach_generation_family(session, graph, attempt, family)
            receipt = await seal_generation(
                session,
                dataset_id=graph.dataset_id,
                generation_id=attempt.generation_id,
                lease_fence=attempt.fence,
                lease_token=attempt.token,
            )
            assert (receipt.root_count, receipt.family_count, receipt.generation_family_count) == (1, 1, 1)
    return attempt, family


async def _assert_nested_statement_rejected(session, statement, parameters, pattern: str) -> None:
    """Execute one statement in a savepoint and assert its trigger failure."""

    with pytest.raises(DBAPIError, match=pattern):
        async with session.begin_nested():
            await session.execute(statement, parameters)


async def _assert_generation_family_append_rejected(session, graph: PublicationGraph, attempt, family) -> None:
    """Prove a sealed generation cannot gain another family link."""

    with pytest.raises(DBAPIError, match="custom_import_sealed_append"):
        async with session.begin_nested():
            session.add(
                CustomImportGenerationFamily(
                    generation_id=attempt.generation_id,
                    dataset_id=graph.dataset_id,
                    definition_revision_id=graph.definition_revision_id,
                    schema_revision_id=graph.schema_revision_id,
                    root_record_id=family.root_record_id,
                    family_revision_id=family.family_revision_id,
                )
            )
            await session.flush()


def _family_append_rows(graph: PublicationGraph, attempt, family):
    """Return sealed family-owned tables and their minimal insert parameters."""

    child_revision_id = family.child_revision_ids[0]
    return (
        (
            "custom_import_winner",
            "generation_id, dataset_id",
            {"generation_id": attempt.generation_id, "dataset_id": graph.dataset_id},
        ),
        (
            "custom_import_family_child",
            "family_revision_id, dataset_id",
            {"family_revision_id": family.family_revision_id, "dataset_id": graph.dataset_id},
        ),
        (
            "custom_import_root_scalar",
            "root_revision_id, dataset_id",
            {"root_revision_id": family.root_revision_id, "dataset_id": graph.dataset_id},
        ),
        (
            "custom_import_child_scalar",
            "child_revision_id, dataset_id",
            {"child_revision_id": child_revision_id, "dataset_id": graph.dataset_id},
        ),
    )


def _definition_append_rows(graph: PublicationGraph):
    """Return definition-owned tables protected once a generation is sealed."""

    return (
        (
            "custom_import_field",
            "dataset_id, schema_revision_id",
            {"dataset_id": graph.dataset_id, "schema_revision_id": graph.schema_revision_id},
        ),
        (
            "custom_import_child_collection",
            "dataset_id, schema_revision_id",
            {"dataset_id": graph.dataset_id, "schema_revision_id": graph.schema_revision_id},
        ),
        (
            "custom_import_source_stream",
            "dataset_id, definition_revision_id, schema_revision_id",
            {
                "dataset_id": graph.dataset_id,
                "definition_revision_id": graph.definition_revision_id,
                "schema_revision_id": graph.schema_revision_id,
            },
        ),
        (
            "custom_import_field_alias",
            "dataset_id, definition_revision_id, schema_revision_id",
            {
                "dataset_id": graph.dataset_id,
                "definition_revision_id": graph.definition_revision_id,
                "schema_revision_id": graph.schema_revision_id,
            },
        ),
        (
            "custom_import_selection_profile",
            "dataset_id, definition_revision_id, schema_revision_id",
            {
                "dataset_id": graph.dataset_id,
                "definition_revision_id": graph.definition_revision_id,
                "schema_revision_id": graph.schema_revision_id,
            },
        ),
    )


def _execution_append_rows(graph: PublicationGraph, attempt):
    """Return execution-owned tables protected by the generation seal."""

    return (
        (
            "custom_import_pack",
            "execution_id, dataset_id",
            {"execution_id": attempt.execution_id, "dataset_id": graph.dataset_id},
        ),
        (
            "custom_import_rejection",
            "execution_id, dataset_id",
            {"execution_id": attempt.execution_id, "dataset_id": graph.dataset_id},
        ),
    )


async def _assert_sealed_append_rejected(session, case, table_name: str, columns: str, parameters) -> None:
    """Try a minimal insert into one sealed dependency table."""

    placeholders = ", ".join(f":{column}" for column in parameters)
    await _assert_nested_statement_rejected(
        session,
        text(f"INSERT INTO {_table(case, table_name)} ({columns}) VALUES ({placeholders})"),
        parameters,
        "custom_import_sealed_append",
    )


async def _assert_capture_changes_rejected(session, case, graph: PublicationGraph) -> None:
    """Prove both append and in-place mutations fail after a capture is sealed."""

    capture_table = _table(case, "custom_import_capture")
    duplicate_capture = text(
        f"INSERT INTO {capture_table} "
        "(capture_bundle_id, dataset_id, definition_revision_id, schema_revision_id, stream_slot, "
        "content_sha256, byte_count, canonical_manifest, manifest_sha256) "
        f"SELECT capture_bundle_id, dataset_id, definition_revision_id, schema_revision_id, stream_slot, "
        f"content_sha256, byte_count, canonical_manifest, manifest_sha256 FROM {capture_table} "
        "WHERE capture_bundle_id = :capture_bundle_id AND stream_slot = 1"
    )
    capture_parameter_map = {"capture_bundle_id": graph.capture_bundle_id}
    await _assert_nested_statement_rejected(
        session,
        duplicate_capture,
        capture_parameter_map,
        "custom_import_sealed_append",
    )
    await _assert_nested_statement_rejected(
        session,
        text(
            f"UPDATE {capture_table} SET byte_count = byte_count "
            "WHERE capture_bundle_id = :capture_bundle_id AND stream_slot = 1"
        ),
        capture_parameter_map,
        "custom_import_immutable_row",
    )
    await _assert_nested_statement_rejected(
        session,
        text(f"DELETE FROM {capture_table} WHERE capture_bundle_id = :capture_bundle_id AND stream_slot = 1"),
        capture_parameter_map,
        "custom_import_immutable_row",
    )


async def _assert_post_seal_changes_rejected(case, graph: PublicationGraph, attempt, family) -> None:
    """Exercise all schema guards that protect a sealed generation's inputs."""

    async with case.sessions() as session:
        async with session.begin():
            await _assert_generation_family_append_rejected(session, graph, attempt, family)
            await session.execute(text("SET LOCAL search_path TO pg_catalog"))
            await _assert_capture_changes_rejected(session, case, graph)
            for table_name, columns, parameters in (
                *_family_append_rows(graph, attempt, family),
                *_definition_append_rows(graph),
                *_execution_append_rows(graph, attempt),
            ):
                await _assert_sealed_append_rejected(session, case, table_name, columns, parameters)


@pytest.mark.asyncio
async def test_sealed_generation_counts_material_and_rejects_changes():
    """A seal records prepared material and freezes all protected dependencies."""

    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        attempt, family = await _seed_counted_family(case, graph)
        await _assert_post_seal_changes_rejected(case, graph, attempt, family)


async def _seal_ordered_family(
    session,
    graph: PublicationGraph,
    suffix: str,
    reverse_insertion: bool,
):
    """Seed, attach, and seal one two-child family in a chosen insertion order."""

    attempt = await seed_running_generation(
        session,
        graph,
        suffix=suffix,
        base_generation_id=None,
        root_count=1,
        family_count=1,
    )
    family = await seed_family_material(
        session,
        graph,
        attempt,
        FamilyMaterialSpec(
            suffix="ordered-family",
            child_keys=("alpha", "beta"),
            reverse_insertion=reverse_insertion,
            include_root_winner=False,
        ),
    )
    await attach_generation_family(session, graph, attempt, family)
    winner_keys = ("ordered-winner-alpha", "ordered-winner-beta")
    if reverse_insertion:
        winner_keys = tuple(reversed(winner_keys))
    session.add_all(
        CustomImportWinner(
            generation_id=attempt.generation_id,
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            profile_slot=1,
            entity_binding_id=family.entity_binding_id,
            family_revision_id=family.family_revision_id,
            context_collection_slot=0,
            context_key_sha256=digest(winner_key),
            context_child_revision_id=None,
        )
        for winner_key in winner_keys
    )
    await session.flush()
    return await seal_generation(
        session,
        dataset_id=graph.dataset_id,
        generation_id=attempt.generation_id,
        lease_fence=attempt.fence,
        lease_token=attempt.token,
    )


async def _seed_contextual_winner_candidate(
    session,
    graph: PublicationGraph,
    *,
    base_generation_id: int | None,
    prior_material: FamilyMaterial | None = None,
    root_source_ordinal: int = 0,
    child_source_ordinals: tuple[int, ...] = (0,),
) -> tuple[GenerationAttempt, FamilyMaterial]:
    """Create one nonempty contextual winner, optionally reusing its stable identities."""

    suffix = "contextual-winner-family"
    attempt = await seed_running_generation(
        session,
        graph,
        suffix=("contextual-winner-base" if prior_material is None else "contextual-winner-candidate"),
        base_generation_id=base_generation_id,
        root_count=1,
        family_count=1,
    )
    material = await seed_family_material(
        session,
        graph,
        attempt,
        FamilyMaterialSpec(
            suffix=suffix,
            child_keys=("contextual-winner-child",),
            root_record_id=None if prior_material is None else prior_material.root_record_id,
            entity_binding_id=None if prior_material is None else prior_material.entity_binding_id,
            root_source_ordinal=root_source_ordinal,
            child_source_ordinals=child_source_ordinals,
        ),
    )
    await attach_generation_family(session, graph, attempt, material)
    session.add(
        CustomImportWinner(
            generation_id=attempt.generation_id,
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            profile_slot=1,
            entity_binding_id=material.entity_binding_id,
            family_revision_id=material.family_revision_id,
            context_collection_slot=1,
            context_key_sha256=digest("contextual-winner-context"),
            context_child_revision_id=material.child_revision_ids[0],
        )
    )
    await session.flush()
    return attempt, material


@pytest.mark.parametrize(
    ("candidate_root_ordinal", "candidate_child_ordinals"),
    (
        (0, (0,)),
        (7, (0,)),
        (0, (9,)),
        (7, (9,)),
    ),
    ids=("allocation-only", "root-reordered", "child-reordered", "root-and-child-reordered"),
)
@pytest.mark.asyncio
async def test_contextual_winner_provenance_does_not_prevent_no_change(
    candidate_root_ordinal: int,
    candidate_child_ordinals: tuple[int, ...],
):
    """Equivalent output ignores allocation IDs and source-position provenance."""

    async with isolated_publication_case() as case:
        async with case.sessions() as session:
            async with session.begin():
                graph = await seed_publication_graph(session, context_collection_slot=1)
                base_attempt, base_material = await _seed_contextual_winner_candidate(
                    session,
                    graph,
                    base_generation_id=None,
                )
                base_seal = await seal_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    generation_id=base_attempt.generation_id,
                    lease_fence=base_attempt.fence,
                    lease_token=base_attempt.token,
                )
                await activate_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    target_generation_id=base_attempt.generation_id,
                    expected_generation_id=None,
                    expected_pointer_version=0,
                )

        async with case.sessions() as session:
            async with session.begin():
                candidate_attempt, candidate_material = await _seed_contextual_winner_candidate(
                    session,
                    graph,
                    base_generation_id=base_attempt.generation_id,
                    prior_material=base_material,
                    root_source_ordinal=candidate_root_ordinal,
                    child_source_ordinals=candidate_child_ordinals,
                )
                assert candidate_material.child_revision_ids != base_material.child_revision_ids

                receipt = await record_no_change(
                    session,
                    dataset_id=graph.dataset_id,
                    execution_id=candidate_attempt.execution_id,
                    expected_generation_id=base_attempt.generation_id,
                    expected_pointer_version=1,
                    candidate_generation_id=candidate_attempt.generation_id,
                    lease_fence=candidate_attempt.fence,
                    lease_token=candidate_attempt.token,
                )
                candidate_seal = await session.get(
                    CustomImportGenerationSeal,
                    candidate_attempt.generation_id,
                )
                assert candidate_seal is not None
                candidate_materialization = candidate_seal.materialization_sha256.hex()
                if candidate_root_ordinal == 0 and candidate_child_ordinals == (0,):
                    assert candidate_materialization == base_seal.materialization_sha256
                else:
                    assert candidate_materialization != base_seal.materialization_sha256
                assert candidate_seal.effective_output_sha256.hex() == base_seal.effective_output_sha256
                assert receipt.event_kind == "no_change"


@pytest.mark.asyncio
async def test_effective_output_digest_is_stable_for_semantic_insertion_ties():
    """Equivalent served output retains one digest despite nonsemantic insertion order."""

    async with isolated_publication_case() as case:
        async with case.sessions() as session:
            async with session.begin():
                first_graph = await seed_publication_graph(session, semantic_suffix="ordered-materialization")
                second_graph = await seed_publication_graph(session, semantic_suffix="ordered-materialization")
                first_seal = await _seal_ordered_family(session, first_graph, "ordered-first", False)
                second_seal = await _seal_ordered_family(session, second_graph, "ordered-second", True)

                assert first_seal.effective_output_sha256 == second_seal.effective_output_sha256
                assert (
                    first_seal.family_child_count,
                    first_seal.child_scalar_count,
                    first_seal.winner_count,
                    first_seal.profile_count,
                ) == (2, 4, 2, 1)
                assert (
                    second_seal.family_child_count,
                    second_seal.child_scalar_count,
                    second_seal.winner_count,
                    second_seal.profile_count,
                ) == (2, 4, 2, 1)


async def _seed_duplicate_child_candidate(session, graph: PublicationGraph, reverse_insertion: bool):
    """Create a family with equal logical child keys and different payloads."""

    attempt = await seed_running_generation(
        session,
        graph,
        suffix=f"duplicate-child-{reverse_insertion}",
        base_generation_id=None,
        root_count=1,
        family_count=1,
    )
    family = await seed_family_material(
        session,
        graph,
        attempt,
        FamilyMaterialSpec(
            suffix=f"duplicate-child-{reverse_insertion}",
            child_keys=("same-logical-key", "same-logical-key"),
            child_payloads=("first-payload", "second-payload"),
            reverse_insertion=reverse_insertion,
        ),
    )
    await attach_generation_family(session, graph, attempt, family)
    return attempt


async def _assert_duplicate_child_seal_rejected(session, case, graph: PublicationGraph, attempt) -> None:
    """Prove API and direct seal insertion both reject duplicate child keys."""

    with pytest.raises(PublicationConflict, match="duplicate logical child key"):
        await seal_generation(
            session,
            dataset_id=graph.dataset_id,
            generation_id=attempt.generation_id,
            lease_fence=attempt.fence,
            lease_token=attempt.token,
        )
    with pytest.raises(DBAPIError, match="custom_import_generation_seal_duplicate_child_key"):
        async with session.begin_nested():
            await session.execute(
                text(
                    f"INSERT INTO {_table(case, 'custom_import_generation_seal')} "
                    "(generation_id, dataset_id, definition_revision_id, schema_revision_id, "
                    "execution_id, capture_bundle_id, seal_contract, sealing_fence, "
                    "sealing_token_sha256, root_count, family_count, generation_family_count, "
                    "family_child_count, winner_count, profile_count, root_scalar_count, "
                    "child_scalar_count, materialization_sha256) "
                    "VALUES (:generation_id, :dataset_id, :definition_revision_id, "
                    ":schema_revision_id, :execution_id, :capture_bundle_id, :seal_contract, "
                    ":sealing_fence, :sealing_token_sha256, 0, 0, 0, 0, 0, 0, 0, 0, "
                    ":materialization_sha256)"
                ),
                {
                    "generation_id": attempt.generation_id,
                    "dataset_id": graph.dataset_id,
                    "definition_revision_id": graph.definition_revision_id,
                    "schema_revision_id": graph.schema_revision_id,
                    "execution_id": attempt.execution_id,
                    "capture_bundle_id": graph.capture_bundle_id,
                    "seal_contract": "custom-import-generation-seal/v1",
                    "sealing_fence": attempt.fence,
                    "sealing_token_sha256": lease_digest(attempt.token),
                    "materialization_sha256": digest("duplicate-logical-child-key"),
                },
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_insertion", (False, True), ids=("forward", "reversed"))
async def test_duplicate_logical_child_keys_fail_before_or_during_the_seal(
    reverse_insertion: bool,
):
    """Equal child keys with divergent payloads cannot form one root family."""

    async with isolated_publication_case() as case:
        async with case.sessions() as session:
            async with session.begin():
                graph = await seed_publication_graph(session)
                attempt = await _seed_duplicate_child_candidate(session, graph, reverse_insertion)
                await _assert_duplicate_child_seal_rejected(session, case, graph, attempt)


@pytest.mark.asyncio
async def test_no_change_seals_distinct_capture_when_effective_output_matches():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        async with case.sessions() as session:
            async with session.begin():
                execution_id, candidate_generation_id, token = await _seed_distinct_capture_no_change_execution(
                    session, graph
                )
                receipt = await record_no_change(
                    session,
                    dataset_id=graph.dataset_id,
                    execution_id=execution_id,
                    expected_generation_id=graph.first_generation_id,
                    expected_pointer_version=1,
                    candidate_generation_id=candidate_generation_id,
                    lease_fence=1,
                    lease_token=token,
                )
                assert receipt.event_kind == "no_change"
                assert (
                    await session.scalar(
                        select(CustomImportExecution.state).where(CustomImportExecution.execution_id == execution_id)
                    )
                    == "no_change"
                )
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(CustomImportPublicationEvent)
                        .where(
                            CustomImportPublicationEvent.dataset_id == graph.dataset_id,
                            CustomImportPublicationEvent.event_kind == "no_change",
                        )
                    )
                    == 1
                )


@pytest.mark.asyncio
async def test_exact_activation_and_rollback_replay_after_later_pointer_transitions():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        activated = await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        await _activate_committed(
            case,
            graph,
            target_generation_id=graph.second_generation_id,
            expected_generation_id=graph.first_generation_id,
            expected_pointer_version=1,
        )
        rolled_back = await _rollback_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=graph.second_generation_id,
            expected_pointer_version=2,
        )
        await _activate_committed(
            case,
            graph,
            target_generation_id=graph.second_generation_id,
            expected_generation_id=graph.first_generation_id,
            expected_pointer_version=3,
        )

        activation_replay = await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        rollback_replay = await _rollback_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=graph.second_generation_id,
            expected_pointer_version=2,
        )

        assert activation_replay.replayed is True
        assert activation_replay.publication_event_id == activated.publication_event_id
        assert rollback_replay.replayed is True
        assert rollback_replay.publication_event_id == rolled_back.publication_event_id
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, graph.dataset_id)
            assert pointer is not None
            assert (pointer.generation_id, pointer.pointer_version) == (graph.second_generation_id, 4)


@pytest.mark.asyncio
async def test_no_change_replay_uses_immutable_receipt():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        async with case.sessions() as session:
            async with session.begin():
                recorded = await record_no_change(
                    session,
                    dataset_id=graph.dataset_id,
                    execution_id=graph.no_change_execution_id,
                    expected_generation_id=graph.first_generation_id,
                    expected_pointer_version=1,
                    candidate_generation_id=graph.no_change_candidate_generation_id,
                    lease_fence=graph.no_change_fence,
                    lease_token=graph.no_change_token,
                )
        await _activate_committed(
            case,
            graph,
            target_generation_id=graph.second_generation_id,
            expected_generation_id=graph.first_generation_id,
            expected_pointer_version=1,
        )
        async with case.sessions() as session:
            async with session.begin():
                replay = await record_no_change(
                    session,
                    dataset_id=graph.dataset_id,
                    execution_id=graph.no_change_execution_id,
                    expected_generation_id=graph.first_generation_id,
                    expected_pointer_version=1,
                    candidate_generation_id=graph.no_change_candidate_generation_id,
                    lease_fence=graph.no_change_fence,
                    lease_token=graph.no_change_token,
                )
                assert replay.replayed is True
                assert replay.publication_event_id == recorded.publication_event_id


async def _seal_then_remove_lease(case, graph: PublicationGraph):
    """Seal a candidate and remove its lease row before replaying the receipt."""

    async with case.sessions() as session:
        async with session.begin():
            attempt = await seed_running_generation(
                session,
                graph,
                suffix=f"lease-cleanup-seal-{graph.dataset_id}",
                base_generation_id=None,
            )
            sealed = await seal_generation(
                session,
                dataset_id=graph.dataset_id,
                generation_id=attempt.generation_id,
                lease_fence=attempt.fence,
                lease_token=attempt.token,
            )
            await session.execute(
                delete(CustomImportLease).where(CustomImportLease.execution_id == attempt.execution_id)
            )
    return attempt, sealed


async def _assert_seal_replay(case, graph: PublicationGraph, attempt, sealed) -> None:
    """Verify a sealed generation replays after its lease row is retained no longer."""

    async with case.sessions() as session:
        async with session.begin():
            replay = await seal_generation(
                session,
                dataset_id=graph.dataset_id,
                generation_id=attempt.generation_id,
                lease_fence=attempt.fence,
                lease_token=attempt.token,
            )
            assert replay.replayed is True
            assert replay.generation_id == sealed.generation_id


async def _record_no_change_then_remove_lease(case, graph: PublicationGraph):
    """Record a no-change receipt and remove the associated live lease."""

    async with case.sessions() as session:
        async with session.begin():
            recorded = await record_no_change(
                session,
                dataset_id=graph.dataset_id,
                execution_id=graph.no_change_execution_id,
                expected_generation_id=graph.first_generation_id,
                expected_pointer_version=1,
                candidate_generation_id=graph.no_change_candidate_generation_id,
                lease_fence=graph.no_change_fence,
                lease_token=graph.no_change_token,
            )
            await session.execute(
                delete(CustomImportLease).where(CustomImportLease.execution_id == graph.no_change_execution_id)
            )
    return recorded


async def _assert_no_change_replay(case, graph: PublicationGraph, recorded) -> None:
    """Verify a no-change receipt replays after its lease row has been removed."""

    async with case.sessions() as session:
        async with session.begin():
            replay = await record_no_change(
                session,
                dataset_id=graph.dataset_id,
                execution_id=graph.no_change_execution_id,
                expected_generation_id=graph.first_generation_id,
                expected_pointer_version=1,
                candidate_generation_id=graph.no_change_candidate_generation_id,
                lease_fence=graph.no_change_fence,
                lease_token=graph.no_change_token,
            )
            assert replay.replayed is True
            assert replay.publication_event_id == recorded.publication_event_id


@pytest.mark.asyncio
async def test_immutable_finality_replays_do_not_require_retained_lease_rows():
    """Final receipts replay from immutable evidence rather than live lease rows."""

    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        attempt, sealed = await _seal_then_remove_lease(case, graph)
        await _assert_seal_replay(case, graph, attempt, sealed)
        await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        recorded = await _record_no_change_then_remove_lease(case, graph)
        await _assert_no_change_replay(case, graph, recorded)


@pytest.mark.asyncio
async def test_finality_requires_read_committed_isolation():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.sessions() as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                with pytest.raises(PublicationConflict, match="READ COMMITTED"):
                    await activate_generation(
                        session,
                        dataset_id=graph.dataset_id,
                        target_generation_id=graph.first_generation_id,
                        expected_generation_id=None,
                        expected_pointer_version=0,
                    )


@pytest.mark.parametrize("authority_case", ("wrong_token", "stale_fence", "expired_lease"))
@pytest.mark.asyncio
async def test_no_change_rejects_invalid_lease_authority(authority_case: str):
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        lease_fence, lease_token = await _invalid_no_change_authority(case, graph, authority_case)
        async with case.sessions() as session:
            async with session.begin():
                with pytest.raises(PublicationConflict, match="lease is lost or expired"):
                    await record_no_change(
                        session,
                        dataset_id=graph.dataset_id,
                        execution_id=graph.no_change_execution_id,
                        expected_generation_id=graph.first_generation_id,
                        expected_pointer_version=1,
                        candidate_generation_id=graph.no_change_candidate_generation_id,
                        lease_fence=lease_fence,
                        lease_token=lease_token,
                    )
        await _assert_no_change_remains_unpublished(case, graph, "running")


@pytest.mark.asyncio
async def test_cancellation_wins_over_an_overlapping_no_change_publication():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.sessions() as session:
            async with session.begin():
                await activate_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    target_generation_id=graph.first_generation_id,
                    expected_generation_id=None,
                    expected_pointer_version=0,
                )

        cancellation_session = case.sessions()
        cancellation_transaction = await cancellation_session.begin()
        contender = None
        try:
            cancellation = await request_cancellation(
                cancellation_session,
                execution_id=graph.no_change_execution_id,
            )
            assert cancellation.changed is True
            assert cancellation.state == "canceling"

            backend_pid_ready = asyncio.get_running_loop().create_future()
            contender = asyncio.create_task(_contending_no_change(case, graph, backend_pid_ready))
            backend_pid = await asyncio.wait_for(backend_pid_ready, timeout=2)
            await _wait_for_backend_lock(case, backend_pid)
            await cancellation_transaction.commit()
            with pytest.raises(PublicationConflict, match="only a running execution"):
                await asyncio.wait_for(contender, timeout=5)
        finally:
            if cancellation_transaction.is_active:
                await cancellation_transaction.rollback()
            if contender is not None and not contender.done():
                contender.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await contender
            await cancellation_session.close()

        async with case.sessions() as verification_session:
            assert (
                await verification_session.scalar(
                    select(CustomImportExecution.state).where(
                        CustomImportExecution.execution_id == graph.no_change_execution_id
                    )
                )
                == "canceling"
            )
            assert (
                await verification_session.scalar(
                    select(func.count())
                    .select_from(CustomImportPublicationEvent)
                    .where(CustomImportPublicationEvent.execution_id == graph.no_change_execution_id)
                )
                == 0
            )


@pytest.mark.asyncio
async def test_stale_pointer_and_incomplete_producer_are_rejected():
    async with transaction_session() as session, session.begin():
        graph = await seed_publication_graph(session)
        await activate_generation(
            session,
            dataset_id=graph.dataset_id,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )

        with pytest.raises(PublicationConflict, match="compare-and-swap failed"):
            await activate_generation(
                session,
                dataset_id=graph.dataset_id,
                target_generation_id=graph.second_generation_id,
                expected_generation_id=graph.first_generation_id,
                expected_pointer_version=7,
            )

        await session.execute(
            update(CustomImportExecution)
            .where(CustomImportExecution.execution_id == graph.second_execution_id)
            .values(state="running")
        )
        with pytest.raises(PublicationConflict, match="producer is not completed"):
            await activate_generation(
                session,
                dataset_id=graph.dataset_id,
                target_generation_id=graph.second_generation_id,
                expected_generation_id=graph.first_generation_id,
                expected_pointer_version=1,
            )

        assert (
            await session.scalar(
                select(func.count())
                .select_from(CustomImportPublicationEvent)
                .where(CustomImportPublicationEvent.dataset_id == graph.dataset_id)
            )
            == 1
        )


@dataclass(frozen=True)
class _PointerRace:
    winner_generation_id: int
    loser_generation_id: int
    expected_generation_id: int | None
    expected_pointer_version: int
    committed_pointer_version: int


async def _pointer_race_setup(
    case,
    graph: PublicationGraph,
    pointer_case: str,
    competing_generation_id: int,
) -> _PointerRace:
    if pointer_case == "initial":
        return _PointerRace(
            winner_generation_id=graph.first_generation_id,
            loser_generation_id=competing_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
            committed_pointer_version=1,
        )
    await _activate_committed(
        case,
        graph,
        target_generation_id=graph.first_generation_id,
        expected_generation_id=None,
        expected_pointer_version=0,
    )
    return _PointerRace(
        winner_generation_id=graph.second_generation_id,
        loser_generation_id=competing_generation_id,
        expected_generation_id=graph.first_generation_id,
        expected_pointer_version=1,
        committed_pointer_version=2,
    )


async def _run_pointer_race(case, graph: PublicationGraph, race: _PointerRace) -> None:
    winner_session = case.sessions()
    winner_transaction = await winner_session.begin()
    contender = None
    try:
        winner = await activate_generation(
            winner_session,
            dataset_id=graph.dataset_id,
            target_generation_id=race.winner_generation_id,
            expected_generation_id=race.expected_generation_id,
            expected_pointer_version=race.expected_pointer_version,
        )
        assert winner.committed_pointer_version == race.committed_pointer_version
        backend_pid_ready = asyncio.get_running_loop().create_future()
        contender = asyncio.create_task(
            _contending_activation(
                case,
                dataset_id=graph.dataset_id,
                target_generation_id=race.loser_generation_id,
                expected_generation_id=race.expected_generation_id,
                expected_pointer_version=race.expected_pointer_version,
                backend_pid_ready=backend_pid_ready,
            )
        )
        backend_pid = await asyncio.wait_for(backend_pid_ready, timeout=2)
        await _wait_for_backend_lock(case, backend_pid)
        await winner_transaction.commit()
        with pytest.raises(PublicationConflict, match="compare-and-swap failed"):
            await asyncio.wait_for(contender, timeout=5)
    finally:
        if winner_transaction.is_active:
            await winner_transaction.rollback()
        await _cancel_pending_task(contender)
        await winner_session.close()


async def _assert_pointer_race_outcome(case, graph: PublicationGraph, race: _PointerRace) -> None:
    async with case.sessions() as verification_session:
        pointer = await verification_session.get(CustomImportCurrentGeneration, graph.dataset_id)
        assert pointer is not None
        assert (pointer.generation_id, pointer.pointer_version) == (
            race.winner_generation_id,
            race.committed_pointer_version,
        )
        assert (
            await verification_session.scalar(
                select(func.count())
                .select_from(CustomImportPublicationEvent)
                .where(
                    CustomImportPublicationEvent.dataset_id == graph.dataset_id,
                    CustomImportPublicationEvent.to_generation_id == race.loser_generation_id,
                )
            )
            == 0
        )


@pytest.mark.parametrize("pointer_case", ("initial", "existing"))
@pytest.mark.asyncio
async def test_overlapping_generation_candidates_allow_only_one_pointer_cas(pointer_case: str):
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        competing_generation_id = await _seed_competing_generation(
            case,
            graph,
            base_generation_id=None if pointer_case == "initial" else graph.first_generation_id,
        )
        race = await _pointer_race_setup(case, graph, pointer_case, competing_generation_id)
        await _run_pointer_race(case, graph, race)
        await _assert_pointer_race_outcome(case, graph, race)


@pytest.mark.asyncio
async def test_two_session_stale_pointer_rejects_no_change_after_preloaded_orm_state():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        await _activate_committed(
            case,
            graph,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        async with case.sessions() as stale_session:
            async with stale_session.begin():
                stale_pointer = await stale_session.get(CustomImportCurrentGeneration, graph.dataset_id)
                stale_execution = await stale_session.get(CustomImportExecution, graph.no_change_execution_id)
                assert stale_pointer is not None
                assert stale_execution is not None
                assert (stale_pointer.generation_id, stale_pointer.pointer_version) == (
                    graph.first_generation_id,
                    1,
                )
                assert stale_execution.state == "running"
                advanced = await _activate_committed(
                    case,
                    graph,
                    target_generation_id=graph.second_generation_id,
                    expected_generation_id=graph.first_generation_id,
                    expected_pointer_version=1,
                )
                assert advanced.committed_pointer_version == 2
                with pytest.raises(PublicationConflict, match="compare-and-swap failed"):
                    await record_no_change(
                        stale_session,
                        dataset_id=graph.dataset_id,
                        execution_id=graph.no_change_execution_id,
                        expected_generation_id=graph.first_generation_id,
                        expected_pointer_version=1,
                        candidate_generation_id=graph.no_change_candidate_generation_id,
                        lease_fence=graph.no_change_fence,
                        lease_token=graph.no_change_token,
                    )

                assert (stale_pointer.generation_id, stale_pointer.pointer_version) == (
                    graph.second_generation_id,
                    2,
                )
                assert stale_execution.state == "running"
        await _assert_no_change_remains_unpublished(case, graph, "running")


@pytest.mark.asyncio
async def test_outer_transaction_rollback_after_activation_leaves_no_pointer_or_event():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.sessions() as session:
            outer_transaction = await session.begin()
            try:
                activated = await activate_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    target_generation_id=graph.first_generation_id,
                    expected_generation_id=None,
                    expected_pointer_version=0,
                )
                assert activated.replayed is False
                assert await session.get(CustomImportCurrentGeneration, graph.dataset_id) is not None
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(CustomImportPublicationEvent)
                        .where(CustomImportPublicationEvent.dataset_id == graph.dataset_id)
                    )
                    == 1
                )
            finally:
                await outer_transaction.rollback()

        async with case.sessions() as verification_session:
            assert await verification_session.get(CustomImportCurrentGeneration, graph.dataset_id) is None
            assert (
                await verification_session.scalar(
                    select(func.count())
                    .select_from(CustomImportPublicationEvent)
                    .where(CustomImportPublicationEvent.dataset_id == graph.dataset_id)
                )
                == 0
            )


@pytest.mark.asyncio
async def test_completed_execution_then_activation_in_one_transaction_rolls_back_to_running():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.sessions() as session:
            outer_transaction = await session.begin()
            try:
                completed = await finish_execution(
                    session,
                    execution_id=graph.no_change_execution_id,
                    fence=graph.no_change_fence,
                    token=graph.no_change_token,
                    terminal_state="completed",
                )
                assert completed.changed is True
                with pytest.raises(ExecutionLifecycleError, match="commit execution lifecycle work"):
                    await activate_generation(
                        session,
                        dataset_id=graph.dataset_id,
                        target_generation_id=graph.first_generation_id,
                        expected_generation_id=None,
                        expected_pointer_version=0,
                    )
            finally:
                await outer_transaction.rollback()

        async with case.sessions() as verification_session:
            assert (
                await verification_session.scalar(
                    select(CustomImportExecution.state).where(
                        CustomImportExecution.execution_id == graph.no_change_execution_id
                    )
                )
                == "running"
            )
            assert await verification_session.get(CustomImportCurrentGeneration, graph.dataset_id) is None
            assert (
                await verification_session.scalar(
                    select(func.count())
                    .select_from(CustomImportPublicationEvent)
                    .where(CustomImportPublicationEvent.dataset_id == graph.dataset_id)
                )
                == 0
            )


@pytest.mark.asyncio
async def test_heartbeat_then_no_change_requires_a_separate_transaction():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.sessions() as session:
            async with session.begin():
                await activate_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    target_generation_id=graph.first_generation_id,
                    expected_generation_id=None,
                    expected_pointer_version=0,
                )

        async with case.sessions() as session:
            async with session.begin():
                heartbeat = await heartbeat_execution(
                    session,
                    execution_id=graph.no_change_execution_id,
                    fence=graph.no_change_fence,
                    token=graph.no_change_token,
                )
                assert heartbeat is not None
                with pytest.raises(ExecutionLifecycleError, match="commit execution lifecycle work"):
                    await record_no_change(
                        session,
                        dataset_id=graph.dataset_id,
                        execution_id=graph.no_change_execution_id,
                        expected_generation_id=graph.first_generation_id,
                        expected_pointer_version=1,
                        candidate_generation_id=graph.no_change_candidate_generation_id,
                        lease_fence=graph.no_change_fence,
                        lease_token=graph.no_change_token,
                    )

        async with case.sessions() as verification_session:
            assert (
                await verification_session.scalar(
                    select(CustomImportExecution.state).where(
                        CustomImportExecution.execution_id == graph.no_change_execution_id
                    )
                )
                == "running"
            )
            assert (
                await verification_session.scalar(
                    select(func.count())
                    .select_from(CustomImportPublicationEvent)
                    .where(CustomImportPublicationEvent.execution_id == graph.no_change_execution_id)
                )
                == 0
            )


def _joined_savepoint_session(connection) -> AsyncSession:
    return AsyncSession(
        bind=connection,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )


async def _rollback_open_transaction(transaction) -> None:
    if transaction is not None and transaction.is_active:
        await transaction.rollback()


async def _assert_joined_lifecycle_blocks_publication(connection, graph: PublicationGraph) -> None:
    outer_transaction = await connection.begin()
    lifecycle_session = _joined_savepoint_session(connection)
    publication_session = _joined_savepoint_session(connection)
    lifecycle_transaction = await lifecycle_session.begin()
    publication_transaction = None
    try:
        heartbeat = await heartbeat_execution(
            lifecycle_session,
            execution_id=graph.no_change_execution_id,
            fence=graph.no_change_fence,
            token=graph.no_change_token,
        )
        assert heartbeat is not None
        await lifecycle_transaction.commit()
        assert outer_transaction.is_active
        publication_transaction = await publication_session.begin()
        with pytest.raises(ExecutionLifecycleError, match="commit execution lifecycle work"):
            await activate_generation(
                publication_session,
                dataset_id=graph.dataset_id,
                target_generation_id=graph.first_generation_id,
                expected_generation_id=None,
                expected_pointer_version=0,
            )
        assert outer_transaction.is_active
        assert (
            await connection.scalar(
                select(func.count())
                .select_from(CustomImportPublicationEvent)
                .where(CustomImportPublicationEvent.dataset_id == graph.dataset_id)
            )
            == 0
        )
    finally:
        await _rollback_open_transaction(publication_transaction)
        await _rollback_open_transaction(lifecycle_transaction)
        await publication_session.close()
        await lifecycle_session.close()
        await _rollback_open_transaction(outer_transaction)


async def _assert_new_root_allows_activation(connection, graph: PublicationGraph) -> None:
    next_root_transaction = await connection.begin()
    next_root_session = _joined_savepoint_session(connection)
    next_session_transaction = await next_root_session.begin()
    try:
        activated = await activate_generation(
            next_root_session,
            dataset_id=graph.dataset_id,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        assert activated.replayed is False
    finally:
        await _rollback_open_transaction(next_session_transaction)
        await next_root_session.close()
        await _rollback_open_transaction(next_root_transaction)


async def _assert_joined_lifecycle_rollback(case, graph: PublicationGraph) -> None:
    async with case.sessions() as verification_session:
        assert await verification_session.get(CustomImportCurrentGeneration, graph.dataset_id) is None
        assert (
            await verification_session.scalar(
                select(CustomImportExecution.state).where(
                    CustomImportExecution.execution_id == graph.no_change_execution_id
                )
            )
            == "running"
        )
        assert (
            await verification_session.scalar(
                select(func.count())
                .select_from(CustomImportPublicationEvent)
                .where(CustomImportPublicationEvent.dataset_id == graph.dataset_id)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_joined_savepoint_lifecycle_blocks_second_session_until_root_ends():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.engine.connect() as connection:
            await _assert_joined_lifecycle_blocks_publication(connection, graph)
            await _assert_new_root_allows_activation(connection, graph)
        await _assert_joined_lifecycle_rollback(case, graph)


_FINALITY_MIGRATION_ROOT = Path(__file__).resolve().parents[1]
_FINALITY_MIGRATION_PATH = (
    _FINALITY_MIGRATION_ROOT / "alembic" / "versions" / "20260917130000_custom_import_generation_finality.py"
)


def _finality_migration():
    spec = importlib.util.spec_from_file_location("custom_import_finality_downgrade_probe", _FINALITY_MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _downgrade_finality_schema(sync_connection, schema_name: str) -> None:
    """Invoke the exact downgrade through Alembic against one disposable schema."""

    migration = _finality_migration()
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(sync_connection))
    migration.downgrade()


def _upgrade_finality_schema(sync_connection, schema_name: str) -> None:
    """Invoke the exact upgrade against a disposable pre-finality schema."""

    migration = _finality_migration()
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(sync_connection))
    migration.upgrade()


def _finality_table(schema_name: str, table_name: str) -> str:
    assert schema_name.startswith("custom_import_publication_")
    assert table_name.startswith("custom_import_")
    return f'"{schema_name}"."{table_name}"'


def _finality_digest(label: str) -> bytes:
    return hashlib.sha256(label.encode("utf-8")).digest()


@dataclass(frozen=True)
class _FinalityFencedPackPrerequisites:
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    capture_bundle_id: int
    execution_id: int
    token_sha256: bytes


async def _finality_insert_legacy_dataset(connection, schema_name: str, label: str) -> int:
    """Insert the dataset prerequisite for a legacy identity graph."""

    dataset_id = await connection.scalar(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_dataset')} (dataset_key) "
            "VALUES (:dataset_key) RETURNING dataset_id"
        ),
        {"dataset_key": f"legacy_{label}"},
    )
    assert isinstance(dataset_id, int)
    return dataset_id


async def _finality_insert_legacy_schema_revision(connection, schema_name: str, dataset_id: int, label: str) -> int:
    """Insert the schema revision prerequisite for a legacy identity graph."""

    schema_revision_id = await connection.scalar(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_schema_revision')} "
            "(dataset_id, revision_number, canonical_schema, schema_sha256) "
            "VALUES (:dataset_id, 1, :canonical_schema, :schema_sha256) "
            "RETURNING schema_revision_id"
        ),
        {
            "dataset_id": dataset_id,
            "canonical_schema": '{"legacy":true}',
            "schema_sha256": _finality_digest(f"schema:{label}"),
        },
    )
    assert isinstance(schema_revision_id, int)
    return schema_revision_id


async def _finality_insert_legacy_definition_revision(
    connection,
    schema_name: str,
    dataset_id: int,
    schema_revision_id: int,
    label: str,
) -> int:
    """Insert the definition prerequisite for a legacy identity graph."""

    definition_revision_id = await connection.scalar(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_definition_revision')} "
            "(dataset_id, schema_revision_id, revision_number, contract_version, refresh_mode, "
            "canonical_definition, definition_sha256) "
            "VALUES (:dataset_id, :schema_revision_id, 1, 'custom-import/v1', 'upsert', "
            ":canonical_definition, :definition_sha256) RETURNING definition_revision_id"
        ),
        {
            "dataset_id": dataset_id,
            "schema_revision_id": schema_revision_id,
            "canonical_definition": '{"legacy":true}',
            "definition_sha256": _finality_digest(f"definition:{label}"),
        },
    )
    assert isinstance(definition_revision_id, int)
    return definition_revision_id


async def _finality_insert_legacy_capture_bundle(
    connection,
    schema_name: str,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    label: str,
) -> int:
    """Insert the capture bundle prerequisite for a legacy identity graph."""

    capture_bundle_id = await connection.scalar(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_capture_bundle')} "
            "(dataset_id, definition_revision_id, schema_revision_id, snapshot_token, "
            "snapshot_token_sha256, canonical_manifest, manifest_sha256, stream_count) "
            "VALUES (:dataset_id, :definition_revision_id, :schema_revision_id, :snapshot_token, "
            ":snapshot_token_sha256, :canonical_manifest, :manifest_sha256, 1) "
            "RETURNING capture_bundle_id"
        ),
        {
            "dataset_id": dataset_id,
            "definition_revision_id": definition_revision_id,
            "schema_revision_id": schema_revision_id,
            "snapshot_token": f"legacy-snapshot-{label}",
            "snapshot_token_sha256": _finality_digest(f"snapshot:{label}"),
            "canonical_manifest": '{"legacy":true}',
            "manifest_sha256": _finality_digest(f"manifest:{label}"),
        },
    )
    assert isinstance(capture_bundle_id, int)
    return capture_bundle_id


async def _finality_insert_legacy_capture_streams(
    connection,
    schema_name: str,
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    capture_bundle_id: int,
    label: str,
) -> None:
    """Insert one source stream and its matching retained capture."""

    await connection.execute(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_source_stream')} "
            "(definition_revision_id, dataset_id, schema_revision_id, stream_slot, stream_id, "
            "record_kind, collection_slot, decoder, compression, snapshot_token_selector, record_path) "
            "VALUES (:definition_revision_id, :dataset_id, :schema_revision_id, 1, 'legacy_root', "
            "'root', NULL, 'json', 'none', 'snapshot', NULL)"
        ),
        {
            "dataset_id": dataset_id,
            "definition_revision_id": definition_revision_id,
            "schema_revision_id": schema_revision_id,
        },
    )
    await connection.execute(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_capture')} "
            "(capture_bundle_id, dataset_id, definition_revision_id, schema_revision_id, stream_slot, "
            "content_sha256, byte_count, canonical_manifest, manifest_sha256) "
            "VALUES (:capture_bundle_id, :dataset_id, :definition_revision_id, :schema_revision_id, 1, "
            ":content_sha256, 0, :canonical_manifest, :manifest_sha256)"
        ),
        {
            "capture_bundle_id": capture_bundle_id,
            "dataset_id": dataset_id,
            "definition_revision_id": definition_revision_id,
            "schema_revision_id": schema_revision_id,
            "content_sha256": _finality_digest(f"capture-content:{label}"),
            "canonical_manifest": '{"legacy":true}',
            "manifest_sha256": _finality_digest(f"capture-manifest:{label}"),
        },
    )


async def _finality_insert_minimal_identity(connection, schema_name: str, *, label: str) -> tuple[int, int, int, int]:
    """Insert the smallest valid dataset/definition/capture identity graph."""

    dataset_id = await _finality_insert_legacy_dataset(connection, schema_name, label)
    schema_revision_id = await _finality_insert_legacy_schema_revision(connection, schema_name, dataset_id, label)
    definition_revision_id = await _finality_insert_legacy_definition_revision(
        connection,
        schema_name,
        dataset_id,
        schema_revision_id,
        label,
    )
    capture_bundle_id = await _finality_insert_legacy_capture_bundle(
        connection,
        schema_name,
        dataset_id,
        definition_revision_id,
        schema_revision_id,
        label,
    )
    await _finality_insert_legacy_capture_streams(
        connection,
        schema_name,
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        capture_bundle_id=capture_bundle_id,
        label=label,
    )
    return dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id


async def _seed_finality_fenced_pack_prerequisites(case) -> _FinalityFencedPackPrerequisites:
    """Create a running, live-lease execution with no output yet."""

    async with case.engine.begin() as connection:
        (
            dataset_id,
            definition_revision_id,
            schema_revision_id,
            capture_bundle_id,
        ) = await _finality_insert_minimal_identity(connection, case.schema_name, label="downgrade_guard")
        execution_id = await connection.scalar(
            text(
                f"INSERT INTO {_finality_table(case.schema_name, 'custom_import_execution')} "
                "(dataset_id, definition_revision_id, schema_revision_id, idempotency_key, mechanism, state, "
                "capture_bundle_id) VALUES (:dataset_id, :definition_revision_id, :schema_revision_id, "
                "'downgrade-guard', 'local', 'running', :capture_bundle_id) RETURNING execution_id"
            ),
            {
                "dataset_id": dataset_id,
                "definition_revision_id": definition_revision_id,
                "schema_revision_id": schema_revision_id,
                "capture_bundle_id": capture_bundle_id,
            },
        )
        assert isinstance(execution_id, int)
        token_sha256 = _finality_digest("downgrade-guard-token")
        await connection.execute(
            text(
                f"INSERT INTO {_finality_table(case.schema_name, 'custom_import_lease')} "
                "(execution_id, fence, token_sha256, heartbeat_at, expires_at) "
                "VALUES (:execution_id, 1, :token_sha256, clock_timestamp(), "
                "clock_timestamp() + interval '5 minutes')"
            ),
            {"execution_id": execution_id, "token_sha256": token_sha256},
        )
    return _FinalityFencedPackPrerequisites(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        capture_bundle_id=capture_bundle_id,
        execution_id=execution_id,
        token_sha256=token_sha256,
    )


async def _wait_for_finality_downgrade_backend_lock(case, backend_pid: int) -> None:
    async with case.engine.connect() as connection:
        for _ in range(200):
            wait_event_type = await connection.scalar(
                text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :backend_pid"),
                {"backend_pid": backend_pid},
            )
            if wait_event_type == "Lock":
                return
            await asyncio.sleep(0.01)
    pytest.fail("concurrent writer did not wait on the downgrade guard lock")


async def _insert_finality_fenced_pack(case, attempt: _FinalityFencedPackPrerequisites, backend_pid_ready):
    """Insert one valid new-attempt pack after exposing its PostgreSQL backend."""

    async with case.engine.begin() as connection:
        await connection.execute(text("SET LOCAL statement_timeout = '5s'"))
        backend_pid = await connection.scalar(text("SELECT pg_backend_pid()"))
        assert isinstance(backend_pid, int)
        backend_pid_ready.set_result(backend_pid)
        await connection.execute(
            text(
                f"INSERT INTO {_finality_table(case.schema_name, 'custom_import_pack')} "
                "(execution_id, dataset_id, definition_revision_id, schema_revision_id, stream_slot, "
                "pack_ordinal, capture_bundle_id, record_count, pack_sha256, producing_fence, "
                "producing_token_sha256) VALUES (:execution_id, :dataset_id, :definition_revision_id, "
                ":schema_revision_id, 1, 0, :capture_bundle_id, 0, :pack_sha256, 1, :token_sha256)"
            ),
            {
                "execution_id": attempt.execution_id,
                "dataset_id": attempt.dataset_id,
                "definition_revision_id": attempt.definition_revision_id,
                "schema_revision_id": attempt.schema_revision_id,
                "capture_bundle_id": attempt.capture_bundle_id,
                "pack_sha256": _finality_digest("downgrade-guard-pack"),
                "token_sha256": attempt.token_sha256,
            },
        )


@pytest.mark.asyncio
async def test_finality_downgrade_refuses_fenced_data_without_removing_anything():
    """A sealed candidate makes downgrade fail before its first destructive statement."""

    async with isolated_publication_case() as case:
        async with case.sessions() as session:
            async with session.begin():
                graph = await seed_publication_graph(session)

        async with case.engine.connect() as connection:
            with pytest.raises(DBAPIError, match="custom_import_generation_finality_downgrade_blocked"):
                await connection.run_sync(_downgrade_finality_schema, case.schema_name)
            await connection.rollback()

        async with case.sessions() as session:
            seal_count = await session.scalar(
                select(func.count())
                .select_from(CustomImportGenerationSeal)
                .where(CustomImportGenerationSeal.dataset_id == graph.dataset_id)
            )
            generation = await session.get(CustomImportGeneration, graph.first_generation_id)
            seal_relation_exists = await session.scalar(
                text("SELECT to_regclass(:relation_name) IS NOT NULL"),
                {"relation_name": f"{case.schema_name}.custom_import_generation_seal"},
            )
            no_change_relation_exists = await session.scalar(
                text("SELECT to_regclass(:relation_name) IS NOT NULL"),
                {"relation_name": f"{case.schema_name}.custom_import_no_change_seal"},
            )

        assert seal_count == 2
        assert generation is not None
        assert generation.candidate_sha256 is not None
        assert generation.producing_fence == 1
        assert seal_relation_exists is True
        assert no_change_relation_exists is True


@pytest.mark.asyncio
async def test_downgrade_guard_blocks_concurrent_fenced_output_before_any_drop():
    """A fenced pack cannot enter between the downgrade guard and destructive DDL."""

    async with isolated_publication_case() as case:
        attempt = await _seed_finality_fenced_pack_prerequisites(case)
        migration = _finality_migration()
        guard_session = case.sessions()
        guard_transaction = await guard_session.begin()
        writer = None
        try:
            await guard_session.execute(text(migration._downgrade_finality_data_guard_sql(case.schema_name)))
            backend_pid_ready = asyncio.get_running_loop().create_future()
            writer = asyncio.create_task(_insert_finality_fenced_pack(case, attempt, backend_pid_ready))
            backend_pid = await asyncio.wait_for(backend_pid_ready, timeout=2)
            await _wait_for_finality_downgrade_backend_lock(case, backend_pid)
            await guard_transaction.commit()
            await asyncio.wait_for(writer, timeout=5)
        finally:
            if guard_transaction.is_active:
                await guard_transaction.rollback()
            if writer is not None and not writer.done():
                writer.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await writer
            await guard_session.close()

        async with case.engine.connect() as connection:
            with pytest.raises(DBAPIError, match="custom_import_generation_finality_downgrade_blocked"):
                await connection.run_sync(_downgrade_finality_schema, case.schema_name)
            await connection.rollback()


async def _finality_insert_legacy_completed_execution(
    connection, schema_name: str, identity: tuple[int, int, int, int]
) -> int:
    """Insert the completed execution used by retained legacy publication events."""

    dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id = identity
    execution_id = await connection.scalar(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_execution')} "
            "(dataset_id, definition_revision_id, schema_revision_id, idempotency_key, mechanism, "
            "state, capture_bundle_id) VALUES (:dataset_id, :definition_revision_id, "
            ":schema_revision_id, 'legacy-duplicate-events', 'local', 'completed', "
            ":capture_bundle_id) RETURNING execution_id"
        ),
        {
            "dataset_id": dataset_id,
            "definition_revision_id": definition_revision_id,
            "schema_revision_id": schema_revision_id,
            "capture_bundle_id": capture_bundle_id,
        },
    )
    assert isinstance(execution_id, int)
    return execution_id


async def _finality_insert_legacy_generation(
    connection,
    schema_name: str,
    identity: tuple[int, int, int, int],
    execution_id: int,
) -> int:
    """Insert the unsealed generation referenced by legacy publication events."""

    dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id = identity
    generation_id = await connection.scalar(
        text(
            f"INSERT INTO {_finality_table(schema_name, 'custom_import_generation')} "
            "(dataset_id, definition_revision_id, schema_revision_id, execution_id, capture_bundle_id, "
            "base_generation_id, base_dataset_id, source_bundle_sha256, generation_sha256, root_count, "
            "family_count) VALUES (:dataset_id, :definition_revision_id, :schema_revision_id, "
            ":execution_id, :capture_bundle_id, NULL, NULL, :source_bundle_sha256, "
            ":generation_sha256, 0, 0) RETURNING generation_id"
        ),
        {
            "dataset_id": dataset_id,
            "definition_revision_id": definition_revision_id,
            "schema_revision_id": schema_revision_id,
            "execution_id": execution_id,
            "capture_bundle_id": capture_bundle_id,
            "source_bundle_sha256": _finality_digest("legacy-source-bundle"),
            "generation_sha256": _finality_digest("legacy-generation"),
        },
    )
    assert isinstance(generation_id, int)
    return generation_id


async def _finality_insert_duplicate_legacy_publication_events(
    connection,
    schema_name: str,
    identity: tuple[int, int, int, int],
    execution_id: int,
    generation_id: int,
) -> None:
    """Retain two valid pre-finality events with the same legacy identity."""

    dataset_id, definition_revision_id, schema_revision_id, _capture_bundle_id = identity
    for ordinal in (1, 2):
        await connection.execute(
            text(
                f"INSERT INTO {_finality_table(schema_name, 'custom_import_publication_event')} "
                "(dataset_id, definition_revision_id, schema_revision_id, execution_id, event_kind, "
                "from_generation_id, to_generation_id, expected_pointer_version, "
                "committed_pointer_version, canonical_event, event_sha256) "
                "VALUES (:dataset_id, :definition_revision_id, :schema_revision_id, :execution_id, "
                "'activated', NULL, :generation_id, 0, 1, :canonical_event, :event_sha256)"
            ),
            {
                "dataset_id": dataset_id,
                "definition_revision_id": definition_revision_id,
                "schema_revision_id": schema_revision_id,
                "execution_id": execution_id,
                "generation_id": generation_id,
                "canonical_event": f'{{"legacy_duplicate":{ordinal}}}',
                "event_sha256": _finality_digest(f"legacy-event:{ordinal}"),
            },
        )


async def _seed_finality_duplicate_legacy_events(case) -> None:
    """Downgrade and populate duplicate events before the finality upgrade."""

    async with case.engine.begin() as connection:
        await connection.run_sync(_downgrade_finality_schema, case.schema_name)
    async with case.engine.begin() as connection:
        identity = await _finality_insert_minimal_identity(
            connection, case.schema_name, label="legacy_duplicate_events"
        )
        execution_id = await _finality_insert_legacy_completed_execution(connection, case.schema_name, identity)
        generation_id = await _finality_insert_legacy_generation(connection, case.schema_name, identity, execution_id)
        await _finality_insert_duplicate_legacy_publication_events(
            connection,
            case.schema_name,
            identity,
            execution_id,
            generation_id,
        )


@pytest.mark.asyncio
async def test_finality_downgrade_restores_legacy_rejection_code_shape():
    """A valid legacy rejection survives upgrade and a later downgrade."""

    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_downgrade_finality_schema, case.schema_name)
            identity = await _finality_insert_minimal_identity(
                connection,
                case.schema_name,
                label="legacy_rejection_code",
            )
            execution_id = await _finality_insert_legacy_completed_execution(
                connection,
                case.schema_name,
                identity,
            )
            dataset_id, definition_revision_id, schema_revision_id, _capture_bundle_id = identity
            rejection_code = await connection.scalar(
                text(
                    f"INSERT INTO {_finality_table(case.schema_name, 'custom_import_rejection')} "
                    "(execution_id, rejection_ordinal, dataset_id, definition_revision_id, "
                    "schema_revision_id, code, canonical_evidence) VALUES "
                    "(:execution_id, 0, :dataset_id, :definition_revision_id, "
                    ":schema_revision_id, 'invalid_child', '{\"reason\":\"synthetic\"}') "
                    "RETURNING code"
                ),
                {
                    "execution_id": execution_id,
                    "dataset_id": dataset_id,
                    "definition_revision_id": definition_revision_id,
                    "schema_revision_id": schema_revision_id,
                },
            )
            assert rejection_code == "invalid_child"
            await connection.run_sync(_upgrade_finality_schema, case.schema_name)
            await connection.run_sync(_downgrade_finality_schema, case.schema_name)
            retained_rejection = (
                await connection.execute(
                    text(
                        f"SELECT rejection_ordinal, code, canonical_evidence "
                        f"FROM {_finality_table(case.schema_name, 'custom_import_rejection')} "
                        "WHERE execution_id = :execution_id"
                    ),
                    {"execution_id": execution_id},
                )
            ).one()
        assert retained_rejection == (0, "invalid_child", '{"reason":"synthetic"}')


async def _finality_upgraded_duplicate_event_receipt(case) -> tuple[int | None, object | None]:
    """Upgrade finality and return the retained-event count plus identity index DDL."""

    async with case.engine.begin() as connection:
        await connection.run_sync(_upgrade_finality_schema, case.schema_name)
    async with case.engine.connect() as connection:
        retained_count = await connection.scalar(
            text(
                f"SELECT count(*) FROM {_finality_table(case.schema_name, 'custom_import_publication_event')} "
                "WHERE finality_contract IS NULL"
            )
        )
        event_index = await connection.scalar(
            text(
                "SELECT indexdef FROM pg_indexes "
                "WHERE schemaname = :schema_name "
                "AND indexname = 'custom_import_publication_event_identity_key'"
            ),
            {"schema_name": case.schema_name},
        )
    return retained_count, event_index


@pytest.mark.asyncio
async def test_upgrade_retains_populated_legacy_duplicate_publication_events():
    """Feature-on indexes exclude, rather than silently deleting, legacy event duplicates."""

    async with isolated_publication_case() as case:
        await _seed_finality_duplicate_legacy_events(case)
        retained_count, event_index = await _finality_upgraded_duplicate_event_receipt(case)

    assert retained_count == 2
    assert isinstance(event_index, str)
    assert "finality_contract" in event_index
