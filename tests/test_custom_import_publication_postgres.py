# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportPublicationEvent,
)
from process.custom_import.execution import (
    ExecutionLifecycleError,
    finish_execution,
    heartbeat_execution,
    request_cancellation,
)
from process.custom_import.publication import (
    PublicationConflict,
    activate_generation,
    record_no_change,
    rollback_generation,
)
from tests.custom_import_postgres_support import (
    digest,
    isolated_publication_case,
    seed_publication_graph,
    transaction_session,
)


async def _seed_committed_publication_graph(case):
    async with case.sessions() as session:
        async with session.begin():
            return await seed_publication_graph(session)


async def _seed_competing_generation(case, graph, *, base_generation_id: int | None = None) -> int:
    async with case.sessions() as session:
        async with session.begin():
            source = await session.get(CustomImportGeneration, graph.second_generation_id)
            assert source is not None
            execution = CustomImportExecution(
                dataset_id=graph.dataset_id,
                definition_revision_id=graph.definition_revision_id,
                schema_revision_id=graph.schema_revision_id,
                capture_bundle_id=source.capture_bundle_id,
                idempotency_key=f"synthetic-competing-{graph.dataset_id}",
                mechanism="local",
                state="completed",
            )
            session.add(execution)
            await session.flush()
            generation = CustomImportGeneration(
                dataset_id=graph.dataset_id,
                definition_revision_id=graph.definition_revision_id,
                schema_revision_id=graph.schema_revision_id,
                execution_id=execution.execution_id,
                capture_bundle_id=source.capture_bundle_id,
                base_generation_id=base_generation_id,
                base_dataset_id=graph.dataset_id if base_generation_id is not None else None,
                source_bundle_sha256=digest(f"competing-source:{graph.dataset_id}"),
                generation_sha256=digest(f"competing-generation:{graph.dataset_id}"),
                root_count=3,
                family_count=3,
            )
            session.add(generation)
            await session.flush()
            return generation.generation_id


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
                effective_generation_sha256=graph.first_generation_sha256,
                lease_fence=graph.no_change_fence,
                lease_token=graph.no_change_token,
            )


@pytest.mark.asyncio
async def test_activation_rollback_no_change_and_exact_replays_are_atomic():
    async with transaction_session() as session, session.begin():
        graph = await seed_publication_graph(session)

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

        unchanged = await record_no_change(
            session,
            dataset_id=graph.dataset_id,
            execution_id=graph.no_change_execution_id,
            expected_generation_id=graph.first_generation_id,
            expected_pointer_version=3,
            effective_generation_sha256=graph.first_generation_sha256,
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
            effective_generation_sha256=memoryview(graph.first_generation_sha256),
            lease_fence=graph.no_change_fence,
            lease_token=graph.no_change_token,
        )
        assert unchanged_replay.publication_event_id == unchanged.publication_event_id
        assert unchanged_replay.replayed is True

        pointer = (
            await session.execute(
                select(CustomImportCurrentGeneration).where(
                    CustomImportCurrentGeneration.dataset_id == graph.dataset_id
                )
            )
        ).scalar_one()
        assert (pointer.generation_id, pointer.pointer_version) == (
            graph.first_generation_id,
            3,
        )
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
async def test_activation_requires_an_exact_immutable_base_generation_even_after_cas_refresh():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        candidate_generation_id = await _seed_competing_generation(
            case,
            graph,
            base_generation_id=graph.first_generation_id,
        )

        async with case.sessions() as session:
            async with session.begin():
                with pytest.raises(PublicationConflict, match="base generation"):
                    await activate_generation(
                        session,
                        dataset_id=graph.dataset_id,
                        target_generation_id=candidate_generation_id,
                        expected_generation_id=None,
                        expected_pointer_version=0,
                    )

        async with case.sessions() as session:
            async with session.begin():
                first = await activate_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    target_generation_id=graph.first_generation_id,
                    expected_generation_id=None,
                    expected_pointer_version=0,
                )
        assert first.replayed is False

        async with case.sessions() as session:
            async with session.begin():
                replay = await activate_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    target_generation_id=graph.first_generation_id,
                    expected_generation_id=None,
                    expected_pointer_version=0,
                )
        assert replay.publication_event_id == first.publication_event_id
        assert replay.replayed is True

        async with case.sessions() as session:
            async with session.begin():
                second = await activate_generation(
                    session,
                    dataset_id=graph.dataset_id,
                    target_generation_id=graph.second_generation_id,
                    expected_generation_id=graph.first_generation_id,
                    expected_pointer_version=1,
                )
        assert second.committed_pointer_version == 2

        async with case.sessions() as session:
            async with session.begin():
                with pytest.raises(PublicationConflict, match="base generation"):
                    await activate_generation(
                        session,
                        dataset_id=graph.dataset_id,
                        target_generation_id=candidate_generation_id,
                        expected_generation_id=graph.second_generation_id,
                        expected_pointer_version=2,
                    )

        async with case.sessions() as verification_session:
            pointer = await verification_session.get(CustomImportCurrentGeneration, graph.dataset_id)
            assert pointer is not None
            assert (pointer.generation_id, pointer.pointer_version) == (
                graph.second_generation_id,
                2,
            )
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

        async with case.sessions() as session:
            async with session.begin():
                rollback = await rollback_generation(
                    session,
                    dataset_id=graph.dataset_id,
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

        with pytest.raises(PublicationConflict, match="effective content differs"):
            await record_no_change(
                session,
                dataset_id=graph.dataset_id,
                execution_id=graph.no_change_execution_id,
                expected_generation_id=graph.first_generation_id,
                expected_pointer_version=1,
                effective_generation_sha256=digest("different-content"),
                lease_fence=graph.no_change_fence,
                lease_token=graph.no_change_token,
            )

        assert (
            await session.scalar(
                select(CustomImportExecution.state).where(
                    CustomImportExecution.execution_id == graph.no_change_execution_id
                )
            )
            == "running"
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
            == 0
        )


@pytest.mark.parametrize("authority_case", ("wrong_token", "stale_fence", "expired_lease"))
@pytest.mark.asyncio
async def test_no_change_rejects_invalid_lease_authority(authority_case: str):
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

        lease_fence = graph.no_change_fence
        lease_token = graph.no_change_token
        if authority_case == "wrong_token":
            lease_token = "synthetic-wrong-token"
        elif authority_case == "stale_fence":
            lease_fence += 1
        else:
            async with case.sessions() as session:
                async with session.begin():
                    await session.execute(
                        update(CustomImportLease)
                        .where(CustomImportLease.execution_id == graph.no_change_execution_id)
                        .values(expires_at=func.clock_timestamp() - func.make_interval(0, 0, 0, 0, 0, 1))
                    )

        async with case.sessions() as session:
            async with session.begin():
                with pytest.raises(PublicationConflict, match="lease is lost or expired"):
                    await record_no_change(
                        session,
                        dataset_id=graph.dataset_id,
                        execution_id=graph.no_change_execution_id,
                        expected_generation_id=graph.first_generation_id,
                        expected_pointer_version=1,
                        effective_generation_sha256=graph.first_generation_sha256,
                        lease_fence=lease_fence,
                        lease_token=lease_token,
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
        if pointer_case == "initial":
            winner_generation_id = graph.first_generation_id
            loser_generation_id = competing_generation_id
            expected_generation_id = None
            expected_pointer_version = 0
            committed_pointer_version = 1
        else:
            async with case.sessions() as session:
                async with session.begin():
                    await activate_generation(
                        session,
                        dataset_id=graph.dataset_id,
                        target_generation_id=graph.first_generation_id,
                        expected_generation_id=None,
                        expected_pointer_version=0,
                    )
            winner_generation_id = graph.second_generation_id
            loser_generation_id = competing_generation_id
            expected_generation_id = graph.first_generation_id
            expected_pointer_version = 1
            committed_pointer_version = 2

        winner_session = case.sessions()
        winner_transaction = await winner_session.begin()
        contender = None
        try:
            winner = await activate_generation(
                winner_session,
                dataset_id=graph.dataset_id,
                target_generation_id=winner_generation_id,
                expected_generation_id=expected_generation_id,
                expected_pointer_version=expected_pointer_version,
            )
            assert winner.committed_pointer_version == committed_pointer_version

            backend_pid_ready = asyncio.get_running_loop().create_future()
            contender = asyncio.create_task(
                _contending_activation(
                    case,
                    dataset_id=graph.dataset_id,
                    target_generation_id=loser_generation_id,
                    expected_generation_id=expected_generation_id,
                    expected_pointer_version=expected_pointer_version,
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
            if contender is not None and not contender.done():
                contender.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await contender
            await winner_session.close()

        async with case.sessions() as verification_session:
            pointer = await verification_session.get(CustomImportCurrentGeneration, graph.dataset_id)
            assert pointer is not None
            assert (pointer.generation_id, pointer.pointer_version) == (
                winner_generation_id,
                committed_pointer_version,
            )
            assert (
                await verification_session.scalar(
                    select(func.count())
                    .select_from(CustomImportPublicationEvent)
                    .where(
                        CustomImportPublicationEvent.dataset_id == graph.dataset_id,
                        CustomImportPublicationEvent.to_generation_id == loser_generation_id,
                    )
                )
                == 0
            )


@pytest.mark.asyncio
async def test_two_session_stale_pointer_rejects_no_change_after_preloaded_orm_state():
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

                async with case.sessions() as advancing_session:
                    async with advancing_session.begin():
                        advanced = await activate_generation(
                            advancing_session,
                            dataset_id=graph.dataset_id,
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
                        effective_generation_sha256=graph.first_generation_sha256,
                        lease_fence=graph.no_change_fence,
                        lease_token=graph.no_change_token,
                    )

                assert (stale_pointer.generation_id, stale_pointer.pointer_version) == (
                    graph.second_generation_id,
                    2,
                )
                assert stale_execution.state == "running"

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
                        effective_generation_sha256=graph.first_generation_sha256,
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


@pytest.mark.asyncio
async def test_joined_savepoint_lifecycle_blocks_publication_in_a_second_session_until_the_root_ends():
    async with isolated_publication_case() as case:
        graph = await _seed_committed_publication_graph(case)
        async with case.engine.connect() as connection:
            outer_transaction = await connection.begin()
            lifecycle_session = AsyncSession(
                bind=connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            publication_session = AsyncSession(
                bind=connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
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
                if publication_transaction is not None and publication_transaction.is_active:
                    await publication_transaction.rollback()
                if lifecycle_transaction.is_active:
                    await lifecycle_transaction.rollback()
                await publication_session.close()
                await lifecycle_session.close()
                if outer_transaction.is_active:
                    await outer_transaction.rollback()

            next_root_transaction = await connection.begin()
            next_root_session = AsyncSession(
                bind=connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
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
                if next_session_transaction.is_active:
                    await next_session_transaction.rollback()
                await next_root_session.close()
                if next_root_transaction.is_active:
                    await next_root_transaction.rollback()

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
