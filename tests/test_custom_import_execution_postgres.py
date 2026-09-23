# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native PostgreSQL proof for custom-import execution fencing."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import os
import uuid
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Awaitable, Callable, TypeVar

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from db.connection import Base
from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportLease,
    CustomImportSchemaRevision,
)
from process.custom_import import execution as lifecycle

_DSN_ENV = "HLTHPRT_CUSTOM_IMPORT_POSTGRES_DSN"
_RESULT = TypeVar("_RESULT")
_LOCK_OBSERVATION_TIMEOUT_SECONDS = 5
_LOCK_POLL_SECONDS = 0.01
_WORKER = "synthetic-worker"
_WORKER_A = "synthetic-worker-a"
_WORKER_B = "synthetic-worker-b"
_REQUEST_IDENTITY_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260922010000_custom_import_execution_request_identity.py"
)
_TABLES = (
    CustomImportDataset.__table__,
    CustomImportSchemaRevision.__table__,
    CustomImportDefinitionRevision.__table__,
    CustomImportCaptureBundle.__table__,
    CustomImportExecution.__table__,
    CustomImportLease.__table__,
)


def _postgres_url():
    raw_dsn = str(os.getenv(_DSN_ENV) or "").strip()
    if not raw_dsn:
        pytest.skip(f"set {_DSN_ENV} for the isolated PostgreSQL proof")
    url = make_url(raw_dsn)
    database_name = str(url.database or "")
    if (
        not url.drivername.startswith("postgresql")
        or not url.host
        or not url.username
        or "test" not in database_name.lower()
    ):
        pytest.fail(f"{_DSN_ENV} must identify a dedicated PostgreSQL test database")
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+asyncpg")
    if url.drivername != "postgresql+asyncpg":
        pytest.fail(f"{_DSN_ENV} must use the PostgreSQL asyncpg dialect")
    return url


@dataclass(frozen=True)
class _PostgresCase:
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    schema_name: str
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int


def _quoted_identifier(identifier: str) -> str:
    assert identifier.startswith("custom_import_execution_")
    assert identifier.replace("_", "").isalnum()
    return f'"{identifier}"'


def _downgrade_request_identity(sync_connection, schema_name: str) -> None:
    spec = importlib.util.spec_from_file_location(
        "custom_import_request_identity_downgrade", _REQUEST_IDENTITY_MIGRATION_PATH
    )
    assert spec and spec.loader
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(sync_connection))
    migration.downgrade()


@asynccontextmanager
async def _postgres_case() -> AsyncIterator[_PostgresCase]:
    url = _postgres_url()
    raw_engine = create_async_engine(url, pool_pre_ping=True)
    schema_name = f"custom_import_execution_{uuid.uuid4().hex[:16]}"
    engine = raw_engine.execution_options(schema_translate_map={"mrf": schema_name})
    sessions = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    schema = _quoted_identifier(schema_name)
    is_schema_created = False
    try:
        async with raw_engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {schema}"))
        is_schema_created = True
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=_TABLES,
                    checkfirst=False,
                )
            )
        async with sessions() as session:
            async with session.begin():
                dataset = CustomImportDataset(dataset_key="synthetic_execution")
                session.add(dataset)
                await session.flush()
                schema_revision = CustomImportSchemaRevision(
                    dataset_id=dataset.dataset_id,
                    revision_number=1,
                    canonical_schema="{}",
                    schema_sha256=hashlib.sha256(b"synthetic-schema").digest(),
                )
                session.add(schema_revision)
                await session.flush()
                definition_revision = CustomImportDefinitionRevision(
                    dataset_id=dataset.dataset_id,
                    schema_revision_id=schema_revision.schema_revision_id,
                    revision_number=1,
                    contract_version="custom-import/v1",
                    refresh_mode="upsert",
                    canonical_definition="{}",
                    definition_sha256=hashlib.sha256(b"synthetic-definition").digest(),
                )
                session.add(definition_revision)
                await session.flush()
                case = _PostgresCase(
                    engine=engine,
                    sessions=sessions,
                    schema_name=schema_name,
                    dataset_id=dataset.dataset_id,
                    definition_revision_id=definition_revision.definition_revision_id,
                    schema_revision_id=schema_revision.schema_revision_id,
                )
        yield case
    finally:
        if is_schema_created:
            async with raw_engine.begin() as connection:
                await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        await raw_engine.dispose()


async def _in_transaction(
    case: _PostgresCase,
    operation: Callable[[AsyncSession], Awaitable[_RESULT]],
) -> _RESULT:
    async with case.sessions() as session:
        async with session.begin():
            return await operation(session)


async def _submit(case: _PostgresCase, *, key: str):
    return await _in_transaction(
        case,
        lambda session: lifecycle.create_execution(
            session,
            dataset_id=case.dataset_id,
            definition_revision_id=case.definition_revision_id,
            schema_revision_id=case.schema_revision_id,
            idempotency_key=key,
            mechanism="queued",
        ),
    )


async def _expire_lease(case: _PostgresCase, execution_id: int) -> None:
    await _in_transaction(
        case,
        lambda session: session.execute(
            update(CustomImportLease)
            .where(CustomImportLease.execution_id == execution_id)
            .values(expires_at=text("clock_timestamp() - interval '1 second'"))
        ),
    )


async def _read_execution_and_lease(case: _PostgresCase, execution_id: int):
    async with case.sessions() as session:
        execution = await session.get(CustomImportExecution, execution_id)
        lease = await session.get(CustomImportLease, execution_id)
        assert execution is not None
        assert lease is not None
        return execution, lease


async def _claim(
    case: _PostgresCase,
    execution_id: int,
    token: str,
    *,
    lease_seconds: int = lifecycle.DEFAULT_LEASE_SECONDS,
):
    return await _in_transaction(
        case,
        lambda session: lifecycle.claim_execution(
            session,
            execution_id=execution_id,
            token=token,
            lease_seconds=lease_seconds,
        ),
    )


async def _heartbeat(case: _PostgresCase, execution_id: int, fence: int, token: str):
    return await _in_transaction(
        case,
        lambda session: lifecycle.heartbeat_execution(
            session,
            execution_id=execution_id,
            fence=fence,
            token=token,
        ),
    )


async def _finish(
    case: _PostgresCase,
    execution_id: int,
    fence: int,
    token: str,
    terminal_state: str,
    *,
    terminal_reason: str | None = None,
):
    return await _in_transaction(
        case,
        lambda session: lifecycle.finish_execution(
            session,
            execution_id=execution_id,
            fence=fence,
            token=token,
            terminal_state=terminal_state,
            terminal_reason=terminal_reason,
        ),
    )


async def _request_cancellation(
    case: _PostgresCase,
    execution_id: int,
    *,
    terminal_reason: str | None = None,
):
    return await _in_transaction(
        case,
        lambda session: lifecycle.request_cancellation(
            session,
            execution_id=execution_id,
            terminal_reason=terminal_reason,
        ),
    )


async def _cancellation_with_stale_completion(
    case: _PostgresCase,
    execution_id: int,
    fence: int,
) -> tuple[object, object]:
    async with case.sessions() as stale_session:
        async with stale_session.begin():
            preloaded = await stale_session.get(CustomImportExecution, execution_id)
            assert preloaded is not None
            assert preloaded.state == "running"
            cancellation = await _request_cancellation(case, execution_id)
            stale_completion = await lifecycle.finish_execution(
                stale_session,
                execution_id=execution_id,
                fence=fence,
                token=_WORKER,
                terminal_state="completed",
            )
            return cancellation, stale_completion


async def _backend_pid(session: AsyncSession) -> int:
    backend_pid = await session.scalar(text("SELECT pg_backend_pid()"))
    assert isinstance(backend_pid, int)
    return backend_pid


async def _wait_for_backend_lock(case: _PostgresCase, *, backend_pid: int) -> None:
    """Require a real PostgreSQL lock wait before releasing the winning transaction."""

    deadline = asyncio.get_running_loop().time() + _LOCK_OBSERVATION_TIMEOUT_SECONDS
    async with case.engine.connect() as monitoring_connection:
        while True:
            wait_event_type = await monitoring_connection.scalar(
                text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :backend_pid"),
                {"backend_pid": backend_pid},
            )
            if wait_event_type == "Lock":
                return
            if asyncio.get_running_loop().time() >= deadline:
                pytest.fail(f"PostgreSQL backend {backend_pid} did not enter a Lock wait")
            await asyncio.sleep(_LOCK_POLL_SECONDS)


async def _cancel_task(task: asyncio.Task[object] | None) -> None:
    if task is not None and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_postgres_duplicate_claim_takeover_and_stale_worker_fences():
    async with _postgres_case() as case:
        first = await _submit(case, key="synthetic-duplicate")
        duplicate = await _submit(case, key="synthetic-duplicate")
        assert first.execution_id == duplicate.execution_id
        assert first.created is True
        assert duplicate.created is False

        grant = await _claim(case, first.execution_id, _WORKER_A, lease_seconds=60)
        assert grant is not None
        competing = await _claim(case, first.execution_id, _WORKER_B)
        assert competing is None

        await _expire_lease(case, first.execution_id)
        takeover = await _in_transaction(
            case,
            lambda session: lifecycle.resume_execution(
                session,
                execution_id=first.execution_id,
                token=_WORKER_B,
                lease_seconds=60,
            ),
        )
        assert takeover is not None
        assert takeover.fence == grant.fence + 1

        stale_heartbeat = await _heartbeat(case, first.execution_id, grant.fence, _WORKER_A)
        current_heartbeat = await _heartbeat(case, first.execution_id, takeover.fence, _WORKER_B)
        stale_finish = await _finish(case, first.execution_id, grant.fence, _WORKER_A, "completed")
        execution, lease = await _read_execution_and_lease(case, first.execution_id)

        assert stale_heartbeat is None
        assert current_heartbeat is not None
        assert stale_finish.changed is False
        assert execution.state == "running"
        assert lease.fence == takeover.fence
        assert lease.token_sha256 == lifecycle.lease_token_sha256(_WORKER_B)
        assert lease.token_sha256 != _WORKER_B.encode()


@pytest.mark.asyncio
async def test_postgres_cancellation_refreshes_stale_identity_maps_and_terminal_state_is_immutable():
    async with _postgres_case() as case:
        submission = await _submit(case, key="synthetic-cancel")
        grant = await _claim(case, submission.execution_id, _WORKER)
        assert grant is not None

        cancellation, stale_completion = await _cancellation_with_stale_completion(
            case,
            submission.execution_id,
            grant.fence,
        )
        assert cancellation.changed is True
        assert stale_completion.changed is False
        assert stale_completion.state == "canceling"

        canceled = await _finish(
            case,
            submission.execution_id,
            grant.fence,
            _WORKER,
            "canceled",
            terminal_reason="requested",
        )
        cancel_after_terminal = await _request_cancellation(case, submission.execution_id)
        failed_after_terminal = await _finish(case, submission.execution_id, grant.fence, _WORKER, "failed")
        execution, lease = await _read_execution_and_lease(case, submission.execution_id)
        now = await _in_transaction(
            case,
            lambda session: session.scalar(select(func.clock_timestamp())),
        )

        assert canceled.changed is True
        assert cancel_after_terminal.changed is False
        assert failed_after_terminal.changed is False
        assert execution.state == "canceled"
        assert execution.terminal_reason == "requested"
        assert lease.fence == grant.fence
        assert lease.token_sha256 == lifecycle.lease_token_sha256(_WORKER)
        assert lease.expires_at is not None
        assert lease.expires_at <= now


@pytest.mark.asyncio
async def test_postgres_running_cancellation_reason_survives_default_worker_acknowledgement():
    async with _postgres_case() as case:
        submission = await _submit(case, key="synthetic-cancel-reason")
        grant = await _claim(case, submission.execution_id, _WORKER)
        assert grant is not None

        cancellation = await _request_cancellation(
            case,
            submission.execution_id,
            terminal_reason="operator_request",
        )
        canceled = await _finish(
            case,
            submission.execution_id,
            grant.fence,
            _WORKER,
            "canceled",
        )
        execution, _lease = await _read_execution_and_lease(case, submission.execution_id)

        assert cancellation.changed is True
        assert canceled.changed is True
        assert execution.state == "canceled"
        assert execution.terminal_reason == "operator_request"


@pytest.mark.asyncio
async def test_postgres_overlapping_duplicate_submissions_wait_on_the_unique_key_and_replay():
    async with _postgres_case() as case:
        async with case.sessions() as first_session, case.sessions() as second_session:
            first_transaction = await first_session.begin()
            second_transaction = await second_session.begin()
            replay_task = None
            try:
                created = await lifecycle.create_execution(
                    first_session,
                    dataset_id=case.dataset_id,
                    definition_revision_id=case.definition_revision_id,
                    schema_revision_id=case.schema_revision_id,
                    idempotency_key="synthetic-overlapping-submission",
                    mechanism="queued",
                )
                first_backend_pid = await _backend_pid(first_session)
                second_backend_pid = await _backend_pid(second_session)
                assert first_backend_pid != second_backend_pid

                replay_task = asyncio.create_task(
                    lifecycle.create_execution(
                        second_session,
                        dataset_id=case.dataset_id,
                        definition_revision_id=case.definition_revision_id,
                        schema_revision_id=case.schema_revision_id,
                        idempotency_key="synthetic-overlapping-submission",
                        mechanism="queued",
                    )
                )
                await _wait_for_backend_lock(case, backend_pid=second_backend_pid)
                assert replay_task.done() is False

                await first_transaction.commit()
                replay = await asyncio.wait_for(replay_task, timeout=_LOCK_OBSERVATION_TIMEOUT_SECONDS)
                await second_transaction.commit()
            finally:
                await _cancel_task(replay_task)
                if second_transaction.is_active:
                    await second_transaction.rollback()
                if first_transaction.is_active:
                    await first_transaction.rollback()

        execution, lease = await _read_execution_and_lease(case, created.execution_id)
        assert created.created is True
        assert replay.created is False
        assert replay.execution_id == created.execution_id
        assert execution.state == "queued"
        assert lease.fence == 0


async def _reserve_overlapping_identity(case, session, request_identity):
    """Reserve the same synthetic command with a caller-supplied identity seal."""

    return await lifecycle.reserve_execution(
        session,
        dataset_id=case.dataset_id,
        definition_revision_id=case.definition_revision_id,
        schema_revision_id=case.schema_revision_id,
        idempotency_key="synthetic-overlapping-request-identity",
        mechanism="local",
        request_identity_sha256=request_identity,
    )


@pytest.mark.asyncio
async def test_postgres_overlapping_request_identity_reservations_wait_and_reject_drift():
    """The unique-key wait must compare a persisted digest after the winner commits."""

    async with _postgres_case() as case:
        request_identity = hashlib.sha256(b"synthetic-overlapping-request-identity").digest()
        conflicting_identity = hashlib.sha256(b"synthetic-conflicting-request-identity").digest()
        async with case.sessions() as first_session, case.sessions() as second_session:
            first_transaction = await first_session.begin()
            second_transaction = await second_session.begin()
            replay_task = None
            try:
                created = await _reserve_overlapping_identity(case, first_session, request_identity)
                second_backend_pid = await _backend_pid(second_session)
                replay_task = asyncio.create_task(_reserve_overlapping_identity(case, second_session, request_identity))
                await _wait_for_backend_lock(case, backend_pid=second_backend_pid)
                assert replay_task.done() is False

                await first_transaction.commit()
                replay = await asyncio.wait_for(replay_task, timeout=_LOCK_OBSERVATION_TIMEOUT_SECONDS)
                await second_transaction.commit()
            finally:
                await _cancel_task(replay_task)
                if second_transaction.is_active:
                    await second_transaction.rollback()
                if first_transaction.is_active:
                    await first_transaction.rollback()

        execution, lease = await _read_execution_and_lease(case, created.execution_id)
        assert created.created is True
        assert replay.created is False
        assert replay.execution_id == created.execution_id
        assert execution.request_identity_sha256 == request_identity
        assert lease.fence == 0

        with pytest.raises(lifecycle.IdempotencyConflict):
            await _in_transaction(
                case,
                lambda session: _reserve_overlapping_identity(case, session, conflicting_identity),
            )


@pytest.mark.asyncio
async def test_postgres_request_identity_storage_shape_and_downgrade_guard():
    async with _postgres_case() as case:
        request_identity = hashlib.sha256(b"synthetic-request-identity-storage").digest()
        submission = await _in_transaction(
            case,
            lambda session: lifecycle.reserve_execution(
                session,
                dataset_id=case.dataset_id,
                definition_revision_id=case.definition_revision_id,
                schema_revision_id=case.schema_revision_id,
                idempotency_key="synthetic-request-identity-storage",
                mechanism="local",
                request_identity_sha256=request_identity,
            ),
        )

        with pytest.raises(DBAPIError):
            await _in_transaction(
                case,
                lambda session: session.execute(
                    update(CustomImportExecution)
                    .where(CustomImportExecution.execution_id == submission.execution_id)
                    .values(request_identity_sha256=b"short")
                ),
            )
        with pytest.raises(DBAPIError, match="custom_import_execution_request_identity_downgrade_blocked"):
            async with case.engine.begin() as connection:
                await connection.run_sync(_downgrade_request_identity, case.schema_name)

        execution, _lease = await _read_execution_and_lease(case, submission.execution_id)
        assert execution.request_identity_sha256 == request_identity


@pytest.mark.asyncio
async def test_postgres_overlapping_claims_wait_and_grant_one_lease():
    async with _postgres_case() as case:
        submission = await _submit(case, key="synthetic-overlapping-claim")

        async with case.sessions() as first_session, case.sessions() as second_session:
            first_transaction = await first_session.begin()
            second_transaction = await second_session.begin()
            losing_claim_task = None
            try:
                winning_claim = await lifecycle.claim_execution(
                    first_session,
                    execution_id=submission.execution_id,
                    token=_WORKER_A,
                    lease_seconds=60,
                )
                assert winning_claim is not None
                first_backend_pid = await _backend_pid(first_session)
                second_backend_pid = await _backend_pid(second_session)
                assert first_backend_pid != second_backend_pid

                losing_claim_task = asyncio.create_task(
                    lifecycle.claim_execution(
                        second_session,
                        execution_id=submission.execution_id,
                        token=_WORKER_B,
                        lease_seconds=60,
                    )
                )
                await _wait_for_backend_lock(case, backend_pid=second_backend_pid)
                assert losing_claim_task.done() is False

                await first_transaction.commit()
                losing_claim = await asyncio.wait_for(
                    losing_claim_task,
                    timeout=_LOCK_OBSERVATION_TIMEOUT_SECONDS,
                )
                await second_transaction.commit()
            finally:
                await _cancel_task(losing_claim_task)
                if second_transaction.is_active:
                    await second_transaction.rollback()
                if first_transaction.is_active:
                    await first_transaction.rollback()

        execution, lease = await _read_execution_and_lease(case, submission.execution_id)
        assert winning_claim.fence == 1
        assert losing_claim is None
        assert execution.state == "running"
        assert lease.fence == winning_claim.fence
        assert lease.token_sha256 == lifecycle.lease_token_sha256(_WORKER_A)


@pytest.mark.asyncio
async def test_claim_waits_on_dataset_before_execution_lock():
    async with _postgres_case() as case:
        submission = await _submit(case, key="synthetic-dataset-first-lock")
        async with case.sessions() as dataset_holder, case.sessions() as claimant, case.sessions() as inspector:
            holder_transaction = await dataset_holder.begin()
            claimant_transaction = await claimant.begin()
            inspection_transaction = None
            claim_task = None
            try:
                await dataset_holder.execute(
                    select(CustomImportDataset)
                    .where(CustomImportDataset.dataset_id == case.dataset_id)
                    .with_for_update()
                )
                claimant_backend_pid = await _backend_pid(claimant)
                claim_task = asyncio.create_task(
                    lifecycle.claim_execution(
                        claimant,
                        execution_id=submission.execution_id,
                        token=_WORKER_A,
                    )
                )
                await _wait_for_backend_lock(case, backend_pid=claimant_backend_pid)

                inspection_transaction = await inspector.begin()
                execution = (
                    await inspector.execute(
                        select(CustomImportExecution)
                        .where(CustomImportExecution.execution_id == submission.execution_id)
                        .with_for_update(nowait=True)
                    )
                ).scalar_one()
                assert execution.execution_id == submission.execution_id
                await inspection_transaction.commit()
                inspection_transaction = None

                await holder_transaction.commit()
                claim = await asyncio.wait_for(claim_task, timeout=_LOCK_OBSERVATION_TIMEOUT_SECONDS)
                await claimant_transaction.commit()
                assert claim is not None
                assert claim.fence == 1
            finally:
                await _cancel_task(claim_task)
                if inspection_transaction is not None and inspection_transaction.is_active:
                    await inspection_transaction.rollback()
                if claimant_transaction.is_active:
                    await claimant_transaction.rollback()
                if holder_transaction.is_active:
                    await holder_transaction.rollback()
