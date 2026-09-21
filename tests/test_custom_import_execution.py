# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic lifecycle coverage for source-neutral custom-import executions."""

from __future__ import annotations

import datetime as dt
import hashlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from process.custom_import import execution as lifecycle

UTC = dt.UTC
_WORKER = "synthetic-worker"
_WORKER_A = "synthetic-worker-a"
_WORKER_B = "synthetic-worker-b"
_WORKER_A_BYTES = _WORKER_A.encode()
_WORKER_B_BYTES = _WORKER_B.encode()


class _Result:
    def __init__(self, value: Any = None):
        self.value = value

    def scalar_one_or_none(self):
        return self.value

    def scalar_one(self):
        assert self.value is not None
        return self.value


class _SyntheticConnection:
    """Minimal connection transaction identity for lifecycle-boundary tests."""

    def __init__(self):
        self.info: dict[str, Any] = {}
        self.root_transaction = object()

    def get_transaction(self):
        return self.root_transaction


def _statement_values(statement: Any) -> dict[str, Any]:
    return {column.name: getattr(value, "value", value) for column, value in getattr(statement, "_values", {}).items()}


def _where_values(statement: Any) -> dict[str, Any]:
    return {
        criterion.left.name: getattr(criterion.right, "value", None)
        for criterion in getattr(statement, "_where_criteria", ())
    }


class _SyntheticSession:
    """Small SQLAlchemy-statement interpreter for lifecycle state-machine tests."""

    def __init__(self, connection: _SyntheticConnection | None = None):
        self.now = dt.datetime(2026, 9, 17, 12, 0, tzinfo=UTC)
        self.info: dict[str, Any] = {}
        self._transaction = object()
        self._connection = connection or _SyntheticConnection()
        self.executions: dict[int, SimpleNamespace] = {}
        self.execution_by_request: dict[tuple[int, str], SimpleNamespace] = {}
        self.leases: dict[int, SimpleNamespace] = {}
        self.statements: list[Any] = []
        self._next_execution_id = 1

    def in_transaction(self) -> bool:
        return True

    def get_transaction(self):
        return self._transaction

    async def connection(self):
        return self._connection

    async def execute(self, statement: Any):
        self.statements.append(statement)
        if getattr(statement, "is_insert", False):
            return self._insert(statement)
        if getattr(statement, "is_update", False):
            return self._update(statement)
        if getattr(statement, "is_select", False):
            return self._select(statement)
        raise AssertionError(f"unexpected lifecycle statement: {statement!r}")

    def _insert(self, statement: Any) -> _Result:
        table_name = statement.table.name
        column_values = _statement_values(statement)
        if table_name == "custom_import_execution":
            request = (column_values["definition_revision_id"], column_values["idempotency_key"])
            if request in self.execution_by_request:
                return _Result()
            execution_id = self._next_execution_id
            self._next_execution_id += 1
            execution = SimpleNamespace(
                execution_id=execution_id,
                dataset_id=column_values["dataset_id"],
                definition_revision_id=column_values["definition_revision_id"],
                schema_revision_id=column_values["schema_revision_id"],
                idempotency_key=column_values["idempotency_key"],
                mechanism=column_values["mechanism"],
                state=column_values["state"],
                capture_bundle_id=column_values.get("capture_bundle_id"),
                terminal_reason=None,
                started_at=None,
                finished_at=None,
                updated_at=None,
            )
            self.executions[execution_id] = execution
            self.execution_by_request[request] = execution
            return _Result(execution_id)
        if table_name == "custom_import_lease":
            execution_id = column_values["execution_id"]
            self.leases.setdefault(
                execution_id,
                SimpleNamespace(
                    execution_id=execution_id,
                    fence=column_values.get("fence", 0),
                    token_sha256=None,
                    heartbeat_at=None,
                    expires_at=None,
                    updated_at=None,
                ),
            )
            return _Result()
        raise AssertionError(f"unexpected insert table: {table_name}")

    def _select(self, statement: Any) -> _Result:
        if "clock_timestamp" in str(statement):
            return _Result(self.now)
        table_name = statement.get_final_froms()[0].name
        where = _where_values(statement)
        if table_name == "custom_import_dataset":
            return _Result(SimpleNamespace(dataset_id=where["dataset_id"]))
        if table_name == "custom_import_execution":
            if "execution_id" in where:
                return _Result(self.executions.get(where["execution_id"]))
            return _Result(self.execution_by_request.get((where["definition_revision_id"], where["idempotency_key"])))
        if table_name == "custom_import_lease":
            return _Result(self.leases.get(where["execution_id"]))
        raise AssertionError(f"unexpected select table: {table_name}")

    def _update(self, statement: Any) -> _Result:
        table_name = statement.table.name
        execution_id = _where_values(statement)["execution_id"]
        record = self.executions[execution_id] if table_name == "custom_import_execution" else self.leases[execution_id]
        for name, value in _statement_values(statement).items():
            setattr(record, name, value)
        return _Result()


async def _submission(session: _SyntheticSession, *, key: str = "synthetic-request"):
    return await lifecycle.create_execution(
        session,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        idempotency_key=key,
        mechanism="queued",
        capture_bundle_id=None,
    )


@pytest.mark.asyncio
async def test_duplicate_submission_returns_the_same_execution_and_rejects_drift():
    session = _SyntheticSession()

    first = await _submission(session)
    duplicate = await _submission(session)

    assert first.execution_id == duplicate.execution_id
    assert first.created is True
    assert duplicate.created is False
    assert session.leases[first.execution_id].fence == 0
    with pytest.raises(lifecycle.IdempotencyConflict):
        await lifecycle.create_execution(
            session,
            dataset_id=11,
            definition_revision_id=22,
            schema_revision_id=33,
            idempotency_key="synthetic-request",
            mechanism="external",
        )


@pytest.mark.asyncio
async def test_reservation_reuses_its_bound_execution_without_replacing_the_bundle():
    session = _SyntheticSession()

    reserved = await lifecycle.reserve_execution(
        session,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        idempotency_key="synthetic-reservation",
        mechanism="local",
    )
    first_owner = await lifecycle.claim_execution(
        session,
        execution_id=reserved.execution_id,
        token=_WORKER_A,
    )
    assert first_owner is not None
    bound = await lifecycle.bind_execution_capture_bundle(
        session,
        execution_id=reserved.execution_id,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        capture_bundle_id=44,
        fence=first_owner.fence,
        token=_WORKER_A,
    )
    replay = await lifecycle.reserve_execution(
        session,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        idempotency_key="synthetic-reservation",
        mechanism="local",
    )

    assert reserved.created is True and reserved.capture_bundle_id is None
    assert bound is not None
    assert bound.execution_id == reserved.execution_id and bound.capture_bundle_id == 44
    assert replay == lifecycle.ExecutionSubmission(
        execution_id=reserved.execution_id,
        state="running",
        created=False,
        capture_bundle_id=44,
    )
    with pytest.raises(lifecycle.IdempotencyConflict):
        await lifecycle.bind_execution_capture_bundle(
            session,
            execution_id=reserved.execution_id,
            dataset_id=11,
            definition_revision_id=22,
            schema_revision_id=33,
            capture_bundle_id=45,
            fence=first_owner.fence,
            token=_WORKER_A,
        )

    canceled = await lifecycle.reserve_execution(
        session,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        idempotency_key="synthetic-canceled-reservation",
        mechanism="local",
    )
    canceled_owner = await lifecycle.claim_execution(
        session,
        execution_id=canceled.execution_id,
        token=_WORKER_A,
    )
    assert canceled_owner is not None
    await lifecycle.request_cancellation(session, execution_id=canceled.execution_id)
    unavailable = await lifecycle.bind_execution_capture_bundle(
        session,
        execution_id=canceled.execution_id,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        capture_bundle_id=46,
        fence=canceled_owner.fence,
        token=_WORKER_A,
    )
    assert unavailable is None


@pytest.mark.asyncio
async def test_reservation_binding_rejects_a_stale_owner_after_lease_takeover():
    session = _SyntheticSession()
    reserved = await lifecycle.reserve_execution(
        session,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        idempotency_key="synthetic-reservation-takeover",
        mechanism="local",
    )
    first_owner = await lifecycle.claim_execution(
        session,
        execution_id=reserved.execution_id,
        token=_WORKER_A,
        lease_seconds=60,
    )
    assert first_owner is not None
    session.now = first_owner.expires_at
    recovery_owner = await lifecycle.claim_execution(
        session,
        execution_id=reserved.execution_id,
        token=_WORKER_B,
    )
    assert recovery_owner is not None and recovery_owner.fence == first_owner.fence + 1

    stale = await lifecycle.bind_execution_capture_bundle(
        session,
        execution_id=reserved.execution_id,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        capture_bundle_id=44,
        fence=first_owner.fence,
        token=_WORKER_A,
    )
    recovered = await lifecycle.bind_execution_capture_bundle(
        session,
        execution_id=reserved.execution_id,
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        capture_bundle_id=44,
        fence=recovery_owner.fence,
        token=_WORKER_B,
    )

    assert stale is None
    assert recovered is not None and recovered.capture_bundle_id == 44


@pytest.mark.asyncio
async def test_competing_claims_leave_only_the_first_worker_authorized():
    session = _SyntheticSession()
    submission = await _submission(session)

    first = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER_A_BYTES,
        lease_seconds=60,
    )
    competing = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER_B_BYTES,
        lease_seconds=60,
    )

    assert first is not None
    assert competing is None
    lease = session.leases[submission.execution_id]
    assert lease.fence == 1
    assert lease.token_sha256 == hashlib.sha256(_WORKER_A_BYTES).digest()
    assert lease.token_sha256 != _WORKER_A_BYTES


@pytest.mark.asyncio
async def test_expired_execution_takeover_advances_the_fence_without_resetting_state():
    session = _SyntheticSession()
    submission = await _submission(session)
    first = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER_A,
        lease_seconds=60,
    )
    assert first is not None
    session.now = first.expires_at

    takeover = await lifecycle.resume_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER_B,
        lease_seconds=90,
    )

    assert takeover is not None
    assert takeover.fence == first.fence + 1
    assert takeover.state == "running"
    assert session.executions[submission.execution_id].started_at == dt.datetime(
        2026,
        9,
        17,
        12,
        0,
        tzinfo=UTC,
    )


@pytest.mark.asyncio
async def test_stale_fence_or_token_cannot_heartbeat_or_finish_after_takeover():
    session = _SyntheticSession()
    submission = await _submission(session)
    first = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER_A,
        lease_seconds=60,
    )
    assert first is not None
    session.now = first.expires_at
    takeover = await lifecycle.resume_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER_B,
    )
    assert takeover is not None

    stale_heartbeat = await lifecycle.heartbeat_execution(
        session,
        execution_id=submission.execution_id,
        fence=first.fence,
        token=_WORKER_A,
    )
    stale_finish = await lifecycle.finish_execution(
        session,
        execution_id=submission.execution_id,
        fence=first.fence,
        token=_WORKER_A,
        terminal_state="completed",
    )

    assert stale_heartbeat is None
    assert stale_finish.changed is False
    assert stale_finish.state == "running"


@pytest.mark.asyncio
async def test_current_holder_can_heartbeat_after_takeover_but_old_holder_cannot():
    session = _SyntheticSession()
    submission = await _submission(session)
    first = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER_A,
        lease_seconds=60,
    )
    assert first is not None
    session.now = first.expires_at
    takeover = await lifecycle.resume_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER_B,
        lease_seconds=60,
    )
    assert takeover is not None
    session.now += dt.timedelta(seconds=1)

    old_heartbeat = await lifecycle.heartbeat_execution(
        session,
        execution_id=submission.execution_id,
        fence=first.fence,
        token=_WORKER_A,
    )
    renewed = await lifecycle.heartbeat_execution(
        session,
        execution_id=submission.execution_id,
        fence=takeover.fence,
        token=_WORKER_B,
        lease_seconds=120,
    )

    assert old_heartbeat is None
    assert renewed is not None
    assert renewed.fence == takeover.fence
    assert renewed.expires_at == session.now + dt.timedelta(seconds=120)


@pytest.mark.asyncio
async def test_cancellation_wins_over_later_completion_but_allows_canceled_terminal_state():
    session = _SyntheticSession()
    submission = await _submission(session)
    grant = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER,
    )
    assert grant is not None

    cancellation = await lifecycle.request_cancellation(
        session,
        execution_id=submission.execution_id,
    )
    completion = await lifecycle.finish_execution(
        session,
        execution_id=submission.execution_id,
        fence=grant.fence,
        token=_WORKER,
        terminal_state="completed",
    )
    canceled = await lifecycle.finish_execution(
        session,
        execution_id=submission.execution_id,
        fence=grant.fence,
        token=_WORKER,
        terminal_state="canceled",
        terminal_reason="requested",
    )

    assert cancellation == lifecycle.ExecutionTransition(
        execution_id=submission.execution_id,
        state="canceling",
        changed=True,
    )
    assert completion.changed is False
    assert canceled.changed is True
    assert canceled.state == "canceled"


@pytest.mark.asyncio
async def test_running_cancellation_reason_survives_a_default_canceled_acknowledgement():
    session = _SyntheticSession()
    submission = await _submission(session)
    grant = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER,
    )
    assert grant is not None

    cancellation = await lifecycle.request_cancellation(
        session,
        execution_id=submission.execution_id,
        terminal_reason="operator_request",
    )
    canceled = await lifecycle.finish_execution(
        session,
        execution_id=submission.execution_id,
        fence=grant.fence,
        token=_WORKER,
        terminal_state="canceled",
    )

    assert cancellation.changed is True
    assert canceled.changed is True
    assert session.executions[submission.execution_id].terminal_reason == "operator_request"


def test_lifecycle_identifiers_and_fences_reject_postgresql_bigint_overflow():
    assert lifecycle._positive_id(lifecycle.MAX_BIGINT, "synthetic_id") == lifecycle.MAX_BIGINT
    assert lifecycle._fence(lifecycle.MAX_BIGINT) == lifecycle.MAX_BIGINT
    with pytest.raises(ValueError, match="positive integer"):
        lifecycle._positive_id(lifecycle.MAX_BIGINT + 1, "synthetic_id")
    with pytest.raises(ValueError, match="positive integer"):
        lifecycle._fence(lifecycle.MAX_BIGINT + 1)


@pytest.mark.asyncio
async def test_terminal_state_is_immutable_and_expiration_keeps_the_fence_and_digest():
    session = _SyntheticSession()
    submission = await _submission(session)
    grant = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER,
    )
    assert grant is not None
    digest = session.leases[submission.execution_id].token_sha256

    completed = await lifecycle.finish_execution(
        session,
        execution_id=submission.execution_id,
        fence=grant.fence,
        token=_WORKER,
        terminal_state="completed",
    )
    cancel_after_completion = await lifecycle.request_cancellation(
        session,
        execution_id=submission.execution_id,
    )
    failed_after_completion = await lifecycle.finish_execution(
        session,
        execution_id=submission.execution_id,
        fence=grant.fence,
        token=_WORKER,
        terminal_state="failed",
    )

    lease = session.leases[submission.execution_id]
    assert completed.changed is True
    assert cancel_after_completion.changed is False
    assert failed_after_completion.changed is False
    assert session.executions[submission.execution_id].state == "completed"
    assert lease.fence == grant.fence
    assert lease.token_sha256 == digest
    assert lease.expires_at == session.now


@pytest.mark.asyncio
async def test_lifecycle_rejects_a_session_without_a_caller_owned_transaction():
    class _NoTransaction:
        def in_transaction(self):
            return False

    with pytest.raises(lifecycle.ExecutionTransactionRequired):
        await lifecycle.create_execution(
            _NoTransaction(),
            dataset_id=11,
            definition_revision_id=22,
            schema_revision_id=33,
            idempotency_key="synthetic-request",
            mechanism="queued",
        )


def test_lease_token_hashing_is_bounded_and_never_returns_the_raw_token():
    digest = lifecycle.lease_token_sha256(_WORKER)

    assert digest == hashlib.sha256(_WORKER.encode()).digest()
    assert digest != _WORKER.encode()
    with pytest.raises(ValueError, match="from 1 through"):
        lifecycle.lease_token_sha256("")
    with pytest.raises(ValueError, match="text or bytes"):
        lifecycle.lease_token_sha256(42)


@pytest.mark.asyncio
async def test_lock_loaders_force_refresh_of_preloaded_identity_map_rows():
    session = _SyntheticSession()
    submission = await _submission(session)

    await lifecycle._lock_execution(session, submission.execution_id)
    await lifecycle._lock_execution_by_request(
        session,
        definition_revision_id=22,
        idempotency_key="synthetic-request",
    )
    await lifecycle._lock_lease(session, submission.execution_id)

    lock_statements = [
        statement
        for statement in session.statements
        if getattr(statement, "is_select", False) and "clock_timestamp" not in str(statement)
    ]
    assert lock_statements
    assert all(statement.get_execution_options().get("populate_existing") is True for statement in lock_statements)


@pytest.mark.asyncio
async def test_claim_locks_dataset_execution_and_lease_before_clock():
    session = _SyntheticSession()
    submission = await _submission(session)
    statement_offset = len(session.statements)

    grant = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER,
    )

    assert grant is not None
    statements = session.statements[statement_offset:]
    clock_index = next(index for index, statement in enumerate(statements) if "clock_timestamp" in str(statement))
    selected_tables = [
        statement.get_final_froms()[0].name
        for statement in statements[:clock_index]
        if getattr(statement, "is_select", False)
    ]
    locked_tables = [
        statement.get_final_froms()[0].name
        for statement in statements[:clock_index]
        if getattr(statement, "_for_update_arg", None) is not None
    ]
    assert selected_tables == [
        "custom_import_execution",
        "custom_import_dataset",
        "custom_import_execution",
        "custom_import_lease",
    ]
    assert locked_tables == [
        "custom_import_dataset",
        "custom_import_execution",
        "custom_import_lease",
    ]


@pytest.mark.asyncio
async def test_generic_lifecycle_refuses_no_change_terminal_transition():
    session = _SyntheticSession()
    submission = await _submission(session)
    grant = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER,
    )
    assert grant is not None

    with pytest.raises(ValueError, match="canceled, failed, or completed"):
        await lifecycle.finish_execution(
            session,
            execution_id=submission.execution_id,
            fence=grant.fence,
            token=_WORKER,
            terminal_state="no_change",
        )


@pytest.mark.asyncio
async def test_any_lifecycle_work_marks_same_transaction_publication_as_unsafe():
    session = _SyntheticSession()
    submission = await _submission(session)
    grant = await lifecycle.claim_execution(
        session,
        execution_id=submission.execution_id,
        token=_WORKER,
    )
    assert grant is not None
    completed = await lifecycle.finish_execution(
        session,
        execution_id=submission.execution_id,
        fence=grant.fence,
        token=_WORKER,
        terminal_state="completed",
    )
    assert completed.changed is True

    with pytest.raises(lifecycle.ExecutionLifecycleError, match="commit execution lifecycle work"):
        await lifecycle.require_separate_publication_transaction(session)

    session._transaction = object()
    with pytest.raises(lifecycle.ExecutionLifecycleError, match="commit execution lifecycle work"):
        await lifecycle.require_separate_publication_transaction(session)

    session._connection.root_transaction = object()
    await lifecycle.require_separate_publication_transaction(session)


@pytest.mark.asyncio
async def test_lifecycle_marker_blocks_second_session_until_outer_transaction_changes():
    connection = _SyntheticConnection()
    lifecycle_session = _SyntheticSession(connection)
    await _submission(lifecycle_session)

    lifecycle_session._transaction = object()
    publication_session = _SyntheticSession(connection)
    with pytest.raises(lifecycle.ExecutionLifecycleError, match="commit execution lifecycle work"):
        await lifecycle.require_separate_publication_transaction(publication_session)

    connection.root_transaction = object()
    publication_session._transaction = object()
    await lifecycle.require_separate_publication_transaction(publication_session)


@pytest.mark.parametrize(
    "operation",
    (
        lambda: lifecycle._bounded_text("", "value", maximum=8),
        lambda: lifecycle._bounded_text(" padded", "value", maximum=8),
        lambda: lifecycle._idempotency_key("key\x7f"),
        lambda: lifecycle._mechanism("other"),
        lambda: lifecycle._lease_seconds(0),
        lambda: lifecycle._persisted_digest("digest"),
        lambda: lifecycle._persisted_digest(b"short"),
        lambda: lifecycle._validate_execution_state(SimpleNamespace(state="unknown")),
        lambda: lifecycle._validate_lease(SimpleNamespace(fence=-1, token_sha256=None, expires_at=None)),
        lambda: lifecycle._validate_lease(SimpleNamespace(fence=0, token_sha256=b"x" * 32, expires_at=None)),
        lambda: lifecycle._validate_lease(
            SimpleNamespace(
                fence=1,
                token_sha256=b"x" * 32,
                expires_at=dt.datetime(2026, 9, 17, 12, 0),
            )
        ),
    ),
)
def test_lifecycle_validation_guards_fail_closed(operation):
    with pytest.raises((ValueError, lifecycle.ExecutionInvariantError)):
        operation()


@pytest.mark.asyncio
async def test_root_transaction_and_clean_session_guards_fail_closed():
    class _NoConnection:
        connection = None

    class _ConnectionWithoutTransaction:
        async def connection(self):
            return SimpleNamespace(get_transaction=None)

    class _ConnectionWithoutRoot:
        async def connection(self):
            return SimpleNamespace(get_transaction=lambda: None)

    class _ConnectionWithoutInfo:
        async def connection(self):
            return SimpleNamespace(get_transaction=lambda: object(), info="invalid")

    class _NoSessionTransaction:
        info: dict[str, Any] = {}

        def get_transaction(self):
            return None

    assert await lifecycle._root_transaction_context(_NoConnection()) == (None, None)
    assert await lifecycle._root_transaction_context(_ConnectionWithoutTransaction()) == (None, None)
    with pytest.raises(lifecycle.ExecutionTransactionRequired):
        await lifecycle._root_transaction_context(_ConnectionWithoutRoot())
    root, info = await lifecycle._root_transaction_context(_ConnectionWithoutInfo())
    assert root is not None and info is None
    with pytest.raises(lifecycle.ExecutionTransactionRequired):
        await lifecycle._mark_lifecycle_transaction(_NoSessionTransaction())
    with pytest.raises(lifecycle.ExecutionInvariantError, match="clean session"):
        lifecycle._require_clean_lifecycle_session(SimpleNamespace(new={object()}))


@pytest.mark.asyncio
async def test_lifecycle_storage_guards_cover_disappearing_rows_and_exhausted_authority():
    session = _SyntheticSession()
    submission = await _submission(session)
    await lifecycle._lock_execution_by_request(
        session,
        dataset_id=11,
        definition_revision_id=22,
        idempotency_key="synthetic-request",
    )

    class _MissingDatasetSession(_SyntheticSession):
        def _select(self, statement):
            if statement.get_final_froms()[0].name == "custom_import_dataset":
                return _Result()
            return super()._select(statement)

    with pytest.raises(lifecycle.ExecutionInvariantError, match="dataset does not exist"):
        await lifecycle._lock_dataset(_MissingDatasetSession(), 11)
    with pytest.raises(lifecycle.ExecutionNotFound):
        await lifecycle._locked_lifecycle_execution(_SyntheticSession(), 404)
    with pytest.raises(lifecycle.ExecutionNotFound):
        await lifecycle._locked_heartbeat_execution(_SyntheticSession(), 404)

    del session.leases[submission.execution_id]
    assert (await lifecycle._lock_lease(session, submission.execution_id)).fence == 0
    session.now = session.now.replace(tzinfo=None)
    with pytest.raises(lifecycle.ExecutionInvariantError, match="aware timestamp"):
        await lifecycle._database_now(session)

    empty = _SyntheticSession()
    request = lifecycle._validated_execution_request(
        dataset_id=11,
        definition_revision_id=22,
        schema_revision_id=33,
        idempotency_key="missing",
        mechanism="queued",
        capture_bundle_id=None,
    )
    assert await lifecycle._locked_submission_execution(empty, request, None) is None

    queued = _SyntheticSession()
    queued_submission = await _submission(queued)
    assert (
        await lifecycle.resume_execution(
            queued,
            execution_id=queued_submission.execution_id,
            token=_WORKER,
        )
        is None
    )
    queued.leases[queued_submission.execution_id].fence = lifecycle.MAX_FENCE
    queued.leases[queued_submission.execution_id].token_sha256 = hashlib.sha256(_WORKER.encode()).digest()
    queued.leases[queued_submission.execution_id].expires_at = queued.now
    with pytest.raises(lifecycle.ExecutionInvariantError, match="cannot advance"):
        await lifecycle.claim_execution(
            queued,
            execution_id=queued_submission.execution_id,
            token=_WORKER,
        )


@pytest.mark.asyncio
async def test_queued_cancellation_expires_only_an_existing_lease_authority():
    without_authority = _SyntheticSession()
    first = await _submission(without_authority)
    assert (
        await lifecycle.request_cancellation(
            without_authority,
            execution_id=first.execution_id,
        )
    ).state == "canceled"

    with_authority = _SyntheticSession()
    second = await _submission(with_authority)
    lease = with_authority.leases[second.execution_id]
    lease.fence = 1
    lease.token_sha256 = hashlib.sha256(_WORKER.encode()).digest()
    lease.expires_at = with_authority.now + dt.timedelta(seconds=60)
    assert (
        await lifecycle.request_cancellation(
            with_authority,
            execution_id=second.execution_id,
        )
    ).state == "canceled"
    assert lease.expires_at == with_authority.now


@pytest.mark.asyncio
async def test_lifecycle_guards_handle_unbacked_transactions_and_disappearing_rows(monkeypatch):
    transaction = object()
    unbacked_session = SimpleNamespace(
        get_transaction=lambda: transaction,
        info={},
    )
    await lifecycle._mark_lifecycle_transaction(unbacked_session)
    marker = unbacked_session.info[lifecycle._LIFECYCLE_TRANSACTION_MARKER]
    assert marker.session_transaction is transaction
    assert marker.root_transaction is None

    clean_session = SimpleNamespace(
        get_transaction=lambda: object(),
        info={},
    )
    await lifecycle.require_separate_publication_transaction(clean_session)
    assert clean_session.info == {}

    monkeypatch.setattr(
        lifecycle,
        "_execution_snapshot",
        AsyncMock(return_value=SimpleNamespace(dataset_id=11)),
    )
    monkeypatch.setattr(lifecycle, "_lock_dataset", AsyncMock())
    monkeypatch.setattr(lifecycle, "_lock_execution", AsyncMock(return_value=None))
    with pytest.raises(lifecycle.ExecutionNotFound, match="404"):
        await lifecycle._locked_lifecycle_execution(SimpleNamespace(), 404)


@pytest.mark.asyncio
async def test_lifecycle_storage_fails_when_required_rows_disappear(monkeypatch):
    class _SequencedSession:
        def __init__(self):
            self.results = iter((_Result(), _Result(), _Result()))

        async def execute(self, _statement):
            return next(self.results)

    with pytest.raises(lifecycle.ExecutionInvariantError, match="lease could not be created"):
        await lifecycle._lock_lease(_SequencedSession(), 7)

    session = _SyntheticSession()
    monkeypatch.setattr(lifecycle, "_lock_dataset", AsyncMock())
    monkeypatch.setattr(lifecycle, "_insert_execution", AsyncMock(return_value=101))
    monkeypatch.setattr(lifecycle, "_locked_submission_execution", AsyncMock(return_value=None))
    with pytest.raises(lifecycle.ExecutionInvariantError, match="row disappeared"):
        await lifecycle.create_execution(
            session,
            dataset_id=11,
            definition_revision_id=22,
            schema_revision_id=33,
            idempotency_key="disappearing-row",
            mechanism="queued",
        )
