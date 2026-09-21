# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Caller-owned execution and fenced-lease lifecycle for ``custom-import/v1``.

The public coroutines deliberately neither begin nor finish a transaction.  A
coordinator supplies an active :class:`~sqlalchemy.ext.asyncio.AsyncSession`,
so an execution state change can remain atomic with its caller's surrounding
work.  Lease authority is represented by a monotonic fence plus a SHA-256
digest; raw lease tokens never enter the database or a returned value.

A lifecycle transaction is scoped to one execution.  Callers that batch
multiple executions must acquire them in a deterministic order.  State
transitions serialize through the dataset before their execution and lease;
the narrowly scoped heartbeat is deliberately execution-and-lease-only so one
large finality scan cannot starve another live execution.  Before any
generation publication, the underlying database transaction—not only a
savepoint owned by an externally joined session—must end.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
from collections.abc import MutableMapping
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import CustomImportDataset, CustomImportExecution, CustomImportLease

DEFAULT_LEASE_SECONDS = 300
MAX_LEASE_SECONDS = 3_600
MAX_TOKEN_BYTES = 4_096
MAX_BIGINT = 9_223_372_036_854_775_807
MAX_FENCE = MAX_BIGINT

EXECUTION_STATES = frozenset({"queued", "running", "canceling", "canceled", "failed", "completed", "no_change"})
TERMINAL_STATES = frozenset({"canceled", "failed", "completed", "no_change"})
_LEASED_TERMINAL_STATES = frozenset({"canceled", "failed", "completed"})
_ACTIVE_STATES = frozenset({"queued", "running", "canceling"})
_HEARTBEAT_STATES = frozenset({"running", "canceling"})
_MECHANISMS = frozenset({"local", "queued", "external"})
_LIFECYCLE_TRANSACTION_MARKER = "custom_import_execution_lifecycle_transaction"


@dataclass(frozen=True)
class _LifecycleTransactionMarker:
    """The session and root transactions that contain lifecycle work."""

    session_transaction: object
    root_transaction: object | None


@dataclass(frozen=True)
class _ExecutionRequest:
    """Validated immutable inputs for one idempotent execution submission."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    idempotency_key: str
    mechanism: str
    capture_bundle_id: int | None


class ExecutionLifecycleError(RuntimeError):
    """Base class for custom-import execution lifecycle failures."""


class ExecutionTransactionRequired(ExecutionLifecycleError):
    """The supplied session is not currently owned by an active transaction."""


class ExecutionNotFound(ExecutionLifecycleError):
    """The requested execution does not exist in the caller's transaction."""


class IdempotencyConflict(ExecutionLifecycleError):
    """An idempotency key was reused for different immutable execution inputs."""


class ExecutionInvariantError(ExecutionLifecycleError):
    """Persisted execution or lease state violates the v1 lifecycle contract."""


@dataclass(frozen=True)
class ExecutionSubmission:
    """The stable result of a create-or-return idempotent submission."""

    execution_id: int
    state: str
    created: bool
    capture_bundle_id: int | None = None


@dataclass(frozen=True)
class LeaseGrant:
    """A caller-owned lease result without returning the raw token or its digest."""

    execution_id: int
    fence: int
    expires_at: dt.datetime
    state: str


@dataclass(frozen=True)
class ExecutionTransition:
    """A cancellation or terminal transition result.

    ``changed`` is false for a stale lease, a cancellation that lost a race to
    a terminal state, or an otherwise disallowed transition.  This makes
    retries safe without turning normal concurrency loss into an exception.
    """

    execution_id: int
    state: str
    changed: bool


def _require_caller_transaction(session: AsyncSession) -> None:
    """Reject implicit sessions before any lifecycle statement is issued."""

    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction) or not in_transaction():
        raise ExecutionTransactionRequired("custom-import execution lifecycle requires an active caller transaction")


async def _root_transaction_context(
    session: AsyncSession,
) -> tuple[object | None, MutableMapping[str, Any] | None]:
    """Return the externally visible root transaction and its shared info.

    ``AsyncSession.get_transaction()`` identifies a session-owned savepoint
    when a session joins an existing connection transaction.  The connection's
    root transaction is the lifecycle boundary in that case.  Small synthetic
    sessions used by unit tests do not expose ``connection()``, so retain the
    session-level fallback for them.
    """

    connection_method = getattr(session, "connection", None)
    if not callable(connection_method):
        return None, None
    connection = await connection_method()
    get_transaction = getattr(connection, "get_transaction", None)
    if not callable(get_transaction):
        return None, None
    root_transaction = get_transaction()
    if root_transaction is None:
        raise ExecutionTransactionRequired("custom-import execution lifecycle requires an active root transaction")
    connection_info = getattr(connection, "info", None)
    if not isinstance(connection_info, MutableMapping):
        connection_info = None
    return root_transaction, connection_info


async def _mark_lifecycle_transaction(session: AsyncSession) -> None:
    """Remember lifecycle row locks until their root transaction changes."""

    transaction = session.get_transaction()
    if transaction is None:
        raise ExecutionTransactionRequired("custom-import execution lifecycle requires an active caller transaction")
    root_transaction, connection_info = await _root_transaction_context(session)
    marker = _LifecycleTransactionMarker(
        session_transaction=transaction,
        root_transaction=root_transaction,
    )
    session.info[_LIFECYCLE_TRANSACTION_MARKER] = marker
    if connection_info is not None:
        connection_info[_LIFECYCLE_TRANSACTION_MARKER] = marker


def _is_matching_lifecycle_marker(
    marker: _LifecycleTransactionMarker,
    *,
    session_transaction: object | None,
    root_transaction: object | None,
) -> bool:
    return marker.session_transaction is session_transaction or (
        root_transaction is not None and marker.root_transaction is root_transaction
    )


async def require_separate_publication_transaction(session: AsyncSession) -> None:
    """Reject generation publication after any lifecycle work in this transaction.

    Lifecycle state transitions and finality operations share the
    dataset→execution→lease lock order, but lifecycle work may still change an
    execution before a finality receipt exists.  Keeping those semantic
    transitions in separate underlying database transactions makes the
    boundary explicit.  Committing only an ``AsyncSession`` savepoint joined
    to an externally owned transaction is not sufficient.  The atomic
    no-change path establishes its terminal result without calling a generic
    lifecycle operation first.  Heartbeats use a narrower lock scope for
    concurrency, but their execution and lease row locks remain held until the
    root transaction ends, so publication still requires a new transaction.
    """

    current_transaction = session.get_transaction()
    root_transaction, connection_info = await _root_transaction_context(session)
    session_marker = session.info.get(_LIFECYCLE_TRANSACTION_MARKER)
    connection_marker = connection_info.get(_LIFECYCLE_TRANSACTION_MARKER) if connection_info is not None else None
    markers = tuple(
        marker for marker in (session_marker, connection_marker) if isinstance(marker, _LifecycleTransactionMarker)
    )
    if any(
        _is_matching_lifecycle_marker(
            marker,
            session_transaction=current_transaction,
            root_transaction=root_transaction,
        )
        for marker in markers
    ):
        raise ExecutionLifecycleError("commit execution lifecycle work before starting generation publication")

    session.info.pop(_LIFECYCLE_TRANSACTION_MARKER, None)
    if connection_info is not None:
        connection_info.pop(_LIFECYCLE_TRANSACTION_MARKER, None)


def _positive_id(value: Any, name: str, *, allow_none: bool = False) -> int | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= MAX_BIGINT:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _require_clean_lifecycle_session(session: AsyncSession) -> None:
    """Keep autoflush from acquiring a lower-order lock before the dataset."""

    for attribute in ("new", "dirty", "deleted"):
        if getattr(session, attribute, ()):
            raise ExecutionInvariantError(
                "custom-import lifecycle requires a clean session before it acquires the dataset lock"
            )


def _no_autoflush(session: AsyncSession):
    return getattr(session, "no_autoflush", nullcontext())


def _bounded_text(value: Any, name: str, *, maximum: int, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{name} must be non-empty text no longer than {maximum} characters")
    if value != value.strip() or any(character.isspace() and character not in {" "} for character in value):
        raise ValueError(f"{name} cannot have leading, trailing, or control whitespace")
    return value


def _idempotency_key(value: Any) -> str:
    key = _bounded_text(value, "idempotency_key", maximum=128)
    assert key is not None
    if any(ord(character) < 32 or ord(character) == 127 for character in key):
        raise ValueError("idempotency_key cannot contain control characters")
    return key


def _mechanism(value: Any) -> str:
    if not isinstance(value, str) or value not in _MECHANISMS:
        raise ValueError("mechanism must be one of local, queued, or external")
    return value


def _terminal_state(value: Any) -> str:
    if not isinstance(value, str) or value not in _LEASED_TERMINAL_STATES:
        raise ValueError("terminal_state must be canceled, failed, or completed")
    return value


def _fence(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= MAX_BIGINT:
        raise ValueError("fence must be a positive integer")
    return value


def _lease_seconds(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_LEASE_SECONDS:
        raise ValueError(f"lease_seconds must be an integer from 1 through {MAX_LEASE_SECONDS}")
    return value


def lease_token_sha256(token: str | bytes | bytearray | memoryview) -> bytes:
    """Return the only token representation permitted in persistent state."""

    if isinstance(token, str):
        token_bytes = token.encode("utf-8")
    elif isinstance(token, (bytes, bytearray, memoryview)):
        token_bytes = bytes(token)
    else:
        raise ValueError("token must be text or bytes")
    if not token_bytes or len(token_bytes) > MAX_TOKEN_BYTES:
        raise ValueError(f"token must contain from 1 through {MAX_TOKEN_BYTES} bytes")
    return hashlib.sha256(token_bytes).digest()


def _persisted_digest(value: Any) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ExecutionInvariantError("lease token digest is missing or malformed")
    digest = bytes(value)
    if len(digest) != hashlib.sha256().digest_size:
        raise ExecutionInvariantError("lease token digest has the wrong length")
    return digest


def _validate_execution_state(execution: CustomImportExecution) -> str:
    state = getattr(execution, "state", None)
    if state not in EXECUTION_STATES:
        raise ExecutionInvariantError("execution has an unknown lifecycle state")
    return state


def _validate_lease(lease: CustomImportLease) -> None:
    fence = getattr(lease, "fence", None)
    expires_at = getattr(lease, "expires_at", None)
    token_sha256 = getattr(lease, "token_sha256", None)
    if isinstance(fence, bool) or not isinstance(fence, int) or not 0 <= fence <= MAX_BIGINT:
        raise ExecutionInvariantError("lease fence is malformed")
    if fence == 0:
        if token_sha256 is not None or expires_at is not None:
            raise ExecutionInvariantError("unclaimed lease has authority fields")
        return
    _persisted_digest(token_sha256)
    if not isinstance(expires_at, dt.datetime) or expires_at.tzinfo is None:
        raise ExecutionInvariantError("claimed lease expiration is malformed")


def _is_lease_unexpired(lease: CustomImportLease, now: dt.datetime) -> bool:
    _validate_lease(lease)
    return lease.fence > 0 and lease.expires_at > now


def _has_matching_lease_authority(
    lease: CustomImportLease,
    *,
    fence: int,
    token_sha256: bytes,
    now: dt.datetime,
) -> bool:
    return (
        _is_lease_unexpired(lease, now)
        and lease.fence == fence
        and hmac.compare_digest(_persisted_digest(lease.token_sha256), token_sha256)
    )


async def _lock_execution(
    session: AsyncSession,
    execution_id: int,
    *,
    dataset_id: int | None = None,
) -> CustomImportExecution | None:
    await _mark_lifecycle_transaction(session)
    statement = select(CustomImportExecution).where(CustomImportExecution.execution_id == execution_id)
    if dataset_id is not None:
        statement = statement.where(CustomImportExecution.dataset_id == dataset_id)
    with _no_autoflush(session):
        result = await session.execute(statement.with_for_update().execution_options(populate_existing=True))
    return result.scalar_one_or_none()


async def _lock_execution_by_request(
    session: AsyncSession,
    *,
    dataset_id: int | None = None,
    definition_revision_id: int,
    idempotency_key: str,
) -> CustomImportExecution | None:
    await _mark_lifecycle_transaction(session)
    statement = select(CustomImportExecution)
    if dataset_id is not None:
        statement = statement.where(CustomImportExecution.dataset_id == dataset_id)
    statement = (
        statement.where(CustomImportExecution.definition_revision_id == definition_revision_id)
        .where(CustomImportExecution.idempotency_key == idempotency_key)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    with _no_autoflush(session):
        result = await session.execute(statement)
    return result.scalar_one_or_none()


async def _execution_snapshot(session: AsyncSession, execution_id: int) -> CustomImportExecution | None:
    """Read immutable execution ownership before taking the dataset lock."""

    with _no_autoflush(session):
        result = await session.execute(
            select(CustomImportExecution)
            .where(CustomImportExecution.execution_id == execution_id)
            .execution_options(populate_existing=True)
        )
    return result.scalar_one_or_none()


async def _lock_dataset(session: AsyncSession, dataset_id: int) -> CustomImportDataset:
    """Acquire the common lifecycle/finality serialization parent first."""

    await _mark_lifecycle_transaction(session)
    with _no_autoflush(session):
        result = await session.execute(
            select(CustomImportDataset)
            .where(CustomImportDataset.dataset_id == dataset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise ExecutionInvariantError("custom-import dataset does not exist")
    return dataset


async def _locked_lifecycle_execution(session: AsyncSession, execution_id: int) -> CustomImportExecution:
    """Lock one execution only after locking its immutable dataset parent."""

    snapshot = await _execution_snapshot(session, execution_id)
    if snapshot is None:
        raise ExecutionNotFound(f"custom-import execution {execution_id} does not exist")
    await _lock_dataset(session, snapshot.dataset_id)
    execution = await _lock_execution(session, execution_id, dataset_id=snapshot.dataset_id)
    if execution is None:
        raise ExecutionNotFound(f"custom-import execution {execution_id} does not exist")
    return execution


async def _locked_heartbeat_execution(session: AsyncSession, execution_id: int) -> CustomImportExecution:
    """Lock a heartbeat target without the dataset-wide finality parent.

    A heartbeat only extends its exact lease: it neither changes shared
    dataset state nor gains authority to append or publish.  It may therefore
    take execution then lease while every other lifecycle/finality writer uses
    dataset→execution→lease.  Because this path never subsequently waits for
    the dataset, a queued finality writer can wait for the heartbeat without a
    lock cycle, and a long seal cannot starve unrelated live executions.
    """

    execution = await _lock_execution(session, execution_id)
    if execution is None:
        raise ExecutionNotFound(f"custom-import execution {execution_id} does not exist")
    return execution


async def _ensure_lease(session: AsyncSession, execution_id: int) -> None:
    """Create only an empty lease row, atomically with execution submission."""

    await session.execute(
        pg_insert(CustomImportLease)
        .values(execution_id=execution_id, fence=0)
        .on_conflict_do_nothing(index_elements=(CustomImportLease.execution_id,))
    )


async def _lock_lease(session: AsyncSession, execution_id: int) -> CustomImportLease:
    statement = (
        select(CustomImportLease)
        .where(CustomImportLease.execution_id == execution_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    with _no_autoflush(session):
        result = await session.execute(statement)
    lease = result.scalar_one_or_none()
    if lease is not None:
        return lease
    await _ensure_lease(session, execution_id)
    with _no_autoflush(session):
        result = await session.execute(statement)
    lease = result.scalar_one_or_none()
    if lease is None:
        raise ExecutionInvariantError("execution lease could not be created")
    return lease


async def _database_now(session: AsyncSession) -> dt.datetime:
    """Read database time only after the operation's authoritative row locks."""

    result = await session.execute(select(func.clock_timestamp()))
    now = result.scalar_one()
    if not isinstance(now, dt.datetime) or now.tzinfo is None:
        raise ExecutionInvariantError("database clock did not return an aware timestamp")
    return now


def _has_matching_submission(
    execution: CustomImportExecution,
    request: _ExecutionRequest,
) -> bool:
    return (
        _has_matching_execution_identity(execution, request)
        and execution.capture_bundle_id == request.capture_bundle_id
    )


def _has_matching_execution_identity(
    execution: CustomImportExecution,
    request: _ExecutionRequest,
) -> bool:
    return (
        execution.dataset_id == request.dataset_id
        and execution.definition_revision_id == request.definition_revision_id
        and execution.schema_revision_id == request.schema_revision_id
        and execution.mechanism == request.mechanism
    )


def _submission_result(execution: CustomImportExecution, *, is_created: bool) -> ExecutionSubmission:
    state = _validate_execution_state(execution)
    return ExecutionSubmission(
        execution_id=execution.execution_id,
        state=state,
        created=is_created,
        capture_bundle_id=execution.capture_bundle_id,
    )


def _validated_execution_request(
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    idempotency_key: str,
    mechanism: str,
    capture_bundle_id: int | None,
) -> _ExecutionRequest:
    normalized_dataset_id = _positive_id(dataset_id, "dataset_id")
    normalized_definition_revision_id = _positive_id(definition_revision_id, "definition_revision_id")
    normalized_schema_revision_id = _positive_id(schema_revision_id, "schema_revision_id")
    normalized_capture_bundle_id = _positive_id(capture_bundle_id, "capture_bundle_id", allow_none=True)
    normalized_idempotency_key = _idempotency_key(idempotency_key)
    normalized_mechanism = _mechanism(mechanism)
    return _ExecutionRequest(
        dataset_id=normalized_dataset_id,
        definition_revision_id=normalized_definition_revision_id,
        schema_revision_id=normalized_schema_revision_id,
        idempotency_key=normalized_idempotency_key,
        mechanism=normalized_mechanism,
        capture_bundle_id=normalized_capture_bundle_id,
    )


async def _insert_execution(session: AsyncSession, request: _ExecutionRequest) -> int | None:
    insert_result = await session.execute(
        pg_insert(CustomImportExecution)
        .values(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            idempotency_key=request.idempotency_key,
            mechanism=request.mechanism,
            state="queued",
            capture_bundle_id=request.capture_bundle_id,
        )
        .on_conflict_do_nothing(
            index_elements=(
                CustomImportExecution.definition_revision_id,
                CustomImportExecution.idempotency_key,
            )
        )
        .returning(CustomImportExecution.execution_id)
    )
    return insert_result.scalar_one_or_none()


async def _locked_submission_execution(
    session: AsyncSession,
    request: _ExecutionRequest,
    inserted_execution_id: int | None,
) -> CustomImportExecution | None:
    if inserted_execution_id is not None:
        return await _lock_execution(session, inserted_execution_id, dataset_id=request.dataset_id)
    snapshot = await _submission_snapshot(
        session,
        definition_revision_id=request.definition_revision_id,
        idempotency_key=request.idempotency_key,
    )
    if snapshot is None or snapshot.dataset_id != request.dataset_id:
        return snapshot
    return await _lock_execution(
        session,
        snapshot.execution_id,
        dataset_id=request.dataset_id,
    )


async def _submission_snapshot(
    session: AsyncSession,
    *,
    definition_revision_id: int,
    idempotency_key: str,
) -> CustomImportExecution | None:
    with _no_autoflush(session):
        result = await session.execute(
            select(CustomImportExecution)
            .where(CustomImportExecution.definition_revision_id == definition_revision_id)
            .where(CustomImportExecution.idempotency_key == idempotency_key)
            .execution_options(populate_existing=True)
        )
    return result.scalar_one_or_none()


async def create_execution(
    session: AsyncSession,
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    idempotency_key: str,
    mechanism: str,
    capture_bundle_id: int | None = None,
) -> ExecutionSubmission:
    """Create one queued execution or return its exact prior submission.

    The definition-scoped idempotency key is safe to retry only with the same
    immutable execution inputs.  A reused key with different inputs is refused
    rather than silently binding a caller to another capture or mechanism.
    """

    _require_caller_transaction(session)
    _require_clean_lifecycle_session(session)
    request = _validated_execution_request(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        idempotency_key=idempotency_key,
        mechanism=mechanism,
        capture_bundle_id=capture_bundle_id,
    )

    return await _submit_execution(session, request, allow_bound_capture_reuse=False)


async def reserve_execution(
    session: AsyncSession,
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    idempotency_key: str,
    mechanism: str,
) -> ExecutionSubmission:
    """Reserve a source-neutral execution before an external acquisition.

    Replays retain the exact execution even after its capture bundle is bound.
    A caller must still take a current lease before acquiring a source; the
    ``created`` flag only reports whether this transaction inserted the row.
    """

    _require_caller_transaction(session)
    _require_clean_lifecycle_session(session)
    request = _validated_execution_request(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        idempotency_key=idempotency_key,
        mechanism=mechanism,
        capture_bundle_id=None,
    )
    return await _submit_execution(session, request, allow_bound_capture_reuse=True)


async def _submit_execution(
    session: AsyncSession,
    request: _ExecutionRequest,
    *,
    allow_bound_capture_reuse: bool,
) -> ExecutionSubmission:
    """Create or lock one idempotent execution under the dataset parent."""

    # The INSERT itself can wait on a competing idempotency row.  Serialize it
    # beneath the dataset lock so no execution/lease lock precedes that parent.
    await _lock_dataset(session, request.dataset_id)
    inserted_execution_id = await _insert_execution(session, request)
    is_created = inserted_execution_id is not None
    execution = await _locked_submission_execution(session, request, inserted_execution_id)
    if execution is None:
        raise ExecutionInvariantError("idempotent execution row disappeared before it could be locked")
    if not _has_matching_submission(execution, request) and not (
        allow_bound_capture_reuse
        and request.capture_bundle_id is None
        and _has_matching_execution_identity(execution, request)
    ):
        raise IdempotencyConflict("idempotency_key is already bound to different execution inputs")
    await _ensure_lease(session, execution.execution_id)
    return _submission_result(execution, is_created=is_created)


async def _lock_current_capture_binding(
    session: AsyncSession,
    *,
    execution_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    fence: int,
    token_sha256: bytes,
) -> tuple[CustomImportExecution, str, dt.datetime] | None:
    """Lock and return a running execution only for its current lease owner."""

    await _lock_dataset(session, dataset_id)
    execution = await _lock_execution(session, execution_id, dataset_id=dataset_id)
    if execution is None:
        raise ExecutionNotFound(f"custom-import execution {execution_id} does not exist")
    if execution.definition_revision_id != definition_revision_id or execution.schema_revision_id != schema_revision_id:
        raise IdempotencyConflict("execution identity does not match the capture bundle")
    state = _validate_execution_state(execution)
    lease = await _lock_lease(session, execution_id)
    _validate_lease(lease)
    now = await _database_now(session)
    if state != "running" or not _has_matching_lease_authority(
        lease,
        fence=fence,
        token_sha256=token_sha256,
        now=now,
    ):
        return None
    return execution, state, now


async def _bind_locked_capture_bundle(
    session: AsyncSession,
    *,
    execution: CustomImportExecution,
    dataset_id: int,
    capture_bundle_id: int,
    state: str,
    now: dt.datetime,
) -> ExecutionSubmission:
    """Attach a bundle to an authorized locked execution without replacing one."""

    existing_bundle_id = execution.capture_bundle_id
    if existing_bundle_id is not None:
        if existing_bundle_id != capture_bundle_id:
            raise IdempotencyConflict("execution is already bound to a different capture bundle")
        return _submission_result(execution, is_created=False)
    await session.execute(
        update(CustomImportExecution)
        .where(CustomImportExecution.execution_id == execution.execution_id)
        .where(CustomImportExecution.dataset_id == dataset_id)
        .values(capture_bundle_id=capture_bundle_id, updated_at=now)
    )
    return ExecutionSubmission(
        execution_id=execution.execution_id,
        state=state,
        created=False,
        capture_bundle_id=capture_bundle_id,
    )


async def bind_execution_capture_bundle(
    session: AsyncSession,
    *,
    execution_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    capture_bundle_id: int,
    fence: int,
    token: str | bytes | bytearray | memoryview,
) -> ExecutionSubmission | None:
    """Bind one retained bundle only while the exact owner lease is live.

    A stale or canceled execution returns ``None`` without attaching the
    bundle, allowing the caller to roll back its capture registration.
    """

    _require_caller_transaction(session)
    _require_clean_lifecycle_session(session)
    execution_id = _positive_id(execution_id, "execution_id")
    dataset_id = _positive_id(dataset_id, "dataset_id")
    definition_revision_id = _positive_id(definition_revision_id, "definition_revision_id")
    schema_revision_id = _positive_id(schema_revision_id, "schema_revision_id")
    capture_bundle_id = _positive_id(capture_bundle_id, "capture_bundle_id")
    fence = _fence(fence)
    binding_context = await _lock_current_capture_binding(
        session,
        execution_id=execution_id,
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        fence=fence,
        token_sha256=lease_token_sha256(token),
    )
    if binding_context is None:
        return None
    execution, state, now = binding_context
    return await _bind_locked_capture_bundle(
        session,
        execution=execution,
        dataset_id=dataset_id,
        capture_bundle_id=capture_bundle_id,
        state=state,
        now=now,
    )


async def _claim_or_resume_execution(
    session: AsyncSession,
    *,
    execution_id: int,
    token_sha256: bytes,
    lease_seconds: int,
    allow_queued: bool,
) -> LeaseGrant | None:
    execution = await _locked_lifecycle_execution(session, execution_id)
    state = _validate_execution_state(execution)
    lease = await _lock_lease(session, execution_id)
    now = await _database_now(session)
    _validate_lease(lease)
    if state not in _ACTIVE_STATES or (state == "queued" and not allow_queued):
        return None
    if _is_lease_unexpired(lease, now):
        return None
    if lease.fence >= MAX_FENCE:
        raise ExecutionInvariantError("lease fence cannot advance further")

    fence = lease.fence + 1
    expires_at = now + dt.timedelta(seconds=lease_seconds)
    await session.execute(
        update(CustomImportLease)
        .where(CustomImportLease.execution_id == execution_id)
        .values(
            fence=fence,
            token_sha256=token_sha256,
            heartbeat_at=now,
            expires_at=expires_at,
            updated_at=now,
        )
    )
    claimed_state = "running" if state == "queued" else state
    execution_update_dict: dict[str, Any] = {"state": claimed_state, "updated_at": now}
    if execution.started_at is None:
        execution_update_dict["started_at"] = now
    await session.execute(
        update(CustomImportExecution)
        .where(CustomImportExecution.execution_id == execution_id)
        .values(**execution_update_dict)
    )
    return LeaseGrant(
        execution_id=execution_id,
        fence=fence,
        expires_at=expires_at,
        state=claimed_state,
    )


async def claim_execution(
    session: AsyncSession,
    *,
    execution_id: int,
    token: str | bytes | bytearray | memoryview,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> LeaseGrant | None:
    """Claim a queued execution or take over an expired active execution.

    A live lease is never overwritten, including when its caller presents the
    same token.  The holder must use :func:`heartbeat_execution` to renew it.
    """

    _require_caller_transaction(session)
    _require_clean_lifecycle_session(session)
    execution_id = _positive_id(execution_id, "execution_id")
    token_sha256 = lease_token_sha256(token)
    lease_seconds = _lease_seconds(lease_seconds)
    return await _claim_or_resume_execution(
        session,
        execution_id=execution_id,
        token_sha256=token_sha256,
        lease_seconds=lease_seconds,
        allow_queued=True,
    )


async def resume_execution(
    session: AsyncSession,
    *,
    execution_id: int,
    token: str | bytes | bytearray | memoryview,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> LeaseGrant | None:
    """Take over an expired running or canceling execution with a new fence."""

    _require_caller_transaction(session)
    _require_clean_lifecycle_session(session)
    execution_id = _positive_id(execution_id, "execution_id")
    token_sha256 = lease_token_sha256(token)
    lease_seconds = _lease_seconds(lease_seconds)
    return await _claim_or_resume_execution(
        session,
        execution_id=execution_id,
        token_sha256=token_sha256,
        lease_seconds=lease_seconds,
        allow_queued=False,
    )


async def heartbeat_execution(
    session: AsyncSession,
    *,
    execution_id: int,
    fence: int,
    token: str | bytes | bytearray | memoryview,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> LeaseGrant | None:
    """Renew one unexpired lease only when the holder still owns its fence."""

    _require_caller_transaction(session)
    _require_clean_lifecycle_session(session)
    execution_id = _positive_id(execution_id, "execution_id")
    fence = _fence(fence)
    token_sha256 = lease_token_sha256(token)
    lease_seconds = _lease_seconds(lease_seconds)

    execution = await _locked_heartbeat_execution(session, execution_id)
    state = _validate_execution_state(execution)
    lease = await _lock_lease(session, execution_id)
    now = await _database_now(session)
    if state not in _HEARTBEAT_STATES or not _has_matching_lease_authority(
        lease,
        fence=fence,
        token_sha256=token_sha256,
        now=now,
    ):
        return None

    expires_at = now + dt.timedelta(seconds=lease_seconds)
    await session.execute(
        update(CustomImportLease)
        .where(CustomImportLease.execution_id == execution_id)
        .values(heartbeat_at=now, expires_at=expires_at, updated_at=now)
    )
    return LeaseGrant(
        execution_id=execution_id,
        fence=fence,
        expires_at=expires_at,
        state=state,
    )


async def request_cancellation(
    session: AsyncSession,
    *,
    execution_id: int,
    terminal_reason: str | None = None,
) -> ExecutionTransition:
    """Record an idempotent cancellation request without bypassing a lease.

    Queued work becomes terminal immediately because it has no worker-owned
    authority.  Running work becomes ``canceling`` so its active holder can
    observe the request and finish as ``canceled``; completion is thereafter
    refused by :func:`finish_execution`.
    """

    _require_caller_transaction(session)
    _require_clean_lifecycle_session(session)
    execution_id = _positive_id(execution_id, "execution_id")
    terminal_reason = _bounded_text(
        terminal_reason,
        "terminal_reason",
        maximum=64,
        allow_none=True,
    )
    execution = await _locked_lifecycle_execution(session, execution_id)
    state = _validate_execution_state(execution)
    lease = await _lock_lease(session, execution_id)
    now = await _database_now(session)
    _validate_lease(lease)

    if state in TERMINAL_STATES or state == "canceling":
        return ExecutionTransition(execution_id=execution_id, state=state, changed=False)
    if state == "queued":
        await session.execute(
            update(CustomImportExecution)
            .where(CustomImportExecution.execution_id == execution_id)
            .values(
                state="canceled",
                terminal_reason=terminal_reason,
                finished_at=now,
                updated_at=now,
            )
        )
        if lease.fence > 0:
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == execution_id)
                .values(expires_at=now, updated_at=now)
            )
        return ExecutionTransition(execution_id=execution_id, state="canceled", changed=True)

    canceling_fields_by_name: dict[str, Any] = {"state": "canceling", "updated_at": now}
    if terminal_reason is not None:
        canceling_fields_by_name["terminal_reason"] = terminal_reason
    await session.execute(
        update(CustomImportExecution)
        .where(CustomImportExecution.execution_id == execution_id)
        .values(**canceling_fields_by_name)
    )
    return ExecutionTransition(execution_id=execution_id, state="canceling", changed=True)


def _terminal_transition_states(terminal_state: str) -> frozenset[str]:
    if terminal_state == "canceled":
        return frozenset({"running", "canceling"})
    return frozenset({"running"})


def _can_finish_execution(
    state: str,
    lease: CustomImportLease,
    *,
    terminal_state: str,
    fence: int,
    token_sha256: bytes,
    now: dt.datetime,
) -> bool:
    return state in _terminal_transition_states(terminal_state) and _has_matching_lease_authority(
        lease,
        fence=fence,
        token_sha256=token_sha256,
        now=now,
    )


async def _apply_terminal_transition(
    session: AsyncSession,
    *,
    execution_id: int,
    terminal_state: str,
    terminal_reason: str | None,
    now: dt.datetime,
) -> None:
    terminal_fields_by_name: dict[str, Any] = {
        "state": terminal_state,
        "finished_at": now,
        "updated_at": now,
    }
    # A cancellation request is the authoritative reason when the worker
    # later acknowledges it without supplying a replacement reason.
    if terminal_reason is not None:
        terminal_fields_by_name["terminal_reason"] = terminal_reason
    await session.execute(
        update(CustomImportExecution)
        .where(CustomImportExecution.execution_id == execution_id)
        .values(**terminal_fields_by_name)
    )
    # Keep the holder digest and fence as an audit fence.  The database shape
    # forbids clearing a claimed lease, and expiration invalidates the holder.
    await session.execute(
        update(CustomImportLease)
        .where(CustomImportLease.execution_id == execution_id)
        .values(expires_at=now, updated_at=now)
    )


async def finish_execution(
    session: AsyncSession,
    *,
    execution_id: int,
    fence: int,
    token: str | bytes | bytearray | memoryview,
    terminal_state: str,
    terminal_reason: str | None = None,
) -> ExecutionTransition:
    """Apply a fenced terminal transition and expire, never clear, its lease.

    A generic ``completed`` transition is intentionally not coupled to
    generation activation.  Commit the caller-owned completion transaction
    before opening a separate publication transaction.  Only the no-change
    publication path may atomically establish a terminal result and current
    generation evidence.
    """

    _require_caller_transaction(session)
    _require_clean_lifecycle_session(session)
    execution_id = _positive_id(execution_id, "execution_id")
    fence = _fence(fence)
    token_sha256 = lease_token_sha256(token)
    terminal_state = _terminal_state(terminal_state)
    terminal_reason = _bounded_text(
        terminal_reason,
        "terminal_reason",
        maximum=64,
        allow_none=True,
    )

    execution = await _locked_lifecycle_execution(session, execution_id)
    state = _validate_execution_state(execution)
    lease = await _lock_lease(session, execution_id)
    now = await _database_now(session)
    if state in TERMINAL_STATES:
        return ExecutionTransition(execution_id=execution_id, state=state, changed=False)
    if not _can_finish_execution(
        state,
        lease,
        terminal_state=terminal_state,
        fence=fence,
        token_sha256=token_sha256,
        now=now,
    ):
        return ExecutionTransition(execution_id=execution_id, state=state, changed=False)

    await _apply_terminal_transition(
        session,
        execution_id=execution_id,
        terminal_state=terminal_state,
        terminal_reason=terminal_reason,
        now=now,
    )
    return ExecutionTransition(execution_id=execution_id, state=terminal_state, changed=True)


# A concise public synonym for request-oriented coordinators.
cancel_execution = request_cancellation


__all__ = (
    "DEFAULT_LEASE_SECONDS",
    "EXECUTION_STATES",
    "ExecutionInvariantError",
    "ExecutionLifecycleError",
    "ExecutionNotFound",
    "ExecutionSubmission",
    "ExecutionTransactionRequired",
    "ExecutionTransition",
    "IdempotencyConflict",
    "LeaseGrant",
    "MAX_LEASE_SECONDS",
    "MAX_BIGINT",
    "TERMINAL_STATES",
    "bind_execution_capture_bundle",
    "cancel_execution",
    "claim_execution",
    "create_execution",
    "finish_execution",
    "heartbeat_execution",
    "lease_token_sha256",
    "request_cancellation",
    "require_separate_publication_transaction",
    "reserve_execution",
    "resume_execution",
)
