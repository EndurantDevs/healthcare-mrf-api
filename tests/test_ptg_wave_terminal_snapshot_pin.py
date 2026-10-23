# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact source lifetime admission for ordinary terminal pin mutations."""

from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg_wave_terminal_snapshot_pin as pins
from process.ptg_parts.ptg2_lifecycle_lock import PTG2LifecycleLockDeferred
from process.ptg_parts.source_snapshot_reference_lifetime import (
    SnapshotReferenceIdentityError,
    acquire_snapshot_reference_lifetime,
)
from process.ptg_wave_receipt_contract import ordinary_cutover_id

OPERATION_ID = "8" * 64
SNAPSHOT_ID = "snapshot-synthetic"
RUN_ID = "run-synthetic"


def _identity(**changes):
    return {
        "snapshot_id": SNAPSHOT_ID,
        "import_run_id": RUN_ID,
        "source_key": "synthetic_source",
        "source_present": True,
        "source_is_text": True,
        "run_source_key": "synthetic_source",
        "run_source_is_text": True,
        **changes,
    }


def _options():
    return {
        "ordinary_cutover_operation_id": OPERATION_ID,
        "ordinary_cutover_id": ordinary_cutover_id(OPERATION_ID),
        "ordinary_cutover_member_ordinal": 7,
    }


class _Rows:
    def __init__(self, rows=()):
        self.rows = rows

    def all(self):
        return self.rows

    def one_or_none(self):
        return self.rows[0] if self.rows else None


class _Session:
    def __init__(self, *, identities=None, active=True, lock_error=None, recheck_error=None):
        self.identities = list(identities) if identities is not None else None
        self.active = active
        self.lock_error = lock_error
        self.recheck_error = recheck_error
        self.calls = []
        self.pin = None

    def in_transaction(self):
        return self.active

    async def execute(self, statement, parameters):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if "AS run_source_key" in sql:
            if "FOR SHARE" in sql and self.recheck_error is not None:
                raise self.recheck_error
            row = self.identities.pop(0) if self.identities is not None else _identity()
            return _Rows([SimpleNamespace(_mapping=row)] if row is not None else [])
        if "pg_advisory_xact_lock(" in sql and self.lock_error is not None:
            raise self.lock_error
        if "INSERT INTO" in sql:
            self.pin = self.pin or {"snapshot_id": parameters["snapshot_id"], "reason": parameters["reason"]}
        if "DELETE FROM" in sql:
            has_deleted_pin = self.pin is not None
            self.pin = None
            return _Rows([{"snapshot_id": SNAPSHOT_ID}] if has_deleted_pin else [])
        if "FOR UPDATE" in sql:
            return _Rows([self.pin] if self.pin else [])
        return _Rows()


async def _insert(session, *, options=None):
    return await pins.insert_ordinary_terminal_snapshot_pin(
        session,
        schema_name="mrf",
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=RUN_ID,
        options=_options() if options is None else options,
    )


async def _delete(session):
    return await pins.delete_ordinary_terminal_snapshot_pin(
        session,
        schema_name="mrf",
        operation_id=OPERATION_ID,
        member_ordinal=7,
        snapshot_id=SNAPSHOT_ID,
        lock_timeout="250ms",
        statement_timeout="5s",
    )


@pytest.mark.asyncio
async def test_prelock_acquires_source_fence_before_header_locks_and_pin_writes():
    session = _Session()
    pin = await pins.lock_ordinary_terminal_snapshot_pin(
        session, schema_name="mrf", snapshot_id=SNAPSHOT_ID, internal_run_id=RUN_ID, options=_options()
    )
    assert pin.owner_id == f"{OPERATION_ID}:7"
    assert len(session.calls) == 5
    assert "manifest::jsonb #>>" in session.calls[0][0]
    assert "FOR SHARE" not in session.calls[0][0]
    assert "set_config" in session.calls[1][0]
    assert "pg_advisory_xact_lock_shared" in session.calls[2][0]
    assert session.calls[3][1]["source_lock_key"] == "ptg2_source_lifecycle_v2:synthetic_source"
    assert "manifest::jsonb #>>" in session.calls[4][0]
    assert "FOR SHARE OF snapshot,run" in session.calls[4][0]
    assert all("FOR UPDATE" not in sql and "INSERT" not in sql for sql, _parameters in session.calls)


@pytest.mark.asyncio
async def test_pin_replay_and_exact_deletion_remain_inside_the_caller_transaction():
    session = _Session()
    first = await _insert(session)
    assert await _insert(session) == first
    assert session.pin == {"snapshot_id": SNAPSHOT_ID, "reason": pins.ORDINARY_TERMINAL_PIN_REASON}
    assert await _delete(session) == 1
    assert await _delete(session) == 0
    writes = [(sql, parameters) for sql, parameters in session.calls if "INSERT INTO" in sql or "DELETE FROM" in sql]
    assert len(writes) == 4
    assert all(parameters["owner_id"] == f"{OPERATION_ID}:7" for _sql, parameters in writes)
    assert all(parameters["snapshot_id"] == SNAPSHOT_ID for _sql, parameters in writes)
    assert all("ON CONFLICT (owner_type, owner_id, snapshot_id) DO NOTHING" in sql for sql, _ in writes[:2])
    settings = [parameters for sql, parameters in session.calls if "set_config" in sql]
    assert settings[-2:] == [{"lock_timeout": "250ms", "statement_timeout": "5s"}] * 2


@pytest.mark.asyncio
async def test_unrelated_completion_does_not_query_or_lock_snapshot_metadata():
    session = _Session(active=False)
    assert await _insert(session, options={}) is None
    assert session.calls == []


@pytest.mark.asyncio
async def test_blank_snapshot_uses_its_exact_run_source_without_a_serving_index():
    session = _Session(identities=[_identity(source_key=None, source_present=False, source_is_text=None)] * 2)
    assert await _delete(session) == 0
    assert session.calls[3][1]["source_lock_key"] == "ptg2_source_lifecycle_v2:synthetic_source"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity",
    [
        None,
        _identity(source_key=None),
        _identity(source_key="Source-Key"),
        _identity(source_is_text=False),
        _identity(import_run_id=""),
        _identity(snapshot_id="other-snapshot"),
        _identity(run_source_key=""),
        _identity(run_source_key="Source-Key"),
        _identity(run_source_is_text=False),
    ],
)
async def test_missing_or_noncanonical_identity_refuses_before_locking(identity):
    session = _Session(identities=[identity])
    with pytest.raises(SnapshotReferenceIdentityError, match="source identity is unavailable"):
        await _insert(session)
    assert len(session.calls) == 1
    assert session.pin is None


@pytest.mark.asyncio
async def test_run_identity_mismatch_refuses_before_lifetime_acquisition():
    session = _Session(identities=[_identity(import_run_id="other-run")])
    with pytest.raises(SnapshotReferenceIdentityError, match="run identity changed"):
        await _insert(session)
    assert len(session.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("source_key", [None, "other_source"])
async def test_terminal_options_cannot_replace_the_authenticated_run_source(source_key):
    session = _Session()
    with pytest.raises(SnapshotReferenceIdentityError, match="source identity changed"):
        await _insert(session, options={**_options(), "source_key": source_key})
    assert session.pin is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "after",
    [_identity(source_key="other_source", run_source_key="other_source"), _identity(import_run_id="other-run")],
)
async def test_identity_drift_after_source_fence_refuses_pin_mutation(after):
    session = _Session(identities=[_identity(), after])
    with pytest.raises(SnapshotReferenceIdentityError, match="source identity changed"):
        await _insert(session)
    assert session.pin is None
    assert all("INSERT" not in sql for sql, _parameters in session.calls)


@pytest.mark.asyncio
async def test_reference_lifetime_requires_the_callers_existing_transaction():
    session = _Session(active=False)
    with pytest.raises(RuntimeError, match="caller transaction"):
        await acquire_snapshot_reference_lifetime(session, schema_name="mrf", snapshot_id=SNAPSHOT_ID)
    assert session.calls == []


@pytest.mark.asyncio
async def test_busy_source_propagates_retryable_refusal_before_pin_write():
    error = RuntimeError("busy source")
    error.sqlstate = "55P03"
    session = _Session(lock_error=error)
    with pytest.raises(PTG2LifecycleLockDeferred):
        await _insert(session)
    assert session.pin is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sqlstate", "expected"),
    [
        ("40001", SnapshotReferenceIdentityError),
        ("55P03", PTG2LifecycleLockDeferred),
        ("57014", PTG2LifecycleLockDeferred),
        ("42501", RuntimeError),
    ],
)
async def test_header_recheck_preserves_retry_and_identity_errors(sqlstate, expected):
    error = RuntimeError("header recheck refused")
    error.sqlstate = sqlstate
    session = _Session(recheck_error=error)
    with pytest.raises(expected) as refused:
        await _insert(session)
    if expected is RuntimeError:
        assert refused.value is error
    else:
        assert refused.value.__cause__ is error
    if expected is PTG2LifecycleLockDeferred:
        assert refused.value.retryable is True
    assert session.pin is None
