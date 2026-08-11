# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_snapshot_rollback as rollback
from process.ptg_parts.source_snapshot_rollback_types import (
    PTG2SourceSnapshotRollbackConflict,
)
from tests.ptg_source_snapshot_rollback_unit_support import (
    CURRENT_SNAPSHOT,
    IMPORT_MONTH,
    ROLLBACK_OWNER,
    SOURCE_KEY,
    TARGET_SNAPSHOT,
    activated_attestation as _activated_attestation,
    allowed_index as _allowed_index,
    context as _context,
    decision as _decision,
    expected_snapshot as _expected_snapshot,
    live_plan_pointer as _live_plan_pointer,
    pin as _pin,
    serving_manifest as _serving_manifest,
    snapshot_scope as _snapshot_scope,
    target_snapshot as _target_snapshot,
)


@pytest.fixture(autouse=True)
def _projection_queue_fakes(monkeypatch):
    monkeypatch.setattr(
        rollback,
        "mark_legacy_global_projection_dirty",
        AsyncMock(),
    )
    monkeypatch.setattr(
        rollback,
        "drain_legacy_global_projection_queue",
        AsyncMock(return_value=SimpleNamespace(reconciled=1)),
    )


def test_rollback_decision_reverses_exact_source_plan_and_global_pair():
    decision = _decision(_context())

    assert decision.is_already_rolled_back is False
    assert decision.should_reverse_global_pointer is True
    assert decision.allowed_action == "unchanged"
    assert len(decision.plan_pointer_entries) == 1
    pointer = decision.plan_pointer_entries[0]
    assert pointer["source_key"] == SOURCE_KEY
    assert pointer["snapshot_id"] == TARGET_SNAPSHOT
    assert pointer["previous_snapshot_id"] == CURRENT_SNAPSHOT
    assert pointer["plan_id"] == "plan-1"


def test_rollback_decision_accepts_only_a_complete_exact_retry():
    initial = _decision(_context())
    retry_context = _context(
        source_pointer_by_field={
            "source_key": SOURCE_KEY,
            "snapshot_id": TARGET_SNAPSHOT,
            "previous_snapshot_id": CURRENT_SNAPSHOT,
            "import_month": IMPORT_MONTH,
        },
        source_plan_pointer_records=initial.plan_pointer_entries,
        global_pointer_by_field={
            "snapshot_id": TARGET_SNAPSHOT,
            "previous_snapshot_id": CURRENT_SNAPSHOT,
            "source_key": SOURCE_KEY,
        },
    )

    decision = _decision(retry_context)

    assert decision.is_already_rolled_back is True
    assert decision.should_reverse_global_pointer is False

    incomplete_retry = replace(
        retry_context,
        source_plan_pointer_records=(
            {
                **dict(initial.plan_pointer_entries[0]),
                "previous_snapshot_id": "another-snapshot",
            },
        ),
    )
    with pytest.raises(
        PTG2SourceSnapshotRollbackConflict,
        match="exact completed rollback",
    ):
        _decision(incomplete_retry)


@pytest.mark.parametrize(
    ("context", "message"),
    [
        (
            _context(
                source_pointer_by_field={
                    "snapshot_id": "stale-snapshot",
                    "previous_snapshot_id": TARGET_SNAPSHOT,
                }
            ),
            "current/previous pair",
        ),
        (
            _context(rollback_pin_by_field=_pin(owner_id="another-owner")),
            "exact requested rollback pin",
        ),
        (
            _context(
                source_plan_pointer_records=(
                    _live_plan_pointer(previous_snapshot_id="another-snapshot"),
                )
            ),
            "requested rollback pair",
        ),
        (
            _context(
                global_pointer_by_field={
                    "snapshot_id": CURRENT_SNAPSHOT,
                    "previous_snapshot_id": "another-snapshot",
                    "source_key": SOURCE_KEY,
                }
            ),
            "same-source global pointer",
        ),
    ],
)
def test_rollback_decision_rejects_stale_pointer_or_pin_state(context, message):
    with pytest.raises(
        PTG2SourceSnapshotRollbackConflict,
        match=message,
    ):
        _decision(context)


@pytest.mark.parametrize(
    ("target_override", "message"),
    [
        ({"status": "validated"}, "not published"),
        (
            {
                "manifest": _serving_manifest(source_key="another_source"),
            },
            "does not match requested",
        ),
        ({"layout_state": "building"}, "sealed immutable layout"),
        ({"mapping_digest": b"short"}, "sealed immutable layout"),
    ],
)
def test_rollback_decision_rejects_invalid_target_before_mutation(
    target_override,
    message,
):
    target_by_field = {**_target_snapshot(), **target_override}

    with pytest.raises(ValueError, match=message):
        _decision(_context(target_snapshot_by_field=target_by_field))


def test_rollback_leaves_global_pointer_for_another_source():
    decision = _decision(
        _context(
            global_pointer_by_field={
                "snapshot_id": "other-source-snapshot",
                "previous_snapshot_id": CURRENT_SNAPSHOT,
                "source_key": "other_source",
            }
        )
    )

    assert decision.should_reverse_global_pointer is False


@pytest.mark.parametrize(
    ("predecessor", "allowed_pointer", "action", "snapshot_id"),
    [
        (
            "allowed-snapshot-a",
            {
                "snapshot_id": CURRENT_SNAPSHOT,
                "previous_snapshot_id": "allowed-snapshot-a",
                "previous_snapshot_import_month": IMPORT_MONTH,
            },
            "reverse",
            "allowed-snapshot-a",
        ),
        (
            None,
            {
                "snapshot_id": CURRENT_SNAPSHOT,
                "previous_snapshot_id": None,
            },
            "delete",
            None,
        ),
    ],
)
def test_rollback_reverses_or_removes_current_allowed_amount_pointer(
    predecessor,
    allowed_pointer,
    action,
    snapshot_id,
):
    decision = _decision(
        _context(
            expected_snapshot_by_field=_expected_snapshot(
                allowed_predecessor=predecessor,
            ),
            allowed_pointer_by_field=allowed_pointer,
        )
    )

    assert decision.allowed_action == action
    assert decision.allowed_snapshot_id == snapshot_id


def test_rollback_exact_retry_verifies_reversed_allowed_amount_pointer():
    initial = _decision(
        _context(
            expected_snapshot_by_field=_expected_snapshot(
                allowed_predecessor="allowed-snapshot-a",
            ),
            allowed_pointer_by_field={
                "snapshot_id": CURRENT_SNAPSHOT,
                "previous_snapshot_id": "allowed-snapshot-a",
                "previous_snapshot_import_month": IMPORT_MONTH,
            },
        )
    )
    retry_context = _context(
        source_pointer_by_field={
            "snapshot_id": TARGET_SNAPSHOT,
            "previous_snapshot_id": CURRENT_SNAPSHOT,
        },
        expected_snapshot_by_field=_expected_snapshot(
            allowed_predecessor="allowed-snapshot-a",
        ),
        source_plan_pointer_records=initial.plan_pointer_entries,
        global_pointer_by_field={
            "snapshot_id": TARGET_SNAPSHOT,
            "previous_snapshot_id": CURRENT_SNAPSHOT,
            "source_key": SOURCE_KEY,
        },
        allowed_pointer_by_field={
            "snapshot_id": "allowed-snapshot-a",
            "previous_snapshot_id": CURRENT_SNAPSHOT,
            "import_month": IMPORT_MONTH,
            "current_snapshot_import_month": IMPORT_MONTH,
        },
    )

    assert _decision(retry_context).allowed_action == "verified"


def test_rollback_rejects_undeclared_allowed_amount_pointer():
    with pytest.raises(
        PTG2SourceSnapshotRollbackConflict,
        match="not declared",
    ):
        _decision(
            _context(
                allowed_pointer_by_field={
                    "snapshot_id": CURRENT_SNAPSHOT,
                    "previous_snapshot_id": None,
                }
            )
        )


class _Transaction:
    def __init__(self):
        self.session = object()
        self.entered = 0
        self.exited = 0

    async def __aenter__(self):
        self.entered += 1
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        self.exited += 1
        return False


def _install_atomic_rollback_fakes(
    monkeypatch,
    transaction,
    context,
    events,
) -> None:
    """Install transaction-bound rollback operations with ordered evidence."""

    async def acquire_lifecycle_lock(session, *, source_key):
        assert session is transaction.session
        assert source_key == SOURCE_KEY
        events.append("locked")

    async def load_context(session, **_kwargs):
        assert session is transaction.session
        events.append("loaded")
        return context

    async def load_timestamp(session):
        assert session is transaction.session
        events.append("timestamped")
        return datetime.datetime(2026, 7, 2, 1, 0)

    async def apply_pointer_changes(session, **kwargs):
        assert session is transaction.session
        assert kwargs["decision"].should_reverse_global_pointer is True
        events.append("applied")

    async def mark_projection_dirty(session, **_kwargs):
        assert session is transaction.session
        events.append("projection-dirty")

    monkeypatch.setattr(rollback.db, "transaction", lambda: transaction)
    collaborator_by_name = {
        "acquire_ptg2_source_lifecycle_lock": acquire_lifecycle_lock,
        "load_rollback_context": load_context,
        "database_utc_timestamp": load_timestamp,
        "apply_rollback": apply_pointer_changes,
        "mark_legacy_global_projection_dirty": mark_projection_dirty,
    }
    for collaborator_name, collaborator in collaborator_by_name.items():
        monkeypatch.setattr(rollback, collaborator_name, collaborator)


@pytest.mark.asyncio
async def test_rollback_runs_lock_validation_and_pointer_updates_in_one_transaction(
    monkeypatch,
):
    """Keep lock, validation, pointer writes, and dirty mark in one transaction."""

    transaction = _Transaction()
    events = []
    context = _context()
    _install_atomic_rollback_fakes(
        monkeypatch,
        transaction,
        context,
        events,
    )

    rollback_report = await rollback.rollback_pinned_ptg2_source_snapshot(
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT,
        expected_current_snapshot_id=CURRENT_SNAPSHOT,
        rollback_owner_id=ROLLBACK_OWNER,
    )

    assert rollback_report["status"] == "rolled_back"
    assert rollback_report["idempotent"] is False
    assert rollback_report["rollback_owner_id"] == ROLLBACK_OWNER
    assert rollback_report["global_pointer"] == "reconciled"
    assert events == [
        "locked", "loaded", "timestamped", "applied", "projection-dirty"
    ]
    assert transaction.entered == transaction.exited == 1


@pytest.mark.asyncio
async def test_rollback_exact_retry_performs_no_pointer_writes(monkeypatch):
    transaction = _Transaction()
    initial = _decision(_context())
    retry_context = _context(
        source_pointer_by_field={
            "snapshot_id": TARGET_SNAPSHOT,
            "previous_snapshot_id": CURRENT_SNAPSHOT,
        },
        source_plan_pointer_records=initial.plan_pointer_entries,
        global_pointer_by_field={
            "snapshot_id": TARGET_SNAPSHOT,
            "previous_snapshot_id": CURRENT_SNAPSHOT,
            "source_key": SOURCE_KEY,
        },
    )
    apply_pointer_changes = AsyncMock()
    monkeypatch.setattr(rollback.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        rollback,
        "acquire_ptg2_source_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        rollback,
        "load_rollback_context",
        AsyncMock(return_value=retry_context),
    )
    monkeypatch.setattr(rollback, "apply_rollback", apply_pointer_changes)

    rollback_report = await rollback.rollback_pinned_ptg2_source_snapshot(
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT,
        expected_current_snapshot_id=CURRENT_SNAPSHOT,
        rollback_owner_id=ROLLBACK_OWNER,
    )

    assert rollback_report["status"] == "already_rolled_back"
    assert rollback_report["idempotent"] is True
    assert rollback_report["rollback_owner_id"] == ROLLBACK_OWNER
    apply_pointer_changes.assert_not_awaited()
