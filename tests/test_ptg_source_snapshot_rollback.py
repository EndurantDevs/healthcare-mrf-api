# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_snapshot_rollback as rollback
from process.ptg_parts import source_snapshot_rollback_state as rollback_state
from process.ptg_parts.source_snapshot_rollback_types import (
    PTG2SourceSnapshotRollbackConflict,
    RollbackContext,
    RollbackDecision,
)


SOURCE_KEY = "source_a"
TARGET_SNAPSHOT = "snapshot_a"
CURRENT_SNAPSHOT = "snapshot_b"
ROLLBACK_OWNER = "source-a-reference"
IMPORT_MONTH = datetime.date(2026, 7, 1)


def _serving_manifest(*, source_key: str = SOURCE_KEY) -> dict:
    return {
        "serving_index": {
            "source_key": source_key,
            "arch_version": "postgres_binary_v3",
            "storage_generation": "shared_blocks_v3",
        }
    }


def _allowed_index(*, previous_snapshot_id: str | None) -> dict:
    return {
        "contract": "ptg2_allowed_amounts_v1",
        "arch_version": "postgres_binary_v3",
        "storage": "postgresql",
        "data_domain": "allowed_amounts",
        "source_key": SOURCE_KEY,
        "current_source_key": "source_a_allowed_amounts",
        "snapshot_scoped": True,
        "previous_snapshot_id": previous_snapshot_id,
    }


def _target_snapshot() -> dict:
    return {
        "snapshot_id": TARGET_SNAPSHOT,
        "status": "published",
        "published_at": datetime.datetime(2026, 7, 1, 1, 0),
        "import_month": IMPORT_MONTH,
        "manifest": _serving_manifest(),
        "snapshot_key": 17,
        "layout_state": "sealed",
        "layout_generation": "shared_blocks_v3",
        "mapping_digest": b"m" * 32,
        "support_digest": b"s" * 32,
    }


def _expected_snapshot(*, allowed_predecessor: object = ...) -> dict:
    manifest = _serving_manifest()
    if allowed_predecessor is not ...:
        manifest["allowed_amount_index"] = _allowed_index(
            previous_snapshot_id=allowed_predecessor,
        )
    return {
        "snapshot_id": CURRENT_SNAPSHOT,
        "status": "published",
        "manifest": manifest,
    }


def _pin(*, owner_id: str = ROLLBACK_OWNER) -> dict:
    return {
        "owner_type": "ptg_v4_rollback",
        "owner_id": owner_id,
        "snapshot_id": TARGET_SNAPSHOT,
        "reason": "retained rollback reference",
    }


def _snapshot_scope() -> dict:
    return {
        "snapshot_id": TARGET_SNAPSHOT,
        "plan_id": "plan-1",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
    }


def _activated_attestation(**overrides) -> dict:
    attestation_by_field = {
        **_snapshot_scope(),
        "snapshot_key": 17,
        "source_key": SOURCE_KEY,
        "contract": "ptg2_v3_release_audit_attestation_v3",
        "activated_at": datetime.datetime(2026, 7, 1, 1, tzinfo=datetime.UTC),
    }
    attestation_by_field.update(overrides)
    return attestation_by_field


def _live_plan_pointer(
    *,
    snapshot_id: str = CURRENT_SNAPSHOT,
    previous_snapshot_id: str = TARGET_SNAPSHOT,
) -> dict:
    return {
        "plan_source_key": "current-plan-key",
        "plan_id": "plan-1",
        "plan_market_type": "group",
        "import_month": IMPORT_MONTH,
        "source_key": SOURCE_KEY,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
    }


def _context(**overrides) -> RollbackContext:
    context_by_field = {
        "source_pointer_by_field": {
            "source_key": SOURCE_KEY,
            "snapshot_id": CURRENT_SNAPSHOT,
            "previous_snapshot_id": TARGET_SNAPSHOT,
            "import_month": IMPORT_MONTH,
        },
        "target_snapshot_by_field": _target_snapshot(),
        "expected_snapshot_by_field": _expected_snapshot(),
        "rollback_pin_by_field": _pin(),
        "target_snapshot_scope_by_field": _snapshot_scope(),
        "target_attestation_by_field": _activated_attestation(),
        "target_plan_scope_records": (
            {"plan_id": "plan-1", "plan_market_type": "group"},
        ),
        "source_plan_pointer_records": (_live_plan_pointer(),),
        "global_pointer_by_field": {
            "snapshot_id": CURRENT_SNAPSHOT,
            "previous_snapshot_id": TARGET_SNAPSHOT,
            "source_key": SOURCE_KEY,
        },
        "allowed_pointer_by_field": {},
    }
    context_by_field.update(overrides)
    return RollbackContext(**context_by_field)


def _decision(
    context: RollbackContext,
) -> RollbackDecision:
    return rollback_state.rollback_decision(
        context,
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT,
        expected_current_snapshot_id=CURRENT_SNAPSHOT,
        rollback_owner_id=ROLLBACK_OWNER,
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


@pytest.mark.asyncio
async def test_rollback_runs_lock_validation_and_pointer_updates_in_one_transaction(
    monkeypatch,
):
    transaction = _Transaction()
    events = []
    context = _context()

    async def acquire_lifecycle_lock(session):
        assert session is transaction.session
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

    monkeypatch.setattr(rollback.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        rollback,
        "acquire_ptg2_lifecycle_lock",
        acquire_lifecycle_lock,
    )
    monkeypatch.setattr(rollback, "load_rollback_context", load_context)
    monkeypatch.setattr(rollback, "database_utc_timestamp", load_timestamp)
    monkeypatch.setattr(rollback, "apply_rollback", apply_pointer_changes)

    rollback_report = await rollback.rollback_pinned_ptg2_source_snapshot(
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT,
        expected_current_snapshot_id=CURRENT_SNAPSHOT,
        rollback_owner_id=ROLLBACK_OWNER,
    )

    assert rollback_report["status"] == "rolled_back"
    assert rollback_report["idempotent"] is False
    assert events == ["locked", "loaded", "timestamped", "applied"]
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
        "acquire_ptg2_lifecycle_lock",
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
    apply_pointer_changes.assert_not_awaited()
