# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed margin for held candidate approval and exact rollback."""

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_candidate_attestation as attestation
from process.ptg_parts import source_snapshot_rollback_allowed as rollback_allowed
from process.ptg_parts import source_snapshot_rollback_state as rollback_state
from process.ptg_parts.source_snapshot_rollback_types import (
    PTG2SourceSnapshotRollbackConflict,
)
from tests.test_ptg_source_snapshot_rollback import (
    CURRENT_SNAPSHOT,
    SOURCE_KEY,
    TARGET_SNAPSHOT,
    _context,
    _expected_snapshot,
    _target_snapshot,
)


class _Transaction:
    def __init__(self, session: object) -> None:
        self.session = session

    async def __aenter__(self) -> object:
        return self.session

    async def __aexit__(self, *_args: object) -> None:
        return None


def _attestation_row(
    *,
    stored_intent: str = "audit_only",
    requested_intent: str = "audit_only",
    activated_at: object = None,
    is_current: bool = True,
    digest: bytes | None = None,
) -> tuple[object, ...]:
    report_digest = b"r" * 32
    stored_digest = digest or attestation.candidate_attestation_digest(
        report_digest,
        stored_intent,
    )
    return (
        report_digest,
        {"contract": "synthetic"},
        stored_intent,
        stored_digest,
        datetime.datetime(2026, 7, 20, 12),
        datetime.datetime(2026, 7, 20, 13),
        activated_at,
        is_current,
    )


@pytest.mark.parametrize("value", ("", "activate", None))
def test_candidate_activation_intent_rejects_implicit_or_unknown_values(value):
    with pytest.raises(ValueError, match="activation intent is invalid"):
        attestation.normalize_candidate_activation_intent(value)


def test_candidate_attestation_digest_rejects_non_sha256_report_digest():
    with pytest.raises(ValueError, match="report digest is invalid"):
        attestation.candidate_attestation_digest(b"short", "audit_only")


def test_held_attestation_binding_rejects_conflict_tamper_and_reuse():
    conflicting_row = _attestation_row(stored_intent="audit_and_activate")
    with pytest.raises(ValueError, match="activation intent conflicts"):
        attestation._validated_attestation_binding(
            conflicting_row,
            activation_intent="audit_only",
            allow_expired=False,
        )

    with pytest.raises(ValueError, match="intent digest is invalid"):
        attestation._validated_attestation_binding(
            _attestation_row(digest=b"x" * 32),
            activation_intent="audit_only",
            allow_expired=False,
        )

    with pytest.raises(
        attestation.CandidateAttestationApprovalConflict,
        match="already consumed",
    ):
        attestation._validated_attestation_binding(
            _attestation_row(activated_at=datetime.datetime(2026, 7, 20, 12)),
            activation_intent="audit_only",
            allow_expired=False,
        )


def test_held_attestation_binding_distinguishes_expired_probe_from_activation():
    expired_row = _attestation_row(is_current=False)
    assert (
        attestation._validated_attestation_binding(
            expired_row,
            activation_intent="audit_only",
            allow_expired=True,
        )
        is None
    )
    with pytest.raises(ValueError, match="no current passing"):
        attestation._validated_attestation_binding(
            expired_row,
            activation_intent="audit_only",
            allow_expired=False,
        )


def test_held_attestation_target_requires_every_release_coordinate():
    with pytest.raises(ValueError, match="snapshot, source, plan, and market"):
        attestation._held_attestation_target(
            snapshot_id="snapshot",
            source_key="",
            plan_id="plan",
            plan_market_type="group",
            storage_generation="shared_blocks_v4",
        )
    assert attestation._held_attestation_target(
        snapshot_id=" snapshot ",
        source_key=" SOURCE_A ",
        plan_id=" plan ",
        plan_market_type=" GROUP ",
        storage_generation="shared_blocks_v4",
    ) == {
        "snapshot_id": "snapshot",
        "source_key": "source_a",
        "plan_id": "plan",
        "plan_market_type": "group",
        "storage_generation": "shared_blocks_v4",
    }


@pytest.mark.asyncio
async def test_held_attestation_loader_returns_public_digest_projection(
    monkeypatch,
):
    session = object()
    attested_at = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.UTC)
    expires_at = attested_at + datetime.timedelta(hours=1)
    loaded_attestation_by_field = {
        "report_digest": b"r" * 32,
        "attestation_digest": b"a" * 32,
        "attested_at": attested_at,
        "expires_at": expires_at,
    }
    monkeypatch.setattr(attestation.db, "transaction", lambda: _Transaction(session))
    lifecycle_lock = AsyncMock()
    monkeypatch.setattr(
        attestation,
        "acquire_ptg2_source_lifecycle_lock",
        lifecycle_lock,
    )
    monkeypatch.setattr(
        attestation,
        "_locked_candidate_identity",
        AsyncMock(
            return_value={
                "source_key": "source_a",
                "plan_id": "plan",
                "plan_market_type": "group",
                "storage_generation": "shared_blocks_v4",
            }
        ),
    )
    loader = AsyncMock(return_value=loaded_attestation_by_field)
    monkeypatch.setattr(
        attestation,
        "_load_candidate_audit_attestation_in_transaction",
        loader,
    )

    public_attestation = await attestation.load_held_candidate_audit_attestation(
        snapshot_id="snapshot",
        source_key="source_a",
        plan_id="plan",
        plan_market_type="group",
        storage_generation="shared_blocks_v4",
    )

    assert public_attestation == {
        **loaded_attestation_by_field,
        "report_digest": (b"r" * 32).hex(),
        "attestation_digest": (b"a" * 32).hex(),
        "attested_at": attested_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }
    lifecycle_lock.assert_awaited_once_with(session, source_key="source_a")
    assert loader.await_args.kwargs["allow_expired"] is True


@pytest.mark.asyncio
async def test_held_attestation_loader_rejects_identity_drift_and_expiry(
    monkeypatch,
):
    session = object()
    monkeypatch.setattr(attestation.db, "transaction", lambda: _Transaction(session))
    monkeypatch.setattr(
        attestation,
        "acquire_ptg2_source_lifecycle_lock",
        AsyncMock(),
    )
    identity_loader = AsyncMock(
        return_value={
            "source_key": "changed",
            "plan_id": "plan",
            "plan_market_type": "group",
            "storage_generation": "shared_blocks_v4",
        }
    )
    monkeypatch.setattr(attestation, "_locked_candidate_identity", identity_loader)
    held_loader = AsyncMock(return_value=None)
    monkeypatch.setattr(
        attestation,
        "_load_candidate_audit_attestation_in_transaction",
        held_loader,
    )

    with pytest.raises(ValueError, match="identity changed"):
        await attestation.load_held_candidate_audit_attestation(
            snapshot_id="snapshot",
            source_key="source_a",
            plan_id="plan",
            plan_market_type="group",
            storage_generation="shared_blocks_v4",
        )

    identity_loader.return_value["source_key"] = "source_a"
    assert (
        await attestation.load_held_candidate_audit_attestation(
            snapshot_id="snapshot",
            source_key="source_a",
            plan_id="plan",
            plan_market_type="group",
            storage_generation="shared_blocks_v4",
        )
        is None
    )


def test_audit_only_consumption_requires_explicit_review_digest():
    with pytest.raises(ValueError, match="requires its exact approval digest"):
        attestation._consumed_attestation_binding(
            report_digest=b"r" * 32,
            activation_intent="audit_only",
            expected_attestation_digest=None,
        )


@pytest.mark.parametrize(
    "value",
    (
        '{"allowed_amount_index":',
        "[]",
        17,
    ),
)
def test_rollback_manifest_parsers_fail_closed_on_non_objects(value):
    assert rollback_allowed._manifest_mapping(value) == {}
    assert rollback_state._manifest_mapping(value) == {}


def test_allowed_pointer_contract_rejects_malformed_binding():
    with pytest.raises(ValueError, match="must be an object"):
        rollback_allowed._allowed_pointer_predecessor(
            {"manifest": {"allowed_amount_index": []}},
            source_key=SOURCE_KEY,
        )
    with pytest.raises(ValueError, match="invalid contract binding"):
        rollback_allowed._allowed_pointer_predecessor(
            {
                "manifest": {
                    "allowed_amount_index": {
                        "contract": "wrong",
                    }
                }
            },
            source_key=SOURCE_KEY,
        )


def test_allowed_pointer_contract_requires_snapshot_scope():
    manifest = _expected_snapshot(allowed_predecessor=TARGET_SNAPSHOT)["manifest"]
    manifest["allowed_amount_index"]["snapshot_scoped"] = False
    with pytest.raises(ValueError, match="not snapshot scoped"):
        rollback_allowed._allowed_pointer_predecessor(
            {"manifest": manifest},
            source_key=SOURCE_KEY,
        )


def test_allowed_pointer_completed_state_requires_exact_pair_and_month():
    with pytest.raises(
        PTG2SourceSnapshotRollbackConflict,
        match="exact completed rollback",
    ):
        rollback_allowed._completed_pointer_decision(
            {"snapshot_id": "wrong"},
            predecessor=TARGET_SNAPSHOT,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
        )
    with pytest.raises(
        PTG2SourceSnapshotRollbackConflict,
        match="invalid completed import month",
    ):
        rollback_allowed._completed_pointer_decision(
            {
                "snapshot_id": TARGET_SNAPSHOT,
                "previous_snapshot_id": CURRENT_SNAPSHOT,
                "current_snapshot_import_month": "2026-07",
            },
            predecessor=TARGET_SNAPSHOT,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
        )


def test_allowed_pointer_forward_state_requires_predecessor_and_month():
    with pytest.raises(
        PTG2SourceSnapshotRollbackConflict,
        match="published predecessor",
    ):
        rollback_allowed._forward_pointer_decision(
            {"snapshot_id": "wrong"},
            predecessor=TARGET_SNAPSHOT,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
        )
    with pytest.raises(
        PTG2SourceSnapshotRollbackConflict,
        match="no retained import month",
    ):
        rollback_allowed._forward_pointer_decision(
            {
                "snapshot_id": CURRENT_SNAPSHOT,
                "previous_snapshot_id": TARGET_SNAPSHOT,
            },
            predecessor=TARGET_SNAPSHOT,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
        )


@pytest.mark.parametrize(
    ("target_override", "message"),
    (
        ({"snapshot_id": "missing"}, "not found"),
        ({"published_at": None}, "no publication timestamp"),
        ({"manifest": {}}, "must use postgres_binary_v3"),
        ({"import_month": "2026-07"}, "no import month"),
    ),
)
def test_rollback_target_validation_rejects_incomplete_publication(
    target_override,
    message,
):
    target_snapshot_by_field = {**_target_snapshot(), **target_override}
    with pytest.raises(ValueError, match=message):
        rollback_state._validate_target_snapshot(
            target_snapshot_by_field,
            source_key=SOURCE_KEY,
            snapshot_id=TARGET_SNAPSHOT,
        )


def test_rollback_expected_snapshot_and_plan_scope_are_mandatory():
    with pytest.raises(ValueError, match="expected current snapshot"):
        rollback_state._validate_expected_snapshot(
            {},
            source_key=SOURCE_KEY,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
        )
    with pytest.raises(ValueError, match="no immutable logical plan mappings"):
        rollback_state._target_plan_pointer_entries(
            _context(target_plan_scope_records=()),
            source_key=SOURCE_KEY,
            snapshot_id=TARGET_SNAPSHOT,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
            import_month=datetime.date(2026, 7, 1),
        )


def test_rollback_global_pointer_can_be_absent_but_not_anonymous():
    assert (
        rollback_state._should_reverse_global_pointer(
            _context(global_pointer_by_field={}),
            source_key=SOURCE_KEY,
            snapshot_id=TARGET_SNAPSHOT,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
            is_already_rolled_back=False,
        )
        is False
    )
    with pytest.raises(
        PTG2SourceSnapshotRollbackConflict,
        match="source identity is unavailable",
    ):
        rollback_state._should_reverse_global_pointer(
            _context(
                global_pointer_by_field={
                    "snapshot_id": CURRENT_SNAPSHOT,
                    "previous_snapshot_id": TARGET_SNAPSHOT,
                }
            ),
            source_key=SOURCE_KEY,
            snapshot_id=TARGET_SNAPSHOT,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
            is_already_rolled_back=False,
        )
