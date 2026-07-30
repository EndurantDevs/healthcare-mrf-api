# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact replay and rollback-retention proofs for reviewed PTG cutover."""

from __future__ import annotations

import datetime

import pytest

from process.ptg_parts import source_pointer_reviewed_activation as reviewed


class _Result:
    def __init__(self, row):
        self._row = row

    def one_or_none(self):
        return self._row


class _Session:
    def __init__(self, *rows):
        self.rows = list(rows)
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        row = self.rows.pop(0) if self.rows else None
        return _Result(row)


def _activation_fields(previous_snapshot_id: str | None = "snap_old"):
    return {
        "previous_snapshot_id": previous_snapshot_id,
        "storage_generation": "shared_blocks_v4",
    }


@pytest.mark.asyncio
async def test_reviewed_cutover_pins_exact_published_predecessor():
    session = _Session(None, ("reviewed rollback",))

    await reviewed.pin_reviewed_activation_predecessor(
        session,
        schema_name="mrf",
        activation_by_field=_activation_fields(),
        activated_at=datetime.datetime(2026, 7, 30, 12, 0),
        rollback_owner_id="activation-operation",
        is_reviewed_audit_only=True,
    )

    assert len(session.calls) == 2
    assert 'INSERT INTO "mrf".ptg2_snapshot_pin' in session.calls[0][0]
    assert session.calls[0][1]["snapshot_id"] == "snap_old"
    assert session.calls[0][1]["owner_id"] == "activation-operation"
    assert session.calls[1][1]["owner_type"] == "ptg_v4_rollback"


@pytest.mark.asyncio
async def test_reviewed_cutover_requires_predecessor_before_pin_write():
    session = _Session()

    with pytest.raises(ValueError, match="requires a published predecessor"):
        await reviewed.pin_reviewed_activation_predecessor(
            session,
            schema_name="mrf",
            activation_by_field=_activation_fields(None),
            activated_at=datetime.datetime(2026, 7, 30, 12, 0),
            rollback_owner_id="activation-operation",
            is_reviewed_audit_only=True,
        )

    assert session.calls == []


@pytest.mark.asyncio
async def test_reviewed_cutover_requires_owner_before_pin_write():
    with pytest.raises(ValueError, match="requires rollback_owner_id"):
        await reviewed.pin_reviewed_activation_predecessor(
            _Session(),
            schema_name="mrf",
            activation_by_field=_activation_fields(),
            activated_at=datetime.datetime(2026, 7, 30, 12, 0),
            rollback_owner_id=None,
            is_reviewed_audit_only=True,
        )


@pytest.mark.asyncio
async def test_non_reviewed_activation_rejects_or_ignores_owner():
    session = _Session()
    await reviewed.pin_reviewed_activation_predecessor(
        session,
        schema_name="mrf",
        activation_by_field=_activation_fields(),
        activated_at=datetime.datetime(2026, 7, 30, 12, 0),
        rollback_owner_id=None,
        is_reviewed_audit_only=False,
    )
    assert session.calls == []

    with pytest.raises(ValueError, match="supported only for reviewed"):
        await reviewed.pin_reviewed_activation_predecessor(
            session,
            schema_name="mrf",
            activation_by_field=_activation_fields(),
            activated_at=datetime.datetime(2026, 7, 30, 12, 0),
            rollback_owner_id="unexpected-owner",
            is_reviewed_audit_only=False,
        )


@pytest.mark.asyncio
async def test_reviewed_cutover_rejects_missing_pin_after_insert():
    with pytest.raises(
        reviewed.PTG2SourcePointerConflict,
        match="rollback pin was not created",
    ):
        await reviewed.pin_reviewed_activation_predecessor(
            _Session(None, None),
            schema_name="mrf",
            activation_by_field=_activation_fields(),
            activated_at=datetime.datetime(2026, 7, 30, 12, 0),
            rollback_owner_id="activation-operation",
            is_reviewed_audit_only=True,
        )


def test_reviewed_activation_mapping_accepts_supported_row_shapes():
    class _Row:
        _mapping = {"value": 3}

    assert reviewed._mapping({"value": 1}) == {"value": 1}
    assert reviewed._mapping('{"value": 2}') == {"value": 2}
    assert reviewed._mapping("[]") == {}
    assert reviewed._mapping("{") == {}
    assert reviewed._mapping(None) == {}
    assert reviewed._mapping(_Row()) == {"value": 3}


def test_reviewed_rollback_owner_rejects_oversized_identity():
    with pytest.raises(ValueError, match="exceeds 96"):
        reviewed.reviewed_rollback_owner_id("x" * 97, required=True)


def _completed_row(*, digest: bytes, status: str = "published"):
    return {
        "status": status,
        "published_at": datetime.datetime(2026, 7, 30, 12, 0),
        "previous_snapshot_id": "snap_old",
        "manifest": {
            "activation": {
                "state": "activated",
                "mode": "reviewed_audit_only_control",
                "source_key": "source_a",
            }
        },
        "storage_generation": "shared_blocks_v4",
        "activation_intent": "audit_only",
        "attestation_digest": digest,
        "activated_at": datetime.datetime(2026, 7, 30, 12, 0),
        "plan_id": "000000001",
        "plan_market_type": "group",
        "current_snapshot_id": "snap_new",
        "current_previous_snapshot_id": "snap_old",
        "rollback_pin_reason": "reviewed rollback",
        "plan_source_count": 1,
        "conflicting_plan_source_count": 0,
    }


@pytest.mark.asyncio
async def test_reviewed_cutover_replay_accepts_only_exact_committed_pair():
    approval_digest = bytes.fromhex("ab" * 32)
    session = _Session(_completed_row(digest=approval_digest))

    replay = await reviewed.completed_reviewed_activation(
        session,
        schema_name="mrf",
        source_key="source_a",
        snapshot_id="snap_new",
        expected_current_snapshot_id="snap_old",
        expected_audit_only_attestation_digest=approval_digest,
        rollback_owner_id="activation-operation",
    )

    assert replay == {
        "status": "already_promoted",
        "source_key": "source_a",
        "snapshot_id": "snap_new",
        "storage_generation": "shared_blocks_v4",
        "previous_snapshot_id": "snap_old",
        "plan_source_count": 1,
        "global_pointer": "reconciled",
        "rollback_owner_id": "activation-operation",
        "idempotent": True,
    }


@pytest.mark.asyncio
async def test_reviewed_cutover_replay_rejects_digest_or_route_drift():
    approval_digest = bytes.fromhex("ab" * 32)
    drifted = _completed_row(digest=bytes.fromhex("cd" * 32))
    drifted["current_snapshot_id"] = "snap_other"

    with pytest.raises(
        reviewed.PTG2SourcePointerConflict,
        match="does not match the exact reviewed activation",
    ):
        await reviewed.completed_reviewed_activation(
            _Session(drifted),
            schema_name="mrf",
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
            expected_audit_only_attestation_digest=approval_digest,
            rollback_owner_id="activation-operation",
        )


@pytest.mark.asyncio
async def test_reviewed_cutover_replay_leaves_validated_candidate_to_fresh_path():
    approval_digest = bytes.fromhex("ab" * 32)

    result = await reviewed.completed_reviewed_activation(
        _Session(_completed_row(digest=approval_digest, status="validated")),
        schema_name="mrf",
        source_key="source_a",
        snapshot_id="snap_new",
        expected_current_snapshot_id="snap_old",
        expected_audit_only_attestation_digest=approval_digest,
        rollback_owner_id="activation-operation",
    )

    assert result is None


@pytest.mark.asyncio
async def test_reviewed_cutover_replay_requires_digest_and_predecessor():
    assert (
        await reviewed.completed_reviewed_activation(
            _Session(),
            schema_name="mrf",
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
            expected_audit_only_attestation_digest=None,
            rollback_owner_id="activation-operation",
        )
        is None
    )

    with pytest.raises(ValueError, match="expected_current_snapshot_id"):
        await reviewed.completed_reviewed_activation(
            _Session(),
            schema_name="mrf",
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id=None,
            expected_audit_only_attestation_digest=bytes.fromhex("ab" * 32),
            rollback_owner_id="activation-operation",
        )


@pytest.mark.asyncio
async def test_reviewed_cutover_replay_returns_none_without_matching_row():
    result = await reviewed.completed_reviewed_activation(
        _Session(None),
        schema_name="mrf",
        source_key="source_a",
        snapshot_id="snap_new",
        expected_current_snapshot_id="snap_old",
        expected_audit_only_attestation_digest=bytes.fromhex("ab" * 32),
        rollback_owner_id="activation-operation",
    )

    assert result is None


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("published_at", None),
        ("previous_snapshot_id", "snap_other"),
        ("manifest", {}),
        (
            "manifest",
            {
                "activation": {
                    "state": "activated",
                    "mode": "automatic",
                    "source_key": "source_a",
                }
            },
        ),
        (
            "manifest",
            {
                "activation": {
                    "state": "activated",
                    "mode": "reviewed_audit_only_control",
                    "source_key": "source_other",
                }
            },
        ),
        ("activation_intent", "audit_and_activate"),
        ("activated_at", None),
        ("attestation_digest", b""),
        ("current_snapshot_id", "snap_other"),
        ("current_previous_snapshot_id", "snap_other"),
        ("rollback_pin_reason", ""),
        ("plan_source_count", 0),
        ("conflicting_plan_source_count", 1),
    ],
)
def test_reviewed_cutover_exact_state_rejects_each_drift(
    field_name,
    replacement,
):
    row = _completed_row(digest=bytes.fromhex("ab" * 32))
    row[field_name] = replacement

    with pytest.raises(
        reviewed.PTG2SourcePointerConflict,
        match="does not match the exact reviewed activation",
    ):
        reviewed._require_exact_reviewed_activation(
            row,
            source_key="source_a",
            snapshot_id="snap_new",
            predecessor_snapshot_id="snap_old",
            expected_attestation_digest=bytes.fromhex("ab" * 32),
        )
