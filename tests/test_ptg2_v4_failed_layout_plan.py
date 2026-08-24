# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attempt identity and secret-free plan contracts for failed V4 recovery."""

from __future__ import annotations

import pytest

from process.ptg_parts import ptg2_v4_failed_layout_recovery as recovery
from process.ptg_parts import ptg2_v4_failed_layout_marker as marker
from tests.test_ptg2_v4_failed_layout_recovery import (
    _RECOVERY_RUN_ID,
    _RECOVERY_SNAPSHOT_ID,
    _failed_owner_fixture,
)


def test_recovery_token_namespace_is_reserved_and_restartable() -> None:
    original_token = "a" * 32
    claimed_token = recovery._owned_v4_abandonment_token(original_token)

    assert recovery._require_recovery_build_token(original_token) == original_token
    assert recovery._require_recovery_build_token(claimed_token) == claimed_token
    assert recovery._plan_digest({}, original_token) == recovery._plan_digest(
        {}, claimed_token
    )
    with pytest.raises(recovery.PTG2V4RecoveryConflict, match="namespace"):
        recovery._require_recovery_build_token("caller-controlled")


def test_plan_digest_binds_exact_attempt_fence_identity() -> None:
    base_plan_by_field = {"contract": marker.PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT}
    first_plan_by_field = {
        **base_plan_by_field,
        "attempt_fence": {"nonce_sha256": "a" * 64, "created_at": "first"},
    }
    recreated_plan_by_field = {
        **base_plan_by_field,
        "attempt_fence": {"nonce_sha256": "b" * 64, "created_at": "second"},
    }

    assert recovery._plan_digest(
        first_plan_by_field, "a" * 32
    ) != recovery._plan_digest(recreated_plan_by_field, "a" * 32)


def test_recovery_plan_binds_only_a_digest_of_the_fingerprint() -> None:
    _, _, layout_by_field, _ = _failed_owner_fixture()
    plan_inputs_by_field = {
        "owner_ids": (_RECOVERY_SNAPSHOT_ID, _RECOVERY_RUN_ID, 491),
        "count_by_name": {},
        "stats_by_name": {"candidate_hashes": 0, "stored_bytes": 0},
        "gate_by_name": {},
        "build_token": "a" * 32,
        "fence_by_field": {
            "fence_nonce": "11111111-1111-1111-1111-111111111111",
            "created_at": "2026-08-24T00:00:00+00:00",
        },
    }
    first_plan_by_field = recovery._recovery_plan(
        layout_by_field=layout_by_field,
        **plan_inputs_by_field,
    )
    changed_fingerprint = b"changed-fingerprint".ljust(32, b"\0")
    second_plan_by_field = recovery._recovery_plan(
        layout_by_field={
            **layout_by_field,
            "semantic_fingerprint": changed_fingerprint,
        },
        **plan_inputs_by_field,
    )

    assert first_plan_by_field["plan_digest"] != second_plan_by_field["plan_digest"]
    fingerprint_hex = bytes(layout_by_field["semantic_fingerprint"]).hex()
    assert fingerprint_hex not in str(first_plan_by_field)
    assert changed_fingerprint.hex() not in str(second_plan_by_field)
