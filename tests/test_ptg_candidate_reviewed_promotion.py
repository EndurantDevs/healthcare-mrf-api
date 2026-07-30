# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control-boundary proofs for reviewed PTG candidate promotion."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_snapshot_control


def test_promote_ptg2_source_snapshot_threads_reviewed_hold_digest(monkeypatch):
    """Decode and pass the exact full approval digest to pointer control."""

    approval_digest = "ab" * 32
    publish = AsyncMock(return_value={"status": "promoted"})
    monkeypatch.setattr(
        source_snapshot_control,
        "activate_ptg2_source_candidate",
        publish,
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_clear_ptg2_snapshot_cache",
        lambda: None,
    )

    promotion_by_field = asyncio.run(
        source_snapshot_control.promote_ptg2_source_snapshot(
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
            expected_audit_only_attestation_digest=approval_digest,
            rollback_owner_id="activation-operation",
        )
    )

    assert promotion_by_field == {"status": "promoted"}
    assert publish.await_args.kwargs[
        "expected_audit_only_attestation_digest"
    ] == bytes.fromhex(approval_digest)
    assert publish.await_args.kwargs["rollback_owner_id"] == (
        "activation-operation"
    )


def test_promote_ptg2_source_snapshot_rejects_malformed_hold_digest(monkeypatch):
    """Reject truncated approval material before activation or pointer I/O."""

    publish = AsyncMock()
    monkeypatch.setattr(
        source_snapshot_control,
        "activate_ptg2_source_candidate",
        publish,
    )

    with pytest.raises(ValueError, match="64 hex characters"):
        asyncio.run(
            source_snapshot_control.promote_ptg2_source_snapshot(
                source_key="source_a",
                snapshot_id="snap_new",
                expected_audit_only_attestation_digest="ab",
            )
        )
    publish.assert_not_awaited()
