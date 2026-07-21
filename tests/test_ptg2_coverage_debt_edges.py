# See LICENSE.

from __future__ import annotations

import asyncio
import hashlib
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_artifact_blobs as artifact_blobs
from process.ptg_parts import ptg2_fast_candidate_audit as fast_audit
from process.ptg_parts import ptg2_shared_source_set as shared_source_set
from process.ptg_parts.ptg2_candidate_audit_contract import FastCandidateAuditError


@pytest.mark.asyncio
async def test_artifact_store_rejects_missing_or_changed_files(tmp_path):
    missing_path = tmp_path / "missing.bin"
    assert await artifact_blobs.store_ptg2_artifact_file(
        missing_path,
        snapshot_id="snapshot",
        artifact_kind="test",
    ) == {"name": "test", "path": str(missing_path)}

    artifact_path = tmp_path / "artifact.bin"
    artifact_path.write_bytes(b"artifact")
    with pytest.raises(ValueError, match="checksum changed"):
        await artifact_blobs.store_ptg2_artifact_file(
            artifact_path,
            snapshot_id="snapshot",
            artifact_kind="test",
            metadata={"sha256": "00" * 32},
        )
    with pytest.raises(ValueError, match="byte_count changed"):
        await artifact_blobs.store_ptg2_artifact_file(
            artifact_path,
            snapshot_id="snapshot",
            artifact_kind="test",
            metadata={
                "sha256": hashlib.sha256(b"artifact").hexdigest(),
                "byte_count": 99,
            },
        )


def test_shared_source_sets_reject_empty_inputs():
    with pytest.raises(ValueError, match="at least one source"):
        shared_source_set.shared_source_set_metadata([])
    with pytest.raises(ValueError, match="ordered source scope"):
        shared_source_set.ordered_source_ordinal_digest([])


@pytest.mark.asyncio
async def test_fast_audit_requires_uvloop_when_requested(monkeypatch):
    class NeverActiveLoop:
        pass

    monkeypatch.setattr(fast_audit.uvloop, "Loop", NeverActiveLoop)
    with pytest.raises(FastCandidateAuditError, match="uvloop_required"):
        fast_audit._event_loop_contract(require_uvloop=True)


def test_fast_audit_challenge_params_allow_optional_fields():
    audit_target = SimpleNamespace(
        plan_id="plan",
        snapshot_id="snapshot",
        plan_market_type="group",
        source_key="source",
    )
    challenge = SimpleNamespace(
        query=SimpleNamespace(code_system="CPT", code="99213", npi="1234567890"),
        negotiated_rate="100",
        service_codes=(),
        modifiers=(),
    )

    query_by_parameter = fast_audit._challenge_params(
        audit_target,
        challenge,
        offset=0,
    )

    assert "service_code" not in query_by_parameter
    assert "billing_code_modifier" not in query_by_parameter


@pytest.mark.asyncio
async def test_fast_audit_cancels_incomplete_tasks():
    async def wait_forever():
        await asyncio.Event().wait()

    audit_task = asyncio.create_task(wait_forever())
    await fast_audit._cancel_incomplete_tasks([audit_task])

    assert audit_task.cancelled()
