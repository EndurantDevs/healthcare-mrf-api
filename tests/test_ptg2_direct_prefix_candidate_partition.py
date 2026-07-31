# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_batch as candidate_batch
from api import ptg2_candidate_audit_partition as candidate_partition
from tests.test_ptg2_manifest_tables import (
    FakeSession,
    strict_candidate_row,
    strict_direct_v4_serving_index,
    strict_v4_root_row,
)
from tests.test_ptg2_partitioned_candidate_audit_api import (
    _access,
    _candidate_io,
    _request,
)


@pytest.mark.asyncio
async def test_partition_accepts_complete_direct_prefix_manifest(monkeypatch):
    audit_request = _request()
    serving_index = strict_direct_v4_serving_index()
    candidate_row = strict_candidate_row(
        serving_index,
        snapshot_plan_id="12-3456789",
    )
    candidate_row["candidate_serving_index"]["source_key"] = "test-source"
    session = FakeSession(
        [
            None,
            candidate_row,
            strict_v4_root_row(serving_index),
        ]
    )
    binding_validator = AsyncMock()
    monkeypatch.setattr(
        candidate_partition,
        "_validate_partition_binding",
        binding_validator,
    )
    challenge = audit_request.source_challenges[0]
    condition_key = (
        challenge.code_system,
        challenge.code,
        challenge.npi,
        challenge.source_artifact_key,
        challenge.tuple_digest,
    )
    data_loader = AsyncMock(
        return_value=candidate_batch._CandidateAuditData(
            challenges=candidate_partition._partition_challenges(audit_request),
            witness_io={},
            network_digest_sets_by_condition={
                condition_key: (
                    frozenset(challenge.network_name_digests),
                ),
            },
            candidate_processing_io=_candidate_io(),
            persisted_audit_occurrence_count=1,
        )
    )
    monkeypatch.setattr(
        candidate_batch,
        "_candidate_data_for_conditions",
        data_loader,
    )

    audit_result = await candidate_partition.audit_candidate_partition(
        session,
        audit_request,
        _access(),
    )

    assert audit_result.matched_challenge_count == 1
    assert audit_result.validated_persisted_audit_occurrence_count == 1
    assert len(session.calls) == 3
    binding_validator.assert_awaited_once()
    data_loader.assert_awaited_once()
