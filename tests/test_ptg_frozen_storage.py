# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Storage attribution proofs for frozen multipart control metadata."""

from __future__ import annotations

import pytest

from process.ptg_parts.frozen_rate_storage import (
    FROZEN_RATE_STORAGE_CONTRACT,
    FROZEN_RATE_ZERO_OWNED_STORAGE_FIELDS,
    frozen_rate_storage_measurement,
)


def test_storage_definition_never_double_counts_shared_payload():
    measurement = frozen_rate_storage_measurement(
        binding_rows=3,
        binding_relation_bytes=65_536,
    )

    assert measurement["contract"] == FROZEN_RATE_STORAGE_CONTRACT
    assert measurement["attribution"] == "control_metadata_only"
    assert measurement["owned_payload_bytes"] == {
        field_name: 0
        for field_name in FROZEN_RATE_ZERO_OWNED_STORAGE_FIELDS
    }
    assert measurement["retained_metadata"] == {
        "binding_rows": 3,
        "binding_relation_total_bytes": 65_536,
        "candidate_audit_metadata": "measured_by_candidate_audit_gate",
        "retained_raw_artifacts": (
            "measured_by_whole_snapshot_retained_artifact_gate"
        ),
    }
    assert measurement["excluded_shared_storage"] == [
        "shared_layout",
        "logical_snapshot",
    ]


@pytest.mark.parametrize(
    ("binding_rows", "binding_relation_bytes"),
    [(-1, 0), (0, -1), (False, 0), (0, True)],
)
def test_storage_definition_rejects_ambiguous_measurements(
    binding_rows,
    binding_relation_bytes,
):
    with pytest.raises(ValueError, match="non-negative integers"):
        frozen_rate_storage_measurement(
            binding_rows=binding_rows,
            binding_relation_bytes=binding_relation_bytes,
        )
