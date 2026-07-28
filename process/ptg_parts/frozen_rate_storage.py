# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Storage attribution contract for immutable frozen multipart dispatch."""

from __future__ import annotations

from typing import Any


FROZEN_RATE_STORAGE_CONTRACT = "ptg_frozen_rate_storage_attribution_v2"
FROZEN_RATE_ZERO_OWNED_STORAGE_FIELDS = (
    "cas_block_bytes",
    "provider_graph_bytes",
    "rate_bytes",
    "serving_bytes",
    "snapshot_bytes",
    "snapshot_map_bytes",
)


def frozen_rate_storage_measurement(
    *,
    binding_rows: int,
    binding_relation_bytes: int,
) -> dict[str, Any]:
    """Separate frozen control metadata from shared snapshot payload storage."""

    if (
        isinstance(binding_rows, bool)
        or not isinstance(binding_rows, int)
        or binding_rows < 0
        or isinstance(binding_relation_bytes, bool)
        or not isinstance(binding_relation_bytes, int)
        or binding_relation_bytes < 0
    ):
        raise ValueError(
            "frozen storage metadata measurements must be non-negative integers"
        )
    return {
        "contract": FROZEN_RATE_STORAGE_CONTRACT,
        "attribution": "control_metadata_only",
        "owned_payload_bytes": {
            field_name: 0
            for field_name in FROZEN_RATE_ZERO_OWNED_STORAGE_FIELDS
        },
        "retained_metadata": {
            "binding_rows": binding_rows,
            "binding_relation_total_bytes": binding_relation_bytes,
            "candidate_audit_metadata": "measured_by_candidate_audit_gate",
            "retained_raw_artifacts": (
                "measured_by_whole_snapshot_retained_artifact_gate"
            ),
        },
        "excluded_shared_storage": [
            "shared_layout",
            "logical_snapshot",
        ],
    }


__all__ = [
    "FROZEN_RATE_STORAGE_CONTRACT",
    "FROZEN_RATE_ZERO_OWNED_STORAGE_FIELDS",
    "frozen_rate_storage_measurement",
]
