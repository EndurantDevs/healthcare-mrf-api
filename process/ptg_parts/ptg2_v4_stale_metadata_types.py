# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared constants and state for PTG V4 stale metadata reconciliation."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Mapping


PTG2_V4_STALE_METADATA_CONTRACT = "ptg2_v4_stale_metadata_reconcile_v1"
PTG2_V4_STALE_METADATA_MARKER = "ptg2_v4_stale_metadata_reconciliation"
PTG2_V4_STALE_METADATA_SECONDS_ENV = (
    "HLTHPRT_PTG2_V4_METADATA_STALE_SECONDS"
)
PTG2_V4_STALE_METADATA_SECONDS_DEFAULT = 21_600
PTG2_V4_STALE_METADATA_SECONDS_MINIMUM = 300
PTG2_V4_STALE_METADATA_ERROR = (
    "authenticated metadata-only stale PTG V4 build reconciliation"
)
PTG2_V4_STALE_METADATA_ZERO_EFFECTS = {
    "deleted_rows": 0,
    "released_layouts": 0,
    "queued_block_hashes": 0,
    "swept_block_hashes": 0,
    "deleted_artifacts": 0,
    "updated_pointers": 0,
    "gc_cycles_started": 0,
}


class PTG2V4StaleMetadataConflict(RuntimeError):
    """The exact stale-build target no longer satisfies the reviewed plan."""


@dataclass(frozen=True)
class PTG2V4StaleMetadataContext:
    """One transaction-consistent view of the exact snapshot/run pair."""

    observed_at: dt.datetime
    snapshot_by_field: Mapping[str, Any] | None
    internal_run_by_field: Mapping[str, Any] | None
    attempt_fence_by_field: Mapping[str, Any] | None
    attachment_count_by_name: Mapping[str, int]


@dataclass(frozen=True)
class PTG2V4StaleMetadataWrite:
    """All exact coordinates and evidence for the three-row CAS write."""

    schema_name: str
    snapshot_id: str
    internal_run_id: str
    context: PTG2V4StaleMetadataContext
    marker_by_field: Mapping[str, Any]
    v4_storage_generation: str
    stale_after_seconds: int


@dataclass(frozen=True)
class PTG2V4StaleMetadataRequest:
    """Normalized execute request and server-owned policy."""

    snapshot_id: str
    internal_run_id: str
    expected_plan_digest: str
    stale_after_seconds: int
    target_digest: str
    schema_name: str


__all__ = [
    "PTG2V4StaleMetadataConflict",
    "PTG2V4StaleMetadataContext",
    "PTG2V4StaleMetadataRequest",
    "PTG2V4StaleMetadataWrite",
    "PTG2_V4_STALE_METADATA_CONTRACT",
    "PTG2_V4_STALE_METADATA_ERROR",
    "PTG2_V4_STALE_METADATA_MARKER",
    "PTG2_V4_STALE_METADATA_SECONDS_DEFAULT",
    "PTG2_V4_STALE_METADATA_SECONDS_ENV",
    "PTG2_V4_STALE_METADATA_SECONDS_MINIMUM",
    "PTG2_V4_STALE_METADATA_ZERO_EFFECTS",
]
