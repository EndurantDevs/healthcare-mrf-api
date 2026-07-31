# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical identities for Provider Directory Profile control-WAL plans."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any

from process.provider_directory_profile_capacity_control_budget import (
    _validate_control_wal_plan_input,
)
from process.provider_directory_profile_capacity_types import (
    ProfileControlWalPlanInput,
    ProviderDirectoryProfileMetadataMutationInput,
    _CONTROL_WAL_PLAN_INPUT_HASH_DOMAIN,
)

def _control_metadata_payload(
    mutation: ProviderDirectoryProfileMetadataMutationInput,
) -> dict[str, Any]:
    return {
        "relation_name": mutation.relation_name,
        "operation": mutation.operation,
        "payload_upper_bytes": mutation.payload_upper_bytes,
        "deleted_toast_chunks": mutation.deleted_toast_chunks,
        "main_index_pages": list(mutation.main_index_pages),
        "toast_index_pages": list(mutation.toast_index_pages),
    }


def profile_control_wal_plan_input_payload(
    plan_input: ProfileControlWalPlanInput,
) -> dict[str, Any]:
    """Return the geometry-independent ordered control-plan coordinates."""

    _validate_control_wal_plan_input(plan_input)
    return {
        "artifact_batch_counts": [
            dataclasses.asdict(artifact_batch)
            for artifact_batch in plan_input.artifact_batch_counts
        ],
        "artifact_scope_recovery_contract_id": (
            plan_input.artifact_scope_recovery_contract_id
        ),
        "evidence_batch_count": plan_input.evidence_batch_count,
        "compact_batch_count": plan_input.compact_batch_count,
        "affected_source_count": plan_input.affected_source_count,
        "admission_row_lock_count": (
            plan_input.admission_row_lock_count
        ),
        "cutover_row_lock_count": plan_input.cutover_row_lock_count,
        "build_checkpoint_insert": _control_metadata_payload(
            plan_input.build_checkpoint_insert
        ),
        "build_checkpoint_update": _control_metadata_payload(
            plan_input.build_checkpoint_update
        ),
        "import_run_update": _control_metadata_payload(
            plan_input.import_run_update
        ),
        "capacity_consumption_insert": _control_metadata_payload(
            plan_input.capacity_consumption_insert
        ),
    }


def canonical_control_wal_plan_json(
    plan_input: ProfileControlWalPlanInput,
) -> str:
    """Return canonical JSON without any capacity-geometry dependency."""

    return json.dumps(
        profile_control_wal_plan_input_payload(plan_input),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def profile_control_wal_plan_input_hash(
    plan_input: ProfileControlWalPlanInput,
) -> str:
    """Return the deterministic identity signed into capacity geometry."""

    canonical_plan_input = canonical_control_wal_plan_json(
        plan_input
    )
    hash_input = (
        f"{_CONTROL_WAL_PLAN_INPUT_HASH_DOMAIN}:{canonical_plan_input}"
    )
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
