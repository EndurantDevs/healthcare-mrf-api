# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stable execution evidence for immutable dataset rehydration."""

from __future__ import annotations

from typing import Any

from process.provider_directory_dataset_rehydrate_types import (
    DatasetScope,
    RehydrationCheckpoint,
    RehydrationRequest,
    ResourceProof,
)


def _resource_summary(
    checkpoint: RehydrationCheckpoint,
    proof: ResourceProof,
    *,
    reused: bool,
) -> dict[str, Any]:
    """Build one resource type's durable execution evidence."""
    return {
        "state": checkpoint.state,
        "input": checkpoint.input_count,
        "mapped": checkpoint.mapped_count,
        "rejected": checkpoint.rejected_count,
        **proof.as_dict(),
        "reused_complete_checkpoint": reused,
    }


def _execution_summary(
    request: RehydrationRequest,
    scope: DatasetScope,
    summary_by_resource: dict[str, dict[str, Any]],
    digest_before: str,
    digest_after: str,
) -> dict[str, Any]:
    """Build aggregate identity, digest, and count evidence."""
    rejected_count = sum(
        resource_summary["rejected"]
        for resource_summary in summary_by_resource.values()
    )
    return {
        "mode": "provider_directory_dataset_rehydrate",
        "status": "rejected" if rejected_count else "complete",
        "network_calls": 0,
        "source_id": scope.source_id,
        "endpoint_id": scope.endpoint_id,
        "dataset_id": scope.dataset_id,
        "acquisition_root_run_id": scope.acquisition_root_run_id,
        "owner_run_id": request.owner_run_id,
        "dataset_hash": scope.dataset_hash,
        "dataset_hash_verified_before": digest_before,
        "dataset_hash_verified_after": digest_after,
        "batch_size": request.batch_size,
        "dataset_resource_count": scope.resource_count,
        "input_count": sum(
            resource_summary["input"]
            for resource_summary in summary_by_resource.values()
        ),
        "mapped_count": sum(
            resource_summary["mapped"]
            for resource_summary in summary_by_resource.values()
        ),
        "rejected_count": rejected_count,
        "resources": summary_by_resource,
    }
