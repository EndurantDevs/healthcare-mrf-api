# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared sealed-content proof builders for synthetic directory fixtures."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from process import provider_directory_proof_store as proof_store


_ENDPOINT_ID = "endpoint-a"
_SOURCE_IDS = ("synthetic-source",)


def _proof_shard_descriptors(
    resource_rows: Sequence[Mapping[str, Any]],
    *,
    dataset_id: str,
    root_run_id: str,
) -> list[dict[str, Any]]:
    """Build one deterministic shard descriptor per synthetic family."""

    descriptors = [
        proof_store.build_dataset_proof_shard(
            [resource_row],
            dataset_id=dataset_id,
            endpoint_id=_ENDPOINT_ID,
            acquisition_root_run_id=root_run_id,
            source_ids=_SOURCE_IDS,
        )[0]
        for resource_row in resource_rows
    ]
    return sorted(descriptors, key=lambda descriptor: descriptor["shard_id"])


def _proof_metadata(
    proof_by_field: Mapping[str, Any],
    resource_types: Sequence[str],
    *,
    dataset_id: str,
    root_run_id: str,
    descriptors: list[dict[str, Any]],
) -> dict[str, Any]:
    """Project synthetic rows into the public stored-proof contract."""

    completion_dataset = proof_by_field["dataset"]
    return {
        "contract_id": proof_store.PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID,
        "complete": True,
        "dataset_id": dataset_id,
        "endpoint_id": _ENDPOINT_ID,
        "acquisition_root_run_id": root_run_id,
        "source_ids": list(_SOURCE_IDS),
        "selected_resources": list(resource_types),
        "dataset_hash": completion_dataset["hash"],
        "resource_count": completion_dataset["count"],
        "resource_hashes": completion_dataset["resource_hashes"],
        "resource_counts": completion_dataset["resource_counts"],
        "source_metrics": {
            "address_records": 0,
            "addressed_locations": 0,
            "distinct_npis": 0,
            "geocoded_locations": 0,
        },
        "npi_set_sha256": proof_store._line_hash(()),
        "shard_count": len(descriptors),
        "shard_set_sha256": proof_store._line_hash(
            proof_store._stable_json(descriptor).encode()
            for descriptor in descriptors
        ),
        "shards": descriptors,
    }


def stored_content_proof(
    proof_by_field: Mapping[str, Any],
    resource_types: Sequence[str],
    resource_rows: Sequence[Mapping[str, Any]],
    *,
    dataset_id: str,
    root_run_id: str,
) -> dict[str, Any]:
    """Build and validate one exact sealed synthetic content proof."""

    descriptors = _proof_shard_descriptors(
        resource_rows,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
    )
    metadata = _proof_metadata(
        proof_by_field,
        resource_types,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
        descriptors=descriptors,
    )
    metadata["proof_sha256"] = proof_store._json_hash(metadata)
    return proof_store.validate_stored_dataset_proof_metadata(
        metadata,
        dataset_id=dataset_id,
        endpoint_id=_ENDPOINT_ID,
        acquisition_root_run_id=root_run_id,
        source_ids=_SOURCE_IDS,
        selected_resources=resource_types,
    )


def metadata_with_stored_content_proof(
    metadata_by_field: Mapping[str, Any],
    proof_by_field: Mapping[str, Any],
    resource_types: Sequence[str],
    resource_rows: Sequence[Mapping[str, Any]],
    *,
    dataset_id: str,
    root_run_id: str,
) -> dict[str, Any]:
    """Return metadata extended with its exact sealed content proof."""

    return {
        **metadata_by_field,
        proof_store.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: (
            stored_content_proof(
                proof_by_field,
                resource_types,
                resource_rows,
                dataset_id=dataset_id,
                root_run_id=root_run_id,
            )
        ),
    }
