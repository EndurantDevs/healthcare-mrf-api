# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic fixtures shared by semantic-content edge tests."""

from __future__ import annotations

import importlib


importer = importlib.import_module("process.provider_directory_fhir")
resource_hash = importlib.import_module("process.provider_directory_resource_hash")


SEMANTIC_CONTRACT = resource_hash.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
LEGACY_CONTRACT = resource_hash.LEGACY_RESOURCE_HASH_CONTRACT
NEUTRAL_CONTRACT = resource_hash.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
PROJECTION_DATE = "2026-08-09"
ZERO_HASH = "0" * 64


def candidate(
    *,
    resource_hash_contract: str = SEMANTIC_CONTRACT,
    proof_resource_scope: tuple[str, ...] | None = ("Practitioner",),
) -> importer.EndpointDatasetCandidate:
    """Build one synthetic mutable dataset candidate."""

    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-edge",
        dataset_id="dataset-edge",
        acquisition_root_run_id="root-edge",
        source_ids=("source-edge",),
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
        import_run_id="root-edge",
        previous_dataset_id=None,
        resource_hash_contract=resource_hash_contract,
        semantic_projection_as_of=(
            PROJECTION_DATE if resource_hash_contract == SEMANTIC_CONTRACT else None
        ),
        proof_resource_scope=proof_resource_scope,
    )


def semantic_parent_metadata(
    *,
    selected_resources: list[str] | None = None,
    proof_resource_scope: list[str] | None = None,
) -> dict[str, object]:
    """Build one synthetic semantic-v3 parent metadata object."""

    return {
        "resource_hash_contract": SEMANTIC_CONTRACT,
        "semantic_projection_as_of": PROJECTION_DATE,
        "selected_resources": selected_resources or ["Practitioner"],
        "proof_resource_scope": proof_resource_scope or ["Practitioner"],
    }


def generic_dataset_row(
    *,
    dataset_id: str | None = "dataset-edge",
    resource_type: str | None = "Organization",
    resource_id: str | None = "resource-edge",
) -> dict[str, object]:
    """Build one exact generic retained-resource row."""

    payload = {"resource_id": resource_id}
    return {
        "dataset_id": dataset_id,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_json": payload,
        "payload_hash": resource_hash.resource_payload_sha256_for_contract(
            payload,
            SEMANTIC_CONTRACT,
        ),
        "acquired_resource_sha256": None,
    }


def practitioner_payload(
    *,
    fhir_meta: dict[str, object] | None = None,
) -> dict[str, object]:
    """Build one canonical synthetic practitioner payload."""

    payload: dict[str, object] = {
        "resource_id": "practitioner-edge",
        "names": [
            {
                "family": "Example",
                "given": ["Taylor"],
                "text": "Taylor Example",
            }
        ],
        "family_name": "Example",
        "given_names": ["Taylor"],
        "full_name": "Taylor Example",
    }
    if fhir_meta is not None:
        payload["fhir_meta"] = fhir_meta
    return payload
