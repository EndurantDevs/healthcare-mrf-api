# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic identity for reviewed single-root graph acquisition."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any

from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    ReviewedRootPolicy,
)
from process.provider_directory_rooted_graph_identity import (
    SHA256_PATTERN,
    build_provider_directory_rooted_graph_scope,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphAcquisitionIdentity,
    build_provider_directory_rooted_graph_acquisition_identity,
)


SINGLE_ROOT_OPERATOR_CONTRACT_SHA256 = (
    "a8a6d85a7eff0812216589c85f6adeac3582a28325be86881bb71097252c3253"
)
_CURRENT_IDENTITY_FIELDS = (
    "dataset_id",
    "endpoint_id",
    "source_id",
    "root_source_id",
    "root_endpoint_id",
    "acquisition_source_id",
    "acquisition_endpoint_id",
    "practitioner_origin_source_id",
    "practitioner_origin_endpoint_id",
    "source_authority_id",
    "endpoint_signature_sha256",
    "dataset_hash",
    "resource_count",
    "practitioner_resource_count",
    "root_content_proof_sha256",
    "root_cohort_id",
    "semantic_projection_as_of",
    "operation_key",
    "acquisition_root_run_id",
    "variant",
    "root_publication_contract_id",
)


@dataclass(frozen=True, slots=True, repr=False)
class RootedGraphSingleIdentity:
    """Bind one policy-one operation to its candidate acquisition."""

    operation_key: str = field(repr=False)
    dataset_intent_id: str = field(repr=False)
    scope: Any = field(repr=False)
    candidate: ProviderDirectoryRootedGraphAcquisitionIdentity = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.operation_key) is not str
            or SHA256_PATTERN.fullmatch(self.operation_key) is None
            or self.candidate.dataset_intent_id != self.dataset_intent_id
            or self.candidate.scope_id != getattr(self.scope, "scope_id", None)
            or self.candidate.acquisition_role != "candidate"
        ):
            raise ValueError(
                "provider_directory_rooted_graph_single_root_identity_invalid"
            )


def _identity_digest(prefix: str, payload: object) -> str:
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return prefix + hashlib.sha256(encoded).hexdigest()[:48]


def _single_root_scope(current: Any) -> Any:
    return build_provider_directory_rooted_graph_scope(
        root_dataset_variant=current.variant,
        root_publication_contract_id=current.root_publication_contract_id,
        root_source_id=current.root_source_id,
        root_endpoint_id=current.root_endpoint_id,
        acquisition_source_id=current.acquisition_source_id,
        acquisition_endpoint_id=current.acquisition_endpoint_id,
        source_authority_id=current.source_authority_id,
        root_dataset_id=current.dataset_id,
        root_dataset_hash=current.dataset_hash,
        root_content_proof_sha256=current.root_content_proof_sha256,
        root_resource_count=current.practitioner_resource_count,
    )


def _single_root_intent_id(current: Any, operation_key: str, scope_id: str) -> str:
    return _identity_digest(
        "pdrgi_",
        {
            "operation_key": operation_key,
            "operator_contract_sha256": SINGLE_ROOT_OPERATOR_CONTRACT_SHA256,
            "reviewed_root_policy": ReviewedRootPolicy(1).document(),
            "root": {
                name: getattr(current, name) for name in _CURRENT_IDENTITY_FIELDS
            },
            "scope_id": scope_id,
        },
    )


def derive_single_root_identity(
    current: object,
    *,
    operation_key: str,
) -> RootedGraphSingleIdentity:
    """Derive the exact candidate from one current root and resume key."""

    from process.provider_directory_dataset_scoped_publication_contract import (
        ExactCurrentDataset,
    )

    if (
        type(current) is not ExactCurrentDataset
        or type(operation_key) is not str
        or SHA256_PATTERN.fullmatch(operation_key) is None
    ):
        raise ValueError("provider_directory_rooted_graph_single_root_identity_invalid")
    scope = _single_root_scope(current)
    intent_id = _single_root_intent_id(current, operation_key, scope.scope_id)
    run_id = _identity_digest(
        "pdrgr_",
        {
            "acquisition_role": "candidate",
            "dataset_intent_id": intent_id,
            "operation_key": operation_key,
            "operator_contract_sha256": SINGLE_ROOT_OPERATOR_CONTRACT_SHA256,
        },
    )
    candidate = build_provider_directory_rooted_graph_acquisition_identity(
        scope,
        root_cohort_id=current.root_cohort_id,
        endpoint_signature_sha256=current.endpoint_signature_sha256,
        acquisition_role="candidate",
        run_id=run_id,
        dataset_intent_id=intent_id,
    )
    return RootedGraphSingleIdentity(operation_key, intent_id, scope, candidate)


def single_root_operation_payload(
    current: Any,
    receipt: Any,
    admission: Any,
    operation_key: str,
) -> dict[str, Any]:
    """Project bounded manual-operation evidence without transport details."""

    return {
        "acquisition": {
            "acquisition_id": receipt.acquisition_id,
            "completed_count": receipt.completed_count,
            "edge_count": receipt.edge_count,
            "resource_count": receipt.resource_count,
            "rooted_graph_sha256": receipt.rooted_graph_sha256,
            "run_id": receipt.run_id,
        },
        "admission_contract_id": admission.admission_contract_id,
        "admission_id": admission.admission_id,
        "dataset_intent_id": admission.dataset_intent_id,
        "operation_key": operation_key,
        "publication_acquisition_id": admission.publication_acquisition_id,
        REVIEWED_ROOT_POLICY_METADATA_KEY: admission.reviewed_root_policy_json,
        "root_dataset_hash": current.dataset_hash,
        "root_dataset_id": current.dataset_id,
        "root_dataset_variant": current.variant,
        "rooted_graph_sha256": admission.rooted_graph_sha256,
        "status": "admitted",
    }


__all__ = (
    "derive_single_root_identity",
    "RootedGraphSingleIdentity",
    "SINGLE_ROOT_OPERATOR_CONTRACT_SHA256",
    "single_root_operation_payload",
)
