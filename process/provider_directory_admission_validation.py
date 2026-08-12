# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Completion checks for streamed Provider Directory admission proofs."""

from __future__ import annotations

from dataclasses import dataclass
import json
from collections.abc import Mapping
from typing import Any

from process import provider_directory_proof_store as proof_store
from process.provider_directory_admission_seal import (
    ADMISSION_KIND_GENERIC,
    ADMISSION_KIND_UHC_CANONICAL,
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
    _generic_proof_summary,
    _LEGACY_PROOF_FIELDS,
    _receipt,
    _SEMANTIC_PROOF_FIELDS,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID,
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY,
    ProviderDirectoryProofStoreError,
)
from process.provider_directory_resource_hash import LEGACY_RESOURCE_HASH_CONTRACT
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
    UhcCanonicalProofError,
    validate_uhc_canonical_content_proof,
)
from process.uhc_final_publication_contract import (
    UhcFinalPublicationError,
    UhcFinalPublicationExpectation,
    validate_uhc_final_publication,
)


@dataclass(frozen=True)
class _AdmissionCopyExpectation:
    dataset_id: str
    endpoint_id: str
    evidence_run_id: str
    dataset_hash: str
    resource_count: int
    expected_resource_hashes: Any = None
    expected_resource_counts: Any = None


def _validate_uhc_contract(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
) -> dict[str, Any]:
    try:
        canonical_proof = validate_uhc_canonical_content_proof(
            proof_stream.metadata.get(UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY),
            dataset_id=expected.dataset_id,
            endpoint_id=expected.endpoint_id,
            acquisition_root_run_id=expected.evidence_run_id,
        )
        publication_record_by_field = {
            "source_id": canonical_proof["source_id"],
            "dataset_id": expected.dataset_id,
            "endpoint_id": expected.endpoint_id,
            "acquisition_root_run_id": expected.evidence_run_id,
            "status": "published",
            "is_current": True,
            "dataset_hash": expected.dataset_hash,
            "resource_count": expected.resource_count,
            "publication_metadata_json": proof_stream.metadata,
        }
        validate_uhc_final_publication(
            publication_record_by_field,
            UhcFinalPublicationExpectation(
                source_id=canonical_proof["source_id"],
                dataset_id=expected.dataset_id,
                endpoint_id=expected.endpoint_id,
                acquisition_root_run_id=expected.evidence_run_id,
                selected_resources=tuple(
                    sorted(canonical_proof["resource_counts"])
                ),
                semantic_contract_id=canonical_proof["semantic_contract_id"],
                catalog_set_sha256=canonical_proof["catalog_set_sha256"],
            ),
        )
    except (UhcCanonicalProofError, UhcFinalPublicationError) as error:
        raise AdmissionSealError(
            "provider_directory_admission_uhc_proof_invalid"
        ) from error
    return canonical_proof


def _uhc_receipt(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
) -> ProviderDirectoryAdmissionSeal:
    if PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY in proof_stream.seen_root_keys:
        raise AdmissionSealError("provider_directory_admission_proof_kind_invalid")
    canonical_proof = _validate_uhc_contract(proof_stream, expected)
    if (
        canonical_proof.get("dataset_hash") != expected.dataset_hash
        or canonical_proof.get("resource_count") != expected.resource_count
        or expected.expected_resource_hashes is not None
        and canonical_proof.get("resource_hashes")
        != expected.expected_resource_hashes
        or expected.expected_resource_counts is not None
        and canonical_proof.get("resource_counts")
        != expected.expected_resource_counts
    ):
        raise AdmissionSealError(
            "provider_directory_admission_completion_summary_invalid"
        )
    return _receipt(
        proof_stream.metadata,
        admission_kind=ADMISSION_KIND_UHC_CANONICAL,
        proof_sha256=canonical_proof.get("proof_sha256"),
        resource_counts=canonical_proof.get("resource_counts"),
    )


def _require_generic_header_summaries(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
) -> None:
    expected_fields = (
        _LEGACY_PROOF_FIELDS
        if proof_stream.proof_header.get("contract_id")
        == PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
        else _SEMANTIC_PROOF_FIELDS
    )
    if set(proof_stream.proof_header).union({"shards"}) != expected_fields:
        raise AdmissionSealError("provider_directory_admission_proof_keyset_invalid")
    if (
        proof_stream.shard_count <= 0
        or type(proof_stream.proof_header.get("shard_count")) is not int
        or proof_stream.proof_header.get("shard_count") <= 0
        or proof_stream.proof_header.get("shard_count") != proof_stream.shard_count
        or proof_stream.proof_header.get("shard_set_sha256")
        != proof_stream.shard_set_digest.hexdigest()
        or proof_stream.proof_header.get("resource_count") != expected.resource_count
        or proof_stream.proof_header.get("dataset_hash") != expected.dataset_hash
        or proof_stream.proof_header.get("proof_sha256")
        != proof_stream._proof_digest()
    ):
        raise AdmissionSealError("provider_directory_admission_shard_summary_invalid")
    if (
        expected.expected_resource_hashes is not None
        or expected.expected_resource_counts is not None
    ) and (
        not isinstance(expected.expected_resource_hashes, Mapping)
        or not isinstance(expected.expected_resource_counts, Mapping)
        or proof_stream.proof_header.get("resource_hashes")
        != expected.expected_resource_hashes
        or proof_stream.proof_header.get("resource_counts")
        != expected.expected_resource_counts
    ):
        raise AdmissionSealError(
            "provider_directory_admission_completion_summary_invalid"
        )


def _require_parent_identity(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
) -> None:
    if (
        "dataset_hash" in proof_stream.metadata
        and proof_stream.metadata["dataset_hash"] != expected.dataset_hash
    ) or (
        "resource_count" in proof_stream.metadata
        and (
            type(proof_stream.metadata["resource_count"]) is not int
            or proof_stream.metadata["resource_count"] != expected.resource_count
        )
    ) or (
        "acquisition_root_run_id" in proof_stream.metadata
        and proof_stream.metadata["acquisition_root_run_id"]
        != expected.evidence_run_id
    ):
        raise AdmissionSealError(
            "provider_directory_admission_parent_identity_invalid"
        )


def _validated_lineage_context(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
) -> tuple[Any, bool, Any]:
    is_legacy_contract = (
        proof_stream.proof_header.get("contract_id")
        == PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
    )
    proof_scope = proof_stream.metadata.get(
        PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
    )
    source_ids = proof_stream.metadata.get("source_ids")
    selected_resources = proof_stream.metadata.get("selected_resources")
    scope_groups = (
        source_ids if isinstance(source_ids, list) else (),
        selected_resources if isinstance(selected_resources, list) else (),
        proof_scope or () if not is_legacy_contract else (),
    )
    if (
        not isinstance(source_ids, list)
        or not isinstance(selected_resources, list)
        or not is_legacy_contract
        and proof_scope is not None
        and not isinstance(proof_scope, list)
        or any(
            type(scope_item) is not str
            for scope_group in scope_groups
            for scope_item in scope_group
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof finalization lineage is invalid"
        )
    if (
        "resource_hash_contract" in proof_stream.metadata
        and proof_stream.metadata["resource_hash_contract"] is None
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory expected proof contract is invalid"
        )
    lineage = proof_store._validated_proof_lineage(
        dataset_id=expected.dataset_id,
        endpoint_id=expected.endpoint_id,
        acquisition_root_run_id=expected.evidence_run_id,
        source_ids=source_ids,
        selected_resources=selected_resources,
        proof_resource_scope=None if is_legacy_contract else proof_scope,
    )
    return lineage, is_legacy_contract, proof_scope


def _require_unchanged_lineage(
    proof_stream: Any,
    lineage: Any,
    is_legacy_contract: bool,
    proof_scope: Any,
) -> None:
    if (
        proof_stream.metadata["source_ids"] != lineage.source_ids
        or proof_stream.metadata["selected_resources"]
        != lineage.selected_resources
        or not is_legacy_contract
        and proof_scope is not None
        and proof_scope != lineage.proof_resource_scope
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof finalization lineage is invalid"
        )
    if (
        not is_legacy_contract
        and proof_scope is not None
        and proof_scope
        != proof_stream.proof_header.get(
            PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource scope changed"
        )


def _require_exact_resource_scope(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
    lineage: Any,
    is_legacy_contract: bool,
) -> set[str]:
    proof_store._validate_metadata_lineage(proof_stream.proof_header, lineage)
    proof_store._validate_metadata_summary(proof_stream.proof_header, lineage)
    exact_resource_types = set(
        lineage.proof_resource_scope or lineage.selected_resources
    )
    if (
        set(proof_stream.proof_header["resource_counts"])
        != exact_resource_types
        or set(proof_stream.proof_header["resource_hashes"])
        != exact_resource_types
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource scope changed"
        )
    if (
        proof_stream.resource_count != expected.resource_count
        or not set(proof_stream.resource_counts).issubset(exact_resource_types)
        or any(
            proof_stream.resource_counts.get(resource_type, 0)
            != finalized_count
            for resource_type, finalized_count in proof_stream.proof_header[
                "resource_counts"
            ].items()
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard resource total changed"
        )
    proof_store._assert_expected_proof_contract(
        proof_stream.proof_header,
        proof_stream.metadata.get(
            "resource_hash_contract",
            LEGACY_RESOURCE_HASH_CONTRACT,
        ),
        (
            None
            if is_legacy_contract
            else proof_stream.metadata.get("semantic_projection_as_of")
        ),
        lineage.proof_resource_scope,
    )
    return exact_resource_types


def _require_shard_scopes(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
    lineage: Any,
    exact_resource_types: set[str],
) -> None:
    with proof_stream.descriptor_path.open("rb") as descriptors:
        for line in descriptors:
            descriptor_by_field = proof_store._validated_shard_descriptor(
                json.loads(line),
                dataset_id=expected.dataset_id,
                endpoint_id=expected.endpoint_id,
                acquisition_root_run_id=expected.evidence_run_id,
                source_ids=lineage.source_ids,
            )
            if not set(descriptor_by_field["resource_counts"]).issubset(
                exact_resource_types
            ):
                raise ProviderDirectoryProofStoreError(
                    "provider directory proof shard resource scope changed"
                )


def _require_generic_proof_store_contract(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
) -> None:
    try:
        lineage, is_legacy_contract, proof_scope = _validated_lineage_context(
            proof_stream,
            expected,
        )
        _require_unchanged_lineage(
            proof_stream,
            lineage,
            is_legacy_contract,
            proof_scope,
        )
        exact_resource_types = _require_exact_resource_scope(
            proof_stream,
            expected,
            lineage,
            is_legacy_contract,
        )
        _require_shard_scopes(
            proof_stream,
            expected,
            lineage,
            exact_resource_types,
        )
    except (ProviderDirectoryProofStoreError, TypeError, ValueError) as error:
        raise AdmissionSealError(
            f"provider_directory_admission_shard_validation_invalid:{error}"
        ) from error


def _generic_receipt(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
) -> ProviderDirectoryAdmissionSeal:
    _require_generic_header_summaries(proof_stream, expected)
    _require_parent_identity(proof_stream, expected)
    _require_generic_proof_store_contract(proof_stream, expected)
    return _receipt(
        proof_stream.metadata,
        admission_kind=ADMISSION_KIND_GENERIC,
        proof_sha256=proof_stream.proof_header.get("proof_sha256"),
        resource_counts=proof_stream.proof_header.get("resource_counts"),
        proof_summary=_generic_proof_summary(proof_stream.proof_header),
    )


def _validate_finished_stream(
    proof_stream: Any,
    expected: _AdmissionCopyExpectation,
) -> ProviderDirectoryAdmissionSeal:
    proof_stream.descriptor_file.close()
    if not proof_stream.complete or proof_stream.mode != "root":
        raise AdmissionSealError(
            "provider_directory_admission_metadata_incomplete"
        )
    if UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY in proof_stream.seen_root_keys:
        return _uhc_receipt(proof_stream, expected)
    return _generic_receipt(proof_stream, expected)
