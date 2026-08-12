# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Matched rooted-graph twin publication authority contract."""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
import json

from process.provider_directory_rooted_graph_identity import (
    ROOTED_GRAPH_SCOPE_PATTERN,
    SHA256_PATTERN,
)
from process.provider_directory_rooted_graph_store_contract import (
    ACQUISITION_PATTERN,
    INTENT_PATTERN,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID,
    RUN_PATTERN,
)


PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-single-root-admission.v1"
)


def _is_single_root_policy(raw_policy: object) -> bool:
    from process.provider_directory_fhir_root_policy import ReviewedRootPolicy

    return type(raw_policy) is dict and raw_policy == ReviewedRootPolicy(1).document()


def _has_valid_authority_branch(candidate: object) -> bool:
    from process.provider_directory_rooted_graph_twin_contract import (
        ATTEMPT_PATTERN,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
    )

    is_historical_twin = bool(
        candidate.admission_contract_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID
        and type(candidate.attempt_id) is str
        and ATTEMPT_PATTERN.fullmatch(candidate.attempt_id) is not None
        and type(candidate.comparison_acquisition_id) is str
        and ACQUISITION_PATTERN.fullmatch(candidate.comparison_acquisition_id)
        is not None
        and candidate.publication_acquisition_id
        != candidate.comparison_acquisition_id
        and candidate.reviewed_root_policy_json is None
        and candidate.acquisition_operation_key is None
    )
    is_single_root = bool(
        candidate.admission_contract_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID
        and candidate.attempt_id is None
        and candidate.comparison_acquisition_id is None
        and _is_single_root_policy(candidate.reviewed_root_policy_json)
        and type(candidate.acquisition_operation_key) is str
        and SHA256_PATTERN.fullmatch(candidate.acquisition_operation_key) is not None
    )
    return is_historical_twin or is_single_root


def _has_valid_admission_coordinates(candidate: object) -> bool:
    from process.provider_directory_rooted_graph_twin_contract import (
        ADMISSION_PATTERN,
        _ENDPOINT_PATTERN,
        _has_invalid_variant_lineage,
        _is_bounded_text,
    )

    return bool(
        candidate.storage_contract_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID
        and ADMISSION_PATTERN.fullmatch(candidate.admission_id) is not None
        and ACQUISITION_PATTERN.fullmatch(candidate.publication_acquisition_id)
        is not None
        and _has_valid_authority_branch(candidate)
        and RUN_PATTERN.fullmatch(candidate.publication_run_id) is not None
        and INTENT_PATTERN.fullmatch(candidate.dataset_intent_id) is not None
        and ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(candidate.scope_id) is not None
        and _is_bounded_text(candidate.root_source_id, 64)
        and _ENDPOINT_PATTERN.fullmatch(candidate.root_endpoint_id) is not None
        and _is_bounded_text(candidate.acquisition_source_id, 64)
        and _ENDPOINT_PATTERN.fullmatch(candidate.acquisition_endpoint_id) is not None
        and not _has_invalid_variant_lineage(candidate)
        and _is_bounded_text(candidate.source_authority_id, 64)
        and SHA256_PATTERN.fullmatch(candidate.endpoint_signature_sha256) is not None
        and _is_bounded_text(candidate.root_dataset_id, 96)
        and SHA256_PATTERN.fullmatch(candidate.root_dataset_hash) is not None
        and SHA256_PATTERN.fullmatch(candidate.root_content_proof_sha256) is not None
        and _is_bounded_text(candidate.root_cohort_id, 128)
        and type(candidate.root_resource_count) is int
        and candidate.root_resource_count >= 1
        and candidate.max_work_items > candidate.root_resource_count
        and _is_bounded_text(candidate.connector_id, 64)
        and SHA256_PATTERN.fullmatch(candidate.graph_contract_sha256) is not None
        and SHA256_PATTERN.fullmatch(candidate.query_contract_sha256) is not None
    )


def _has_valid_admission_proof(candidate: object) -> bool:
    count_fields = (
        "completed_count",
        "resource_count",
        "edge_count",
        "insurance_plan_count",
        "insurance_plan_page_count",
        "used_work_items",
        "used_resource_rows",
        "used_edge_rows",
        "used_payload_bytes",
    )
    hash_fields = (
        "terminal_set_sha256",
        "resource_set_sha256",
        "edge_set_sha256",
        "rooted_graph_sha256",
    )
    return bool(
        1 <= candidate.max_work_items <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS
        and 1
        <= candidate.max_resource_rows
        <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS
        and 1
        <= candidate.max_edge_rows
        <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS
        and 1
        <= candidate.max_payload_bytes
        <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES
        and all(
            type(getattr(candidate, name)) is int and getattr(candidate, name) >= 0
            for name in count_fields
        )
        and candidate.completed_count >= 1
        and candidate.insurance_plan_page_count >= 1
        and candidate.used_work_items == candidate.completed_count
        and candidate.used_resource_rows == candidate.resource_count
        and candidate.used_edge_rows == candidate.edge_count
        and candidate.used_work_items <= candidate.max_work_items
        and candidate.used_resource_rows <= candidate.max_resource_rows
        and candidate.used_edge_rows <= candidate.max_edge_rows
        and candidate.used_payload_bytes <= candidate.max_payload_bytes
        and all(
            SHA256_PATTERN.fullmatch(getattr(candidate, name)) is not None
            for name in hash_fields
        )
        and candidate.publication_authority is True
        and type(candidate.admitted_at) is datetime
        and candidate.admitted_at.tzinfo is not None
    )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphTwinAdmission:
    """Immutable historical-twin or reviewed single-root publication authority."""

    admission_id: str
    admission_contract_id: str
    storage_contract_id: str
    attempt_id: str | None
    publication_acquisition_id: str
    comparison_acquisition_id: str | None
    publication_run_id: str
    dataset_intent_id: str
    scope_id: str
    root_source_id: str
    root_endpoint_id: str
    acquisition_source_id: str
    acquisition_endpoint_id: str
    source_authority_id: str
    endpoint_signature_sha256: str
    root_dataset_id: str
    root_dataset_variant: str
    root_publication_contract_id: str
    root_dataset_hash: str
    root_content_proof_sha256: str
    root_cohort_id: str
    root_resource_count: int
    connector_id: str
    graph_contract_sha256: str
    query_contract_sha256: str
    max_work_items: int
    max_resource_rows: int
    max_edge_rows: int
    max_payload_bytes: int
    completed_count: int
    resource_count: int
    edge_count: int
    insurance_plan_count: int
    insurance_plan_page_count: int
    used_work_items: int
    used_resource_rows: int
    used_edge_rows: int
    used_payload_bytes: int
    terminal_set_sha256: str
    resource_set_sha256: str
    edge_set_sha256: str
    rooted_graph_sha256: str
    publication_authority: bool
    admitted_at: datetime
    reviewed_root_policy_json: dict[str, object] | None = None
    acquisition_operation_key: str | None = None

    def __post_init__(self) -> None:
        """Reject forged admission capabilities at the public type boundary."""

        from process.provider_directory_rooted_graph_twin_contract import (
            _digest_identifier,
        )

        identity_values = _admission_identity_values(self)
        expected_id = _digest_identifier(
            "pdrgad_",
            (
                self.admission_contract_id,
                *identity_values,
            ),
        )
        if (
            self.admission_id != expected_id
            or not _has_valid_admission_coordinates(self)
            or not _has_valid_admission_proof(self)
        ):
            raise ValueError("provider_directory_rooted_graph_twin_admission_invalid")


def _admission_identity_fields(contract_id: object) -> tuple[str, ...]:
    from process.provider_directory_rooted_graph_twin_contract import (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
    )

    excluded_fields = {"admission_id", "admitted_at"}
    if contract_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID:
        excluded_fields.update(
            {"reviewed_root_policy_json", "acquisition_operation_key"}
        )
    return tuple(
        field.name
        for field in fields(ProviderDirectoryRootedGraphTwinAdmission)
        if field.name not in excluded_fields
    )


def _admission_identity_values(admission: object) -> tuple[object, ...]:
    values: list[object] = []
    for field_name in _admission_identity_fields(
        getattr(admission, "admission_contract_id", None)
    ):
        values.append(
            _normalized_admission_identity_value(
                field_name, getattr(admission, field_name)
            )
        )
    return tuple(values)


def _normalized_admission_identity_value(
    field_name: str,
    value: object,
) -> object:
    if field_name == "reviewed_root_policy_json":
        return json.dumps(value, separators=(",", ":"), sort_keys=True)
    return value


def _attempt_proof_for_root(
    attempt: ProviderDirectoryRootedGraphTwinAttempt,
    publication_root: ProviderDirectoryRootedGraphSealedRoot,
) -> tuple[object, ...]:
    from process.provider_directory_rooted_graph_twin_contract import (
        _SEALED_COUNT_FIELDS,
        _SEALED_HASH_FIELDS,
    )

    prefix = (
        "first_"
        if publication_root.acquisition_id == attempt.first_acquisition_id
        else "second_"
    )
    return tuple(
        getattr(attempt, prefix + name)
        for name in (*_SEALED_COUNT_FIELDS, *_SEALED_HASH_FIELDS)
    )


def _admission_lineage_by_field(
    attempt: ProviderDirectoryRootedGraphTwinAttempt,
    publication_root: ProviderDirectoryRootedGraphSealedRoot,
    admitted_at: datetime,
) -> dict[str, object]:
    from process.provider_directory_rooted_graph_twin_contract import (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
    )

    comparison_id = (
        attempt.second_acquisition_id
        if publication_root.acquisition_id == attempt.first_acquisition_id
        else attempt.first_acquisition_id
    )
    return {
        "admission_contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
        "storage_contract_id": attempt.storage_contract_id,
        "attempt_id": attempt.attempt_id,
        "publication_acquisition_id": publication_root.acquisition_id,
        "comparison_acquisition_id": comparison_id,
        "reviewed_root_policy_json": None,
        "acquisition_operation_key": None,
        "publication_run_id": publication_root.run_id,
        "dataset_intent_id": attempt.dataset_intent_id,
        "scope_id": attempt.scope_id,
        "root_source_id": attempt.root_source_id,
        "root_endpoint_id": attempt.root_endpoint_id,
        "acquisition_source_id": attempt.acquisition_source_id,
        "acquisition_endpoint_id": attempt.acquisition_endpoint_id,
        "source_authority_id": attempt.source_authority_id,
        "endpoint_signature_sha256": attempt.endpoint_signature_sha256,
        "root_dataset_id": attempt.root_dataset_id,
        "root_dataset_variant": attempt.root_dataset_variant,
        "root_publication_contract_id": attempt.root_publication_contract_id,
        "root_dataset_hash": attempt.root_dataset_hash,
        "root_content_proof_sha256": attempt.root_content_proof_sha256,
        "root_cohort_id": attempt.root_cohort_id,
        "root_resource_count": attempt.root_resource_count,
        "connector_id": attempt.connector_id,
        "graph_contract_sha256": attempt.graph_contract_sha256,
        "query_contract_sha256": attempt.query_contract_sha256,
        "admitted_at": admitted_at,
    }


def _admission_proof_by_field(
    attempt: ProviderDirectoryRootedGraphTwinAttempt,
    publication_root: ProviderDirectoryRootedGraphSealedRoot,
) -> dict[str, object]:
    return {
        "max_work_items": attempt.max_work_items,
        "max_resource_rows": attempt.max_resource_rows,
        "max_edge_rows": attempt.max_edge_rows,
        "max_payload_bytes": attempt.max_payload_bytes,
        "completed_count": publication_root.completed_count,
        "resource_count": publication_root.resource_count,
        "edge_count": publication_root.edge_count,
        "insurance_plan_count": publication_root.insurance_plan_count,
        "insurance_plan_page_count": publication_root.insurance_plan_page_count,
        "used_work_items": publication_root.used_work_items,
        "used_resource_rows": publication_root.used_resource_rows,
        "used_edge_rows": publication_root.used_edge_rows,
        "used_payload_bytes": publication_root.used_payload_bytes,
        "terminal_set_sha256": publication_root.terminal_set_sha256,
        "resource_set_sha256": publication_root.resource_set_sha256,
        "edge_set_sha256": publication_root.edge_set_sha256,
        "rooted_graph_sha256": publication_root.rooted_graph_sha256,
        "publication_authority": True,
    }


def build_rooted_graph_twin_admission(
    attempt: ProviderDirectoryRootedGraphTwinAttempt,
    publication_root: ProviderDirectoryRootedGraphSealedRoot,
    *,
    admitted_at: datetime,
) -> ProviderDirectoryRootedGraphTwinAdmission:
    """Build authority only for the candidate-role member of a matched pair."""

    from process.provider_directory_rooted_graph_twin_contract import (
        _digest_identifier,
        ProviderDirectoryRootedGraphSealedRoot,
        ProviderDirectoryRootedGraphTwinAttempt,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
    )

    if (
        type(attempt) is not ProviderDirectoryRootedGraphTwinAttempt
        or type(publication_root) is not ProviderDirectoryRootedGraphSealedRoot
        or attempt.matched is not True
        or publication_root.acquisition_role != "candidate"
        or publication_root.acquisition_id
        not in {attempt.first_acquisition_id, attempt.second_acquisition_id}
        or publication_root.dataset_intent_id != attempt.dataset_intent_id
        or publication_root.scope_id != attempt.scope_id
        or publication_root.sealed_proof()
        != _attempt_proof_for_root(attempt, publication_root)
    ):
        raise ValueError("provider_directory_rooted_graph_twin_authority_invalid")
    admission_by_field = {
        **_admission_lineage_by_field(attempt, publication_root, admitted_at),
        **_admission_proof_by_field(attempt, publication_root),
    }
    identity_values = tuple(
        _normalized_admission_identity_value(
            field_name, admission_by_field[field_name]
        )
        for field_name in _admission_identity_fields(
            admission_by_field["admission_contract_id"]
        )
    )
    admission_by_field["admission_id"] = _digest_identifier(
        "pdrgad_",
        (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
            *identity_values,
        ),
    )
    return ProviderDirectoryRootedGraphTwinAdmission(**admission_by_field)


def build_rooted_graph_single_root_admission(
    publication_root: object,
    *,
    acquisition_operation_key: str,
    admitted_at: datetime,
) -> ProviderDirectoryRootedGraphTwinAdmission:
    """Build authority for one candidate-role seal under explicit policy one."""

    from process.provider_directory_fhir_root_policy import ReviewedRootPolicy
    from process.provider_directory_rooted_graph_twin_contract import (
        _digest_identifier,
        ProviderDirectoryRootedGraphSealedRoot,
    )

    if (
        type(publication_root) is not ProviderDirectoryRootedGraphSealedRoot
        or publication_root.acquisition_role != "candidate"
        or type(acquisition_operation_key) is not str
        or SHA256_PATTERN.fullmatch(acquisition_operation_key) is None
        or type(admitted_at) is not datetime
        or admitted_at.tzinfo is None
    ):
        raise ValueError(
            "provider_directory_rooted_graph_single_root_authority_invalid"
        )
    admission_by_field = {
        field.name: getattr(publication_root, field.name)
        for field in fields(ProviderDirectoryRootedGraphTwinAdmission)
        if hasattr(publication_root, field.name)
    }
    admission_by_field.update(
        admission_contract_id=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID
        ),
        attempt_id=None,
        publication_acquisition_id=publication_root.acquisition_id,
        comparison_acquisition_id=None,
        reviewed_root_policy_json=ReviewedRootPolicy(1).document(),
        acquisition_operation_key=acquisition_operation_key,
        publication_run_id=publication_root.run_id,
        publication_authority=True,
        admitted_at=admitted_at,
    )
    identity_values = tuple(
        _normalized_admission_identity_value(
            field_name, admission_by_field[field_name]
        )
        for field_name in _admission_identity_fields(
            admission_by_field["admission_contract_id"]
        )
    )
    admission_by_field["admission_id"] = _digest_identifier(
        "pdrgad_",
        (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
            *identity_values,
        ),
    )
    return ProviderDirectoryRootedGraphTwinAdmission(**admission_by_field)


build_provider_directory_rooted_graph_twin_admission = build_rooted_graph_twin_admission


__all__ = (
    "build_rooted_graph_single_root_admission",
    "build_provider_directory_rooted_graph_twin_admission",
    "ProviderDirectoryRootedGraphTwinAdmission",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID",
)
