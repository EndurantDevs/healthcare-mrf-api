# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Role-neutral sealed comparison and publication authority contracts."""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
import hashlib
import re

from process.provider_directory_rooted_graph_identity import (
    ROOTED_GRAPH_SCOPE_PATTERN,
    SHA256_PATTERN,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
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


PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ATTEMPT_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-twin-attempt.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-matched-admission.v1"
)

ATTEMPT_PATTERN = re.compile(r"pdrgat_[0-9a-f]{48}\Z")
ADMISSION_PATTERN = re.compile(r"pdrgad_[0-9a-f]{48}\Z")
_ENDPOINT_PATTERN = re.compile(r"[0-9a-f]{64}\Z")

_SEALED_COUNT_FIELDS = (
    "pending_count",
    "leased_count",
    "completed_count",
    "error_count",
    "resource_count",
    "edge_count",
    "insurance_plan_count",
    "insurance_plan_page_count",
    "used_work_items",
    "used_resource_rows",
    "used_edge_rows",
    "used_payload_bytes",
)
_SEALED_HASH_FIELDS = (
    "terminal_set_sha256",
    "resource_set_sha256",
    "edge_set_sha256",
    "rooted_graph_sha256",
)
_SHARED_LINEAGE_FIELDS = (
    "storage_contract_id",
    "scope_id",
    "root_source_id",
    "root_endpoint_id",
    "acquisition_source_id",
    "acquisition_endpoint_id",
    "source_authority_id",
    "endpoint_signature_sha256",
    "root_dataset_id",
    "root_dataset_variant",
    "root_publication_contract_id",
    "root_dataset_hash",
    "root_content_proof_sha256",
    "root_cohort_id",
    "root_resource_count",
    "connector_id",
    "graph_contract_sha256",
    "query_contract_sha256",
    "dataset_intent_id",
    "max_work_items",
    "max_resource_rows",
    "max_edge_rows",
    "max_payload_bytes",
)


class ProviderDirectoryRootedGraphTwinError(RuntimeError):
    """Expose one bounded comparison or authority failure."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "identity": "rooted graph twin identity is invalid",
            "mismatch": "rooted graph twins do not match",
            "missing": "rooted graph twin admission is missing",
            "stale": "rooted graph twin root is no longer current",
            "state": "rooted graph twin state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


def _is_bounded_text(value: object, maximum: int) -> bool:
    return bool(
        type(value) is str
        and 1 <= len(value) <= maximum
        and value == value.strip()
        and all(character.isprintable() for character in value)
    )


def _digest_identifier(prefix: str, values: tuple[object, ...]) -> str:
    content = "\x1f".join(str(value) for value in values)
    return prefix + hashlib.sha256(content.encode("utf-8")).hexdigest()[:48]


def _has_invalid_variant_lineage(candidate: object) -> bool:
    variant = getattr(candidate, "root_dataset_variant", None)
    publication_contract_id = getattr(candidate, "root_publication_contract_id", None)
    has_same_source = getattr(candidate, "root_source_id", None) == getattr(
        candidate, "acquisition_source_id", None
    )
    has_same_endpoint = getattr(candidate, "root_endpoint_id", None) == getattr(
        candidate, "acquisition_endpoint_id", None
    )
    return bool(
        variant not in {"uhc_flex_practitioner", "rooted_combined"}
        or PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT.get(variant)
        != publication_contract_id
        or (
            variant == "rooted_combined" and not (has_same_source and has_same_endpoint)
        )
        or (
            variant == "uhc_flex_practitioner"
            and (has_same_source or has_same_endpoint)
        )
    )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphSealedRoot:
    """One immutable sealed acquisition projected without request order."""

    acquisition_id: str
    storage_contract_id: str
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
    acquisition_role: str
    run_id: str
    dataset_intent_id: str
    max_work_items: int
    max_resource_rows: int
    max_edge_rows: int
    max_payload_bytes: int
    pending_count: int
    leased_count: int
    completed_count: int
    error_count: int
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

    def __post_init__(self) -> None:
        counts = tuple(getattr(self, name) for name in _SEALED_COUNT_FIELDS)
        hashes = tuple(getattr(self, name) for name in _SEALED_HASH_FIELDS)
        if (
            ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or self.storage_contract_id
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID
            or ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(self.scope_id) is None
            or not _is_bounded_text(self.root_source_id, 64)
            or _ENDPOINT_PATTERN.fullmatch(self.root_endpoint_id) is None
            or not _is_bounded_text(self.acquisition_source_id, 64)
            or _ENDPOINT_PATTERN.fullmatch(self.acquisition_endpoint_id) is None
            or not _is_bounded_text(self.source_authority_id, 64)
            or SHA256_PATTERN.fullmatch(self.endpoint_signature_sha256) is None
            or not _is_bounded_text(self.root_dataset_id, 96)
            or _has_invalid_variant_lineage(self)
            or SHA256_PATTERN.fullmatch(self.root_dataset_hash) is None
            or SHA256_PATTERN.fullmatch(self.root_content_proof_sha256) is None
            or not _is_bounded_text(self.root_cohort_id, 128)
            or type(self.root_resource_count) is not int
            or self.root_resource_count < 1
            or self.max_work_items <= self.root_resource_count
            or not _is_bounded_text(self.connector_id, 64)
            or SHA256_PATTERN.fullmatch(self.graph_contract_sha256) is None
            or SHA256_PATTERN.fullmatch(self.query_contract_sha256) is None
            or self.acquisition_role not in {"baseline", "candidate"}
            or RUN_PATTERN.fullmatch(self.run_id) is None
            or INTENT_PATTERN.fullmatch(self.dataset_intent_id) is None
            or not 1
            <= self.max_work_items
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS
            or not 1
            <= self.max_resource_rows
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS
            or not 1
            <= self.max_edge_rows
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS
            or not 1
            <= self.max_payload_bytes
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES
            or any(type(count) is not int or count < 0 for count in counts)
            or self.pending_count != 0
            or self.leased_count != 0
            or self.completed_count < 1
            or self.error_count != 0
            or self.insurance_plan_page_count < 1
            or self.used_work_items != self.completed_count
            or self.used_resource_rows != self.resource_count
            or self.used_edge_rows != self.edge_count
            or self.used_work_items > self.max_work_items
            or self.used_resource_rows > self.max_resource_rows
            or self.used_edge_rows > self.max_edge_rows
            or self.used_payload_bytes > self.max_payload_bytes
            or any(SHA256_PATTERN.fullmatch(digest) is None for digest in hashes)
        ):
            raise ValueError("provider_directory_rooted_graph_sealed_root_invalid")

    def shared_lineage(self) -> tuple[object, ...]:
        """Return the request-order-neutral lineage compared across twins."""

        return tuple(getattr(self, name) for name in _SHARED_LINEAGE_FIELDS)

    def sealed_proof(self) -> tuple[object, ...]:
        """Return every sealed count and hash that must match exactly."""

        return tuple(
            getattr(self, name)
            for name in (*_SEALED_COUNT_FIELDS, *_SEALED_HASH_FIELDS)
        )


def _ordered_roots(
    first: ProviderDirectoryRootedGraphSealedRoot,
    second: ProviderDirectoryRootedGraphSealedRoot,
) -> tuple[
    ProviderDirectoryRootedGraphSealedRoot, ProviderDirectoryRootedGraphSealedRoot
]:
    if (
        type(first) is not ProviderDirectoryRootedGraphSealedRoot
        or type(second) is not ProviderDirectoryRootedGraphSealedRoot
        or first.acquisition_id == second.acquisition_id
        or {first.acquisition_role, second.acquisition_role}
        != {"baseline", "candidate"}
        or first.run_id == second.run_id
        or first.shared_lineage() != second.shared_lineage()
    ):
        raise ValueError("provider_directory_rooted_graph_twin_lineage_invalid")
    return tuple(sorted((first, second), key=lambda root: root.acquisition_id))


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphTwinAttempt:
    """One immutable request-order-neutral comparison of sealed roots."""

    attempt_id: str
    attempt_contract_id: str
    storage_contract_id: str
    first_acquisition_id: str
    second_acquisition_id: str
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
    first_pending_count: int
    second_pending_count: int
    first_leased_count: int
    second_leased_count: int
    first_completed_count: int
    second_completed_count: int
    first_error_count: int
    second_error_count: int
    first_resource_count: int
    second_resource_count: int
    first_edge_count: int
    second_edge_count: int
    first_insurance_plan_count: int
    second_insurance_plan_count: int
    first_insurance_plan_page_count: int
    second_insurance_plan_page_count: int
    first_used_work_items: int
    second_used_work_items: int
    first_used_resource_rows: int
    second_used_resource_rows: int
    first_used_edge_rows: int
    second_used_edge_rows: int
    first_used_payload_bytes: int
    second_used_payload_bytes: int
    first_terminal_set_sha256: str
    second_terminal_set_sha256: str
    first_resource_set_sha256: str
    second_resource_set_sha256: str
    first_edge_set_sha256: str
    second_edge_set_sha256: str
    first_rooted_graph_sha256: str
    second_rooted_graph_sha256: str
    matched: bool
    attempted_at: datetime

    def __post_init__(self) -> None:
        """Reject forged or mismatched role-neutral attempt evidence."""

        first_values, second_values, expected_id = _attempt_validation_values(self)
        if (
            self.attempt_contract_id
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ATTEMPT_CONTRACT_ID
            or self.storage_contract_id
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID
            or ATTEMPT_PATTERN.fullmatch(self.attempt_id) is None
            or self.attempt_id != expected_id
            or ACQUISITION_PATTERN.fullmatch(self.first_acquisition_id) is None
            or ACQUISITION_PATTERN.fullmatch(self.second_acquisition_id) is None
            or self.first_acquisition_id >= self.second_acquisition_id
            or INTENT_PATTERN.fullmatch(self.dataset_intent_id) is None
            or ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(self.scope_id) is None
            or not _is_bounded_text(self.root_source_id, 64)
            or _ENDPOINT_PATTERN.fullmatch(self.root_endpoint_id) is None
            or not _is_bounded_text(self.acquisition_source_id, 64)
            or _ENDPOINT_PATTERN.fullmatch(self.acquisition_endpoint_id) is None
            or _has_invalid_variant_lineage(self)
            or not _is_bounded_text(self.source_authority_id, 64)
            or SHA256_PATTERN.fullmatch(self.endpoint_signature_sha256) is None
            or not _is_bounded_text(self.root_dataset_id, 96)
            or SHA256_PATTERN.fullmatch(self.root_dataset_hash) is None
            or SHA256_PATTERN.fullmatch(self.root_content_proof_sha256) is None
            or not _is_bounded_text(self.root_cohort_id, 128)
            or type(self.root_resource_count) is not int
            or self.root_resource_count < 1
            or self.max_work_items <= self.root_resource_count
            or not _is_bounded_text(self.connector_id, 64)
            or SHA256_PATTERN.fullmatch(self.graph_contract_sha256) is None
            or SHA256_PATTERN.fullmatch(self.query_contract_sha256) is None
            or not 1
            <= self.max_work_items
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS
            or not 1
            <= self.max_resource_rows
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS
            or not 1
            <= self.max_edge_rows
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS
            or not 1
            <= self.max_payload_bytes
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES
            or self.matched is not (first_values == second_values)
            or type(self.attempted_at) is not datetime
            or self.attempted_at.tzinfo is None
        ):
            raise ValueError("provider_directory_rooted_graph_twin_attempt_invalid")


def _attempt_validation_values(
    attempt: ProviderDirectoryRootedGraphTwinAttempt,
) -> tuple[tuple[object, ...], tuple[object, ...], str]:
    first_values = tuple(
        getattr(attempt, "first_" + name) for name in _SEALED_COUNT_FIELDS
    ) + tuple(getattr(attempt, "first_" + name) for name in _SEALED_HASH_FIELDS)
    second_values = tuple(
        getattr(attempt, "second_" + name) for name in _SEALED_COUNT_FIELDS
    ) + tuple(getattr(attempt, "second_" + name) for name in _SEALED_HASH_FIELDS)
    identity_values = tuple(
        getattr(attempt, field.name)
        for field in fields(attempt)
        if field.name not in {"attempt_id", "attempted_at"}
    )
    expected_id = _digest_identifier(
        "pdrgat_",
        (PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ATTEMPT_CONTRACT_ID, *identity_values),
    )
    return first_values, second_values, expected_id


def build_rooted_graph_twin_attempt(
    one: ProviderDirectoryRootedGraphSealedRoot,
    two: ProviderDirectoryRootedGraphSealedRoot,
    *,
    attempted_at: datetime,
) -> ProviderDirectoryRootedGraphTwinAttempt:
    """Build the same attempt for either caller order."""

    first, second = _ordered_roots(one, two)
    attempt_by_field: dict[str, object] = {
        "attempt_contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ATTEMPT_CONTRACT_ID,
        "storage_contract_id": first.storage_contract_id,
        "first_acquisition_id": first.acquisition_id,
        "second_acquisition_id": second.acquisition_id,
        "dataset_intent_id": first.dataset_intent_id,
        "scope_id": first.scope_id,
        "root_source_id": first.root_source_id,
        "root_endpoint_id": first.root_endpoint_id,
        "acquisition_source_id": first.acquisition_source_id,
        "acquisition_endpoint_id": first.acquisition_endpoint_id,
        "source_authority_id": first.source_authority_id,
        "endpoint_signature_sha256": first.endpoint_signature_sha256,
        "root_dataset_id": first.root_dataset_id,
        "root_dataset_variant": first.root_dataset_variant,
        "root_publication_contract_id": first.root_publication_contract_id,
        "root_dataset_hash": first.root_dataset_hash,
        "root_content_proof_sha256": first.root_content_proof_sha256,
        "root_cohort_id": first.root_cohort_id,
        "root_resource_count": first.root_resource_count,
        "connector_id": first.connector_id,
        "graph_contract_sha256": first.graph_contract_sha256,
        "query_contract_sha256": first.query_contract_sha256,
        "max_work_items": first.max_work_items,
        "max_resource_rows": first.max_resource_rows,
        "max_edge_rows": first.max_edge_rows,
        "max_payload_bytes": first.max_payload_bytes,
        "matched": first.sealed_proof() == second.sealed_proof(),
        "attempted_at": attempted_at,
    }
    for name in (*_SEALED_COUNT_FIELDS, *_SEALED_HASH_FIELDS):
        attempt_by_field["first_" + name] = getattr(first, name)
        attempt_by_field["second_" + name] = getattr(second, name)
    identity_values = tuple(
        attempt_by_field[field.name]
        for field in fields(ProviderDirectoryRootedGraphTwinAttempt)
        if field.name not in {"attempt_id", "attempted_at"}
    )
    attempt_by_field["attempt_id"] = _digest_identifier(
        "pdrgat_",
        (PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ATTEMPT_CONTRACT_ID, *identity_values),
    )
    return ProviderDirectoryRootedGraphTwinAttempt(**attempt_by_field)


build_provider_directory_rooted_graph_twin_attempt = build_rooted_graph_twin_attempt


from process.provider_directory_rooted_graph_twin_admission_contract import (
    build_rooted_graph_single_root_admission,
    build_provider_directory_rooted_graph_twin_admission,
    ProviderDirectoryRootedGraphTwinAdmission,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
)


__all__ = (
    "build_rooted_graph_single_root_admission",
    "build_provider_directory_rooted_graph_twin_admission",
    "build_provider_directory_rooted_graph_twin_attempt",
    "ProviderDirectoryRootedGraphSealedRoot",
    "ProviderDirectoryRootedGraphTwinAdmission",
    "ProviderDirectoryRootedGraphTwinAttempt",
    "ProviderDirectoryRootedGraphTwinError",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ATTEMPT_CONTRACT_ID",
)
