# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed validators for FHIR fences and connector source vectors."""

from __future__ import annotations

from typing import Any

from process.tin_npi_connector_policy import FhirTinNpiIdentifierPolicy
from process.tin_npi_connector_security import TinTokenPolicyDescriptor
from process.tin_npi_connector_support import (
    _HASH_HEX_PATTERN,
    TIN_NPI_FHIR_INPUT_RELATION,
    TIN_NPI_LOOKUP_CONTRACT_ID,
    TIN_NPI_LOOKUP_SCHEMA_VERSION,
    TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION,
    TinNpiConnectorError,
    strict_evidence_text,
)
from process.tin_npi_connector_temporal import canonical_evidence_as_of


def _require_hash(candidate: object, field_name: str) -> str:
    """Return one lowercase SHA-256 hex digest or fail closed."""

    if type(candidate) is not str or _HASH_HEX_PATTERN.fullmatch(candidate) is None:
        raise TinNpiConnectorError(f"{field_name} is invalid")
    return candidate


def _require_optional_text(
    candidate: object,
    field_name: str,
    *,
    limit: int,
) -> str | None:
    """Return absent or bounded printable metadata without normalization."""

    if candidate is None:
        return None
    return strict_evidence_text(candidate, field_name, limit=limit)


def _require_sorted_strings(
    candidate: object,
    field_name: str,
    *,
    limit: int,
) -> tuple[str, ...]:
    """Return one exact sorted unique tuple of bounded strings."""

    if type(candidate) is not tuple:
        raise TinNpiConnectorError(f"{field_name} is invalid")
    strings = tuple(
        strict_evidence_text(entry, field_name, limit=limit) for entry in candidate
    )
    if strings != tuple(sorted(set(strings))):
        raise TinNpiConnectorError(f"{field_name} is invalid")
    return strings


def _validate_dataset_identity(dataset: Any) -> None:
    """Validate immutable source, endpoint, dataset, and run identifiers."""

    strict_evidence_text(dataset.source_id, "source ID", limit=64)
    strict_evidence_text(dataset.endpoint_id, "endpoint ID", limit=64)
    strict_evidence_text(dataset.dataset_id, "dataset ID", limit=128)
    strict_evidence_text(dataset.evidence_run_id, "evidence run ID", limit=128)
    _require_sorted_strings(
        dataset.selected_resources,
        "selected resources",
        limit=64,
    )
    _require_sorted_strings(
        dataset.expected_resources,
        "expected resources",
        limit=64,
    )
    if dataset.recorded_expected_resources is not None:
        _require_sorted_strings(
            dataset.recorded_expected_resources,
            "recorded expected resources",
            limit=64,
        )


def _validate_dataset_lifecycle(dataset: Any) -> None:
    """Require a current published dataset with no staged cutover intent."""

    strict_evidence_text(dataset.status, "dataset status", limit=32)
    if (
        type(dataset.is_current) is not bool
        or type(dataset.promote_on_cutover) is not bool
    ):
        raise TinNpiConnectorError("FHIR dataset selection flags are invalid")
    if (
        dataset.status != "published"
        or not dataset.is_current
        or dataset.promote_on_cutover
        or dataset.expected_incumbent_dataset_id is not None
    ):
        raise TinNpiConnectorError(
            "connector FHIR dataset must already be current and published"
        )


def _validate_dataset_content_proof(dataset: Any) -> None:
    """Validate retained-resource counts and authenticated content digests."""

    _require_hash(dataset.dataset_hash, "FHIR dataset hash")
    if type(dataset.resource_count) is not int or dataset.resource_count < 0:
        raise TinNpiConnectorError("FHIR dataset resource count is invalid")
    if (
        type(dataset.organization_resource_count) is not int
        or not 0 <= dataset.organization_resource_count <= dataset.resource_count
    ):
        raise TinNpiConnectorError("FHIR Organization resource count is invalid")
    _require_hash(
        dataset.organization_resource_sha256,
        "FHIR Organization resource hash",
    )
    _require_hash(dataset.source_summary_sha256, "FHIR source-summary hash")
    strict_evidence_text(dataset.identifier_rule_id, "identifier rule ID", limit=128)
    _require_hash(dataset.identifier_rule_sha256, "FHIR identifier rule hash")


def _validate_dataset_completeness(dataset: Any) -> None:
    """Require importer-recorded completeness metadata for Organizations."""

    _require_optional_text(
        dataset.previous_dataset_id,
        "previous dataset ID",
        limit=128,
    )
    _require_optional_text(
        dataset.expected_incumbent_dataset_id,
        "expected incumbent dataset ID",
        limit=128,
    )
    _require_optional_text(dataset.validated_at, "validated at", limit=64)
    if dataset.validated_at is None:
        raise TinNpiConnectorError(
            "connector FHIR dataset requires validation evidence"
        )
    if (
        dataset.recorded_expected_resources is None
        or dataset.recorded_expected_resources != dataset.expected_resources
    ):
        raise TinNpiConnectorError(
            "connector FHIR dataset requires recorded expected resources"
        )
    if "Organization" not in dataset.selected_resources:
        raise TinNpiConnectorError("connector FHIR dataset must select Organization")


def validate_fhir_dataset_fence(dataset: Any) -> None:
    """Validate every field that binds one immutable FHIR dataset fence."""

    _validate_dataset_identity(dataset)
    _validate_dataset_lifecycle(dataset)
    _validate_dataset_content_proof(dataset)
    _validate_dataset_completeness(dataset)


def _validate_vector_contract(source_vector: Any) -> None:
    """Validate connector and lookup schema identifiers before inputs."""

    if source_vector.schema_version != TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION:
        raise TinNpiConnectorError("connector source-vector version is invalid")
    if (
        source_vector.lookup_schema_version != TIN_NPI_LOOKUP_SCHEMA_VERSION
        or source_vector.lookup_contract_id != TIN_NPI_LOOKUP_CONTRACT_ID
    ):
        raise TinNpiConnectorError("connector lookup contract is invalid")


def _validate_vector_datasets(source_vector: Any) -> None:
    """Require unique source datasets and consistent endpoint identities."""

    from process.tin_npi_connector_source import FhirDatasetFenceIdentity

    datasets = source_vector.fhir_datasets
    if (
        type(datasets) is not tuple
        or not datasets
        or any(type(dataset) is not FhirDatasetFenceIdentity for dataset in datasets)
    ):
        raise TinNpiConnectorError("connector FHIR datasets are invalid")
    dataset_keys = {
        (dataset.source_id, dataset.endpoint_id, dataset.dataset_id)
        for dataset in datasets
    }
    if len(dataset_keys) != len(datasets):
        raise TinNpiConnectorError("connector FHIR datasets are duplicated")
    if len({dataset.source_id for dataset in datasets}) != len(datasets):
        raise TinNpiConnectorError(
            "connector FHIR source selects more than one dataset"
        )
    _validate_endpoint_dataset_identities(datasets)


def _dataset_endpoint_identity(dataset: Any) -> tuple[object, ...]:
    """Return all fields that must agree for one endpoint identity."""

    return (
        dataset.endpoint_id,
        dataset.dataset_id,
        dataset.evidence_run_id,
        dataset.selected_resources,
        dataset.expected_resources,
        dataset.status,
        dataset.is_current,
        dataset.promote_on_cutover,
        dataset.dataset_hash,
        dataset.resource_count,
        dataset.organization_resource_count,
        dataset.organization_resource_sha256,
        dataset.source_summary_sha256,
        dataset.recorded_expected_resources,
        dataset.previous_dataset_id,
        dataset.expected_incumbent_dataset_id,
        dataset.validated_at,
    )


def _validate_endpoint_dataset_identities(datasets: tuple[Any, ...]) -> None:
    """Reject two source rows that disagree about one endpoint dataset."""

    dataset_identity_by_endpoint: dict[str, tuple[object, ...]] = {}
    for dataset in datasets:
        dataset_identity = _dataset_endpoint_identity(dataset)
        incumbent_identity = dataset_identity_by_endpoint.setdefault(
            dataset.endpoint_id,
            dataset_identity,
        )
        if incumbent_identity != dataset_identity:
            raise TinNpiConnectorError(
                "connector FHIR endpoint dataset identities conflict"
            )


def _validate_vector_relations(source_vector: Any) -> None:
    """Require the sole authoritative retained-resource input relation."""

    from process.tin_npi_connector_source import ConnectorRelationIdentity

    relations = source_vector.input_relations
    if (
        type(relations) is not tuple
        or len(relations) != 1
        or any(
            type(relation) is not ConnectorRelationIdentity for relation in relations
        )
    ):
        raise TinNpiConnectorError("connector input relations are invalid")
    if relations[0].relation != TIN_NPI_FHIR_INPUT_RELATION:
        raise TinNpiConnectorError("connector FHIR input relation is invalid")


def _validate_vector_token_policies(source_vector: Any) -> None:
    """Require at least one unique verified PTG token-policy descriptor."""

    token_policies = source_vector.token_policies
    if (
        type(token_policies) is not tuple
        or not token_policies
        or any(
            type(policy) is not TinTokenPolicyDescriptor for policy in token_policies
        )
    ):
        raise TinNpiConnectorError("connector token policies are invalid")
    policy_ids = tuple(policy.token_policy_id for policy in token_policies)
    if len(set(policy_ids)) != len(policy_ids):
        raise TinNpiConnectorError("connector token policies are duplicated")


def _validate_vector_identifier_policy(source_vector: Any) -> None:
    """Require exact equality between selected dataset and policy rule scopes."""

    if type(source_vector.identifier_policy) is not FhirTinNpiIdentifierPolicy:
        raise TinNpiConnectorError("connector identifier policy is invalid")
    selected_rule_by_scope = {
        (dataset.source_id, dataset.endpoint_id): (
            dataset.identifier_rule_id,
            dataset.identifier_rule_sha256,
        )
        for dataset in source_vector.fhir_datasets
    }
    policy_rule_by_scope = {
        (rule.source_id, rule.endpoint_id): (
            rule.rule_id,
            rule.descriptor_sha256,
        )
        for rule in source_vector.identifier_policy.rules
    }
    if selected_rule_by_scope != policy_rule_by_scope:
        raise TinNpiConnectorError("connector identifier policy scope is inconsistent")


def validate_connector_source_vector(source_vector: Any) -> None:
    """Validate exact source, relation, policy, and cutoff generation inputs."""

    _validate_vector_contract(source_vector)
    _validate_vector_datasets(source_vector)
    _validate_vector_relations(source_vector)
    _validate_vector_token_policies(source_vector)
    canonical_evidence_as_of(source_vector.evidence_as_of)
    _validate_vector_identifier_policy(source_vector)
    strict_evidence_text(
        source_vector.projection_policy_id,
        "projection policy ID",
        limit=128,
    )
