# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Inactive, database-free admission contract for connector publications."""

from __future__ import annotations

import hmac
import math
from dataclasses import dataclass

from process.tin_npi_connector_generation import CompactTinNpiGeneration
from process.tin_npi_connector_source import TinNpiConnectorSourceVector
from process.tin_npi_connector_support import (
    TIN_NPI_GENERATION_CONTRACT_ID,
    TIN_NPI_LOOKUP_CONTRACT_ID,
    TIN_NPI_LOOKUP_SCHEMA_VERSION,
    TIN_NPI_PROJECTION_POLICY_ID,
    TIN_NPI_RAW_POLICY_ID,
    TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION,
    TinNpiConnectorError,
)

_MAX_SIGNED_BIGINT = (1 << 63) - 1
_MAX_SIGNED_INTEGER = (1 << 31) - 1
_INTEGER_COUNT_FIELDS = (
    "source_count",
    "dataset_count",
    "token_policy_count",
    "metadata_byte_count",
)
_BIGINT_COUNT_FIELDS = (
    "organization_count",
    "evidence_row_count",
    "forward_row_count",
    "reverse_row_count",
    "npi_edge_count",
)


class TinNpiConnectorPublicationError(TinNpiConnectorError):
    """Reject an unsafe connector publication without exposing its inputs."""


@dataclass(frozen=True)
class ConnectorPublicationCounts:
    """Non-sensitive cardinalities used for capacity admission."""

    source_count: int
    dataset_count: int
    token_policy_count: int
    metadata_byte_count: int
    organization_count: int
    evidence_row_count: int
    forward_row_count: int
    reverse_row_count: int
    npi_edge_count: int

    def __post_init__(self) -> None:
        invalid_integer_count = any(
            type(getattr(self, field_name)) is not int
            or not 0 <= getattr(self, field_name) <= _MAX_SIGNED_INTEGER
            for field_name in _INTEGER_COUNT_FIELDS
        )
        invalid_bigint_count = any(
            type(getattr(self, field_name)) is not int
            or not 0 <= getattr(self, field_name) <= _MAX_SIGNED_BIGINT
            for field_name in _BIGINT_COUNT_FIELDS
        )
        if invalid_integer_count or invalid_bigint_count:
            raise TinNpiConnectorPublicationError(
                "connector publication counts are invalid"
            )


@dataclass(frozen=True)
class ConnectorPublicationLimits:
    """Mandatory finite capacity and strictly nested future DB timeouts."""

    max_sources: int
    max_datasets: int
    max_token_policies: int
    max_metadata_bytes: int
    max_organizations: int
    max_evidence_rows: int
    max_forward_rows: int
    max_reverse_rows: int
    max_npi_edges: int
    allow_complete_zero_evidence: bool = False
    copy_batch_size: int = 4096
    build_lease_seconds: int = 3600
    lock_timeout_ms: int = 500
    statement_timeout_ms: int = 300_000
    operation_timeout_seconds: float = 310.0

    def __post_init__(self) -> None:
        integer_capacity_limits = (
            self.max_sources,
            self.max_datasets,
            self.max_token_policies,
            self.max_metadata_bytes,
        )
        bigint_capacity_limits = (
            self.max_organizations,
            self.max_evidence_rows,
            self.max_forward_rows,
            self.max_reverse_rows,
            self.max_npi_edges,
        )
        operation_timeout = self.operation_timeout_seconds
        is_invalid = (
            any(
                type(limit) is not int or not 0 <= limit <= _MAX_SIGNED_INTEGER
                for limit in integer_capacity_limits
            )
            or any(
                type(limit) is not int or not 0 <= limit <= _MAX_SIGNED_BIGINT
                for limit in bigint_capacity_limits
            )
            or type(self.allow_complete_zero_evidence) is not bool
            or type(self.copy_batch_size) is not int
            or not 128 <= self.copy_batch_size <= 16_384
            or type(self.build_lease_seconds) is not int
            or not 30 <= self.build_lease_seconds <= 86_400
            or type(self.lock_timeout_ms) is not int
            or not 1 <= self.lock_timeout_ms <= 30_000
            or type(self.statement_timeout_ms) is not int
            or not 1 <= self.statement_timeout_ms <= 3_600_000
            or type(operation_timeout) not in {int, float}
            or operation_timeout <= 0
            or operation_timeout >= self.build_lease_seconds
            or not math.isfinite(operation_timeout)
            or operation_timeout * 1000 <= self.statement_timeout_ms
            or self.lock_timeout_ms >= self.statement_timeout_ms
        )
        if is_invalid:
            raise TinNpiConnectorPublicationError(
                "connector publication limits are invalid"
            )


@dataclass(frozen=True, repr=False)
class ConnectorPublicationBundle:
    """One exact immutable source vector and generation pair.

    Constructing a bundle does not authorize a source, load a database, or move
    the serving pointer. A later publisher must require an independent source-
    rights decision; those remain explicit later operations.
    """

    source_vector: TinNpiConnectorSourceVector
    generation: CompactTinNpiGeneration

    def __post_init__(self) -> None:
        _validate_publication_bundle(self.source_vector, self.generation)

    @property
    def counts(self) -> ConnectorPublicationCounts:
        """Return bounded, non-sensitive cardinalities for admission."""

        generation = self.generation
        return ConnectorPublicationCounts(
            source_count=len(generation.source_ordinal_map),
            dataset_count=len(self.source_vector.fhir_datasets),
            token_policy_count=len(self.source_vector.token_policies),
            metadata_byte_count=_metadata_byte_count(self),
            organization_count=generation.organization_count,
            evidence_row_count=len(generation.evidence_rows),
            forward_row_count=len(generation.forward_rows),
            reverse_row_count=len(generation.reverse_rows),
            npi_edge_count=sum(len(row.npis) for row in generation.forward_rows),
        )

    @property
    def generation_contract_id(self) -> str:
        """Return the frozen database generation contract."""

        return TIN_NPI_GENERATION_CONTRACT_ID

    @property
    def raw_policy_id(self) -> str:
        """Return the frozen no-plaintext database policy."""

        return TIN_NPI_RAW_POLICY_ID

    def __repr__(self) -> str:
        generation = self.generation
        return (
            "<tin-npi-connector-publication-bundle "
            f"sources={len(generation.source_ordinal_map)} "
            f"policies={len(self.source_vector.token_policies)} "
            f"evidence={len(generation.evidence_rows)}>"
        )


def admit_connector_publication_bundle(
    bundle: ConnectorPublicationBundle,
    *,
    limits: ConnectorPublicationLimits,
) -> ConnectorPublicationCounts:
    """Apply explicit zero-row and capacity policy before any database work."""

    if (
        type(bundle) is not ConnectorPublicationBundle
        or type(limits) is not ConnectorPublicationLimits
    ):
        raise TinNpiConnectorPublicationError(
            "connector publication admission input is invalid"
        )
    counts = bundle.counts
    if not limits.allow_complete_zero_evidence and counts.evidence_row_count == 0:
        raise TinNpiConnectorPublicationError(
            "connector publication zero evidence requires explicit admission"
        )
    observed_and_limits = (
        (counts.source_count, limits.max_sources),
        (counts.dataset_count, limits.max_datasets),
        (counts.token_policy_count, limits.max_token_policies),
        (counts.metadata_byte_count, limits.max_metadata_bytes),
        (counts.organization_count, limits.max_organizations),
        (counts.evidence_row_count, limits.max_evidence_rows),
        (counts.forward_row_count, limits.max_forward_rows),
        (counts.reverse_row_count, limits.max_reverse_rows),
        (counts.npi_edge_count, limits.max_npi_edges),
    )
    if any(observed > maximum for observed, maximum in observed_and_limits):
        raise TinNpiConnectorPublicationError(
            "connector publication capacity exceeded"
        )
    return counts


def _metadata_byte_count(bundle: ConnectorPublicationBundle) -> int:
    source_vector = bundle.source_vector
    generation = bundle.generation
    canonical_payloads = (
        source_vector.canonical_json,
        source_vector.identifier_policy.descriptor_canonical_json,
        generation.source_ordinal_map_json,
        generation.scan_proof_canonical_json,
    )
    return sum(len(payload.encode("utf-8")) for payload in canonical_payloads)


def _validate_publication_bundle(
    source_vector: object,
    generation: object,
) -> None:
    if (
        type(source_vector) is not TinNpiConnectorSourceVector
        or type(generation) is not CompactTinNpiGeneration
    ):
        raise TinNpiConnectorPublicationError(
            "connector publication bundle is invalid"
        )
    _validate_frozen_contract(source_vector)
    if not hmac.compare_digest(
        source_vector.source_vector_id,
        generation.source_vector_id,
    ):
        raise TinNpiConnectorPublicationError(
            "connector publication source binding is invalid"
        )
    _validate_source_and_proof_scope(source_vector, generation)
    _validate_evidence_scope(source_vector, generation)


def _validate_frozen_contract(source_vector: TinNpiConnectorSourceVector) -> None:
    if (
        source_vector.schema_version != TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION
        or source_vector.lookup_schema_version != TIN_NPI_LOOKUP_SCHEMA_VERSION
        or source_vector.lookup_contract_id != TIN_NPI_LOOKUP_CONTRACT_ID
        or source_vector.projection_policy_id != TIN_NPI_PROJECTION_POLICY_ID
    ):
        raise TinNpiConnectorPublicationError(
            "connector publication contract is unsupported"
        )


def _validate_source_and_proof_scope(
    source_vector: TinNpiConnectorSourceVector,
    generation: CompactTinNpiGeneration,
) -> None:
    datasets_by_key = {
        (dataset.source_id, dataset.endpoint_id, dataset.dataset_id): dataset
        for dataset in source_vector.fhir_datasets
    }
    proofs_by_key = {
        (proof.source_id, proof.endpoint_id, proof.dataset_id): proof
        for proof in generation.scan_proofs
    }
    canonical_source_ids = tuple(
        sorted(
            {dataset.source_id for dataset in source_vector.fhir_datasets},
            key=lambda value: value.encode("utf-8"),
        )
    )
    expected_policy_ids = tuple(sorted(source_vector.token_policy_ids))
    if (
        generation.source_ordinal_map != canonical_source_ids
        or datasets_by_key.keys() != proofs_by_key.keys()
        or any(
            not _is_proof_matching_dataset(
                proofs_by_key[dataset_key],
                dataset,
                expected_policy_ids=expected_policy_ids,
            )
            for dataset_key, dataset in datasets_by_key.items()
        )
    ):
        raise TinNpiConnectorPublicationError(
            "connector publication source proof is invalid"
        )


def _is_proof_matching_dataset(
    proof,
    dataset,
    *,
    expected_policy_ids: tuple[str, ...],
) -> bool:
    proof_policy_ids = tuple(
        policy_id for policy_id, _count in proof.matched_evidence_counts
    )
    return (
        proof.organization_resource_count == dataset.organization_resource_count
        and hmac.compare_digest(
            proof.organization_resource_sha256,
            dataset.organization_resource_sha256,
        )
        and hmac.compare_digest(
            proof.source_summary_sha256,
            dataset.source_summary_sha256,
        )
        and proof.identifier_rule_id == dataset.identifier_rule_id
        and hmac.compare_digest(
            proof.identifier_rule_sha256,
            dataset.identifier_rule_sha256,
        )
        and proof_policy_ids == expected_policy_ids
    )


def _validate_evidence_scope(
    source_vector: TinNpiConnectorSourceVector,
    generation: CompactTinNpiGeneration,
) -> None:
    datasets_by_key = {
        (dataset.source_id, dataset.endpoint_id, dataset.dataset_id): dataset
        for dataset in source_vector.fhir_datasets
    }
    selected_policy_ids = set(source_vector.token_policy_ids)
    identifier_policy = source_vector.identifier_policy
    identifier_policy_digest = identifier_policy.descriptor_sha256
    for evidence in generation.evidence_rows:
        dataset = datasets_by_key.get(
            (
                evidence.source_id,
                evidence.source_endpoint_id,
                evidence.source_dataset_id,
            )
        )
        if (
            dataset is None
            or evidence.token.token_policy_id not in selected_policy_ids
            or evidence.evidence_as_of != source_vector.evidence_as_of
            or evidence.identifier_policy_id != identifier_policy.policy_id
            or not hmac.compare_digest(
                evidence.identifier_policy_sha256,
                identifier_policy_digest,
            )
            or evidence.identifier_rule_id != dataset.identifier_rule_id
            or not hmac.compare_digest(
                evidence.identifier_rule_sha256,
                dataset.identifier_rule_sha256,
            )
        ):
            raise TinNpiConnectorPublicationError(
                "connector publication evidence is outside its source vector"
            )
