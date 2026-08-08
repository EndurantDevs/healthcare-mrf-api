# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict, dormant row shapes for prospective public-evidence persistence."""

from __future__ import annotations

from typing import Literal, NamedTuple, TypeAlias

from public_evidence.evidence_record_contract import PublicEvidenceRecord


PUBLIC_EVIDENCE_RECORD_PERSISTENCE_CANDIDATE_CONTRACT = (
    "healthporta.public-evidence-record-persistence-candidate.v1"
)
PUBLIC_EVIDENCE_RECORD_PERSISTENCE_CANDIDATE_REF_PREFIX = "pepc1_"
SOURCE_LINK_ORDERING_CONTRACT = (
    "healthporta_public_evidence_source_record_ref_"
    "utf8_byte_ascending_zero_based_v1"
)
SOURCE_REPORTED_NAME_BYTES_CONTRACT = "healthporta_exact_utf8_sha256_v1"

_INVALID = "public_evidence_record_persistence_candidate_invalid"


class PublicEvidenceRecordPersistenceCandidateError(RuntimeError):
    """One uniform prospective-row validation failure."""


def persistence_candidate_error() -> PublicEvidenceRecordPersistenceCandidateError:
    """Return a fresh public error without retaining private input values."""

    return PublicEvidenceRecordPersistenceCandidateError(_INVALID)


class PersistenceCandidateAuthorityState(NamedTuple):
    """Fixed proof and non-authority boundary for one row projection."""

    lifecycle_state: Literal["prospective_row_shape_only"]
    normalized_record_validated: Literal[True]
    row_shape_frozen: Literal[True]
    source_link_order_verified: Literal[True]
    exactly_one_typed_row_verified: Literal[True]
    row_digests_recomputed: Literal[True]
    positive_evidence_only: Literal[True]
    storage_schema_state: Literal["not_defined"]
    database_write_state: Literal["not_executed"]
    database_row_presence_verified: Literal[False]
    database_constraint_parity_verified: Literal[False]
    source_bytes_authenticated: Literal[False]
    complete_inventory_scan_verified: Literal[False]
    source_authenticity_claimed: Literal[False]
    legal_ownership_claimed: Literal[False]
    employment_claimed: Literal[False]
    facility_ownership_claimed: Literal[False]
    exact_rate_site_claimed: Literal[False]
    payer_confirmed_site_claimed: Literal[False]
    site_match_claimed: Literal[False]
    confidence_claimed: Literal[False]
    independence_claimed: Literal[False]
    database_io_authority: Literal["none"]
    writer_authority: Literal["none"]
    migration_authority: Literal["none"]
    adapter_execution_authority: Literal["none"]
    serving_authority: Literal["none"]
    current_pointer_authority: Literal["none"]
    publication_enabled: Literal[False]
    replacement_enabled: Literal[False]
    deletion_enabled: Literal[False]
    retirement_enabled: Literal[False]
    supersession_enabled: Literal[False]

    def __repr__(self) -> str:
        return "<persistence-candidate-authority prospective_row_shape_only>"

    __str__ = __repr__


class PublicEvidenceRecordCommonRow(NamedTuple):
    """Scalar common-row candidate shared by every evidence variant."""

    evidence_ref: str
    record_contract: str
    record_contract_sha256: str
    foundation_scope: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    observed_at: str
    effective_start_at: str
    effective_end_at: str | None
    record_type: str
    relationship_class: str
    source_record_count: int
    source_link_ordering_contract_id: str
    source_link_vector_sha256: str
    typed_row_sha256: str
    authority_state_sha256: str
    lifecycle_state: str
    positive_evidence_only: bool
    serving_authority: str
    current_pointer_authority: str
    database_io_authority: str
    publication_enabled: bool
    row_sha256: str

    def __repr__(self) -> str:
        return f"<public-evidence-common-row type={self.record_type!r}>"

    __str__ = __repr__


class PublicEvidenceRecordSourceLinkRow(NamedTuple):
    """One ordered source-record link owned by the common row."""

    evidence_ref: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    source_record_ordinal: int
    source_record_ref: str
    record_kind: str
    row_sha256: str

    def __repr__(self) -> str:
        return "<public-evidence-source-link-row>"

    __str__ = __repr__


class TaxIdentityRelationshipRow(NamedTuple):
    evidence_ref: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    record_type: Literal["tax_identity_relationship"]
    relationship_class: str
    tax_identity_ref: str
    provider_group_ref: str | None
    related_npi: str | None
    source_entity_ref: str | None
    membership_state: str | None
    candidate_only: bool
    row_sha256: str

    def __repr__(self) -> str:
        return "<public-evidence-typed-row type='tax_identity_relationship'>"

    __str__ = __repr__


class TaxIdentityNameRow(NamedTuple):
    evidence_ref: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    record_type: Literal["tax_identity_name"]
    relationship_class: str
    tax_identity_ref: str
    provider_group_ref: str | None
    source_entity_ref: str | None
    source_reported_name: str
    source_reported_name_bytes_contract_id: str
    source_reported_name_utf8_sha256: str
    name_kind: str
    name_normalization_contract_id: str
    normalized_name_sha256: str
    candidate_only: bool
    row_sha256: str

    def __repr__(self) -> str:
        return "<public-evidence-typed-row type='tax_identity_name'>"

    __str__ = __repr__


class NpiEnumerationRow(NamedTuple):
    evidence_ref: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    record_type: Literal["npi_enumeration"]
    relationship_class: str
    npi: str
    npi_entity_type: str
    enumeration_state: str
    row_sha256: str

    def __repr__(self) -> str:
        return "<public-evidence-typed-row type='npi_enumeration'>"

    __str__ = __repr__


class EntityAddressRow(NamedTuple):
    evidence_ref: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    record_type: Literal["entity_address"]
    relationship_class: str
    subject_npi: str | None
    source_entity_ref: str | None
    address_key: str
    address_site_key: str | None
    canonicalization_contract_id: str
    purpose: str
    zip5: str | None
    geo_derivation_contract_id: str
    geo_quality: str
    freshness_state: str
    freshness_rule_version: str
    freshness_as_of: str
    selection_rule_version: str
    selection_eligible: bool
    candidate_only: bool
    row_sha256: str

    def __repr__(self) -> str:
        return "<public-evidence-typed-row type='entity_address'>"

    __str__ = __repr__


class ProviderDirectoryNetworkLocationRow(NamedTuple):
    evidence_ref: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    record_type: Literal["provider_directory_network_location"]
    relationship_class: str
    npi: str
    address_key: str
    address_site_key: str | None
    canonicalization_contract_id: str
    purpose: str
    zip5: str | None
    geo_derivation_contract_id: str
    geo_quality: str
    freshness_state: str
    freshness_rule_version: str
    freshness_as_of: str
    selection_rule_version: str
    selection_eligible: bool
    npi_source_record_ref: str
    practitioner_role_source_record_ref: str
    location_source_record_ref: str
    network_source_record_ref: str
    insurance_plan_source_record_ref: str
    role_active: bool
    pricing_bridge_state: str
    row_sha256: str

    def __repr__(self) -> str:
        return (
            "<public-evidence-typed-row "
            "type='provider_directory_network_location'>"
        )

    __str__ = __repr__


PublicEvidenceTypedRow: TypeAlias = (
    TaxIdentityRelationshipRow
    | TaxIdentityNameRow
    | NpiEnumerationRow
    | EntityAddressRow
    | ProviderDirectoryNetworkLocationRow
)


class PublicEvidenceRecordPersistenceCandidate(NamedTuple):
    """One validated record projected into dormant prospective rows."""

    contract: str
    foundation_scope: str
    record: PublicEvidenceRecord
    common_row: PublicEvidenceRecordCommonRow
    source_link_rows: tuple[PublicEvidenceRecordSourceLinkRow, ...]
    typed_row: PublicEvidenceTypedRow
    candidate_ref: str
    contract_sha256: str
    authority_state: PersistenceCandidateAuthorityState

    def __repr__(self) -> str:
        return f"<public-evidence-persistence-candidate type={self.record.record_type!r}>"

    __str__ = __repr__


def fixed_persistence_candidate_authority() -> PersistenceCandidateAuthorityState:
    """Return the sole allowed no-I/O authority state."""

    return PersistenceCandidateAuthorityState(
        lifecycle_state="prospective_row_shape_only",
        normalized_record_validated=True,
        row_shape_frozen=True,
        source_link_order_verified=True,
        exactly_one_typed_row_verified=True,
        row_digests_recomputed=True,
        positive_evidence_only=True,
        storage_schema_state="not_defined",
        database_write_state="not_executed",
        database_row_presence_verified=False,
        database_constraint_parity_verified=False,
        source_bytes_authenticated=False,
        complete_inventory_scan_verified=False,
        source_authenticity_claimed=False,
        legal_ownership_claimed=False,
        employment_claimed=False,
        facility_ownership_claimed=False,
        exact_rate_site_claimed=False,
        payer_confirmed_site_claimed=False,
        site_match_claimed=False,
        confidence_claimed=False,
        independence_claimed=False,
        database_io_authority="none",
        writer_authority="none",
        migration_authority="none",
        adapter_execution_authority="none",
        serving_authority="none",
        current_pointer_authority="none",
        publication_enabled=False,
        replacement_enabled=False,
        deletion_enabled=False,
        retirement_enabled=False,
        supersession_enabled=False,
    )
