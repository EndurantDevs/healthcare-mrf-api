# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed, publication-disabled records for phase-one public evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
import hmac
from pathlib import Path
from typing import Literal, Mapping, TypeAlias

from process.evidence_record_values import (
    CanonicalAddressEvidence,
    EvidenceSourceRecordReference,
    OpaqueTaxIdentityReference,
    OrganizationNpiWitness,
    PublicEvidenceRecordError,
    PUBLIC_EVIDENCE_RECORD_CONTRACT,
    _EvidenceRecordBase,
    _detached_typed,
    _fail,
    _interval_payload,
    _normalize_variant,
    _source_record_payload,
    _strict_npi,
    _strict_prefixed_digest,
    _validated_release,
    _validated_temporal_scope,
    _variant_payload,
)
from process.evidence_record_batch import (
    PUBLIC_EVIDENCE_BATCH_MAX_RECORDS,
    PublicEvidenceBatch,
    _canonical_digest,
    build_public_evidence_batch,
    validate_public_evidence_batch,
)
from process.evidence_tic_binding_proof import _TicTaxIdentityBindingReceipt
from process.evidence_tic_tax_identity_binding import _resolve_tic_tax_identity_binding
from process.evidence_source_release_contract import (
    CanonicalUtcInterval,
    PublicEvidenceSourceReleaseDescriptor,
)

_EVIDENCE_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_V1\x00"
_COMMON_FIELDS = frozenset(
    ("record_type", "source_record", "observed_at", "effective_interval")
)
_TIC_RECORD_TYPE = "tic_provider_group_member"
_MAX_GENERIC_INPUT_FIELDS = 11
_TIC_VARIANT_FIELDS = (
    "tax_identity",
    "provider_group_ref",
    "member_npi",
    "_source_binding_receipt",
)
_VARIANT_FIELDS = {
    "fhir_same_organization_identifier": (
        "tax_identity",
        "organization_npi",
        "organization_resource_ref",
    ),
    "hospital_ein_type2_npi": (
        "tax_identity",
        "organization_npi",
        "hospital_entity_ref",
        "organization_witness",
    ),
    "npi_address": ("npi", "address"),
    "provider_directory_network_location": (
        "npi",
        "address",
        "practitioner_role_ref",
        "location_resource_ref",
        "network_resource_ref",
        "insurance_plan_resource_ref",
        "role_active",
    ),
    "direct_tax_identity_address": ("tax_identity", "address"),
}


@dataclass(frozen=True, slots=True, repr=False, init=False)
class TicProviderGroupMemberEvidence(_EvidenceRecordBase):
    tax_identity: OpaqueTaxIdentityReference
    provider_group_ref: str
    member_npi: str
    _source_binding_receipt: _TicTaxIdentityBindingReceipt
    record_type: Literal["tic_provider_group_member"] = field(
        default="tic_provider_group_member", init=False
    )

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise _fail()


@dataclass(frozen=True, slots=True, repr=False)
class FhirSameOrganizationIdentifierEvidence(_EvidenceRecordBase):
    tax_identity: OpaqueTaxIdentityReference
    organization_npi: str
    organization_resource_ref: str
    record_type: Literal["fhir_same_organization_identifier"] = field(
        default="fhir_same_organization_identifier", init=False
    )

    def __post_init__(self) -> None:
        _EvidenceRecordBase.__post_init__(self)
        _finish_record(self)


@dataclass(frozen=True, slots=True, repr=False)
class HospitalEinType2NpiEvidence(_EvidenceRecordBase):
    tax_identity: OpaqueTaxIdentityReference
    organization_npi: str
    hospital_entity_ref: str
    organization_witness: OrganizationNpiWitness
    record_type: Literal["hospital_ein_type2_npi"] = field(
        default="hospital_ein_type2_npi", init=False
    )

    def __post_init__(self) -> None:
        _EvidenceRecordBase.__post_init__(self)
        _finish_record(self)


@dataclass(frozen=True, slots=True, repr=False)
class NpiAddressEvidence(_EvidenceRecordBase):
    npi: str
    address: CanonicalAddressEvidence
    record_type: Literal["npi_address"] = field(default="npi_address", init=False)

    def __post_init__(self) -> None:
        _EvidenceRecordBase.__post_init__(self)
        _finish_record(self)


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryNetworkLocationEvidence(_EvidenceRecordBase):
    npi: str
    address: CanonicalAddressEvidence
    practitioner_role_ref: str
    location_resource_ref: str
    network_resource_ref: str
    insurance_plan_resource_ref: str
    role_active: Literal[True]
    record_type: Literal["provider_directory_network_location"] = field(
        default="provider_directory_network_location", init=False
    )

    def __post_init__(self) -> None:
        _EvidenceRecordBase.__post_init__(self)
        _finish_record(self)


@dataclass(frozen=True, slots=True, repr=False)
class DirectTaxIdentityAddressEvidence(_EvidenceRecordBase):
    tax_identity: OpaqueTaxIdentityReference
    address: CanonicalAddressEvidence
    candidate_only: Literal[True] = field(default=True, init=False)
    record_type: Literal["direct_tax_identity_address"] = field(
        default="direct_tax_identity_address", init=False
    )

    def __post_init__(self) -> None:
        _EvidenceRecordBase.__post_init__(self)
        _finish_record(self)


PublicEvidenceRecord: TypeAlias = (
    TicProviderGroupMemberEvidence
    | FhirSameOrganizationIdentifierEvidence
    | HospitalEinType2NpiEvidence
    | NpiAddressEvidence
    | ProviderDirectoryNetworkLocationEvidence
    | DirectTaxIdentityAddressEvidence
)


def _record_payload(
    record_type: str,
    release: PublicEvidenceSourceReleaseDescriptor,
    source_record: EvidenceSourceRecordReference,
    observed_at: str,
    effective_interval: CanonicalUtcInterval,
    values: Mapping[str, object],
) -> dict[str, object]:
    return {
        "contract": PUBLIC_EVIDENCE_RECORD_CONTRACT,
        "record_type": record_type,
        "source_kind": release.source_kind,
        "release_contract_sha256": release.contract_sha256,
        "source_record": _source_record_payload(source_record),
        "observed_at": observed_at,
        "effective_interval": _interval_payload(effective_interval),
        "evidence": _variant_payload(record_type, values),
        "positive_evidence_only": True,
    }


def _record_values(record: PublicEvidenceRecord, record_type: str) -> dict[str, object]:
    fields = (
        _TIC_VARIANT_FIELDS
        if record_type == _TIC_RECORD_TYPE
        else _VARIANT_FIELDS.get(record_type)
    )
    if fields is None:
        raise _fail()
    return {name: getattr(record, name) for name in fields}


_RECORD_CLASS_BY_TYPE = {
    "tic_provider_group_member": TicProviderGroupMemberEvidence,
    "fhir_same_organization_identifier": FhirSameOrganizationIdentifierEvidence,
    "hospital_ein_type2_npi": HospitalEinType2NpiEvidence,
    "npi_address": NpiAddressEvidence,
    "provider_directory_network_location": ProviderDirectoryNetworkLocationEvidence,
    "direct_tax_identity_address": DirectTaxIdentityAddressEvidence,
}
_RECORD_TYPE_BY_CLASS = dict(zip(_RECORD_CLASS_BY_TYPE.values(), _RECORD_CLASS_BY_TYPE))


def _finish_record(record: PublicEvidenceRecord) -> None:
    record_type = _RECORD_TYPE_BY_CLASS.get(type(record))
    if record_type is None or not _has_exact_fixed_record_state(record, record_type):
        raise _fail()
    normalized = _normalize_variant(
        record_type,
        record.release,
        record.source_record,
        _record_values(record, record_type),
    )
    for name, value in normalized.items():
        object.__setattr__(record, name, value)
    expected_id = _canonical_digest(
        _EVIDENCE_DOMAIN,
        _record_payload(
            record_type,
            record.release,
            record.source_record,
            record.observed_at,
            record.effective_interval,
            normalized,
        ),
        "ev1_",
    )
    supplied_id = _strict_prefixed_digest(record.evidence_id, "ev1_")
    if not hmac.compare_digest(expected_id, supplied_id):
        raise _fail()


def _construct_tic_provider_group_member_evidence(
    record_common_by_field: dict[str, object],
    variant_by_field: dict[str, object],
) -> TicProviderGroupMemberEvidence:
    tic_record = object.__new__(TicProviderGroupMemberEvidence)
    construction_by_field = {
        **record_common_by_field,
        **variant_by_field,
        "record_type": _TIC_RECORD_TYPE,
        "positive_evidence_only": True,
        "serving_authority": "none",
        "legal_ownership_claimed": False,
        "employment_claimed": False,
        "facility_claimed": False,
        "exact_rate_site_claimed": False,
        "site_match_claimed": False,
        "confidence_claimed": False,
        "deletion_enabled": False,
        "replacement_enabled": False,
        "publication_enabled": False,
    }
    for field_name, field_value in construction_by_field.items():
        object.__setattr__(tic_record, field_name, field_value)
    _EvidenceRecordBase.__post_init__(tic_record)
    _finish_record(tic_record)
    return tic_record


def _normalized_tic_variant(
    release: PublicEvidenceSourceReleaseDescriptor,
    source_record: EvidenceSourceRecordReference,
    member_npi: str,
    receipt: _TicTaxIdentityBindingReceipt,
) -> dict[str, object]:
    return _normalize_variant(
        _TIC_RECORD_TYPE,
        release,
        source_record,
        {
            "tax_identity": OpaqueTaxIdentityReference(
                receipt.identity_type,
                receipt.token_policy_ref,
                1,
                receipt.locator,
                receipt.full_hmac,
            ),
            "provider_group_ref": receipt.provider_group_ref,
            "member_npi": member_npi,
            "_source_binding_receipt": receipt,
        },
    )


def build_tic_provider_group_member_evidence(
    release: PublicEvidenceSourceReleaseDescriptor,
    admitted_bundle: object,
    *,
    scratch_root: str | Path,
    provider_group_global_id_128: bytes,
    member_npi: str,
    source_record: EvidenceSourceRecordReference,
    observed_at: str,
    effective_interval: CanonicalUtcInterval,
) -> TicProviderGroupMemberEvidence:
    """Build TiC evidence only from an exact row authenticated in this call."""
    try:
        fixed_release = _validated_release(release)
        fixed_source_record = _detached_typed(
            source_record, EvidenceSourceRecordReference
        )
        observed, effective = _validated_temporal_scope(
            fixed_release, observed_at, effective_interval
        )
        fixed_member_npi = _strict_npi(member_npi)
        receipt = _resolve_tic_tax_identity_binding(
            fixed_release,
            admitted_bundle,
            scratch_root=scratch_root,
            provider_group_global_id_128=provider_group_global_id_128,
        )
        variant_by_field = _normalized_tic_variant(
            fixed_release, fixed_source_record, fixed_member_npi, receipt
        )
        evidence_id = _canonical_digest(
            _EVIDENCE_DOMAIN,
            _record_payload(
                _TIC_RECORD_TYPE,
                fixed_release,
                fixed_source_record,
                observed,
                effective,
                variant_by_field,
            ),
            "ev1_",
        )
        record_common_by_field = dict(
            release=fixed_release,
            source_record=fixed_source_record,
            observed_at=observed,
            effective_interval=effective,
            evidence_id=evidence_id,
        )
        return _construct_tic_provider_group_member_evidence(
            record_common_by_field, variant_by_field
        )
    except PublicEvidenceRecordError:
        raise
    except Exception:
        raise _fail() from None


def build_public_evidence_record(
    release: PublicEvidenceSourceReleaseDescriptor, raw: Mapping[str, object]
) -> PublicEvidenceRecord:
    """Build one of five generic variants; TiC requires its dedicated builder."""
    try:
        fixed_release = _validated_release(release)
        if type(raw) is not dict or len(raw) > _MAX_GENERIC_INPUT_FIELDS:
            raise _fail()
        raw_keys = tuple(raw.keys())
        if any(type(name) is not str for name in raw_keys):
            raise _fail()
        if "record_type" not in raw:
            raise _fail()
        record_type = raw["record_type"]
        if type(record_type) is not str or record_type not in _VARIANT_FIELDS:
            raise _fail()
        if frozenset(raw_keys) != _COMMON_FIELDS.union(_VARIANT_FIELDS[record_type]):
            raise _fail()
        source_record = _detached_typed(
            raw["source_record"], EvidenceSourceRecordReference
        )
        observed_at, effective_interval = _validated_temporal_scope(
            fixed_release, raw["observed_at"], raw["effective_interval"]
        )
        variant_by_field = _normalize_variant(
            record_type,
            fixed_release,
            source_record,
            {name: raw[name] for name in _VARIANT_FIELDS[record_type]},
        )
        evidence_id = _canonical_digest(
            _EVIDENCE_DOMAIN,
            _record_payload(
                record_type,
                fixed_release,
                source_record,
                observed_at,
                effective_interval,
                variant_by_field,
            ),
            "ev1_",
        )
        return _RECORD_CLASS_BY_TYPE[record_type](
            release=fixed_release,
            source_record=source_record,
            observed_at=observed_at,
            effective_interval=effective_interval,
            evidence_id=evidence_id,
            **variant_by_field,
        )
    except PublicEvidenceRecordError:
        raise
    except Exception:
        raise _fail() from None


def _has_exact_fixed_record_state(
    record: PublicEvidenceRecord, record_type: str
) -> bool:
    return (
        type(record.record_type) is str
        and record.record_type == record_type
        and record.positive_evidence_only is True
        and type(record.serving_authority) is str
        and record.serving_authority == "none"
        and record.legal_ownership_claimed is False
        and record.employment_claimed is False
        and record.facility_claimed is False
        and record.exact_rate_site_claimed is False
        and record.site_match_claimed is False
        and record.confidence_claimed is False
        and record.deletion_enabled is False
        and record.replacement_enabled is False
        and record.publication_enabled is False
    )


def _rebuild_record(
    value: PublicEvidenceRecord, record_type: str
) -> PublicEvidenceRecord:
    variant_by_field = _record_values(value, record_type)
    record_common_by_field = dict(
        release=value.release,
        source_record=value.source_record,
        observed_at=value.observed_at,
        effective_interval=value.effective_interval,
        evidence_id=value.evidence_id,
    )
    if record_type == _TIC_RECORD_TYPE:
        return _construct_tic_provider_group_member_evidence(
            record_common_by_field, variant_by_field
        )
    return _RECORD_CLASS_BY_TYPE[record_type](
        **record_common_by_field, **variant_by_field
    )


def validate_public_evidence_record(value: object) -> PublicEvidenceRecord:
    """Integrity-only validation; it never enables serving or publication."""
    record_type = _RECORD_TYPE_BY_CLASS.get(type(value))
    if record_type is None:
        raise _fail()
    try:
        if not _has_exact_fixed_record_state(value, record_type):
            raise _fail()
        if (
            type(value) is DirectTaxIdentityAddressEvidence
            and value.candidate_only is not True
        ):
            raise _fail()
        return _rebuild_record(value, record_type)
    except PublicEvidenceRecordError:
        raise
    except Exception:
        raise _fail() from None


__all__ = [
    "CanonicalAddressEvidence",
    "DirectTaxIdentityAddressEvidence",
    "EvidenceSourceRecordReference",
    "FhirSameOrganizationIdentifierEvidence",
    "HospitalEinType2NpiEvidence",
    "NpiAddressEvidence",
    "OpaqueTaxIdentityReference",
    "OrganizationNpiWitness",
    "PUBLIC_EVIDENCE_BATCH_MAX_RECORDS",
    "PUBLIC_EVIDENCE_RECORD_CONTRACT",
    "ProviderDirectoryNetworkLocationEvidence",
    "PublicEvidenceBatch",
    "PublicEvidenceRecord",
    "PublicEvidenceRecordError",
    "TicProviderGroupMemberEvidence",
    "build_public_evidence_batch",
    "build_public_evidence_record",
    "build_tic_provider_group_member_evidence",
    "validate_public_evidence_batch",
    "validate_public_evidence_record",
]
