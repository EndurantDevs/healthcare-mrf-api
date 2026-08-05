# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict value objects shared by phase-one public evidence records."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hmac
import re
from typing import Any, Literal, Mapping
import uuid

from process.evidence_source_release_contract import (
    CanonicalUtcInterval,
    PublicEvidenceSourceReleaseDescriptor,
    validate_public_evidence_source_release,
)
from process.evidence_tic_binding_proof import (
    TIC_TAX_IDENTITY_POLICY_VERSION,
    _validate_tic_binding_for_record,
    _validate_tic_tax_identity_binding_receipt,
)

_INVALID = "public_evidence_record_invalid"
PUBLIC_EVIDENCE_RECORD_CONTRACT = "healthporta.public-evidence-record.v1"
_HEX_32_RE = re.compile(r"[0-9a-f]{32}", flags=re.ASCII)
_HEX_64_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_NPI_RE = re.compile(r"[0-9]{10}", flags=re.ASCII)
_NPI_MIN = 1_000_000_000
_NPI_MAX = 2_999_999_999
_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
    flags=re.ASCII,
)


class PublicEvidenceRecordError(RuntimeError):
    pass


def _fail() -> PublicEvidenceRecordError:
    return PublicEvidenceRecordError(_INVALID)


def _strict_prefixed_digest(value: object, prefix: str, *, short: bool = False) -> str:
    digest_re = _HEX_32_RE if short else _HEX_64_RE
    if (
        type(value) is not str
        or not value.startswith(prefix)
        or digest_re.fullmatch(value[len(prefix) :]) is None
    ):
        raise _fail()
    return value


def _strict_utc(value: object) -> str:
    if type(value) is not str or _UTC_RE.fullmatch(value) is None:
        raise _fail()
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise _fail() from None
    return value


def _strict_uuid_reference(value: object, prefix: str) -> str:
    if type(value) is not str or not value.startswith(prefix):
        raise _fail()
    raw_uuid = value[len(prefix) :]
    try:
        parsed = uuid.UUID(raw_uuid)
    except (AttributeError, TypeError, ValueError):
        raise _fail() from None
    if str(parsed) != raw_uuid:
        raise _fail()
    return value


def _strict_npi(value: object) -> str:
    if type(value) is not str or _NPI_RE.fullmatch(value) is None:
        raise _fail()
    if not _NPI_MIN <= int(value) <= _NPI_MAX:
        raise _fail()
    digits = [int(character) for character in "80840" + value]
    for offset in range(1, len(digits), 2):
        doubled = digits[-1 - offset] * 2
        digits[-1 - offset] = doubled // 10 + doubled % 10
    if sum(digits) % 10:
        raise _fail()
    return value


def _detached_typed(value: object, expected_type: type[Any]) -> Any:
    if type(value) is not expected_type:
        raise _fail()
    try:
        return expected_type(
            **{field_name: getattr(value, field_name) for field_name in value.__slots__}
        )
    except PublicEvidenceRecordError:
        raise
    except Exception:
        raise _fail() from None


def _validated_release(value: object) -> PublicEvidenceSourceReleaseDescriptor:
    try:
        return validate_public_evidence_source_release(value)
    except Exception:
        raise _fail() from None


@dataclass(frozen=True, slots=True, repr=False)
class OpaqueTaxIdentityReference:
    identity_type: Literal["ein", "npi"]
    token_policy_ref: str
    token_policy_version: Literal[1]
    locator: str
    full_hmac: str

    def __post_init__(self) -> None:
        if (
            type(self.identity_type) is not str
            or self.identity_type not in {"ein", "npi"}
            or type(self.token_policy_version) is not int
            or self.token_policy_version != TIC_TAX_IDENTITY_POLICY_VERSION
        ):
            raise _fail()
        _strict_prefixed_digest(self.token_policy_ref, "tip1_")
        locator = _strict_prefixed_digest(self.locator, "til1_", short=True)
        full_hmac = _strict_prefixed_digest(self.full_hmac, "tih1_")
        if not hmac.compare_digest(locator[5:], full_hmac[5:37]):
            raise _fail()


@dataclass(frozen=True, slots=True, repr=False)
class EvidenceSourceRecordReference:
    record_hmac: str
    payload_digest: str

    def __post_init__(self) -> None:
        _strict_prefixed_digest(self.record_hmac, "esr1_")
        _strict_prefixed_digest(self.payload_digest, "esp1_")


@dataclass(frozen=True, slots=True, repr=False)
class CanonicalAddressEvidence:
    address_key: str
    premise_key: str | None
    purpose: str

    def __post_init__(self) -> None:
        _strict_uuid_reference(self.address_key, "ak1_")
        if self.premise_key is not None:
            _strict_uuid_reference(self.premise_key, "pk1_")
        if type(self.purpose) is not str or self.purpose not in {
            "nppes_practice_location",
            "nppes_mailing",
            "provider_directory_location",
            "hospital_location_candidate",
        }:
            raise _fail()


def _validated_temporal_scope(
    release: PublicEvidenceSourceReleaseDescriptor,
    observed_at: object,
    effective_interval: object,
) -> tuple[str, CanonicalUtcInterval]:
    observed = _strict_utc(observed_at)
    effective = _detached_typed(effective_interval, CanonicalUtcInterval)
    release_observed = release.observed_interval
    release_effective = release.effective_interval
    if not release_observed.start_at <= observed <= release_observed.end_at:
        raise _fail()
    if effective.start_at < release_effective.start_at:
        raise _fail()
    if release_effective.end_at is not None and (
        effective.end_at is None or effective.end_at > release_effective.end_at
    ):
        raise _fail()
    return observed, effective


@dataclass(frozen=True, slots=True, repr=False)
class OrganizationNpiWitness:
    release_contract_ref: str
    source_record: EvidenceSourceRecordReference
    organization_npi: str
    semantic_type: Literal["organization_type_2"] = field(
        default="organization_type_2", init=False
    )
    source_semantics: Literal["hpt_same_record_organization_npi"] = field(
        default="hpt_same_record_organization_npi", init=False
    )

    def __post_init__(self) -> None:
        source_record = _detached_typed(
            self.source_record, EvidenceSourceRecordReference
        )
        _strict_prefixed_digest(self.release_contract_ref, "src1_")
        object.__setattr__(self, "source_record", source_record)
        object.__setattr__(self, "organization_npi", _strict_npi(self.organization_npi))


def _validated_organization_witness(value: object) -> OrganizationNpiWitness:
    if (
        type(value) is not OrganizationNpiWitness
        or type(value.semantic_type) is not str
        or value.semantic_type != "organization_type_2"
        or type(value.source_semantics) is not str
        or value.source_semantics != "hpt_same_record_organization_npi"
    ):
        raise _fail()
    try:
        return OrganizationNpiWitness(
            value.release_contract_ref,
            value.source_record,
            value.organization_npi,
        )
    except PublicEvidenceRecordError:
        raise
    except Exception:
        raise _fail() from None


@dataclass(frozen=True, slots=True, repr=False)
class _EvidenceRecordBase:
    release: PublicEvidenceSourceReleaseDescriptor
    source_record: EvidenceSourceRecordReference
    observed_at: str
    effective_interval: CanonicalUtcInterval
    evidence_id: str
    positive_evidence_only: Literal[True] = field(default=True, init=False)
    serving_authority: Literal["none"] = field(default="none", init=False)
    legal_ownership_claimed: Literal[False] = field(default=False, init=False)
    employment_claimed: Literal[False] = field(default=False, init=False)
    facility_claimed: Literal[False] = field(default=False, init=False)
    exact_rate_site_claimed: Literal[False] = field(default=False, init=False)
    site_match_claimed: Literal[False] = field(default=False, init=False)
    confidence_claimed: Literal[False] = field(default=False, init=False)
    deletion_enabled: Literal[False] = field(default=False, init=False)
    replacement_enabled: Literal[False] = field(default=False, init=False)
    publication_enabled: Literal[False] = field(default=False, init=False)

    def __post_init__(self) -> None:
        release = _validated_release(self.release)
        source_record = _detached_typed(
            self.source_record, EvidenceSourceRecordReference
        )
        observed, effective = _validated_temporal_scope(
            release, self.observed_at, self.effective_interval
        )
        object.__setattr__(self, "release", release)
        object.__setattr__(self, "source_record", source_record)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "effective_interval", effective)


def _tax_payload(value: OpaqueTaxIdentityReference) -> dict[str, object]:
    return {
        "identity_type": value.identity_type,
        "token_policy_ref": value.token_policy_ref,
        "token_policy_version": value.token_policy_version,
        "locator": value.locator,
        "full_hmac": value.full_hmac,
    }


def _source_record_payload(value: EvidenceSourceRecordReference) -> dict[str, str]:
    return {"record_hmac": value.record_hmac, "payload_digest": value.payload_digest}


def _address_payload(value: CanonicalAddressEvidence) -> dict[str, object]:
    return {
        "address_key": value.address_key,
        "premise_key": value.premise_key,
        "purpose": value.purpose,
    }


def _interval_payload(value: CanonicalUtcInterval) -> dict[str, object]:
    return {"start_at": value.start_at, "end_at": value.end_at}


def _witness_payload(value: OrganizationNpiWitness) -> dict[str, object]:
    return {
        "release_contract_ref": value.release_contract_ref,
        "source_record": _source_record_payload(value.source_record),
        "organization_npi": value.organization_npi,
        "semantic_type": value.semantic_type,
        "source_semantics": value.source_semantics,
    }


def _variant_payload(
    record_type: str, variant_by_field: Mapping[str, object]
) -> dict[str, object]:
    if record_type == "tic_provider_group_member":
        return {
            "tax_identity": _tax_payload(variant_by_field["tax_identity"]),
            "provider_group_ref": variant_by_field["provider_group_ref"],
            "member_npi": variant_by_field["member_npi"],
            "source_binding_receipt_ref": variant_by_field[
                "_source_binding_receipt"
            ].receipt_ref,
        }
    if record_type == "fhir_same_organization_identifier":
        return {
            "tax_identity": _tax_payload(variant_by_field["tax_identity"]),
            "organization_npi": variant_by_field["organization_npi"],
            "organization_resource_ref": variant_by_field["organization_resource_ref"],
        }
    if record_type == "hospital_ein_type2_npi":
        return {
            "tax_identity": _tax_payload(variant_by_field["tax_identity"]),
            "organization_npi": variant_by_field["organization_npi"],
            "hospital_entity_ref": variant_by_field["hospital_entity_ref"],
            "organization_witness": _witness_payload(
                variant_by_field["organization_witness"]
            ),
        }
    if record_type == "npi_address":
        return {
            "npi": variant_by_field["npi"],
            "address": _address_payload(variant_by_field["address"]),
        }
    if record_type == "provider_directory_network_location":
        network_fields = (
            "practitioner_role_ref",
            "location_resource_ref",
            "network_resource_ref",
            "insurance_plan_resource_ref",
            "role_active",
        )
        return {
            "npi": variant_by_field["npi"],
            "address": _address_payload(variant_by_field["address"]),
            **{name: variant_by_field[name] for name in network_fields},
        }
    if record_type == "direct_tax_identity_address":
        return {
            "tax_identity": _tax_payload(variant_by_field["tax_identity"]),
            "address": _address_payload(variant_by_field["address"]),
            "candidate_only": True,
        }
    raise _fail()


def _normalize_variant(
    record_type: str,
    release: PublicEvidenceSourceReleaseDescriptor,
    source_record: EvidenceSourceRecordReference,
    variant_by_field: Mapping[str, object],
) -> dict[str, object]:
    normalized_by_field = dict(variant_by_field)
    for field_name, expected_type in (
        ("tax_identity", OpaqueTaxIdentityReference),
        ("address", CanonicalAddressEvidence),
    ):
        if field_name in normalized_by_field:
            normalized_by_field[field_name] = _detached_typed(
                normalized_by_field[field_name], expected_type
            )
    for npi_field in ("member_npi", "organization_npi", "npi"):
        if npi_field in normalized_by_field:
            normalized_by_field[npi_field] = _strict_npi(normalized_by_field[npi_field])
    if "_source_binding_receipt" in normalized_by_field:
        normalized_by_field["_source_binding_receipt"] = (
            _validate_tic_tax_identity_binding_receipt(
                normalized_by_field["_source_binding_receipt"]
            )
        )
    return _normalize_variant_semantics(
        record_type, release, source_record, normalized_by_field
    )


def _normalize_variant_semantics(
    record_type: str,
    release: PublicEvidenceSourceReleaseDescriptor,
    source_record: EvidenceSourceRecordReference,
    normalized_by_field: dict[str, object],
) -> dict[str, object]:
    if record_type == "tic_provider_group_member":
        if release.source_kind != "tic":
            raise _fail()
        normalized_by_field["provider_group_ref"] = _strict_prefixed_digest(
            normalized_by_field["provider_group_ref"], "pg1_"
        )
        tax_identity = normalized_by_field["tax_identity"]
        normalized_by_field["_source_binding_receipt"] = (
            _validate_tic_binding_for_record(
                normalized_by_field["_source_binding_receipt"],
                release,
                identity_type=tax_identity.identity_type,
                token_policy_ref=tax_identity.token_policy_ref,
                provider_group_ref=normalized_by_field["provider_group_ref"],
                locator=tax_identity.locator,
                full_hmac=tax_identity.full_hmac,
            )
        )
        return normalized_by_field
    if record_type == "fhir_same_organization_identifier":
        if release.source_kind != "public_provider_directory_fhir":
            raise _fail()
        normalized_by_field["organization_resource_ref"] = _strict_prefixed_digest(
            normalized_by_field["organization_resource_ref"], "org1_"
        )
        return normalized_by_field
    if record_type == "hospital_ein_type2_npi":
        return _normalize_hospital_link(release, source_record, normalized_by_field)
    if record_type == "npi_address":
        return _normalize_npi_address(release.source_kind, normalized_by_field)
    if record_type == "provider_directory_network_location":
        return _normalize_network_location(release.source_kind, normalized_by_field)
    if record_type == "direct_tax_identity_address":
        tax_identity = normalized_by_field["tax_identity"]
        address = normalized_by_field["address"]
        if (
            release.source_kind != "public_hpt"
            or tax_identity.identity_type != "ein"
            or address.purpose != "hospital_location_candidate"
        ):
            raise _fail()
        return normalized_by_field
    raise _fail()


def _normalize_hospital_link(
    release: PublicEvidenceSourceReleaseDescriptor,
    source_record: EvidenceSourceRecordReference,
    normalized: dict[str, object],
) -> dict[str, object]:
    witness = _validated_organization_witness(normalized["organization_witness"])
    tax_identity = normalized["tax_identity"]
    if (
        release.source_kind != "public_hpt"
        or tax_identity.identity_type != "ein"
        or witness.organization_npi != normalized["organization_npi"]
        or witness.release_contract_ref != "src1_" + release.contract_sha256
        or witness.source_record != source_record
    ):
        raise _fail()
    normalized["organization_witness"] = witness
    normalized["hospital_entity_ref"] = _strict_prefixed_digest(
        normalized["hospital_entity_ref"], "hpt1_"
    )
    return normalized


def _normalize_npi_address(
    source_kind: str, normalized: dict[str, object]
) -> dict[str, object]:
    purposes_by_source = {
        "nppes_entity_address": {"nppes_practice_location", "nppes_mailing"},
        "public_provider_directory_fhir": {"provider_directory_location"},
    }
    address = normalized["address"]
    if (
        source_kind not in purposes_by_source
        or address.purpose not in purposes_by_source[source_kind]
    ):
        raise _fail()
    return normalized


def _normalize_network_location(
    source_kind: str, normalized: dict[str, object]
) -> dict[str, object]:
    address = normalized["address"]
    if (
        source_kind != "public_provider_directory_fhir"
        or address.purpose != "provider_directory_location"
        or normalized["role_active"] is not True
    ):
        raise _fail()
    prefix_by_field = {
        "practitioner_role_ref": "role1_",
        "location_resource_ref": "loc1_",
        "network_resource_ref": "net1_",
        "insurance_plan_resource_ref": "plan1_",
    }
    for name, prefix in prefix_by_field.items():
        normalized[name] = _strict_prefixed_digest(normalized[name], prefix)
    return normalized


__all__ = [
    "CanonicalAddressEvidence",
    "EvidenceSourceRecordReference",
    "OpaqueTaxIdentityReference",
    "OrganizationNpiWitness",
    "PublicEvidenceRecordError",
]
